use std::any::{Any, TypeId, type_name};
use std::collections::HashMap;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::{Arc, RwLock};

use pharos_core::DomainEvent;
use serde::de::DeserializeOwned;
use thiserror::Error;
use tracing::{Instrument, debug, info_span};

use crate::event_handler::EventHandler;

/// Errors produced while publishing events through the [`EventBus`].
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum EventBusError {
    /// A registered handler returned an error.
    #[error("event handler failed for '{event_type}': {source}")]
    HandlerError {
        /// Logical event type that was being dispatched.
        event_type: &'static str,
        /// The handler's original error, preserved as a typed source.
        #[source]
        source: Box<dyn std::error::Error + Send + Sync + 'static>,
    },
    /// Several handlers failed while publishing under
    /// [`PublishErrorPolicy::CollectAll`].
    #[error("{} event handlers failed for '{event_type}'", errors.len())]
    HandlerErrors {
        /// Logical event type that was being dispatched.
        event_type: &'static str,
        /// The individual handler failures, in registration order.
        errors: Vec<EventBusError>,
    },
    /// A payload could not be decoded back into its concrete event type during
    /// [`EventBus::publish_erased`] (the relay/outbox seam). The bytes did not
    /// match the type registered for `topic` via [`EventBus::register_decoder`].
    #[error("failed to decode payload for topic '{topic}': {source}")]
    DecodeError {
        /// Routing topic whose registered decoder rejected the payload.
        topic: String,
        /// The underlying deserialization error.
        #[source]
        source: serde_json::Error,
    },
}

/// Decides what happens when a handler fails during [`EventBus::publish`].
///
/// Handlers registered for the same event are independent reactions; the
/// policy controls whether one failing reaction blocks the others.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum PublishErrorPolicy {
    /// Stop at the first failing handler. Handlers registered after the
    /// failing one do not see the event; a retried publish re-delivers to
    /// every handler, so handlers must be idempotent.
    #[default]
    FailFast,
    /// Deliver the event to every handler regardless of failures, then report
    /// all collected errors together. Use this when handlers are independent
    /// and one failure must not starve the others.
    CollectAll,
}

type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Registry of erased handlers keyed by the concrete event `TypeId`.
type HandlerRegistry = HashMap<TypeId, Vec<Arc<dyn ErasedHandler>>>;

/// Decodes an outbox payload back into a boxed concrete event of a known type.
type DecodeFn =
    Arc<dyn Fn(&[u8]) -> Result<Box<dyn Any + Send + Sync>, serde_json::Error> + Send + Sync>;

/// Registry mapping a routing topic to the concrete event type it decodes into,
/// used by [`EventBus::publish_erased`] to reconstruct a typed event from bytes.
type DecoderRegistry = HashMap<String, (TypeId, DecodeFn)>;

trait ErasedHandler: Send + Sync {
    fn call<'a>(
        &'a self,
        event: &'a (dyn Any + Send + Sync),
    ) -> BoxFuture<'a, Result<(), EventBusError>>;
}

struct HandlerWrapper<E, H> {
    inner: Arc<H>,
    _marker: PhantomData<fn(E)>,
}

impl<E, H> ErasedHandler for HandlerWrapper<E, H>
where
    E: DomainEvent,
    H: EventHandler<E>,
{
    fn call<'a>(
        &'a self,
        event: &'a (dyn Any + Send + Sync),
    ) -> BoxFuture<'a, Result<(), EventBusError>> {
        let handler = Arc::clone(&self.inner);
        Box::pin(async move {
            // The map is keyed by `TypeId::of::<E>()`, so this downcast always
            // succeeds; the fallible API documents that invariant defensively.
            let typed = event
                .downcast_ref::<E>()
                .ok_or_else(|| EventBusError::HandlerError {
                    event_type: "<unknown>",
                    source: Box::<dyn std::error::Error + Send + Sync>::from(
                        "event bus invariant violated: TypeId matched but downcast failed",
                    ),
                })?;

            handler
                .handle(typed)
                .instrument(info_span!(
                    "event_handler",
                    handler = type_name::<H>(),
                    event_type = typed.event_type(),
                    event.aggregate_id = typed.aggregate_id(),
                ))
                .await
                .map_err(|error| EventBusError::HandlerError {
                    event_type: typed.event_type(),
                    source: Box::new(error),
                })
        })
    }
}

/// In-process event bus that dispatches domain events to typed handlers.
///
/// `EventBus` is a concrete, cheaply cloneable type. All clones share the same
/// registered handlers through an internal `Arc`. Dispatch is fully typed: the
/// publishing call site keeps the concrete event type, so there is no trait
/// object, no `Any` leakage into the domain, and no per-event allocation beyond
/// the handler futures themselves.
///
/// For cross-process delivery, publish through the outbox seam instead.
#[derive(Clone, Default)]
pub struct EventBus {
    handlers: Arc<RwLock<HandlerRegistry>>,
    decoders: Arc<RwLock<DecoderRegistry>>,
    error_policy: PublishErrorPolicy,
}

impl std::fmt::Debug for EventBus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let registered = self.handlers.read().map(|m| m.len()).unwrap_or(0);
        f.debug_struct("EventBus")
            .field("registered_event_types", &registered)
            .field("error_policy", &self.error_policy)
            .finish()
    }
}

impl EventBus {
    /// Creates an empty event bus with the default
    /// [`PublishErrorPolicy::FailFast`] policy.
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates an empty event bus with an explicit [`PublishErrorPolicy`].
    pub fn with_error_policy(error_policy: PublishErrorPolicy) -> Self {
        Self {
            handlers: Arc::default(),
            decoders: Arc::default(),
            error_policy,
        }
    }

    /// Returns the configured publish error policy.
    pub fn error_policy(&self) -> PublishErrorPolicy {
        self.error_policy
    }

    /// Registers a handler for a concrete domain event type.
    ///
    /// Multiple handlers may be registered for the same event type; they run in
    /// registration order.
    pub fn register<E, H>(&self, handler: H)
    where
        E: DomainEvent,
        H: EventHandler<E>,
    {
        let wrapper = Arc::new(HandlerWrapper::<E, H> {
            inner: Arc::new(handler),
            _marker: PhantomData,
        });

        self.handlers
            .write()
            .unwrap_or_else(|p| p.into_inner())
            .entry(TypeId::of::<E>())
            .or_default()
            .push(wrapper);
    }

    /// Publishes a concrete domain event to all handlers registered for its type.
    ///
    /// Events without registered handlers are dropped silently, which keeps
    /// publishing decoupled from consumption.
    ///
    /// Handlers run sequentially in registration order. What happens when one
    /// fails depends on the configured [`PublishErrorPolicy`]:
    ///
    /// - [`FailFast`](PublishErrorPolicy::FailFast) (default): the first error
    ///   stops the run, so handlers registered after the failing one do not
    ///   see the event.
    /// - [`CollectAll`](PublishErrorPolicy::CollectAll): every handler sees the
    ///   event; the collected failures are reported together.
    ///
    /// In both cases a retried publish re-delivers the event to every handler,
    /// including those that already succeeded — handlers must be idempotent
    /// under this at-least-once semantic.
    pub async fn publish<E>(&self, event: &E) -> Result<(), EventBusError>
    where
        E: DomainEvent,
    {
        let span = info_span!(
            "event_bus.publish",
            event_type = event.event_type(),
            event.aggregate_id = event.aggregate_id(),
            event.occurred_at = %event.occurred_at(),
        );

        async move {
            let handlers = {
                let map = self.handlers.read().unwrap_or_else(|p| p.into_inner());

                match map.get(&TypeId::of::<E>()) {
                    Some(handlers) => handlers.clone(),
                    None => {
                        debug!("no handler registered for event");
                        return Ok(());
                    }
                }
            };

            let any: &(dyn Any + Send + Sync) = event;

            let mut errors = Vec::new();
            for handler in &handlers {
                match handler.call(any).await {
                    Ok(()) => {}
                    Err(error) => match self.error_policy {
                        PublishErrorPolicy::FailFast => return Err(error),
                        PublishErrorPolicy::CollectAll => errors.push(error),
                    },
                }
            }

            match errors.len() {
                0 => Ok(()),
                1 => Err(errors.remove(0)),
                _ => Err(EventBusError::HandlerErrors {
                    event_type: event.event_type(),
                    errors,
                }),
            }
        }
        .instrument(span)
        .await
    }

    /// Registers how to decode a serialized event carried on `topic` back into
    /// its concrete type `E`, so [`publish_erased`](Self::publish_erased) can
    /// deliver outbox/relay payloads to the same typed handlers as
    /// [`publish`](Self::publish).
    ///
    /// This is the in-process bridge for the durable outbox seam: a background
    /// relay reads `Message { topic, payload }` rows and has only bytes, while
    /// dispatch is keyed by `TypeId`. Registering a decoder ties a stable topic
    /// string to `TypeId::of::<E>()` and a JSON decoder, closing that gap
    /// without leaking `Any`/`TypeId` into the domain. Register the decoder for
    /// every event type you route through the outbox, alongside its handlers.
    ///
    /// Re-registering the same topic overwrites the previous decoder.
    pub fn register_decoder<E>(&self, topic: impl Into<String>)
    where
        E: DomainEvent + DeserializeOwned,
    {
        let decoder: DecodeFn = Arc::new(|bytes: &[u8]| {
            let event: E = serde_json::from_slice(bytes)?;
            Ok(Box::new(event) as Box<dyn Any + Send + Sync>)
        });

        self.decoders
            .write()
            .unwrap_or_else(|p| p.into_inner())
            .insert(topic.into(), (TypeId::of::<E>(), decoder));
    }

    /// Decodes a serialized event and publishes it to the handlers registered
    /// for its concrete type — the type-erased counterpart of
    /// [`publish`](Self::publish) used by the outbox relay.
    ///
    /// `topic` must have been registered with
    /// [`register_decoder`](Self::register_decoder). Behaviour mirrors
    /// `publish` exactly once the concrete event is reconstructed: same handler
    /// order, same [`PublishErrorPolicy`], same at-least-once semantics.
    ///
    /// - Unknown `topic` (no decoder) is a silent no-op returning `Ok`, so
    ///   publishing stays decoupled from consumption — matching how `publish`
    ///   drops events with no registered handler.
    /// - A payload that fails to decode returns
    ///   [`EventBusError::DecodeError`] so the relay can retry or dead-letter.
    pub async fn publish_erased(
        &self,
        topic: &str,
        payload: &[u8],
    ) -> Result<(), EventBusError> {
        let span = info_span!("event_bus.publish_erased", event.topic = topic);

        async move {
            let Some((type_id, decoder)) = ({
                let decoders = self.decoders.read().unwrap_or_else(|p| p.into_inner());
                decoders.get(topic).cloned()
            }) else {
                debug!("no decoder registered for topic");
                return Ok(());
            };

            let boxed = decoder(payload).map_err(|source| EventBusError::DecodeError {
                topic: topic.to_owned(),
                source,
            })?;

            let handlers = {
                let map = self.handlers.read().unwrap_or_else(|p| p.into_inner());
                match map.get(&type_id) {
                    Some(handlers) => handlers.clone(),
                    None => {
                        debug!("no handler registered for decoded event");
                        return Ok(());
                    }
                }
            };

            let any: &(dyn Any + Send + Sync) = &*boxed;

            let mut errors = Vec::new();
            for handler in &handlers {
                match handler.call(any).await {
                    Ok(()) => {}
                    Err(error) => match self.error_policy {
                        PublishErrorPolicy::FailFast => return Err(error),
                        PublishErrorPolicy::CollectAll => errors.push(error),
                    },
                }
            }

            match errors.len() {
                0 => Ok(()),
                1 => Err(errors.remove(0)),
                _ => Err(EventBusError::HandlerErrors {
                    event_type: "<erased>",
                    errors,
                }),
            }
        }
        .instrument(span)
        .await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU32, Ordering};

    use chrono::{DateTime, Utc};

    use super::*;

    #[derive(Debug)]
    struct Ping {
        occurred_at: DateTime<Utc>,
    }

    impl DomainEvent for Ping {
        fn event_type(&self) -> &'static str {
            "Ping"
        }
        fn occurred_at(&self) -> DateTime<Utc> {
            self.occurred_at
        }
        fn aggregate_id(&self) -> &str {
            "ping-1"
        }
    }

    #[derive(Debug, thiserror::Error)]
    #[error("boom")]
    struct Boom;

    struct Failing;
    impl EventHandler<Ping> for Failing {
        type Error = Boom;
        async fn handle(&self, _event: &Ping) -> Result<(), Self::Error> {
            Err(Boom)
        }
    }

    struct Counting(Arc<AtomicU32>);
    impl EventHandler<Ping> for Counting {
        type Error = std::convert::Infallible;
        async fn handle(&self, _event: &Ping) -> Result<(), Self::Error> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    fn ping() -> Ping {
        Ping {
            occurred_at: Utc::now(),
        }
    }

    #[tokio::test]
    async fn fail_fast_stops_at_the_first_failing_handler() {
        let bus = EventBus::new();
        let seen = Arc::new(AtomicU32::new(0));
        bus.register::<Ping, _>(Failing);
        bus.register::<Ping, _>(Counting(Arc::clone(&seen)));

        let result = bus.publish(&ping()).await;

        let Err(EventBusError::HandlerError { event_type, source }) = result else {
            panic!("expected a single handler error, got {result:?}");
        };
        assert_eq!(event_type, "Ping");
        // The original handler error is preserved as the typed source.
        assert!(source.downcast_ref::<Boom>().is_some());
        // The handler registered after the failing one never saw the event.
        assert_eq!(seen.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn collect_all_delivers_to_every_handler_and_aggregates_failures() {
        let bus = EventBus::with_error_policy(PublishErrorPolicy::CollectAll);
        let seen = Arc::new(AtomicU32::new(0));
        bus.register::<Ping, _>(Failing);
        bus.register::<Ping, _>(Counting(Arc::clone(&seen)));
        bus.register::<Ping, _>(Failing);

        let result = bus.publish(&ping()).await;

        let Err(EventBusError::HandlerErrors { event_type, errors }) = result else {
            panic!("expected aggregated handler errors, got {result:?}");
        };
        assert_eq!(event_type, "Ping");
        assert_eq!(errors.len(), 2);
        // Every handler saw the event despite the failures around it.
        assert_eq!(seen.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn collect_all_with_a_single_failure_returns_it_directly() {
        let bus = EventBus::with_error_policy(PublishErrorPolicy::CollectAll);
        let seen = Arc::new(AtomicU32::new(0));
        bus.register::<Ping, _>(Failing);
        bus.register::<Ping, _>(Counting(Arc::clone(&seen)));

        let result = bus.publish(&ping()).await;

        assert!(matches!(result, Err(EventBusError::HandlerError { .. })));
        assert_eq!(seen.load(Ordering::SeqCst), 1);
    }

    // A serde-capable event to exercise the outbox/relay bridge.
    #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
    struct Echo {
        aggregate_id: String,
        occurred_at: DateTime<Utc>,
        note: String,
    }

    impl DomainEvent for Echo {
        fn event_type(&self) -> &'static str {
            "Echo"
        }
        fn occurred_at(&self) -> DateTime<Utc> {
            self.occurred_at
        }
        fn aggregate_id(&self) -> &str {
            &self.aggregate_id
        }
    }

    struct Recorder(Arc<std::sync::Mutex<Vec<String>>>);
    impl EventHandler<Echo> for Recorder {
        type Error = std::convert::Infallible;
        async fn handle(&self, event: &Echo) -> Result<(), Self::Error> {
            self.0
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .push(event.note.clone());
            Ok(())
        }
    }

    #[tokio::test]
    #[allow(clippy::unwrap_used, clippy::expect_used)]
    async fn publish_erased_decodes_and_dispatches_to_the_typed_handlers() {
        let bus = EventBus::new();
        let seen = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
        bus.register::<Echo, _>(Recorder(Arc::clone(&seen)));
        bus.register_decoder::<Echo>("Echo");

        let event = Echo {
            aggregate_id: "a-1".to_string(),
            occurred_at: Utc::now(),
            note: "hello".to_string(),
        };
        let payload = serde_json::to_vec(&event).expect("serialize");

        // The bytes are decoded back into `Echo` and reach the same handler as
        // a typed `publish` would.
        bus.publish_erased("Echo", &payload).await.expect("dispatch");
        assert_eq!(&*seen.lock().unwrap(), &["hello".to_string()]);

        // Unknown topic (no decoder) is a silent no-op, like `publish` with no
        // handler — publishing stays decoupled from consumption.
        bus.publish_erased("Unknown", &payload)
            .await
            .expect("unknown topic is a no-op");
        assert_eq!(seen.lock().unwrap().len(), 1);

        // A corrupt payload surfaces a DecodeError so the relay can retry or
        // dead-letter instead of silently dropping the message.
        let err = bus
            .publish_erased("Echo", b"not json")
            .await
            .expect_err("corrupt payload must fail");
        assert!(matches!(err, EventBusError::DecodeError { .. }));
    }
}
