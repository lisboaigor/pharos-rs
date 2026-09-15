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

use crate::cascade::CascadeError;
use crate::event_handler::{CascadingEventHandler, EventHandler};

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
    /// A command returned by a [`CascadingEventHandler`] failed. Commands
    /// already dispatched earlier in the same cascade are not undone.
    #[error("cascaded command failed for '{event_type}': {source}")]
    CascadeFailed {
        /// Logical event type whose cascade produced the failing command.
        event_type: &'static str,
        /// The cascaded command's own failure, naming the command.
        #[source]
        source: CascadeError,
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
    /// Stable name for this handler, used for tracing and as the basis of a
    /// per-handler inbox consumer id a caller derives (see
    /// [`EventBus::handler_names_for_topic`]).
    fn name(&self) -> &'static str;

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
    fn name(&self) -> &'static str {
        type_name::<H>()
    }

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

struct CascadingHandlerWrapper<E, H> {
    inner: Arc<H>,
    _marker: PhantomData<fn(E)>,
}

impl<E, H> ErasedHandler for CascadingHandlerWrapper<E, H>
where
    E: DomainEvent,
    H: CascadingEventHandler<E>,
{
    fn name(&self) -> &'static str {
        type_name::<H>()
    }

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

            let cascaded = handler
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
                })?;

            for command in cascaded {
                command
                    .dispatch()
                    .await
                    .map_err(|source| EventBusError::CascadeFailed {
                        event_type: typed.event_type(),
                        source,
                    })?;
            }

            Ok(())
        })
    }
}

/// In-process event bus that dispatches domain events to typed handlers.
///
/// `EventBus` is a concrete, cheaply cloneable type. All clones share the same
/// registered handlers through an internal `Arc`. The public API is fully
/// typed — callers publish and handle concrete event types, never `dyn Any` —
/// but internally, dispatch erases each handler to `Arc<dyn ErasedHandler>`
/// keyed by `TypeId`, downcasts the event through `&dyn Any` to find the
/// concrete type again, clones the matched handler `Vec` on every publish,
/// and boxes each handler invocation as a pinned future. None of that is
/// visible to domain code, but it is real `Any` use and real per-publish
/// allocation, not zero-cost dispatch.
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

    /// Registers a [`CascadingEventHandler`] for a concrete domain event type.
    ///
    /// Behaves exactly like [`register`](Self::register) for ordering and
    /// error-policy purposes — it shares the same per-event-type handler
    /// list — except this handler returns the commands it wants run instead
    /// of running them itself; [`publish`](Self::publish) dispatches them
    /// right after the handler returns. See [`CascadingEventHandler`] for the
    /// cascade's failure semantics.
    pub fn register_cascading<E, H>(&self, handler: H)
    where
        E: DomainEvent,
        H: CascadingEventHandler<E>,
    {
        let wrapper = Arc::new(CascadingHandlerWrapper::<E, H> {
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
    /// its concrete type `E`, so
    /// [`publish_trusted_bytes`](Self::publish_trusted_bytes) can deliver
    /// outbox/relay payloads to the same typed handlers as
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
    /// # This deserializes `payload` straight into a domain event — call it
    /// # only with bytes you already trust
    ///
    /// There is no aggregate here, no `decide`/`handle` step, no signature or
    /// provenance check: a decoder registered via
    /// [`register_decoder`](Self::register_decoder) turns any bytes that
    /// happen to deserialize into `E` into a real event delivered to every
    /// handler registered for it, and `E::aggregate_id()`/`occurred_at()`
    /// come straight from those bytes — a forged payload can claim any
    /// aggregate and any timestamp.
    ///
    /// The intended caller is your own outbox relay reading rows it wrote
    /// itself. **Never** feed this a payload whose `topic` or bytes came
    /// from a network-facing broker subject or connection without your own
    /// authentication and provenance check first — a wildcard subscription
    /// or attacker-chosen subject can otherwise select which decoder runs.
    ///
    /// `topic` must have been registered with
    /// [`register_decoder`](Self::register_decoder). Behaviour mirrors
    /// `publish` exactly once the concrete event is reconstructed: same handler
    /// order, same [`PublishErrorPolicy`], same at-least-once semantics.
    ///
    /// - Unknown `topic` (no decoder) returns `Ok` without dispatching, so
    ///   publishing stays decoupled from consumption — matching how `publish`
    ///   drops events with no registered handler. It also increments the
    ///   unlabeled `pharos.event_bus.unknown_topic` counter and logs `topic`
    ///   at debug level, so a misrouted or unexpected topic is visible
    ///   without being silently invisible — an unknown topic is not
    ///   necessarily hostile, but it is always worth being able to see.
    ///   The metric carries no `topic` label deliberately: this is precisely
    ///   the path a network-facing subject or attacker-chosen string can
    ///   reach (see the warning above), and a label built from it would
    ///   hand the metrics backend one time series per string an attacker
    ///   cares to invent. The topic itself is still visible in the debug
    ///   log, which is not similarly cardinality-limited.
    /// - A payload that fails to decode returns
    ///   [`EventBusError::DecodeError`] so the relay can retry or dead-letter.
    pub async fn publish_trusted_bytes(
        &self,
        topic: &str,
        payload: &[u8],
    ) -> Result<(), EventBusError> {
        let span = info_span!("event_bus.publish_trusted_bytes", event.topic = topic);

        async move {
            let Some((type_id, decoder)) = ({
                let decoders = self.decoders.read().unwrap_or_else(|p| p.into_inner());
                decoders.get(topic).cloned()
            }) else {
                debug!(topic, "no decoder registered for topic");
                // No `topic` label: this counter is reachable from a
                // network-facing broker subject an attacker chooses (see the
                // safety warning above), and a label built from that string
                // would give the metrics backend one time series per value
                // an attacker cares to invent. The topic itself is still
                // visible in the debug log above, which carries no such
                // cardinality cost.
                metrics::counter!("pharos.event_bus.unknown_topic").increment(1);
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

    /// Names of the handlers registered for the event type `topic` decodes
    /// into, in registration order — for a caller (the outbox relay) that
    /// wants to dispatch to each handler individually, tracking its own
    /// idempotency/retry outcome per handler instead of one outcome for the
    /// whole event. See [`dispatch_trusted_bytes_to_handler`](Self::dispatch_trusted_bytes_to_handler).
    ///
    /// Empty when `topic` has no registered decoder or no handler is
    /// registered for the type it decodes into.
    pub fn handler_names_for_topic(&self, topic: &str) -> Vec<&'static str> {
        let Some(type_id) = ({
            let decoders = self.decoders.read().unwrap_or_else(|p| p.into_inner());
            decoders.get(topic).map(|(type_id, _)| *type_id)
        }) else {
            return Vec::new();
        };

        let map = self.handlers.read().unwrap_or_else(|p| p.into_inner());
        match map.get(&type_id) {
            Some(handlers) => handlers.iter().map(|h| h.name()).collect(),
            None => Vec::new(),
        }
    }

    /// Decodes a trusted payload and dispatches it to a single named
    /// handler, leaving every other handler registered for the same event
    /// type untouched — the per-handler counterpart of
    /// [`publish_trusted_bytes`](Self::publish_trusted_bytes), which
    /// dispatches to all of them at once.
    ///
    /// Get `handler_name` from [`handler_names_for_topic`](Self::handler_names_for_topic).
    /// An unknown topic or a name that matches no registered handler is a
    /// no-op, logged at debug level — the same posture `publish_trusted_bytes`
    /// takes for an unknown topic. This re-decodes `payload` on every call;
    /// dispatching to several handlers for the same bytes pays that cost
    /// once per handler, same as `publish_trusted_bytes` already pays it
    /// once per delivery.
    ///
    /// Same trust requirement as `publish_trusted_bytes`: only ever call
    /// this with bytes you already trust.
    pub async fn dispatch_trusted_bytes_to_handler(
        &self,
        topic: &str,
        payload: &[u8],
        handler_name: &str,
    ) -> Result<(), EventBusError> {
        let span = info_span!(
            "event_bus.dispatch_trusted_bytes_to_handler",
            event.topic = topic,
            handler = handler_name,
        );

        async move {
            let Some((type_id, decoder)) = ({
                let decoders = self.decoders.read().unwrap_or_else(|p| p.into_inner());
                decoders.get(topic).cloned()
            }) else {
                debug!(topic, "no decoder registered for topic");
                return Ok(());
            };

            let boxed = decoder(payload).map_err(|source| EventBusError::DecodeError {
                topic: topic.to_owned(),
                source,
            })?;

            let handler = {
                let map = self.handlers.read().unwrap_or_else(|p| p.into_inner());
                match map.get(&type_id) {
                    Some(handlers) => handlers.iter().find(|h| h.name() == handler_name).cloned(),
                    None => None,
                }
            };

            let Some(handler) = handler else {
                debug!(
                    handler = handler_name,
                    "no handler registered with this name"
                );
                return Ok(());
            };

            let any: &(dyn Any + Send + Sync) = &*boxed;
            handler.call(any).await
        }
        .instrument(span)
        .await
    }

    /// Deprecated alias for [`publish_trusted_bytes`](Self::publish_trusted_bytes).
    ///
    /// Renamed to make the trust requirement impossible to miss at the call
    /// site: this method deserializes `payload` straight into a domain event
    /// with no aggregate, no signature, and no provenance check, so it must
    /// only ever be called with bytes you already trust.
    #[deprecated(note = "renamed to publish_trusted_bytes to make its trust requirement explicit")]
    pub async fn publish_erased(&self, topic: &str, payload: &[u8]) -> Result<(), EventBusError> {
        self.publish_trusted_bytes(topic, payload).await
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
    async fn publish_trusted_bytes_decodes_and_dispatches_to_the_typed_handlers() {
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
        bus.publish_trusted_bytes("Echo", &payload)
            .await
            .expect("dispatch");
        assert_eq!(&*seen.lock().unwrap(), &["hello".to_string()]);

        // Unknown topic (no decoder) is a no-op, like `publish` with no
        // handler — publishing stays decoupled from consumption. (It also
        // increments `pharos.event_bus.unknown_topic`, not asserted here
        // since this crate has no metrics-recorder test harness.)
        bus.publish_trusted_bytes("Unknown", &payload)
            .await
            .expect("unknown topic is a no-op");
        assert_eq!(seen.lock().unwrap().len(), 1);

        // A corrupt payload surfaces a DecodeError so the relay can retry or
        // dead-letter instead of silently dropping the message.
        let err = bus
            .publish_trusted_bytes("Echo", b"not json")
            .await
            .expect_err("corrupt payload must fail");
        assert!(matches!(err, EventBusError::DecodeError { .. }));
    }

    /// The deprecated alias must keep working identically, unrenamed callers
    /// included — deprecation is a warning, not a break.
    #[tokio::test]
    #[allow(clippy::unwrap_used, clippy::expect_used, deprecated)]
    async fn the_deprecated_alias_still_dispatches() {
        let bus = EventBus::new();
        let seen = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
        bus.register::<Echo, _>(Recorder(Arc::clone(&seen)));
        bus.register_decoder::<Echo>("Echo");

        let event = Echo {
            aggregate_id: "a-1".to_string(),
            occurred_at: Utc::now(),
            note: "still works".to_string(),
        };
        let payload = serde_json::to_vec(&event).expect("serialize");

        bus.publish_erased("Echo", &payload)
            .await
            .expect("dispatch");
        assert_eq!(&*seen.lock().unwrap(), &["still works".to_string()]);
    }

    // A second, distinct handler type for `Echo`, so `handler_names_for_topic`
    // has two different names to tell apart (two `Recorder` instances would
    // share the same `type_name`).
    struct SecondRecorder(Arc<std::sync::Mutex<Vec<String>>>);
    impl EventHandler<Echo> for SecondRecorder {
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
    async fn handler_names_for_topic_lists_registered_handlers_in_order() {
        let bus = EventBus::new();
        bus.register::<Echo, _>(Recorder(Arc::default()));
        bus.register::<Echo, _>(SecondRecorder(Arc::default()));
        bus.register_decoder::<Echo>("Echo");

        let names = bus.handler_names_for_topic("Echo");
        assert_eq!(names.len(), 2);
        assert!(names[0].ends_with("Recorder"));
        assert!(names[1].ends_with("SecondRecorder"));

        // No decoder for this topic: empty, not a panic.
        assert!(bus.handler_names_for_topic("Unknown").is_empty());
    }

    #[tokio::test]
    #[allow(clippy::unwrap_used, clippy::expect_used)]
    async fn dispatch_trusted_bytes_to_handler_only_runs_the_named_handler() {
        let bus = EventBus::new();
        let seen1 = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
        let seen2 = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
        bus.register::<Echo, _>(Recorder(Arc::clone(&seen1)));
        bus.register::<Echo, _>(SecondRecorder(Arc::clone(&seen2)));
        bus.register_decoder::<Echo>("Echo");

        let names = bus.handler_names_for_topic("Echo");
        let event = Echo {
            aggregate_id: "a-1".to_string(),
            occurred_at: Utc::now(),
            note: "only the first".to_string(),
        };
        let payload = serde_json::to_vec(&event).expect("serialize");

        // Dispatching to the first handler by name leaves the second
        // untouched — the per-handler counterpart of `publish_trusted_bytes`,
        // which would have run both.
        bus.dispatch_trusted_bytes_to_handler("Echo", &payload, names[0])
            .await
            .expect("dispatch to the first handler");
        assert_eq!(&*seen1.lock().unwrap(), &["only the first".to_string()]);
        assert!(seen2.lock().unwrap().is_empty());

        // Dispatching to the second by name now runs only that one.
        bus.dispatch_trusted_bytes_to_handler("Echo", &payload, names[1])
            .await
            .expect("dispatch to the second handler");
        assert_eq!(&*seen1.lock().unwrap(), &["only the first".to_string()]);
        assert_eq!(&*seen2.lock().unwrap(), &["only the first".to_string()]);
    }

    #[tokio::test]
    #[allow(clippy::unwrap_used, clippy::expect_used)]
    async fn dispatch_trusted_bytes_to_handler_is_a_noop_for_unknown_topic_or_name() {
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

        bus.dispatch_trusted_bytes_to_handler("Unknown", &payload, "whatever")
            .await
            .expect("unknown topic is a no-op");
        bus.dispatch_trusted_bytes_to_handler("Echo", &payload, "NotRegistered")
            .await
            .expect("unknown handler name is a no-op");
        assert!(seen.lock().unwrap().is_empty());
    }

    use crate::cascade::{CascadedCommand, cascade};
    use crate::command::{Command, CommandHandler};

    struct RecordNote(String);

    impl Command for RecordNote {
        const NAME: &'static str = "RecordNote";
    }

    struct Recording(Arc<std::sync::Mutex<Vec<String>>>);

    impl CommandHandler<RecordNote> for Recording {
        type Output = ();
        type Error = std::convert::Infallible;

        async fn handle(&self, cmd: RecordNote) -> Result<(), Self::Error> {
            self.0.lock().unwrap_or_else(|p| p.into_inner()).push(cmd.0);
            Ok(())
        }
    }

    struct AlwaysFailsCommand;

    impl Command for AlwaysFailsCommand {
        const NAME: &'static str = "AlwaysFailsCommand";
    }

    struct AlwaysFailingCommandHandler;

    impl CommandHandler<AlwaysFailsCommand> for AlwaysFailingCommandHandler {
        type Output = ();
        type Error = Boom;

        async fn handle(&self, _cmd: AlwaysFailsCommand) -> Result<(), Self::Error> {
            Err(Boom)
        }
    }

    struct CascadingRecorder(Arc<Recording>);

    impl CascadingEventHandler<Ping> for CascadingRecorder {
        type Error = std::convert::Infallible;

        async fn handle(
            &self,
            _event: &Ping,
        ) -> Result<Vec<Box<dyn CascadedCommand>>, Self::Error> {
            Ok(vec![
                cascade(RecordNote("first".to_string()), Arc::clone(&self.0)),
                cascade(RecordNote("second".to_string()), Arc::clone(&self.0)),
            ])
        }
    }

    #[tokio::test]
    #[allow(clippy::unwrap_used)]
    async fn cascading_handler_dispatches_returned_commands_in_order() {
        let bus = EventBus::new();
        let notes = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
        let recorder = Arc::new(Recording(Arc::clone(&notes)));
        bus.register_cascading::<Ping, _>(CascadingRecorder(recorder));

        bus.publish(&ping()).await.unwrap();

        assert_eq!(
            &*notes.lock().unwrap_or_else(|p| p.into_inner()),
            &["first".to_string(), "second".to_string()]
        );
    }

    struct CascadingWithAFailure(Arc<Recording>);

    impl CascadingEventHandler<Ping> for CascadingWithAFailure {
        type Error = std::convert::Infallible;

        async fn handle(
            &self,
            _event: &Ping,
        ) -> Result<Vec<Box<dyn CascadedCommand>>, Self::Error> {
            Ok(vec![
                cascade(RecordNote("before".to_string()), Arc::clone(&self.0)),
                cascade(AlwaysFailsCommand, Arc::new(AlwaysFailingCommandHandler)),
                cascade(RecordNote("after".to_string()), Arc::clone(&self.0)),
            ])
        }
    }

    #[tokio::test]
    async fn a_failing_cascaded_command_stops_the_rest_of_its_own_cascade() {
        let bus = EventBus::new();
        let notes = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
        let recorder = Arc::new(Recording(Arc::clone(&notes)));
        bus.register_cascading::<Ping, _>(CascadingWithAFailure(recorder));

        let result = bus.publish(&ping()).await;

        assert!(matches!(result, Err(EventBusError::CascadeFailed { .. })));
        // "before" was dispatched ahead of the failing command and stays
        // committed; "after" never ran because the cascade stops at the
        // first failure — a cascade is a best-effort chain, not a saga.
        assert_eq!(
            &*notes.lock().unwrap_or_else(|p| p.into_inner()),
            &["before".to_string()]
        );
    }

    struct AlwaysFailingCascadingHandler;

    impl CascadingEventHandler<Ping> for AlwaysFailingCascadingHandler {
        type Error = Boom;

        async fn handle(
            &self,
            _event: &Ping,
        ) -> Result<Vec<Box<dyn CascadedCommand>>, Self::Error> {
            Err(Boom)
        }
    }

    #[tokio::test]
    async fn a_failing_cascading_handler_never_builds_any_cascaded_command() {
        let bus = EventBus::new();
        bus.register_cascading::<Ping, _>(AlwaysFailingCascadingHandler);

        let result = bus.publish(&ping()).await;

        assert!(matches!(result, Err(EventBusError::HandlerError { .. })));
    }
}
