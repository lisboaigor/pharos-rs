use std::any::{Any, TypeId, type_name};
use std::collections::HashMap;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::{Arc, RwLock};

use pharos_core::DomainEvent;
use thiserror::Error;
use tracing::{Instrument, debug, info_span};

use crate::event_handler::EventHandler;

/// Errors produced while publishing events through the [`EventBus`].
#[derive(Debug, Error)]
pub enum EventBusError {
    /// A registered handler returned an error.
    #[error("event handler failed for '{event_type}': {detail}")]
    HandlerError {
        /// Logical event type that was being dispatched.
        event_type: &'static str,
        /// Stringified handler error.
        detail: String,
    },
}

type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Registry of erased handlers keyed by the concrete event `TypeId`.
type HandlerRegistry = HashMap<TypeId, Vec<Arc<dyn ErasedHandler>>>;

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
                .ok_or(EventBusError::HandlerError {
                    event_type: "<unknown>",
                    detail: "event bus invariant violated: TypeId matched but downcast failed"
                        .to_string(),
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
                    detail: error.to_string(),
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
}

impl std::fmt::Debug for EventBus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let registered = self.handlers.read().map(|m| m.len()).unwrap_or(0);
        f.debug_struct("EventBus")
            .field("registered_event_types", &registered)
            .finish()
    }
}

impl EventBus {
    /// Creates an empty event bus.
    pub fn new() -> Self {
        Self::default()
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
            .expect("event bus handler registry poisoned")
            .entry(TypeId::of::<E>())
            .or_default()
            .push(wrapper);
    }

    /// Publishes a concrete domain event to all handlers registered for its type.
    ///
    /// Events without registered handlers are dropped silently, which keeps
    /// publishing decoupled from consumption.
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
                let map = self
                    .handlers
                    .read()
                    .expect("event bus handler registry poisoned");
                match map.get(&TypeId::of::<E>()) {
                    Some(handlers) => handlers.clone(),
                    None => {
                        debug!("no handler registered for event");
                        return Ok(());
                    }
                }
            };

            let any: &(dyn Any + Send + Sync) = event;
            for handler in &handlers {
                handler.call(any).await?;
            }
            Ok(())
        }
        .instrument(span)
        .await
    }
}
