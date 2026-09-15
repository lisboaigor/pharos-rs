use std::fmt::Display;
use std::str::FromStr;
use std::sync::Arc;

use pharos_app::{
    AggregateStore, EventBus, Message, MessageEnricher, SaveAndEnqueueError, StoreError,
    save_and_enqueue_in, save_and_publish,
};
use pharos_core::{AggregateRoot, DomainEvent, Entity, Repository, RepositoryError};
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::pool::Pool;
use crate::tenant_repository::CurrentTenantJsonRepository;
use crate::transaction::PostgresUnitOfWork;

/// How a [`PostgresAggregateStore`] delivers an aggregate's pending domain
/// events once the snapshot save succeeds.
enum Delivery {
    /// Publish in-process, in the same call that saves the aggregate — see
    /// [`pharos_app::save_and_publish`]. Every registered `EventBus` handler
    /// runs before `save` returns; a handler failure surfaces to the caller.
    Inline,
    /// Enqueue to the transactional outbox atomically with the snapshot save
    /// — see [`pharos_app::save_and_enqueue_in`]. A background dispatcher
    /// (`pharos_messaging::OutboxDispatcher`) delivers the events later, so a
    /// slow or failing handler never blocks the command that raised them.
    Outbox {
        topic: &'static str,
        signal: Option<pharos_app::OutboxSignal>,
    },
}

/// Loads and saves one aggregate type against `pharos_tenant_aggregates`,
/// resolving the tenant from [`pharos_app::CURRENT_TENANT`] on every call.
///
/// This is the store a `CommandHandler` depends on through
/// [`AggregateStore`] instead of composing a repository, a save helper, and
/// (for the outbox delivery mode) message-header plumbing by hand. Build one
/// per aggregate type at startup and share it (cloning is cheap — every
/// field is an `Arc`/`Pool` clone or a `Copy` enum):
///
/// ```ignore
/// // In-process delivery: handlers run synchronously inside `save`.
/// let orders = PostgresAggregateStore::<Order>::new(pool.clone(), "Order", bus.clone());
///
/// // Durable outbox delivery, with a signal to wake the dispatcher right
/// // after commit, and headers stamped on every outgoing message.
/// let orders = PostgresAggregateStore::<Order>::new(pool.clone(), "Order", bus.clone())
///     .with_outbox("OrderEvent", Some(signal))
///     .with_enricher(Arc::new(TenantHeader));
/// ```
pub struct PostgresAggregateStore<A: AggregateRoot> {
    repo: CurrentTenantJsonRepository<A>,
    uow: PostgresUnitOfWork,
    bus: EventBus,
    delivery: Delivery,
    enrichers: Vec<Arc<dyn MessageEnricher>>,
}

impl<A: AggregateRoot> Clone for PostgresAggregateStore<A> {
    fn clone(&self) -> Self {
        Self {
            repo: self.repo.clone(),
            uow: self.uow.clone(),
            bus: self.bus.clone(),
            delivery: match &self.delivery {
                Delivery::Inline => Delivery::Inline,
                Delivery::Outbox { topic, signal } => Delivery::Outbox {
                    topic,
                    signal: signal.clone(),
                },
            },
            enrichers: self.enrichers.clone(),
        }
    }
}

impl<A: AggregateRoot> PostgresAggregateStore<A> {
    /// Creates a store that delivers events in-process (see [`Delivery::Inline`]).
    /// Call [`Self::with_outbox`] to switch to durable outbox delivery.
    pub fn new(pool: Pool, aggregate_type: impl Into<Arc<str>>, bus: EventBus) -> Self {
        Self {
            repo: CurrentTenantJsonRepository::new(pool.clone(), aggregate_type),
            uow: PostgresUnitOfWork::new(pool),
            bus,
            delivery: Delivery::Inline,
            enrichers: Vec::new(),
        }
    }

    /// Switches delivery to the durable outbox: the snapshot save and the
    /// outbox inserts commit in one transaction, and `topic` is what
    /// `EventBus::register_decoder` must be keyed on for a relay to route
    /// these messages back to their concrete event type.
    ///
    /// Pass `signal` when a same-process dispatcher is polling this outbox —
    /// it is notified right after each successful commit (see
    /// [`pharos_app::OutboxSignal`]).
    pub fn with_outbox(
        mut self,
        topic: &'static str,
        signal: Option<pharos_app::OutboxSignal>,
    ) -> Self {
        self.delivery = Delivery::Outbox { topic, signal };
        self
    }

    /// Registers a [`MessageEnricher`] applied to every outgoing message
    /// under outbox delivery, in registration order. Has no effect under
    /// inline delivery, which never builds a [`Message`].
    pub fn with_enricher(mut self, enricher: Arc<dyn MessageEnricher>) -> Self {
        self.enrichers.push(enricher);
        self
    }

    /// The underlying connection pool.
    ///
    /// Exposed so a repository that wraps this store for its own read-model
    /// queries (the common shape: one `Postgres*Repository` per aggregate,
    /// implementing both [`AggregateStore`] via this type and a context's own
    /// query trait) can reuse the same pool handle instead of holding a
    /// second `Pool` field and cloning it separately at construction time —
    /// `Pool` is reference-counted, so `store.pool().clone()` is exactly the
    /// same handle either way, just without the redundant field.
    pub fn pool(&self) -> &Pool {
        self.uow.pool()
    }
}

impl<A> AggregateStore<A> for PostgresAggregateStore<A>
where
    A: AggregateRoot + Serialize + DeserializeOwned + Send + Sync + 'static,
    A::Event: Serialize + Clone,
    <A as Entity>::Id: Display + FromStr + Send + Sync + 'static,
    <<A as Entity>::Id as FromStr>::Err: Display + Send + Sync + 'static,
{
    async fn find(&self, id: &A::Id) -> Result<Option<A>, StoreError> {
        self.repo.find_by_id(id).await.map_err(StoreError::storage)
    }

    async fn save(&self, aggregate: &mut A) -> Result<(), StoreError> {
        match &self.delivery {
            Delivery::Inline => save_and_publish(&self.repo, &self.bus, aggregate)
                .await
                .map_err(StoreError::from),
            Delivery::Outbox { topic, signal } => {
                let enrichers = &self.enrichers;
                let map_event = |event: &A::Event| -> Result<Message, serde_json::Error> {
                    let mut message =
                        Message::new(*topic, serde_json::to_vec(event)?, "application/json")
                            .with_key(event.aggregate_id())
                            .with_header("event_type", event.event_type());
                    for enricher in enrichers {
                        enricher.enrich(&mut message);
                    }
                    Ok(message)
                };

                save_and_enqueue_in(&self.uow, &self.repo, aggregate, map_event)
                    .await
                    .map_err(|error| match error {
                        SaveAndEnqueueError::Repository(RepositoryError::ConcurrencyConflict {
                            expected,
                            actual,
                        }) => StoreError::ConcurrencyConflict { expected, actual },
                        other => StoreError::storage(other),
                    })?;

                if let Some(signal) = signal {
                    signal.notify();
                }
                Ok(())
            }
        }
    }
}
