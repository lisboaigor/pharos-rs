//! Backend-agnostic [`AggregateStore`] implementation.
//!
//! `PostgresAggregateStore` (from `pharos-postgres`, historically) duplicated
//! this exact type against `sqlx::PgPool`: the delivery-mode enum, the
//! enricher chain, and the `save`/`find` bodies were already 100%
//! backend-agnostic, and only the repository/unit-of-work underneath it were
//! Postgres-specific. [`DefaultAggregateStore`] is that same type, generic
//! over any [`Repository`] + [`TransactionalRepository`] pair against a
//! [`TransactionalStore`] — implement those two traits against your own
//! storage and you get delivery-mode switching, atomic outbox writes, and
//! message enrichment for free, exactly like the Postgres adapter used to
//! provide.
//!
//! Both `R` (the repository) and `S` (the transactional store) are required
//! unconditionally, mirroring how `PostgresAggregateStore` always carried a
//! `sqlx::PgPool`-backed unit of work even under in-process delivery: the
//! store just goes unused until [`Self::with_outbox`] switches delivery
//! modes. A repository that has no transactional counterpart can implement
//! [`TransactionalRepository`] trivially (e.g. an in-memory store whose
//! `Tx` is `()`) — see `pharos-memory`'s `InMemoryUnitOfWork`.
use std::fmt;
use std::sync::Arc;

use pharos_core::{AggregateRoot, DomainEvent, Repository, RepositoryError};
use serde::Serialize;

use crate::aggregate_store::{AggregateStore, StoreError};
use crate::enrichment::MessageEnricher;
use crate::event_bus::EventBus;
use crate::service::save_and_publish;
use crate::unit_of_work::{
    SaveAndEnqueueError, TransactionalRepository, TransactionalStore, save_and_enqueue_in,
};
use pharos_messaging::{Message, OutboxSignal};

/// How a [`DefaultAggregateStore`] delivers an aggregate's pending domain
/// events once the save succeeds.
enum Delivery {
    /// Publish in-process, in the same call that saves the aggregate — see
    /// [`save_and_publish`]. Every registered `EventBus` handler runs before
    /// `save` returns; a handler failure surfaces to the caller.
    Inline,
    /// Enqueue to the transactional outbox atomically with the aggregate
    /// save — see [`save_and_enqueue_in`]. A background dispatcher
    /// (`pharos_messaging::OutboxDispatcher`) delivers the events later, so a
    /// slow or failing handler never blocks the command that raised them.
    Outbox {
        topic: &'static str,
        signal: Option<OutboxSignal>,
    },
}

impl Clone for Delivery {
    fn clone(&self) -> Self {
        match self {
            Delivery::Inline => Delivery::Inline,
            Delivery::Outbox { topic, signal } => Delivery::Outbox {
                topic,
                signal: signal.clone(),
            },
        }
    }
}

/// Loads and saves one aggregate type through a [`Repository`] +
/// [`TransactionalRepository`]/[`TransactionalStore`] pair, switching between
/// in-process and durable-outbox event delivery without the handler above it
/// knowing which one is in play.
///
/// This is the store a `CommandHandler` depends on through [`AggregateStore`]
/// instead of composing a repository, a save helper, and (for the outbox
/// delivery mode) message-header plumbing by hand. Build one per aggregate
/// type at startup and share it (cloning is cheap as long as `R` and `S`
/// clone cheaply — typically an `Arc`/pool-handle clone):
///
/// ```ignore
/// // In-process delivery: handlers run synchronously inside `save`.
/// let orders = DefaultAggregateStore::new(repo.clone(), store.clone(), bus.clone());
///
/// // Durable outbox delivery, with a signal to wake the dispatcher right
/// // after commit, and headers stamped on every outgoing message.
/// let orders = DefaultAggregateStore::new(repo.clone(), store.clone(), bus.clone())
///     .with_outbox("OrderEvent", Some(signal))
///     .with_enricher(Arc::new(TenantHeader));
/// ```
pub struct DefaultAggregateStore<A, R, S>
where
    A: AggregateRoot,
    R: Repository<A> + TransactionalRepository<A, S>,
    S: TransactionalStore,
{
    repo: R,
    store: S,
    bus: EventBus,
    delivery: Delivery,
    enrichers: Vec<Arc<dyn MessageEnricher>>,
    _aggregate: std::marker::PhantomData<fn() -> A>,
}

impl<A, R, S> fmt::Debug for DefaultAggregateStore<A, R, S>
where
    A: AggregateRoot,
    R: Repository<A> + TransactionalRepository<A, S>,
    S: TransactionalStore,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DefaultAggregateStore")
            .field("aggregate_type", &std::any::type_name::<A>())
            .finish_non_exhaustive()
    }
}

impl<A, R, S> Clone for DefaultAggregateStore<A, R, S>
where
    A: AggregateRoot,
    R: Repository<A> + TransactionalRepository<A, S> + Clone,
    S: TransactionalStore + Clone,
{
    fn clone(&self) -> Self {
        Self {
            repo: self.repo.clone(),
            store: self.store.clone(),
            bus: self.bus.clone(),
            delivery: self.delivery.clone(),
            enrichers: self.enrichers.clone(),
            _aggregate: std::marker::PhantomData,
        }
    }
}

impl<A, R, S> DefaultAggregateStore<A, R, S>
where
    A: AggregateRoot,
    R: Repository<A> + TransactionalRepository<A, S>,
    S: TransactionalStore,
{
    /// Creates a store that delivers events in-process (see
    /// [`save_and_publish`]). Call [`Self::with_outbox`] to switch to durable
    /// outbox delivery.
    pub fn new(repo: R, store: S, bus: EventBus) -> Self {
        Self {
            repo,
            store,
            bus,
            delivery: Delivery::Inline,
            enrichers: Vec::new(),
            _aggregate: std::marker::PhantomData,
        }
    }

    /// Switches delivery to the durable outbox: the aggregate save and the
    /// outbox insert commit in one transaction against the store passed to
    /// [`Self::new`], and `topic` is what `EventBus::register_decoder` must
    /// be keyed on for a relay to route these messages back to their
    /// concrete event type.
    ///
    /// Pass `signal` when a same-process dispatcher is polling this outbox —
    /// it is notified right after each successful commit (see
    /// [`OutboxSignal`]).
    pub fn with_outbox(mut self, topic: &'static str, signal: Option<OutboxSignal>) -> Self {
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

    /// The underlying repository.
    pub fn repository(&self) -> &R {
        &self.repo
    }

    /// The underlying transactional store.
    pub fn store(&self) -> &S {
        &self.store
    }
}

impl<A, R, S> AggregateStore<A> for DefaultAggregateStore<A, R, S>
where
    A: AggregateRoot + Send + Sync + 'static,
    A::Event: Clone + Serialize,
    R: Repository<A> + TransactionalRepository<A, S>,
    S: TransactionalStore,
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

                save_and_enqueue_in(&self.store, &self.repo, aggregate, map_event)
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

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::convert::Infallible;
    use std::sync::Arc;

    use chrono::{DateTime, Utc};
    use pharos_core::{AggregateEvents, Entity};
    use tokio::sync::Mutex;

    use super::*;
    use crate::event_bus::EventBus;
    use crate::event_handler::EventHandler;
    use pharos_messaging::{OutboxMessage, OutboxStatus};

    #[derive(Debug, Clone, Serialize)]
    struct TestEvent {
        aggregate_id: String,
        occurred_at: DateTime<Utc>,
    }

    impl DomainEvent for TestEvent {
        fn event_type(&self) -> &'static str {
            "TestEvent"
        }

        fn occurred_at(&self) -> DateTime<Utc> {
            self.occurred_at
        }

        fn aggregate_id(&self) -> &str {
            &self.aggregate_id
        }
    }

    #[derive(Debug, Clone)]
    struct TestAggregate {
        id: u64,
        version: u64,
        events: AggregateEvents<TestEvent>,
    }

    impl TestAggregate {
        fn new(id: u64) -> Self {
            let mut events = AggregateEvents::default();
            events.raise(TestEvent {
                aggregate_id: id.to_string(),
                occurred_at: Utc::now(),
            });
            Self {
                id,
                version: 0,
                events,
            }
        }
    }

    impl Entity for TestAggregate {
        type Id = u64;

        fn id(&self) -> &Self::Id {
            &self.id
        }
    }

    impl AggregateRoot for TestAggregate {
        type Event = TestEvent;

        fn pending_events(&self) -> &[Self::Event] {
            self.events.pending()
        }

        fn drain_events(&mut self) -> Vec<Self::Event> {
            self.events.drain()
        }

        fn restore_events(&mut self, events: Vec<Self::Event>) {
            self.events.restore(events);
        }

        fn version(&self) -> u64 {
            self.version
        }

        fn set_version(&mut self, version: u64) {
            self.version = version;
        }
    }

    /// A minimal in-memory repository + transactional store pair, standing
    /// in for what a real adapter (e.g. a SeaORM one) provides: both traits
    /// share the same backing map, exactly like a real backend's repository
    /// and unit of work share one connection pool.
    #[derive(Default, Clone)]
    struct TestBackend {
        aggregates: Arc<Mutex<HashMap<u64, TestAggregate>>>,
        outbox: Arc<Mutex<Vec<OutboxMessage>>>,
    }

    impl Repository<TestAggregate> for TestBackend {
        type Error = Infallible;

        async fn find_by_id(&self, id: &u64) -> Result<Option<TestAggregate>, Self::Error> {
            Ok(self.aggregates.lock().await.get(id).cloned())
        }

        async fn save(
            &self,
            aggregate: &mut TestAggregate,
        ) -> Result<(), RepositoryError<Self::Error>> {
            aggregate.set_version(aggregate.version() + 1);
            self.aggregates
                .lock()
                .await
                .insert(aggregate.id, aggregate.clone());
            Ok(())
        }

        async fn delete(&self, id: &u64) -> Result<(), Self::Error> {
            self.aggregates.lock().await.remove(id);
            Ok(())
        }
    }

    /// Staged writes for one transaction: applied to `TestBackend`'s shared
    /// maps only on [`TransactionalStore::commit`], so a caller that never
    /// commits (e.g. `save_and_enqueue_in` on a mapping failure) leaves both
    /// maps untouched — the atomicity `DefaultAggregateStore`'s outbox
    /// delivery mode depends on.
    #[derive(Default)]
    struct TestTx {
        staged_aggregate: Option<(u64, TestAggregate)>,
        staged_outbox: Vec<OutboxMessage>,
    }

    impl TransactionalStore for TestBackend {
        type Tx = TestTx;
        type Error = Infallible;

        async fn begin(&self) -> Result<Self::Tx, Self::Error> {
            Ok(TestTx::default())
        }

        async fn commit(&self, tx: Self::Tx) -> Result<(), Self::Error> {
            if let Some((id, aggregate)) = tx.staged_aggregate {
                self.aggregates.lock().await.insert(id, aggregate);
            }
            self.outbox.lock().await.extend(tx.staged_outbox);
            Ok(())
        }

        async fn insert_outbox_in_tx<'a>(
            &'a self,
            tx: &'a mut Self::Tx,
            message: &'a OutboxMessage,
        ) -> Result<(), Self::Error> {
            tx.staged_outbox.push(message.clone());
            Ok(())
        }
    }

    impl TransactionalRepository<TestAggregate, TestBackend> for TestBackend {
        type Error = Infallible;

        async fn save_in_tx<'c>(
            &'c self,
            tx: &'c mut TestTx,
            aggregate: &'c mut TestAggregate,
        ) -> Result<(), RepositoryError<Self::Error>> {
            aggregate.set_version(aggregate.version() + 1);
            tx.staged_aggregate = Some((aggregate.id, aggregate.clone()));
            Ok(())
        }
    }

    struct CountingHandler {
        seen: Arc<Mutex<u32>>,
    }

    impl EventHandler<TestEvent> for CountingHandler {
        type Error = Infallible;

        async fn handle(&self, _event: &TestEvent) -> Result<(), Self::Error> {
            *self.seen.lock().await += 1;
            Ok(())
        }
    }

    #[tokio::test]
    async fn inline_delivery_persists_and_publishes_in_process()
    -> Result<(), Box<dyn std::error::Error>> {
        let backend = TestBackend::default();
        let bus = EventBus::new();
        let seen = Arc::new(Mutex::new(0));
        bus.register::<TestEvent, _>(CountingHandler {
            seen: Arc::clone(&seen),
        });

        let store = DefaultAggregateStore::new(backend.clone(), backend, bus);
        let mut aggregate = TestAggregate::new(1);

        store.save(&mut aggregate).await?;

        assert_eq!(aggregate.version(), 1);
        assert!(aggregate.pending_events().is_empty());
        assert_eq!(*seen.lock().await, 1);

        let loaded = store.find(&1).await?.ok_or("aggregate not found")?;
        assert_eq!(loaded.version(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn outbox_delivery_enqueues_atomically_and_signals()
    -> Result<(), Box<dyn std::error::Error>> {
        let backend = TestBackend::default();
        let bus = EventBus::new();
        let signal = pharos_messaging::OutboxSignal::new();

        let store = DefaultAggregateStore::new(backend.clone(), backend.clone(), bus)
            .with_outbox("TestEvent", Some(signal.clone()));
        let mut aggregate = TestAggregate::new(2);

        store.save(&mut aggregate).await?;

        assert_eq!(aggregate.version(), 1);
        assert!(aggregate.pending_events().is_empty());

        // Aggregate row and outbox row commit together: both are visible
        // once `save` returns.
        let loaded = store.find(&2).await?.ok_or("aggregate not found")?;
        assert_eq!(loaded.version(), 1);

        let pending = backend.outbox.lock().await;
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].message.topic, "TestEvent");
        assert_eq!(pending[0].status, OutboxStatus::Pending);
        drop(pending);

        // `signal.notify()` left a permit outstanding; `notified()` resolves
        // immediately instead of hanging, proving `save` signaled the
        // dispatcher after commit.
        tokio::time::timeout(std::time::Duration::from_secs(1), signal.notified())
            .await
            .map_err(|_| "outbox signal was not notified after save")?;

        Ok(())
    }

    #[tokio::test]
    async fn outbox_delivery_applies_enrichers_to_outgoing_messages()
    -> Result<(), Box<dyn std::error::Error>> {
        struct StaticHeader;
        impl MessageEnricher for StaticHeader {
            fn enrich(&self, message: &mut Message) {
                message.headers.insert("x-tenant".into(), "acme".into());
            }
        }

        let backend = TestBackend::default();
        let bus = EventBus::new();
        let store = DefaultAggregateStore::new(backend.clone(), backend.clone(), bus)
            .with_outbox("TestEvent", None)
            .with_enricher(Arc::new(StaticHeader));
        let mut aggregate = TestAggregate::new(3);

        store.save(&mut aggregate).await?;

        let pending = backend.outbox.lock().await;
        assert_eq!(
            pending[0]
                .message
                .headers
                .get("x-tenant")
                .map(String::as_str),
            Some("acme")
        );
        Ok(())
    }

    #[tokio::test]
    async fn concurrency_conflict_surfaces_and_leaves_aggregate_retryable()
    -> Result<(), Box<dyn std::error::Error>> {
        // A repository whose `save_in_tx` always reports a stale write,
        // proving `DefaultAggregateStore::save` maps
        // `RepositoryError::ConcurrencyConflict` to `StoreError::
        // ConcurrencyConflict` instead of the catch-all `Storage` variant.
        #[derive(Clone, Default)]
        struct ConflictingBackend(TestBackend);

        impl Repository<TestAggregate> for ConflictingBackend {
            type Error = Infallible;

            async fn find_by_id(&self, id: &u64) -> Result<Option<TestAggregate>, Self::Error> {
                self.0.find_by_id(id).await
            }

            async fn save(
                &self,
                aggregate: &mut TestAggregate,
            ) -> Result<(), RepositoryError<Self::Error>> {
                self.0.save(aggregate).await
            }

            async fn delete(&self, id: &u64) -> Result<(), Self::Error> {
                self.0.delete(id).await
            }
        }

        impl TransactionalStore for ConflictingBackend {
            type Tx = TestTx;
            type Error = Infallible;

            async fn begin(&self) -> Result<Self::Tx, Self::Error> {
                self.0.begin().await
            }

            async fn commit(&self, tx: Self::Tx) -> Result<(), Self::Error> {
                self.0.commit(tx).await
            }

            async fn insert_outbox_in_tx<'a>(
                &'a self,
                tx: &'a mut Self::Tx,
                message: &'a OutboxMessage,
            ) -> Result<(), Self::Error> {
                self.0.insert_outbox_in_tx(tx, message).await
            }
        }

        impl TransactionalRepository<TestAggregate, ConflictingBackend> for ConflictingBackend {
            type Error = Infallible;

            async fn save_in_tx<'c>(
                &'c self,
                _tx: &'c mut TestTx,
                aggregate: &'c mut TestAggregate,
            ) -> Result<(), RepositoryError<Self::Error>> {
                Err(RepositoryError::ConcurrencyConflict {
                    expected: aggregate.version(),
                    actual: Some(aggregate.version() + 1),
                })
            }
        }

        let backend = ConflictingBackend::default();
        let bus = EventBus::new();
        let store = DefaultAggregateStore::new(backend.clone(), backend, bus)
            .with_outbox("TestEvent", None);
        let mut aggregate = TestAggregate::new(4);

        let result = store.save(&mut aggregate).await;

        assert!(matches!(
            result,
            Err(StoreError::ConcurrencyConflict {
                expected: 0,
                actual: Some(1)
            })
        ));
        // Version was reverted and the pending event kept, so a retry after
        // reloading starts from a clean, unpublished state.
        assert_eq!(aggregate.version(), 0);
        assert_eq!(aggregate.pending_events().len(), 1);
        Ok(())
    }
}
