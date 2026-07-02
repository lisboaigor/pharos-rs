use std::error::Error;

use pharos_core::{AggregateRoot, DomainEvent, Repository, RepositoryError};
use tracing::{Instrument, info_span};

use crate::error::ApplicationError;
use crate::event_bus::EventBus;
use crate::messaging::Message;
use crate::outbox::{OutboxMessage, OutboxRepository};

/// Maps a repository error into the application-level error type.
fn map_repository_error<E: Error>(error: RepositoryError<E>) -> ApplicationError {
    match error {
        RepositoryError::ConcurrencyConflict { expected, actual } => {
            ApplicationError::ConcurrencyConflict { expected, actual }
        }
        RepositoryError::Storage(error) => ApplicationError::Repository(error.to_string()),
    }
}

/// Persists an aggregate and publishes all of its pending domain events.
///
/// The aggregate is saved first (advancing its optimistic-concurrency
/// version); its pending events are published afterwards and only drained once
/// every handler ran. A failed save — e.g. a `ConcurrencyConflict`, the normal
/// retry case — therefore never discards events: the aggregate keeps them and a
/// retry starts from intact state. If a handler fails midway the remaining
/// events also stay pending, so a retry republishes the batch; event handlers
/// must be idempotent under this at-least-once semantic.
pub async fn save_and_publish<A, R>(
    repo: &R,
    bus: &EventBus,
    aggregate: &mut A,
) -> Result<(), ApplicationError>
where
    A: AggregateRoot,
    R: Repository<A>,
{
    let aggregate_type = std::any::type_name::<A>();

    async move {
        repo.save(aggregate).await.map_err(map_repository_error)?;

        tracing::info!(
            aggregate = aggregate_type,
            version = aggregate.version(),
            event_count = aggregate.pending_events().len(),
            "aggregate persisted; publishing pending events"
        );

        for event in aggregate.pending_events() {
            bus.publish(event).await?;
            metrics::counter!("pharos.events.published", "event_type" => event.event_type())
                .increment(1);
        }
        aggregate.drain_events();
        Ok(())
    }
    .instrument(info_span!("save_and_publish", aggregate = aggregate_type))
    .await
}

/// Persists an aggregate and enqueues its pending domain events as outbox messages.
///
/// This function keeps the existing domain model intact while providing an
/// explicit outbox seam for distributed event-driven systems. A production
/// adapter can make the repository save and outbox insert participate in the
/// same database transaction.
pub async fn save_and_enqueue<A, R, O, F>(
    repo: &R,
    outbox: &O,
    aggregate: &mut A,
    map_event: F,
) -> Result<(), ApplicationError>
where
    A: AggregateRoot,
    R: Repository<A>,
    O: OutboxRepository,
    F: Fn(&A::Event) -> Message + Send + Sync,
{
    let aggregate_type = std::any::type_name::<A>();

    async move {
        repo.save(aggregate).await.map_err(map_repository_error)?;

        tracing::info!(
            aggregate = aggregate_type,
            version = aggregate.version(),
            event_count = aggregate.pending_events().len(),
            "aggregate persisted; enqueueing pending events to outbox"
        );

        for event in aggregate.pending_events() {
            let outbox_span = info_span!(
                "enqueue_pending_event",
                event_type = event.event_type(),
                event.aggregate_id = event.aggregate_id(),
            );
            async {
                let message = map_event(event);
                outbox.insert(OutboxMessage::new(message)).await?;
                metrics::counter!("pharos.outbox.enqueued", "event_type" => event.event_type())
                    .increment(1);
                Ok::<(), ApplicationError>(())
            }
            .instrument(outbox_span)
            .await?;
        }
        aggregate.drain_events();

        Ok(())
    }
    .instrument(info_span!("save_and_enqueue", aggregate = aggregate_type))
    .await
}

#[cfg(test)]
mod tests {
    use std::convert::Infallible;
    use std::sync::Arc;

    use chrono::{DateTime, Utc};
    use pharos_core::{AggregateEvents, Entity};
    use tokio::sync::Mutex;
    use uuid::Uuid;

    use super::*;
    use crate::event_handler::EventHandler;
    use crate::outbox::{OutboxError, OutboxStatus};

    #[derive(Debug, Clone)]
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

        fn version(&self) -> u64 {
            self.version
        }

        fn set_version(&mut self, version: u64) {
            self.version = version;
        }
    }

    #[derive(Default)]
    struct TestRepository {
        saved: Mutex<Vec<TestAggregate>>,
    }

    impl Repository<TestAggregate> for TestRepository {
        type Error = Infallible;

        async fn find_by_id(&self, id: &u64) -> Result<Option<TestAggregate>, Self::Error> {
            Ok(self
                .saved
                .lock()
                .await
                .iter()
                .find(|a| a.id == *id)
                .cloned())
        }

        async fn save(
            &self,
            aggregate: &mut TestAggregate,
        ) -> Result<(), RepositoryError<Self::Error>> {
            aggregate.set_version(aggregate.version() + 1);
            self.saved.lock().await.push(aggregate.clone());
            Ok(())
        }

        async fn delete(&self, id: &u64) -> Result<(), Self::Error> {
            self.saved.lock().await.retain(|a| a.id != *id);
            Ok(())
        }
    }

    #[derive(Default)]
    struct TestOutbox {
        messages: Arc<Mutex<Vec<OutboxMessage>>>,
    }

    impl OutboxRepository for TestOutbox {
        async fn insert(&self, message: OutboxMessage) -> Result<(), OutboxError> {
            self.messages.lock().await.push(message);
            Ok(())
        }

        async fn pending(&self, limit: usize) -> Result<Vec<OutboxMessage>, OutboxError> {
            let mut messages: Vec<_> = self
                .messages
                .lock()
                .await
                .iter()
                .filter(|message| message.status == OutboxStatus::Pending)
                .cloned()
                .collect();
            messages.truncate(limit);
            Ok(messages)
        }

        async fn record_attempt(&self, id: Uuid) -> Result<(), OutboxError> {
            let mut messages = self.messages.lock().await;
            let message = messages
                .iter_mut()
                .find(|message| message.id == id)
                .ok_or(OutboxError::NotFound(id))?;
            message.record_attempt();
            Ok(())
        }

        async fn mark_published(&self, id: Uuid) -> Result<(), OutboxError> {
            let mut messages = self.messages.lock().await;
            let message = messages
                .iter_mut()
                .find(|message| message.id == id)
                .ok_or(OutboxError::NotFound(id))?;
            message.mark_published();
            Ok(())
        }

        async fn mark_failed(&self, id: Uuid, error: String) -> Result<(), OutboxError> {
            let mut messages = self.messages.lock().await;
            let message = messages
                .iter_mut()
                .find(|message| message.id == id)
                .ok_or(OutboxError::NotFound(id))?;
            message.mark_failed(error);
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
    async fn save_and_publish_persists_aggregate_advances_version_and_dispatches()
    -> Result<(), Box<dyn std::error::Error>> {
        let repo = TestRepository::default();
        let bus = EventBus::new();
        let seen = Arc::new(Mutex::new(0));
        bus.register::<TestEvent, _>(CountingHandler {
            seen: Arc::clone(&seen),
        });
        let mut aggregate = TestAggregate::new(7);

        save_and_publish(&repo, &bus, &mut aggregate).await?;

        assert_eq!(aggregate.version(), 1);
        assert!(aggregate.pending_events().is_empty());
        assert_eq!(*seen.lock().await, 1);
        assert_eq!(repo.saved.lock().await.len(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn save_and_enqueue_persists_aggregate_and_creates_outbox_message()
    -> Result<(), Box<dyn std::error::Error>> {
        let repo = TestRepository::default();
        let outbox = TestOutbox::default();
        let mut aggregate = TestAggregate::new(42);

        save_and_enqueue(&repo, &outbox, &mut aggregate, |event| {
            Message::new(
                "test-events",
                event.aggregate_id().as_bytes().to_vec(),
                "text/plain",
            )
            .with_key(event.aggregate_id())
        })
        .await?;

        assert!(aggregate.pending_events().is_empty());
        assert_eq!(aggregate.version(), 1);
        assert_eq!(repo.saved.lock().await.len(), 1);

        let pending = outbox.pending(10).await?;
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].message.topic, "test-events");
        assert_eq!(pending[0].message.key.as_deref(), Some("42"));
        assert_eq!(pending[0].status, OutboxStatus::Pending);
        Ok(())
    }
}
