use std::error::Error;

use pharos_core::{AggregateRoot, DomainEvent, Repository, RepositoryError};
use tracing::{Instrument, info_span};

use crate::error::ApplicationError;
use crate::event_bus::EventBus;
use pharos_messaging::messaging::Message;
use pharos_messaging::outbox::{OutboxMessage, OutboxRepository};

/// Maps a repository error into the application-level error type.
fn map_repository_error<E>(error: RepositoryError<E>) -> ApplicationError
where
    E: Error + Send + Sync + 'static,
{
    match error {
        RepositoryError::ConcurrencyConflict { expected, actual } => {
            ApplicationError::ConcurrencyConflict { expected, actual }
        }
        RepositoryError::Storage(error) => ApplicationError::Repository(Box::new(error)),
        // `RepositoryError` is non_exhaustive; treat unknown future variants
        // as storage failures so the caller still gets the full error text.
        other => ApplicationError::Repository(other.to_string().into()),
    }
}

/// Persists an aggregate and publishes all of its pending domain events.
///
/// The aggregate is saved first (advancing its optimistic-concurrency
/// version), then the batch it persisted is published. A failed save — e.g. a
/// `ConcurrencyConflict`, the normal retry case — never discards events: the
/// aggregate keeps them and a retry starts from intact state.
///
/// # Why the batch is copied before the save, not re-read afterwards
///
/// The events are copied up front and published from that owned copy, which
/// is what `A::Event: Clone` buys. Re-reading `pending_events()` after the
/// save would require the buffer to survive it, and a surviving buffer paired
/// with an already-advanced version is what turns a repeated `save` into
/// duplicated history on an event-sourced repository: the stream head matches
/// the bumped expected version and the next sequence numbers are free, so
/// neither the concurrency check nor the primary key objects. Event-sourced
/// repositories therefore drain while appending, and this function never
/// depends on what the repository left behind.
///
/// # Retrying after a handler failure
///
/// When this function fails **after** the save succeeded (an
/// [`ApplicationError::EventBus`] error), the aggregate is already persisted
/// and its version already advanced. Do **not** call `save_and_publish` again
/// to retry — that would version-bump the aggregate a second time. The events
/// that were not published are restored onto the aggregate, so retry only the
/// publishing step with [`republish_pending`]:
///
/// ```ignore
/// if let Err(ApplicationError::EventBus(_)) =
///     save_and_publish(&repo, &bus, &mut order).await
/// {
///     // the save already succeeded; retry only the events that did not publish
///     republish_pending(&bus, &mut order).await?;
/// }
/// ```
///
/// Only the *unpublished remainder* comes back, so a retry does not re-deliver
/// events whose handlers already ran. Handlers may still see an event more
/// than once if one of them fails after another succeeded, so they must stay
/// idempotent.
pub async fn save_and_publish<A, R>(
    repo: &R,
    bus: &EventBus,
    aggregate: &mut A,
) -> Result<(), ApplicationError>
where
    A: AggregateRoot,
    A::Event: Clone,
    R: Repository<A>,
{
    let aggregate_type = std::any::type_name::<A>();

    async move {
        let events = aggregate.pending_events().to_vec();
        repo.save(aggregate).await.map_err(map_repository_error)?;
        // An event-sourced repository already drained while appending; a
        // state-based one did not. Clearing here leaves both in the same
        // state, with the batch owned by this function either way.
        aggregate.drain_events();

        tracing::info!(
            aggregate = aggregate_type,
            version = aggregate.version(),
            event_count = events.len(),
            "aggregate persisted; publishing pending events"
        );

        publish_batch(bus, aggregate, events).await
    }
    .instrument(info_span!("save_and_publish", aggregate = aggregate_type))
    .await
}

/// Publishes an owned batch, restoring whatever did not make it onto the
/// aggregate so the caller can retry exactly the remainder.
async fn publish_batch<A>(
    bus: &EventBus,
    aggregate: &mut A,
    events: Vec<A::Event>,
) -> Result<(), ApplicationError>
where
    A: AggregateRoot,
{
    let mut remaining = events.into_iter();
    while let Some(event) = remaining.next() {
        if let Err(error) = bus.publish(&event).await {
            let mut unpublished = vec![event];
            unpublished.extend(remaining);
            aggregate.restore_events(unpublished);
            return Err(error.into());
        }
        metrics::counter!("pharos.events.published", "event_type" => event.event_type())
            .increment(1);
    }
    Ok(())
}

/// Publishes an aggregate's pending domain events without saving it.
///
/// This is the safe retry path after [`save_and_publish`] failed on the
/// publishing side: the aggregate is already persisted, so a retry must not
/// save (and version-bump) it again. Whatever fails to publish is restored
/// onto the aggregate, so this can be called repeatedly and each attempt only
/// covers the events still outstanding. Handlers may still see the same event
/// more than once across retries and must be idempotent.
pub async fn republish_pending<A>(bus: &EventBus, aggregate: &mut A) -> Result<(), ApplicationError>
where
    A: AggregateRoot,
{
    let aggregate_type = std::any::type_name::<A>();

    async move {
        let events = aggregate.drain_events();
        publish_batch(bus, aggregate, events).await
    }
    .instrument(info_span!("republish_pending", aggregate = aggregate_type))
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
    A::Event: Clone,
    R: Repository<A>,
    O: OutboxRepository,
    F: Fn(&A::Event) -> Message + Send + Sync,
{
    let aggregate_type = std::any::type_name::<A>();

    async move {
        // Same reasoning as `save_and_publish`: own the batch before the save
        // rather than re-reading a buffer that must not survive it.
        let events = aggregate.pending_events().to_vec();
        repo.save(aggregate).await.map_err(map_repository_error)?;
        aggregate.drain_events();

        tracing::info!(
            aggregate = aggregate_type,
            version = aggregate.version(),
            event_count = events.len(),
            "aggregate persisted; enqueueing pending events to outbox"
        );

        let mut remaining = events.into_iter();
        while let Some(event) = remaining.next() {
            let outbox_span = info_span!(
                "enqueue_pending_event",
                event_type = event.event_type(),
                event.aggregate_id = event.aggregate_id(),
            );
            let result = async {
                let message = map_event(&event);
                outbox.insert(OutboxMessage::new(message)).await?;
                Ok::<(), ApplicationError>(())
            }
            .instrument(outbox_span)
            .await;

            if let Err(error) = result {
                let mut unenqueued = vec![event];
                unenqueued.extend(remaining);
                aggregate.restore_events(unenqueued);
                return Err(error);
            }
            metrics::counter!("pharos.outbox.enqueued", "event_type" => event.event_type())
                .increment(1);
        }

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
    use pharos_messaging::outbox::{OutboxError, OutboxStatus};

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

        async fn failed(&self, limit: usize) -> Result<Vec<OutboxMessage>, OutboxError> {
            let mut failed: Vec<_> = self
                .messages
                .lock()
                .await
                .iter()
                .filter(|message| message.status == OutboxStatus::Failed)
                .cloned()
                .collect();
            failed.truncate(limit);
            Ok(failed)
        }

        async fn mark_dead_lettered(&self, id: Uuid) -> Result<(), OutboxError> {
            let mut messages = self.messages.lock().await;
            let message = messages
                .iter_mut()
                .find(|message| message.id == id)
                .ok_or(OutboxError::NotFound(id))?;
            message.status = OutboxStatus::DeadLettered;
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

    /// Stands in for an event-sourced repository, whose `save` takes ownership
    /// of the batch it appends rather than leaving it in the buffer.
    #[derive(Default)]
    struct DrainingRepository {
        appended: Mutex<Vec<TestEvent>>,
    }

    impl Repository<TestAggregate> for DrainingRepository {
        type Error = Infallible;

        async fn find_by_id(&self, _id: &u64) -> Result<Option<TestAggregate>, Self::Error> {
            Ok(None)
        }

        async fn save(
            &self,
            aggregate: &mut TestAggregate,
        ) -> Result<(), RepositoryError<Self::Error>> {
            let events = aggregate.drain_events();
            aggregate.set_version(aggregate.version() + events.len() as u64);
            self.appended.lock().await.extend(events);
            Ok(())
        }

        async fn delete(&self, _id: &u64) -> Result<(), Self::Error> {
            Ok(())
        }
    }

    /// Regression test: `save_and_publish` must publish the events even when
    /// the repository drained them while saving.
    ///
    /// Reading `pending_events()` back after the save — which an earlier
    /// version did — meant an event-sourced aggregate's domain events were
    /// appended to the store and then silently never delivered to any
    /// `EventBus` handler, because the buffer the loop read was already empty.
    #[tokio::test]
    async fn save_and_publish_publishes_even_when_the_repository_drains()
    -> Result<(), Box<dyn std::error::Error>> {
        let repo = DrainingRepository::default();
        let bus = EventBus::new();
        let seen = Arc::new(Mutex::new(0));
        bus.register::<TestEvent, _>(CountingHandler {
            seen: Arc::clone(&seen),
        });
        let mut aggregate = TestAggregate::new(11);

        save_and_publish(&repo, &bus, &mut aggregate).await?;

        assert_eq!(
            repo.appended.lock().await.len(),
            1,
            "the event was persisted"
        );
        assert_eq!(*seen.lock().await, 1, "and it also reached the handler");
        assert!(aggregate.pending_events().is_empty());
        assert_eq!(aggregate.version(), 1);
        Ok(())
    }

    #[derive(Debug, thiserror::Error)]
    #[error("handler failed")]
    struct FailOnce;

    struct FailingOnceHandler {
        failed: Arc<Mutex<bool>>,
        seen: Arc<Mutex<u32>>,
    }

    impl EventHandler<TestEvent> for FailingOnceHandler {
        type Error = FailOnce;

        async fn handle(&self, _event: &TestEvent) -> Result<(), Self::Error> {
            let mut failed = self.failed.lock().await;
            if !*failed {
                *failed = true;
                return Err(FailOnce);
            }
            *self.seen.lock().await += 1;
            Ok(())
        }
    }

    #[tokio::test]
    async fn republish_pending_retries_events_without_saving_again()
    -> Result<(), Box<dyn std::error::Error>> {
        let repo = TestRepository::default();
        let bus = EventBus::new();
        let seen = Arc::new(Mutex::new(0));
        bus.register::<TestEvent, _>(FailingOnceHandler {
            failed: Arc::new(Mutex::new(false)),
            seen: Arc::clone(&seen),
        });
        let mut aggregate = TestAggregate::new(9);

        // First publish fails; the save succeeded and the event stays pending.
        let result = save_and_publish(&repo, &bus, &mut aggregate).await;
        assert!(matches!(result, Err(ApplicationError::EventBus(_))));
        assert_eq!(aggregate.version(), 1);
        assert_eq!(aggregate.pending_events().len(), 1);
        assert_eq!(repo.saved.lock().await.len(), 1);

        // The safe retry path republishes without saving (no version bump,
        // no second row in the repository).
        republish_pending(&bus, &mut aggregate).await?;
        assert_eq!(aggregate.version(), 1);
        assert!(aggregate.pending_events().is_empty());
        assert_eq!(*seen.lock().await, 1);
        assert_eq!(repo.saved.lock().await.len(), 1);
        Ok(())
    }

    /// Fails on the event whose `aggregate_id` matches, letting a test place
    /// the failure at a chosen position inside a batch.
    struct FailOnAggregateId {
        id: String,
        published: Arc<Mutex<Vec<String>>>,
    }

    impl EventHandler<TestEvent> for FailOnAggregateId {
        type Error = FailOnce;

        async fn handle(&self, event: &TestEvent) -> Result<(), Self::Error> {
            if event.aggregate_id == self.id {
                return Err(FailOnce);
            }
            self.published.lock().await.push(event.aggregate_id.clone());
            Ok(())
        }
    }

    /// Only the events that never reached a handler come back, in order — a
    /// retry must not re-deliver the ones that already published.
    #[tokio::test]
    async fn a_failed_publish_restores_only_the_unpublished_remainder()
    -> Result<(), Box<dyn std::error::Error>> {
        let repo = TestRepository::default();
        let bus = EventBus::new();
        let published = Arc::new(Mutex::new(Vec::new()));
        bus.register::<TestEvent, _>(FailOnAggregateId {
            id: "second".to_string(),
            published: Arc::clone(&published),
        });

        let mut aggregate = TestAggregate::new(3);
        aggregate.events.drain();
        for id in ["first", "second", "third"] {
            aggregate.events.raise(TestEvent {
                aggregate_id: id.to_string(),
                occurred_at: Utc::now(),
            });
        }

        let result = save_and_publish(&repo, &bus, &mut aggregate).await;
        assert!(matches!(result, Err(ApplicationError::EventBus(_))));

        assert_eq!(
            *published.lock().await,
            vec!["first".to_string()],
            "only the event before the failure published"
        );
        let pending: Vec<_> = aggregate
            .pending_events()
            .iter()
            .map(|e| e.aggregate_id.clone())
            .collect();
        assert_eq!(
            pending,
            vec!["second".to_string(), "third".to_string()],
            "the failed event and everything after it came back, in order"
        );
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
