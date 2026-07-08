//! Event sourcing building blocks for Pharos.
//!
//! This crate provides an append-only event store contract, snapshot support,
//! and a repository that rehydrates an aggregate by applying stored events.

use std::error::Error;
use std::future::Future;

use chrono::{DateTime, Utc};
use pharos_core::{AggregateRoot, Repository, RepositoryError};

/// One event persisted in an event stream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredEvent<E> {
    /// Sequence number in the stream, starting at `1`.
    pub sequence: u64,
    /// Domain event payload.
    pub event: E,
    /// Persistence timestamp.
    pub recorded_at: DateTime<Utc>,
}

impl<E> StoredEvent<E> {
    /// Creates a stored event with the current timestamp.
    pub fn new(sequence: u64, event: E) -> Self {
        Self {
            sequence,
            event,
            recorded_at: Utc::now(),
        }
    }
}

/// Event-sourced aggregate behavior.
pub trait EventSourced: AggregateRoot {
    /// Applies one historical event to the in-memory aggregate state.
    fn apply(&mut self, event: &Self::Event);
}

/// Persistence boundary for append-only streams.
///
/// `I` and `E` mirror `Entity::Id` and `DomainEvent`, which are already
/// `Send + Sync`; the bounds here let default method bodies hold them across
/// `.await` points.
pub trait EventStore<I, E>: Send + Sync + 'static
where
    I: Sync,
    E: Send,
{
    /// Storage error.
    type Error: Error + Send + Sync + 'static;

    /// Loads all events for a stream.
    fn load(&self, id: &I)
    -> impl Future<Output = Result<Vec<StoredEvent<E>>, Self::Error>> + Send;

    /// Loads the events of a stream with a sequence greater than `after`.
    ///
    /// Used by snapshot-aware rehydration to replay only the tail of a
    /// stream. The default implementation loads everything and filters in
    /// memory; stores should override it with a range query.
    fn load_after(
        &self,
        id: &I,
        after: u64,
    ) -> impl Future<Output = Result<Vec<StoredEvent<E>>, Self::Error>> + Send {
        async move {
            let events = self.load(id).await?;
            Ok(events
                .into_iter()
                .filter(|stored| stored.sequence > after)
                .collect())
        }
    }

    /// Appends events at the current expected version.
    fn append(
        &self,
        id: &I,
        expected_version: u64,
        events: Vec<E>,
    ) -> impl Future<Output = Result<(), RepositoryError<Self::Error>>> + Send;

    /// Deletes a whole stream.
    ///
    /// Event streams are usually immutable history and many stores will keep
    /// them forever; implementations that refuse deletion must return an
    /// error rather than silently succeeding, so callers never believe data
    /// was removed when it was not.
    fn delete_stream(&self, id: &I) -> impl Future<Output = Result<(), Self::Error>> + Send;
}

/// Materialized snapshot for an aggregate state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Snapshot<S> {
    /// Aggregate state captured in the snapshot.
    pub state: S,
    /// Aggregate version represented by the snapshot.
    pub version: u64,
    /// Snapshot timestamp.
    pub taken_at: DateTime<Utc>,
}

impl<S> Snapshot<S> {
    /// Creates a snapshot.
    pub fn new(state: S, version: u64) -> Self {
        Self {
            state,
            version,
            taken_at: Utc::now(),
        }
    }
}

/// Optional snapshot persistence boundary.
pub trait SnapshotStore<I, S>: Send + Sync + 'static {
    /// Storage error.
    type Error: Error + Send + Sync + 'static;

    /// Loads the latest snapshot, when present.
    fn load(&self, id: &I)
    -> impl Future<Output = Result<Option<Snapshot<S>>, Self::Error>> + Send;

    /// Saves or replaces a snapshot.
    fn save(
        &self,
        id: &I,
        snapshot: Snapshot<S>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;
}

/// Repository that rehydrates an aggregate by replaying its event stream.
pub struct EventSourcedRepository<A, Store> {
    store: Store,
    _marker: std::marker::PhantomData<fn() -> A>,
}

impl<A, Store> EventSourcedRepository<A, Store> {
    /// Creates a repository from an event store.
    pub fn new(store: Store) -> Self {
        Self {
            store,
            _marker: std::marker::PhantomData,
        }
    }

    /// Returns the underlying store.
    pub fn store(&self) -> &Store {
        &self.store
    }
}

impl<A, Store> Repository<A> for EventSourcedRepository<A, Store>
where
    A: EventSourced + Default,
    Store: EventStore<A::Id, A::Event>,
{
    type Error = Store::Error;

    async fn find_by_id(&self, id: &A::Id) -> Result<Option<A>, Self::Error> {
        let events = self.store.load(id).await?;
        if events.is_empty() {
            return Ok(None);
        }

        let mut aggregate = A::default();
        let mut version = 0;
        for stored in events {
            aggregate.apply(&stored.event);
            version = stored.sequence;
        }
        aggregate.set_version(version);
        Ok(Some(aggregate))
    }

    async fn save(&self, aggregate: &mut A) -> Result<(), RepositoryError<Self::Error>> {
        let expected_version = aggregate.version();
        let events = aggregate.drain_events();
        let event_count = events.len() as u64;
        self.store
            .append(aggregate.id(), expected_version, events)
            .await?;
        aggregate.set_version(expected_version + event_count);
        Ok(())
    }

    async fn delete(&self, id: &A::Id) -> Result<(), Self::Error> {
        self.store.delete_stream(id).await
    }
}

/// Event-sourced repository that uses snapshots to bound replay cost.
///
/// `find_by_id` loads the latest snapshot (when one exists) and replays only
/// the events recorded after it; `save` appends the pending events and then
/// refreshes the snapshot once at least `snapshot_every` new events
/// accumulated since the last one.
///
/// Snapshots are an optimization, never the source of truth: any snapshot
/// load or save failure is logged and the repository falls back to the full
/// event stream, so a broken snapshot store degrades performance, not
/// correctness.
pub struct SnapshottingEventSourcedRepository<A, Store, Snap> {
    store: Store,
    snapshots: Snap,
    snapshot_every: u64,
    _marker: std::marker::PhantomData<fn() -> A>,
}

impl<A, Store, Snap> SnapshottingEventSourcedRepository<A, Store, Snap> {
    /// Creates a snapshotting repository.
    ///
    /// `snapshot_every` is the number of events after which a new snapshot is
    /// taken (values below 1 are clamped to 1).
    pub fn new(store: Store, snapshots: Snap, snapshot_every: u64) -> Self {
        Self {
            store,
            snapshots,
            snapshot_every: snapshot_every.max(1),
            _marker: std::marker::PhantomData,
        }
    }

    /// Returns the underlying event store.
    pub fn store(&self) -> &Store {
        &self.store
    }

    /// Returns the underlying snapshot store.
    pub fn snapshots(&self) -> &Snap {
        &self.snapshots
    }
}

impl<A, Store, Snap> Repository<A> for SnapshottingEventSourcedRepository<A, Store, Snap>
where
    A: EventSourced + Default + Clone,
    Store: EventStore<A::Id, A::Event>,
    Snap: SnapshotStore<A::Id, A>,
{
    type Error = Store::Error;

    async fn find_by_id(&self, id: &A::Id) -> Result<Option<A>, Self::Error> {
        let snapshot = match self.snapshots.load(id).await {
            Ok(snapshot) => snapshot,
            Err(error) => {
                tracing::warn!(error = %error, "snapshot load failed; replaying full stream");
                None
            }
        };

        let (mut aggregate, mut version) = match snapshot {
            Some(snapshot) => (snapshot.state, snapshot.version),
            None => (A::default(), 0),
        };

        let events = self.store.load_after(id, version).await?;
        if version == 0 && events.is_empty() {
            return Ok(None);
        }
        for stored in events {
            aggregate.apply(&stored.event);
            version = stored.sequence;
        }
        aggregate.set_version(version);
        Ok(Some(aggregate))
    }

    async fn save(&self, aggregate: &mut A) -> Result<(), RepositoryError<Self::Error>> {
        let expected_version = aggregate.version();
        let events = aggregate.drain_events();
        let event_count = events.len() as u64;
        self.store
            .append(aggregate.id(), expected_version, events)
            .await?;
        let new_version = expected_version + event_count;
        aggregate.set_version(new_version);

        // Refresh the snapshot when the stream grew past the boundary. This is
        // best-effort: the events are already durable.
        let previous_boundary = expected_version / self.snapshot_every;
        let new_boundary = new_version / self.snapshot_every;
        if new_boundary > previous_boundary
            && let Err(error) = self
                .snapshots
                .save(
                    aggregate.id(),
                    Snapshot::new(aggregate.clone(), new_version),
                )
                .await
        {
            tracing::warn!(error = %error, "snapshot save failed; stream remains authoritative");
        }
        Ok(())
    }

    async fn delete(&self, id: &A::Id) -> Result<(), Self::Error> {
        self.store.delete_stream(id).await
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::convert::Infallible;
    use std::sync::Mutex;

    use pharos_core::{AggregateEvents, DomainEvent, Entity};

    use super::*;

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct AccountOpened {
        account_id: String,
        owner: String,
    }

    impl DomainEvent for AccountOpened {
        fn event_type(&self) -> &'static str {
            "AccountOpened"
        }

        fn occurred_at(&self) -> DateTime<Utc> {
            Utc::now()
        }

        fn aggregate_id(&self) -> &str {
            &self.account_id
        }
    }

    #[derive(Debug, Clone, Default)]
    struct Account {
        id: String,
        owner: String,
        version: u64,
        events: AggregateEvents<AccountOpened>,
    }

    impl Account {
        fn open(id: impl Into<String>, owner: impl Into<String>) -> Self {
            let id = id.into();
            let owner = owner.into();
            let mut events = AggregateEvents::default();
            events.raise(AccountOpened {
                account_id: id.clone(),
                owner: owner.clone(),
            });
            Self {
                id,
                owner,
                version: 0,
                events,
            }
        }
    }

    impl Entity for Account {
        type Id = String;

        fn id(&self) -> &Self::Id {
            &self.id
        }
    }

    impl AggregateRoot for Account {
        type Event = AccountOpened;

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

    impl EventSourced for Account {
        fn apply(&mut self, event: &Self::Event) {
            self.id = event.account_id.clone();
            self.owner = event.owner.clone();
        }
    }

    #[derive(Default)]
    struct InMemoryEventStore {
        streams: Mutex<HashMap<String, Vec<StoredEvent<AccountOpened>>>>,
    }

    impl EventStore<String, AccountOpened> for InMemoryEventStore {
        type Error = Infallible;

        async fn load(&self, id: &String) -> Result<Vec<StoredEvent<AccountOpened>>, Self::Error> {
            Ok(self
                .streams
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .get(id)
                .cloned()
                .unwrap_or_default())
        }

        async fn append(
            &self,
            id: &String,
            expected_version: u64,
            events: Vec<AccountOpened>,
        ) -> Result<(), RepositoryError<Self::Error>> {
            let mut streams = self.streams.lock().unwrap_or_else(|p| p.into_inner());
            let stream = streams.entry(id.clone()).or_default();
            let current = stream.len() as u64;
            if current != expected_version {
                return Err(RepositoryError::ConcurrencyConflict {
                    expected: expected_version,
                    actual: Some(current),
                });
            }
            for event in events {
                let sequence = stream.len() as u64 + 1;
                stream.push(StoredEvent::new(sequence, event));
            }
            Ok(())
        }

        async fn delete_stream(&self, id: &String) -> Result<(), Self::Error> {
            self.streams
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .remove(id);
            Ok(())
        }
    }

    #[derive(Default)]
    struct InMemorySnapshotStore {
        snapshots: Mutex<HashMap<String, Snapshot<Account>>>,
    }

    impl SnapshotStore<String, Account> for InMemorySnapshotStore {
        type Error = Infallible;

        async fn load(&self, id: &String) -> Result<Option<Snapshot<Account>>, Self::Error> {
            Ok(self
                .snapshots
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .get(id)
                .cloned())
        }

        async fn save(&self, id: &String, snapshot: Snapshot<Account>) -> Result<(), Self::Error> {
            self.snapshots
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .insert(id.clone(), snapshot);
            Ok(())
        }
    }

    #[tokio::test]
    async fn snapshotting_repository_snapshots_and_replays_only_the_tail()
    -> Result<(), Box<dyn std::error::Error>> {
        let repo = SnapshottingEventSourcedRepository::<Account, _, _>::new(
            InMemoryEventStore::default(),
            InMemorySnapshotStore::default(),
            1, // snapshot after every event, to exercise the path aggressively
        );
        let mut account = Account::open("acc-1", "Igor");
        repo.save(&mut account).await?;

        // The snapshot must exist and carry the persisted version.
        let snapshot = repo
            .snapshots()
            .load(&"acc-1".to_string())
            .await?
            .ok_or("snapshot must have been taken")?;
        assert_eq!(snapshot.version, 1);

        // Loading rehydrates from the snapshot + empty tail.
        let loaded = repo
            .find_by_id(&"acc-1".to_string())
            .await?
            .ok_or("account not found")?;
        assert_eq!(loaded.owner, "Igor");
        assert_eq!(loaded.version(), 1);

        // Unknown streams still come back as None even with a snapshot store.
        assert!(repo.find_by_id(&"missing".to_string()).await?.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn delete_removes_the_stream() -> Result<(), Box<dyn std::error::Error>> {
        let repo = EventSourcedRepository::<Account, _>::new(InMemoryEventStore::default());
        let mut account = Account::open("acc-2", "Igor");
        repo.save(&mut account).await?;

        repo.delete(&"acc-2".to_string()).await?;
        assert!(repo.find_by_id(&"acc-2".to_string()).await?.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn repository_rehydrates_from_stream() -> Result<(), Box<dyn std::error::Error>> {
        let repo = EventSourcedRepository::<Account, _>::new(InMemoryEventStore::default());
        let mut account = Account::open("acc-1", "Igor");

        repo.save(&mut account).await?;
        let loaded = repo
            .find_by_id(&"acc-1".to_string())
            .await?
            .ok_or("account not found")?;

        assert_eq!(loaded.id, "acc-1");
        assert_eq!(loaded.owner, "Igor");
        Ok(())
    }
}
