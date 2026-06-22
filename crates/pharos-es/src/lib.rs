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
pub trait EventStore<I, E>: Send + Sync + 'static {
    /// Storage error.
    type Error: Error + Send + Sync + 'static;

    /// Loads all events for a stream.
    fn load(&self, id: &I)
    -> impl Future<Output = Result<Vec<StoredEvent<E>>, Self::Error>> + Send;

    /// Appends events at the current expected version.
    fn append(
        &self,
        id: &I,
        expected_version: u64,
        events: Vec<E>,
    ) -> impl Future<Output = Result<(), RepositoryError<Self::Error>>> + Send;
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

    async fn delete(&self, _id: &A::Id) -> Result<(), Self::Error> {
        Ok(())
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
                .unwrap()
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
            let mut streams = self.streams.lock().unwrap();
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
    }

    #[tokio::test]
    async fn repository_rehydrates_from_stream() {
        let repo = EventSourcedRepository::<Account, _>::new(InMemoryEventStore::default());
        let mut account = Account::open("acc-1", "Igor");

        repo.save(&mut account).await.unwrap();
        let loaded = repo
            .find_by_id(&"acc-1".to_string())
            .await
            .unwrap()
            .unwrap();

        assert_eq!(loaded.id, "acc-1");
        assert_eq!(loaded.owner, "Igor");
    }
}
