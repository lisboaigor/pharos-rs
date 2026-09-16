use std::error::Error;

use thiserror::Error;

#[cfg(feature = "messaging")]
use pharos_core::AggregateRoot;
#[cfg(feature = "messaging")]
use pharos_core::RepositoryError;
#[cfg(feature = "messaging")]
use pharos_messaging::{Message, OutboxMessage};

/// Error produced by unit-of-work implementations.
///
/// Every variant keeps the originating error as a typed `source`, so callers
/// can walk the chain down to the adapter failure (e.g. a `sqlx::Error`)
/// instead of matching on strings.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum UnitOfWorkError {
    /// Transaction begin failed.
    #[error("begin transaction failed: {0}")]
    Begin(#[source] Box<dyn Error + Send + Sync + 'static>),
    /// Transaction commit failed.
    #[error("commit transaction failed: {0}")]
    Commit(#[source] Box<dyn Error + Send + Sync + 'static>),
    /// Transaction rollback failed.
    #[error("rollback transaction failed: {0}")]
    Rollback(#[source] Box<dyn Error + Send + Sync + 'static>),
    /// The transactional operation failed.
    #[error("transactional operation failed: {0}")]
    Operation(#[source] Box<dyn Error + Send + Sync + 'static>),
}

impl UnitOfWorkError {
    /// Wraps an error as a begin failure.
    pub fn begin(e: impl Error + Send + Sync + 'static) -> Self {
        Self::Begin(Box::new(e))
    }
    /// Wraps an error as a commit failure.
    pub fn commit(e: impl Error + Send + Sync + 'static) -> Self {
        Self::Commit(Box::new(e))
    }
    /// Wraps an error as a rollback failure.
    pub fn rollback(e: impl Error + Send + Sync + 'static) -> Self {
        Self::Rollback(Box::new(e))
    }
    /// Wraps an error as an operation failure.
    pub fn operation(e: impl Error + Send + Sync + 'static) -> Self {
        Self::Operation(Box::new(e))
    }
}

// A previous version of this file argued there could be no `UnitOfWork`
// trait: "a generic trait whose closure cannot borrow the live transaction
// handle would prevent repositories from participating in the transaction
// — an abstraction that looks like a unit of work but cannot compose one."
// That is correct about a *closure-shaped* trait (`fn transaction(&self, f:
// impl FnOnce(&mut Conn) -> ...)`), where `Conn` has to be named concretely
// for the closure signature to type-check at all. It does not hold for a
// trait built around an associated type ([`TransactionalStore::Tx`] below):
// it lets [`save_and_enqueue_in`] hold one live transaction handle across two
// independent calls — the repository's [`TransactionalRepository::save_in_tx`]
// and the store's own [`TransactionalStore::insert_outbox_in_tx`] — without
// either the trait or the composing function ever naming a concrete
// connection type. The conclusion the old comment drew (bind the handle to
// one driver) was stronger than the actual problem required.
//
// A later revision made `Tx` a GAT (`type Tx<'a>: Send where Self: 'a`, with
// `begin` returning `Self::Tx<'_>` borrowed from `&self`) to additionally let
// a backend's handle borrow its store. That shape hits rust-lang/rust#100013
// ("lifetime bound not satisfied") whenever `save_and_enqueue_in` is called
// from inside another trait's `async fn` — exactly the shape of a
// `CommandHandler::handle` calling it on a locally-owned `Store`. No backend
// implementation ever needed the borrow (`pharos-postgres`'s handle is
// already an owned, `'static` transaction), so `Tx` went back to a plain
// associated type — same composability, no lifetime parameter, and the bug
// no longer applies.

/// Backend-agnostic transactional boundary.
///
/// `Tx` is the live handle a backend's queries run against inside one
/// transaction. For `pharos-postgres`, that is an owned `sqlx::Transaction<
/// 'static, Postgres>`; a different backend names its own type. Generic code
/// that composes an aggregate save with an outbox insert —
/// [`save_and_enqueue_in`] — is written once, against this trait, and never
/// names a concrete connection type: the same composing function works
/// against any backend that implements it, not only PostgreSQL.
///
/// `Tx` is a plain associated type, not a GAT over a borrow of `&self`: an
/// earlier design used `type Tx<'a>: Send where Self: 'a`, with `begin`
/// returning `Self::Tx<'_>` tied to `&self`'s borrow. That shape — a GAT
/// whose lifetime parameter is itself introduced inside another trait's
/// `async fn` body (a `CommandHandler::handle` calling [`save_and_enqueue_in`]
/// on a locally-owned `Store`) — triggers rust-lang/rust#100013 ("lifetime
/// bound not satisfied"), a real, still-open rustc limitation on opaque-type
/// nesting. Every backend implementation to date (`pharos-postgres`'s
/// `PostgresUnitOfWork`) already produces an owned, `'static` handle — the
/// borrowed-`'a` generality was never exercised — so dropping the lifetime
/// parameter costs no real backend-agnosticism while sidestepping the bug
/// entirely. A backend that genuinely needs a borrowed handle can still wrap
/// it in an owned guard (e.g. `Arc<Mutex<..>>` or a boxed handle) to satisfy
/// `Tx: Send` without a lifetime parameter.
#[cfg(feature = "messaging")]
#[trait_variant::make(Send)]
pub trait TransactionalStore: Sync {
    /// The live transaction handle, owned by the caller once `begin` returns
    /// it.
    type Tx: Send;
    /// Storage error.
    type Error: Error + Send + Sync + 'static;

    /// Opens a new transaction.
    async fn begin(&self) -> Result<Self::Tx, Self::Error>;

    /// Commits a transaction opened with [`Self::begin`].
    ///
    /// Takes `tx` by value: once committed (or, on the caller's error path,
    /// simply dropped to roll back), the handle cannot be reused — the type
    /// system enforces the "one transaction, one outcome" rule a closure-
    /// shaped API would only enforce by convention.
    async fn commit(&self, tx: Self::Tx) -> Result<(), Self::Error>;

    /// Inserts a pending outbox message using the same live transaction a
    /// [`TransactionalRepository::save_in_tx`] call just wrote the aggregate
    /// through, so both become visible atomically or neither does.
    async fn insert_outbox_in_tx<'a>(
        &'a self,
        tx: &'a mut Self::Tx,
        message: &'a OutboxMessage,
    ) -> Result<(), Self::Error>;
}

/// A repository whose `save` can run inside a [`TransactionalStore`]'s live
/// transaction.
///
/// Implement this in addition to `Repository<A>` and [`save_and_enqueue_in`]
/// gives your aggregate the atomic save+outbox guarantee against whichever
/// `Store` you implement it for — a JSONB repository, or explicit normalized
/// tables with your own SQL.
///
/// # Contract
///
/// `save_in_tx` must enforce optimistic concurrency exactly like
/// `Repository::save`: check the expected version, advance the aggregate's
/// in-memory version on success, and return
/// [`RepositoryError::ConcurrencyConflict`] on a stale write **without**
/// mutating rows. It must not begin, commit, or roll back the transaction —
/// the composing caller ([`save_and_enqueue_in`]) owns that boundary (and
/// reverts the in-memory version if the surrounding transaction later fails).
#[cfg(feature = "messaging")]
#[trait_variant::make(Send)]
pub trait TransactionalRepository<A, Store>: Sync
where
    A: AggregateRoot,
    Store: TransactionalStore,
{
    /// The repository-specific storage error type.
    type Error: Error + Send + Sync + 'static;

    /// Persists the aggregate using the caller's live transaction handle.
    async fn save_in_tx<'c>(
        &'c self,
        tx: &'c mut Store::Tx,
        aggregate: &'c mut A,
    ) -> Result<(), RepositoryError<Self::Error>>;
}

/// Error returned by [`save_and_enqueue_in`].
#[cfg(feature = "messaging")]
#[derive(Debug, Error)]
pub enum SaveAndEnqueueError<RepoErr, StoreErr>
where
    RepoErr: Error + Send + Sync + 'static,
    StoreErr: Error + Send + Sync + 'static,
{
    /// The repository failed to persist the aggregate (including
    /// optimistic-concurrency conflicts).
    #[error(transparent)]
    Repository(RepositoryError<RepoErr>),
    /// Opening/committing the transaction, or writing the outbox, failed.
    #[error(transparent)]
    Store(StoreErr),
    /// `map_event` failed to turn a pending domain event into an outgoing
    /// [`Message`]. Surfaces before any transaction opens: an event that
    /// cannot be mapped is caught before the aggregate save is attempted, so
    /// nothing is left half-committed.
    #[error("event mapping failed: {0}")]
    Mapping(#[source] Box<dyn Error + Send + Sync + 'static>),
}

/// Persists an aggregate through any [`TransactionalRepository`] and enqueues
/// its pending events as outbox messages, atomically, against any
/// [`TransactionalStore`] — not only PostgreSQL.
///
/// This is the production write path: one transaction covers the aggregate
/// rows *and* the outbox inserts, so either both become visible or neither
/// does.
///
/// On any failure the aggregate's in-memory state is left intact: the
/// version is reverted and the pending events are kept, so a retry starts
/// clean. Events are drained only after the commit succeeds.
///
/// `map_event` is fallible (e.g. serializing the event payload): every
/// pending event is mapped **before** any transaction opens, so a mapping
/// failure never leaves a half-open transaction behind and never needs a
/// placeholder payload for the events it could not map.
#[cfg(feature = "messaging")]
pub async fn save_and_enqueue_in<A, Store, Repo, F, E>(
    store: &Store,
    repo: &Repo,
    aggregate: &mut A,
    map_event: F,
) -> Result<(), SaveAndEnqueueError<Repo::Error, Store::Error>>
where
    A: AggregateRoot,
    Store: TransactionalStore,
    Repo: TransactionalRepository<A, Store>,
    F: Fn(&A::Event) -> Result<Message, E> + Send + Sync,
    E: Error + Send + Sync + 'static,
{
    let expected = aggregate.version();

    // Build the outbox messages from the still-pending events, before
    // touching the database; they are only drained after the transaction
    // commits.
    let mut messages = Vec::with_capacity(aggregate.pending_events().len());
    for event in aggregate.pending_events() {
        let message = map_event(event).map_err(|e| SaveAndEnqueueError::Mapping(Box::new(e)))?;
        messages.push(OutboxMessage::new(message));
    }

    let result = async {
        let mut tx = store.begin().await.map_err(SaveAndEnqueueError::Store)?;

        repo.save_in_tx(&mut tx, aggregate)
            .await
            .map_err(SaveAndEnqueueError::Repository)?;

        for message in &messages {
            store
                .insert_outbox_in_tx(&mut tx, message)
                .await
                .map_err(SaveAndEnqueueError::Store)?;
        }

        store.commit(tx).await.map_err(SaveAndEnqueueError::Store)?;
        Ok(())
    }
    .await;

    match result {
        Ok(()) => {
            aggregate.drain_events();
            Ok(())
        }
        Err(error) => {
            aggregate.set_version(expected);
            Err(error)
        }
    }
}
