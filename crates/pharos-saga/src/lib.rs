//! Saga and process-manager building blocks for Pharos.
//!
//! A saga reacts to domain or integration events, persists a long-lived state
//! machine, and emits follow-up commands. The runner in this crate keeps that
//! flow explicit and testable without imposing transport or storage details.

use std::error::Error;
use std::future::Future;
use std::marker::PhantomData;

use chrono::{DateTime, Utc};
use pharos_messaging::{Message, MessagingError, OutboxError, OutboxMessage, OutboxRepository};
use thiserror::Error;

/// Lifecycle of a saga instance.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SagaStatus {
    /// The saga is waiting for more events.
    Running,
    /// The saga has reached a terminal success state.
    Completed,
    /// The saga failed irrecoverably.
    Failed,
}

/// Persisted state for one saga instance.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SagaInstance<I, S> {
    /// Stable saga identifier.
    pub id: I,
    /// Current state machine payload.
    pub state: S,
    /// Current lifecycle status.
    pub status: SagaStatus,
    /// Instant after which the saga times out, when set.
    ///
    /// Only meaningful while the saga is [`SagaStatus::Running`]; terminal
    /// transitions clear it. [`SagaRunner::run_due_timeouts`] fires
    /// [`Saga::on_timeout`] for running instances past this instant.
    pub deadline: Option<DateTime<Utc>>,
    /// Last update timestamp.
    pub updated_at: DateTime<Utc>,
    /// Version this instance was loaded (or created) at, for
    /// [`SagaStore::save`]'s optimistic-concurrency check.
    ///
    /// `0` means "never persisted": [`save`](SagaStore::save) must create the
    /// row rather than update one. A caller never sets this to anything but
    /// what [`SagaStore::load`] (or a fresh [`running`](Self::running)) handed
    /// it — `save` derives the version it actually stores (`version + 1`)
    /// from its own count, so there is no way to hand-construct an instance
    /// that claims a version beyond what was legitimately persisted.
    pub version: u64,
}

impl<I, S> SagaInstance<I, S> {
    /// Creates a running saga instance with no deadline.
    pub fn running(id: I, state: S) -> Self {
        Self {
            id,
            state,
            status: SagaStatus::Running,
            deadline: None,
            updated_at: Utc::now(),
            version: 0,
        }
    }

    /// Creates a running saga instance that times out at `deadline`.
    pub fn running_until(id: I, state: S, deadline: DateTime<Utc>) -> Self {
        Self {
            deadline: Some(deadline),
            ..Self::running(id, state)
        }
    }
}

/// Transition produced by a saga in response to an event or timeout.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SagaTransition<S, C> {
    /// The saga is not interested in the event.
    ///
    /// On the timeout path this clears the elapsed deadline and keeps the
    /// saga running, so an ignored timeout never refires.
    Ignore,
    /// Start a new saga instance, optionally with a timeout deadline.
    Start {
        state: S,
        commands: Vec<C>,
        deadline: Option<DateTime<Utc>>,
    },
    /// Update an already-running saga; `deadline` replaces the previous one
    /// (`None` cancels any pending timeout).
    Advance {
        state: S,
        commands: Vec<C>,
        deadline: Option<DateTime<Utc>>,
    },
    /// Complete the saga. Clears any pending deadline.
    Complete { state: S, commands: Vec<C> },
    /// Fail the saga with a reason, optionally emitting compensating commands.
    /// Clears any pending deadline.
    ///
    /// Unlike a plain workflow abort, a failure here often still has real work
    /// to undo — refund an escrow, release a held reservation, cancel a
    /// booking. Those compensating `commands` ride along and are dispatched by
    /// [`SagaRunner`] exactly as a [`Complete`](Self::Complete)'s are, *before*
    /// the terminal [`SagaRunnerError::Failed`] is surfaced. Pass an empty
    /// `Vec` when failing needs no compensation (an immediate rejection with
    /// nothing to undo, e.g. leaving a matchmaking queue).
    Fail { reason: String, commands: Vec<C> },
}

/// Pure saga state machine.
#[trait_variant::make(Send)]
pub trait Saga: Sync + 'static {
    /// Saga identifier type.
    type Id: Clone + Send + Sync + 'static;
    /// Persisted state machine payload.
    type State: Clone + Send + Sync + 'static;
    /// Event this saga reacts to.
    type Event: Send + Sync + 'static;
    /// Follow-up command emitted by the saga.
    type Command: Clone + Send + Sync + 'static;
    /// User-defined error returned while computing a transition.
    type Error: Error + Send + Sync + 'static;

    /// Extracts the saga id that should handle `event`.
    fn id_for(&self, event: &Self::Event) -> Option<Self::Id>;

    /// Computes the transition for `event`, given the current persisted state.
    async fn react(
        &self,
        state: Option<&SagaInstance<Self::Id, Self::State>>,
        event: &Self::Event,
    ) -> Result<SagaTransition<Self::State, Self::Command>, Self::Error>;

    /// Computes the transition for an elapsed deadline.
    ///
    /// Called by [`SagaRunner::run_due_timeouts`] for running instances whose
    /// [`SagaInstance::deadline`] has passed. The default fails the saga,
    /// which is the safe outcome: the instance is marked
    /// [`SagaStatus::Failed`] and the timeout never refires. Sagas that
    /// compensate on timeout (expire a payment, release a reservation)
    /// override this and return the appropriate transition.
    ///
    /// Kept in the manual `-> impl Future<..> + Send { async { .. } }`
    /// shape rather than `async fn`: `#[trait_variant::make(Send)]` only
    /// rewrites a bodyless method's signature — a provided method's body is
    /// passed through unchanged while its signature's `asyncness` is
    /// stripped, so an `async fn` body would stop being inside an async
    /// context after expansion.
    fn on_timeout(
        &self,
        instance: &SagaInstance<Self::Id, Self::State>,
    ) -> impl Future<Output = Result<SagaTransition<Self::State, Self::Command>, Self::Error>> + Send
    {
        let _ = instance;
        async {
            Ok(SagaTransition::Fail {
                reason: "saga deadline elapsed".to_string(),
                commands: Vec::new(),
            })
        }
    }
}

/// Error returned by [`SagaStore::save`].
///
/// Distinguishes a lost optimistic-concurrency race from an ordinary storage
/// failure — the same split `pharos_core::RepositoryError` draws for
/// aggregates. `pharos-saga` has no dependency on `pharos-core` (each crate
/// in this workspace owns its own boundary error type), so this is a small
/// local echo of that shape rather than a re-export. Without it, `save` had
/// no way to refuse a write whose [`SagaInstance::version`] no longer matched
/// what was stored, so two events racing on the same saga silently
/// overwrote one another instead of one of them failing visibly.
#[derive(Debug, Error)]
pub enum SagaSaveError<E: Error + 'static> {
    /// The version `save` was given no longer matches what is stored:
    /// something else saved this instance first. [`SagaRunner`] reloads and
    /// recomputes the transition against the fresh state before retrying.
    #[error("saga concurrency conflict: expected version {expected}, found {actual:?}")]
    ConcurrencyConflict {
        /// Version the caller's [`SagaInstance`] was loaded at.
        expected: u64,
        /// Version currently stored, when known.
        actual: Option<u64>,
    },
    /// Adapter-specific storage failure.
    #[error(transparent)]
    Storage(E),
}

/// Persistence boundary for saga instances.
#[trait_variant::make(Send)]
pub trait SagaStore<I, S>: Sync + 'static {
    /// Concrete storage error.
    type Error: Error + Send + Sync + 'static;

    /// Loads the current instance for `id`, when it exists.
    async fn load(&self, id: &I) -> Result<Option<SagaInstance<I, S>>, Self::Error>;

    /// Persists an instance, enforcing optimistic concurrency on
    /// [`SagaInstance::version`].
    ///
    /// `instance.version == 0` means "this saga has never been persisted":
    /// implementations must create the row, refusing with
    /// [`SagaSaveError::ConcurrencyConflict`] if one already exists (someone
    /// else created it first). Otherwise implementations must update only
    /// when the stored version still equals `instance.version`, storing
    /// `instance.version + 1`, and refuse with
    /// [`SagaSaveError::ConcurrencyConflict`] otherwise. An implementation
    /// that upserts unconditionally reintroduces the lost-update bug this
    /// type exists to prevent.
    async fn save(&self, instance: SagaInstance<I, S>) -> Result<(), SagaSaveError<Self::Error>>;
}

/// Saga store that can also claim instances with an elapsed deadline.
///
/// Implement this in addition to [`SagaStore`] to drive timeouts through
/// [`SagaRunner::run_due_timeouts`].
#[trait_variant::make(Send)]
pub trait SagaTimeoutStore<I, S>: SagaStore<I, S> {
    /// Atomically claims up to `limit` [`SagaStatus::Running`] instances
    /// whose deadline is at or before `now`, soonest deadline first.
    ///
    /// Claiming must postpone the stored deadline to `now + lease` in the
    /// same operation, so concurrent sweepers never receive the same
    /// instance twice: the claim either wins the row or skips it. The
    /// returned instances carry the **original** (elapsed) deadline, which
    /// is what a timeout handler wants to reason about. If the claimer
    /// crashes before applying a transition, the instance simply becomes
    /// due again once the lease expires.
    ///
    /// Size `lease` comfortably above the worst-case time to process one
    /// sweep batch; a lease that is too short lets another sweeper re-claim
    /// instances that are still being processed, reintroducing duplicate
    /// timeout transitions.
    async fn claim_due(
        &self,
        now: DateTime<Utc>,
        lease: chrono::Duration,
        limit: usize,
    ) -> Result<Vec<SagaInstance<I, S>>, Self::Error>;
}

/// Command dispatch boundary used by the runner.
#[trait_variant::make(Send)]
pub trait CommandDispatcher<C>: Sync + 'static {
    /// Concrete dispatch error.
    type Error: Error + Send + Sync + 'static;

    /// Dispatches one command emitted by a saga.
    async fn dispatch(&self, command: C) -> Result<(), Self::Error>;
}

/// Error returned by [`SagaRunner`].
#[derive(Debug, Error)]
pub enum SagaRunnerError<SE, StoreE>
where
    SE: Error + 'static,
    StoreE: Error + 'static,
{
    /// The saga state machine failed.
    #[error("saga transition failed: {0}")]
    Saga(#[source] SE),
    /// Loading or saving the persisted state failed.
    #[error("saga store failed: {0}")]
    Store(#[source] StoreE),
    /// Enqueuing an emitted command onto the durable outbox failed.
    ///
    /// The saga's own state transition was already persisted by the time
    /// this can happen (see [`SagaRunner::enqueue_all`]'s doc comment for
    /// exactly what that does and does not guarantee). Unlike the old
    /// direct-[`CommandDispatcher`] path, a command that *did* make it into
    /// the outbox before this error is durable and will still be delivered
    /// by an [`OutboxDispatcher`](pharos_messaging::OutboxDispatcher) even
    /// if the whole process crashes right after — this error means the
    /// *insert itself* failed, not that a queued command was lost.
    #[error("enqueuing a command failed: {0}")]
    Enqueue(#[source] OutboxError),
    /// The saga reached [`SagaTransition::Fail`]: a terminal business failure.
    ///
    /// The instance (when one exists) has already been persisted with
    /// [`SagaStatus::Failed`] before this error is returned.
    #[error("saga failed: {reason}")]
    Failed {
        /// Reason supplied by the saga's transition.
        reason: String,
    },
    /// [`SagaStore::save`] lost an optimistic-concurrency race and
    /// [`SagaRunner::handle`]'s bounded retry ([`SagaRunner::MAX_CONFLICT_RETRIES`]
    /// attempts) still could not win it — sustained contention on one saga
    /// instance, not a transient loss.
    #[error(
        "saga concurrency conflict was not resolved after retrying: expected version \
         {expected}, found {actual:?}"
    )]
    Conflict {
        /// Version this attempt expected to find stored.
        expected: u64,
        /// Version actually stored, when known.
        actual: Option<u64>,
    },
}

impl<SE, StoreE> SagaRunnerError<SE, StoreE>
where
    SE: Error + 'static,
    StoreE: Error + 'static,
{
    /// Splits a [`SagaSaveError`] into the matching [`SagaRunnerError`]
    /// variant: a lost race becomes [`Conflict`](Self::Conflict), everything
    /// else becomes [`Store`](Self::Store).
    fn from_save_error(error: SagaSaveError<StoreE>) -> Self {
        match error {
            SagaSaveError::ConcurrencyConflict { expected, actual } => {
                Self::Conflict { expected, actual }
            }
            SagaSaveError::Storage(error) => Self::Store(error),
        }
    }
}

/// Drives a saga end-to-end: load state, react, save, enqueue commands onto
/// a durable outbox.
///
/// Commands are never dispatched directly. `MapCommand` serializes each
/// emitted [`Saga::Command`] into a [`pharos_messaging::Message`], which is
/// inserted into `Outbox` — the same durable-outbox pattern
/// [`pharos_app::save_and_enqueue_in`] uses for domain events. Run a
/// [`pharos_messaging::OutboxDispatcher`] against that same outbox, paired
/// with a [`DurableCommandPublisher`] wrapping your real
/// [`CommandDispatcher`], to actually deliver the commands — which gets the
/// dispatcher's existing retry/backoff/dead-letter machinery for free
/// instead of a saga-specific reimplementation of it.
pub struct SagaRunner<SG, Store, Outbox, MapCommand> {
    saga: SG,
    store: Store,
    outbox: Outbox,
    map_command: MapCommand,
}

impl<SG, Store, Outbox, MapCommand> SagaRunner<SG, Store, Outbox, MapCommand> {
    /// Upper bound on how many times [`handle`](Self::handle) retries a
    /// transition after an optimistic-concurrency conflict before giving up
    /// and returning [`SagaRunnerError::Conflict`]. Bounded so a saga stuck
    /// under sustained contention fails loudly instead of retrying forever.
    pub const MAX_CONFLICT_RETRIES: u32 = 8;

    /// Creates a runner. `map_command` encodes a [`Saga::Command`] into the
    /// [`Message`] its outbox row carries — the same role
    /// [`pharos_app::save_and_enqueue_in`]'s `map_event` plays for domain
    /// events.
    pub fn new(saga: SG, store: Store, outbox: Outbox, map_command: MapCommand) -> Self {
        Self {
            saga,
            store,
            outbox,
            map_command,
        }
    }
}

impl<SG, Store, Outbox, MapCommand> SagaRunner<SG, Store, Outbox, MapCommand>
where
    SG: Saga,
    Store: SagaStore<SG::Id, SG::State>,
    Outbox: OutboxRepository,
    MapCommand: Fn(&SG::Command) -> Message + Send + Sync,
{
    /// Handles an event from start to finish.
    ///
    /// Load → react → save is retried up to [`Self::MAX_CONFLICT_RETRIES`]
    /// times when [`SagaStore::save`] reports a
    /// [`SagaSaveError::ConcurrencyConflict`] — two events for the same saga
    /// instance handled concurrently (two consumers, or a saga event racing a
    /// timeout sweep) load the same state, and without this the second
    /// `save` used to win silently, discarding whatever the first one
    /// computed even though its commands had already been dispatched. A
    /// retry reloads the state the winner actually persisted and recomputes
    /// [`Saga::react`] against it, so the loser's effect is folded in rather
    /// than lost.
    pub async fn handle(
        &self,
        event: &SG::Event,
    ) -> Result<(), SagaRunnerError<SG::Error, Store::Error>> {
        let Some(id) = self.saga.id_for(event) else {
            return Ok(());
        };

        let mut last_conflict = None;
        for _ in 0..Self::MAX_CONFLICT_RETRIES {
            let current = self.store.load(&id).await.map_err(SagaRunnerError::Store)?;
            let transition = self
                .saga
                .react(current.as_ref(), event)
                .await
                .map_err(SagaRunnerError::Saga)?;

            if matches!(transition, SagaTransition::Ignore) {
                return Ok(());
            }

            // A terminal instance must not be revived by a redelivered or
            // late-arriving duplicate just because `react` did not itself
            // check `status` — at-least-once delivery means a duplicate of
            // an event this saga already completed or failed on can still
            // arrive, and `Saga::react` is user-supplied business logic that
            // should not have to re-derive "am I already done?" in every
            // implementation.
            if let Some(instance) = &current
                && !matches!(instance.status, SagaStatus::Running)
            {
                tracing::warn!(
                    saga_status = ?instance.status,
                    "ignoring a non-Ignore transition computed for a saga instance that already \
                     reached a terminal status; a duplicate or late-arriving event cannot revive it"
                );
                return Ok(());
            }

            match self.apply_transition(id.clone(), current, transition).await {
                Ok(()) => return Ok(()),
                Err(SagaRunnerError::Conflict { expected, actual }) => {
                    last_conflict = Some((expected, actual));
                }
                Err(other) => return Err(other),
            }
        }

        let (expected, actual) = last_conflict.unwrap_or((0, None));
        Err(SagaRunnerError::Conflict { expected, actual })
    }

    /// Handles an event that isn't the saga's own [`Saga::Event`] but converts
    /// into it, so one saga instance can react to events from several bounded
    /// contexts.
    ///
    /// A cross-context saga — an escrow that must react to both a wagering
    /// `StakeConfirmed` and a game's `GameEnded`, correlated by a shared id —
    /// defines a single unifying event enum with a `From` impl per source, then
    /// registers one `EventBus` handler per source event type, each forwarding
    /// through `handle_any`. Conversion happens here and the event flows through
    /// the same [`handle`](Self::handle) path; the [`Saga`] trait itself stays
    /// single-event, and the fan-in lives in the wiring rather than in the state
    /// machine.
    ///
    /// ```ignore
    /// // EscrowEvent: From<WagerEvent> + From<GameEvent>
    /// wager_bus.register::<WagerEvent, _>(move |e: &WagerEvent| runner.handle_any(e.clone()));
    /// game_bus.register::<GameEvent, _>(move |e: &GameEvent| runner.handle_any(e.clone()));
    /// ```
    pub async fn handle_any<E>(
        &self,
        event: E,
    ) -> Result<(), SagaRunnerError<SG::Error, Store::Error>>
    where
        E: Into<SG::Event>,
    {
        let event = event.into();
        self.handle(&event).await
    }

    async fn apply_transition(
        &self,
        id: SG::Id,
        current: Option<SagaInstance<SG::Id, SG::State>>,
        transition: SagaTransition<SG::State, SG::Command>,
    ) -> Result<(), SagaRunnerError<SG::Error, Store::Error>> {
        match transition {
            // Ignore is resolved by the caller: a no-op for events, a
            // deadline clear on the timeout path.
            SagaTransition::Ignore => Ok(()),
            SagaTransition::Start {
                state,
                commands,
                deadline,
            } => {
                let mut instance = SagaInstance::running(id, state);
                instance.deadline = deadline;
                self.store
                    .save(instance)
                    .await
                    .map_err(SagaRunnerError::from_save_error)?;
                self.enqueue_all(commands).await
            }
            SagaTransition::Advance {
                state,
                commands,
                deadline,
            } => {
                let mut instance =
                    current.unwrap_or_else(|| SagaInstance::running(id, state.clone()));
                instance.state = state;
                instance.status = SagaStatus::Running;
                instance.deadline = deadline;
                instance.updated_at = Utc::now();
                self.store
                    .save(instance)
                    .await
                    .map_err(SagaRunnerError::from_save_error)?;
                self.enqueue_all(commands).await
            }
            SagaTransition::Complete { state, commands } => {
                let mut instance =
                    current.unwrap_or_else(|| SagaInstance::running(id, state.clone()));
                instance.state = state;
                instance.status = SagaStatus::Completed;
                instance.deadline = None;
                instance.updated_at = Utc::now();
                self.store
                    .save(instance)
                    .await
                    .map_err(SagaRunnerError::from_save_error)?;
                self.enqueue_all(commands).await
            }
            SagaTransition::Fail { reason, commands } => {
                // A saga that fails before any instance was persisted has no
                // state to mark; the error itself is the only record.
                if let Some(mut instance) = current {
                    instance.status = SagaStatus::Failed;
                    instance.deadline = None;
                    instance.updated_at = Utc::now();
                    self.store
                        .save(instance)
                        .await
                        .map_err(SagaRunnerError::from_save_error)?;
                }
                // Compensating commands are enqueued the same way a
                // `Complete`'s are: the saga is terminally failed, but the
                // money (or reservation) it was guarding still has to be moved.
                // Enqueued after persisting the failed state so a crash
                // between the two never leaves a live saga believing it can
                // still act.
                //
                // `store.save` and `enqueue_all` are still two separate
                // awaits, not one transaction: a crash between them leaves
                // the saga terminally `Failed` with its compensating
                // commands never enqueued, and this runner has no way to
                // retry just the enqueue afterwards (a fresh `handle` call
                // for the same event won't re-attempt it either — the
                // already-`Failed` instance short-circuits before `react`
                // runs again, by design, so a duplicate event can't revive
                // a terminal saga). What *is* fixed relative to dispatching
                // straight to a `CommandDispatcher`: once a command clears
                // this `await` — even if the process dies on the very next
                // line — it is durably queued and a pharos_messaging
                // `OutboxDispatcher` will still deliver it, with real retry
                // and backoff before falling to the dead-letter queue. The
                // old direct-dispatch path had no such window at all: a
                // `CommandDispatcher` failure here was simply lost.
                self.enqueue_all(commands).await?;
                Err(SagaRunnerError::Failed { reason })
            }
        }
    }

    /// Fires [`Saga::on_timeout`] for up to `limit` running instances whose
    /// deadline elapsed at `now`, and returns how many were processed.
    ///
    /// Due instances are **claimed** with `lease` (see
    /// [`SagaTimeoutStore::claim_due`]), so this is safe to run on multiple
    /// service instances concurrently: each due saga is delivered to exactly
    /// one sweeper per lease window.
    ///
    /// A [`SagaTransition::Fail`] returned by `on_timeout` is a normal
    /// business outcome here (an expired payment, an abandoned checkout):
    /// the instance is persisted as [`SagaStatus::Failed`] and the sweep
    /// continues. Only saga, store, or dispatch errors abort the sweep;
    /// instances claimed but not yet processed become due again when the
    /// lease expires.
    ///
    /// Pharos provides the mechanism, not the scheduler: call this from a
    /// periodic task in the application, e.g. a `tokio::time::interval` loop.
    pub async fn run_due_timeouts(
        &self,
        now: DateTime<Utc>,
        lease: chrono::Duration,
        limit: usize,
    ) -> Result<usize, SagaRunnerError<SG::Error, Store::Error>>
    where
        Store: SagaTimeoutStore<SG::Id, SG::State>,
    {
        let due = self
            .store
            .claim_due(now, lease, limit)
            .await
            .map_err(SagaRunnerError::Store)?;

        let mut processed = 0;
        for instance in due {
            let transition = self
                .saga
                .on_timeout(&instance)
                .await
                .map_err(SagaRunnerError::Saga)?;

            if matches!(transition, SagaTransition::Ignore) {
                // Clear the elapsed deadline so the timeout never refires.
                let mut instance = instance;
                instance.deadline = None;
                instance.updated_at = Utc::now();
                self.store
                    .save(instance)
                    .await
                    .map_err(SagaRunnerError::from_save_error)?;
                processed += 1;
                continue;
            }

            let id = instance.id.clone();
            match self.apply_transition(id, Some(instance), transition).await {
                Ok(()) => {}
                Err(SagaRunnerError::Failed { reason }) => {
                    tracing::info!(reason, "saga failed on timeout");
                }
                Err(error) => return Err(error),
            }
            processed += 1;
        }
        Ok(processed)
    }

    /// Enqueues `commands` onto the durable outbox as a single batch (via
    /// [`OutboxRepository::insert_many`]), skipping the call entirely when
    /// there is nothing to enqueue.
    ///
    /// This is not itself atomic with the [`SagaStore::save`] that precedes
    /// it in [`apply_transition`](Self::apply_transition) — see that
    /// function's `Fail` arm for exactly what that costs and what it buys
    /// relative to dispatching straight to a [`CommandDispatcher`].
    async fn enqueue_all(
        &self,
        commands: Vec<SG::Command>,
    ) -> Result<(), SagaRunnerError<SG::Error, Store::Error>> {
        if commands.is_empty() {
            return Ok(());
        }
        let messages = commands
            .iter()
            .map(|command| OutboxMessage::new((self.map_command)(command)))
            .collect();
        self.outbox
            .insert_many(messages)
            .await
            .map_err(SagaRunnerError::Enqueue)?;
        Ok(())
    }
}

/// Bridges a durable saga-command outbox to [`CommandDispatcher`].
///
/// [`SagaRunner`] only ever enqueues commands; something has to drain that
/// outbox and actually deliver them. This is that something's `publish`
/// half: implementing [`pharos_messaging::MessagePublisher`] over it lets a
/// standard [`pharos_messaging::OutboxDispatcher`] run against the same
/// outbox [`SagaRunner`] writes to, decoding each queued
/// [`pharos_messaging::Message`] back into a [`Saga::Command`] and handing
/// it to a real [`CommandDispatcher`] — which is how the dispatcher's
/// existing retry/backoff/dead-letter machinery ends up covering saga
/// commands too, instead of `pharos-saga` reimplementing a second copy of
/// it.
///
/// ```ignore
/// let outbox = PostgresOutboxRepository::new(pool.clone());
/// let runner = SagaRunner::new(saga, store, outbox.clone(), map_command);
/// // ... runner.handle(&event).await? enqueues commands onto `outbox` ...
///
/// let publisher = DurableCommandPublisher::new(real_dispatcher, decode_command);
/// let drainer = OutboxDispatcher::new(outbox, publisher);
/// drainer.dispatch_batch().await; // delivers whatever the runner enqueued
/// ```
pub struct DurableCommandPublisher<D, C, F> {
    dispatcher: D,
    decode: F,
    _command: PhantomData<fn() -> C>,
}

impl<D, C, F> DurableCommandPublisher<D, C, F> {
    /// Creates a publisher that decodes each outbox message's payload with
    /// `decode` and hands the result to `dispatcher`.
    pub fn new(dispatcher: D, decode: F) -> Self {
        Self {
            dispatcher,
            decode,
            _command: PhantomData,
        }
    }
}

impl<D, C, F, E> pharos_messaging::MessagePublisher for DurableCommandPublisher<D, C, F>
where
    D: CommandDispatcher<C>,
    C: Send + 'static,
    F: Fn(&[u8]) -> Result<C, E> + Send + Sync + 'static,
    E: Error + Send + Sync + 'static,
{
    /// Decodes `message.payload` back into a [`Saga::Command`] and
    /// dispatches it. A decode failure or a dispatch failure both become a
    /// [`MessagingError::Publish`], so the outbox dispatcher's ordinary
    /// retry/backoff/dead-letter handling applies to either — a command
    /// that can never decode ends up dead-lettered the same way a command a
    /// downstream service permanently rejects would.
    async fn publish(&self, message: Message) -> Result<(), MessagingError> {
        let command = (self.decode)(&message.payload).map_err(MessagingError::publish)?;
        self.dispatcher
            .dispatch(command)
            .await
            .map_err(MessagingError::publish)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::convert::Infallible;
    use std::sync::{Arc, Mutex};

    use super::*;

    #[derive(Clone)]
    struct OrderPlaced {
        order_id: String,
        amount_cents: u32,
    }

    #[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
    enum BillingCommand {
        ReserveFunds { order_id: String, amount_cents: u32 },
        FinalizeOrder { order_id: String },
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    enum BillingState {
        AwaitingReservation { amount_cents: u32 },
        Reserved,
    }

    struct BillingSaga;

    impl Saga for BillingSaga {
        type Id = String;
        type State = BillingState;
        type Event = OrderPlaced;
        type Command = BillingCommand;
        type Error = Infallible;

        fn id_for(&self, event: &Self::Event) -> Option<Self::Id> {
            Some(event.order_id.clone())
        }

        async fn react(
            &self,
            state: Option<&SagaInstance<Self::Id, Self::State>>,
            event: &Self::Event,
        ) -> Result<SagaTransition<Self::State, Self::Command>, Self::Error> {
            Ok(match state {
                None => SagaTransition::Start {
                    state: BillingState::AwaitingReservation {
                        amount_cents: event.amount_cents,
                    },
                    commands: vec![BillingCommand::ReserveFunds {
                        order_id: event.order_id.clone(),
                        amount_cents: event.amount_cents,
                    }],
                    deadline: None,
                },
                Some(_) => SagaTransition::Complete {
                    state: BillingState::Reserved,
                    commands: vec![BillingCommand::FinalizeOrder {
                        order_id: event.order_id.clone(),
                    }],
                },
            })
        }
    }

    #[derive(Default)]
    struct InMemorySagaStore {
        instances: Mutex<HashMap<String, SagaInstance<String, BillingState>>>,
    }

    impl SagaStore<String, BillingState> for InMemorySagaStore {
        type Error = Infallible;

        async fn load(
            &self,
            id: &String,
        ) -> Result<Option<SagaInstance<String, BillingState>>, Self::Error> {
            Ok(self
                .instances
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .get(id)
                .cloned())
        }

        async fn save(
            &self,
            mut instance: SagaInstance<String, BillingState>,
        ) -> Result<(), SagaSaveError<Self::Error>> {
            let mut instances = self.instances.lock().unwrap_or_else(|p| p.into_inner());
            let expected = instance.version;
            let actual = instances.get(&instance.id).map(|stored| stored.version);
            if actual.unwrap_or(0) != expected {
                return Err(SagaSaveError::ConcurrencyConflict { expected, actual });
            }
            instance.version = expected + 1;
            instances.insert(instance.id.clone(), instance);
            Ok(())
        }
    }

    impl SagaTimeoutStore<String, BillingState> for InMemorySagaStore {
        async fn claim_due(
            &self,
            now: DateTime<Utc>,
            lease: chrono::Duration,
            limit: usize,
        ) -> Result<Vec<SagaInstance<String, BillingState>>, Self::Error> {
            let mut instances = self.instances.lock().unwrap_or_else(|p| p.into_inner());
            let mut candidate_ids: Vec<String> = instances
                .values()
                .filter(|i| {
                    i.status == SagaStatus::Running
                        && i.deadline.is_some_and(|deadline| deadline <= now)
                })
                .map(|i| i.id.clone())
                .collect();
            candidate_ids.sort_by_key(|id| instances[id].deadline);
            candidate_ids.truncate(limit);

            // Claim: postpone the stored deadline and bump the version (so a
            // concurrent `save` racing this claim loses the compare-and-swap
            // instead of silently clobbering the lease). The caller must see
            // the original (elapsed) deadline and the post-claim version —
            // the same split the PostgreSQL adapter's `RETURNING` draws
            // between its `due` CTE (pre-claim deadline) and the updated row
            // (post-claim version) — so the clone happens *after* bumping,
            // with the original deadline restored onto it afterward.
            let mut due = Vec::with_capacity(candidate_ids.len());
            for id in candidate_ids {
                if let Some(stored) = instances.get_mut(&id) {
                    let original_deadline = stored.deadline;
                    stored.deadline = Some(now + lease);
                    stored.version += 1;
                    let mut claimed = stored.clone();
                    claimed.deadline = original_deadline;
                    due.push(claimed);
                }
            }
            Ok(due)
        }
    }

    #[derive(Default, Clone)]
    struct VecDispatcher {
        commands: Arc<Mutex<Vec<BillingCommand>>>,
    }

    impl CommandDispatcher<BillingCommand> for VecDispatcher {
        type Error = Infallible;

        async fn dispatch(&self, command: BillingCommand) -> Result<(), Self::Error> {
            self.commands
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .push(command);
            Ok(())
        }
    }

    /// Minimal in-memory [`OutboxRepository`] — just enough of the contract
    /// for [`SagaRunner`] to enqueue into and [`OutboxDispatcher`] to drain,
    /// not a store meant to prove claim/lease correctness under concurrency
    /// (see `pharos-postgres`'s `PostgresOutboxRepository` for that).
    #[derive(Default, Clone)]
    struct TestOutbox {
        messages: Arc<Mutex<HashMap<uuid::Uuid, OutboxMessage>>>,
    }

    impl OutboxRepository for TestOutbox {
        async fn insert(&self, message: OutboxMessage) -> Result<(), OutboxError> {
            self.messages
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .insert(message.id, message);
            Ok(())
        }

        async fn pending(&self, limit: usize) -> Result<Vec<OutboxMessage>, OutboxError> {
            let messages = self.messages.lock().unwrap_or_else(|p| p.into_inner());
            let mut due: Vec<_> = messages
                .values()
                .filter(|m| {
                    m.status == pharos_messaging::OutboxStatus::Pending
                        && m.next_attempt_at <= Utc::now()
                })
                .cloned()
                .collect();
            due.sort_by_key(|m| m.created_at);
            due.truncate(limit);
            Ok(due)
        }

        async fn record_attempt(&self, id: uuid::Uuid) -> Result<(), OutboxError> {
            if let Some(message) = self
                .messages
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .get_mut(&id)
            {
                message.record_attempt();
            }
            Ok(())
        }

        async fn mark_published(&self, id: uuid::Uuid) -> Result<(), OutboxError> {
            if let Some(message) = self
                .messages
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .get_mut(&id)
            {
                message.mark_published();
            }
            Ok(())
        }

        async fn mark_failed(&self, id: uuid::Uuid, error: String) -> Result<(), OutboxError> {
            if let Some(message) = self
                .messages
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .get_mut(&id)
            {
                message.mark_failed(error);
            }
            Ok(())
        }

        async fn failed(&self, limit: usize) -> Result<Vec<OutboxMessage>, OutboxError> {
            let messages = self.messages.lock().unwrap_or_else(|p| p.into_inner());
            let mut failed: Vec<_> = messages
                .values()
                .filter(|m| m.status == pharos_messaging::OutboxStatus::Failed)
                .cloned()
                .collect();
            failed.truncate(limit);
            Ok(failed)
        }

        async fn mark_dead_lettered(&self, id: uuid::Uuid) -> Result<(), OutboxError> {
            if let Some(message) = self
                .messages
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .get_mut(&id)
            {
                message.status = pharos_messaging::OutboxStatus::DeadLettered;
            }
            Ok(())
        }
    }

    fn map_billing_command(command: &BillingCommand) -> Message {
        let Ok(payload) = serde_json::to_vec(command) else {
            panic!("BillingCommand must always serialize");
        };
        Message::new("billing-commands", payload, "application/json")
    }

    fn decode_billing_command(payload: &[u8]) -> Result<BillingCommand, serde_json::Error> {
        serde_json::from_slice(payload)
    }

    /// Drains `outbox` through a [`DurableCommandPublisher`] wrapping
    /// `dispatcher` and returns whatever `dispatcher` collected — the same
    /// shape the pre-durable-outbox tests asserted on directly, but now
    /// exercising the real path end to end: [`SagaRunner::enqueue_all`] ->
    /// [`pharos_messaging::OutboxDispatcher::dispatch_batch`] ->
    /// [`CommandDispatcher::dispatch`].
    async fn drain(outbox: TestOutbox, dispatcher: VecDispatcher) -> Vec<BillingCommand> {
        let publisher = DurableCommandPublisher::new(dispatcher.clone(), decode_billing_command);
        let outbox_dispatcher = pharos_messaging::OutboxDispatcher::new(outbox, publisher);
        let result = outbox_dispatcher.dispatch_batch().await;
        assert!(
            result.is_ok(),
            "dispatch_batch must not fail in this test: {:?}",
            result.errors
        );
        dispatcher
            .commands
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone()
    }

    struct AlwaysFailingSaga;

    impl Saga for AlwaysFailingSaga {
        type Id = String;
        type State = BillingState;
        type Event = OrderPlaced;
        type Command = BillingCommand;
        type Error = Infallible;

        fn id_for(&self, event: &Self::Event) -> Option<Self::Id> {
            Some(event.order_id.clone())
        }

        async fn react(
            &self,
            _state: Option<&SagaInstance<Self::Id, Self::State>>,
            _event: &Self::Event,
        ) -> Result<SagaTransition<Self::State, Self::Command>, Self::Error> {
            Ok(SagaTransition::Fail {
                reason: "funds could not be reserved".to_string(),
                commands: Vec::new(),
            })
        }
    }

    #[tokio::test]
    async fn fail_transition_returns_error_instead_of_panicking()
    -> Result<(), Box<dyn std::error::Error>> {
        let runner = SagaRunner::new(
            AlwaysFailingSaga,
            InMemorySagaStore::default(),
            TestOutbox::default(),
            map_billing_command,
        );
        let event = OrderPlaced {
            order_id: "order-9".into(),
            amount_cents: 100,
        };

        let Err(SagaRunnerError::Failed { reason }) = runner.handle(&event).await else {
            panic!("expected SagaRunnerError::Failed");
        };
        assert_eq!(reason, "funds could not be reserved");
        Ok(())
    }

    #[tokio::test]
    async fn fail_transition_marks_existing_instance_as_failed()
    -> Result<(), Box<dyn std::error::Error>> {
        let store = InMemorySagaStore::default();
        store
            .save(SagaInstance::running(
                "order-9".to_string(),
                BillingState::AwaitingReservation { amount_cents: 100 },
            ))
            .await?;
        let runner = SagaRunner::new(
            AlwaysFailingSaga,
            store,
            TestOutbox::default(),
            map_billing_command,
        );
        let event = OrderPlaced {
            order_id: "order-9".into(),
            amount_cents: 100,
        };

        assert!(matches!(
            runner.handle(&event).await,
            Err(SagaRunnerError::Failed { .. })
        ));
        let stored = runner
            .store
            .instances
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .get("order-9")
            .cloned()
            .ok_or("instance must exist")?;
        assert_eq!(stored.status, SagaStatus::Failed);
        Ok(())
    }

    /// Saga that fails but emits a compensating command on the way out — the
    /// shape an escrow uses to refund stakes on an abandoned wager.
    struct RefundingSaga;

    impl Saga for RefundingSaga {
        type Id = String;
        type State = BillingState;
        type Event = OrderPlaced;
        type Command = BillingCommand;
        type Error = Infallible;

        fn id_for(&self, event: &Self::Event) -> Option<Self::Id> {
            Some(event.order_id.clone())
        }

        async fn react(
            &self,
            _state: Option<&SagaInstance<Self::Id, Self::State>>,
            event: &Self::Event,
        ) -> Result<SagaTransition<Self::State, Self::Command>, Self::Error> {
            Ok(SagaTransition::Fail {
                reason: "wager abandoned".to_string(),
                commands: vec![BillingCommand::FinalizeOrder {
                    order_id: event.order_id.clone(),
                }],
            })
        }
    }

    #[tokio::test]
    async fn fail_transition_dispatches_compensating_commands()
    -> Result<(), Box<dyn std::error::Error>> {
        let store = InMemorySagaStore::default();
        store
            .save(SagaInstance::running(
                "order-42".to_string(),
                BillingState::AwaitingReservation { amount_cents: 500 },
            ))
            .await?;
        let outbox = TestOutbox::default();
        let dispatcher = VecDispatcher::default();
        let runner = SagaRunner::new(RefundingSaga, store, outbox.clone(), map_billing_command);
        let event = OrderPlaced {
            order_id: "order-42".into(),
            amount_cents: 500,
        };

        // The saga still reports terminal failure to the caller...
        assert!(matches!(
            runner.handle(&event).await,
            Err(SagaRunnerError::Failed { .. })
        ));
        // ...the instance is persisted as failed...
        let stored = runner
            .store
            .load(&"order-42".to_string())
            .await?
            .ok_or("instance must exist")?;
        assert_eq!(stored.status, SagaStatus::Failed);
        // ...and the compensating command was durably enqueued despite the
        // failure, and reaches the dispatcher once drained.
        let commands = drain(outbox, dispatcher).await;
        assert_eq!(
            commands,
            vec![BillingCommand::FinalizeOrder {
                order_id: "order-42".into(),
            }]
        );
        Ok(())
    }

    /// Saga that refunds on timeout — an escrow whose funded game is abandoned
    /// past its deadline. Exercises `Fail { commands }` on the sweep path.
    struct RefundOnTimeoutSaga;

    impl Saga for RefundOnTimeoutSaga {
        type Id = String;
        type State = BillingState;
        type Event = OrderPlaced;
        type Command = BillingCommand;
        type Error = Infallible;

        fn id_for(&self, event: &Self::Event) -> Option<Self::Id> {
            Some(event.order_id.clone())
        }

        async fn react(
            &self,
            _state: Option<&SagaInstance<Self::Id, Self::State>>,
            _event: &Self::Event,
        ) -> Result<SagaTransition<Self::State, Self::Command>, Self::Error> {
            Ok(SagaTransition::Ignore)
        }

        async fn on_timeout(
            &self,
            instance: &SagaInstance<Self::Id, Self::State>,
        ) -> Result<SagaTransition<Self::State, Self::Command>, Self::Error> {
            Ok(SagaTransition::Fail {
                reason: "game abandoned".to_string(),
                commands: vec![BillingCommand::FinalizeOrder {
                    order_id: instance.id.clone(),
                }],
            })
        }
    }

    #[tokio::test]
    async fn timeout_fail_dispatches_compensation_and_continues_sweep()
    -> Result<(), Box<dyn std::error::Error>> {
        let store = InMemorySagaStore::default();
        store.save(expired_instance("order-77")).await?;
        let outbox = TestOutbox::default();
        let dispatcher = VecDispatcher::default();
        let runner = SagaRunner::new(
            RefundOnTimeoutSaga,
            store,
            outbox.clone(),
            map_billing_command,
        );

        // A timeout `Fail` is a normal business outcome on the sweep path: the
        // instance is counted as processed, not propagated as an error.
        assert_eq!(
            runner
                .run_due_timeouts(Utc::now(), chrono::Duration::minutes(1), 10)
                .await?,
            1
        );

        let stored = runner
            .store
            .load(&"order-77".to_string())
            .await?
            .ok_or("instance must exist")?;
        assert_eq!(stored.status, SagaStatus::Failed);
        assert_eq!(stored.deadline, None);

        // The refund command was enqueued even though the transition failed
        // the saga, and reaches the dispatcher once drained.
        let commands = drain(outbox, dispatcher).await;
        assert_eq!(
            commands,
            vec![BillingCommand::FinalizeOrder {
                order_id: "order-77".into(),
            }]
        );

        // Terminal: nothing is due on a second sweep.
        assert_eq!(
            runner
                .run_due_timeouts(Utc::now(), chrono::Duration::minutes(1), 10)
                .await?,
            0
        );
        Ok(())
    }

    /// A foreign event from another bounded context that folds into the saga's
    /// own event type via `From` — the cross-context fan-in `handle_any` serves.
    struct DepositConfirmed {
        order_id: String,
        amount_cents: u32,
    }

    impl From<DepositConfirmed> for OrderPlaced {
        fn from(value: DepositConfirmed) -> Self {
            OrderPlaced {
                order_id: value.order_id,
                amount_cents: value.amount_cents,
            }
        }
    }

    #[tokio::test]
    async fn handle_any_converts_foreign_events_into_the_saga_event()
    -> Result<(), Box<dyn std::error::Error>> {
        let store = InMemorySagaStore::default();
        let outbox = TestOutbox::default();
        let dispatcher = VecDispatcher::default();
        let runner = SagaRunner::new(BillingSaga, store, outbox.clone(), map_billing_command);

        // The saga's own Event is `OrderPlaced`; `handle_any` accepts anything
        // that converts into it, so a handler registered for a different
        // context's event type drives the same instance.
        runner
            .handle_any(DepositConfirmed {
                order_id: "order-7".into(),
                amount_cents: 900,
            })
            .await?;

        let commands = drain(outbox, dispatcher).await;
        assert_eq!(
            commands,
            vec![BillingCommand::ReserveFunds {
                order_id: "order-7".into(),
                amount_cents: 900,
            }]
        );
        Ok(())
    }

    /// Saga that compensates on timeout: emits a cancel command and completes.
    struct ExpiringSaga;

    impl Saga for ExpiringSaga {
        type Id = String;
        type State = BillingState;
        type Event = OrderPlaced;
        type Command = BillingCommand;
        type Error = Infallible;

        fn id_for(&self, event: &Self::Event) -> Option<Self::Id> {
            Some(event.order_id.clone())
        }

        async fn react(
            &self,
            _state: Option<&SagaInstance<Self::Id, Self::State>>,
            _event: &Self::Event,
        ) -> Result<SagaTransition<Self::State, Self::Command>, Self::Error> {
            Ok(SagaTransition::Ignore)
        }

        async fn on_timeout(
            &self,
            instance: &SagaInstance<Self::Id, Self::State>,
        ) -> Result<SagaTransition<Self::State, Self::Command>, Self::Error> {
            Ok(SagaTransition::Complete {
                state: instance.state.clone(),
                commands: vec![BillingCommand::FinalizeOrder {
                    order_id: instance.id.clone(),
                }],
            })
        }
    }

    /// Saga whose timeout is not interesting: the runner must clear the
    /// deadline so it never refires.
    struct SnoozingSaga;

    impl Saga for SnoozingSaga {
        type Id = String;
        type State = BillingState;
        type Event = OrderPlaced;
        type Command = BillingCommand;
        type Error = Infallible;

        fn id_for(&self, event: &Self::Event) -> Option<Self::Id> {
            Some(event.order_id.clone())
        }

        async fn react(
            &self,
            _state: Option<&SagaInstance<Self::Id, Self::State>>,
            _event: &Self::Event,
        ) -> Result<SagaTransition<Self::State, Self::Command>, Self::Error> {
            Ok(SagaTransition::Ignore)
        }

        async fn on_timeout(
            &self,
            _instance: &SagaInstance<Self::Id, Self::State>,
        ) -> Result<SagaTransition<Self::State, Self::Command>, Self::Error> {
            Ok(SagaTransition::Ignore)
        }
    }

    fn expired_instance(id: &str) -> SagaInstance<String, BillingState> {
        SagaInstance::running_until(
            id.to_string(),
            BillingState::AwaitingReservation { amount_cents: 100 },
            Utc::now() - chrono::Duration::minutes(5),
        )
    }

    #[tokio::test]
    async fn default_on_timeout_fails_the_saga_and_clears_the_deadline()
    -> Result<(), Box<dyn std::error::Error>> {
        let store = InMemorySagaStore::default();
        store.save(expired_instance("order-1")).await?;
        let runner = SagaRunner::new(
            BillingSaga,
            store,
            TestOutbox::default(),
            map_billing_command,
        );

        let processed = runner
            .run_due_timeouts(Utc::now(), chrono::Duration::minutes(1), 10)
            .await?;
        assert_eq!(processed, 1);

        let stored = runner
            .store
            .load(&"order-1".to_string())
            .await?
            .ok_or("instance must exist")?;
        assert_eq!(stored.status, SagaStatus::Failed);
        assert_eq!(stored.deadline, None);

        // The failed instance is terminal: nothing is due anymore.
        assert_eq!(
            runner
                .run_due_timeouts(Utc::now(), chrono::Duration::minutes(1), 10)
                .await?,
            0
        );
        Ok(())
    }

    #[tokio::test]
    async fn on_timeout_override_compensates_and_completes()
    -> Result<(), Box<dyn std::error::Error>> {
        let store = InMemorySagaStore::default();
        store.save(expired_instance("order-2")).await?;
        let outbox = TestOutbox::default();
        let dispatcher = VecDispatcher::default();
        let runner = SagaRunner::new(ExpiringSaga, store, outbox.clone(), map_billing_command);

        assert_eq!(
            runner
                .run_due_timeouts(Utc::now(), chrono::Duration::minutes(1), 10)
                .await?,
            1
        );

        let stored = runner
            .store
            .load(&"order-2".to_string())
            .await?
            .ok_or("instance must exist")?;
        assert_eq!(stored.status, SagaStatus::Completed);
        assert_eq!(stored.deadline, None);
        let commands = drain(outbox, dispatcher).await;
        assert_eq!(
            commands,
            vec![BillingCommand::FinalizeOrder {
                order_id: "order-2".into(),
            }]
        );
        Ok(())
    }

    #[tokio::test]
    async fn ignored_timeout_clears_the_deadline_and_keeps_running()
    -> Result<(), Box<dyn std::error::Error>> {
        let store = InMemorySagaStore::default();
        store.save(expired_instance("order-3")).await?;
        let runner = SagaRunner::new(
            SnoozingSaga,
            store,
            TestOutbox::default(),
            map_billing_command,
        );

        assert_eq!(
            runner
                .run_due_timeouts(Utc::now(), chrono::Duration::minutes(1), 10)
                .await?,
            1
        );

        let stored = runner
            .store
            .load(&"order-3".to_string())
            .await?
            .ok_or("instance must exist")?;
        assert_eq!(stored.status, SagaStatus::Running);
        assert_eq!(stored.deadline, None);
        assert_eq!(
            runner
                .run_due_timeouts(Utc::now(), chrono::Duration::minutes(1), 10)
                .await?,
            0
        );
        Ok(())
    }

    #[tokio::test]
    async fn run_due_timeouts_respects_future_deadlines_and_limit()
    -> Result<(), Box<dyn std::error::Error>> {
        let store = InMemorySagaStore::default();
        store.save(expired_instance("order-4")).await?;
        store.save(expired_instance("order-5")).await?;
        store
            .save(SagaInstance::running_until(
                "order-future".to_string(),
                BillingState::Reserved,
                Utc::now() + chrono::Duration::hours(1),
            ))
            .await?;
        let runner = SagaRunner::new(
            BillingSaga,
            store,
            TestOutbox::default(),
            map_billing_command,
        );

        // Only one of the two due instances fits the limit.
        assert_eq!(
            runner
                .run_due_timeouts(Utc::now(), chrono::Duration::minutes(1), 1)
                .await?,
            1
        );
        assert_eq!(
            runner
                .run_due_timeouts(Utc::now(), chrono::Duration::minutes(1), 10)
                .await?,
            1
        );

        // The future deadline stays untouched.
        let future = runner
            .store
            .load(&"order-future".to_string())
            .await?
            .ok_or("instance must exist")?;
        assert_eq!(future.status, SagaStatus::Running);
        assert!(future.deadline.is_some());
        Ok(())
    }

    #[tokio::test]
    async fn runner_starts_and_then_completes_a_saga() -> Result<(), Box<dyn std::error::Error>> {
        let store = InMemorySagaStore::default();
        let outbox = TestOutbox::default();
        let dispatcher = VecDispatcher::default();
        let runner = SagaRunner::new(BillingSaga, store, outbox.clone(), map_billing_command);

        let event = OrderPlaced {
            order_id: "order-1".into(),
            amount_cents: 1500,
        };

        runner.handle(&event).await?;
        runner.handle(&event).await?;

        let commands = drain(outbox, dispatcher).await;
        assert_eq!(
            commands,
            vec![
                BillingCommand::ReserveFunds {
                    order_id: "order-1".into(),
                    amount_cents: 1500,
                },
                BillingCommand::FinalizeOrder {
                    order_id: "order-1".into(),
                },
            ]
        );
        Ok(())
    }

    #[derive(Debug, thiserror::Error)]
    #[error("dispatch failed transiently")]
    struct TransientDispatchFailure;

    /// Dispatcher whose first `dispatch` call always fails, every later call
    /// succeeds. Standing in for a downstream service that was briefly
    /// unreachable — the exact case a direct `CommandDispatcher` call had no
    /// recovery from at all: the old `SagaRunner` surfaced the error once
    /// and moved on, the command gone.
    #[derive(Default, Clone)]
    struct FlakyOnceDispatcher {
        failed_once: Arc<Mutex<bool>>,
        commands: Arc<Mutex<Vec<BillingCommand>>>,
    }

    impl CommandDispatcher<BillingCommand> for FlakyOnceDispatcher {
        type Error = TransientDispatchFailure;

        async fn dispatch(&self, command: BillingCommand) -> Result<(), Self::Error> {
            let mut failed_once = self.failed_once.lock().unwrap_or_else(|p| p.into_inner());
            if !*failed_once {
                *failed_once = true;
                return Err(TransientDispatchFailure);
            }
            self.commands
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .push(command);
            Ok(())
        }
    }

    /// The property this whole durable-outbox design exists for: a command
    /// whose first delivery attempt fails is retried by the
    /// `OutboxDispatcher`'s own `RetryPolicy`, not lost. Dispatching straight
    /// to a `CommandDispatcher` (the pre-durable-outbox shape) had no such
    /// recovery — a failed `dispatch` call surfaced once as an error and the
    /// command was gone.
    #[tokio::test]
    async fn a_transient_dispatch_failure_is_retried_instead_of_lost()
    -> Result<(), Box<dyn std::error::Error>> {
        let outbox = TestOutbox::default();
        let store = InMemorySagaStore::default();
        let runner = SagaRunner::new(BillingSaga, store, outbox.clone(), map_billing_command);

        runner
            .handle(&OrderPlaced {
                order_id: "order-99".into(),
                amount_cents: 250,
            })
            .await?;

        let dispatcher = FlakyOnceDispatcher::default();
        let publisher = DurableCommandPublisher::new(dispatcher.clone(), decode_billing_command);
        // Zero delay: the second `dispatch_batch` call sees the retried
        // message immediately due, no sleep needed in the test.
        let outbox_dispatcher = pharos_messaging::OutboxDispatcher::with_config(
            outbox,
            publisher,
            pharos_messaging::DispatchConfig::new(
                10,
                pharos_messaging::RetryPolicy::new(3, std::time::Duration::ZERO),
            ),
        );

        let first = outbox_dispatcher.dispatch_batch().await;
        assert_eq!(
            first.failure_count(),
            1,
            "the first delivery attempt must fail, exercising the flaky dispatcher"
        );
        assert!(
            dispatcher
                .commands
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .is_empty(),
            "nothing reached the dispatcher's log on the failed attempt"
        );

        let second = outbox_dispatcher.dispatch_batch().await;
        assert!(
            second.is_ok(),
            "the retried attempt must succeed: {:?}",
            second.errors
        );
        assert_eq!(
            *dispatcher
                .commands
                .lock()
                .unwrap_or_else(|p| p.into_inner()),
            vec![BillingCommand::ReserveFunds {
                order_id: "order-99".into(),
                amount_cents: 250,
            }],
            "the command that failed once is delivered exactly once on retry, not dropped"
        );
        Ok(())
    }
}
