use std::fmt::Display;
use std::future::Future;
use std::pin::Pin;

use chrono::Utc;
use pharos_app::{Message, OutboxMessage, UnitOfWorkError};
use pharos_core::{AggregateRoot, Entity, RepositoryError};
use serde::Serialize;
use sqlx::{PgConnection, Row};
use thiserror::Error;
use tracing::{Instrument, info_span};

use crate::pool::Pool;

/// A repository whose `save` can run inside a caller-provided transaction.
///
/// This is the PostgreSQL transactional-composition contract: implement it in
/// addition to `Repository<A>` and [`save_and_enqueue_in`] gives your aggregate
/// the atomic save+outbox guarantee — whether it persists as JSONB
/// ([`PostgresJsonRepository`](crate::PostgresJsonRepository) implements this)
/// or as explicit normalized tables (implement `save_in_tx` with your own SQL).
///
/// # Contract
///
/// `save_in_tx` must enforce optimistic concurrency exactly like
/// `Repository::save`: check the expected version, advance the aggregate's
/// in-memory version on success, and return
/// [`RepositoryError::ConcurrencyConflict`] on a stale write **without**
/// mutating rows. It must not begin, commit, or roll back the transaction —
/// the composing caller owns the boundary (and reverts the in-memory version
/// if the surrounding transaction later fails).
pub trait TransactionalRepository<A: AggregateRoot>: Send + Sync {
    /// The repository-specific storage error type.
    type Error: std::error::Error + Send + Sync + 'static;

    /// Persists the aggregate using the caller's live transaction connection.
    fn save_in_tx<'c>(
        &'c self,
        conn: &'c mut PgConnection,
        aggregate: &'c mut A,
    ) -> impl Future<Output = Result<(), RepositoryError<Self::Error>>> + Send + 'c;
}

/// Error returned by [`save_and_enqueue_in`].
#[derive(Debug, Error)]
pub enum SaveAndEnqueueError<E: std::error::Error> {
    /// The repository failed to persist the aggregate (including
    /// optimistic-concurrency conflicts).
    #[error(transparent)]
    Repository(RepositoryError<E>),
    /// Opening/committing the transaction or writing the outbox failed.
    #[error(transparent)]
    Transaction(PostgresTransactionError),
}

/// Persists an aggregate through any [`TransactionalRepository`] and enqueues
/// its pending events as outbox messages, atomically.
///
/// This is the production write path: one `BEGIN … COMMIT` covers the
/// aggregate rows *and* the outbox inserts, so either both become visible or
/// neither does. Works identically for the JSONB repository and for explicit
/// relational repositories.
///
/// On any failure the aggregate's in-memory state is left intact: the version
/// is reverted and the pending events are kept, so a retry starts clean.
/// Events are drained only after the commit succeeds.
pub async fn save_and_enqueue_in<A, R, F>(
    pool: &Pool,
    repo: &R,
    aggregate: &mut A,
    map_event: F,
) -> Result<(), SaveAndEnqueueError<R::Error>>
where
    A: AggregateRoot,
    R: TransactionalRepository<A>,
    F: Fn(&A::Event) -> Message + Send + Sync,
{
    let aggregate_type = std::any::type_name::<A>();
    let expected = aggregate.version();

    // Build the outbox messages from the still-pending events; they are only
    // drained after the transaction commits.
    let messages: Vec<OutboxMessage> = aggregate
        .pending_events()
        .iter()
        .map(|e| OutboxMessage::new(map_event(e)))
        .collect();

    let result = async {
        let mut tx = pool.begin().await.map_err(|e| {
            SaveAndEnqueueError::Transaction(PostgresTransactionError::Transaction(e))
        })?;

        repo.save_in_tx(&mut tx, aggregate)
            .await
            .map_err(SaveAndEnqueueError::Repository)?;

        for message in &messages {
            insert_outbox_in_tx(&mut tx, message)
                .await
                .map_err(SaveAndEnqueueError::Transaction)?;
        }

        tx.commit().await.map_err(|e| {
            SaveAndEnqueueError::Transaction(PostgresTransactionError::Transaction(e))
        })?;

        metrics::counter!(
            "pharos.postgres.save_and_enqueue.committed",
            "aggregate_type" => aggregate_type.to_string()
        )
        .increment(messages.len() as u64 + 1);
        Ok(())
    }
    .instrument(info_span!("postgres.save_and_enqueue_in", aggregate_type))
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

/// A connection-pooled unit of work backed by a real PostgreSQL transaction.
///
/// The closure receives `&mut PgConnection` which is the live connection inside
/// the transaction. All sqlx queries executed through it participate in the same
/// transaction and commit or roll back atomically.
#[derive(Debug, Clone)]
pub struct PostgresUnitOfWork {
    pool: Pool,
}

impl PostgresUnitOfWork {
    pub fn new(pool: Pool) -> Self {
        Self { pool }
    }
    pub fn pool(&self) -> &Pool {
        &self.pool
    }

    /// Runs `work` inside a single database transaction.
    ///
    /// Commits on `Ok`, rolls back on `Err`. The closure receives a
    /// `&mut PgConnection` that can be passed to any `sqlx` query:
    ///
    /// ```no_run
    /// # use pharos_postgres::{PostgresUnitOfWork, connect_pool};
    /// # async fn run() -> Result<(), Box<dyn std::error::Error>> {
    /// let pool = connect_pool("postgres://postgres@localhost/app", 8)?;
    /// let uow = PostgresUnitOfWork::new(pool);
    /// uow.transaction(|conn| {
    ///     Box::pin(async move {
    ///         sqlx::query("INSERT INTO ledger (amount) VALUES ($1)")
    ///             .bind(10i64)
    ///             .execute(conn)
    ///             .await?;
    ///         Ok::<(), sqlx::Error>(())
    ///     })
    /// })
    /// .await?;
    /// # Ok(()) }
    /// ```
    pub async fn transaction<T, E, F>(&self, work: F) -> Result<T, UnitOfWorkError>
    where
        F: for<'c> FnOnce(
                &'c mut PgConnection,
            ) -> Pin<Box<dyn Future<Output = Result<T, E>> + Send + 'c>>
            + Send,
        T: Send,
        E: std::error::Error + Send + Sync + 'static,
    {
        async move {
            let mut tx = self.pool.begin().await.map_err(UnitOfWorkError::begin)?;

            match work(&mut tx).await {
                Ok(value) => {
                    tx.commit().await.map_err(UnitOfWorkError::commit)?;
                    metrics::counter!("pharos.postgres.uow.committed").increment(1);
                    Ok(value)
                }
                Err(error) => {
                    tx.rollback().await.ok();
                    metrics::counter!("pharos.postgres.uow.rolled_back").increment(1);
                    Err(UnitOfWorkError::operation(error))
                }
            }
        }
        .instrument(info_span!("postgres.uow.transaction"))
        .await
    }
}

/// Error produced by the atomic save-and-enqueue helpers.
///
/// Variants keep the originating error as a typed `source` so callers can
/// inspect the real failure instead of matching on strings.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum PostgresTransactionError {
    #[error("transaction failed: {0}")]
    Transaction(#[source] sqlx::Error),
    #[error("postgres statement failed: {0}")]
    Storage(#[source] sqlx::Error),
    #[error("aggregate serialization failed: {0}")]
    Serialization(#[from] serde_json::Error),
    #[error("optimistic concurrency conflict: expected version {expected}, found {actual:?}")]
    ConcurrencyConflict { expected: u64, actual: Option<u64> },
}

impl From<sqlx::Error> for PostgresTransactionError {
    fn from(e: sqlx::Error) -> Self {
        PostgresTransactionError::Transaction(e)
    }
}

/// Persists an aggregate and enqueues its pending events in one transaction.
///
/// This helper targets the JSONB [`PostgresJsonRepository`] table
/// (`pharos_aggregates`). For aggregates persisted through an **explicit
/// relational repository** — the recommended production direction — compose
/// the same guarantee with [`PostgresUnitOfWork::transaction`] and
/// [`insert_outbox_in_tx`]:
///
/// ```ignore
/// let uow = PostgresUnitOfWork::new(pool.clone());
/// let messages: Vec<OutboxMessage> = order
///     .pending_events()
///     .iter()
///     .map(|e| OutboxMessage::new(map_event(e)))
///     .collect();
///
/// uow.transaction(|conn| {
///     Box::pin(async move {
///         // your explicit SQL for the aggregate's normalized tables
///         save_order_rows(conn, &order_rows).await?;
///         for message in &messages {
///             insert_outbox_in_tx(conn, message).await?;
///         }
///         Ok::<(), PostgresTransactionError>(())
///     })
/// })
/// .await?;
/// order.drain_events(); // only after the transaction committed
/// ```
///
/// [`PostgresJsonRepository`]: crate::PostgresJsonRepository
pub async fn save_aggregate_and_enqueue<A, F>(
    pool: &Pool,
    aggregate_type: &str,
    aggregate: &mut A,
    map_event: F,
) -> Result<(), PostgresTransactionError>
where
    A: AggregateRoot + Serialize + Send + Sync,
    <A as Entity>::Id: Display,
    F: Fn(&A::Event) -> Message + Send,
{
    let aggregate_id = aggregate.id().to_string();
    let expected = aggregate.version();
    let new_version = expected + 1;

    // Serialize with the new version already applied so the stored payload
    // matches what a future load will observe (same contract as
    // `PostgresJsonRepository::save`); revert it on every failure path so a
    // failed call leaves the in-memory aggregate untouched.
    aggregate.set_version(new_version);
    let payload = serde_json::to_string(&*aggregate).map_err(|e| {
        aggregate.set_version(expected);
        PostgresTransactionError::Serialization(e)
    })?;

    // Build the outbox messages from the still-pending events; they are only
    // drained after the transaction commits, so a failed save (e.g. a
    // concurrency conflict) never discards them.
    let messages: Vec<OutboxMessage> = aggregate
        .pending_events()
        .iter()
        .map(|e| OutboxMessage::new(map_event(e)))
        .collect();

    let result = async {
        let mut tx = pool
            .begin()
            .await
            .map_err(PostgresTransactionError::Transaction)?;

        save_aggregate_in_tx(
            &mut tx,
            aggregate_type,
            &aggregate_id,
            &payload,
            expected,
            new_version,
        )
        .await?;

        for message in &messages {
            insert_outbox_in_tx(&mut tx, message).await?;
        }

        tx.commit()
            .await
            .map_err(PostgresTransactionError::Transaction)?;

        metrics::counter!(
            "pharos.postgres.save_and_enqueue.committed",
            "aggregate_type" => aggregate_type.to_string()
        )
        .increment(messages.len() as u64 + 1);
        Ok(())
    }
    .instrument(info_span!(
        "postgres.save_aggregate_and_enqueue",
        aggregate_type
    ))
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

/// Writes an aggregate payload with OCC inside an existing transaction.
///
/// Pass `&mut *tx` where `tx: sqlx::Transaction<'_, sqlx::Postgres>`, or pass
/// the `&mut PgConnection` from [`PostgresUnitOfWork::transaction`].
pub async fn save_aggregate_in_tx(
    conn: &mut PgConnection,
    aggregate_type: &str,
    aggregate_id: &str,
    payload: &str,
    expected_version: u64,
    new_version: u64,
) -> Result<(), PostgresTransactionError> {
    let now = Utc::now();
    let affected = if expected_version == 0 {
        sqlx::query(
            "INSERT INTO pharos_aggregates
                (aggregate_type, aggregate_id, payload, version, updated_at)
             VALUES ($1, $2, $3::jsonb, $4, $5)
             ON CONFLICT (aggregate_type, aggregate_id) DO NOTHING",
        )
        .bind(aggregate_type)
        .bind(aggregate_id)
        .bind(payload)
        .bind(new_version as i64)
        .bind(now)
        .execute(&mut *conn)
        .await
    } else {
        sqlx::query(
            "UPDATE pharos_aggregates
             SET payload = $3::jsonb, version = $4, updated_at = $5
             WHERE aggregate_type = $1 AND aggregate_id = $2 AND version = $6",
        )
        .bind(aggregate_type)
        .bind(aggregate_id)
        .bind(payload)
        .bind(new_version as i64)
        .bind(now)
        .bind(expected_version as i64)
        .execute(&mut *conn)
        .await
    }
    .map_err(PostgresTransactionError::Storage)?
    .rows_affected();

    if affected == 0 {
        let actual = stored_version_in_tx(conn, aggregate_type, aggregate_id).await?;
        return Err(PostgresTransactionError::ConcurrencyConflict {
            expected: expected_version,
            actual,
        });
    }
    Ok(())
}

/// Inserts a pending outbox message inside an existing transaction.
pub async fn insert_outbox_in_tx(
    conn: &mut PgConnection,
    message: &OutboxMessage,
) -> Result<(), PostgresTransactionError> {
    let headers = serde_json::to_value(&message.message.headers)
        .map_err(PostgresTransactionError::Serialization)?;
    sqlx::query(
        "INSERT INTO pharos_outbox (
            id, message_id, topic, message_key, headers, payload, content_type,
            status, attempts, created_at, updated_at, next_attempt_at, last_error
        ) VALUES ($1,$2,$3,$4,$5,$6,$7,'pending',$8,$9,$10,$11,NULL)",
    )
    .bind(message.id)
    .bind(message.message.message_id)
    .bind(&message.message.topic)
    .bind(&message.message.key)
    .bind(sqlx::types::Json(&headers))
    .bind(&message.message.payload)
    .bind(&message.message.content_type)
    .bind(message.attempts as i32)
    .bind(message.created_at)
    .bind(message.updated_at)
    .bind(message.next_attempt_at)
    .execute(&mut *conn)
    .await
    .map_err(PostgresTransactionError::Storage)?;
    Ok(())
}

async fn stored_version_in_tx(
    conn: &mut PgConnection,
    aggregate_type: &str,
    aggregate_id: &str,
) -> Result<Option<u64>, PostgresTransactionError> {
    let row = sqlx::query(
        "SELECT version FROM pharos_aggregates
         WHERE aggregate_type = $1 AND aggregate_id = $2",
    )
    .bind(aggregate_type)
    .bind(aggregate_id)
    .fetch_optional(&mut *conn)
    .await
    .map_err(PostgresTransactionError::Storage)?;
    row.map(|r| {
        let v: i64 = r
            .try_get("version")
            .map_err(PostgresTransactionError::Storage)?;
        Ok(v as u64)
    })
    .transpose()
}
