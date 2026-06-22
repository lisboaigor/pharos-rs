use std::fmt::Display;
use std::future::Future;
use std::pin::Pin;

use chrono::Utc;
use pharos_app::{Message, OutboxMessage, UnitOfWorkError};
use pharos_core::{AggregateRoot, Entity};
use serde::Serialize;
use sqlx::{PgConnection, Row};
use thiserror::Error;
use tracing::{Instrument, info_span};

use crate::pool::Pool;

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
        E: Display + Send,
    {
        async move {
            let mut tx = self
                .pool
                .begin()
                .await
                .map_err(|e| UnitOfWorkError::Begin(e.to_string()))?;

            match work(&mut tx).await {
                Ok(value) => {
                    tx.commit()
                        .await
                        .map_err(|e| UnitOfWorkError::Commit(e.to_string()))?;
                    metrics::counter!("pharos.postgres.uow.committed").increment(1);
                    Ok(value)
                }
                Err(error) => {
                    tx.rollback().await.ok();
                    metrics::counter!("pharos.postgres.uow.rolled_back").increment(1);
                    Err(UnitOfWorkError::Operation(error.to_string()))
                }
            }
        }
        .instrument(info_span!("postgres.uow.transaction"))
        .await
    }
}

/// Error produced by the atomic save-and-enqueue helpers.
#[derive(Debug, Error)]
pub enum PostgresTransactionError {
    #[error("transaction failed: {0}")]
    Transaction(String),
    #[error("postgres statement failed: {0}")]
    Storage(String),
    #[error("aggregate serialization failed: {0}")]
    Serialization(String),
    #[error("optimistic concurrency conflict: expected version {expected}, found {actual:?}")]
    ConcurrencyConflict { expected: u64, actual: Option<u64> },
}

impl From<sqlx::Error> for PostgresTransactionError {
    fn from(e: sqlx::Error) -> Self {
        PostgresTransactionError::Transaction(e.to_string())
    }
}

/// Persists an aggregate and enqueues its pending events in one transaction.
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
    let payload = serde_json::to_value(&*aggregate)
        .map_err(|e| PostgresTransactionError::Serialization(e.to_string()))?;
    let events = aggregate.drain_events();
    let messages: Vec<OutboxMessage> = events
        .iter()
        .map(|e| OutboxMessage::new(map_event(e)))
        .collect();

    async move {
        let mut tx = pool
            .begin()
            .await
            .map_err(|e| PostgresTransactionError::Transaction(e.to_string()))?;

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
            .map_err(|e| PostgresTransactionError::Transaction(e.to_string()))?;

        aggregate.set_version(new_version);
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
    .await
}

/// Writes an aggregate payload with OCC inside an existing transaction.
///
/// Pass `&mut *tx` where `tx: sqlx::Transaction<'_, sqlx::Postgres>`, or pass
/// the `&mut PgConnection` from [`PostgresUnitOfWork::transaction`].
pub async fn save_aggregate_in_tx(
    conn: &mut PgConnection,
    aggregate_type: &str,
    aggregate_id: &str,
    payload: &serde_json::Value,
    expected_version: u64,
    new_version: u64,
) -> Result<(), PostgresTransactionError> {
    let now = Utc::now();
    let affected = if expected_version == 0 {
        sqlx::query(
            "INSERT INTO pharos_aggregates
                (aggregate_type, aggregate_id, payload, version, updated_at)
             VALUES ($1, $2, $3, $4, $5)
             ON CONFLICT (aggregate_type, aggregate_id) DO NOTHING",
        )
        .bind(aggregate_type)
        .bind(aggregate_id)
        .bind(sqlx::types::Json(payload))
        .bind(new_version as i64)
        .bind(now)
        .execute(&mut *conn)
        .await
    } else {
        sqlx::query(
            "UPDATE pharos_aggregates
             SET payload = $3, version = $4, updated_at = $5
             WHERE aggregate_type = $1 AND aggregate_id = $2 AND version = $6",
        )
        .bind(aggregate_type)
        .bind(aggregate_id)
        .bind(sqlx::types::Json(payload))
        .bind(new_version as i64)
        .bind(now)
        .bind(expected_version as i64)
        .execute(&mut *conn)
        .await
    }
    .map_err(|e| PostgresTransactionError::Storage(e.to_string()))?
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
        .map_err(|e| PostgresTransactionError::Serialization(e.to_string()))?;
    sqlx::query(
        "INSERT INTO pharos_outbox (
            id, message_id, topic, message_key, headers, payload, content_type,
            status, attempts, created_at, updated_at, last_error
        ) VALUES ($1,$2,$3,$4,$5,$6,$7,'pending',$8,$9,$10,NULL)",
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
    .execute(&mut *conn)
    .await
    .map_err(|e| PostgresTransactionError::Storage(e.to_string()))?;
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
    .map_err(|e| PostgresTransactionError::Storage(e.to_string()))?;
    Ok(row.map(|r| {
        let v: i64 = r.try_get("version").unwrap_or(0);
        v as u64
    }))
}
