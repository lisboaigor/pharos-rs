use std::fmt::Display;
use std::str::FromStr;

use chrono::{DateTime, Utc};
use pharos_saga::{SagaInstance, SagaSaveError, SagaStatus, SagaStore, SagaTimeoutStore};
use serde::{Serialize, de::DeserializeOwned};
use serde_json::Value;
use sqlx::Row;
use thiserror::Error;
use tracing::{Instrument, info_span};

use crate::pool::{PgPoolError, Pool};

/// Default PostgreSQL schema for saga instances.
///
/// The partial index serves [`SagaTimeoutStore::claim_due`]: only running
/// instances with a deadline are candidates for a timeout sweep.
pub const POSTGRES_SAGA_SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS pharos_sagas (
    saga_type TEXT NOT NULL,
    saga_id TEXT NOT NULL,
    state JSONB NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('running', 'completed', 'failed')),
    deadline_at TIMESTAMPTZ NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    version BIGINT NOT NULL DEFAULT 1,
    PRIMARY KEY (saga_type, saga_id)
);
ALTER TABLE pharos_sagas ADD COLUMN IF NOT EXISTS version BIGINT NOT NULL DEFAULT 1;
CREATE INDEX IF NOT EXISTS idx_pharos_sagas_due
    ON pharos_sagas (saga_type, deadline_at)
    WHERE status = 'running' AND deadline_at IS NOT NULL;
"#;

/// Installs the saga store schema.
pub async fn migrate_postgres_saga_schema(pool: &Pool) -> Result<(), PgPoolError> {
    sqlx::raw_sql(POSTGRES_SAGA_SCHEMA).execute(pool).await?;
    Ok(())
}

/// Error produced by [`PgSagaStore`].
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum PostgresSagaStoreError {
    #[error("postgres saga store failed: {0}")]
    Storage(#[from] sqlx::Error),
    #[error("saga state serialization failed: {0}")]
    Serialization(#[from] serde_json::Error),
    #[error("invalid saga id: {0}")]
    InvalidSagaId(String),
    #[error("invalid saga status: {0}")]
    InvalidStatus(String),
}

fn status_to_str(status: SagaStatus) -> &'static str {
    match status {
        SagaStatus::Running => "running",
        SagaStatus::Completed => "completed",
        SagaStatus::Failed => "failed",
    }
}

fn status_from_str(raw: &str) -> Result<SagaStatus, PostgresSagaStoreError> {
    match raw {
        "running" => Ok(SagaStatus::Running),
        "completed" => Ok(SagaStatus::Completed),
        "failed" => Ok(SagaStatus::Failed),
        other => Err(PostgresSagaStoreError::InvalidStatus(other.to_string())),
    }
}

/// PostgreSQL saga instance store with JSONB state.
///
/// Implements both [`SagaStore`] (load/save upsert) and
/// [`SagaTimeoutStore`] (`claim_due` over the partial deadline index with
/// `FOR UPDATE SKIP LOCKED` + lease), so one adapter drives event-sourced
/// progress and timeout sweeps, and sweeps are safe to run on multiple
/// service instances concurrently.
pub struct PgSagaStore<I, S> {
    pool: Pool,
    saga_type: String,
    _marker: std::marker::PhantomData<fn() -> (I, S)>,
}

impl<I, S> std::fmt::Debug for PgSagaStore<I, S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PgSagaStore")
            .field("saga_type", &self.saga_type)
            .finish_non_exhaustive()
    }
}

impl<I, S> PgSagaStore<I, S> {
    /// Creates a saga store with an explicit, stable saga type discriminator.
    pub fn with_saga_type(pool: Pool, saga_type: impl Into<String>) -> Self {
        Self {
            pool,
            saga_type: saga_type.into(),
            _marker: std::marker::PhantomData,
        }
    }

    pub fn pool(&self) -> &Pool {
        &self.pool
    }
    pub fn saga_type(&self) -> &str {
        &self.saga_type
    }

    pub async fn migrate(&self) -> Result<(), PgPoolError> {
        migrate_postgres_saga_schema(&self.pool).await
    }

    async fn stored_version(&self, saga_id: &str) -> Result<Option<u64>, PostgresSagaStoreError> {
        let row =
            sqlx::query("SELECT version FROM pharos_sagas WHERE saga_type = $1 AND saga_id = $2")
                .bind(&self.saga_type)
                .bind(saga_id)
                .fetch_optional(&self.pool)
                .await?;
        row.map(|r| {
            let v: i64 = r.try_get("version")?;
            Ok::<_, PostgresSagaStoreError>(v as u64)
        })
        .transpose()
    }
}

fn instance_from_row<I, S>(
    row: &sqlx::postgres::PgRow,
) -> Result<SagaInstance<I, S>, PostgresSagaStoreError>
where
    I: FromStr,
    <I as FromStr>::Err: Display,
    S: DeserializeOwned,
{
    let saga_id: String = row.try_get("saga_id")?;
    let id = saga_id
        .parse::<I>()
        .map_err(|e| PostgresSagaStoreError::InvalidSagaId(format!("{saga_id:?}: {e}")))?;
    let state: Value = row
        .try_get::<sqlx::types::Json<Value>, _>("state")
        .map(|j| j.0)?;
    let status: String = row.try_get("status")?;
    let deadline: Option<DateTime<Utc>> = row.try_get("deadline_at")?;
    let updated_at: DateTime<Utc> = row.try_get("updated_at")?;
    let version: i64 = row.try_get("version")?;
    Ok(SagaInstance {
        id,
        state: serde_json::from_value(state)?,
        status: status_from_str(&status)?,
        deadline,
        updated_at,
        version: version as u64,
    })
}

impl<I, S> SagaStore<I, S> for PgSagaStore<I, S>
where
    I: Display + FromStr + Send + Sync + 'static,
    <I as FromStr>::Err: Display + Send + Sync + 'static,
    S: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    type Error = PostgresSagaStoreError;

    async fn load(&self, id: &I) -> Result<Option<SagaInstance<I, S>>, Self::Error> {
        async move {
            let row = sqlx::query(
                "SELECT saga_id, state, status, deadline_at, updated_at, version
                 FROM pharos_sagas
                 WHERE saga_type = $1 AND saga_id = $2",
            )
            .bind(&self.saga_type)
            .bind(id.to_string())
            .fetch_optional(&self.pool)
            .await?;
            row.map(|row| instance_from_row(&row)).transpose()
        }
        .instrument(info_span!(
            "postgres.saga_store.load",
            saga_type = self.saga_type,
        ))
        .await
    }

    async fn save(&self, instance: SagaInstance<I, S>) -> Result<(), SagaSaveError<Self::Error>> {
        async move {
            let state = serde_json::to_string(&instance.state)
                .map_err(|e| SagaSaveError::Storage(PostgresSagaStoreError::Serialization(e)))?;
            let expected = instance.version;
            let new_version = expected as i64 + 1;
            let saga_id = instance.id.to_string();

            // `expected == 0` means the caller believes no row exists yet:
            // an INSERT that another writer wins is a conflict, exactly like
            // an UPDATE whose `version` predicate misses. Splitting the two
            // is what makes this a real compare-and-swap instead of the
            // previous unconditional UPSERT, which let two concurrent
            // `react` outcomes for the same saga silently overwrite one
            // another — the second `save` always won, and the first's
            // dispatched commands were never reflected in the persisted
            // state.
            let affected = if expected == 0 {
                sqlx::query(
                    "INSERT INTO pharos_sagas
                        (saga_type, saga_id, state, status, deadline_at, updated_at, version)
                     VALUES ($1, $2, $3::jsonb, $4, $5, $6, $7)
                     ON CONFLICT (saga_type, saga_id) DO NOTHING",
                )
                .bind(&self.saga_type)
                .bind(&saga_id)
                .bind(&state)
                .bind(status_to_str(instance.status))
                .bind(instance.deadline)
                .bind(instance.updated_at)
                .bind(new_version)
                .execute(&self.pool)
                .await
            } else {
                sqlx::query(
                    "UPDATE pharos_sagas
                     SET state = $3::jsonb, status = $4, deadline_at = $5, updated_at = $6,
                         version = $7
                     WHERE saga_type = $1 AND saga_id = $2 AND version = $8",
                )
                .bind(&self.saga_type)
                .bind(&saga_id)
                .bind(&state)
                .bind(status_to_str(instance.status))
                .bind(instance.deadline)
                .bind(instance.updated_at)
                .bind(new_version)
                .bind(expected as i64)
                .execute(&self.pool)
                .await
            }
            .map_err(|e| SagaSaveError::Storage(PostgresSagaStoreError::Storage(e)))?
            .rows_affected();

            if affected == 0 {
                let actual = self
                    .stored_version(&saga_id)
                    .await
                    .map_err(SagaSaveError::Storage)?;
                return Err(SagaSaveError::ConcurrencyConflict { expected, actual });
            }

            metrics::counter!(
                "pharos.postgres.saga_store.saved",
                "saga_type" => self.saga_type.clone()
            )
            .increment(1);
            Ok(())
        }
        .instrument(info_span!(
            "postgres.saga_store.save",
            saga_type = self.saga_type,
        ))
        .await
    }
}

impl<I, S> SagaTimeoutStore<I, S> for PgSagaStore<I, S>
where
    I: Display + FromStr + Send + Sync + 'static,
    <I as FromStr>::Err: Display + Send + Sync + 'static,
    S: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    async fn claim_due(
        &self,
        now: DateTime<Utc>,
        lease: chrono::Duration,
        limit: usize,
    ) -> Result<Vec<SagaInstance<I, S>>, Self::Error> {
        async move {
            // `FOR UPDATE SKIP LOCKED` arbitrates simultaneous sweepers, and
            // pushing `deadline_at` to `now + lease` in the same statement
            // keeps a claimed instance out of later sweeps until the claimer
            // either applies a transition or crashes and the lease expires.
            // The RETURNING clause reads the deadline from the CTE, so the
            // caller sees the original (elapsed) deadline, not the lease.
            //
            // Bumping `version` here too means a concurrent event handler's
            // `save` — racing this claim on the same saga — arbitrates
            // through the same compare-and-swap `save` already uses: only
            // one of them can be first, and the loser gets a
            // `ConcurrencyConflict` instead of silently clobbering the
            // claim's lease with a stale `deadline_at`.
            let rows = sqlx::query(
                "WITH due AS (
                     SELECT saga_type, saga_id, deadline_at FROM pharos_sagas
                     WHERE saga_type = $1
                       AND status = 'running'
                       AND deadline_at IS NOT NULL
                       AND deadline_at <= $2
                     ORDER BY deadline_at
                     LIMIT $3
                     FOR UPDATE SKIP LOCKED
                 )
                 UPDATE pharos_sagas p
                 SET deadline_at = $4, version = p.version + 1
                 FROM due
                 WHERE p.saga_type = due.saga_type AND p.saga_id = due.saga_id
                 RETURNING p.saga_id, p.state, p.status,
                           due.deadline_at AS deadline_at, p.updated_at, p.version",
            )
            .bind(&self.saga_type)
            .bind(now)
            .bind(limit as i64)
            .bind(now + lease)
            .fetch_all(&self.pool)
            .await?;
            let mut claimed = rows
                .iter()
                .map(instance_from_row)
                .collect::<Result<Vec<SagaInstance<I, S>>, _>>()?;
            // UPDATE ... FROM does not guarantee output order; restore the
            // soonest-deadline-first contract.
            claimed.sort_by_key(|instance| instance.deadline);
            if !claimed.is_empty() {
                metrics::counter!(
                    "pharos.postgres.saga_store.claimed_due",
                    "saga_type" => self.saga_type.clone()
                )
                .increment(claimed.len() as u64);
            }
            Ok(claimed)
        }
        .instrument(info_span!(
            "postgres.saga_store.claim_due",
            saga_type = self.saga_type,
        ))
        .await
    }
}
