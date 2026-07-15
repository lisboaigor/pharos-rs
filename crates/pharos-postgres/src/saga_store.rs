use std::fmt::Display;
use std::str::FromStr;

use chrono::{DateTime, Utc};
use pharos_saga::{SagaInstance, SagaStatus, SagaStore, SagaTimeoutStore};
use serde::{Serialize, de::DeserializeOwned};
use serde_json::Value;
use sqlx::Row;
use thiserror::Error;
use tracing::{Instrument, info_span};

use crate::pool::{PgPoolError, Pool};

/// Default PostgreSQL schema for saga instances.
///
/// The partial index serves [`SagaTimeoutStore::find_due`]: only running
/// instances with a deadline are candidates for a timeout sweep.
pub const POSTGRES_SAGA_SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS pharos_sagas (
    saga_type TEXT NOT NULL,
    saga_id TEXT NOT NULL,
    state JSONB NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('running', 'completed', 'failed')),
    deadline_at TIMESTAMPTZ NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (saga_type, saga_id)
);
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
/// [`SagaTimeoutStore`] (`find_due` over the partial deadline index), so one
/// adapter drives event-sourced progress and timeout sweeps.
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
    Ok(SagaInstance {
        id,
        state: serde_json::from_value(state)?,
        status: status_from_str(&status)?,
        deadline,
        updated_at,
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
                "SELECT saga_id, state, status, deadline_at, updated_at FROM pharos_sagas
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

    async fn save(&self, instance: SagaInstance<I, S>) -> Result<(), Self::Error> {
        async move {
            let state = serde_json::to_string(&instance.state)?;
            sqlx::query(
                "INSERT INTO pharos_sagas
                    (saga_type, saga_id, state, status, deadline_at, updated_at)
                 VALUES ($1, $2, $3::jsonb, $4, $5, $6)
                 ON CONFLICT (saga_type, saga_id) DO UPDATE
                 SET state = EXCLUDED.state,
                     status = EXCLUDED.status,
                     deadline_at = EXCLUDED.deadline_at,
                     updated_at = EXCLUDED.updated_at",
            )
            .bind(&self.saga_type)
            .bind(instance.id.to_string())
            .bind(&state)
            .bind(status_to_str(instance.status))
            .bind(instance.deadline)
            .bind(instance.updated_at)
            .execute(&self.pool)
            .await?;
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
    async fn find_due(
        &self,
        now: DateTime<Utc>,
        limit: usize,
    ) -> Result<Vec<SagaInstance<I, S>>, Self::Error> {
        async move {
            let rows = sqlx::query(
                "SELECT saga_id, state, status, deadline_at, updated_at FROM pharos_sagas
                 WHERE saga_type = $1
                   AND status = 'running'
                   AND deadline_at IS NOT NULL
                   AND deadline_at <= $2
                 ORDER BY deadline_at
                 LIMIT $3",
            )
            .bind(&self.saga_type)
            .bind(now)
            .bind(limit as i64)
            .fetch_all(&self.pool)
            .await?;
            rows.iter().map(instance_from_row).collect()
        }
        .instrument(info_span!(
            "postgres.saga_store.find_due",
            saga_type = self.saga_type,
        ))
        .await
    }
}
