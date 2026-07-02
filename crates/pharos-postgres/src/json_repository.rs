use std::fmt::Display;
use std::str::FromStr;

use chrono::Utc;
use pharos_core::{AggregateRoot, Entity, Repository, RepositoryError};
use serde::{Serialize, de::DeserializeOwned};
use serde_json::Value;
use sqlx::Row;
use thiserror::Error;
use tracing::{Instrument, info_span};

use crate::pool::{PgPoolError, Pool};

/// Default PostgreSQL schema for JSON-backed aggregate persistence.
pub const POSTGRES_AGGREGATE_SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS pharos_aggregates (
    aggregate_type TEXT NOT NULL,
    aggregate_id TEXT NOT NULL,
    payload JSONB NOT NULL,
    version BIGINT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (aggregate_type, aggregate_id)
);
CREATE INDEX IF NOT EXISTS idx_pharos_aggregates_type_updated_at
    ON pharos_aggregates (aggregate_type, updated_at);
"#;

/// Installs the aggregate repository schema.
pub async fn migrate_postgres_aggregate_schema(pool: &Pool) -> Result<(), PgPoolError> {
    sqlx::raw_sql(POSTGRES_AGGREGATE_SCHEMA)
        .execute(pool)
        .await?;
    Ok(())
}

/// Error produced by [`PostgresJsonRepository`] and [`TenantJsonRepository`].
///
/// Variants keep the originating error as a typed `source` so callers can
/// inspect the real failure (e.g. `sqlx::Error::Database`) instead of matching
/// on strings.
///
/// [`TenantJsonRepository`]: crate::TenantJsonRepository
#[derive(Debug, Error)]
pub enum PostgresRepositoryError {
    #[error("postgres repository failed: {0}")]
    Storage(#[from] sqlx::Error),
    #[error("aggregate serialization failed: {0}")]
    Serialization(#[from] serde_json::Error),
    #[error("invalid aggregate id: {0}")]
    InvalidAggregateId(String),
}

/// PostgreSQL JSONB-backed repository for aggregate roots.
pub struct PostgresJsonRepository<A>
where
    A: AggregateRoot,
{
    pool: Pool,
    aggregate_type: String,
    _marker: std::marker::PhantomData<fn() -> A>,
}

impl<A> std::fmt::Debug for PostgresJsonRepository<A>
where
    A: AggregateRoot,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PostgresJsonRepository")
            .field("aggregate_type", &self.aggregate_type)
            .finish_non_exhaustive()
    }
}

impl<A> PostgresJsonRepository<A>
where
    A: AggregateRoot,
{
    /// Creates a repository with an explicit, stable aggregate type discriminator.
    pub fn with_aggregate_type(pool: Pool, aggregate_type: impl Into<String>) -> Self {
        Self {
            pool,
            aggregate_type: aggregate_type.into(),
            _marker: std::marker::PhantomData,
        }
    }

    pub fn pool(&self) -> &Pool {
        &self.pool
    }
    pub fn aggregate_type(&self) -> &str {
        &self.aggregate_type
    }

    pub async fn migrate(&self) -> Result<(), PgPoolError> {
        migrate_postgres_aggregate_schema(&self.pool).await
    }

    async fn stored_version(
        &self,
        aggregate_id: &str,
    ) -> Result<Option<u64>, PostgresRepositoryError> {
        let row = sqlx::query(
            "SELECT version FROM pharos_aggregates
             WHERE aggregate_type = $1 AND aggregate_id = $2",
        )
        .bind(&self.aggregate_type)
        .bind(aggregate_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(PostgresRepositoryError::Storage)?;
        row.map(|r| {
            let v: i64 = r
                .try_get("version")
                .map_err(PostgresRepositoryError::Storage)?;
            Ok(v as u64)
        })
        .transpose()
    }
}

impl<A> Repository<A> for PostgresJsonRepository<A>
where
    A: AggregateRoot + Serialize + DeserializeOwned + Send + Sync + 'static,
    <A as Entity>::Id: Display + FromStr + Send + Sync + 'static,
    <<A as Entity>::Id as FromStr>::Err: Display + Send + Sync + 'static,
{
    type Error = PostgresRepositoryError;

    async fn find_by_id(&self, id: &A::Id) -> Result<Option<A>, Self::Error> {
        async move {
            let aggregate_id = id.to_string();
            let row = sqlx::query(
                "SELECT payload, version FROM pharos_aggregates
                 WHERE aggregate_type = $1 AND aggregate_id = $2",
            )
            .bind(&self.aggregate_type)
            .bind(&aggregate_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(PostgresRepositoryError::Storage)?;

            row.map(|r| {
                let db_version: i64 = r
                    .try_get("version")
                    .map_err(PostgresRepositoryError::Storage)?;
                let payload: Value = r
                    .try_get::<sqlx::types::Json<Value>, _>("payload")
                    .map(|j| j.0)
                    .map_err(PostgresRepositoryError::Storage)?;
                let mut aggregate: A = serde_json::from_value(payload)
                    .map_err(PostgresRepositoryError::Serialization)?;
                // The version column is authoritative: every write path bumps
                // it atomically, while the version embedded in the payload can
                // lag for rows written by older save paths.
                aggregate.set_version(db_version as u64);
                Ok(aggregate)
            })
            .transpose()
        }
        .instrument(info_span!(
            "postgres.repository.find_by_id",
            aggregate_type = self.aggregate_type,
        ))
        .await
    }

    async fn save(&self, aggregate: &mut A) -> Result<(), RepositoryError<Self::Error>> {
        async move {
            let aggregate_id = aggregate.id().to_string();
            let expected = aggregate.version();
            let new_version = expected + 1;

            // Set new_version before serializing so the stored payload reflects the
            // version a future load will observe, preventing spurious ConcurrencyConflict.
            aggregate.set_version(new_version);

            let payload = serde_json::to_value(&*aggregate).map_err(|e| {
                aggregate.set_version(expected); // revert on serialization error
                RepositoryError::Storage(PostgresRepositoryError::Serialization(e))
            })?;
            let now = Utc::now();

            let affected = if expected == 0 {
                sqlx::query(
                    "INSERT INTO pharos_aggregates
                        (aggregate_type, aggregate_id, payload, version, updated_at)
                     VALUES ($1, $2, $3, $4, $5)
                     ON CONFLICT (aggregate_type, aggregate_id) DO NOTHING",
                )
                .bind(&self.aggregate_type)
                .bind(&aggregate_id)
                .bind(sqlx::types::Json(&payload))
                .bind(new_version as i64)
                .bind(now)
                .execute(&self.pool)
                .await
            } else {
                sqlx::query(
                    "UPDATE pharos_aggregates
                     SET payload = $3, version = $4, updated_at = $5
                     WHERE aggregate_type = $1 AND aggregate_id = $2 AND version = $6",
                )
                .bind(&self.aggregate_type)
                .bind(&aggregate_id)
                .bind(sqlx::types::Json(&payload))
                .bind(new_version as i64)
                .bind(now)
                .bind(expected as i64)
                .execute(&self.pool)
                .await
            }
            .map_err(|e| {
                aggregate.set_version(expected); // revert on DB error
                RepositoryError::Storage(PostgresRepositoryError::Storage(e))
            })?
            .rows_affected();

            if affected == 0 {
                aggregate.set_version(expected); // revert on optimistic lock conflict
                let actual = self
                    .stored_version(&aggregate_id)
                    .await
                    .map_err(RepositoryError::Storage)?;
                return Err(RepositoryError::ConcurrencyConflict { expected, actual });
            }
            metrics::counter!(
                "pharos.postgres.repository.saved",
                "aggregate_type" => self.aggregate_type.clone()
            )
            .increment(1);
            Ok(())
        }
        .instrument(info_span!(
            "postgres.repository.save",
            aggregate_type = self.aggregate_type,
        ))
        .await
    }

    async fn delete(&self, id: &A::Id) -> Result<(), Self::Error> {
        async move {
            sqlx::query(
                "DELETE FROM pharos_aggregates
                 WHERE aggregate_type = $1 AND aggregate_id = $2",
            )
            .bind(&self.aggregate_type)
            .bind(id.to_string())
            .execute(&self.pool)
            .await
            .map_err(PostgresRepositoryError::Storage)?;
            metrics::counter!(
                "pharos.postgres.repository.deleted",
                "aggregate_type" => self.aggregate_type.clone()
            )
            .increment(1);
            Ok(())
        }
        .instrument(info_span!(
            "postgres.repository.delete",
            aggregate_type = self.aggregate_type,
        ))
        .await
    }
}
