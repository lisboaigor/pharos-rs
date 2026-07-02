use std::fmt::Display;
use std::str::FromStr;

use chrono::Utc;
use pharos_app::TenantContext;
use pharos_core::{AggregateRoot, Entity, Repository, RepositoryError};
use serde::{Serialize, de::DeserializeOwned};
use serde_json::Value;
use sqlx::Row;
use tracing::{Instrument, info_span};
use uuid::Uuid;

use crate::json_repository::PostgresRepositoryError;
use crate::pool::{PgPoolError, Pool};

/// Default schema for tenant-scoped JSON aggregate persistence.
///
/// Both identifiers are `UUID`: the tenant id is guaranteed by
/// [`pharos_app::TenantId`] (validated once, at the edge), and aggregate ids
/// must render as UUIDs — use `id_type!` from `pharos-macros` or store a raw
/// `Uuid`. Row-level isolation comes from exact equality on the composite
/// primary key.
pub const POSTGRES_TENANT_AGGREGATE_SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS pharos_tenant_aggregates (
    tenant_id UUID NOT NULL,
    aggregate_type TEXT NOT NULL,
    aggregate_id UUID NOT NULL,
    payload JSONB NOT NULL,
    version BIGINT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (tenant_id, aggregate_type, aggregate_id)
);
CREATE INDEX IF NOT EXISTS idx_pharos_tenant_aggregates_type_updated_at
    ON pharos_tenant_aggregates (tenant_id, aggregate_type, updated_at);
"#;

/// Installs the tenant-scoped aggregate schema.
pub async fn migrate_postgres_tenant_aggregate_schema(pool: &Pool) -> Result<(), PgPoolError> {
    sqlx::raw_sql(POSTGRES_TENANT_AGGREGATE_SCHEMA)
        .execute(pool)
        .await?;
    Ok(())
}

fn parse_aggregate_id(s: &str) -> Result<Uuid, PostgresRepositoryError> {
    Uuid::parse_str(s).map_err(|e| {
        PostgresRepositoryError::InvalidAggregateId(format!(
            "aggregate id {s:?} is not a valid UUID: {e}"
        ))
    })
}

/// Tenant-scoped JSONB repository enforcing row-level isolation.
///
/// The tenant identity comes from an already-validated [`TenantContext`], so
/// constructing the repository is infallible and there is no fallback path
/// that could ever merge two tenants into one bucket.
pub struct TenantJsonRepository<A>
where
    A: AggregateRoot,
{
    pool: Pool,
    tenant_id: Uuid,
    aggregate_type: String,
    _marker: std::marker::PhantomData<fn() -> A>,
}

impl<A> std::fmt::Debug for TenantJsonRepository<A>
where
    A: AggregateRoot,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TenantJsonRepository")
            .field("tenant_id", &self.tenant_id)
            .field("aggregate_type", &self.aggregate_type)
            .finish_non_exhaustive()
    }
}

impl<A> TenantJsonRepository<A>
where
    A: AggregateRoot,
{
    /// Creates a repository scoped to one tenant.
    pub fn new(pool: Pool, tenant: &TenantContext, aggregate_type: impl Into<String>) -> Self {
        Self {
            pool,
            tenant_id: tenant.tenant_id().as_uuid(),
            aggregate_type: aggregate_type.into(),
            _marker: std::marker::PhantomData,
        }
    }

    pub fn pool(&self) -> &Pool {
        &self.pool
    }
    pub fn tenant_id(&self) -> Uuid {
        self.tenant_id
    }
    pub fn aggregate_type(&self) -> &str {
        &self.aggregate_type
    }

    pub async fn migrate(&self) -> Result<(), PgPoolError> {
        migrate_postgres_tenant_aggregate_schema(&self.pool).await
    }

    async fn stored_version(
        &self,
        aggregate_id: Uuid,
    ) -> Result<Option<u64>, PostgresRepositoryError> {
        let row = sqlx::query(
            "SELECT version FROM pharos_tenant_aggregates
             WHERE tenant_id = $1 AND aggregate_type = $2 AND aggregate_id = $3",
        )
        .bind(self.tenant_id)
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

impl<A> Repository<A> for TenantJsonRepository<A>
where
    A: AggregateRoot + Serialize + DeserializeOwned + Send + Sync + 'static,
    <A as Entity>::Id: Display + FromStr + Send + Sync + 'static,
    <<A as Entity>::Id as FromStr>::Err: Display + Send + Sync + 'static,
{
    type Error = PostgresRepositoryError;

    async fn find_by_id(&self, id: &A::Id) -> Result<Option<A>, Self::Error> {
        async move {
            let aggregate_id = parse_aggregate_id(&id.to_string())?;
            let row = sqlx::query(
                "SELECT payload, version FROM pharos_tenant_aggregates
                 WHERE tenant_id = $1 AND aggregate_type = $2 AND aggregate_id = $3",
            )
            .bind(self.tenant_id)
            .bind(&self.aggregate_type)
            .bind(aggregate_id)
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
            "postgres.tenant_repository.find_by_id",
            tenant_id = %self.tenant_id,
            aggregate_type = self.aggregate_type,
        ))
        .await
    }

    async fn save(&self, aggregate: &mut A) -> Result<(), RepositoryError<Self::Error>> {
        async move {
            let aggregate_id = parse_aggregate_id(&aggregate.id().to_string())
                .map_err(RepositoryError::Storage)?;
            let expected = aggregate.version();
            let new_version = expected + 1;

            // Set new_version before serializing so the stored payload reflects
            // the version a future load will observe (same contract as
            // `PostgresJsonRepository::save`).
            aggregate.set_version(new_version);

            let payload = serde_json::to_value(&*aggregate).map_err(|e| {
                aggregate.set_version(expected); // revert on serialization error
                RepositoryError::Storage(PostgresRepositoryError::Serialization(e))
            })?;
            let now = Utc::now();

            let affected = if expected == 0 {
                sqlx::query(
                    "INSERT INTO pharos_tenant_aggregates
                        (tenant_id, aggregate_type, aggregate_id, payload, version, updated_at)
                     VALUES ($1, $2, $3, $4, $5, $6)
                     ON CONFLICT (tenant_id, aggregate_type, aggregate_id) DO NOTHING",
                )
                .bind(self.tenant_id)
                .bind(&self.aggregate_type)
                .bind(aggregate_id)
                .bind(sqlx::types::Json(&payload))
                .bind(new_version as i64)
                .bind(now)
                .execute(&self.pool)
                .await
            } else {
                sqlx::query(
                    "UPDATE pharos_tenant_aggregates
                     SET payload = $4, version = $5, updated_at = $6
                     WHERE tenant_id = $1 AND aggregate_type = $2 AND aggregate_id = $3
                       AND version = $7",
                )
                .bind(self.tenant_id)
                .bind(&self.aggregate_type)
                .bind(aggregate_id)
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
                    .stored_version(aggregate_id)
                    .await
                    .map_err(RepositoryError::Storage)?;
                return Err(RepositoryError::ConcurrencyConflict { expected, actual });
            }

            metrics::counter!(
                "pharos.postgres.tenant_repository.saved",
                "tenant_id" => self.tenant_id.to_string(),
                "aggregate_type" => self.aggregate_type.clone()
            )
            .increment(1);
            Ok(())
        }
        .instrument(info_span!(
            "postgres.tenant_repository.save",
            tenant_id = %self.tenant_id,
            aggregate_type = self.aggregate_type,
        ))
        .await
    }

    async fn delete(&self, id: &A::Id) -> Result<(), Self::Error> {
        async move {
            let aggregate_id = parse_aggregate_id(&id.to_string())?;
            sqlx::query(
                "DELETE FROM pharos_tenant_aggregates
                 WHERE tenant_id = $1 AND aggregate_type = $2 AND aggregate_id = $3",
            )
            .bind(self.tenant_id)
            .bind(&self.aggregate_type)
            .bind(aggregate_id)
            .execute(&self.pool)
            .await
            .map_err(PostgresRepositoryError::Storage)?;
            metrics::counter!(
                "pharos.postgres.tenant_repository.deleted",
                "tenant_id" => self.tenant_id.to_string(),
                "aggregate_type" => self.aggregate_type.clone()
            )
            .increment(1);
            Ok(())
        }
        .instrument(info_span!(
            "postgres.tenant_repository.delete",
            tenant_id = %self.tenant_id,
            aggregate_type = self.aggregate_type,
        ))
        .await
    }
}
