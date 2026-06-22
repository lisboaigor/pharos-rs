use std::fmt::Display;
use std::str::FromStr;

use chrono::Utc;
use pharos_app::TenantContext;
use pharos_core::{AggregateRoot, Entity, Repository, RepositoryError};
use serde::{Serialize, de::DeserializeOwned};
use serde_json::Value;
use sqlx::Row;
use tracing::{Instrument, info_span};

use crate::json_repository::PostgresRepositoryError;
use crate::pool::{PgPoolError, Pool};

/// Default schema for tenant-scoped JSON aggregate persistence.
pub const POSTGRES_TENANT_AGGREGATE_SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS pharos_tenant_aggregates (
    tenant_id TEXT NOT NULL,
    aggregate_type TEXT NOT NULL,
    aggregate_id TEXT NOT NULL,
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

/// Tenant-scoped JSONB repository enforcing row-level isolation.
pub struct TenantJsonRepository<A>
where
    A: AggregateRoot,
{
    pool: Pool,
    tenant_id: String,
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
    pub fn new(pool: Pool, tenant: &TenantContext, aggregate_type: impl Into<String>) -> Self {
        Self {
            pool,
            tenant_id: tenant.tenant_id().to_string(),
            aggregate_type: aggregate_type.into(),
            _marker: std::marker::PhantomData,
        }
    }

    pub fn pool(&self) -> &Pool {
        &self.pool
    }
    pub fn tenant_id(&self) -> &str {
        &self.tenant_id
    }
    pub fn aggregate_type(&self) -> &str {
        &self.aggregate_type
    }

    pub async fn migrate(&self) -> Result<(), PgPoolError> {
        migrate_postgres_tenant_aggregate_schema(&self.pool).await
    }

    async fn stored_version(
        &self,
        aggregate_id: &str,
    ) -> Result<Option<u64>, PostgresRepositoryError> {
        let row = sqlx::query(
            "SELECT version FROM pharos_tenant_aggregates
             WHERE tenant_id = $1 AND aggregate_type = $2 AND aggregate_id = $3",
        )
        .bind(&self.tenant_id)
        .bind(&self.aggregate_type)
        .bind(aggregate_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| PostgresRepositoryError::Storage(e.to_string()))?;
        Ok(row.map(|r| {
            let v: i64 = r.try_get("version").unwrap_or(0);
            v as u64
        }))
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
            let aggregate_id = id.to_string();
            let row = sqlx::query(
                "SELECT payload FROM pharos_tenant_aggregates
                 WHERE tenant_id = $1 AND aggregate_type = $2 AND aggregate_id = $3",
            )
            .bind(&self.tenant_id)
            .bind(&self.aggregate_type)
            .bind(&aggregate_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| PostgresRepositoryError::Storage(e.to_string()))?;

            row.map(|r| {
                let payload: Value = r
                    .try_get::<sqlx::types::Json<Value>, _>("payload")
                    .map(|j| j.0)
                    .map_err(|e| PostgresRepositoryError::Storage(e.to_string()))?;
                serde_json::from_value(payload)
                    .map_err(|e| PostgresRepositoryError::Serialization(e.to_string()))
            })
            .transpose()
        }
        .instrument(info_span!(
            "postgres.tenant_repository.find_by_id",
            tenant_id = self.tenant_id,
            aggregate_type = self.aggregate_type,
        ))
        .await
    }

    async fn save(&self, aggregate: &mut A) -> Result<(), RepositoryError<Self::Error>> {
        async move {
            let aggregate_id = aggregate.id().to_string();
            let expected = aggregate.version();
            let new_version = expected + 1;
            let payload = serde_json::to_value(&*aggregate).map_err(|e| {
                RepositoryError::Storage(PostgresRepositoryError::Serialization(e.to_string()))
            })?;
            let now = Utc::now();

            let affected = if expected == 0 {
                sqlx::query(
                    "INSERT INTO pharos_tenant_aggregates
                        (tenant_id, aggregate_type, aggregate_id, payload, version, updated_at)
                     VALUES ($1, $2, $3, $4, $5, $6)
                     ON CONFLICT (tenant_id, aggregate_type, aggregate_id) DO NOTHING",
                )
                .bind(&self.tenant_id)
                .bind(&self.aggregate_type)
                .bind(&aggregate_id)
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
                .bind(&self.tenant_id)
                .bind(&self.aggregate_type)
                .bind(&aggregate_id)
                .bind(sqlx::types::Json(&payload))
                .bind(new_version as i64)
                .bind(now)
                .bind(expected as i64)
                .execute(&self.pool)
                .await
            }
            .map_err(|e| RepositoryError::Storage(PostgresRepositoryError::Storage(e.to_string())))?
            .rows_affected();

            if affected == 0 {
                let actual = self
                    .stored_version(&aggregate_id)
                    .await
                    .map_err(RepositoryError::Storage)?;
                return Err(RepositoryError::ConcurrencyConflict { expected, actual });
            }

            aggregate.set_version(new_version);
            metrics::counter!(
                "pharos.postgres.tenant_repository.saved",
                "tenant_id" => self.tenant_id.clone(),
                "aggregate_type" => self.aggregate_type.clone()
            )
            .increment(1);
            Ok(())
        }
        .instrument(info_span!(
            "postgres.tenant_repository.save",
            tenant_id = self.tenant_id,
            aggregate_type = self.aggregate_type,
        ))
        .await
    }

    async fn delete(&self, id: &A::Id) -> Result<(), Self::Error> {
        async move {
            sqlx::query(
                "DELETE FROM pharos_tenant_aggregates
                 WHERE tenant_id = $1 AND aggregate_type = $2 AND aggregate_id = $3",
            )
            .bind(&self.tenant_id)
            .bind(&self.aggregate_type)
            .bind(id.to_string())
            .execute(&self.pool)
            .await
            .map_err(|e| PostgresRepositoryError::Storage(e.to_string()))?;
            metrics::counter!(
                "pharos.postgres.tenant_repository.deleted",
                "tenant_id" => self.tenant_id.clone(),
                "aggregate_type" => self.aggregate_type.clone()
            )
            .increment(1);
            Ok(())
        }
        .instrument(info_span!(
            "postgres.tenant_repository.delete",
            tenant_id = self.tenant_id,
            aggregate_type = self.aggregate_type,
        ))
        .await
    }
}
