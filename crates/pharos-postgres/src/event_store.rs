use std::fmt::Display;

use chrono::{DateTime, Utc};
use pharos_core::RepositoryError;
use pharos_es::{EventStore, Snapshot, SnapshotStore, StoredEvent};
use serde::{Serialize, de::DeserializeOwned};
use serde_json::Value;
use sqlx::Row;
use thiserror::Error;
use tracing::{Instrument, info_span};

use crate::pool::{PgPoolError, Pool};

/// Default PostgreSQL schema for event streams and snapshots.
pub const POSTGRES_EVENT_STORE_SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS pharos_event_streams (
    stream_type TEXT NOT NULL,
    stream_id TEXT NOT NULL,
    sequence BIGINT NOT NULL,
    payload JSONB NOT NULL,
    recorded_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (stream_type, stream_id, sequence)
);
CREATE TABLE IF NOT EXISTS pharos_snapshots (
    stream_type TEXT NOT NULL,
    stream_id TEXT NOT NULL,
    payload JSONB NOT NULL,
    version BIGINT NOT NULL,
    taken_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (stream_type, stream_id)
);
"#;

/// Installs the event store schema.
pub async fn migrate_postgres_event_store_schema(pool: &Pool) -> Result<(), PgPoolError> {
    sqlx::raw_sql(POSTGRES_EVENT_STORE_SCHEMA)
        .execute(pool)
        .await?;
    Ok(())
}

/// Error produced by [`PgEventStore`] and [`PgSnapshotStore`].
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum PostgresEventStoreError {
    #[error("postgres event store failed: {0}")]
    Storage(#[from] sqlx::Error),
    #[error("event serialization failed: {0}")]
    Serialization(#[from] serde_json::Error),
}

/// PostgreSQL append-only event store with JSONB payloads.
///
/// Optimistic concurrency is enforced twice: the current stream head is
/// compared against `expected_version` inside the append transaction, and the
/// `(stream_type, stream_id, sequence)` primary key is the arbiter for
/// concurrent appenders — the loser's unique violation is reported as
/// [`RepositoryError::ConcurrencyConflict`].
pub struct PgEventStore<I, E> {
    pool: Pool,
    stream_type: String,
    _marker: std::marker::PhantomData<fn() -> (I, E)>,
}

impl<I, E> std::fmt::Debug for PgEventStore<I, E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PgEventStore")
            .field("stream_type", &self.stream_type)
            .finish_non_exhaustive()
    }
}

impl<I, E> PgEventStore<I, E> {
    /// Creates an event store with an explicit, stable stream type discriminator.
    pub fn with_stream_type(pool: Pool, stream_type: impl Into<String>) -> Self {
        Self {
            pool,
            stream_type: stream_type.into(),
            _marker: std::marker::PhantomData,
        }
    }

    pub fn pool(&self) -> &Pool {
        &self.pool
    }
    pub fn stream_type(&self) -> &str {
        &self.stream_type
    }

    pub async fn migrate(&self) -> Result<(), PgPoolError> {
        migrate_postgres_event_store_schema(&self.pool).await
    }

    async fn stream_head(&self, stream_id: &str) -> Result<u64, PostgresEventStoreError> {
        let row = sqlx::query(
            "SELECT COALESCE(MAX(sequence), 0) AS head FROM pharos_event_streams
             WHERE stream_type = $1 AND stream_id = $2",
        )
        .bind(&self.stream_type)
        .bind(stream_id)
        .fetch_one(&self.pool)
        .await?;
        let head: i64 = row.try_get("head")?;
        Ok(head as u64)
    }
}

fn is_unique_violation(error: &sqlx::Error) -> bool {
    matches!(
        error,
        sqlx::Error::Database(db) if db.is_unique_violation()
    )
}

impl<I, E> EventStore<I, E> for PgEventStore<I, E>
where
    I: Display + Send + Sync + 'static,
    E: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    type Error = PostgresEventStoreError;

    async fn load(&self, id: &I) -> Result<Vec<StoredEvent<E>>, Self::Error> {
        self.load_after(id, 0).await
    }

    async fn load_after(&self, id: &I, after: u64) -> Result<Vec<StoredEvent<E>>, Self::Error> {
        async move {
            let rows = sqlx::query(
                "SELECT sequence, payload, recorded_at FROM pharos_event_streams
                 WHERE stream_type = $1 AND stream_id = $2 AND sequence > $3
                 ORDER BY sequence",
            )
            .bind(&self.stream_type)
            .bind(id.to_string())
            .bind(after as i64)
            .fetch_all(&self.pool)
            .await?;

            rows.into_iter()
                .map(|row| {
                    let sequence: i64 = row.try_get("sequence")?;
                    let payload: Value = row
                        .try_get::<sqlx::types::Json<Value>, _>("payload")
                        .map(|j| j.0)?;
                    let recorded_at: DateTime<Utc> = row.try_get("recorded_at")?;
                    Ok(StoredEvent {
                        sequence: sequence as u64,
                        event: serde_json::from_value(payload)?,
                        recorded_at,
                    })
                })
                .collect()
        }
        .instrument(info_span!(
            "postgres.event_store.load",
            stream_type = self.stream_type,
        ))
        .await
    }

    async fn append(
        &self,
        id: &I,
        expected_version: u64,
        events: Vec<E>,
    ) -> Result<(), RepositoryError<Self::Error>> {
        async move {
            if events.is_empty() {
                return Ok(());
            }
            let stream_id = id.to_string();

            // Serialize before touching the database so a bad payload never
            // opens a transaction.
            let payloads = events
                .iter()
                .map(serde_json::to_string)
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| RepositoryError::Storage(PostgresEventStoreError::Serialization(e)))?;

            let mut tx = self
                .pool
                .begin()
                .await
                .map_err(|e| RepositoryError::Storage(PostgresEventStoreError::Storage(e)))?;

            // A stale expected_version above the head would otherwise insert
            // past a gap without tripping the primary key.
            let row = sqlx::query(
                "SELECT COALESCE(MAX(sequence), 0) AS head FROM pharos_event_streams
                 WHERE stream_type = $1 AND stream_id = $2",
            )
            .bind(&self.stream_type)
            .bind(&stream_id)
            .fetch_one(&mut *tx)
            .await
            .map_err(|e| RepositoryError::Storage(PostgresEventStoreError::Storage(e)))?;
            let head: i64 = row
                .try_get("head")
                .map_err(|e| RepositoryError::Storage(PostgresEventStoreError::Storage(e)))?;
            if head as u64 != expected_version {
                return Err(RepositoryError::ConcurrencyConflict {
                    expected: expected_version,
                    actual: Some(head as u64),
                });
            }

            let now = Utc::now();
            for (offset, payload) in payloads.iter().enumerate() {
                let sequence = expected_version + offset as u64 + 1;
                let result = sqlx::query(
                    "INSERT INTO pharos_event_streams
                        (stream_type, stream_id, sequence, payload, recorded_at)
                     VALUES ($1, $2, $3, $4::jsonb, $5)",
                )
                .bind(&self.stream_type)
                .bind(&stream_id)
                .bind(sequence as i64)
                .bind(payload)
                .bind(now)
                .execute(&mut *tx)
                .await;

                if let Err(error) = result {
                    // The primary key arbitrates concurrent appenders: the
                    // loser sees a unique violation, reported as a conflict.
                    if is_unique_violation(&error) {
                        drop(tx);
                        let actual = self.stream_head(&stream_id).await.ok();
                        return Err(RepositoryError::ConcurrencyConflict {
                            expected: expected_version,
                            actual,
                        });
                    }
                    return Err(RepositoryError::Storage(PostgresEventStoreError::Storage(
                        error,
                    )));
                }
            }

            tx.commit()
                .await
                .map_err(|e| RepositoryError::Storage(PostgresEventStoreError::Storage(e)))?;
            metrics::counter!(
                "pharos.postgres.event_store.appended",
                "stream_type" => self.stream_type.clone()
            )
            .increment(payloads.len() as u64);
            Ok(())
        }
        .instrument(info_span!(
            "postgres.event_store.append",
            stream_type = self.stream_type,
        ))
        .await
    }

    async fn delete_stream(&self, id: &I) -> Result<(), Self::Error> {
        async move {
            let stream_id = id.to_string();
            let mut tx = self.pool.begin().await?;
            sqlx::query(
                "DELETE FROM pharos_event_streams WHERE stream_type = $1 AND stream_id = $2",
            )
            .bind(&self.stream_type)
            .bind(&stream_id)
            .execute(&mut *tx)
            .await?;
            // A snapshot without its stream would resurrect deleted state.
            sqlx::query("DELETE FROM pharos_snapshots WHERE stream_type = $1 AND stream_id = $2")
                .bind(&self.stream_type)
                .bind(&stream_id)
                .execute(&mut *tx)
                .await?;
            tx.commit().await?;
            metrics::counter!(
                "pharos.postgres.event_store.stream_deleted",
                "stream_type" => self.stream_type.clone()
            )
            .increment(1);
            Ok(())
        }
        .instrument(info_span!(
            "postgres.event_store.delete_stream",
            stream_type = self.stream_type,
        ))
        .await
    }
}

/// PostgreSQL snapshot store with JSONB payloads.
///
/// Pairs with [`PgEventStore`] under the same `stream_type` so
/// `delete_stream` removes both the events and the snapshot.
pub struct PgSnapshotStore<I, S> {
    pool: Pool,
    stream_type: String,
    _marker: std::marker::PhantomData<fn() -> (I, S)>,
}

impl<I, S> std::fmt::Debug for PgSnapshotStore<I, S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PgSnapshotStore")
            .field("stream_type", &self.stream_type)
            .finish_non_exhaustive()
    }
}

impl<I, S> PgSnapshotStore<I, S> {
    /// Creates a snapshot store scoped to a stream type discriminator.
    pub fn with_stream_type(pool: Pool, stream_type: impl Into<String>) -> Self {
        Self {
            pool,
            stream_type: stream_type.into(),
            _marker: std::marker::PhantomData,
        }
    }

    pub fn pool(&self) -> &Pool {
        &self.pool
    }
    pub fn stream_type(&self) -> &str {
        &self.stream_type
    }

    pub async fn migrate(&self) -> Result<(), PgPoolError> {
        migrate_postgres_event_store_schema(&self.pool).await
    }
}

impl<I, S> SnapshotStore<I, S> for PgSnapshotStore<I, S>
where
    I: Display + Send + Sync + 'static,
    S: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    type Error = PostgresEventStoreError;

    async fn load(&self, id: &I) -> Result<Option<Snapshot<S>>, Self::Error> {
        async move {
            let row = sqlx::query(
                "SELECT payload, version, taken_at FROM pharos_snapshots
                 WHERE stream_type = $1 AND stream_id = $2",
            )
            .bind(&self.stream_type)
            .bind(id.to_string())
            .fetch_optional(&self.pool)
            .await?;

            row.map(|row| {
                let version: i64 = row.try_get("version")?;
                let payload: Value = row
                    .try_get::<sqlx::types::Json<Value>, _>("payload")
                    .map(|j| j.0)?;
                let taken_at: DateTime<Utc> = row.try_get("taken_at")?;
                Ok(Snapshot {
                    state: serde_json::from_value(payload)?,
                    version: version as u64,
                    taken_at,
                })
            })
            .transpose()
        }
        .instrument(info_span!(
            "postgres.snapshot_store.load",
            stream_type = self.stream_type,
        ))
        .await
    }

    async fn save(&self, id: &I, snapshot: Snapshot<S>) -> Result<(), Self::Error> {
        async move {
            let payload = serde_json::to_string(&snapshot.state)?;
            sqlx::query(
                "INSERT INTO pharos_snapshots
                    (stream_type, stream_id, payload, version, taken_at)
                 VALUES ($1, $2, $3::jsonb, $4, $5)
                 ON CONFLICT (stream_type, stream_id) DO UPDATE
                 SET payload = EXCLUDED.payload,
                     version = EXCLUDED.version,
                     taken_at = EXCLUDED.taken_at",
            )
            .bind(&self.stream_type)
            .bind(id.to_string())
            .bind(&payload)
            .bind(snapshot.version as i64)
            .bind(snapshot.taken_at)
            .execute(&self.pool)
            .await?;
            metrics::counter!(
                "pharos.postgres.snapshot_store.saved",
                "stream_type" => self.stream_type.clone()
            )
            .increment(1);
            Ok(())
        }
        .instrument(info_span!(
            "postgres.snapshot_store.save",
            stream_type = self.stream_type,
        ))
        .await
    }
}
