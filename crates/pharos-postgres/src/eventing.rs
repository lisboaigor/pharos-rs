use std::time::Duration;

use chrono::Utc;
use pharos_app::{
    IdempotencyDecision, InboxError, InboxMessage, InboxStatus, InboxStore, Message, OutboxError,
    OutboxMessage, OutboxRepository, OutboxStatus,
};
use serde_json::Value;
use sqlx::{Row, postgres::PgRow};
use tracing::{Instrument, info_span};
use uuid::Uuid;

use crate::pool::{PgPoolError, Pool};

/// Default PostgreSQL schema used by the provided outbox and inbox adapters.
pub const POSTGRES_EVENTING_SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS pharos_outbox (
    id UUID PRIMARY KEY,
    message_id UUID NOT NULL,
    topic TEXT NOT NULL,
    message_key TEXT NULL,
    headers JSONB NOT NULL DEFAULT '{}'::jsonb,
    payload BYTEA NOT NULL,
    content_type TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('pending', 'published', 'failed')),
    attempts INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    last_error TEXT NULL
);
CREATE INDEX IF NOT EXISTS idx_pharos_outbox_pending_created_at
    ON pharos_outbox (created_at)
    WHERE status = 'pending';
CREATE TABLE IF NOT EXISTS pharos_inbox (
    message_id UUID NOT NULL,
    consumer TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('processing', 'completed', 'failed')),
    received_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    last_error TEXT NULL,
    PRIMARY KEY (message_id, consumer)
);
CREATE INDEX IF NOT EXISTS idx_pharos_inbox_status_updated_at
    ON pharos_inbox (status, updated_at);
"#;

/// Installs the default eventing schema.
pub async fn migrate_postgres_eventing_schema(pool: &Pool) -> Result<(), PgPoolError> {
    sqlx::raw_sql(POSTGRES_EVENTING_SCHEMA)
        .execute(pool)
        .await?;
    Ok(())
}

// ─── Outbox ───────────────────────────────────────────────────────────────────

/// PostgreSQL implementation of [`OutboxRepository`], backed by a connection pool.
#[derive(Debug, Clone)]
pub struct PostgresOutboxRepository {
    pool: Pool,
}

impl PostgresOutboxRepository {
    pub fn new(pool: Pool) -> Self {
        Self { pool }
    }
    pub fn pool(&self) -> &Pool {
        &self.pool
    }
    pub async fn migrate(&self) -> Result<(), PgPoolError> {
        migrate_postgres_eventing_schema(&self.pool).await
    }

    /// Deletes `published` and `failed` outbox rows older than `older_than`.
    ///
    /// Schedule this periodically (e.g. daily) to prevent unbounded table growth.
    /// Returns the number of deleted rows.
    pub async fn cleanup_older_than(&self, older_than: Duration) -> Result<u64, OutboxError> {
        let cutoff = Utc::now()
            - chrono::Duration::from_std(older_than)
                .map_err(|e| OutboxError::Storage(e.to_string()))?;
        let result = sqlx::query(
            "DELETE FROM pharos_outbox
             WHERE status IN ('published', 'failed') AND updated_at < $1",
        )
        .bind(cutoff)
        .execute(&self.pool)
        .await
        .map_err(|e| OutboxError::Storage(e.to_string()))?;
        let deleted = result.rows_affected();
        if deleted > 0 {
            metrics::counter!("pharos.postgres.outbox.cleaned_up").increment(deleted);
        }
        Ok(deleted)
    }
}

impl OutboxRepository for PostgresOutboxRepository {
    async fn insert(&self, message: OutboxMessage) -> Result<(), OutboxError> {
        async move {
            let headers = serde_json::to_value(&message.message.headers)
                .map_err(|e| OutboxError::Storage(e.to_string()))?;
            sqlx::query(
                "INSERT INTO pharos_outbox (
                    id, message_id, topic, message_key, headers, payload, content_type,
                    status, attempts, created_at, updated_at, last_error
                ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)",
            )
            .bind(message.id)
            .bind(message.message.message_id)
            .bind(&message.message.topic)
            .bind(&message.message.key)
            .bind(sqlx::types::Json(&headers))
            .bind(&message.message.payload)
            .bind(&message.message.content_type)
            .bind(status_to_str(message.status))
            .bind(message.attempts as i32)
            .bind(message.created_at)
            .bind(message.updated_at)
            .bind(&message.last_error)
            .execute(&self.pool)
            .await
            .map_err(|e| OutboxError::Storage(e.to_string()))?;
            metrics::counter!("pharos.postgres.outbox.inserted").increment(1);
            Ok(())
        }
        .instrument(info_span!("postgres.outbox.insert"))
        .await
    }

    async fn pending(&self, limit: usize) -> Result<Vec<OutboxMessage>, OutboxError> {
        async move {
            let rows = sqlx::query(
                "SELECT id, message_id, topic, message_key, headers, payload, content_type,
                        status, attempts, created_at, updated_at, last_error
                 FROM pharos_outbox
                 WHERE status = 'pending'
                 ORDER BY created_at ASC
                 LIMIT $1
                 FOR UPDATE SKIP LOCKED",
            )
            .bind(limit as i64)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| OutboxError::Storage(e.to_string()))?;
            rows.into_iter().map(row_to_outbox_message).collect()
        }
        .instrument(info_span!("postgres.outbox.pending", limit))
        .await
    }

    async fn record_attempt(&self, id: Uuid) -> Result<(), OutboxError> {
        async move {
            let r = sqlx::query(
                "UPDATE pharos_outbox SET attempts = attempts + 1, updated_at = $2 WHERE id = $1",
            )
            .bind(id)
            .bind(Utc::now())
            .execute(&self.pool)
            .await
            .map_err(|e| OutboxError::Storage(e.to_string()))?;
            ensure_outbox_updated(id, r.rows_affected())
        }
        .instrument(info_span!("postgres.outbox.record_attempt", %id))
        .await
    }

    async fn mark_published(&self, id: Uuid) -> Result<(), OutboxError> {
        async move {
            let r = sqlx::query(
                "UPDATE pharos_outbox
                 SET status = 'published', updated_at = $2, last_error = NULL
                 WHERE id = $1",
            )
            .bind(id)
            .bind(Utc::now())
            .execute(&self.pool)
            .await
            .map_err(|e| OutboxError::Storage(e.to_string()))?;
            metrics::counter!("pharos.postgres.outbox.published").increment(1);
            ensure_outbox_updated(id, r.rows_affected())
        }
        .instrument(info_span!("postgres.outbox.mark_published", %id))
        .await
    }

    async fn mark_failed(&self, id: Uuid, error: String) -> Result<(), OutboxError> {
        async move {
            let r = sqlx::query(
                "UPDATE pharos_outbox
                 SET status = 'failed', updated_at = $2, last_error = $3
                 WHERE id = $1",
            )
            .bind(id)
            .bind(Utc::now())
            .bind(&error)
            .execute(&self.pool)
            .await
            .map_err(|e| OutboxError::Storage(e.to_string()))?;
            metrics::counter!("pharos.postgres.outbox.failed").increment(1);
            ensure_outbox_updated(id, r.rows_affected())
        }
        .instrument(info_span!("postgres.outbox.mark_failed", %id))
        .await
    }
}

// ─── Inbox ────────────────────────────────────────────────────────────────────

/// PostgreSQL implementation of [`InboxStore`], backed by a connection pool.
#[derive(Debug, Clone)]
pub struct PostgresInboxStore {
    pool: Pool,
}

impl PostgresInboxStore {
    pub fn new(pool: Pool) -> Self {
        Self { pool }
    }
    pub fn pool(&self) -> &Pool {
        &self.pool
    }
    pub async fn migrate(&self) -> Result<(), PgPoolError> {
        migrate_postgres_eventing_schema(&self.pool).await
    }
}

impl InboxStore for PostgresInboxStore {
    async fn begin_processing(
        &self,
        message_id: Uuid,
        consumer: &str,
    ) -> Result<IdempotencyDecision, InboxError> {
        async move {
            let now = Utc::now();
            let row = sqlx::query(
                "INSERT INTO pharos_inbox (
                    message_id, consumer, status, received_at, updated_at, last_error
                ) VALUES ($1, $2, 'processing', $3, $3, NULL)
                ON CONFLICT (message_id, consumer)
                DO UPDATE SET updated_at = pharos_inbox.updated_at
                RETURNING status, (xmax = 0) AS inserted",
            )
            .bind(message_id)
            .bind(consumer)
            .bind(now)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| InboxError::Storage(e.to_string()))?;

            let inserted: bool = row
                .try_get("inserted")
                .map_err(|e| InboxError::Storage(e.to_string()))?;
            if inserted {
                metrics::counter!(
                    "pharos.postgres.inbox.started",
                    "consumer" => consumer.to_string()
                )
                .increment(1);
                return Ok(IdempotencyDecision::StartProcessing);
            }
            let status: String = row
                .try_get("status")
                .map_err(|e| InboxError::Storage(e.to_string()))?;
            Ok(match str_to_inbox_status(&status)? {
                InboxStatus::Processing => IdempotencyDecision::AlreadyProcessing,
                InboxStatus::Completed => IdempotencyDecision::AlreadyCompleted,
                InboxStatus::Failed => IdempotencyDecision::RetryPreviousFailure,
            })
        }
        .instrument(info_span!("postgres.inbox.begin_processing", %message_id, consumer))
        .await
    }

    async fn mark_completed(&self, message_id: Uuid, consumer: &str) -> Result<(), InboxError> {
        async move {
            let r = sqlx::query(
                "UPDATE pharos_inbox
                 SET status = 'completed', updated_at = $3, last_error = NULL
                 WHERE message_id = $1 AND consumer = $2",
            )
            .bind(message_id)
            .bind(consumer)
            .bind(Utc::now())
            .execute(&self.pool)
            .await
            .map_err(|e| InboxError::Storage(e.to_string()))?;
            metrics::counter!(
                "pharos.postgres.inbox.completed",
                "consumer" => consumer.to_string()
            )
            .increment(1);
            ensure_inbox_updated(message_id, consumer, r.rows_affected())
        }
        .instrument(info_span!("postgres.inbox.mark_completed", %message_id, consumer))
        .await
    }

    async fn mark_failed(
        &self,
        message_id: Uuid,
        consumer: &str,
        error: String,
    ) -> Result<(), InboxError> {
        async move {
            let r = sqlx::query(
                "UPDATE pharos_inbox
                 SET status = 'failed', updated_at = $3, last_error = $4
                 WHERE message_id = $1 AND consumer = $2",
            )
            .bind(message_id)
            .bind(consumer)
            .bind(Utc::now())
            .bind(&error)
            .execute(&self.pool)
            .await
            .map_err(|e| InboxError::Storage(e.to_string()))?;
            metrics::counter!(
                "pharos.postgres.inbox.failed",
                "consumer" => consumer.to_string()
            )
            .increment(1);
            ensure_inbox_updated(message_id, consumer, r.rows_affected())
        }
        .instrument(info_span!("postgres.inbox.mark_failed", %message_id, consumer))
        .await
    }

    async fn get(
        &self,
        message_id: Uuid,
        consumer: &str,
    ) -> Result<Option<InboxMessage>, InboxError> {
        async move {
            let row = sqlx::query(
                "SELECT message_id, consumer, status, received_at, updated_at, last_error
                 FROM pharos_inbox WHERE message_id = $1 AND consumer = $2",
            )
            .bind(message_id)
            .bind(consumer)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| InboxError::Storage(e.to_string()))?;
            row.map(row_to_inbox_message).transpose()
        }
        .instrument(info_span!("postgres.inbox.get", %message_id, consumer))
        .await
    }
}

// ─── helpers ──────────────────────────────────────────────────────────────────

fn status_to_str(status: OutboxStatus) -> &'static str {
    match status {
        OutboxStatus::Pending => "pending",
        OutboxStatus::Published => "published",
        OutboxStatus::Failed => "failed",
    }
}

fn str_to_outbox_status(s: &str) -> Result<OutboxStatus, OutboxError> {
    match s {
        "pending" => Ok(OutboxStatus::Pending),
        "published" => Ok(OutboxStatus::Published),
        "failed" => Ok(OutboxStatus::Failed),
        other => Err(OutboxError::Storage(format!(
            "unknown outbox status: {other}"
        ))),
    }
}

fn str_to_inbox_status(s: &str) -> Result<InboxStatus, InboxError> {
    match s {
        "processing" => Ok(InboxStatus::Processing),
        "completed" => Ok(InboxStatus::Completed),
        "failed" => Ok(InboxStatus::Failed),
        other => Err(InboxError::Storage(format!(
            "unknown inbox status: {other}"
        ))),
    }
}

fn row_to_outbox_message(row: PgRow) -> Result<OutboxMessage, OutboxError> {
    let headers: Value = row
        .try_get::<sqlx::types::Json<Value>, _>("headers")
        .map(|j| j.0)
        .map_err(|e| OutboxError::Storage(e.to_string()))?;
    let status: String = row
        .try_get("status")
        .map_err(|e| OutboxError::Storage(e.to_string()))?;
    let attempts: i32 = row
        .try_get("attempts")
        .map_err(|e| OutboxError::Storage(e.to_string()))?;
    Ok(OutboxMessage {
        id: row
            .try_get("id")
            .map_err(|e| OutboxError::Storage(e.to_string()))?,
        message: Message {
            message_id: row
                .try_get("message_id")
                .map_err(|e| OutboxError::Storage(e.to_string()))?,
            topic: row
                .try_get("topic")
                .map_err(|e| OutboxError::Storage(e.to_string()))?,
            key: row
                .try_get("message_key")
                .map_err(|e| OutboxError::Storage(e.to_string()))?,
            headers: serde_json::from_value(headers)
                .map_err(|e| OutboxError::Storage(e.to_string()))?,
            payload: row
                .try_get("payload")
                .map_err(|e| OutboxError::Storage(e.to_string()))?,
            content_type: row
                .try_get("content_type")
                .map_err(|e| OutboxError::Storage(e.to_string()))?,
        },
        status: str_to_outbox_status(&status)?,
        attempts: attempts as u32,
        created_at: row
            .try_get("created_at")
            .map_err(|e| OutboxError::Storage(e.to_string()))?,
        updated_at: row
            .try_get("updated_at")
            .map_err(|e| OutboxError::Storage(e.to_string()))?,
        last_error: row
            .try_get("last_error")
            .map_err(|e| OutboxError::Storage(e.to_string()))?,
    })
}

fn row_to_inbox_message(row: PgRow) -> Result<InboxMessage, InboxError> {
    let status: String = row
        .try_get("status")
        .map_err(|e| InboxError::Storage(e.to_string()))?;
    Ok(InboxMessage {
        message_id: row
            .try_get("message_id")
            .map_err(|e| InboxError::Storage(e.to_string()))?,
        consumer: row
            .try_get("consumer")
            .map_err(|e| InboxError::Storage(e.to_string()))?,
        status: str_to_inbox_status(&status)?,
        received_at: row
            .try_get("received_at")
            .map_err(|e| InboxError::Storage(e.to_string()))?,
        updated_at: row
            .try_get("updated_at")
            .map_err(|e| InboxError::Storage(e.to_string()))?,
        last_error: row
            .try_get("last_error")
            .map_err(|e| InboxError::Storage(e.to_string()))?,
    })
}

fn ensure_outbox_updated(id: Uuid, affected: u64) -> Result<(), OutboxError> {
    if affected == 0 {
        return Err(OutboxError::NotFound(id));
    }
    Ok(())
}

fn ensure_inbox_updated(message_id: Uuid, consumer: &str, affected: u64) -> Result<(), InboxError> {
    if affected == 0 {
        return Err(InboxError::NotFound {
            message_id,
            consumer: consumer.to_string(),
        });
    }
    Ok(())
}
