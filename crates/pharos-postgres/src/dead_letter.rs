use pharos_app::{DeadLetterError, DeadLetterMessage, DeadLetterQueue, Message};
use serde_json::Value;
use sqlx::Row;
use tracing::{Instrument, info_span};

use crate::pool::{PgPoolError, Pool};

/// Default PostgreSQL schema for the dead-letter queue.
pub const POSTGRES_DEAD_LETTER_SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS pharos_dead_letter (
    id UUID PRIMARY KEY,
    message_id UUID NOT NULL,
    topic TEXT NOT NULL,
    message_key TEXT NULL,
    headers JSONB NOT NULL DEFAULT '{}'::jsonb,
    payload BYTEA NOT NULL,
    content_type TEXT NOT NULL,
    reason TEXT NOT NULL,
    attempts INTEGER NOT NULL,
    dead_lettered_at TIMESTAMPTZ NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_pharos_dead_lettered_at
    ON pharos_dead_letter (dead_lettered_at DESC);
"#;

/// Installs the dead-letter schema.
pub async fn migrate_postgres_dead_letter_schema(pool: &Pool) -> Result<(), PgPoolError> {
    sqlx::raw_sql(POSTGRES_DEAD_LETTER_SCHEMA)
        .execute(pool)
        .await?;
    Ok(())
}

/// PostgreSQL implementation of [`DeadLetterQueue`].
#[derive(Debug, Clone)]
pub struct PostgresDeadLetterQueue {
    pool: Pool,
}

impl PostgresDeadLetterQueue {
    pub fn new(pool: Pool) -> Self {
        Self { pool }
    }
    pub fn pool(&self) -> &Pool {
        &self.pool
    }
    pub async fn migrate(&self) -> Result<(), PgPoolError> {
        migrate_postgres_dead_letter_schema(&self.pool).await
    }
}

impl DeadLetterQueue for PostgresDeadLetterQueue {
    async fn dead_letter(&self, message: DeadLetterMessage) -> Result<(), DeadLetterError> {
        async move {
            let headers = serde_json::to_value(&message.message.headers)
                .map_err(|e| DeadLetterError::Storage(e.to_string()))?;
            sqlx::query(
                "INSERT INTO pharos_dead_letter (
                    id, message_id, topic, message_key, headers, payload, content_type,
                    reason, attempts, dead_lettered_at
                ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)",
            )
            .bind(message.id)
            .bind(message.message.message_id)
            .bind(&message.message.topic)
            .bind(&message.message.key)
            .bind(sqlx::types::Json(&headers))
            .bind(&message.message.payload)
            .bind(&message.message.content_type)
            .bind(&message.reason)
            .bind(message.attempts as i32)
            .bind(message.dead_lettered_at)
            .execute(&self.pool)
            .await
            .map_err(|e| DeadLetterError::Storage(e.to_string()))?;
            metrics::counter!("pharos.postgres.dead_letter.inserted").increment(1);
            Ok(())
        }
        .instrument(info_span!("postgres.dead_letter.insert"))
        .await
    }

    async fn list(&self, limit: usize) -> Result<Vec<DeadLetterMessage>, DeadLetterError> {
        async move {
            let rows = sqlx::query(
                "SELECT id, message_id, topic, message_key, headers, payload, content_type,
                        reason, attempts, dead_lettered_at
                 FROM pharos_dead_letter
                 ORDER BY dead_lettered_at DESC
                 LIMIT $1",
            )
            .bind(limit as i64)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| DeadLetterError::Storage(e.to_string()))?;
            rows.into_iter()
                .map(|row| {
                    let headers: Value = row
                        .try_get::<sqlx::types::Json<Value>, _>("headers")
                        .map(|j| j.0)
                        .map_err(|e| DeadLetterError::Storage(e.to_string()))?;
                    let attempts: i32 = row
                        .try_get("attempts")
                        .map_err(|e| DeadLetterError::Storage(e.to_string()))?;
                    Ok(DeadLetterMessage {
                        id: row
                            .try_get("id")
                            .map_err(|e| DeadLetterError::Storage(e.to_string()))?,
                        message: Message {
                            message_id: row
                                .try_get("message_id")
                                .map_err(|e| DeadLetterError::Storage(e.to_string()))?,
                            topic: row
                                .try_get("topic")
                                .map_err(|e| DeadLetterError::Storage(e.to_string()))?,
                            key: row
                                .try_get("message_key")
                                .map_err(|e| DeadLetterError::Storage(e.to_string()))?,
                            headers: serde_json::from_value(headers)
                                .map_err(|e| DeadLetterError::Storage(e.to_string()))?,
                            payload: row
                                .try_get("payload")
                                .map_err(|e| DeadLetterError::Storage(e.to_string()))?,
                            content_type: row
                                .try_get("content_type")
                                .map_err(|e| DeadLetterError::Storage(e.to_string()))?,
                        },
                        reason: row
                            .try_get("reason")
                            .map_err(|e| DeadLetterError::Storage(e.to_string()))?,
                        attempts: attempts as u32,
                        dead_lettered_at: row
                            .try_get("dead_lettered_at")
                            .map_err(|e| DeadLetterError::Storage(e.to_string()))?,
                    })
                })
                .collect()
        }
        .instrument(info_span!("postgres.dead_letter.list", limit))
        .await
    }
}
