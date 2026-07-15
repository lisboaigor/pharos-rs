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
///
/// `next_attempt_at` doubles as the retry-backoff schedule and the claim
/// lease: `pending` only returns rows whose `next_attempt_at` is due and
/// pushes it forward while a dispatcher holds the message. The `ALTER TABLE`
/// statement upgrades installations created before the column existed.
pub const POSTGRES_EVENTING_SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS pharos_outbox (
    id UUID PRIMARY KEY,
    message_id UUID NOT NULL,
    topic TEXT NOT NULL,
    message_key TEXT NULL,
    headers JSONB NOT NULL DEFAULT '{}'::jsonb,
    payload BYTEA NOT NULL,
    content_type TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('pending', 'published', 'failed', 'dead_lettered')),
    attempts INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_error TEXT NULL
);
ALTER TABLE pharos_outbox
    ADD COLUMN IF NOT EXISTS next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now();
CREATE INDEX IF NOT EXISTS idx_pharos_outbox_pending_created_at
    ON pharos_outbox (created_at)
    WHERE status = 'pending';
CREATE INDEX IF NOT EXISTS idx_pharos_outbox_pending_next_attempt_at
    ON pharos_outbox (next_attempt_at)
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
///
/// `pending` claims rows atomically: it advances their `next_attempt_at` by
/// the claim lease inside the same statement that selects them (with
/// `FOR UPDATE SKIP LOCKED`), so concurrent dispatchers always receive
/// disjoint batches. If a dispatcher crashes mid-batch, its rows become
/// claimable again after the lease expires.
#[derive(Debug, Clone)]
pub struct PostgresOutboxRepository {
    pool: Pool,
    claim_lease: Duration,
}

/// How long a claimed pending row stays invisible to other dispatchers.
const DEFAULT_CLAIM_LEASE: Duration = Duration::from_secs(30);

impl PostgresOutboxRepository {
    pub fn new(pool: Pool) -> Self {
        Self {
            pool,
            claim_lease: DEFAULT_CLAIM_LEASE,
        }
    }

    /// Overrides the claim lease applied by [`OutboxRepository::pending`].
    ///
    /// Size it above the worst-case time to publish one batch; a lease that is
    /// too short lets a second dispatcher re-claim messages that are still
    /// being published, reintroducing duplicates.
    pub fn with_claim_lease(mut self, claim_lease: Duration) -> Self {
        self.claim_lease = claim_lease;
        self
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
        let cutoff =
            Utc::now() - chrono::Duration::from_std(older_than).map_err(OutboxError::storage)?;
        let result = sqlx::query(
            "DELETE FROM pharos_outbox
             WHERE status IN ('published', 'failed') AND updated_at < $1",
        )
        .bind(cutoff)
        .execute(&self.pool)
        .await
        .map_err(OutboxError::storage)?;
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
            let headers =
                serde_json::to_value(&message.message.headers).map_err(OutboxError::storage)?;
            sqlx::query(
                "INSERT INTO pharos_outbox (
                    id, message_id, topic, message_key, headers, payload, content_type,
                    status, attempts, created_at, updated_at, next_attempt_at, last_error
                ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)",
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
            .bind(message.next_attempt_at)
            .bind(&message.last_error)
            .execute(&self.pool)
            .await
            .map_err(OutboxError::storage)?;
            metrics::counter!("pharos.postgres.outbox.inserted").increment(1);
            Ok(())
        }
        .instrument(info_span!("postgres.outbox.insert"))
        .await
    }

    async fn pending(&self, limit: usize) -> Result<Vec<OutboxMessage>, OutboxError> {
        async move {
            let now = Utc::now();
            let lease_until =
                now + chrono::Duration::from_std(self.claim_lease).map_err(OutboxError::storage)?;
            // Claim and lease in one atomic statement: the sub-select locks the
            // due rows (skipping ones another dispatcher already locked) and
            // the UPDATE pushes their next_attempt_at past the lease, so no
            // other dispatcher can claim them again until the lease expires.
            // Running this as two separate statements would drop the row locks
            // at the end of the SELECT's implicit transaction and reintroduce
            // duplicate publishes.
            let rows = sqlx::query(
                "UPDATE pharos_outbox
                 SET next_attempt_at = $2, updated_at = $3
                 WHERE id IN (
                     SELECT id FROM pharos_outbox
                     WHERE status = 'pending' AND next_attempt_at <= $3
                     ORDER BY created_at ASC
                     LIMIT $1
                     FOR UPDATE SKIP LOCKED
                 )
                 RETURNING id, message_id, topic, message_key, headers, payload, content_type,
                           status, attempts, created_at, updated_at, next_attempt_at, last_error",
            )
            .bind(limit as i64)
            .bind(lease_until)
            .bind(now)
            .fetch_all(&self.pool)
            .await
            .map_err(OutboxError::storage)?;
            let mut messages = rows
                .into_iter()
                .map(row_to_outbox_message)
                .collect::<Result<Vec<_>, _>>()?;
            // UPDATE ... RETURNING does not guarantee ordering.
            messages.sort_by_key(|message| message.created_at);
            Ok(messages)
        }
        .instrument(info_span!("postgres.outbox.pending", limit))
        .await
    }

    async fn schedule_retry(&self, id: Uuid, delay: Duration) -> Result<(), OutboxError> {
        async move {
            let next_attempt_at =
                Utc::now() + chrono::Duration::from_std(delay).map_err(OutboxError::storage)?;
            let r = sqlx::query(
                "UPDATE pharos_outbox SET next_attempt_at = $2, updated_at = $3 WHERE id = $1",
            )
            .bind(id)
            .bind(next_attempt_at)
            .bind(Utc::now())
            .execute(&self.pool)
            .await
            .map_err(OutboxError::storage)?;
            ensure_outbox_updated(id, r.rows_affected())
        }
        .instrument(
            info_span!("postgres.outbox.schedule_retry", %id, delay_ms = delay.as_millis() as u64),
        )
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
            .map_err(OutboxError::storage)?;
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
            .map_err(OutboxError::storage)?;
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
            .map_err(OutboxError::storage)?;
            metrics::counter!("pharos.postgres.outbox.failed").increment(1);
            ensure_outbox_updated(id, r.rows_affected())
        }
        .instrument(info_span!("postgres.outbox.mark_failed", %id))
        .await
    }

    async fn failed(&self, limit: usize) -> Result<Vec<OutboxMessage>, OutboxError> {
        async move {
            let rows = sqlx::query(
                "SELECT id, message_id, topic, message_key, headers, payload, content_type,
                        status, attempts, created_at, updated_at, next_attempt_at, last_error
                 FROM pharos_outbox
                 WHERE status = 'failed'
                 ORDER BY created_at
                 LIMIT $1",
            )
            .bind(limit as i64)
            .fetch_all(&self.pool)
            .await
            .map_err(OutboxError::storage)?;
            rows.into_iter().map(row_to_outbox_message).collect()
        }
        .instrument(info_span!("postgres.outbox.failed", limit))
        .await
    }

    async fn mark_dead_lettered(&self, id: Uuid) -> Result<(), OutboxError> {
        async move {
            let r = sqlx::query(
                "UPDATE pharos_outbox
                 SET status = 'dead_lettered', updated_at = $2
                 WHERE id = $1",
            )
            .bind(id)
            .bind(Utc::now())
            .execute(&self.pool)
            .await
            .map_err(OutboxError::storage)?;
            ensure_outbox_updated(id, r.rows_affected())
        }
        .instrument(info_span!("postgres.outbox.mark_dead_lettered", %id))
        .await
    }
}

// ─── Inbox ────────────────────────────────────────────────────────────────────

/// PostgreSQL implementation of [`InboxStore`], backed by a connection pool.
///
/// A `processing` record whose `updated_at` is older than the configured
/// stale-processing lease is treated as abandoned (the consumer crashed
/// between `begin_processing` and `mark_completed`/`mark_failed`) and can be
/// taken over by the next `begin_processing` call. Without the lease such
/// messages would stay `AlreadyProcessing` forever.
#[derive(Debug, Clone)]
pub struct PostgresInboxStore {
    pool: Pool,
    stale_after: Duration,
}

/// Default lease after which a `processing` inbox record may be reclaimed.
const DEFAULT_STALE_AFTER: Duration = Duration::from_secs(300);

impl PostgresInboxStore {
    pub fn new(pool: Pool) -> Self {
        Self {
            pool,
            stale_after: DEFAULT_STALE_AFTER,
        }
    }

    /// Overrides the stale-processing lease.
    ///
    /// Size it above the worst-case processing time of one message; a lease
    /// that is too short lets a second consumer start while the first is still
    /// working, so handlers must stay idempotent regardless.
    pub fn with_stale_after(mut self, stale_after: Duration) -> Self {
        self.stale_after = stale_after;
        self
    }

    pub fn pool(&self) -> &Pool {
        &self.pool
    }
    pub async fn migrate(&self) -> Result<(), PgPoolError> {
        migrate_postgres_eventing_schema(&self.pool).await
    }

    /// Deletes `completed` and `failed` inbox rows older than `older_than`.
    ///
    /// Schedule this periodically (e.g. daily) to prevent unbounded table
    /// growth. Returns the number of deleted rows.
    ///
    /// Deleting a record shrinks the idempotency window: a redelivery of a
    /// cleaned-up message is processed again as if it were new. Size
    /// `older_than` above the broker's maximum redelivery horizon (retention
    /// plus DLQ replay window) so that can only happen for messages the
    /// broker will no longer redeliver anyway.
    pub async fn cleanup_older_than(&self, older_than: Duration) -> Result<u64, InboxError> {
        let cutoff =
            Utc::now() - chrono::Duration::from_std(older_than).map_err(InboxError::storage)?;
        let result = sqlx::query(
            "DELETE FROM pharos_inbox
             WHERE status IN ('completed', 'failed') AND updated_at < $1",
        )
        .bind(cutoff)
        .execute(&self.pool)
        .await
        .map_err(InboxError::storage)?;
        let deleted = result.rows_affected();
        if deleted > 0 {
            metrics::counter!("pharos.postgres.inbox.cleaned_up").increment(deleted);
        }
        Ok(deleted)
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
                RETURNING status, updated_at, (xmax = 0) AS inserted",
            )
            .bind(message_id)
            .bind(consumer)
            .bind(now)
            .fetch_one(&self.pool)
            .await
            .map_err(InboxError::storage)?;

            let inserted: bool = row.try_get("inserted").map_err(InboxError::storage)?;
            if inserted {
                metrics::counter!(
                    "pharos.postgres.inbox.started",
                    "consumer" => consumer.to_string()
                )
                .increment(1);
                return Ok(IdempotencyDecision::StartProcessing);
            }
            let status: String = row.try_get("status").map_err(InboxError::storage)?;
            let updated_at: chrono::DateTime<Utc> =
                row.try_get("updated_at").map_err(InboxError::storage)?;

            match str_to_inbox_status(&status)? {
                InboxStatus::Processing => {
                    let stale_after = chrono::Duration::from_std(self.stale_after)
                        .map_err(InboxError::storage)?;
                    if updated_at > now - stale_after {
                        return Ok(IdempotencyDecision::AlreadyProcessing);
                    }
                    // The previous consumer went silent past the lease: take
                    // the record over. The compare-and-set on updated_at makes
                    // sure only one of several competing consumers wins.
                    let takeover = sqlx::query(
                        "UPDATE pharos_inbox
                         SET updated_at = $3, last_error = NULL
                         WHERE message_id = $1 AND consumer = $2
                           AND status = 'processing' AND updated_at = $4",
                    )
                    .bind(message_id)
                    .bind(consumer)
                    .bind(now)
                    .bind(updated_at)
                    .execute(&self.pool)
                    .await
                    .map_err(InboxError::storage)?;

                    if takeover.rows_affected() == 1 {
                        metrics::counter!(
                            "pharos.postgres.inbox.stale_takeover",
                            "consumer" => consumer.to_string()
                        )
                        .increment(1);
                        Ok(IdempotencyDecision::StartProcessing)
                    } else {
                        Ok(IdempotencyDecision::AlreadyProcessing)
                    }
                }
                InboxStatus::Completed => Ok(IdempotencyDecision::AlreadyCompleted),
                InboxStatus::Failed => Ok(IdempotencyDecision::RetryPreviousFailure),
            }
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
            .map_err(InboxError::storage)?;
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
            .map_err(InboxError::storage)?;
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
            .map_err(InboxError::storage)?;
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
        OutboxStatus::DeadLettered => "dead_lettered",
    }
}

fn str_to_outbox_status(s: &str) -> Result<OutboxStatus, OutboxError> {
    match s {
        "pending" => Ok(OutboxStatus::Pending),
        "published" => Ok(OutboxStatus::Published),
        "failed" => Ok(OutboxStatus::Failed),
        "dead_lettered" => Ok(OutboxStatus::DeadLettered),
        other => Err(OutboxError::storage(std::io::Error::other(format!(
            "unknown outbox status: {other}"
        )))),
    }
}

fn str_to_inbox_status(s: &str) -> Result<InboxStatus, InboxError> {
    match s {
        "processing" => Ok(InboxStatus::Processing),
        "completed" => Ok(InboxStatus::Completed),
        "failed" => Ok(InboxStatus::Failed),
        other => Err(InboxError::storage(std::io::Error::other(format!(
            "unknown inbox status: {other}"
        )))),
    }
}

fn row_to_outbox_message(row: PgRow) -> Result<OutboxMessage, OutboxError> {
    let headers: Value = row
        .try_get::<sqlx::types::Json<Value>, _>("headers")
        .map(|j| j.0)
        .map_err(OutboxError::storage)?;
    let status: String = row.try_get("status").map_err(OutboxError::storage)?;
    let attempts: i32 = row.try_get("attempts").map_err(OutboxError::storage)?;
    Ok(OutboxMessage {
        id: row.try_get("id").map_err(OutboxError::storage)?,
        message: Message {
            message_id: row.try_get("message_id").map_err(OutboxError::storage)?,
            topic: row.try_get("topic").map_err(OutboxError::storage)?,
            key: row.try_get("message_key").map_err(OutboxError::storage)?,
            headers: serde_json::from_value(headers).map_err(OutboxError::storage)?,
            payload: row.try_get("payload").map_err(OutboxError::storage)?,
            content_type: row.try_get("content_type").map_err(OutboxError::storage)?,
        },
        status: str_to_outbox_status(&status)?,
        attempts: attempts as u32,
        created_at: row.try_get("created_at").map_err(OutboxError::storage)?,
        updated_at: row.try_get("updated_at").map_err(OutboxError::storage)?,
        next_attempt_at: row
            .try_get("next_attempt_at")
            .map_err(OutboxError::storage)?,
        last_error: row.try_get("last_error").map_err(OutboxError::storage)?,
    })
}

fn row_to_inbox_message(row: PgRow) -> Result<InboxMessage, InboxError> {
    let status: String = row.try_get("status").map_err(InboxError::storage)?;
    Ok(InboxMessage {
        message_id: row.try_get("message_id").map_err(InboxError::storage)?,
        consumer: row.try_get("consumer").map_err(InboxError::storage)?,
        status: str_to_inbox_status(&status)?,
        received_at: row.try_get("received_at").map_err(InboxError::storage)?,
        updated_at: row.try_get("updated_at").map_err(InboxError::storage)?,
        last_error: row.try_get("last_error").map_err(InboxError::storage)?,
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
