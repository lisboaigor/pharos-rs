//! Concurrency and load regression tests against the PostgreSQL outbox and
//! event-store adapters, on a real container. One container is started per
//! test.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use pharos_app::{
    DispatchConfig, IdempotencyDecision, InboxStore, Message, MessagePublisher, MessagingError,
    OutboxDispatcher, OutboxMessage, OutboxRepository, TenantContext,
};
use pharos_core::DomainEvent;
use pharos_es::EventStore;
use pharos_postgres::{
    PgEventStore, Pool, PostgresInboxStore, PostgresOutboxRepository, connect_pool,
    migrate_postgres_event_store_schema, migrate_postgres_eventing_schema,
};
use serde::{Deserialize, Serialize};
use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::{ContainerAsync, GenericImage, ImageExt, runners::AsyncRunner};
use uuid::Uuid;

type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

const POSTGRES_IMAGE: &str = "postgres";
const POSTGRES_TAG: &str = "16-alpine";

async fn start_postgres(
    max_connections: u32,
) -> Result<(ContainerAsync<GenericImage>, Pool), Box<dyn std::error::Error + Send + Sync>> {
    let container = GenericImage::new(POSTGRES_IMAGE, POSTGRES_TAG)
        .with_exposed_port(5432.tcp())
        .with_wait_for(WaitFor::message_on_stderr(
            "database system is ready to accept connections",
        ))
        .with_env_var("POSTGRES_PASSWORD", "postgres")
        .start()
        .await?;
    let host = container.get_host().await?.to_string();
    let port = container.get_host_port_ipv4(5432).await?;
    let pool = connect_pool(
        &format!("postgres://postgres:postgres@{host}:{port}/postgres"),
        max_connections,
    )?;
    Ok((container, pool))
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct Appended {
    seq: u64,
}

impl DomainEvent for Appended {
    fn event_type(&self) -> &'static str {
        "Appended"
    }
    fn occurred_at(&self) -> DateTime<Utc> {
        Utc::now()
    }
    fn aggregate_id(&self) -> &str {
        ""
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// 1. Many dispatchers against one outbox: does the claim lease hold?
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Clone)]
struct SharedOutbox(Arc<PostgresOutboxRepository>);

impl OutboxRepository for SharedOutbox {
    async fn insert(&self, message: OutboxMessage) -> Result<(), pharos_app::OutboxError> {
        self.0.insert(message).await
    }
    async fn pending(&self, limit: usize) -> Result<Vec<OutboxMessage>, pharos_app::OutboxError> {
        self.0.pending(limit).await
    }
    async fn schedule_retry(
        &self,
        id: Uuid,
        delay: Duration,
    ) -> Result<(), pharos_app::OutboxError> {
        self.0.schedule_retry(id, delay).await
    }
    async fn record_attempt(&self, id: Uuid) -> Result<(), pharos_app::OutboxError> {
        self.0.record_attempt(id).await
    }
    async fn mark_published(&self, id: Uuid) -> Result<(), pharos_app::OutboxError> {
        self.0.mark_published(id).await
    }
    async fn mark_failed(&self, id: Uuid, error: String) -> Result<(), pharos_app::OutboxError> {
        self.0.mark_failed(id, error).await
    }
    async fn failed(&self, limit: usize) -> Result<Vec<OutboxMessage>, pharos_app::OutboxError> {
        self.0.failed(limit).await
    }
    async fn mark_dead_lettered(&self, id: Uuid) -> Result<(), pharos_app::OutboxError> {
        self.0.mark_dead_lettered(id).await
    }
}

struct CountingPublisher {
    published: Arc<dashmap::DashMap<Uuid, u64>>,
    delay: Duration,
}

impl MessagePublisher for CountingPublisher {
    async fn publish(&self, message: Message) -> Result<(), MessagingError> {
        if !self.delay.is_zero() {
            tokio::time::sleep(self.delay).await;
        }
        *self.published.entry(message.message_id).or_insert(0) += 1;
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn eight_concurrent_dispatchers_publish_each_outbox_row_exactly_once() -> TestResult {
    let (_container, pool) = start_postgres(16).await?;
    migrate_postgres_eventing_schema(&pool).await?;
    let outbox = Arc::new(PostgresOutboxRepository::new(pool.clone()));

    const MESSAGES: usize = 2_000;
    for i in 0..MESSAGES {
        outbox
            .insert(OutboxMessage::new(
                Message::new("orders", vec![0u8; 256], "application/json")
                    .with_key(format!("key-{}", i % 32)),
            ))
            .await?;
    }

    let published = Arc::new(dashmap::DashMap::new());
    let started = Instant::now();
    let mut workers = Vec::new();
    for _ in 0..8 {
        let outbox = SharedOutbox(Arc::clone(&outbox));
        let published = Arc::clone(&published);
        workers.push(tokio::spawn(async move {
            let dispatcher = OutboxDispatcher::with_config(
                outbox,
                CountingPublisher {
                    published,
                    delay: Duration::from_millis(1),
                },
                DispatchConfig::default()
                    .with_batch_size(100)
                    .with_concurrency(8),
            );
            let mut errors = 0usize;
            loop {
                let result = dispatcher.dispatch_batch().await;
                errors += result.failure_count();
                if result.published == 0 && result.is_ok() {
                    break;
                }
            }
            errors
        }));
    }
    let mut errors = 0;
    for worker in workers {
        errors += worker.await?;
    }
    let elapsed = started.elapsed();

    let total: u64 = published.iter().map(|entry| *entry.value()).sum();
    let duplicated = published.iter().filter(|entry| *entry.value() > 1).count();
    let remaining: i64 =
        sqlx::query_scalar("SELECT count(*) FROM pharos_outbox WHERE status = 'pending'")
            .fetch_one(&pool)
            .await?;

    println!(
        "outbox: {MESSAGES} rows, 8 dispatchers, {:.1}s => {:.0} msg/s | distinct={} \
         total_publishes={total} duplicated={duplicated} still_pending={remaining} errors={errors}",
        elapsed.as_secs_f64(),
        MESSAGES as f64 / elapsed.as_secs_f64(),
        published.len(),
    );

    assert_eq!(published.len(), MESSAGES, "every message was published");
    assert_eq!(duplicated, 0, "the claim lease must prevent double publish");
    assert_eq!(remaining, 0, "nothing left pending");
    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────
// 2. Concurrent appenders to one event stream.
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_appenders_never_produce_gaps_or_duplicate_sequences() -> TestResult {
    let (_container, pool) = start_postgres(16).await?;
    migrate_postgres_event_store_schema(&pool).await?;

    let tenant = TenantContext::new(Uuid::now_v7());
    let stream_id = "hot-stream".to_string();

    let conflicts = Arc::new(AtomicU64::new(0));
    let committed = Arc::new(AtomicU64::new(0));
    let started = Instant::now();

    let mut workers = Vec::new();
    for worker in 0..8u64 {
        let pool = pool.clone();
        let stream_id = stream_id.clone();
        let conflicts = Arc::clone(&conflicts);
        let committed = Arc::clone(&committed);
        workers.push(tokio::spawn(async move {
            let store: PgEventStore<String, Appended> = PgEventStore::new(pool, &tenant, "orders");
            for round in 0..50u64 {
                // Read-modify-write, retried on conflict: the ordinary
                // event-sourced write loop.
                for _attempt in 0..50 {
                    let head = store
                        .load(&stream_id)
                        .await
                        .map(|events| events.len() as u64)
                        .unwrap_or(0);
                    match store
                        .append(
                            &stream_id,
                            head,
                            vec![Appended {
                                seq: worker * 1000 + round,
                            }],
                        )
                        .await
                    {
                        Ok(()) => {
                            committed.fetch_add(1, Ordering::SeqCst);
                            break;
                        }
                        Err(pharos_core::RepositoryError::ConcurrencyConflict { .. }) => {
                            conflicts.fetch_add(1, Ordering::SeqCst);
                            tokio::time::sleep(Duration::from_millis(2)).await;
                        }
                        Err(other) => panic!("unexpected append failure: {other}"),
                    }
                }
            }
        }));
    }
    for worker in workers {
        worker.await?;
    }
    let elapsed = started.elapsed();

    let rows: Vec<i64> = sqlx::query_scalar(
        "SELECT sequence FROM pharos_event_streams WHERE stream_id = $1 ORDER BY sequence",
    )
    .bind(&stream_id)
    .fetch_all(&pool)
    .await?;

    let expected: Vec<i64> = (1..=rows.len() as i64).collect();
    println!(
        "event store: {} events by 8 concurrent appenders in {:.1}s ({:.0}/s), \
         conflicts={} committed={}",
        rows.len(),
        elapsed.as_secs_f64(),
        rows.len() as f64 / elapsed.as_secs_f64(),
        conflicts.load(Ordering::SeqCst),
        committed.load(Ordering::SeqCst),
    );

    assert_eq!(rows, expected, "sequences must be dense and unique");
    assert_eq!(
        rows.len() as u64,
        committed.load(Ordering::SeqCst),
        "every committed append is present exactly once"
    );
    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────
// 3. Cross-tenant reads must find nothing, even with identical stream ids.
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn one_tenants_event_store_cannot_read_anothers_stream() -> TestResult {
    let (_container, pool) = start_postgres(8).await?;
    migrate_postgres_event_store_schema(&pool).await?;

    let tenant_a = TenantContext::new(Uuid::now_v7());
    let tenant_b = TenantContext::new(Uuid::now_v7());
    let shared_id = "order-1".to_string();

    let store_a: PgEventStore<String, Appended> =
        PgEventStore::new(pool.clone(), &tenant_a, "orders");
    let store_b: PgEventStore<String, Appended> =
        PgEventStore::new(pool.clone(), &tenant_b, "orders");

    store_a
        .append(&shared_id, 0, vec![Appended { seq: 1 }])
        .await?;

    assert!(
        store_b.load(&shared_id).await?.is_empty(),
        "tenant B must not see tenant A's stream"
    );
    // And B writing at version 0 must not collide with A's row.
    store_b
        .append(&shared_id, 0, vec![Appended { seq: 99 }])
        .await?;

    let a = store_a.load(&shared_id).await?;
    let b = store_b.load(&shared_id).await?;
    assert_eq!(a.len(), 1);
    assert_eq!(b.len(), 1);
    assert_eq!(a[0].event.seq, 1, "A's stream is untouched");
    assert_eq!(b[0].event.seq, 99);
    println!("tenant isolation: two tenants hold independent `orders/order-1` streams");
    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────
// 4. Many consumers racing on one inbox record.
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn only_one_consumer_wins_begin_processing_for_a_message() -> TestResult {
    let (_container, pool) = start_postgres(16).await?;
    migrate_postgres_eventing_schema(&pool).await?;
    let inbox = Arc::new(PostgresInboxStore::new(pool.clone()));

    let mut started = 0;
    let mut duplicates = 0;
    for _ in 0..50 {
        let message_id = Uuid::now_v7();
        let mut racers = Vec::new();
        for _ in 0..16 {
            let inbox = Arc::clone(&inbox);
            racers.push(tokio::spawn(async move {
                inbox.begin_processing(message_id, "workers").await
            }));
        }
        for racer in racers {
            match racer.await? {
                Ok(IdempotencyDecision::StartProcessing) => started += 1,
                Ok(_) => duplicates += 1,
                Err(error) => panic!("begin_processing failed: {error}"),
            }
        }
    }
    println!("inbox race: {started} winners / {duplicates} skipped across 50 messages × 16 racers");
    assert_eq!(started, 50, "exactly one consumer may start each message");
    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────
// 5. What happens when the pool is smaller than the offered concurrency.
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn saturating_the_pool_queues_rather_than_failing_fast() -> TestResult {
    let (_container, pool) = start_postgres(2).await?;
    migrate_postgres_eventing_schema(&pool).await?;
    let outbox = Arc::new(PostgresOutboxRepository::new(pool.clone()));

    // 64 callers, 2 connections, one slow statement each.
    let started = Instant::now();
    let mut callers = Vec::new();
    for _ in 0..64 {
        let outbox = Arc::clone(&outbox);
        callers.push(tokio::spawn(async move {
            let deadline = Instant::now();
            let result = outbox
                .insert(OutboxMessage::new(Message::new(
                    "orders",
                    vec![0u8; 64],
                    "application/json",
                )))
                .await;
            (deadline.elapsed(), result.is_ok())
        }));
    }
    let mut worst = Duration::ZERO;
    let mut failures = 0;
    for caller in callers {
        let (waited, ok) = caller.await?;
        worst = worst.max(waited);
        if !ok {
            failures += 1;
        }
    }
    println!(
        "pool saturation: 64 callers on a 2-connection pool, wall={:.2}s worst_wait={:.2}s \
         failures={failures} (sqlx default acquire timeout is 30s, and nothing in \
         `connect_pool` lets an app lower it)",
        started.elapsed().as_secs_f64(),
        worst.as_secs_f64(),
    );
    assert_eq!(failures, 0);
    Ok(())
}
