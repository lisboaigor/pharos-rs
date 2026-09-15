//! Docker-backed coverage for `PostgresAggregateStore`, in both delivery
//! modes: inline (in-process `EventBus` dispatch) and outbox (durable,
//! atomic snapshot + outbox insert).

use std::convert::Infallible;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use chrono::{DateTime, Utc};
use pharos_app::{
    AggregateStore, CURRENT_TENANT, EventBus, EventHandler, MessageEnricher, OutboxRepository,
    OutboxSignal, StoreError, TenantContext,
};
use pharos_core::{AggregateRoot, DomainEvent, Entity};
use pharos_postgres::{
    Pool, PostgresAggregateStore, PostgresOutboxRepository, connect_pool,
    migrate_postgres_eventing_schema, migrate_postgres_tenant_aggregate_schema,
};
use serde::{Deserialize, Serialize};
use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::{ContainerAsync, GenericImage, ImageExt, runners::AsyncRunner};
use uuid::Uuid;

const POSTGRES_IMAGE: &str = "postgres";
const POSTGRES_TAG: &str = "16-alpine";
const TOPIC: &str = "TestEvent";

type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct TestAggregate {
    id: String,
    name: String,
    #[serde(default)]
    version: u64,
    #[serde(skip)]
    events: Vec<TestEvent>,
}

impl TestAggregate {
    fn new(name: impl Into<String>) -> Self {
        let id = Uuid::now_v7().to_string();
        Self {
            events: vec![TestEvent {
                aggregate_id: id.clone(),
                occurred_at: Utc::now(),
            }],
            id,
            name: name.into(),
            version: 0,
        }
    }
}

impl Entity for TestAggregate {
    type Id = String;
    fn id(&self) -> &Self::Id {
        &self.id
    }
}

impl AggregateRoot for TestAggregate {
    type Event = TestEvent;
    fn pending_events(&self) -> &[Self::Event] {
        &self.events
    }
    fn drain_events(&mut self) -> Vec<Self::Event> {
        std::mem::take(&mut self.events)
    }
    fn restore_events(&mut self, events: Vec<Self::Event>) {
        let raised_since = std::mem::replace(&mut self.events, events);
        self.events.extend(raised_since);
    }
    fn version(&self) -> u64 {
        self.version
    }
    fn set_version(&mut self, version: u64) {
        self.version = version;
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct TestEvent {
    aggregate_id: String,
    occurred_at: DateTime<Utc>,
}

impl DomainEvent for TestEvent {
    fn event_type(&self) -> &'static str {
        "TestEvent"
    }
    fn occurred_at(&self) -> DateTime<Utc> {
        self.occurred_at
    }
    fn aggregate_id(&self) -> &str {
        &self.aggregate_id
    }
}

/// Stamps a fixed header, standing in for a real enricher (tenant, trace) in
/// tests that only need to prove enrichers run at all.
struct FixedHeader;

impl MessageEnricher for FixedHeader {
    fn enrich(&self, message: &mut pharos_app::Message) {
        message
            .headers
            .insert("x-test-enricher".to_string(), "applied".to_string());
    }
}

struct CountingHandler(Arc<AtomicUsize>);

impl EventHandler<TestEvent> for CountingHandler {
    type Error = Infallible;
    async fn handle(&self, _event: &TestEvent) -> Result<(), Infallible> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

async fn start_postgres()
-> Result<(ContainerAsync<GenericImage>, Pool), Box<dyn std::error::Error + Send + Sync>> {
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
    let connection_string = format!("postgres://postgres:postgres@{host}:{port}/postgres");

    let pool = connect_pool(&connection_string, 8)?;

    Ok((container, pool))
}

#[tokio::test]
async fn inline_delivery_saves_and_dispatches_to_the_event_bus_synchronously() -> TestResult {
    let (_container, pool) = start_postgres().await?;
    migrate_postgres_tenant_aggregate_schema(&pool).await?;

    let seen = Arc::new(AtomicUsize::new(0));
    let bus = EventBus::new();
    bus.register(CountingHandler(seen.clone()));

    let store = PostgresAggregateStore::<TestAggregate>::new(pool.clone(), "TestAggregate", bus);

    let tenant = TenantContext::new(Uuid::now_v7());
    CURRENT_TENANT
        .scope(Some(tenant), async {
            let mut aggregate = TestAggregate::new("inline");
            let id = aggregate.id().clone();

            store.save(&mut aggregate).await?;
            assert_eq!(aggregate.version(), 1);
            assert!(aggregate.pending_events().is_empty());
            assert_eq!(seen.load(Ordering::SeqCst), 1, "handler ran inline");

            let loaded = store.load(&id).await?;
            assert_eq!(loaded.name, "inline");
            Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
        })
        .await
}

#[tokio::test]
async fn outbox_delivery_commits_snapshot_and_enriched_outbox_rows_and_signals() -> TestResult {
    let (_container, pool) = start_postgres().await?;
    migrate_postgres_tenant_aggregate_schema(&pool).await?;
    migrate_postgres_eventing_schema(&pool).await?;

    let bus = EventBus::new();
    let signal = OutboxSignal::new();
    let store = PostgresAggregateStore::<TestAggregate>::new(pool.clone(), "TestAggregate", bus)
        .with_outbox(TOPIC, Some(signal.clone()))
        .with_enricher(Arc::new(FixedHeader));

    let tenant = TenantContext::new(Uuid::now_v7());
    CURRENT_TENANT
        .scope(Some(tenant), async {
            let mut aggregate = TestAggregate::new("outbox");
            let id = aggregate.id().clone();

            store.save(&mut aggregate).await?;
            assert_eq!(aggregate.version(), 1);
            assert!(aggregate.pending_events().is_empty());

            // The snapshot is there...
            let loaded = store.load(&id).await?;
            assert_eq!(loaded.name, "outbox");

            // ...and so is exactly one enriched, correctly-keyed outbox row.
            let outbox = PostgresOutboxRepository::new(pool.clone());
            let pending = outbox.pending(10).await?;
            assert_eq!(pending.len(), 1);
            let message = &pending[0].message;
            assert_eq!(message.topic, TOPIC);
            assert_eq!(message.key.as_deref(), Some(id.as_str()));
            assert_eq!(
                message.headers.get("x-test-enricher").map(String::as_str),
                Some("applied")
            );
            assert_eq!(
                message.headers.get("event_type").map(String::as_str),
                Some("TestEvent")
            );

            // The signal fires after commit — `notified()` resolves without
            // blocking because `save` already called `notify()`.
            tokio::time::timeout(std::time::Duration::from_millis(50), signal.notified()).await?;

            Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
        })
        .await
}

#[tokio::test]
async fn a_concurrency_conflict_is_reported_and_leaves_events_and_version_intact() -> TestResult {
    let (_container, pool) = start_postgres().await?;
    migrate_postgres_tenant_aggregate_schema(&pool).await?;
    migrate_postgres_eventing_schema(&pool).await?;

    let bus = EventBus::new();
    let store = PostgresAggregateStore::<TestAggregate>::new(pool.clone(), "TestAggregate", bus)
        .with_outbox(TOPIC, None);

    let tenant = TenantContext::new(Uuid::now_v7());
    CURRENT_TENANT
        .scope(Some(tenant), async {
            let mut aggregate = TestAggregate::new("first save");
            store.save(&mut aggregate).await?;
            assert_eq!(aggregate.version(), 1);

            // A second in-memory copy at the stale version races the first.
            let mut stale = aggregate.clone();
            stale.version = 0;
            stale.events.push(TestEvent {
                aggregate_id: stale.id.clone(),
                occurred_at: Utc::now(),
            });

            let result = store.save(&mut stale).await;
            assert!(
                matches!(
                    result,
                    Err(StoreError::ConcurrencyConflict { expected: 0, .. })
                ),
                "expected a concurrency conflict, got {result:?}"
            );
            assert_eq!(stale.version, 0, "version reverted on conflict");
            assert_eq!(stale.events.len(), 1, "the event was not drained");

            Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
        })
        .await
}

#[tokio::test]
async fn loading_an_unknown_id_reports_not_found() -> TestResult {
    let (_container, pool) = start_postgres().await?;
    migrate_postgres_tenant_aggregate_schema(&pool).await?;

    let bus = EventBus::new();
    let store = PostgresAggregateStore::<TestAggregate>::new(pool, "TestAggregate", bus);

    let tenant = TenantContext::new(Uuid::now_v7());
    CURRENT_TENANT
        .scope(Some(tenant), async {
            let result = store.load(&Uuid::now_v7().to_string()).await;
            assert!(matches!(result, Err(StoreError::NotFound)));
            Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
        })
        .await
}
