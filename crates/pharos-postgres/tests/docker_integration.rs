use chrono::{DateTime, Utc};
use pharos_app::{
    DeadLetterMessage, DeadLetterQueue, IdempotencyDecision, InboxStore, Message, OutboxMessage,
    OutboxRepository, TenantContext,
};
use pharos_core::{AggregateRoot, DomainEvent, Entity, Repository};
use pharos_postgres::{
    Pool, PostgresDeadLetterQueue, PostgresInboxStore, PostgresJsonRepository,
    PostgresOutboxRepository, PostgresTransactionError, PostgresUnitOfWork, TenantJsonRepository,
    connect_pool, migrate_postgres_aggregate_schema, migrate_postgres_dead_letter_schema,
    migrate_postgres_eventing_schema, migrate_postgres_tenant_aggregate_schema,
    save_aggregate_and_enqueue,
};
use serde::{Deserialize, Serialize};
use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::{ContainerAsync, GenericImage, ImageExt, runners::AsyncRunner};
use uuid::Uuid;

const POSTGRES_IMAGE: &str = "postgres";
const POSTGRES_TAG: &str = "16-alpine";

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

#[tokio::test]
async fn postgres_outbox_repository_works_against_real_database() -> TestResult {
    let (_container, pool) = start_postgres().await?;
    let repo = PostgresOutboxRepository::new(pool);
    repo.migrate().await?;

    let outbox = OutboxMessage::new(
        Message::new(
            "orders",
            br#"{"order_id":"order-1"}"#.to_vec(),
            "application/json",
        )
        .with_key("order-1")
        .with_header("correlation_id", "corr-1"),
    );
    let outbox_id = outbox.id;

    repo.insert(outbox).await?;

    let pending = repo.pending(10).await?;
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].id, outbox_id);
    assert_eq!(pending[0].message.topic, "orders");
    assert_eq!(pending[0].message.key.as_deref(), Some("order-1"));
    assert_eq!(
        pending[0]
            .message
            .headers
            .get("correlation_id")
            .map(String::as_str),
        Some("corr-1")
    );

    repo.record_attempt(outbox_id).await?;
    repo.mark_published(outbox_id).await?;
    assert!(repo.pending(10).await?.is_empty());
    Ok(())
}

#[tokio::test]
async fn postgres_inbox_store_enforces_idempotency_against_real_database() -> TestResult {
    let (_container, pool) = start_postgres().await?;
    let store = PostgresInboxStore::new(pool);
    store.migrate().await?;
    let message_id = Uuid::now_v7();

    assert_eq!(
        store.begin_processing(message_id, "billing").await?,
        IdempotencyDecision::StartProcessing
    );
    assert_eq!(
        store.begin_processing(message_id, "billing").await?,
        IdempotencyDecision::AlreadyProcessing
    );
    store
        .mark_failed(message_id, "billing", "temporary failure".to_string())
        .await?;
    assert_eq!(
        store.begin_processing(message_id, "billing").await?,
        IdempotencyDecision::RetryPreviousFailure
    );
    store.mark_completed(message_id, "billing").await?;
    assert_eq!(
        store.begin_processing(message_id, "billing").await?,
        IdempotencyDecision::AlreadyCompleted
    );
    Ok(())
}

#[tokio::test]
async fn postgres_json_repository_persists_aggregates_against_real_database() -> TestResult {
    let (_container, pool) = start_postgres().await?;
    let repo = PostgresJsonRepository::<TestAggregate>::with_aggregate_type(pool, "test_aggregate");
    repo.migrate().await?;
    let mut aggregate = TestAggregate {
        id: "aggregate-1".to_string(),
        name: "First aggregate".to_string(),
        version: 0,
        events: vec![TestEvent {
            aggregate_id: "aggregate-1".to_string(),
            occurred_at: Utc::now(),
        }],
    };

    repo.save(&mut aggregate).await?;
    assert_eq!(aggregate.version(), 1);

    let found = repo.find_by_id(&"aggregate-1".to_string()).await?;
    assert_eq!(found.map(|a| a.name), Some("First aggregate".to_string()));

    repo.delete(&"aggregate-1".to_string()).await?;
    assert!(repo.find_by_id(&"aggregate-1".to_string()).await?.is_none());
    Ok(())
}

#[tokio::test]
async fn save_and_enqueue_commits_aggregate_and_outbox_atomically() -> TestResult {
    let (_container, pool) = start_postgres().await?;
    migrate_postgres_aggregate_schema(&pool).await?;
    migrate_postgres_eventing_schema(&pool).await?;

    let mut aggregate = TestAggregate {
        id: "agg-atomic".to_string(),
        name: "Atomic aggregate".to_string(),
        version: 0,
        events: vec![
            TestEvent {
                aggregate_id: "agg-atomic".to_string(),
                occurred_at: Utc::now(),
            },
            TestEvent {
                aggregate_id: "agg-atomic".to_string(),
                occurred_at: Utc::now(),
            },
        ],
    };

    save_aggregate_and_enqueue(&pool, "test_aggregate", &mut aggregate, |event| {
        Message::new(
            "orders",
            event.aggregate_id.as_bytes().to_vec(),
            "text/plain",
        )
        .with_key(&event.aggregate_id)
    })
    .await?;

    assert_eq!(aggregate.version(), 1);

    let repo = PostgresJsonRepository::<TestAggregate>::with_aggregate_type(
        pool.clone(),
        "test_aggregate",
    );
    let found = repo.find_by_id(&"agg-atomic".to_string()).await?;
    assert_eq!(found.map(|a| a.name), Some("Atomic aggregate".to_string()));

    let outbox = PostgresOutboxRepository::new(pool);
    let pending = outbox.pending(10).await?;
    assert_eq!(pending.len(), 2);
    Ok(())
}

#[tokio::test]
async fn save_and_enqueue_rolls_back_outbox_on_concurrency_conflict() -> TestResult {
    let (_container, pool) = start_postgres().await?;
    migrate_postgres_aggregate_schema(&pool).await?;
    migrate_postgres_eventing_schema(&pool).await?;

    let mut aggregate = TestAggregate {
        id: "agg-occ".to_string(),
        name: "Original".to_string(),
        version: 0,
        events: vec![],
    };
    save_aggregate_and_enqueue(&pool, "test_aggregate", &mut aggregate, map_to_message).await?;

    let mut stale = TestAggregate {
        id: "agg-occ".to_string(),
        name: "Stale".to_string(),
        version: 0,
        events: vec![TestEvent {
            aggregate_id: "agg-occ".to_string(),
            occurred_at: Utc::now(),
        }],
    };
    let result =
        save_aggregate_and_enqueue(&pool, "test_aggregate", &mut stale, map_to_message).await;
    assert!(matches!(
        result,
        Err(PostgresTransactionError::ConcurrencyConflict { expected: 0, .. })
    ));

    let outbox = PostgresOutboxRepository::new(pool.clone());
    assert!(outbox.pending(10).await?.is_empty());

    let repo = PostgresJsonRepository::<TestAggregate>::with_aggregate_type(pool, "test_aggregate");
    let found = repo.find_by_id(&"agg-occ".to_string()).await?;
    assert_eq!(found.map(|a| a.name), Some("Original".to_string()));
    Ok(())
}

#[tokio::test]
async fn postgres_unit_of_work_commits_and_rolls_back() -> TestResult {
    let (_container, pool) = start_postgres().await?;
    // Create a scratch table directly via sqlx
    sqlx::query("CREATE TABLE uow_scratch (note TEXT NOT NULL)")
        .execute(&pool)
        .await?;

    let uow = PostgresUnitOfWork::new(pool.clone());

    // A committed transaction persists its writes.
    uow.transaction(|conn| {
        Box::pin(async move {
            sqlx::query("INSERT INTO uow_scratch (note) VALUES ($1)")
                .bind("kept")
                .execute(conn)
                .await?;
            Ok::<(), sqlx::Error>(())
        })
    })
    .await?;

    // A failing transaction rolls its writes back.
    let rolled_back = uow
        .transaction(|conn| {
            Box::pin(async move {
                sqlx::query("INSERT INTO uow_scratch (note) VALUES ($1)")
                    .bind("dropped")
                    .execute(&mut *conn)
                    .await?;
                // Execute against a missing column to force an error
                sqlx::query("INSERT INTO uow_scratch (missing) VALUES ($1)")
                    .bind("x")
                    .execute(&mut *conn)
                    .await?;
                Ok::<(), sqlx::Error>(())
            })
        })
        .await;
    assert!(rolled_back.is_err());

    let notes: i64 = sqlx::query_scalar("SELECT count(*) FROM uow_scratch")
        .fetch_one(&pool)
        .await?;
    assert_eq!(notes, 1, "only the committed row should remain");
    Ok(())
}

#[tokio::test]
async fn tenant_json_repository_isolates_rows_by_tenant() -> TestResult {
    let (_container, pool) = start_postgres().await?;
    migrate_postgres_tenant_aggregate_schema(&pool).await?;

    let tenant_a = TenantContext::new("tenant-a");
    let tenant_b = TenantContext::new("tenant-b");
    let repo_a =
        TenantJsonRepository::<TestAggregate>::new(pool.clone(), &tenant_a, "test_aggregate");
    let repo_b =
        TenantJsonRepository::<TestAggregate>::new(pool.clone(), &tenant_b, "test_aggregate");

    let mut a = TestAggregate {
        id: "shared-id".to_string(),
        name: "A's data".to_string(),
        version: 0,
        events: vec![],
    };
    repo_a.save(&mut a).await?;
    let mut b = TestAggregate {
        id: "shared-id".to_string(),
        name: "B's data".to_string(),
        version: 0,
        events: vec![],
    };
    repo_b.save(&mut b).await?;

    let from_a = repo_a.find_by_id(&"shared-id".to_string()).await?;
    let from_b = repo_b.find_by_id(&"shared-id".to_string()).await?;
    assert_eq!(from_a.map(|x| x.name), Some("A's data".to_string()));
    assert_eq!(from_b.map(|x| x.name), Some("B's data".to_string()));

    repo_a.delete(&"shared-id".to_string()).await?;
    assert!(repo_a.find_by_id(&"shared-id".to_string()).await?.is_none());
    assert!(repo_b.find_by_id(&"shared-id".to_string()).await?.is_some());
    Ok(())
}

#[tokio::test]
async fn postgres_dead_letter_queue_persists_messages_against_real_database() -> TestResult {
    let (_container, pool) = start_postgres().await?;
    migrate_postgres_dead_letter_schema(&pool).await?;
    let queue = PostgresDeadLetterQueue::new(pool);

    let message = Message::new(
        "orders",
        br#"{"order_id":"order-dead"}"#.to_vec(),
        "application/json",
    )
    .with_key("order-dead")
    .with_header("correlation_id", "corr-dead");
    queue
        .dead_letter(DeadLetterMessage::new(message, "poison message", 5))
        .await?;

    let listed = queue.list(10).await?;
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].reason, "poison message");
    assert_eq!(listed[0].attempts, 5);
    assert_eq!(listed[0].message.topic, "orders");
    assert_eq!(listed[0].message.key.as_deref(), Some("order-dead"));
    Ok(())
}

#[tokio::test]
async fn outbox_cleanup_removes_old_published_rows() -> TestResult {
    let (_container, pool) = start_postgres().await?;
    let repo = PostgresOutboxRepository::new(pool);
    repo.migrate().await?;

    let msg = OutboxMessage::new(Message::new("test", b"payload".to_vec(), "text/plain"));
    let id = msg.id;
    repo.insert(msg).await?;
    repo.mark_published(id).await?;

    // Zero-duration cutoff to delete everything published/failed immediately
    let deleted = repo
        .cleanup_older_than(std::time::Duration::from_secs(0))
        .await?;
    assert_eq!(deleted, 1);
    Ok(())
}

fn map_to_message(event: &TestEvent) -> Message {
    Message::new(
        "orders",
        event.aggregate_id.as_bytes().to_vec(),
        "text/plain",
    )
    .with_key(&event.aggregate_id)
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
