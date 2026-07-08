use order::domain::order::{Order, OrderStatus};
use order::domain::value_objects::{CustomerId, Money, Quantity};
use order::infrastructure::PostgresOrderRepository;
use pharos_app::{Message, OutboxRepository, OutboxStatus};
use pharos_core::{AggregateRoot, DomainEvent, Entity, Repository};
use pharos_postgres::{
    Pool, PostgresOutboxRepository, connect_pool, migrate_postgres_eventing_schema,
    save_and_enqueue_in,
};
use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::{ContainerAsync, GenericImage, ImageExt, runners::AsyncRunner};

const POSTGRES_IMAGE: &str = "postgres";
const POSTGRES_TAG: &str = "16-alpine";

type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

#[tokio::test]
async fn postgres_order_repository_persists_normalized_relational_model() -> TestResult {
    let (_container, pool) = start_postgres().await?;
    let repo = PostgresOrderRepository::new(pool.clone());
    repo.migrate().await?;

    let mut order = Order::create(CustomerId::new())?;
    let order_id = *order.id();
    order.add_item(
        "Mechanical keyboard".to_string(),
        Quantity::new(2)?,
        Money::brl(350.00)?,
    )?;
    order.add_item(
        "Mousepad".to_string(),
        Quantity::new(1)?,
        Money::brl(80.00)?,
    )?;
    order.confirm()?;

    repo.save(&mut order).await?;

    let loaded = repo
        .find_by_id(&order_id)
        .await?
        .ok_or("expected order to be loaded")?;
    assert_eq!(loaded.status(), OrderStatus::Confirmed);
    assert_eq!(loaded.items().len(), 2);
    assert_eq!(loaded.total()?.cents(), 78_000);

    let order_count: i64 = sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM orders")
        .fetch_one(&pool)
        .await?;
    let item_count: i64 = sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM order_items")
        .fetch_one(&pool)
        .await?;
    assert_eq!(order_count, 1);
    assert_eq!(item_count, 2);

    repo.delete(&order_id).await?;
    assert!(repo.find_by_id(&order_id).await?.is_none());

    let item_count_after_delete: i64 =
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM order_items")
            .fetch_one(&pool)
            .await?;
    assert_eq!(item_count_after_delete, 0);

    Ok(())
}

/// The production write path for explicit relational repositories: one
/// transaction covers the normalized order rows AND the outbox inserts.
#[tokio::test]
async fn save_and_enqueue_in_commits_relational_rows_and_outbox_atomically() -> TestResult {
    let (_container, pool) = start_postgres().await?;
    let repo = PostgresOrderRepository::new(pool.clone());
    repo.migrate().await?;
    migrate_postgres_eventing_schema(&pool).await?;
    let outbox = PostgresOutboxRepository::new(pool.clone());

    let mut order = Order::create(CustomerId::new())?;
    let order_id = *order.id();
    order.add_item(
        "Rust book".to_string(),
        Quantity::new(1)?,
        Money::brl(120.00)?,
    )?;
    order.confirm()?;
    let event_count = order.pending_events().len();
    assert_eq!(event_count, 3);

    save_and_enqueue_in(&pool, &repo, &mut order, |event| {
        Message::new(
            "order-events",
            event.aggregate_id().as_bytes().to_vec(),
            "application/json",
        )
        .with_key(event.aggregate_id())
    })
    .await?;

    // Aggregate committed, version advanced, events drained.
    assert_eq!(order.version(), 1);
    assert!(order.pending_events().is_empty());
    let loaded = repo
        .find_by_id(&order_id)
        .await?
        .ok_or("expected order to be loaded")?;
    assert_eq!(loaded.status(), OrderStatus::Confirmed);

    // Outbox rows committed in the same transaction.
    let pending = outbox.pending(10).await?;
    assert_eq!(pending.len(), event_count);
    assert!(pending.iter().all(|m| m.status == OutboxStatus::Pending));

    // A stale write must fail atomically: no aggregate change, no outbox rows.
    let mut stale = loaded;
    stale.set_version(0);
    stale.cancel("simulated staleness".to_string())?;
    let result = save_and_enqueue_in(&pool, &repo, &mut stale, |event| {
        Message::new("order-events", Vec::new(), "application/json").with_key(event.aggregate_id())
    })
    .await;
    assert!(result.is_err(), "stale save must be rejected");
    // Version reverted, events kept for a clean retry.
    assert_eq!(stale.version(), 0);
    assert!(!stale.pending_events().is_empty());
    // Count rows directly: `pending()` leases what it returns, so it cannot
    // be used as a read; the row count proves no partial outbox writes.
    let outbox_rows: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM pharos_outbox")
        .fetch_one(&pool)
        .await?;
    assert_eq!(
        outbox_rows as usize, event_count,
        "no partial outbox writes"
    );

    Ok(())
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
