use order::domain::order::{Order, OrderStatus};
use order::domain::value_objects::{CustomerId, Money, Quantity};
use order::infrastructure::PostgresOrderRepository;
use pharos_core::{Entity, Repository};
use pharos_postgres::{Pool, connect_pool};
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
