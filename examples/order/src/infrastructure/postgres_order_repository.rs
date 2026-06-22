use pharos_core::{AggregateRoot, Entity, Repository, RepositoryError};
use pharos_postgres::Pool;
use sqlx::{Row, postgres::PgRow};
use thiserror::Error;
use uuid::Uuid;

use crate::domain::order::{Order, OrderItem, OrderStatus};
use crate::domain::value_objects::{CustomerId, ItemId, Money, OrderId, Quantity};

/// Relational PostgreSQL schema for the order example.
pub const POSTGRES_ORDER_SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS orders (
    id UUID PRIMARY KEY,
    customer_id UUID NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('draft', 'confirmed', 'cancelled')),
    version BIGINT NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS order_items (
    id UUID PRIMARY KEY,
    order_id UUID NOT NULL REFERENCES orders(id) ON DELETE CASCADE,
    description TEXT NOT NULL,
    quantity INTEGER NOT NULL CHECK (quantity > 0),
    unit_price_cents BIGINT NOT NULL CHECK (unit_price_cents >= 0),
    position INTEGER NOT NULL,
    UNIQUE (order_id, position)
);
CREATE INDEX IF NOT EXISTS idx_order_items_order_id_position
    ON order_items (order_id, position);
"#;

/// Installs the relational schema for the order example.
pub async fn migrate_postgres_order_schema(
    pool: &Pool,
) -> Result<(), PostgresOrderRepositoryError> {
    sqlx::raw_sql(POSTGRES_ORDER_SCHEMA)
        .execute(pool)
        .await
        .map_err(storage_error)?;
    Ok(())
}

/// Errors produced by [`PostgresOrderRepository`].
#[derive(Debug, Error)]
pub enum PostgresOrderRepositoryError {
    #[error("postgres order repository failed: {0}")]
    Storage(String),
    #[error("invalid persisted order data: {0}")]
    InvalidData(String),
}

/// Explicit relational PostgreSQL repository for the order aggregate.
#[derive(Debug, Clone)]
pub struct PostgresOrderRepository {
    pool: Pool,
}

impl PostgresOrderRepository {
    pub fn new(pool: Pool) -> Self {
        Self { pool }
    }
    pub fn pool(&self) -> &Pool {
        &self.pool
    }

    pub async fn migrate(&self) -> Result<(), PostgresOrderRepositoryError> {
        migrate_postgres_order_schema(&self.pool).await
    }
}

fn storage_error(error: impl std::fmt::Display) -> PostgresOrderRepositoryError {
    PostgresOrderRepositoryError::Storage(error.to_string())
}

impl Repository<Order> for PostgresOrderRepository {
    type Error = PostgresOrderRepositoryError;

    async fn find_by_id(&self, id: &OrderId) -> Result<Option<Order>, Self::Error> {
        let mut tx = self.pool.begin().await.map_err(storage_error)?;

        let order_row =
            sqlx::query("SELECT id, customer_id, status, version FROM orders WHERE id = $1")
                .bind(id.as_uuid())
                .fetch_optional(&mut *tx)
                .await
                .map_err(storage_error)?;

        let Some(order_row) = order_row else {
            tx.commit().await.map_err(storage_error)?;
            return Ok(None);
        };

        let item_rows = sqlx::query(
            "SELECT id, description, quantity, unit_price_cents
             FROM order_items
             WHERE order_id = $1
             ORDER BY position ASC",
        )
        .bind(id.as_uuid())
        .fetch_all(&mut *tx)
        .await
        .map_err(storage_error)?;

        let order = row_to_order(order_row, item_rows)?;
        tx.commit().await.map_err(storage_error)?;
        Ok(Some(order))
    }

    async fn save(&self, aggregate: &mut Order) -> Result<(), RepositoryError<Self::Error>> {
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| RepositoryError::Storage(storage_error(e)))?;

        let expected = aggregate.version();
        let new_version = expected + 1;
        let status = status_to_str(aggregate.status());
        let order_id = aggregate.id().as_uuid();

        let affected = if expected == 0 {
            sqlx::query(
                "INSERT INTO orders (id, customer_id, status, version, updated_at)
                 VALUES ($1, $2, $3, $4, now())
                 ON CONFLICT (id) DO NOTHING",
            )
            .bind(order_id)
            .bind(aggregate.customer_id().as_uuid())
            .bind(status)
            .bind(new_version as i64)
            .execute(&mut *tx)
            .await
        } else {
            sqlx::query(
                "UPDATE orders
                 SET customer_id = $2, status = $3, version = $4, updated_at = now()
                 WHERE id = $1 AND version = $5",
            )
            .bind(order_id)
            .bind(aggregate.customer_id().as_uuid())
            .bind(status)
            .bind(new_version as i64)
            .bind(expected as i64)
            .execute(&mut *tx)
            .await
        }
        .map_err(|e| RepositoryError::Storage(storage_error(e)))?
        .rows_affected();

        if affected == 0 {
            let actual = sqlx::query_scalar::<_, i64>("SELECT version FROM orders WHERE id = $1")
                .bind(order_id)
                .fetch_optional(&mut *tx)
                .await
                .map_err(|e| RepositoryError::Storage(storage_error(e)))?
                .map(|v| v as u64);
            let _ = tx.rollback().await;
            return Err(RepositoryError::ConcurrencyConflict { expected, actual });
        }

        sqlx::query("DELETE FROM order_items WHERE order_id = $1")
            .bind(order_id)
            .execute(&mut *tx)
            .await
            .map_err(|e| RepositoryError::Storage(storage_error(e)))?;

        for (position, item) in aggregate.items().iter().enumerate() {
            let quantity = i32::try_from(item.quantity.value()).map_err(|e| {
                RepositoryError::Storage(PostgresOrderRepositoryError::InvalidData(format!(
                    "quantity overflow: {e}"
                )))
            })?;
            let unit_price_cents = i64::try_from(item.unit_price.cents()).map_err(|e| {
                RepositoryError::Storage(PostgresOrderRepositoryError::InvalidData(format!(
                    "unit price overflow: {e}"
                )))
            })?;
            let pos = i32::try_from(position).map_err(|e| {
                RepositoryError::Storage(PostgresOrderRepositoryError::InvalidData(format!(
                    "position overflow: {e}"
                )))
            })?;

            sqlx::query(
                "INSERT INTO order_items (
                    id, order_id, description, quantity, unit_price_cents, position
                 ) VALUES ($1, $2, $3, $4, $5, $6)",
            )
            .bind(item.id.as_uuid())
            .bind(order_id)
            .bind(&item.description)
            .bind(quantity)
            .bind(unit_price_cents)
            .bind(pos)
            .execute(&mut *tx)
            .await
            .map_err(|e| RepositoryError::Storage(storage_error(e)))?;
        }

        tx.commit()
            .await
            .map_err(|e| RepositoryError::Storage(storage_error(e)))?;

        aggregate.set_version(new_version);
        Ok(())
    }

    async fn delete(&self, id: &OrderId) -> Result<(), Self::Error> {
        let mut tx = self.pool.begin().await.map_err(storage_error)?;
        sqlx::query("DELETE FROM orders WHERE id = $1")
            .bind(id.as_uuid())
            .execute(&mut *tx)
            .await
            .map_err(storage_error)?;
        tx.commit().await.map_err(storage_error)?;
        Ok(())
    }
}

fn row_to_order(
    order_row: PgRow,
    item_rows: Vec<PgRow>,
) -> Result<Order, PostgresOrderRepositoryError> {
    let order_id: Uuid = order_row.try_get("id").map_err(storage_error)?;
    let customer_id: Uuid = order_row.try_get("customer_id").map_err(storage_error)?;
    let status: String = order_row.try_get("status").map_err(storage_error)?;
    let version: i64 = order_row.try_get("version").map_err(storage_error)?;

    let items = item_rows
        .into_iter()
        .map(row_to_item)
        .collect::<Result<Vec<_>, _>>()?;

    Ok(Order::rehydrate(
        OrderId::from_uuid(order_id),
        version as u64,
        CustomerId::from_uuid(customer_id),
        items,
        str_to_status(&status)?,
    ))
}

fn row_to_item(row: PgRow) -> Result<OrderItem, PostgresOrderRepositoryError> {
    let item_id: Uuid = row.try_get("id").map_err(storage_error)?;
    let description: String = row.try_get("description").map_err(storage_error)?;
    let quantity: i32 = row.try_get("quantity").map_err(storage_error)?;
    let unit_price_cents: i64 = row.try_get("unit_price_cents").map_err(storage_error)?;

    if quantity <= 0 {
        return Err(PostgresOrderRepositoryError::InvalidData(format!(
            "quantity must be positive, got {quantity}"
        )));
    }
    if unit_price_cents < 0 {
        return Err(PostgresOrderRepositoryError::InvalidData(format!(
            "unit price cents must be non-negative, got {unit_price_cents}"
        )));
    }

    Ok(OrderItem {
        id: ItemId::from_uuid(item_id),
        description,
        quantity: Quantity::new(quantity as u32).map_err(|e| {
            PostgresOrderRepositoryError::InvalidData(format!("invalid quantity: {e}"))
        })?,
        unit_price: Money::from_cents(unit_price_cents as u64),
    })
}

fn status_to_str(status: OrderStatus) -> &'static str {
    match status {
        OrderStatus::Draft => "draft",
        OrderStatus::Confirmed => "confirmed",
        OrderStatus::Cancelled => "cancelled",
    }
}

fn str_to_status(status: &str) -> Result<OrderStatus, PostgresOrderRepositoryError> {
    match status {
        "draft" => Ok(OrderStatus::Draft),
        "confirmed" => Ok(OrderStatus::Confirmed),
        "cancelled" => Ok(OrderStatus::Cancelled),
        other => Err(PostgresOrderRepositoryError::InvalidData(format!(
            "unknown order status: {other}"
        ))),
    }
}
