use garde::Validate;
use pharos_macros::Command;
use serde::Deserialize;
use uuid::Uuid;

// The DTOs derive `Deserialize` so they double as the JSON request bodies of the
// axum web example (`src/bin/web.rs`): the same type that drives the
// instrumented `dispatch` seam is what crosses the HTTP boundary, with no extra
// wire structs to keep in sync.

#[derive(Command, Deserialize)]
pub struct CreateOrder {
    #[trace(display)]
    pub customer_id: Uuid,
}

#[derive(Command, Deserialize, Validate)]
pub struct AddItem {
    #[trace(display)]
    #[garde(skip)]
    pub order_id: Uuid,
    #[garde(length(min = 1, max = 255))]
    pub description: String,
    #[trace]
    #[garde(range(min = 1))]
    pub quantity: u32,
    #[trace]
    #[garde(range(min = 0.01))]
    pub unit_price_reais: f64,
}

#[derive(Command, Deserialize)]
pub struct ConfirmOrder {
    #[trace(display)]
    pub order_id: Uuid,
}
