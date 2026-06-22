use pharos_app::Command;
use uuid::Uuid;

pub struct CreateOrder {
    pub customer_id: Uuid,
}
impl Command for CreateOrder {}

pub struct AddItem {
    pub order_id: Uuid,
    pub description: String,
    pub quantity: u32,
    pub unit_price_reais: f64,
}
impl Command for AddItem {}

pub struct ConfirmOrder {
    pub order_id: Uuid,
}
impl Command for ConfirmOrder {}
