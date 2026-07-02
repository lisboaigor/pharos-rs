use std::sync::Arc;

use axum::extract::FromRef;
use pharos_infra::InMemoryRepository;

use crate::application::handlers::OrderHandlers;
use crate::application::queries::GetOrderTotalHandler;
use crate::domain::order::Order;

pub type Repo = InMemoryRepository<Order>;
pub type Commands = OrderHandlers<Repo>;
pub type Totals = GetOrderTotalHandler<Repo>;

/// Shared application state injected into every route.
///
/// Each handler is behind an [`Arc`] so the state is cheap to clone per request.
/// The [`FromRef`] impls let the [`pharos_axum`] extractors pick the right
/// handler out of the state by type.
#[derive(Clone)]
pub struct AppState {
    commands: Arc<Commands>,
    totals: Arc<Totals>,
}

impl AppState {
    pub fn new(commands: Arc<Commands>, totals: Arc<Totals>) -> Self {
        Self { commands, totals }
    }
}

impl FromRef<AppState> for Arc<Commands> {
    fn from_ref(state: &AppState) -> Self {
        Arc::clone(&state.commands)
    }
}

impl FromRef<AppState> for Arc<Totals> {
    fn from_ref(state: &AppState) -> Self {
        Arc::clone(&state.totals)
    }
}
