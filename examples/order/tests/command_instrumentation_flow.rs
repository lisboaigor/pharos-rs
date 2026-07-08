//! Verifies the instrumentation seam: `dispatch` / `query_dispatch` wrap each
//! call in the command's / query's tracing span, so handlers carry no tracing
//! code yet every call is observed consistently.

use std::sync::Arc;

use order::application::commands::CreateOrder;
use order::application::handlers::OrderHandlers;
use order::application::queries::{GetOrderTotal, GetOrderTotalHandler};
use order::domain::order::Order;
use pharos_app::{EventBus, dispatch, query_dispatch};
use pharos_memory::InMemoryRepository;
use pharos_testing::TestSubscriber;
use uuid::Uuid;

#[tokio::test]
async fn dispatch_wraps_command_in_its_trace_span() -> Result<(), Box<dyn std::error::Error>> {
    let subscriber = TestSubscriber::new();
    let _guard = subscriber.install();

    let repo = Arc::new(InMemoryRepository::<Order>::new());
    let bus = EventBus::new();
    let handlers = OrderHandlers::new(repo.clone(), bus.clone());

    let customer_id = Uuid::now_v7();
    let order_id = dispatch(&handlers, CreateOrder { customer_id }).await?;

    // `save_and_publish` logs from inside the handler, so the captured line is
    // emitted within the `command.handle` span and carries its fields — even
    // though the handler itself contains no tracing code.
    // The subscriber's writer emits ANSI styling, which splits `field=value`
    // apart, so match on individually contiguous substrings: the span name, the
    // command's quoted name value, and the customer_id.
    let lines = subscriber.lines();
    assert!(
        lines.iter().any(|line| {
            line.contains("command.handle")
                && line.contains(r#""CreateOrder""#)
                && line.contains(&customer_id.to_string())
        }),
        "expected a log line inside the `command.handle` span with the command \
         name and customer_id, got:\n{lines:#?}"
    );

    // The read side runs through the same seam: `query_dispatch` applies the
    // `query.handle` span (identical mechanism, proven above) and returns the
    // handler's result. The query handler emits no event of its own, so we
    // assert behavior here rather than captured span fields.
    let query = GetOrderTotalHandler::new(repo.clone());
    let total = query_dispatch(&query, GetOrderTotal { order_id }).await?;
    assert_eq!(total, Some(0), "a freshly created order has a zero total");

    Ok(())
}
