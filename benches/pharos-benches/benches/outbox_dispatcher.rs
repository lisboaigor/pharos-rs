use criterion::{Criterion, criterion_group, criterion_main};
use pharos_app::{
    DispatchConfig, Message, OutboxDispatcher, OutboxMessage, OutboxRepository, RetryPolicy,
};
use pharos_memory::{InMemoryMessageBroker, InMemoryOutboxRepository};
use std::time::{Duration, Instant};

/// Builds a dispatcher pre-seeded with 100 pending messages. Run as setup
/// before the timer starts (see `iter_custom` below), so this construction
/// and seeding cost is excluded from the timed region — only
/// `dispatch_batch()` itself is measured.
async fn seeded_dispatcher() -> OutboxDispatcher<InMemoryOutboxRepository, InMemoryMessageBroker> {
    let outbox = InMemoryOutboxRepository::new();
    for i in 0..100u32 {
        outbox
            .insert(OutboxMessage::new(Message::new(
                "orders",
                format!(r#"{{"i":{i}}}"#).into_bytes(),
                "application/json",
            )))
            .await
            .unwrap_or_else(|e| panic!("{e}"));
    }
    let broker = InMemoryMessageBroker::new();
    OutboxDispatcher::with_config(
        outbox,
        broker,
        DispatchConfig::new(100, RetryPolicy::new(3, Duration::from_millis(0))),
    )
}

fn bench_dispatch_pending(c: &mut Criterion) {
    let Ok(rt) = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    else {
        panic!("tokio runtime must build");
    };

    c.bench_function("dispatch_pending_100_messages_in_memory", |b| {
        b.to_async(&rt).iter_custom(|iters| async move {
            let mut total = Duration::ZERO;
            for _ in 0..iters {
                // Setup happens inside the same async context `to_async`
                // already drives, so it runs before the timer starts without
                // nesting a second `block_on` inside this runtime.
                let dispatcher = seeded_dispatcher().await;
                let start = Instant::now();
                let result = dispatcher.dispatch_batch().await;
                total += start.elapsed();
                assert_eq!(result.published, 100);
            }
            total
        });
    });
}

criterion_group!(benches, bench_dispatch_pending);
criterion_main!(benches);
