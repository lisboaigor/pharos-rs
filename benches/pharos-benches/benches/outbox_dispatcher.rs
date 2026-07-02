use criterion::{Criterion, criterion_group, criterion_main};
use pharos_app::{
    DispatchConfig, Message, OutboxDispatcher, OutboxMessage, OutboxRepository, RetryPolicy,
};
use pharos_infra::{InMemoryMessageBroker, InMemoryOutboxRepository};
use std::time::Duration;

fn bench_dispatch_pending(c: &mut Criterion) {
    let Ok(rt) = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    else {
        panic!("tokio runtime must build");
    };

    c.bench_function("dispatch_pending_100_messages_in_memory", |b| {
        b.to_async(&rt).iter(|| async {
            let outbox = InMemoryOutboxRepository::new();
            // Seed 100 pending messages
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
            let dispatcher = OutboxDispatcher::with_config(
                outbox,
                broker,
                DispatchConfig::new(100, RetryPolicy::new(3, Duration::from_millis(0))),
            );
            let result = dispatcher.dispatch_batch().await;
            assert_eq!(result.published, 100);
        });
    });
}

criterion_group!(benches, bench_dispatch_pending);
criterion_main!(benches);
