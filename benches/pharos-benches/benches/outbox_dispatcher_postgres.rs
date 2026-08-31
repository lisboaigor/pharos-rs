//! Postgres-backed counterpart to `outbox_dispatcher`'s in-memory benchmark.
//!
//! The in-memory benchmark measures `dispatch_batch()` against a `DashMap`;
//! it says nothing about the production path, which is one claim statement
//! plus two `UPDATE`s per message against real PostgreSQL. This benchmark
//! measures that path directly, against a container. Run it explicitly:
//!
//! ```bash
//! cargo bench -p pharos-benches --bench outbox_dispatcher_postgres
//! ```
//!
//! Requires Docker, like the crate's `docker_integration` tests.

use criterion::{Criterion, criterion_group, criterion_main};
use pharos_app::{
    DispatchConfig, Message, OutboxDispatcher, OutboxMessage, OutboxRepository, RetryPolicy,
};
use pharos_memory::InMemoryMessageBroker;
use pharos_postgres::{
    Pool, PostgresOutboxRepository, connect_pool, migrate_postgres_eventing_schema,
};
use std::time::{Duration, Instant};
use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::{ContainerAsync, GenericImage, ImageExt, runners::AsyncRunner};

const POSTGRES_IMAGE: &str = "postgres";
const POSTGRES_TAG: &str = "16-alpine";
const BATCH: u32 = 100;

async fn start_postgres() -> (ContainerAsync<GenericImage>, Pool) {
    let container = GenericImage::new(POSTGRES_IMAGE, POSTGRES_TAG)
        .with_exposed_port(5432.tcp())
        .with_wait_for(WaitFor::message_on_stderr(
            "database system is ready to accept connections",
        ))
        .with_env_var("POSTGRES_PASSWORD", "postgres")
        .start()
        .await
        .expect("postgres container must start");

    let host = container
        .get_host()
        .await
        .expect("container host must resolve");
    let port = container
        .get_host_port_ipv4(5432)
        .await
        .expect("container port must be mapped");
    let connection_string = format!("postgres://postgres:postgres@{host}:{port}/postgres");

    let pool = connect_pool(&connection_string, 8).expect("pool must connect");
    migrate_postgres_eventing_schema(&pool)
        .await
        .expect("eventing schema must migrate");

    (container, pool)
}

/// Seeds `BATCH` pending rows and returns a dispatcher over them, ready for
/// `dispatch_batch()`. Run before the timer starts (see `iter_custom` below)
/// so the insert cost is excluded from the timed region — only the claim +
/// publish + mark path is measured.
async fn seeded_dispatcher(
    pool: &Pool,
) -> OutboxDispatcher<PostgresOutboxRepository, InMemoryMessageBroker> {
    let outbox = PostgresOutboxRepository::new(pool.clone());
    for i in 0..BATCH {
        outbox
            .insert(OutboxMessage::new(Message::new(
                "orders",
                format!(r#"{{"i":{i}}}"#).into_bytes(),
                "application/json",
            )))
            .await
            .unwrap_or_else(|e| panic!("seed insert must succeed: {e}"));
    }
    let broker = InMemoryMessageBroker::new();
    OutboxDispatcher::with_config(
        outbox,
        broker,
        DispatchConfig::new(
            BATCH as usize,
            RetryPolicy::new(3, Duration::from_millis(0)),
        ),
    )
}

fn bench_dispatch_pending_postgres(c: &mut Criterion) {
    let Ok(rt) = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    else {
        panic!("tokio runtime must build");
    };

    let (container, pool) = rt.block_on(start_postgres());

    c.bench_function("dispatch_pending_100_messages_postgres", |b| {
        b.to_async(&rt).iter_custom(|iters| {
            let pool = pool.clone();
            async move {
                let mut total = Duration::ZERO;
                for _ in 0..iters {
                    let dispatcher = seeded_dispatcher(&pool).await;
                    let start = Instant::now();
                    let result = dispatcher.dispatch_batch().await;
                    total += start.elapsed();
                    assert_eq!(result.published, BATCH as usize);
                }
                total
            }
        });
    });

    // `ContainerAsync`'s `Drop` needs an active Tokio reactor; dropping it
    // here, still inside `rt.block_on`, avoids a panic from dropping it
    // after `rt` itself has gone out of scope.
    rt.block_on(async move { drop(container) });
}

criterion_group! {
    name = benches;
    config = Criterion::default().sample_size(10);
    targets = bench_dispatch_pending_postgres
}
criterion_main!(benches);
