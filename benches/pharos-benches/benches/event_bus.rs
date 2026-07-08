use chrono::{DateTime, Utc};
use criterion::{Criterion, criterion_group, criterion_main};
use pharos_app::EventBus;
use pharos_core::DomainEvent;

#[derive(Clone)]
struct OrderPlaced {
    id: String,
    at: DateTime<Utc>,
}

impl DomainEvent for OrderPlaced {
    fn event_type(&self) -> &'static str {
        "OrderPlaced"
    }
    fn occurred_at(&self) -> DateTime<Utc> {
        self.at
    }
    fn aggregate_id(&self) -> &str {
        &self.id
    }
}

fn bench_event_bus_no_handlers(c: &mut Criterion) {
    let Ok(rt) = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    else {
        panic!("tokio runtime must build");
    };

    let bus = EventBus::new();
    let event = OrderPlaced {
        id: "order-1".into(),
        at: Utc::now(),
    };

    c.bench_function("event_bus_publish_no_handlers", |b| {
        b.to_async(&rt).iter(|| async {
            bus.publish(&event).await.unwrap_or_else(|e| panic!("{e}"));
        });
    });
}

fn bench_event_bus_one_handler(c: &mut Criterion) {
    use pharos_app::EventHandler;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};

    struct Counter(Arc<AtomicU64>);
    impl EventHandler<OrderPlaced> for Counter {
        type Error = std::convert::Infallible;
        async fn handle(&self, _event: &OrderPlaced) -> Result<(), Self::Error> {
            self.0.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }
    }

    let Ok(rt) = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    else {
        panic!("tokio runtime must build");
    };

    let bus = EventBus::new();
    let count = Arc::new(AtomicU64::new(0));
    bus.register::<OrderPlaced, _>(Counter(Arc::clone(&count)));
    let event = OrderPlaced {
        id: "order-1".into(),
        at: Utc::now(),
    };

    c.bench_function("event_bus_publish_one_handler", |b| {
        b.to_async(&rt).iter(|| async {
            bus.publish(&event).await.unwrap_or_else(|e| panic!("{e}"));
        });
    });
}

criterion_group!(
    benches,
    bench_event_bus_no_handlers,
    bench_event_bus_one_handler
);
criterion_main!(benches);
