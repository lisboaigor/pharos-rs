# Observability with Pharos RS

Pharos instruments its operations with `tracing` spans and `metrics` counters.
It does **not** install a pipeline for you — that decision belongs in your
binary's `main.rs`.

## Quick-start: OTLP with opentelemetry-otlp

Add to `Cargo.toml`:

```toml
opentelemetry = "0.27"
opentelemetry-otlp = { version = "0.27", features = ["grpc-tonic"] }
opentelemetry_sdk = { version = "0.27", features = ["rt-tokio"] }
tracing-opentelemetry = "0.28"
tracing-subscriber = { version = "0.3", features = ["env-filter", "fmt"] }
```

Wire in `main.rs`:

```rust
use opentelemetry::KeyValue;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::{Resource, runtime};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

fn init_tracing(service_name: &str, otlp_endpoint: &str) {
    let exporter = opentelemetry_otlp::new_exporter()
        .tonic()
        .with_endpoint(otlp_endpoint);

    let tracer = opentelemetry_otlp::new_pipeline()
        .tracing()
        .with_exporter(exporter)
        .with_trace_config(opentelemetry_sdk::trace::config().with_resource(
            Resource::new(vec![KeyValue::new("service.name", service_name.to_string())]),
        ))
        .install_batch(runtime::Tokio)
        .expect("failed to install OTLP pipeline");

    tracing_subscriber::registry()
        .with(EnvFilter::from_default_env())
        .with(tracing_opentelemetry::layer().with_tracer(tracer))
        .with(tracing_subscriber::fmt::layer())
        .init();
}

#[tokio::main]
async fn main() {
    init_tracing("orders-service", "http://localhost:4317");
    // ... rest of bootstrap
}
```

## Correlation IDs

Pharos propagates `correlation_id` and `causation_id` through `IntegrationEvent`.
Set them at the request edge and thread them into every outgoing envelope:

```rust
let event = IntegrationEvent::new("OrderPlaced", 1, "orders", payload)
    .with_correlation_id(request_correlation_id)
    .with_causation_id(triggering_event_id);
```

To carry them in spans automatically, add a tracing field when handling commands:

```rust
let span = tracing::info_span!(
    "handle_create_order",
    correlation_id = %correlation_id,
    tenant_id = %tenant_id,
);
async move { /* handler body */ }.instrument(span).await
```

## Metrics

Pharos emits `metrics` counters through the `metrics` crate facade. Wire your
preferred backend (e.g. `metrics-exporter-prometheus`) before your application
starts:

```rust
metrics_exporter_prometheus::PrometheusBuilder::new()
    .install()
    .expect("failed to install Prometheus exporter");
```

All Pharos counters are prefixed with `pharos.`:

| Counter                             | Meaning                             |
| ----------------------------------- | ----------------------------------- |
| `pharos.postgres.outbox.inserted`   | Outbox row inserted                 |
| `pharos.postgres.outbox.published`  | Outbox row marked published         |
| `pharos.postgres.outbox.failed`     | Outbox row marked failed            |
| `pharos.postgres.outbox.cleaned_up` | Outbox rows deleted by cleanup      |
| `pharos.outbox.dead_lettered`       | Message moved to DLQ                |
| `pharos.postgres.inbox.started`     | Consumer started processing         |
| `pharos.postgres.uow.committed`     | Unit of work committed              |
| `pharos.events.published`           | Domain event published via EventBus |

## Testing without OTLP

Use `pharos_testing::TestSubscriber` in unit tests to capture tracing output
without any external dependency:

```rust
use pharos_testing::TestSubscriber;

let sub = TestSubscriber::new();
let _guard = sub.install();
// … run code under test …
assert!(sub.contains("postgres.outbox.insert"));
```
