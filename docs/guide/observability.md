# Observability with Pharos RS

A project scaffolded by `pharos-init` comes up already observable: metrics, logs
and traces wired, a compose file that brings up the collectors, and dashboards
provisioned. This page explains what that machinery is, how to reach the same
place in an existing application, and the traps it exists to remove.

## In a generated project

```sh
docker compose up -d
open http://localhost:3002        # Grafana, admin/admin
```

Three signals, one pane:

| Signal | Stored in | Comes from |
| --- | --- | --- |
| Metrics | Prometheus | `pharos_axum::metrics`, scraped from the app |
| Logs | Loki | Grafana Alloy, reading container stdout |
| Traces | Tempo | OTLP, exported by `pharos-observability` |

They are joined by one value. The OpenTelemetry SDK mints a trace id, the log
records carry it, and the metrics' exemplars carry it too — so a spike in a
latency panel leads to the trace behind it, and that trace leads to its log
lines.

## In an existing application

```toml
pharos-observability = { git = "…", features = ["otel", "axum"] }
```

```rust
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Installs the subscriber, the filter and — when an OTLP endpoint is
    // configured — the exporter. The guard flushes pending spans when dropped.
    let _observability = pharos_observability::init(env!("CARGO_PKG_NAME"))?;

    let metrics = pharos_observability::http::http_metrics();
    let app = pharos_observability::http::instrument(my_router, metrics);
    // ... serve
    Ok(())
}
```

A worker with no HTTP surface uses `default-features = false, features =
["otel"]` and only the first line.

### Environment

| Variable | Meaning |
| --- | --- |
| `RUST_LOG` | Filter directives; the framework's targets are merged in |
| `LOG_FORMAT` | `json` for structured records, anything else for text |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | Where spans go; unset disables export |
| `OTEL_TRACES_SAMPLER_ARG` | Sampled fraction, `1.0` by default |

Without an endpoint nothing is exported and the process logs exactly as before,
so local development does not depend on a collector being up.

## What the framework instruments

`dispatch` wraps every command in a `command.handle` span carrying its fields,
and `query_dispatch` does the same on the read side, so a failure in the logs
can be traced to the arguments that produced it. The event bus wraps each
handler in an `event_handler` span. None of that requires code in a handler.

Secrets are the one thing to be deliberate about: because a command's span
records **every** field, wrap sensitive ones in `pharos_core::Secret`, whose
`Debug` writes a placeholder. Use `#[trace(skip)]` for bulk and personal data,
never for secrets — a field added later would leak before anyone noticed.

## Traps this removes

Each of these fails without an error message, which is why they are worth
naming.

**Framework spans have framework targets.** `info_span!` takes its target from
the module where the macro is *written*, so every span built on your behalf is
`pharos_axum` or `pharos_app`, and a plain `RUST_LOG=my_app=info` drops all of
them: the request span, and with it method, URI, trace id and user on every
event inside; the `event_handler` spans, and with them whole cascades.
`init` merges those targets into the filter, in code, because an environment
file preserved across a deployment would otherwise reintroduce the gap. Merged,
not forced — an explicit `pharos_app=debug` survives.

**Layer order decides whether exemplars work.** The metrics middleware must run
inside the trace layer, where the request span is still current; outside it, the
observation records an empty trace id and metrics stop leading to traces.
`instrument` applies the order so it is not a choice.

**A mounted config does not reload.** `docker compose up -d` only recreates a
service whose *definition* changed. Editing `docker/alloy/config.alloy` and
redeploying leaves the old pipeline running:

```sh
docker compose up -d --force-recreate prometheus grafana loki tempo alloy
```

**Row-level security denies silently.** If your application scopes queries with
RLS, build the pool with `pharos_postgres::tenant_pool`. sqlx calls
`before_acquire` only for connections reused from the idle pool; a freshly
opened one passes through `after_connect` alone, and without the tenant on it
the policies match nothing — reads come back empty with no error, writes fail
with a policy violation. It surfaces under load and after restarts, which reads
as intermittent rather than broken.

**An outbox breaks the trace.** Effects dispatched later by a relay run on their
own task, long past the request's span. Stamp the context into the message
headers at enqueue time with `pharos_observability::propagation::inject`, and
restore it in the relay with `extract`; otherwise every effect opens a trace of
its own and the operation cannot be reconstructed.

## Refreshing the configuration

The files under `docker/` belong to the project, so they can be tuned. To pull
in fixes made to the framework's copies since:

```sh
pharos-init observability --update      # --dry-run to preview, --force to overwrite edits
```

Assets you edited are reported and kept. Your compose file, Dockerfile and `.env`
are never touched.

## Metrics the framework emits

Beyond the HTTP series, `pharos-app` and the adapters count their own work
through the `metrics` facade: `pharos.events.published`,
`pharos.postgres.outbox.inserted`, and the retry and circuit-breaker counters in
`pharos_app::resilience`. Install any `metrics` exporter to collect them; they
are separate from the `prometheus-client` registry `pharos-axum` uses, which
exists because that ecosystem has no exemplar support.
