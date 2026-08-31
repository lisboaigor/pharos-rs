use std::fs;

use indoc::formatdoc;

use crate::assets;
use crate::config::{Broker, EventDelivery, Http, Persistence, ProjectConfig, Serialization};

// ── public surface ────────────────────────────────────────────────────────────

pub struct GeneratedFile {
    pub rel_path: String,
    #[allow(dead_code)]
    pub content: String,
}

/// Writes the full project to `cfg.output_path()`.
pub fn generate(cfg: &ProjectConfig) -> std::io::Result<Vec<GeneratedFile>> {
    let root = cfg.output_path();
    let mut files = Vec::new();

    macro_rules! emit {
        ($path:expr, $content:expr) => {{
            let rel = $path.to_string();
            let content = $content;
            let dest = root.join(&rel);
            if let Some(p) = dest.parent() {
                fs::create_dir_all(p)?;
            }
            fs::write(&dest, content.as_bytes())?;
            files.push(GeneratedFile {
                rel_path: rel,
                content,
            });
        }};
    }

    emit!("Cargo.toml", cargo_toml(cfg));
    emit!("src/lib.rs", lib_rs(cfg));
    emit!("src/main.rs", main_rs(cfg));
    emit!("src/domain/mod.rs", domain_mod_rs(cfg));
    emit!(format!("src/domain/{}.rs", cfg.module()), aggregate_rs(cfg));
    emit!("src/domain/events.rs", events_rs(cfg));
    emit!("src/domain/value_objects.rs", value_objects_rs());
    emit!("src/application/mod.rs", application_mod_rs());
    emit!("src/application/commands.rs", commands_rs(cfg));
    emit!("src/application/handlers.rs", handlers_rs(cfg));
    emit!("src/application/error.rs", error_rs());
    emit!("src/infrastructure/mod.rs", infrastructure_mod_rs(cfg));

    if cfg.uses_postgres() && !matches!(cfg.persistence, Persistence::InMemory) {
        emit!("src/infrastructure/repository.rs", repository_rs(cfg));
    }

    emit!("Dockerfile", dockerfile(cfg));
    emit!(".dockerignore", dockerignore());
    emit!("docker-compose.yml", docker_compose(cfg));
    emit!(".env.example", env_example(cfg));
    if cfg.observability {
        emit!(
            "docker/grafana/provisioning/dashboards/dashboards.yml",
            dashboards_provisioning(cfg)
        );
        for asset in assets::OBSERVABILITY {
            emit!(asset.rel_path, asset.contents.to_string());
        }
        // Baseline for `pharos-init observability --update`: it is what
        // later tells a file nobody touched from one the project edited on
        // purpose. Only meaningful when the stack it tracks was written.
        crate::update::write_baseline(&root)?;
    }

    if cfg.uses_axum() {
        emit!("src/web/mod.rs", web_mod_rs(cfg));
        emit!("src/web/state.rs", web_state_rs(cfg));
        emit!("src/web/routes.rs", web_routes_rs(cfg));
        emit!("src/web/error.rs", web_error_rs());
    }

    Ok(files)
}

fn docker_compose(cfg: &ProjectConfig) -> String {
    let name = &cfg.project_name;

    // Plain string, not `formatdoc!`: that macro strips the common indentation,
    // which would lift `postgres:` out of `services:` and produce a compose file
    // the schema rejects. Here the indentation is the payload.
    let postgres = if cfg.uses_postgres() {
        r#"  postgres:
    image: postgres:16-alpine
    environment:
      POSTGRES_USER: postgres
      POSTGRES_PASSWORD: postgres
      POSTGRES_DB: app
    ports:
      - "127.0.0.1:5432:5432"
    volumes:
      - postgres_data:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U postgres -d app"]
      interval: 5s
      timeout: 5s
      retries: 5
    restart: unless-stopped
"#
    } else {
        ""
    };

    let redis = if cfg.uses_redis() {
        r#"  redis:
    image: redis:7-alpine
    ports:
      - "127.0.0.1:6379:6379"
    volumes:
      - redis_data:/data
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
      interval: 5s
      timeout: 5s
      retries: 5
    restart: unless-stopped
"#
    } else {
        ""
    };

    // Apache Kafka's own image, single-node KRaft mode — no ZooKeeper
    // service to also stand up. `CLUSTER_ID` is required but arbitrary; this
    // one is just a fixed, valid base64 UUID.
    let kafka = if cfg.uses_kafka() {
        r#"  kafka:
    image: apache/kafka:3.9.0
    ports:
      - "127.0.0.1:9092:9092"
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_LISTENERS: PLAINTEXT://:9092,CONTROLLER://:9093
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka:9093
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_CLUSTER_ID: MkU3OEVBNTcwNTJENDM2Qk
    volumes:
      - kafka_data:/var/lib/kafka/data
    healthcheck:
      test: ["CMD-SHELL", "/opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:9092"]
      interval: 10s
      timeout: 10s
      retries: 6
    restart: unless-stopped
"#
    } else {
        ""
    };

    let mut depends = String::new();
    if cfg.uses_postgres() {
        depends.push_str("      postgres:\n        condition: service_healthy\n");
    }
    if cfg.uses_redis() {
        depends.push_str("      redis:\n        condition: service_healthy\n");
    }
    if cfg.uses_kafka() {
        depends.push_str("      kafka:\n        condition: service_healthy\n");
    }
    if !depends.is_empty() {
        depends = format!("    depends_on:\n{depends}");
    }

    let pg_volume = if cfg.uses_postgres() {
        "  postgres_data:\n"
    } else {
        ""
    };
    let redis_volume = if cfg.uses_redis() {
        "  redis_data:\n"
    } else {
        ""
    };
    let kafka_volume = if cfg.uses_kafka() {
        "  kafka_data:\n"
    } else {
        ""
    };

    let obs_services = if cfg.observability {
        assets::COMPOSE_SERVICES
    } else {
        ""
    };
    let obs_volumes = if cfg.observability {
        assets::COMPOSE_VOLUMES
    } else {
        ""
    };

    let header = if cfg.observability {
        formatdoc!(
            r#"
            # Everything this application needs to run, plus the observability that
            # makes it explainable: metrics, logs and traces, already wired.
            #
            #   docker compose up -d
            #   open http://localhost:3002        # Grafana (admin/admin)
            #
            # The service is named `app` on purpose: the Prometheus job, the log
            # pipeline and the dashboards all key off that name, which is what lets
            # their configuration ship unmodified.
            #
            # Editing a mounted config file (docker/**) does NOT reach a running
            # container — `up -d` only recreates a service whose definition changed.
            # Apply those with:
            #   docker compose up -d --force-recreate prometheus grafana loki tempo alloy
            #
            # `app` and Grafana are published on 127.0.0.1 only, same as Postgres
            # below — reachable from this machine, not from the rest of the LAN.
            # Grafana in particular ships with the admin/admin default until
            # GRAFANA_ADMIN_PASSWORD is changed in .env, and has read access to
            # every trace, log and metric this stack collects. Widen either
            # binding deliberately (a reverse proxy on another host, an intentional
            # LAN demo) rather than by dropping the `127.0.0.1:` prefix as a
            # shortcut.
            "#
        )
    } else {
        formatdoc!(
            r#"
            # Everything this application needs to run. Scaffolded with `--minimal`:
            # no observability stack (Prometheus/Grafana/Loki/Tempo/Alloy/Telegraf),
            # no Docker-socket access anywhere in this file. The app still logs and
            # traces on its own (see .env's OTEL_EXPORTER_OTLP_ENDPOINT); there is
            # just no collector or dashboard bundled to send them to.
            #
            #   docker compose up -d
            "#
        )
    };

    formatdoc!(
        r#"
        {header}name: {name}

        services:
          app:
            build:
              context: .
            env_file: [.env]
            ports:
              - "127.0.0.1:3000:3000"
        {depends}    restart: unless-stopped

        {postgres}{redis}{kafka}{obs_services}
        volumes:
        {pg_volume}{redis_volume}{kafka_volume}{obs_volumes}
        "#
    )
}

// ── containers ────────────────────────────────────────────────────────────────

fn dockerfile(cfg: &ProjectConfig) -> String {
    let name = &cfg.project_name;
    formatdoc!(
        r#"
        # syntax=docker/dockerfile:1

        FROM rust:1-bookworm AS builder
        WORKDIR /app

        # Dependencies first, so editing source does not rebuild the world. The
        # stub is enough to resolve and compile them.
        COPY Cargo.toml Cargo.lock* ./
        RUN mkdir src && echo 'fn main() {{}}' > src/main.rs \
            && echo '' > src/lib.rs && cargo build --release
        RUN rm -rf src

        COPY src ./src
        # `touch` invalidates the cached artifact so the real code is compiled.
        RUN touch src/main.rs src/lib.rs \
            && cargo build --release --bin {name}

        FROM debian:bookworm-slim AS runtime
        RUN apt-get update \
            && apt-get install -y --no-install-recommends ca-certificates \
            && rm -rf /var/lib/apt/lists/*

        RUN useradd --system --uid 10001 appuser
        USER appuser

        COPY --from=builder /app/target/release/{name} /usr/local/bin/{name}

        # 3000 serves the API; 9464 serves the metrics scrape and is deliberately
        # not published on the host.
        EXPOSE 3000
        CMD ["{name}"]
        "#
    )
}

fn dockerignore() -> String {
    formatdoc!(
        r#"
        target/
        .git/
        .github/
        **/*.log
        .env
        .env.*
        !.env.example
        .DS_Store
        "#
    )
}

fn env_example(cfg: &ProjectConfig) -> String {
    let db = if cfg.uses_postgres() {
        formatdoc!(
            r#"
            # Reachable under this name from inside the compose network.
            DATABASE_URL=postgres://postgres:postgres@postgres:5432/app
            "#
        )
    } else {
        String::new()
    };
    let redis = if cfg.uses_redis() {
        formatdoc!(
            r#"
            REDIS_URL=redis://redis:6379
            "#
        )
    } else {
        String::new()
    };
    let kafka = if cfg.uses_kafka() {
        formatdoc!(
            r#"
            KAFKA_BROKERS=kafka:9092
            "#
        )
    } else {
        String::new()
    };
    let otel_endpoint = if cfg.observability {
        "http://tempo:4317"
    } else {
        // `--minimal` scaffolds no Tempo to send to; leaving this pointed at
        // a host that does not exist would make every span export fail.
        ""
    };
    let grafana = if cfg.observability {
        formatdoc!(
            r#"
            GRAFANA_ADMIN_USER=admin
            GRAFANA_ADMIN_PASSWORD=admin
            "#
        )
    } else {
        String::new()
    };
    formatdoc!(
        r#"
        {db}{redis}{kafka}
        # Filter directives. The framework's own targets are merged in by
        # `pharos_observability::init`, so they cannot be dropped by accident.
        RUST_LOG=info

        # `json` sends span fields to the log store as data, which makes queries
        # like `| json | span_user="alice"` possible. Anything else keeps the
        # readable text a terminal wants.
        LOG_FORMAT=json

        # Where spans go. Empty disables export; the application still logs.
        OTEL_EXPORTER_OTLP_ENDPOINT={otel_endpoint}
        OTEL_TRACES_SAMPLER_ARG=1.0

        {grafana}"#
    )
}

fn dashboards_provisioning(cfg: &ProjectConfig) -> String {
    let name = &cfg.project_name;
    formatdoc!(
        r#"
        apiVersion: 1

        providers:
          - name: {name}
            folder: {name}
            type: file
            updateIntervalSeconds: 30
            allowUiUpdates: true
            options:
              path: /var/lib/grafana/dashboards
        "#
    )
}

// ── Cargo.toml ────────────────────────────────────────────────────────────────

fn cargo_toml(cfg: &ProjectConfig) -> String {
    let git = "https://github.com/lisboaigor/pharos-rs";
    let tower_feat = if cfg.uses_axum() {
        r#", features = ["tower"]"#
    } else {
        ""
    };

    let mut deps = formatdoc!(
        r#"
        pharos-core   = {{ git = "{git}" }}
        pharos-macros = {{ git = "{git}" }}
        pharos-app    = {{ git = "{git}"{tower_feat} }}
        "#
    );

    if cfg.uses_infra() {
        deps.push_str(&format!("pharos-memory    = {{ git = \"{git}\" }}\n"));
    }
    if cfg.uses_postgres() {
        deps.push_str(&format!("pharos-postgres = {{ git = \"{git}\" }}\n"));
    }
    if cfg.uses_redis() {
        deps.push_str(&format!("pharos-redis    = {{ git = \"{git}\" }}\n"));
    }
    if cfg.uses_kafka() {
        deps.push_str(&format!("pharos-kafka    = {{ git = \"{git}\" }}\n"));
    }
    if cfg.uses_axum() {
        deps.push_str(&format!("pharos-axum     = {{ git = \"{git}\" }}\n"));
    }
    // Logging, metrics and traces. Without the `axum` feature it still installs
    // the filter and the log pipeline, which is all a worker needs.
    let obs_feat = if cfg.uses_axum() {
        r#", features = ["otel", "axum"]"#
    } else {
        r#", default-features = false, features = ["otel"]"#
    };
    deps.push_str(&format!(
        "pharos-observability = {{ git = \"{git}\"{obs_feat} }}\n"
    ));
    if cfg.uses_proto() {
        deps.push_str(&format!(
            "pharos-proto    = {{ git = \"{git}\" }}\nprost = \"0.14\"\n"
        ));
    }

    deps.push_str(&formatdoc!(
        r#"
        thiserror          = "2"
        chrono             = {{ version = "0.4", features = ["serde"] }}
        uuid               = {{ version = "1",   features = ["v4", "v7", "serde"] }}
        serde              = {{ version = "1",   features = ["derive"] }}
        serde_json         = "1"
        tokio              = {{ version = "1",   features = ["macros", "rt-multi-thread"] }}
        tracing            = "0.1"
        tracing-subscriber = {{ version = "0.3", features = ["env-filter", "fmt"] }}
    "#
    ));

    if cfg.uses_axum() {
        deps.push_str("axum  = \"0.8\"\ntower = { version = \"0.5\", features = [\"util\"] }\n");
    }

    formatdoc!(
        r#"
        [package]
        name    = "{name}"
        version = "0.1.0"
        edition = "2024"

        [dependencies]
        {deps}
        "#,
        name = cfg.project_name,
        deps = deps.trim(),
    )
}

// ── src/lib.rs ────────────────────────────────────────────────────────────────

fn lib_rs(cfg: &ProjectConfig) -> String {
    let web_mod = if cfg.uses_axum() {
        "\npub mod web;\n"
    } else {
        ""
    };
    formatdoc!(
        r#"
        pub mod application;
        pub mod domain;
        pub mod infrastructure;
        {web_mod}
        "#
    )
}

// ── src/main.rs ───────────────────────────────────────────────────────────────

fn main_rs(cfg: &ProjectConfig) -> String {
    match cfg.http {
        Http::Axum => axum_main_rs(cfg),
        Http::None => minimal_main_rs(cfg),
    }
}

/// Builds the `Create{agg}Handler` construction expression, matching
/// whichever constructor shape [`handlers_rs`] generated for this
/// `event_delivery` — the three handler variants take different arguments
/// (`(repo, bus)`, `(repo, outbox)`, or `(pool)` alone), so this must stay in
/// lockstep with [`inprocess_handler`]/[`outbox_handler`]/[`atomic_handler`].
fn handler_construction(cfg: &ProjectConfig, agg: &str) -> String {
    match cfg.event_delivery {
        EventDelivery::InProcess => {
            let repo_expr = repo_expression(cfg, agg);
            formatdoc!(
                r#"
                let repo = {repo_expr};
                let bus  = pharos_app::EventBus::new();
                let handler = std::sync::Arc::new(Create{agg}Handler::new(repo, bus));
                "#
            )
        }
        EventDelivery::Outbox => {
            let repo_expr = repo_expression(cfg, agg);
            formatdoc!(
                r#"
                let repo   = {repo_expr};
                let outbox = std::sync::Arc::new(pharos_postgres::PostgresOutboxRepository::new(pool.clone()));
                outbox.migrate().await?;
                let handler = std::sync::Arc::new(Create{agg}Handler::new(repo, outbox));
                "#
            )
        }
        EventDelivery::AtomicOutbox => formatdoc!(
            r#"
            let handler = std::sync::Arc::new(Create{agg}Handler::new(pool.clone()));
            "#
        ),
    }
}

/// Background task that drains the outbox to the configured broker.
///
/// Without this, `save_and_enqueue`/`save_aggregate_and_enqueue` fill the
/// outbox but nothing ever calls `OutboxDispatcher::dispatch_batch`, so
/// events accumulate as `pending` forever. Emitted whenever
/// [`ProjectConfig::uses_outbox`] is true; empty otherwise.
fn outbox_dispatcher_setup(cfg: &ProjectConfig) -> String {
    if !cfg.uses_outbox() {
        return String::new();
    }
    let publisher_setup = match cfg.broker {
        Broker::Redis => formatdoc!(
            r#"
            let redis_url = std::env::var("REDIS_URL")
                .unwrap_or_else(|_| "redis://redis:6379".to_string());
            let dispatch_publisher = pharos_redis::RedisMessageBroker::from_url(&redis_url)?;
            "#
        ),
        Broker::Kafka => formatdoc!(
            r#"
            let kafka_brokers = std::env::var("KAFKA_BROKERS")
                .unwrap_or_else(|_| "kafka:9092".to_string());
            let dispatch_publisher = pharos_kafka::KafkaPublisher::from_bootstrap_servers(&kafka_brokers)?;
            "#
        ),
        // Not reachable from the interactive prompt today (every
        // `event_delivery` that sets `uses_outbox()` also selects a broker),
        // but kept exhaustive rather than assuming that stays true.
        Broker::None => {
            return "// TODO: no broker configured — wire OutboxDispatcher to one before this \
                     runs in production, or the outbox never drains.\n"
                .to_string();
        }
    };
    formatdoc!(
        r#"
        {publisher_setup}
        let dispatch_repo = pharos_postgres::PostgresOutboxRepository::new(pool.clone());
        tokio::spawn(async move {{
            let dispatcher = pharos_app::OutboxDispatcher::new(dispatch_repo, dispatch_publisher);
            let mut interval = tokio::time::interval(std::time::Duration::from_millis(200));
            loop {{
                interval.tick().await;
                let result = dispatcher.dispatch_batch().await;
                if !result.errors.is_empty() {{
                    tracing::warn!(errors = ?result.errors, "outbox dispatch reported errors");
                }}
            }}
        }});
        "#
    )
}

fn axum_main_rs(cfg: &ProjectConfig) -> String {
    let agg = cfg.aggregate();
    let pkg = cfg.project_name.replace('-', "_");

    let pg_setup = if cfg.uses_postgres() {
        formatdoc!(
            r#"
            let database_url = std::env::var("DATABASE_URL")
                .expect("DATABASE_URL must be set");
            let pool = pharos_postgres::connect_pool(&database_url, 16)?;
            pharos_postgres::migrate_postgres_aggregate_schema(&pool).await?;
        "#
        )
    } else {
        String::new()
    };

    let handler_construction = handler_construction(cfg, &agg);
    let dispatcher_setup = outbox_dispatcher_setup(cfg);

    let module = cfg.module();
    formatdoc!(
        r#"
        use std::net::SocketAddr;
        use {pkg}::application::handlers::Create{agg}Handler;
        use {pkg}::domain::{module}::{agg};

        /// Serves the metrics scrape on its own port, so `/metrics` is never part
        /// of the public API surface. OpenMetrics is the only exposition that
        /// carries exemplars, which is what links a latency spike to its trace.
        async fn serve_metrics(metrics: std::sync::Arc<pharos_axum::metrics::HttpMetrics>) {{
            let app = axum::Router::new().route(
                "/metrics",
                axum::routing::get(move || {{
                    let metrics = std::sync::Arc::clone(&metrics);
                    async move {{
                        match metrics.encode() {{
                            Ok(body) => Ok(([(axum::http::header::CONTENT_TYPE,
                                pharos_axum::metrics::CONTENT_TYPE)], body)),
                            Err(_) => Err(axum::http::StatusCode::INTERNAL_SERVER_ERROR),
                        }}
                    }}
                }}),
            );
            let addr = SocketAddr::from(([0, 0, 0, 0], 9464));
            match tokio::net::TcpListener::bind(addr).await {{
                Ok(listener) => {{
                    let _ = axum::serve(listener, app).await;
                }}
                Err(error) => tracing::error!(%error, "could not open the metrics port"),
            }}
        }}

        #[tokio::main]
        async fn main() -> Result<(), Box<dyn std::error::Error>> {{
            // Logging, metrics and traces in one call. The guard flushes pending
            // spans when it drops, so the last trace before a shutdown survives.
            let _observability = pharos_observability::init(env!("CARGO_PKG_NAME"))?;
            let metrics = pharos_observability::http::http_metrics();
            tokio::spawn(serve_metrics(std::sync::Arc::clone(&metrics)));
            {pg_setup}
            {handler_construction}
            {dispatcher_setup}
            // `instrument` applies the observability layers in the one order
            // where both the request span and its exemplars work.
            let app  = pharos_observability::http::instrument({pkg}::web::router(handler), metrics);
            let addr = SocketAddr::from(([0, 0, 0, 0], 3000));
            tracing::info!("listening on http://{{addr}}");
            let listener = tokio::net::TcpListener::bind(addr).await?;
            axum::serve(listener, app).await?;
            Ok(())
        }}
        "#
    )
}

fn minimal_main_rs(cfg: &ProjectConfig) -> String {
    let pg_setup = if cfg.uses_postgres() {
        formatdoc!(
            r#"
            let database_url = std::env::var("DATABASE_URL")
                .expect("DATABASE_URL must be set");
            let pool = pharos_postgres::connect_pool(&database_url, 16)?;
            pharos_postgres::migrate_postgres_aggregate_schema(&pool).await?;
        "#
        )
    } else {
        String::new()
    };
    let dispatcher_setup = outbox_dispatcher_setup(cfg);
    formatdoc!(
        r#"
        #[tokio::main]
        async fn main() -> Result<(), Box<dyn std::error::Error>> {{
            // Logging and traces; the guard flushes pending spans on the way out.
            let _observability = pharos_observability::init(env!("CARGO_PKG_NAME"))?;
            {pg_setup}
            {dispatcher_setup}
            tracing::info!("service starting");
            // TODO: wire handlers and start the processing loop
            std::future::pending::<()>().await;
            Ok(())
        }}
    "#
    )
}

// ── src/domain/mod.rs ─────────────────────────────────────────────────────────

fn domain_mod_rs(cfg: &ProjectConfig) -> String {
    formatdoc!(
        "pub mod {};\npub mod events;\npub mod value_objects;\n",
        cfg.module()
    )
}

// ── src/domain/<context>.rs (aggregate) ───────────────────────────────────────

fn aggregate_rs(cfg: &ProjectConfig) -> String {
    let agg = cfg.aggregate();
    let module = cfg.module();
    formatdoc!(
        r#"
        use chrono::Utc;
        use pharos_core::AggregateEvents;
        use pharos_macros::{{AggregateRoot, Entity, id_type}};
        use serde::{{Deserialize, Serialize}};

        use super::events::{agg}Event;

        // id_type! already derives FromStr (via uuid::Uuid::parse_str), which
        // is what PostgresJsonRepository requires.
        id_type!({agg}Id);

        #[derive(Debug, Clone, Entity, AggregateRoot, Serialize, Deserialize)]
        pub struct {agg} {{
            #[id]      id:      {agg}Id,
            #[version] version: u64,
            #[events]  #[serde(skip)] events: AggregateEvents<{agg}Event>,
            // TODO: add domain state fields
        }}

        impl {agg} {{
            pub fn create() -> Self {{
                let id = {agg}Id::new();
                let mut events = AggregateEvents::default();
                events.raise({agg}Event::{agg}Created {{
                    {module}_id: id.to_string(),
                    occurred_at: Utc::now(),
                }});
                Self {{ id, version: 0, events }}
            }}

            pub fn id(&self) -> &{agg}Id {{
                &self.id
            }}
        }}
        "#
    )
}

// ── src/domain/events.rs ──────────────────────────────────────────────────────

fn events_rs(cfg: &ProjectConfig) -> String {
    let agg = cfg.aggregate();
    let module = cfg.module();
    formatdoc!(
        r#"
        use chrono::{{DateTime, Utc}};
        use pharos_macros::DomainEvent;
        use serde::{{Deserialize, Serialize}};

        // Serialize/Deserialize: the outbox and event-store paths both
        // encode this type as the wire payload (`serde_json::to_vec` in the
        // generated command handler, or as the event-sourced state if this
        // profile evolves that way).
        #[derive(Debug, Clone, Serialize, Deserialize, DomainEvent)]
        pub enum {agg}Event {{
            {agg}Created {{
                #[aggregate_id]
                {module}_id: String,
                #[occurred_at]
                occurred_at: DateTime<Utc>,
            }},
            // TODO: add more events
        }}
        "#
    )
}

// ── src/domain/value_objects.rs ───────────────────────────────────────────────

fn value_objects_rs() -> String {
    formatdoc!(
        r#"
        // TODO: add value object types here.
        // Example:
        //
        // use pharos_core::ValueObject;
        //
        // #[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
        // pub struct Email(String);
        // impl ValueObject for Email {{}}
    "#
    )
}

// ── src/application/mod.rs ────────────────────────────────────────────────────

fn application_mod_rs() -> String {
    "pub mod commands;\npub mod error;\npub mod handlers;\n".to_string()
}

// ── src/application/commands.rs ───────────────────────────────────────────────

fn commands_rs(cfg: &ProjectConfig) -> String {
    let agg = cfg.aggregate();
    formatdoc!(
        r#"
        use pharos_macros::Command;

        #[derive(Command)]
        pub struct Create{agg} {{
            // TODO: add command fields
        }}
        "#
    )
}

// ── src/application/handlers.rs ───────────────────────────────────────────────

fn handlers_rs(cfg: &ProjectConfig) -> String {
    let agg = cfg.aggregate();
    let module = cfg.module();
    match &cfg.event_delivery {
        EventDelivery::InProcess => inprocess_handler(cfg, &agg, &module),
        EventDelivery::Outbox => outbox_handler(cfg, &agg, &module),
        EventDelivery::AtomicOutbox => atomic_handler(cfg, &agg, &module),
    }
}

fn inprocess_handler(cfg: &ProjectConfig, agg: &str, module: &str) -> String {
    let repo_type = if cfg.uses_postgres() {
        format!("pharos_postgres::PostgresJsonRepository<{agg}>")
    } else {
        format!("pharos_memory::InMemoryRepository<{agg}>")
    };
    formatdoc!(
        r#"
        use std::sync::Arc;
        use pharos_app::{{CommandHandler, EventBus, save_and_publish}};

        use crate::application::commands::Create{agg};
        use crate::application::error::AppError;
        use crate::domain::{module}::{{{agg}, {agg}Id}};

        pub struct Create{agg}Handler {{
            repo: Arc<{repo_type}>,
            bus:  EventBus,
        }}

        impl Create{agg}Handler {{
            pub fn new(repo: Arc<{repo_type}>, bus: EventBus) -> Self {{
                Self {{ repo, bus }}
            }}
        }}

        impl CommandHandler<Create{agg}> for Create{agg}Handler {{
            type Output = {agg}Id;
            type Error  = AppError;

            async fn handle(&self, _cmd: Create{agg}) -> Result<Self::Output, Self::Error> {{
                let mut entity = {agg}::create();
                let id = entity.id().clone();
                save_and_publish(&*self.repo, &self.bus, &mut entity)
                    .await
                    .map_err(AppError::infra)?;
                Ok(id)
            }}
        }}
        "#
    )
}

fn outbox_handler(cfg: &ProjectConfig, agg: &str, module: &str) -> String {
    let (repo_type, outbox_type) = postgres_repo_and_outbox_types(cfg, agg);
    let message_body = message_mapping_body(cfg);
    formatdoc!(
        r#"
        use std::sync::Arc;
        use pharos_app::{{CommandHandler, Message, save_and_enqueue}};

        use crate::application::commands::Create{agg};
        use crate::application::error::AppError;
        use crate::domain::{module}::{{{agg}, {agg}Id}};

        pub struct Create{agg}Handler {{
            repo:   Arc<{repo_type}>,
            outbox: Arc<{outbox_type}>,
        }}

        impl Create{agg}Handler {{
            pub fn new(repo: Arc<{repo_type}>, outbox: Arc<{outbox_type}>) -> Self {{
                Self {{ repo, outbox }}
            }}
        }}

        impl CommandHandler<Create{agg}> for Create{agg}Handler {{
            type Output = {agg}Id;
            type Error  = AppError;

            async fn handle(&self, _cmd: Create{agg}) -> Result<Self::Output, Self::Error> {{
                let mut entity = {agg}::create();
                let id = entity.id().clone();
                save_and_enqueue(&*self.repo, &*self.outbox, &mut entity, |event| {{
                    {message_body}
                }})
                .await
                .map_err(AppError::infra)?;
                Ok(id)
            }}
        }}
        "#
    )
}

fn atomic_handler(cfg: &ProjectConfig, agg: &str, module: &str) -> String {
    let message_body = message_mapping_body(cfg);
    formatdoc!(
        r#"
        use pharos_app::{{CommandHandler, Message}};
        use pharos_postgres::save_aggregate_and_enqueue;

        use crate::application::commands::Create{agg};
        use crate::application::error::AppError;
        use crate::domain::{module}::{{{agg}, {agg}Id}};

        pub struct Create{agg}Handler {{
            pool: pharos_postgres::Pool,
        }}

        impl Create{agg}Handler {{
            pub fn new(pool: pharos_postgres::Pool) -> Self {{
                Self {{ pool }}
            }}
        }}

        impl CommandHandler<Create{agg}> for Create{agg}Handler {{
            type Output = {agg}Id;
            type Error  = AppError;

            async fn handle(&self, _cmd: Create{agg}) -> Result<Self::Output, Self::Error> {{
                let mut entity = {agg}::create();
                let id = entity.id().clone();
                save_aggregate_and_enqueue(
                    &self.pool,
                    "{agg}",
                    &mut entity,
                    |event| {{ {message_body} }},
                )
                .await
                .map_err(AppError::infra)?;
                Ok(id)
            }}
        }}
        "#
    )
}

fn message_mapping_body(cfg: &ProjectConfig) -> String {
    let topic = cfg.module().replace('_', "-") + "-events";
    match &cfg.serialization {
        Serialization::Json | Serialization::None => formatdoc!(
            r#"Message::new(
                    "{topic}",
                    serde_json::to_vec(event).expect("event serialization"),
                    "application/json",
                )
                .with_key(pharos_core::DomainEvent::aggregate_id(event))"#
        ),
        Serialization::Protobuf => formatdoc!(
            r#"// TODO: map event to a prost::Message and encode.
                // let ie = pharos_app::IntegrationEvent::from_domain_event(event, 1, "{topic}", payload);
                // let wire = pharos_proto::ProtobufEventSerializer.encode(&ie).unwrap();
                Message::new("{topic}", vec![], "application/x-protobuf")
                    .with_key(pharos_core::DomainEvent::aggregate_id(event))"#
        ),
    }
}

// ── src/application/error.rs ──────────────────────────────────────────────────

fn error_rs() -> String {
    formatdoc!(
        r#"
        use thiserror::Error;

        #[derive(Debug, Error)]
        pub enum AppError {{
            #[error("entity not found")]
            NotFound,
            #[error("domain error: {{0}}")]
            Domain(#[from] pharos_core::DomainError),
            #[error("infrastructure error: {{0}}")]
            Infrastructure(String),
        }}

        impl AppError {{
            pub fn infra(e: impl std::fmt::Display) -> Self {{
                Self::Infrastructure(e.to_string())
            }}
        }}
    "#
    )
}

// ── src/infrastructure/mod.rs ─────────────────────────────────────────────────

fn infrastructure_mod_rs(cfg: &ProjectConfig) -> String {
    if cfg.uses_postgres() && !matches!(cfg.persistence, Persistence::InMemory) {
        "pub mod repository;\n".to_string()
    } else {
        "// Infrastructure adapters — add modules here as needed.\n".to_string()
    }
}

// ── src/infrastructure/repository.rs ─────────────────────────────────────────

fn repository_rs(cfg: &ProjectConfig) -> String {
    let agg = cfg.aggregate();
    let module = cfg.module();
    match &cfg.persistence {
        Persistence::PostgresJson => formatdoc!(
            r#"
            pub fn {module}_repository(
                pool: pharos_postgres::Pool,
            ) -> pharos_postgres::PostgresJsonRepository<crate::domain::{module}::{agg}> {{
                pharos_postgres::PostgresJsonRepository::with_aggregate_type(pool, "{agg}")
            }}
            "#
        ),
        Persistence::PostgresTenant => formatdoc!(
            r#"
            pub fn {module}_repository(
                pool:   pharos_postgres::Pool,
                tenant: &pharos_app::TenantContext,
            ) -> pharos_postgres::TenantJsonRepository<crate::domain::{module}::{agg}> {{
                pharos_postgres::TenantJsonRepository::new(pool, tenant, "{agg}")
            }}
            "#
        ),
        _ => formatdoc!(
            "// TODO: implement a hand-written Repository<{agg}> for the normalized schema.\n\
             // See examples/order/src/infrastructure/postgres_order_repository.rs for reference.\n"
        ),
    }
}

// ── src/web/ ──────────────────────────────────────────────────────────────────

fn web_mod_rs(cfg: &ProjectConfig) -> String {
    let agg = cfg.aggregate();
    let route = cfg.module().replace('_', "s/");
    let module = cfg.module();
    formatdoc!(
        r#"
        pub mod error;
        pub mod routes;
        pub mod state;

        use std::sync::Arc;
        use axum::{{Router, routing::post}};

        use crate::application::handlers::Create{agg}Handler;

        pub fn router(handler: Arc<Create{agg}Handler>) -> Router {{
            Router::new()
                .route("/{route}", post(routes::create_{module}))
                .with_state(state::AppState {{ handler }})
        }}
        "#
    )
}

fn web_state_rs(cfg: &ProjectConfig) -> String {
    let agg = cfg.aggregate();
    formatdoc!(
        r#"
        use std::sync::Arc;

        use crate::application::handlers::Create{agg}Handler;

        #[derive(Clone)]
        pub struct AppState {{
            pub handler: Arc<Create{agg}Handler>,
        }}
        "#
    )
}

fn web_routes_rs(cfg: &ProjectConfig) -> String {
    let agg = cfg.aggregate();
    let module = cfg.module();
    formatdoc!(
        r#"
        use axum::{{Json, extract::State}};

        use crate::application::commands::Create{agg};
        use crate::web::{{error::ApiError, state::AppState}};

        pub async fn create_{module}(
            State(state): State<AppState>,
            Json(_body): Json<serde_json::Value>,
        ) -> Result<Json<serde_json::Value>, ApiError> {{
            // TODO: parse _body into Create{agg} fields.
            let cmd = Create{agg} {{}};
            // `dispatch` is the framework seam: it validates the command and
            // applies the tracing span before the handler runs — never call
            // `handler.handle` directly.
            let id = pharos_app::dispatch(&*state.handler, cmd).await?;

            Ok(Json(serde_json::json!({{ "id": id.to_string() }})))
        }}
        "#
    )
}

fn web_error_rs() -> String {
    formatdoc!(
        r#"
        use axum::{{Json, http::StatusCode, response::{{IntoResponse, Response}}}};
        use pharos_app::DispatchError;

        use crate::application::error::AppError;

        pub enum ApiError {{
            /// Input failed validation before the handler ran (422).
            Validation(pharos_app::ValidationError),
            /// The handler failed.
            App(AppError),
        }}

        impl From<AppError> for ApiError {{
            fn from(e: AppError) -> Self {{
                Self::App(e)
            }}
        }}

        impl From<DispatchError<AppError>> for ApiError {{
            fn from(e: DispatchError<AppError>) -> Self {{
                match e {{
                    DispatchError::Validation(e) => Self::Validation(e),
                    DispatchError::Handler(e) => Self::App(e),
                }}
            }}
        }}

        impl IntoResponse for ApiError {{
            fn into_response(self) -> Response {{
                let (status, message) = match &self {{
                    Self::Validation(e) => (StatusCode::UNPROCESSABLE_ENTITY, e.to_string()),
                    Self::App(AppError::NotFound) => (StatusCode::NOT_FOUND, self_message(&self)),
                    Self::App(AppError::Domain(_)) => (StatusCode::UNPROCESSABLE_ENTITY, self_message(&self)),
                    Self::App(AppError::Infrastructure(_)) => {{
                        (StatusCode::INTERNAL_SERVER_ERROR, self_message(&self))
                    }}
                }};
                (status, Json(serde_json::json!({{ "error": message }}))).into_response()
            }}
        }}

        fn self_message(e: &ApiError) -> String {{
            match e {{
                ApiError::Validation(e) => e.to_string(),
                ApiError::App(e) => e.to_string(),
            }}
        }}
    "#
    )
}

// ── shared helpers ────────────────────────────────────────────────────────────

fn repo_expression(cfg: &ProjectConfig, agg: &str) -> String {
    match &cfg.persistence {
        Persistence::InMemory => {
            format!("std::sync::Arc::new(pharos_memory::InMemoryRepository::<{agg}>::new())")
        }
        Persistence::PostgresJson => format!(
            "std::sync::Arc::new(pharos_postgres::PostgresJsonRepository::<{agg}>::with_aggregate_type(pool.clone(), \"{agg}\"))"
        ),
        Persistence::PostgresRelational => format!(
            "std::sync::Arc::new(crate::infrastructure::repository::{module}_repository(pool.clone()))",
            module = cfg.module()
        ),
        Persistence::PostgresTenant => format!(
            "std::sync::Arc::new(pharos_postgres::TenantJsonRepository::<{agg}>::new(pool.clone(), &tenant, \"{agg}\"))"
        ),
    }
}

fn postgres_repo_and_outbox_types(cfg: &ProjectConfig, agg: &str) -> (String, String) {
    let repo = match &cfg.persistence {
        Persistence::InMemory => format!("pharos_memory::InMemoryRepository<{agg}>"),
        Persistence::PostgresJson | Persistence::PostgresRelational => {
            format!("pharos_postgres::PostgresJsonRepository<{agg}>")
        }
        Persistence::PostgresTenant => format!("pharos_postgres::TenantJsonRepository<{agg}>"),
    };
    let outbox = if cfg.uses_postgres() {
        "pharos_postgres::PostgresOutboxRepository".to_string()
    } else {
        "pharos_memory::InMemoryOutboxRepository".to_string()
    };
    (repo, outbox)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::SystemKind;

    /// The interactive prompt needs a terminal, which is why the generator went
    /// untested. Building the config directly is what makes it verifiable.
    ///
    /// Builds a `ProjectConfig` through the same derivation
    /// [`collect`](crate::config::collect) uses
    /// ([`crate::config::derive_technical_choices`]), for any
    /// `(SystemKind, serves_http)` the interactive prompt can produce —
    /// so a test exercising "EventDriven with HTTP" is exercising a
    /// reachable combination, not a fixture that has drifted from what the
    /// prompt actually derives.
    fn config_for(into: &std::path::Path, kind: SystemKind, serves_http: bool) -> ProjectConfig {
        // Only SingleService/ModularMonolith read `stores_data` from the
        // (skipped, in tests) Q3; EventDriven/HighThroughput always store.
        let stores_data = true;
        let derived = crate::config::derive_technical_choices(&kind, serves_http, stores_data);
        ProjectConfig {
            project_name: "demoapp".into(),
            context_name: "order".into(),
            location: into.to_path_buf(),
            kind,
            persistence: derived.persistence,
            event_delivery: derived.event_delivery,
            broker: derived.broker,
            serialization: derived.serialization,
            http: derived.http,
            observability: true,
        }
    }

    /// A directory per call: tests run in parallel, and a shared one had them
    /// deleting each other's output.
    fn generate_into_temp() -> std::io::Result<(std::path::PathBuf, Vec<GeneratedFile>)> {
        generate_into_temp_for(SystemKind::SingleService, true)
    }

    fn generate_into_temp_for(
        kind: SystemKind,
        serves_http: bool,
    ) -> std::io::Result<(std::path::PathBuf, Vec<GeneratedFile>)> {
        static SEQ: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
        let seq = SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let root = std::env::temp_dir().join(format!("pharos-init-{}-{seq}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root)?;
        let cfg = config_for(&root, kind, serves_http);
        let files = generate(&cfg)?;
        Ok((cfg.output_path(), files))
    }

    /// Assets ship byte-for-byte: a project gets exactly what the framework
    /// carries, which is what makes refreshing them meaningful later.
    #[test]
    fn every_asset_lands_verbatim() -> std::io::Result<()> {
        let (root, _) = generate_into_temp()?;
        for asset in assets::OBSERVABILITY {
            let written = fs::read_to_string(root.join(asset.rel_path))?;
            assert_eq!(
                written, asset.contents,
                "{} was altered on the way out",
                asset.rel_path
            );
        }
        Ok(())
    }

    /// A malformed dashboard is silently ignored by Grafana — the panel simply
    /// never appears — so the parse belongs in a test.
    #[test]
    fn dashboards_are_valid_json() -> std::io::Result<()> {
        let (root, _) = generate_into_temp()?;
        for asset in assets::OBSERVABILITY {
            if !asset.rel_path.ends_with(".json") {
                continue;
            }
            let raw = fs::read_to_string(root.join(asset.rel_path))?;
            serde_json::from_str::<serde_json::Value>(&raw)
                .unwrap_or_else(|e| panic!("{} is not valid JSON: {e}", asset.rel_path));
        }
        Ok(())
    }

    /// `{service=~".*"}` is a parse error in Loki: a query needs at least one
    /// matcher that cannot match empty. It costs nothing to catch here.
    #[test]
    fn no_dashboard_query_matches_the_empty_label() -> std::io::Result<()> {
        let (root, _) = generate_into_temp()?;
        let logs = fs::read_to_string(root.join("docker/grafana/dashboards/logs.json"))?;
        assert!(
            !logs.contains(r#"\"allValue\": \".*\""#) && !logs.contains(r#"service=~\\\".*\\\""#),
            "a query would be rejected by Loki for matching the empty label"
        );
        Ok(())
    }

    /// The framework dependency is fetched over public HTTPS (the repository
    /// is public), so the build needs no SSH agent, no `--mount=type=ssh`,
    /// and no forwarded credential of any kind.
    #[test]
    fn the_build_needs_no_ssh_agent_or_credential() -> std::io::Result<()> {
        let (root, _) = generate_into_temp()?;
        let dockerfile = fs::read_to_string(root.join("Dockerfile"))?;
        assert!(
            !dockerfile.contains("--mount=type=ssh"),
            "a public HTTPS dependency needs no SSH mount"
        );
        let compose = fs::read_to_string(root.join("docker-compose.yml"))?;
        assert!(
            !compose.contains("ssh:"),
            "a public HTTPS dependency needs no forwarded SSH agent"
        );
        assert!(
            !root.join(".cargo/config.toml").exists(),
            "git-fetch-with-cli was only ever needed to delegate SSH auth to the system git"
        );
        Ok(())
    }

    /// A masked failure here is a silent one: the dependency-warming layer
    /// never actually populates the build cache, so every subsequent build
    /// recompiles every dependency from scratch on the *next* `RUN` instead —
    /// slow, but never a build failure a developer would notice.
    #[test]
    fn the_dependency_warming_step_does_not_swallow_its_own_failure() -> std::io::Result<()> {
        let (root, _) = generate_into_temp()?;
        let dockerfile = fs::read_to_string(root.join("Dockerfile"))?;
        assert!(
            !dockerfile.contains("|| true"),
            "a masked failure here defeats the whole point of warming the dependency cache"
        );
        Ok(())
    }

    /// The service name is the invariant that lets every config ship unmodified.
    #[test]
    fn the_application_service_is_named_app() -> std::io::Result<()> {
        let (root, _) = generate_into_temp()?;
        let compose = fs::read_to_string(root.join("docker-compose.yml"))?;
        assert!(
            compose.contains("\n  app:\n"),
            "renaming the service breaks the Prometheus job and the log pipeline"
        );
        for service in ["prometheus", "grafana", "loki", "tempo", "alloy"] {
            assert!(
                compose.contains(&format!("\n  {service}:\n")),
                "{service} missing from the stack"
            );
        }
        Ok(())
    }

    /// `app` and Grafana are the only services with a published port —
    /// Grafana holds admin/admin until the operator changes it, and both
    /// must be reachable from this machine only by default, same as
    /// Postgres. Publishing on a bare port number binds `0.0.0.0`, which is
    /// reachable from the whole LAN.
    #[test]
    fn published_ports_are_bound_to_localhost_only() -> std::io::Result<()> {
        let (root, _) = generate_into_temp()?;
        let compose = fs::read_to_string(root.join("docker-compose.yml"))?;
        for line in compose.lines() {
            let trimmed = line.trim();
            // A published port mapping is `"HOST:CONTAINER"` (optionally
            // `"IP:HOST:CONTAINER"`); every other quoted, colon-containing
            // value in this file (image tags, volume mounts) is not one.
            if trimmed.starts_with('-') && trimmed.contains(':') && trimmed.ends_with('"') {
                let after_dash = trimmed.trim_start_matches('-').trim();
                let looks_like_a_port_mapping = after_dash
                    .trim_matches('"')
                    .split(':')
                    .next_back()
                    .is_some_and(|last| last.chars().all(|c| c.is_ascii_digit()));
                if looks_like_a_port_mapping {
                    assert!(
                        after_dash.starts_with("\"127.0.0.1:"),
                        "published port is not bound to localhost: {line:?}"
                    );
                }
            }
        }
        Ok(())
    }

    /// `formatdoc!` strips common indentation, which once lifted `postgres:`
    /// out of `services:` and produced a file the schema rejects. Compose is the
    /// only authority on its own format, so ask it — skipped where it is absent.
    #[test]
    fn compose_is_accepted_by_compose_itself() -> std::io::Result<()> {
        // EventDriven and HighThroughput add the Redis and Kafka service
        // blocks respectively — covered here, not just the default
        // SingleService profile that has neither.
        for (kind, serves_http) in [
            (SystemKind::SingleService, true),
            (SystemKind::EventDriven, true),
            (SystemKind::HighThroughput, true),
        ] {
            let (root, _) = generate_into_temp_for(kind.clone(), serves_http)?;
            fs::copy(root.join(".env.example"), root.join(".env"))?;

            let Ok(output) = std::process::Command::new("docker")
                .args(["compose", "config", "--quiet"])
                .current_dir(&root)
                .output()
            else {
                return Ok(());
            };
            assert!(
                output.status.success(),
                "compose rejected the generated file for {kind:?}:\n{}",
                String::from_utf8_lossy(&output.stderr)
            );
        }
        Ok(())
    }

    /// A generated project must reach the pipeline, not just depend on it.
    #[test]
    fn the_entrypoint_installs_observability() -> std::io::Result<()> {
        let (root, _) = generate_into_temp()?;
        let main = fs::read_to_string(root.join("src/main.rs"))?;
        assert!(main.contains("pharos_observability::init"));
        assert!(
            main.contains("pharos_observability::http::instrument"),
            "the router is not instrumented, so requests carry no span"
        );
        let manifest = fs::read_to_string(root.join("Cargo.toml"))?;
        assert!(manifest.contains("pharos-observability"));
        Ok(())
    }

    /// `--minimal` (`ProjectConfig::observability = false`) must remove
    /// every trace of the observability stack — the compose services, the
    /// docker/ asset tree, and above all any mention of the Docker socket —
    /// while still leaving the application's own logging/tracing wired,
    /// since that costs nothing extra and needs no collector to be useful.
    #[test]
    fn minimal_skips_the_observability_stack_entirely() -> std::io::Result<()> {
        let root_dir =
            std::env::temp_dir().join(format!("pharos-init-minimal-{}", std::process::id()));
        let _ = fs::remove_dir_all(&root_dir);
        fs::create_dir_all(&root_dir)?;
        let cfg = ProjectConfig {
            observability: false,
            ..config_for(&root_dir, SystemKind::SingleService, true)
        };
        generate(&cfg)?;
        let root = cfg.output_path();

        assert!(
            !root.join("docker/grafana").exists(),
            "docker/ observability assets must not be written under --minimal"
        );

        let compose = fs::read_to_string(root.join("docker-compose.yml"))?;
        for absent in [
            "prometheus",
            "grafana",
            "telegraf",
            "loki",
            "alloy",
            "docker-socket-proxy",
            "docker.sock",
        ] {
            assert!(
                !compose.contains(absent),
                "docker-compose.yml must not mention `{absent}` under --minimal"
            );
        }

        let main = fs::read_to_string(root.join("src/main.rs"))?;
        assert!(
            main.contains("pharos_observability::init"),
            "the app should still log/trace on its own even without the collector stack"
        );
        Ok(())
    }

    /// Points every generated `git = "https://..."` pharos dependency at this
    /// checkout's own `crates/` instead, so the generated project can be
    /// built offline, without network access, against the framework version
    /// actually under test.
    ///
    /// A path dependency does not need to be a workspace member: Cargo
    /// resolves each crate's own `workspace = true` fields by walking up
    /// from *that crate's* manifest, so `crates/pharos-core` still resolves
    /// against this repository's root workspace even though the generated
    /// project's own manifest is a standalone, non-member `Cargo.toml`
    /// living outside this tree entirely.
    fn point_dependencies_at_this_checkout(root: &std::path::Path) -> std::io::Result<()> {
        let manifest_path = root.join("Cargo.toml");
        let manifest = fs::read_to_string(&manifest_path)?;

        let workspace_crates = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .and_then(std::path::Path::parent)
            .ok_or_else(|| {
                std::io::Error::other(
                    "tools/pharos-init must have two parent directories: tools/ and the repo root",
                )
            })?
            .join("crates");

        let git = "https://github.com/lisboaigor/pharos-rs";
        let mut patched = String::with_capacity(manifest.len());
        for line in manifest.lines() {
            if let Some((name, rest)) = line.split_once('=') {
                let crate_name = name.trim();
                if crate_name.starts_with("pharos-") && rest.contains(git) {
                    let crate_path = workspace_crates.join(crate_name);
                    let replaced = rest.replacen(
                        &format!(r#"git = "{git}""#),
                        &format!(r#"path = "{}""#, crate_path.display()),
                        1,
                    );
                    patched.push_str(crate_name);
                    patched.push_str(" =");
                    patched.push_str(&replaced);
                    patched.push('\n');
                    continue;
                }
            }
            patched.push_str(line);
            patched.push('\n');
        }
        fs::write(manifest_path, patched)
    }

    /// The audit finding this closes: the generator had never once been
    /// checked against `cargo build`, and `EventDriven`/`HighThroughput`
    /// with HTTP generated a `main.rs` that did not typecheck — the
    /// handler's `new()` signature (three different shapes depending on
    /// `EventDelivery`) had drifted from what `main.rs` actually called it
    /// with. Every `(SystemKind, serves_http)` pair the interactive prompt
    /// can produce is generated and `cargo check`ed here, against this
    /// checkout's own crates (see [`point_dependencies_at_this_checkout`]),
    /// so a future drift between a handler's constructor and its call site
    /// fails this test instead of only surfacing for someone running
    /// `pharos-init` for real.
    #[test]
    fn every_generated_profile_typechecks() -> std::io::Result<()> {
        use std::process::Command;

        let profiles = [
            (SystemKind::SingleService, true),
            (SystemKind::SingleService, false),
            (SystemKind::ModularMonolith, true),
            (SystemKind::EventDriven, true),
            (SystemKind::EventDriven, false),
            (SystemKind::HighThroughput, true),
            (SystemKind::HighThroughput, false),
        ];

        for (kind, serves_http) in profiles {
            let (root, _) = generate_into_temp_for(kind.clone(), serves_http)?;
            point_dependencies_at_this_checkout(&root)?;

            let output = Command::new("cargo")
                .args(["check", "--offline", "--quiet"])
                .current_dir(&root)
                .output()?;
            assert!(
                output.status.success(),
                "generated project for {kind:?} (http={serves_http}) failed to typecheck:\n{}",
                String::from_utf8_lossy(&output.stderr)
            );
        }
        Ok(())
    }
}
