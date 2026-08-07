use std::fs;

use indoc::formatdoc;

use crate::assets;
use crate::config::{EventDelivery, Http, Persistence, ProjectConfig, Serialization};

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
    emit!(".cargo/config.toml", cargo_config_toml());
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
    emit!(
        "docker/grafana/provisioning/dashboards/dashboards.yml",
        dashboards_provisioning(cfg)
    );
    for asset in assets::OBSERVABILITY {
        emit!(asset.rel_path, asset.contents.to_string());
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

    let depends = if cfg.uses_postgres() {
        "    depends_on:\n      postgres:\n        condition: service_healthy\n"
    } else {
        ""
    };
    let pg_volume = if cfg.uses_postgres() {
        "  postgres_data:\n"
    } else {
        ""
    };

    let obs_services = assets::COMPOSE_SERVICES;
    let obs_volumes = assets::COMPOSE_VOLUMES;

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
        name: {name}

        services:
          app:
            build:
              context: .
              # Forwards the SSH agent, which the build needs to fetch the
              # framework from its private repository.
              ssh:
                - default
            env_file: [.env]
            ports:
              - "3000:3000"
        {depends}    restart: unless-stopped

        {postgres}{obs_services}
        volumes:
        {pg_volume}{obs_volumes}
        "#
    )
}

// ── containers ────────────────────────────────────────────────────────────────

fn dockerfile(cfg: &ProjectConfig) -> String {
    let name = &cfg.project_name;
    formatdoc!(
        r#"
        # syntax=docker/dockerfile:1
        #
        # `--mount=type=ssh` is not optional here: the framework is a private git
        # dependency fetched over SSH, and a build has no agent of its own. Build
        # with `docker build --ssh default .`, or through the compose file, which
        # already forwards it.

        FROM rust:1-bookworm AS builder
        WORKDIR /app

        # Dependencies first, so editing source does not rebuild the world. The
        # stub is enough to resolve and compile them.
        COPY Cargo.toml Cargo.lock* ./
        COPY .cargo ./.cargo
        RUN mkdir src && echo 'fn main() {{}}' > src/main.rs && echo '' > src/lib.rs \
            && --mount=type=ssh cargo build --release || true
        RUN rm -rf src

        COPY src ./src
        # `touch` invalidates the cached artifact so the real code is compiled.
        RUN --mount=type=ssh touch src/main.rs src/lib.rs \
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
    formatdoc!(
        r#"
        {db}
        # Filter directives. The framework's own targets are merged in by
        # `pharos_observability::init`, so they cannot be dropped by accident.
        RUST_LOG=info

        # `json` sends span fields to the log store as data, which makes queries
        # like `| json | span_user="alice"` possible. Anything else keeps the
        # readable text a terminal wants.
        LOG_FORMAT=json

        # Where spans go. Empty disables export; the application still logs.
        OTEL_EXPORTER_OTLP_ENDPOINT=http://tempo:4317
        OTEL_TRACES_SAMPLER_ARG=1.0

        GRAFANA_ADMIN_USER=admin
        GRAFANA_ADMIN_PASSWORD=admin
        "#
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
    let git = "ssh://git@github.com/lisboaigor/pharos-rs";
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

// ── .cargo/config.toml ───────────────────────────────────────────────────────

fn cargo_config_toml() -> String {
    // Cargo's built-in SSH client does not use the system ssh-agent or
    // ~/.ssh/config. Setting git-fetch-with-cli = true delegates all git
    // operations to the system `git` binary, which picks up the existing
    // SSH key and agent automatically.
    formatdoc!(
        r#"
        [net]
        git-fetch-with-cli = true
    "#
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
        Http::None => minimal_main_rs(),
    }
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

    let outbox_setup = if cfg.uses_outbox() && cfg.uses_postgres() {
        formatdoc!(
            r#"
            let outbox = std::sync::Arc::new(pharos_postgres::PostgresOutboxRepository::new(pool.clone()));
            outbox.migrate().await?;
        "#
        )
    } else {
        String::new()
    };

    let repo_expr = repo_expression(cfg, &agg);

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
            let repo = {repo_expr};
            {outbox_setup}
            let bus     = pharos_app::EventBus::new();
            let handler = std::sync::Arc::new(Create{agg}Handler::new(
                std::sync::Arc::clone(&repo),
                bus.clone(),
            ));

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

fn minimal_main_rs() -> String {
    formatdoc!(
        r#"
        #[tokio::main]
        async fn main() -> Result<(), Box<dyn std::error::Error>> {{
            // Logging and traces; the guard flushes pending spans on the way out.
            let _observability = pharos_observability::init(env!("CARGO_PKG_NAME"))?;
            tracing::info!("service starting");
            // TODO: wire handlers and start the processing loop
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

        id_type!({agg}Id);

        // id_type! does not derive FromStr; PostgresJsonRepository requires it.
        impl std::str::FromStr for {agg}Id {{
            type Err = uuid::Error;
            fn from_str(s: &str) -> Result<Self, Self::Err> {{
                uuid::Uuid::parse_str(s).map(Self)
            }}
        }}

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

        #[derive(Debug, Clone, DomainEvent)]
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
    use crate::config::{Broker, SystemKind};

    /// The interactive prompt needs a terminal, which is why the generator went
    /// untested. Building the config directly is what makes it verifiable.
    fn config(into: &std::path::Path) -> ProjectConfig {
        ProjectConfig {
            project_name: "demoapp".into(),
            context_name: "order".into(),
            location: into.to_path_buf(),
            kind: SystemKind::SingleService,
            persistence: Persistence::PostgresJson,
            event_delivery: EventDelivery::InProcess,
            broker: Broker::None,
            serialization: Serialization::Json,
            http: Http::Axum,
        }
    }

    /// A directory per call: tests run in parallel, and a shared one had them
    /// deleting each other's output.
    fn generate_into_temp() -> std::io::Result<(std::path::PathBuf, Vec<GeneratedFile>)> {
        static SEQ: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
        let seq = SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let root = std::env::temp_dir().join(format!("pharos-init-{}-{seq}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root)?;
        let cfg = config(&root);
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

    /// The whole promise is `docker compose up`, and it runs against a private
    /// git dependency: without the SSH mount the build cannot fetch it.
    #[test]
    fn the_build_forwards_an_ssh_agent() -> std::io::Result<()> {
        let (root, _) = generate_into_temp()?;
        let dockerfile = fs::read_to_string(root.join("Dockerfile"))?;
        assert!(
            dockerfile.contains("--mount=type=ssh"),
            "the build would fail fetching the framework from its private repository"
        );
        let compose = fs::read_to_string(root.join("docker-compose.yml"))?;
        assert!(
            compose.contains("ssh:") && compose.contains("- default"),
            "compose does not forward the agent the Dockerfile expects"
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

    /// `formatdoc!` strips common indentation, which once lifted `postgres:`
    /// out of `services:` and produced a file the schema rejects. Compose is the
    /// only authority on its own format, so ask it — skipped where it is absent.
    #[test]
    fn compose_is_accepted_by_compose_itself() -> std::io::Result<()> {
        let (root, _) = generate_into_temp()?;
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
            "compose rejected the generated file:\n{}",
            String::from_utf8_lossy(&output.stderr)
        );
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
}
