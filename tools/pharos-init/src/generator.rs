use std::fs;

use indoc::formatdoc;

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

    if cfg.uses_axum() {
        emit!("src/web/mod.rs", web_mod_rs(cfg));
        emit!("src/web/state.rs", web_state_rs(cfg));
        emit!("src/web/routes.rs", web_routes_rs(cfg));
        emit!("src/web/error.rs", web_error_rs());
    }

    Ok(files)
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

        #[tokio::main]
        async fn main() -> Result<(), Box<dyn std::error::Error>> {{
            tracing_subscriber::fmt::init();
            {pg_setup}
            let repo = {repo_expr};
            {outbox_setup}
            let bus     = pharos_app::EventBus::new();
            let handler = std::sync::Arc::new(Create{agg}Handler::new(
                std::sync::Arc::clone(&repo),
                bus.clone(),
            ));

            let app  = {pkg}::web::router(handler);
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
        async fn main() {{
            tracing_subscriber::fmt::init();
            tracing::info!("service starting");
            // TODO: wire handlers and start the processing loop
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
