//! A logging, metrics and tracing pipeline an application can install in one
//! line.
//!
//! `pharos-app` and `pharos-axum` instrument their own operations but stop
//! short of choosing a destination for that instrumentation — deliberately, so
//! neither drags an OpenTelemetry SDK into applications that do not export
//! traces. This crate makes the other choice, once, correctly, so that every
//! application does not have to make it again.
//!
//! ```no_run
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let _observability = pharos_observability::init(env!("CARGO_PKG_NAME"))?;
//! // ... the guard flushes pending spans when it drops
//! # Ok(())
//! # }
//! ```
//!
//! # What it decides for you
//!
//! - **The framework's spans stay enabled.** See [`filter`]: their target is
//!   the `pharos_*` crate, so an ordinary `RUST_LOG=my_app=info` would drop
//!   them without a word.
//! - **Log format follows the environment.** `LOG_FORMAT=json` emits structured
//!   records whose span fields arrive at a log store as data — `| json |
//!   span_user="alice"` instead of a regular expression over the line. Anything
//!   else keeps the readable text a terminal wants.
//! - **Traces are optional.** Without `OTEL_EXPORTER_OTLP_ENDPOINT` nothing is
//!   exported and the process logs exactly as before, so local development does
//!   not depend on a collector being up.
//!
//! # Environment
//!
//! | Variable | Meaning |
//! | --- | --- |
//! | `RUST_LOG` | Filter directives; framework targets are merged in |
//! | `LOG_FORMAT` | `json` for structured records, anything else for text |
//! | `OTEL_EXPORTER_OTLP_ENDPOINT` | Where to export spans; unset disables export |
//! | `OTEL_TRACES_SAMPLER_ARG` | Sampled fraction, `1.0` by default |

pub mod filter;

#[cfg(feature = "otel")]
pub mod otel;

#[cfg(feature = "otel")]
pub mod propagation;

#[cfg(all(feature = "axum", feature = "otel"))]
pub mod http;

use tracing_subscriber::{
    EnvFilter, Layer, fmt::format::FmtSpan, layer::SubscriberExt, util::SubscriberInitExt,
};

/// Environment variable choosing the log format.
const LOG_FORMAT: &str = "LOG_FORMAT";

/// Failures while installing the pipeline.
#[derive(Debug, thiserror::Error)]
pub enum ObservabilityError {
    /// A global subscriber was already installed by something else.
    #[error("a tracing subscriber is already installed: {0}")]
    AlreadyInstalled(String),
    /// The span exporter could not be built.
    #[error("could not build the span exporter: {0}")]
    Exporter(String),
}

/// How records are rendered.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogFormat {
    /// Human-readable text, for a terminal.
    Text,
    /// One JSON object per record, for a log store.
    Json,
}

impl LogFormat {
    /// Reads [`LOG_FORMAT`], defaulting to [`LogFormat::Text`].
    pub fn from_env() -> Self {
        match std::env::var(LOG_FORMAT).as_deref() {
            Ok("json") | Ok("JSON") => Self::Json,
            _ => Self::Text,
        }
    }
}

/// What to install.
#[derive(Debug, Clone)]
pub struct Config {
    /// Identifies this service in traces; becomes `service.name`.
    pub service_name: String,
    /// Filter used when `RUST_LOG` is unset.
    pub default_filter: String,
    /// Record format; `None` reads the environment.
    pub log_format: Option<LogFormat>,
}

impl Config {
    /// Configuration for `service_name`, reading the rest from the environment.
    pub fn new(service_name: impl Into<String>) -> Self {
        let service_name = service_name.into();
        // The service's own crate at info is the useful default; the framework
        // targets are merged in by `filter::build_filter`.
        let default_filter = format!("{}=info,tower_http=info", service_name.replace('-', "_"));
        Self {
            service_name,
            default_filter,
            log_format: None,
        }
    }

    /// Overrides the filter used when `RUST_LOG` is unset.
    pub fn with_default_filter(mut self, directives: impl Into<String>) -> Self {
        self.default_filter = directives.into();
        self
    }

    /// Forces a record format, ignoring `LOG_FORMAT`.
    pub fn with_log_format(mut self, format: LogFormat) -> Self {
        self.log_format = Some(format);
        self
    }

    fn resolved_format(&self) -> LogFormat {
        self.log_format.unwrap_or_else(LogFormat::from_env)
    }
}

/// Keeps the pipeline alive; flushes pending spans when dropped.
///
/// Hold it for as long as the process runs — typically a `let _guard` in `main`.
/// Dropping it early stops trace export.
#[must_use = "dropping the guard shuts the pipeline down immediately"]
pub struct Observability {
    #[cfg(feature = "otel")]
    provider: Option<opentelemetry_sdk::trace::SdkTracerProvider>,
}

impl Observability {
    /// Whether spans are being exported.
    pub fn traces_enabled(&self) -> bool {
        #[cfg(feature = "otel")]
        {
            self.provider.is_some()
        }
        #[cfg(not(feature = "otel"))]
        {
            false
        }
    }

    /// Flushes and shuts the pipeline down.
    ///
    /// [`Drop`] does the same; call this explicitly when the process may end
    /// without unwinding — `std::process::exit` and `panic = "abort"` skip
    /// destructors, and the trace worth having is usually the last one.
    pub fn shutdown(self) {
        // Consuming `self` runs `Drop`.
    }
}

impl Drop for Observability {
    fn drop(&mut self) {
        #[cfg(feature = "otel")]
        if let Some(provider) = self.provider.take()
            && let Err(error) = provider.shutdown()
        {
            tracing::warn!(%error, "could not flush the final batch of spans");
        }
    }
}

/// Installs the pipeline for `service_name` using the environment.
pub fn init(service_name: impl Into<String>) -> Result<Observability, ObservabilityError> {
    init_with(Config::new(service_name))
}

/// Installs the pipeline from an explicit [`Config`].
pub fn init_with(config: Config) -> Result<Observability, ObservabilityError> {
    let filter = filter::build_filter(&config.default_filter);
    let json = config.resolved_format() == LogFormat::Json;

    // Both layers are always built and one is filtered off, because the two
    // have different types and a `match` returning either would not typecheck
    // without boxing every record's formatting.
    let text = tracing_subscriber::fmt::layer()
        .with_span_events(FmtSpan::CLOSE)
        .with_filter(tracing_subscriber::filter::filter_fn(move |_| !json));
    let structured = tracing_subscriber::fmt::layer()
        .with_span_events(FmtSpan::CLOSE)
        .json()
        // Event fields at the root, span fields under `span` — a log store's
        // JSON parser flattens the latter to `span_trace_id`, `span_user`.
        // The full span list would repeat the whole stack on every record.
        .flatten_event(true)
        .with_current_span(true)
        .with_span_list(false)
        .with_filter(tracing_subscriber::filter::filter_fn(move |_| json));

    install(filter, text, structured, &config)
}

#[cfg(feature = "otel")]
fn install<T, J>(
    filter: EnvFilter,
    text: T,
    structured: J,
    config: &Config,
) -> Result<Observability, ObservabilityError>
where
    T: Layer<tracing_subscriber::Registry> + Send + Sync + 'static,
    J: Layer<tracing_subscriber::layer::Layered<T, tracing_subscriber::Registry>>
        + Send
        + Sync
        + 'static,
{
    let provider = otel::install_provider(&config.service_name)?;
    let registry = tracing_subscriber::registry()
        .with(text)
        .with(structured)
        .with(filter);

    match &provider {
        Some(provider) => registry
            .with(otel::layer(provider, &config.service_name))
            .try_init(),
        None => registry.try_init(),
    }
    .map_err(|e| ObservabilityError::AlreadyInstalled(e.to_string()))?;

    Ok(Observability { provider })
}

#[cfg(not(feature = "otel"))]
fn install<T, J>(
    filter: EnvFilter,
    text: T,
    structured: J,
    _config: &Config,
) -> Result<Observability, ObservabilityError>
where
    T: Layer<tracing_subscriber::Registry> + Send + Sync + 'static,
    J: Layer<tracing_subscriber::layer::Layered<T, tracing_subscriber::Registry>>
        + Send
        + Sync
        + 'static,
{
    tracing_subscriber::registry()
        .with(text)
        .with(structured)
        .with(filter)
        .try_init()
        .map_err(|e| ObservabilityError::AlreadyInstalled(e.to_string()))?;
    Ok(Observability {})
}
