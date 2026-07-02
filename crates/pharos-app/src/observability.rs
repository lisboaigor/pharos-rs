/// OpenTelemetry exporter configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OpenTelemetryConfig {
    /// Service name reported to the telemetry backend.
    pub service_name: String,
    /// OTLP endpoint, e.g. `http://localhost:4317`.
    pub otlp_endpoint: String,
}

impl OpenTelemetryConfig {
    /// Creates a new OpenTelemetry configuration descriptor.
    pub fn new(service_name: impl Into<String>, otlp_endpoint: impl Into<String>) -> Self {
        Self {
            service_name: service_name.into(),
            otlp_endpoint: otlp_endpoint.into(),
        }
    }
}

/// Metrics backend configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MetricsBackendConfig {
    /// Prometheus scrape endpoint descriptor.
    Prometheus {
        /// Address to bind, e.g. `0.0.0.0:9000`.
        bind_address: String,
    },
    /// Custom metrics backend configured outside the framework.
    Custom {
        /// Human-readable backend name.
        name: String,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn creates_opentelemetry_config() {
        let config = OpenTelemetryConfig::new("orders", "http://localhost:4317");
        assert_eq!(config.service_name, "orders");
        assert_eq!(config.otlp_endpoint, "http://localhost:4317");
    }
}
