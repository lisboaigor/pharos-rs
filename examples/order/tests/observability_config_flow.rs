use pharos_app::{MetricsBackendConfig, OpenTelemetryConfig};

#[test]
fn observability_descriptors_document_order_service_runtime_configuration() {
    let otel = OpenTelemetryConfig::new("order-example", "http://localhost:4317");
    let metrics = MetricsBackendConfig::Prometheus {
        bind_address: "0.0.0.0:9000".to_string(),
    };

    assert_eq!(otel.service_name, "order-example");
    assert_eq!(otel.otlp_endpoint, "http://localhost:4317");
    assert_eq!(
        metrics,
        MetricsBackendConfig::Prometheus {
            bind_address: "0.0.0.0:9000".to_string()
        }
    );
}
