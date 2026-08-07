//! Span export over OTLP.
//!
//! Kept behind the `otel` feature so an application that only wants correlated
//! logs does not pull in the OpenTelemetry dependency tree — the same reasoning
//! that keeps `pharos-axum` free of it.

use opentelemetry::{KeyValue, global, trace::TracerProvider as _};
use opentelemetry_otlp::{SpanExporter, WithExportConfig};
use opentelemetry_sdk::{
    Resource,
    propagation::TraceContextPropagator,
    trace::{Sampler, SdkTracer, SdkTracerProvider},
};

use crate::ObservabilityError;

/// Where to send spans. Unset disables export entirely.
const ENDPOINT: &str = "OTEL_EXPORTER_OTLP_ENDPOINT";
/// Sampled fraction, between 0.0 and 1.0.
const SAMPLER_ARG: &str = "OTEL_TRACES_SAMPLER_ARG";

/// Sample everything by default.
///
/// Sampling exists to survive volume, and the trace worth having is usually the
/// rare failure — precisely what a low ratio throws away. An application under
/// real load should lower it deliberately.
const DEFAULT_SAMPLE_RATIO: f64 = 1.0;

/// Builds the provider and registers it globally, or returns `None` when no
/// endpoint is configured.
///
/// Also installs the W3C Trace Context propagator, which is what reads and
/// writes `traceparent` on the way in and out.
pub(crate) fn install_provider(
    service_name: &str,
) -> Result<Option<SdkTracerProvider>, ObservabilityError> {
    let Some(endpoint) = std::env::var(ENDPOINT).ok().filter(|e| !e.is_empty()) else {
        return Ok(None);
    };

    let exporter = SpanExporter::builder()
        .with_tonic()
        .with_endpoint(&endpoint)
        .build()
        .map_err(|e| ObservabilityError::Exporter(e.to_string()))?;

    let provider = SdkTracerProvider::builder()
        .with_batch_exporter(exporter)
        .with_sampler(Sampler::TraceIdRatioBased(sample_ratio()))
        .with_resource(
            Resource::builder()
                .with_attributes([KeyValue::new("service.name", service_name.to_owned())])
                .build(),
        )
        .build();

    global::set_text_map_propagator(TraceContextPropagator::new());
    global::set_tracer_provider(provider.clone());
    Ok(Some(provider))
}

/// The `tracing` layer that exports spans through `provider`.
pub(crate) fn layer<S>(
    provider: &SdkTracerProvider,
    service_name: &str,
) -> tracing_opentelemetry::OpenTelemetryLayer<S, SdkTracer>
where
    S: tracing::Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a>,
{
    tracing_opentelemetry::layer().with_tracer(provider.tracer(service_name.to_owned()))
}

fn sample_ratio() -> f64 {
    std::env::var(SAMPLER_ARG)
        .ok()
        .and_then(|raw| raw.parse().ok())
        .filter(|ratio: &f64| (0.0..=1.0).contains(ratio))
        .unwrap_or(DEFAULT_SAMPLE_RATIO)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_endpoint_means_no_exporter() -> Result<(), ObservabilityError> {
        // Safety: single-threaded test, and the variable is read here only.
        unsafe { std::env::remove_var(ENDPOINT) };
        assert!(install_provider("svc")?.is_none());
        Ok(())
    }

    #[test]
    fn a_ratio_outside_the_unit_range_falls_back() {
        unsafe { std::env::set_var(SAMPLER_ARG, "7") };
        assert_eq!(sample_ratio(), DEFAULT_SAMPLE_RATIO);
        unsafe { std::env::set_var(SAMPLER_ARG, "not a number") };
        assert_eq!(sample_ratio(), DEFAULT_SAMPLE_RATIO);
        unsafe { std::env::remove_var(SAMPLER_ARG) };
    }
}
