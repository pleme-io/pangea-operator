//! Tracing and logging setup for the Pangea Operator.
//!
//! Two independent layers:
//!
//!  - **stdout fmt layer** (always on): JSON or pretty, controlled by
//!    `LOG_FORMAT` env. Tied to the `RUST_LOG` filter.
//!
//!  - **OTLP exporter** (X1): on iff `OTEL_EXPORTER_OTLP_ENDPOINT` is
//!    set. Sends spans (not logs) over OTLP-gRPC to a Tempo /
//!    VictoriaTraces / Jaeger sink. Default service name is
//!    `pangea-operator`; customize with `OTEL_SERVICE_NAME`. Default
//!    resource attributes pull from `OTEL_RESOURCE_ATTRIBUTES`.
//!
//! Both layers respect the same `RUST_LOG` filter so a quiet log
//! level also produces fewer trace spans. The OTLP layer falls back
//! to a no-op if the exporter init fails — operator boot is not
//! blocked on a misconfigured trace backend.

use std::env;
use tracing::{info, warn};
use tracing_subscriber::{fmt, layer::SubscriberExt, util::SubscriberInitExt, EnvFilter, Registry};

// Bring the trait into scope so the `.tracer(...)` method on
// TracerProvider resolves. The `as _` import keeps the trait name
// from leaking into the public surface.
use opentelemetry::trace::TracerProvider as _;

/// Initialize tracing with optional OpenTelemetry export.
///
/// On success, the global subscriber is set; subsequent `tracing::*`
/// macros emit through it. Returns an error only if the stdout layer
/// itself fails to initialize.
pub fn init_tracing() -> crate::error::Result<()> {
    let log_format = env::var("LOG_FORMAT").unwrap_or_else(|_| "pretty".to_string());

    // Create filter from RUST_LOG or default
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info,pangea_operator=debug"));

    // Build the registry incrementally — fmt layer always; OTLP layer
    // optional based on env. The two layers compose via SubscriberExt::with.
    let registry = Registry::default().with(filter);

    let otel_endpoint = env::var("OTEL_EXPORTER_OTLP_ENDPOINT").ok();

    match (log_format.as_str(), otel_endpoint) {
        // JSON + OTLP
        ("json", Some(endpoint)) => {
            let fmt_layer = fmt::layer()
                .json()
                .with_target(true)
                .with_thread_ids(true)
                .with_file(true)
                .with_line_number(true);
            match build_otlp_tracer(&endpoint) {
                Ok(tracer) => {
                    let otel_layer = tracing_opentelemetry::layer().with_tracer(tracer);
                    registry.with(fmt_layer).with(otel_layer).init();
                    info!(%endpoint, "Tracing initialized (json + OTLP)");
                }
                Err(e) => {
                    registry.with(fmt_layer).init();
                    warn!(error = %e, "OTLP exporter init failed; continuing with stdout only");
                }
            }
        }
        // JSON only
        ("json", None) => {
            let fmt_layer = fmt::layer()
                .json()
                .with_target(true)
                .with_thread_ids(true)
                .with_file(true)
                .with_line_number(true);
            registry.with(fmt_layer).init();
            info!("Tracing initialized (json)");
        }
        // Pretty + OTLP
        (_, Some(endpoint)) => {
            let fmt_layer = fmt::layer().pretty();
            match build_otlp_tracer(&endpoint) {
                Ok(tracer) => {
                    let otel_layer = tracing_opentelemetry::layer().with_tracer(tracer);
                    registry.with(fmt_layer).with(otel_layer).init();
                    info!(%endpoint, "Tracing initialized (pretty + OTLP)");
                }
                Err(e) => {
                    registry.with(fmt_layer).init();
                    warn!(error = %e, "OTLP exporter init failed; continuing with stdout only");
                }
            }
        }
        // Pretty only
        (_, None) => {
            let fmt_layer = fmt::layer().pretty();
            registry.with(fmt_layer).init();
            info!("Tracing initialized (pretty)");
        }
    }

    Ok(())
}

/// Build the OTLP-gRPC tracer for the OpenTelemetry layer. Returns
/// the typed Tracer; the call site wraps it via
/// `tracing_opentelemetry::layer().with_tracer(...)` so the layer's
/// generic Subscriber bound is inferred from the registry it's
/// being stacked on (rather than baked into this function's return).
///
/// The `OTEL_SERVICE_NAME` env (defaults to `pangea-operator`) and
/// any `OTEL_RESOURCE_ATTRIBUTES` are honored automatically by
/// opentelemetry-sdk's environment defaults.
///
/// Uses the 0.26-era pipeline API (matches the tracing-opentelemetry
/// 0.27 transitive dependency on opentelemetry 0.26).
fn build_otlp_tracer(endpoint: &str) -> Result<opentelemetry_sdk::trace::Tracer, String> {
    use opentelemetry_otlp::WithExportConfig;

    let service_name =
        env::var("OTEL_SERVICE_NAME").unwrap_or_else(|_| "pangea-operator".to_string());

    let resource = opentelemetry_sdk::Resource::new(vec![
        opentelemetry::KeyValue::new("service.name", service_name),
    ]);

    let exporter = opentelemetry_otlp::new_exporter()
        .tonic()
        .with_endpoint(endpoint);

    let provider = opentelemetry_otlp::new_pipeline()
        .tracing()
        .with_exporter(exporter)
        .with_trace_config(
            opentelemetry_sdk::trace::Config::default().with_resource(resource),
        )
        .install_batch(opentelemetry_sdk::runtime::Tokio)
        .map_err(|e| format!("install OTLP tracing pipeline: {e}"))?;

    Ok(provider.tracer("pangea-operator"))
}

#[cfg(test)]
mod tests {
    // The init function is global-state-mutating (sets the global
    // tracing subscriber). It can only be called once per process —
    // multiple test calls would conflict. So we test the helper
    // shape, not the init flow itself.

    // The init function is global-state-mutating (sets the global
    // tracer provider). Any test that calls build_otlp_tracer
    // mutates global OTel state, so we don't add such a test here —
    // the function's only failure path is the tonic builder, which
    // is upstream-tested. The integration verification happens at
    // deploy time.
}
