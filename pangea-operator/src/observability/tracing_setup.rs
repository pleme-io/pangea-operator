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
/// The stdout writer every fmt layer goes through, bounded per record.
///
/// Not an optimisation. `tracing`'s stdout writer is a synchronous mutex, so
/// an oversized record is written on the tokio worker thread that emitted it
/// and starves the runtime — including the liveness handler, which the kubelet
/// then restarts the pod for, mid-cycle. See `observability::bounded_writer`
/// for the 2026-07-30 incident this closes.
fn bounded_stdout(
) -> crate::observability::bounded_writer::BoundedMakeWriter<fn() -> std::io::Stdout> {
    crate::observability::bounded_writer::BoundedMakeWriter::new(
        std::io::stdout as fn() -> std::io::Stdout,
        crate::observability::bounded_writer::Bound::from_env(),
    )
}

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
                .with_writer(bounded_stdout())
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
                .with_writer(bounded_stdout())
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
            let fmt_layer = fmt::layer().with_writer(bounded_stdout()).pretty();
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
            let fmt_layer = fmt::layer().with_writer(bounded_stdout()).pretty();
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
/// Uses the 0.32 explicit-builder API (matches the tracing-opentelemetry
/// 0.33 transitive dependency on opentelemetry 0.32).
///
/// The 0.26 → 0.32 move replaced the whole builder shape and this function is
/// the entire blast radius of it: `new_pipeline().tracing().with_exporter()
/// .install_batch(runtime::Tokio)` became `SdkTracerProvider::builder()
/// .with_batch_exporter(exporter)`. Two things changed underneath that are
/// worth naming, because neither is visible from the diff:
///
/// - **The runtime argument is gone.** 0.26 took `runtime::Tokio` explicitly;
///   0.32's batch processor discovers the ambient runtime. Passing a runtime
///   is no longer possible, so an init from outside a Tokio context now fails
///   at run time rather than at compile time.
/// - **`Resource` lost its public `new`.** It is `Resource::builder()` now,
///   and `with_service_name` is a first-class method rather than a hand-rolled
///   `service.name` KeyValue — which also means the SDK applies its own
///   env-var precedence to it.
fn build_otlp_tracer(endpoint: &str) -> Result<opentelemetry_sdk::trace::Tracer, String> {
    use opentelemetry_otlp::WithExportConfig;

    let service_name =
        env::var("OTEL_SERVICE_NAME").unwrap_or_else(|_| "pangea-operator".to_string());

    let resource = opentelemetry_sdk::Resource::builder()
        .with_service_name(service_name)
        .build();

    let exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_tonic()
        .with_endpoint(endpoint)
        .build()
        .map_err(|e| format!("build OTLP span exporter: {e}"))?;

    let provider = opentelemetry_sdk::trace::SdkTracerProvider::builder()
        .with_batch_exporter(exporter)
        .with_resource(resource)
        .build();

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
