//! Tracing and logging setup for the Pangea Operator.

use std::env;
use tracing::info;
use tracing_subscriber::{fmt, layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

/// Initialize tracing with optional OpenTelemetry export.
///
/// OpenTelemetry integration is planned for Phase 5.
/// For now, this sets up basic tracing with JSON or pretty output.
pub fn init_tracing() -> crate::error::Result<()> {
    let log_format = env::var("LOG_FORMAT").unwrap_or_else(|_| "pretty".to_string());

    // Create filter from RUST_LOG or default
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info,pangea_operator=debug"));

    // Initialize subscriber based on format
    if log_format == "json" {
        tracing_subscriber::registry()
            .with(filter)
            .with(
                fmt::layer()
                    .json()
                    .with_target(true)
                    .with_thread_ids(true)
                    .with_file(true)
                    .with_line_number(true),
            )
            .init();
    } else {
        tracing_subscriber::registry()
            .with(filter)
            .with(fmt::layer().pretty())
            .init();
    }

    // Log if OTEL endpoint is configured (will be supported in Phase 5)
    if let Ok(otlp_endpoint) = env::var("OTEL_EXPORTER_OTLP_ENDPOINT") {
        info!(
            %otlp_endpoint,
            "OpenTelemetry endpoint configured (full integration planned for Phase 5)"
        );
    }

    info!("Tracing initialized");
    Ok(())
}
