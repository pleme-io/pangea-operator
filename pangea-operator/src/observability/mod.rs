//! Observability components for the Pangea Operator.
//!
//! Provides Prometheus metrics, OpenTelemetry tracing, and health endpoints.

mod metrics;
mod tracing_setup;

pub use metrics::Metrics;
pub use tracing_setup::init_tracing;

use axum::{routing::get, Router};
use std::net::SocketAddr;
use tokio::net::TcpListener;
use tracing::info;

/// Health check response.
async fn health() -> &'static str {
    "OK"
}

/// Readiness check - verifies operator is ready to serve.
async fn ready() -> &'static str {
    // TODO: Check database connectivity, etc.
    "OK"
}

/// Prometheus metrics endpoint.
async fn metrics_handler(metrics: axum::Extension<std::sync::Arc<Metrics>>) -> String {
    metrics.gather()
}

/// Create and run the health/metrics HTTP server.
pub async fn run_health_server(
    addr: SocketAddr,
    metrics: std::sync::Arc<Metrics>,
) -> crate::error::Result<()> {
    let app = Router::new()
        .route("/health", get(health))
        .route("/healthz", get(health))
        .route("/ready", get(ready))
        .route("/readyz", get(ready))
        .route("/metrics", get(metrics_handler))
        .layer(axum::Extension(metrics));

    info!(%addr, "Starting health/metrics server");

    let listener = TcpListener::bind(addr).await?;
    axum::serve(listener, app).await.map_err(|e| {
        crate::error::Error::Io(std::io::Error::new(std::io::ErrorKind::Other, e))
    })?;

    Ok(())
}
