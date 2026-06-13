//! Observability components for the Pangea Operator.
//!
//! Provides Prometheus metrics, OpenTelemetry tracing, and health endpoints.

mod metrics;
mod tracing_setup;

pub use metrics::{ActiveReconcileGuard, Metrics};
pub use tracing_setup::init_tracing;

use axum::{http::StatusCode, routing::get, Router};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::sync::RwLock;
use tracing::{info, warn};

/// Optional readiness probe target — the magma state-backend Postgres
/// pool. When wired (`PGPASSWORD` set), `/ready` probes it with a fast
/// `SELECT 1`; when `None` (DB-less / tofu-fallback deploy), `/ready`
/// is liveness-equivalent (no DB to be unready against).
type ReadyPool = Option<Arc<RwLock<sqlx::PgPool>>>;

/// Health check response — liveness only. Always 200 while the process
/// is up; never gates on dependencies (a flapping dependency must not
/// kill the pod, only mark it not-ready).
async fn health() -> &'static str {
    "OK"
}

/// Readiness check — verifies the operator can serve. When a Postgres
/// pool is wired, runs a short, bounded `SELECT 1`; a failure (or a
/// timeout past 2s) returns 503 so the pod is pulled from the Service
/// endpoints until the DB is reachable again. With no pool wired
/// (the DB-less deploy), readiness is liveness-equivalent.
async fn ready(pool: axum::Extension<ReadyPool>) -> (StatusCode, &'static str) {
    let Some(pool) = pool.0.as_ref() else {
        // No DB dependency to probe — ready iff the process is up.
        return (StatusCode::OK, "OK");
    };
    // Bound the probe so a hung DB connection can't wedge the readiness
    // handler past the kubelet's own probe timeout.
    let probe = async {
        let pool = pool.read().await;
        sqlx::query("SELECT 1").execute(&*pool).await
    };
    match tokio::time::timeout(Duration::from_secs(2), probe).await {
        Ok(Ok(_)) => (StatusCode::OK, "OK"),
        Ok(Err(e)) => {
            warn!(error = %e, "readiness probe: SELECT 1 failed");
            (StatusCode::SERVICE_UNAVAILABLE, "db unavailable")
        }
        Err(_) => {
            warn!("readiness probe: SELECT 1 timed out after 2s");
            (StatusCode::SERVICE_UNAVAILABLE, "db probe timeout")
        }
    }
}

/// Prometheus metrics endpoint.
async fn metrics_handler(metrics: axum::Extension<std::sync::Arc<Metrics>>) -> String {
    metrics.gather()
}

/// Create and run the health/metrics HTTP server.
///
/// `ready_pool` is the optional magma state-backend pool the `/ready`
/// probe checks. Pass `None` for the DB-less / tofu-fallback deploy or
/// for the metrics-only server where a DB probe adds no signal.
pub async fn run_health_server(
    addr: SocketAddr,
    metrics: std::sync::Arc<Metrics>,
    ready_pool: ReadyPool,
) -> crate::error::Result<()> {
    let app = Router::new()
        .route("/health", get(health))
        .route("/healthz", get(health))
        .route("/ready", get(ready))
        .route("/readyz", get(ready))
        .route("/metrics", get(metrics_handler))
        .layer(axum::Extension(ready_pool))
        .layer(axum::Extension(metrics));

    info!(%addr, "Starting health/metrics server");

    let listener = TcpListener::bind(addr).await?;
    axum::serve(listener, app).await.map_err(|e| {
        crate::error::Error::Io(std::io::Error::new(std::io::ErrorKind::Other, e))
    })?;

    Ok(())
}
