//! Observability components for the Pangea Operator.
//!
//! Provides Prometheus metrics, OpenTelemetry tracing, and health endpoints.

pub mod bounded_writer;
pub mod live_metrics;
mod metrics;
pub mod projection;
mod tracing_setup;

pub use bounded_writer::{Bound, BoundedMakeWriter, ClipStats};
pub use metrics::{ActiveReconcileGuard, Metrics};
pub use tracing_setup::init_tracing;

use axum::{http::StatusCode, routing::get, Router};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::sync::RwLock;
use tracing::{info, warn};

/// Shared shutdown-in-progress flag (theory/MAGMA-POSTGRES-LIFECYCLE.md
/// §4, M2, Gap D). `main.rs` flips this to `true` the moment a drain
/// signal is received, before aborting any controller — `/readyz`
/// checks it first, so the Service pulls this pod's endpoint
/// immediately rather than waiting on a DB probe (or a DB-less deploy's
/// unconditional 200) to eventually notice the process is going away.
pub type ShuttingDown = Arc<AtomicBool>;

/// Optional readiness probe target — the magma state-backend Postgres
/// pool. When wired (`PGPASSWORD` set), `/ready` probes it with a fast
/// `SELECT 1`; when `None` (DB-less / tofu-fallback deploy), `/ready`
/// is liveness-equivalent (no DB to be unready against).
type ReadyPool = Option<Arc<RwLock<sqlx::PgPool>>>;

/// Health check response — liveness only. Always 200 while the process
/// is up; never gates on dependencies (a flapping dependency must not
/// kill the pod, only mark it not-ready).
async fn health(metrics: axum::Extension<std::sync::Arc<Metrics>>) -> (StatusCode, String) {
    progress_verdict(&metrics, projection::wedge_window_from_env())
}

fn progress_verdict(metrics: &Metrics, window_seconds: i64) -> (StatusCode, String) {
    match metrics
        .progress_gate
        .verdict(chrono::Utc::now(), window_seconds)
    {
        projection::Progress::Wedged {
            pending_seconds,
            idle_seconds,
        } => {
            warn!(
                pending_seconds,
                idle_seconds, "progress gate: work pending and no template reconcile finished"
            );
            (
                StatusCode::SERVICE_UNAVAILABLE,
                format!("wedged: work pending {pending_seconds}s, no template reconcile for {idle_seconds}s"),
            )
        }
        _ => (StatusCode::OK, "OK".to_string()),
    }
}

/// Readiness check — verifies the operator can serve. When a Postgres
/// pool is wired, runs a short, bounded probe against the two tables
/// magma's state backend actually depends on
/// (`pangea_meta.{artifacts,state_locks}`); a failure (or a timeout past
/// 2s) returns 503 so the pod is pulled from the Service endpoints until
/// the DB is genuinely usable again. With no pool wired (the DB-less
/// deploy), readiness is liveness-equivalent.
///
/// Gap C fix (theory/MAGMA-POSTGRES-LIFECYCLE.md §4, M2): a bare
/// `SELECT 1` only proves the socket connects — it stays green even when
/// the schema was never migrated (a fresh CNPG cluster before `--migrate`
/// / the main path's best-effort ensure ran, or a permissions issue
/// blocking `CREATE TABLE`). Readiness must reflect "magma can actually
/// persist", not just "the pool dialed", so the probe queries the real
/// tables every magma op depends on instead.
async fn ready(
    pool: axum::Extension<ReadyPool>,
    shutting_down: axum::Extension<ShuttingDown>,
    metrics: axum::Extension<std::sync::Arc<Metrics>>,
) -> (StatusCode, &'static str) {
    if progress_verdict(&metrics, projection::wedge_window_from_env() / 2).0 != StatusCode::OK {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            "controller not progressing",
        );
    }
    if shutting_down.0.load(Ordering::Relaxed) {
        // Draining — never report ready again regardless of DB state,
        // so the Service pulls this endpoint before new work routes in.
        return (StatusCode::SERVICE_UNAVAILABLE, "shutting down");
    }
    let Some(pool) = pool.0.as_ref() else {
        // No DB dependency to probe — ready iff the process is up.
        return (StatusCode::OK, "OK");
    };
    // Bound the probe so a hung DB connection can't wedge the readiness
    // handler past the kubelet's own probe timeout.
    let probe = async {
        let pool = pool.read().await;
        sqlx::query("SELECT 1 FROM pangea_meta.artifacts LIMIT 0")
            .execute(&*pool)
            .await?;
        sqlx::query("SELECT 1 FROM pangea_meta.state_locks LIMIT 0")
            .execute(&*pool)
            .await
    };
    match tokio::time::timeout(Duration::from_secs(2), probe).await {
        Ok(Ok(_)) => (StatusCode::OK, "OK"),
        Ok(Err(e)) => {
            warn!(error = %e, "readiness probe: schema-presence check failed");
            (StatusCode::SERVICE_UNAVAILABLE, "db schema unavailable")
        }
        Err(_) => {
            warn!("readiness probe: schema-presence check timed out after 2s");
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
///
/// `shutting_down` is the shared flag `/readyz` checks first (Gap D) —
/// callers share ONE `Arc` across every `run_health_server` call and
/// their own shutdown sequence, so every health server this process
/// runs agrees about whether it's draining.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BindRetry {
    pub attempts: u32,
    pub first_delay: Duration,
    pub max_delay: Duration,
}

impl BindRetry {
    pub const ON: Self = Self {
        attempts: 30,
        first_delay: Duration::from_millis(100),
        max_delay: Duration::from_secs(2),
    };
    pub const OFF: Self = Self {
        attempts: 1,
        first_delay: Duration::ZERO,
        max_delay: Duration::ZERO,
    };

    #[must_use]
    pub fn from_setting(enabled: bool) -> Self {
        if enabled {
            Self::ON
        } else {
            Self::OFF
        }
    }
}

pub async fn bind_with_retry(addr: SocketAddr, retry: BindRetry) -> std::io::Result<TcpListener> {
    let mut delay = retry.first_delay;
    let mut attempt = 1;
    loop {
        match TcpListener::bind(addr).await {
            Ok(listener) => {
                if attempt > 1 {
                    info!(%addr, attempt, "bound after the address was released");
                }
                return Ok(listener);
            }
            Err(e)
                if e.kind() == std::io::ErrorKind::AddrInUse && attempt < retry.attempts =>
            {
                warn!(
                    %addr,
                    attempt,
                    attempts = retry.attempts,
                    retry_in_ms = u64::try_from(delay.as_millis()).unwrap_or(u64::MAX),
                    error = %e,
                    "address in use; retrying bind"
                );
                tokio::time::sleep(delay).await;
                delay = (delay * 2).min(retry.max_delay);
                attempt += 1;
            }
            Err(e) => return Err(e),
        }
    }
}

pub async fn run_health_server(
    addr: SocketAddr,
    metrics: std::sync::Arc<Metrics>,
    ready_pool: ReadyPool,
    shutting_down: ShuttingDown,
    bind_retry: BindRetry,
) -> crate::error::Result<()> {
    let app = Router::new()
        .route("/health", get(health))
        .route("/healthz", get(health))
        .route("/ready", get(ready))
        .route("/readyz", get(ready))
        .route("/metrics", get(metrics_handler))
        .layer(axum::Extension(ready_pool))
        .layer(axum::Extension(shutting_down))
        .layer(axum::Extension(metrics));

    info!(%addr, "Starting health/metrics server");

    let listener = bind_with_retry(addr, bind_retry).await?;
    axum::serve(listener, app)
        .await
        .map_err(|e| crate::error::Error::Io(std::io::Error::new(std::io::ErrorKind::Other, e)))?;

    Ok(())
}

#[cfg(test)]
mod bind_retry_tests {
    use super::*;

    fn held_port() -> (std::net::TcpListener, SocketAddr) {
        let held = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = held.local_addr().unwrap();
        (held, addr)
    }

    #[tokio::test]
    async fn a_port_released_after_a_short_delay_is_bound_by_the_retry() {
        let (held, addr) = held_port();
        let release = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(400)).await;
            drop(held);
        });
        let listener = bind_with_retry(addr, BindRetry::ON).await.unwrap();
        assert_eq!(listener.local_addr().unwrap(), addr);
        release.await.unwrap();
    }

    #[tokio::test]
    async fn bind_retry_off_fails_on_the_first_address_in_use() {
        let (_held, addr) = held_port();
        let err = bind_with_retry(addr, BindRetry::from_setting(false))
            .await
            .unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::AddrInUse);
    }

    #[tokio::test]
    async fn the_retry_is_bounded_and_returns_the_bind_error() {
        let (_held, addr) = held_port();
        let quick = BindRetry {
            attempts: 3,
            first_delay: Duration::from_millis(10),
            max_delay: Duration::from_millis(20),
        };
        let started = std::time::Instant::now();
        let err = bind_with_retry(addr, quick).await.unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::AddrInUse);
        assert!(started.elapsed() < Duration::from_secs(2));
    }
}
