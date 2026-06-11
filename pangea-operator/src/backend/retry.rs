//! Bounded connect-retry-with-backoff for the Postgres state backend.
//!
//! # Why this exists
//!
//! The operator's state lives in a CloudNativePG (CNPG) cluster that
//! exposes three services: `pangea-database-rw` (primary, writes),
//! `pangea-database-ro` (replicas, reads), and `pangea-database-r`
//! (any). During an **unsupervised CNPG switchover** the `-rw` service
//! has no ready primary for a few seconds — TCP `connect: connection
//! refused` — and sqlx does **not** retry the initial dial. Without a
//! retry the state read/write errors hard and the whole reconcile
//! surfaces a cycle error (observed on rio: 125× `dial tcp
//! pangea-database-rw:5432 connect: connection refused` across a
//! switchover window).
//!
//! This module makes a transient refuse *recover within the backoff*
//! instead of surfacing as a cycle error. It retries **only** on
//! connection-level errors (refused / closed / pool-acquire timeout) —
//! never on a query/constraint error, where a retry would re-run a
//! genuinely-wrong statement and mask the real fault.
//!
//! # Design
//!
//! Two pieces:
//!
//!   1. [`is_connection_level`] — classifies a `sqlx::Error` into
//!      "the dial/pool failed, the operation never reached a healthy
//!      backend" (retryable) vs "the backend rejected the statement"
//!      (not retryable). This is the connection-vs-logical split the
//!      org error taxonomy already draws at the `Error` layer
//!      (`error.rs::failure_kind`), applied one level lower to the raw
//!      sqlx error so the decorator can decide per-attempt.
//!
//!   2. [`RetryingStateBackend`] — a `StateBackend` decorator that
//!      wraps any inner backend and re-invokes each op on a
//!      connection-level `Error::Database` with exponential backoff
//!      (200ms → ~3s, ~10s total cap). Every `StateBackend` op is
//!      idempotent by contract (reads, `INSERT … ON CONFLICT DO
//!      UPDATE` upserts, idempotent deletes), so a retry can never
//!      double-apply.
//!
//! The decorator is orthogonal to *which* concrete backend is live
//! (`TofuPgStateBackend` in production, `PostgresStateBackend`,
//! `InMemoryStateBackend` in tests) — it wraps the trait, so both the
//! read and write paths get the retry uniformly. Routing reads to
//! `-ro` / writes to `-rw` is a follow-up (the pool here is built from
//! a single host today); the retry alone closes the switchover-window
//! failure.

use crate::backend::state::{StateEntry, TerraformState};
use crate::backend::state_backend::StateBackend;
use crate::error::{Error, Result};

use async_trait::async_trait;
use std::sync::Arc;
use std::time::Duration;
use tracing::warn;

/// Bounded backoff schedule for connection-level retries.
///
/// 5 attempts, exponential 200ms → ~3s, ~10s total cap. Cheap +
/// idempotent: a transient refuse during a CNPG switchover recovers
/// inside this window instead of surfacing as a cycle error.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetryPolicy {
    /// Total attempts including the first (so `attempts = 5` means 1
    /// initial try + 4 retries).
    pub attempts: u32,
    /// Backoff before the first retry. Doubles each subsequent retry,
    /// clamped to `max_backoff`.
    pub base_backoff: Duration,
    /// Upper bound on any single backoff sleep.
    pub max_backoff: Duration,
}

impl RetryPolicy {
    /// The CNPG-switchover default: 5 attempts, 200ms base, 3s cap.
    /// Worst-case wall time ≈ 200ms + 400ms + 800ms + 1.6s ≈ 3s of
    /// sleeping across 4 retries (well under the ~10s cap; the cap
    /// matters only if `attempts` is raised).
    pub fn cnpg_switchover_default() -> Self {
        Self {
            attempts: 5,
            base_backoff: Duration::from_millis(200),
            max_backoff: Duration::from_secs(3),
        }
    }

    /// The backoff to sleep *before* retry number `retry_index`
    /// (0-based: `retry_index = 0` is the sleep before the 2nd attempt).
    /// `base * 2^retry_index`, clamped to `max_backoff`.
    fn backoff_for(&self, retry_index: u32) -> Duration {
        let factor = 1u64.checked_shl(retry_index).unwrap_or(u64::MAX);
        let scaled = self
            .base_backoff
            .saturating_mul(u32::try_from(factor).unwrap_or(u32::MAX));
        scaled.min(self.max_backoff)
    }
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self::cnpg_switchover_default()
    }
}

/// Is this raw sqlx error a **connection-level** failure — the dial /
/// pool never delivered the statement to a healthy backend — as opposed
/// to a **logical** failure where the backend ran the statement and
/// rejected it?
///
/// Retry only the connection-level class. A query error (bad SQL, a
/// constraint violation, a missing table) is deterministic: retrying it
/// just re-runs a wrong statement and hides the real fault.
///
/// Classified as connection-level:
///   * [`sqlx::Error::Io`] — TCP-level failure (connection refused /
///     reset / closed), the literal `connect: connection refused` case.
///   * [`sqlx::Error::PoolTimedOut`] — every pooled connection was busy
///     or the pool couldn't establish one inside `acquire_timeout`
///     (a switchover starves the pool the same way a refuse does).
///   * [`sqlx::Error::PoolClosed`] — the pool was torn down mid-flight.
///   * [`sqlx::Error::Database`] with a Postgres class-08 SQLSTATE
///     (`connection_exception` family) — the server reported a
///     connection-level fault rather than a logical one.
pub fn is_connection_level(err: &sqlx::Error) -> bool {
    match err {
        sqlx::Error::Io(_) | sqlx::Error::PoolTimedOut | sqlx::Error::PoolClosed => true,
        sqlx::Error::Database(db) => db
            .code()
            // SQLSTATE class 08 = "Connection Exception" (08000,
            // 08003 connection_does_not_exist, 08006
            // connection_failure, 08001, 08004). 57P01 = admin_shutdown
            // (primary terminating during a switchover). Both are the
            // backend telling us the connection — not the statement —
            // is the problem.
            .map(|c| c.starts_with("08") || c == "57P01")
            .unwrap_or(false),
        _ => false,
    }
}

/// True iff this `Error` should trigger a connection-level retry —
/// i.e. it is an `Error::Database` whose inner sqlx error
/// [`is_connection_level`]. `WithContext` recurses so a wrapped
/// database error is still classified correctly (mirrors
/// `Error::failure_kind`'s recursion).
fn is_retryable_connection_error(err: &Error) -> bool {
    match err {
        Error::Database(e) => is_connection_level(e),
        Error::WithContext { source, .. } => is_retryable_connection_error(source),
        _ => false,
    }
}

/// A [`StateBackend`] decorator that retries connection-level failures
/// of the inner backend with bounded exponential backoff.
///
/// Wraps any `StateBackend` (production `TofuPgStateBackend`, the
/// `PostgresStateBackend`, or test `InMemoryStateBackend`) so the
/// retry is orthogonal to the concrete state-row layout. Every op is
/// idempotent by the `StateBackend` contract, so a retried op can never
/// double-apply.
pub struct RetryingStateBackend {
    inner: Arc<dyn StateBackend>,
    policy: RetryPolicy,
}

impl RetryingStateBackend {
    /// Wrap `inner` with the CNPG-switchover default retry policy.
    pub fn new(inner: Arc<dyn StateBackend>) -> Self {
        Self {
            inner,
            policy: RetryPolicy::cnpg_switchover_default(),
        }
    }

    /// Wrap `inner` with an explicit retry policy (used in tests).
    pub fn with_policy(inner: Arc<dyn StateBackend>, policy: RetryPolicy) -> Self {
        Self { inner, policy }
    }

    /// Run `op` up to `policy.attempts` times, retrying only on a
    /// connection-level error with exponential backoff between tries.
    /// `op_name` labels the operation in retry logs. Pure control-flow
    /// helper; the actual I/O is the closure the caller passes.
    async fn run_with_retry<T, F, Fut>(&self, op_name: &str, mut op: F) -> Result<T>
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = Result<T>>,
    {
        let mut attempt: u32 = 0;
        loop {
            match op().await {
                Ok(v) => return Ok(v),
                Err(e) => {
                    let is_last = attempt + 1 >= self.policy.attempts;
                    if is_last || !is_retryable_connection_error(&e) {
                        // Either out of attempts, or a logical error we
                        // must surface as-is (never retry a wrong query).
                        return Err(e);
                    }
                    let backoff = self.policy.backoff_for(attempt);
                    warn!(
                        op = op_name,
                        attempt = attempt + 1,
                        max_attempts = self.policy.attempts,
                        backoff_ms = backoff.as_millis() as u64,
                        error = %e,
                        "state-backend connection-level error; retrying after backoff \
                         (likely CNPG switchover window)"
                    );
                    tokio::time::sleep(backoff).await;
                    attempt += 1;
                }
            }
        }
    }
}

#[async_trait]
impl StateBackend for RetryingStateBackend {
    async fn get_state(
        &self,
        schema_name: &str,
        template_name: &str,
        state_name: &str,
    ) -> Result<Option<StateEntry>> {
        self.run_with_retry("get_state", || {
            self.inner.get_state(schema_name, template_name, state_name)
        })
        .await
    }

    async fn get_parsed_state(
        &self,
        schema_name: &str,
        template_name: &str,
        state_name: &str,
    ) -> Result<Option<TerraformState>> {
        self.run_with_retry("get_parsed_state", || {
            self.inner
                .get_parsed_state(schema_name, template_name, state_name)
        })
        .await
    }

    async fn save_state(
        &self,
        schema_name: &str,
        template_name: &str,
        state_name: &str,
        data: &[u8],
    ) -> Result<i64> {
        // `save_state` is an upsert (`INSERT … ON CONFLICT DO UPDATE`),
        // so re-running it after a connection-level failure is safe.
        self.run_with_retry("save_state", || {
            self.inner
                .save_state(schema_name, template_name, state_name, data)
        })
        .await
    }

    async fn delete_state(
        &self,
        schema_name: &str,
        template_name: &str,
        state_name: &str,
    ) -> Result<bool> {
        // Delete is idempotent (a second delete returns false, never
        // errors), so retrying after a connection-level failure is safe.
        self.run_with_retry("delete_state", || {
            self.inner
                .delete_state(schema_name, template_name, state_name)
        })
        .await
    }

    async fn list_states(&self, schema_name: &str, template_name: &str) -> Result<Vec<StateEntry>> {
        self.run_with_retry("list_states", || {
            self.inner.list_states(schema_name, template_name)
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::state_backend::InMemoryStateBackend;
    use std::sync::atomic::{AtomicU32, Ordering};

    // ── RetryPolicy backoff schedule ──────────────────────────────

    #[test]
    fn backoff_doubles_then_clamps_to_max() {
        let p = RetryPolicy {
            attempts: 6,
            base_backoff: Duration::from_millis(200),
            max_backoff: Duration::from_secs(3),
        };
        assert_eq!(p.backoff_for(0), Duration::from_millis(200));
        assert_eq!(p.backoff_for(1), Duration::from_millis(400));
        assert_eq!(p.backoff_for(2), Duration::from_millis(800));
        assert_eq!(p.backoff_for(3), Duration::from_millis(1600));
        // 200ms * 2^4 = 3200ms, clamped to the 3s max.
        assert_eq!(p.backoff_for(4), Duration::from_secs(3));
        // Large shift never panics; stays clamped.
        assert_eq!(p.backoff_for(40), Duration::from_secs(3));
    }

    #[test]
    fn default_policy_is_the_cnpg_switchover_shape() {
        let p = RetryPolicy::default();
        assert_eq!(p.attempts, 5);
        assert_eq!(p.base_backoff, Duration::from_millis(200));
        assert_eq!(p.max_backoff, Duration::from_secs(3));
    }

    // ── is_connection_level classification ────────────────────────

    #[test]
    fn io_error_is_connection_level() {
        // The literal `connect: connection refused` case arrives as
        // sqlx::Error::Io wrapping a std::io::Error.
        let io = std::io::Error::new(std::io::ErrorKind::ConnectionRefused, "connection refused");
        let err = sqlx::Error::Io(io);
        assert!(is_connection_level(&err));
    }

    #[test]
    fn pool_timeout_and_closed_are_connection_level() {
        assert!(is_connection_level(&sqlx::Error::PoolTimedOut));
        assert!(is_connection_level(&sqlx::Error::PoolClosed));
    }

    #[test]
    fn row_not_found_is_not_connection_level() {
        // A logical "no row" is NOT a connection failure — retrying
        // would just re-run the same query and get the same answer.
        assert!(!is_connection_level(&sqlx::Error::RowNotFound));
    }

    #[test]
    fn error_wrapper_classifies_connection_vs_logical() {
        let conn = Error::Database(sqlx::Error::PoolTimedOut);
        assert!(is_retryable_connection_error(&conn));
        // WithContext recurses (mirrors failure_kind's recursion).
        assert!(is_retryable_connection_error(&conn.context("at save")));

        // A non-database error is never a connection-level retry.
        let logical = Error::Config("bad".into());
        assert!(!is_retryable_connection_error(&logical));

        // RowNotFound is a database error but a LOGICAL one — no retry.
        let not_found = Error::Database(sqlx::Error::RowNotFound);
        assert!(!is_retryable_connection_error(&not_found));
    }

    // ── RetryingStateBackend behavior ─────────────────────────────

    /// A backend that fails its first `fail_count` calls with a given
    /// error, then delegates to an inner InMemory backend. Lets us
    /// assert retry-then-succeed and no-retry-on-logical behaviors.
    struct FlakyBackend {
        inner: InMemoryStateBackend,
        remaining_failures: AtomicU32,
        make_error: fn() -> Error,
        attempts: AtomicU32,
    }

    impl FlakyBackend {
        fn new(fail_count: u32, make_error: fn() -> Error) -> Self {
            Self {
                inner: InMemoryStateBackend::new(),
                remaining_failures: AtomicU32::new(fail_count),
                make_error,
                attempts: AtomicU32::new(0),
            }
        }

        fn maybe_fail(&self) -> Result<()> {
            self.attempts.fetch_add(1, Ordering::SeqCst);
            if self.remaining_failures.load(Ordering::SeqCst) > 0 {
                self.remaining_failures.fetch_sub(1, Ordering::SeqCst);
                Err((self.make_error)())
            } else {
                Ok(())
            }
        }
    }

    #[async_trait]
    impl StateBackend for FlakyBackend {
        async fn get_state(&self, s: &str, t: &str, n: &str) -> Result<Option<StateEntry>> {
            self.maybe_fail()?;
            self.inner.get_state(s, t, n).await
        }
        async fn get_parsed_state(
            &self,
            s: &str,
            t: &str,
            n: &str,
        ) -> Result<Option<TerraformState>> {
            self.maybe_fail()?;
            self.inner.get_parsed_state(s, t, n).await
        }
        async fn save_state(&self, s: &str, t: &str, n: &str, data: &[u8]) -> Result<i64> {
            self.maybe_fail()?;
            self.inner.save_state(s, t, n, data).await
        }
        async fn delete_state(&self, s: &str, t: &str, n: &str) -> Result<bool> {
            self.maybe_fail()?;
            self.inner.delete_state(s, t, n).await
        }
        async fn list_states(&self, s: &str, t: &str) -> Result<Vec<StateEntry>> {
            self.maybe_fail()?;
            self.inner.list_states(s, t).await
        }
    }

    fn fast_policy() -> RetryPolicy {
        // Tiny backoffs so the test runs instantly.
        RetryPolicy {
            attempts: 5,
            base_backoff: Duration::from_millis(1),
            max_backoff: Duration::from_millis(4),
        }
    }

    fn conn_error() -> Error {
        Error::Database(sqlx::Error::PoolTimedOut)
    }

    fn logical_error() -> Error {
        Error::Database(sqlx::Error::RowNotFound)
    }

    #[tokio::test]
    async fn retries_connection_error_then_succeeds() {
        // 2 connection-level failures then success → the wrapper must
        // recover within the attempt budget.
        let flaky = Arc::new(FlakyBackend::new(2, conn_error));
        let counter = flaky.clone();
        let backend = RetryingStateBackend::with_policy(flaky, fast_policy());
        let got = backend.get_state("s", "t", "n").await.unwrap();
        assert!(got.is_none());
        // 2 failed + 1 successful = 3 attempts.
        assert_eq!(counter.attempts.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn does_not_retry_logical_error() {
        // A logical (non-connection) error must surface on the FIRST
        // attempt — retrying a wrong query is the anti-pattern.
        let flaky = Arc::new(FlakyBackend::new(1, logical_error));
        let counter = flaky.clone();
        let backend = RetryingStateBackend::with_policy(flaky, fast_policy());
        let err = backend.get_state("s", "t", "n").await.unwrap_err();
        assert!(matches!(err, Error::Database(sqlx::Error::RowNotFound)));
        assert_eq!(
            counter.attempts.load(Ordering::SeqCst),
            1,
            "logical error must not be retried"
        );
    }

    #[tokio::test]
    async fn gives_up_after_attempt_budget_on_persistent_connection_error() {
        // More failures than attempts → surface the connection error
        // after exhausting the budget (5 attempts).
        let flaky = Arc::new(FlakyBackend::new(99, conn_error));
        let counter = flaky.clone();
        let backend = RetryingStateBackend::with_policy(flaky, fast_policy());
        let err = backend.save_state("s", "t", "n", b"x").await.unwrap_err();
        assert!(matches!(err, Error::Database(sqlx::Error::PoolTimedOut)));
        assert_eq!(
            counter.attempts.load(Ordering::SeqCst),
            5,
            "should attempt exactly `attempts` times then give up"
        );
    }

    #[tokio::test]
    async fn save_then_get_round_trips_through_the_decorator() {
        // No failures: the decorator is a transparent pass-through.
        let inner: Arc<dyn StateBackend> = Arc::new(InMemoryStateBackend::new());
        let backend = RetryingStateBackend::new(inner);
        let id = backend.save_state("s", "t", "n", b"hello").await.unwrap();
        assert_eq!(id, 1);
        let got = backend.get_state("s", "t", "n").await.unwrap().unwrap();
        assert_eq!(got.data.as_deref(), Some(&b"hello"[..]));
    }
}
