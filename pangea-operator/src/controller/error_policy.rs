//! Shared `error_policy` plumbing — single source of truth for the
//! kube-rs runtime's per-controller error callback.
//!
//! Lifted during the 2026-05-03 review pass (R1). Before this module,
//! 13 controllers each hand-rolled the same shape:
//!
//!   1. emit `metrics.record_reconcile(kind, "error")`
//!   2. log the error at `warn` (or `error`) level
//!   3. compute a requeue duration and return `Action::requeue(d)`
//!
//! The hand-rolls drifted: 3 controllers (`namespace`, `operator_policy`,
//! `template`) silently skipped step (1), so their error reconciles
//! never appeared in the `pangea_controller_reconciliations_total{result="error"}`
//! counter. That kind of drift is exactly what the prime directive
//! says to delete by lifting to a shared helper.
//!
//! Two backoff shapes cover every controller:
//!
//!   * **Fixed(d)** — always requeue at the same interval. Used when the
//!     local `Error` enum doesn't carry retry intent (`architecture_gem`,
//!     `workspace_catalog`, `flow`, `dashboard`, `synthesizer_format`,
//!     `compliance_*`, `namespace`, `operator_policy`).
//!   * **Tiered { retryable, non_retryable }** — requeue at `retryable`
//!     when `Error::is_retryable() == true`, otherwise at the longer
//!     `non_retryable` (transient errors retry fast; permanent errors
//!     back off so we don't burn the API server). Used by
//!     `image_pipeline`, `packer_build`, `ami_test`, `template`.
//!
//! Callers compute the duration before the `run_error_policy` call so
//! the helper itself stays generic over the local `Error` shape — the
//! two controllers that use a non-`crate::error::Error` (`Context`-style
//! sub-error enums in `architecture_gem` and `workspace_catalog`) plug
//! in via the same surface.

use crate::controller::reconciler::{next_requeue, RequeueCooldowns, RequeueOutcome};
use crate::crd::ControllerKind;
use crate::observability::Metrics;
use kube::runtime::controller::Action;
use std::fmt::Display;
use std::time::Duration;
use tracing::warn;

/// Default backoff for non-retryable errors in the tiered variant.
/// Five minutes — long enough to avoid hot-looping on a permanent
/// failure, short enough that a fix gets picked up within one
/// human-in-the-loop cycle. Numerically identical to
/// [`crate::controller::reconciler::DEFAULT_REQUEUE_INTERVAL`] (both
/// 300s) — kept as its own named constant since the two express
/// different intents (permanent-error backoff vs steady-state poll)
/// that happen to share a value today.
pub const NON_RETRYABLE_BACKOFF: Duration = Duration::from_secs(300);

/// Compute a tiered backoff: short interval if retryable, long
/// interval otherwise. Mirrors the duplicated shape that lived in
/// `image_pipeline`, `packer_build`, `ami_test`, and `template`
/// before R1.
///
/// Routed through [`next_requeue`] (theory/MAGMA-POSTGRES-LIFECYCLE.md
/// §3, M0(b) — the pangea-operator port of breathe's
/// `next_requeue`/`ClassCooldowns` outcome-classification shape) rather
/// than the two branches this function used to compute directly. A
/// provable no-op refactor: `RequeueCooldowns::default().steady`
/// (`DEFAULT_REQUEUE_INTERVAL`, 300s) is numerically identical to the
/// `NON_RETRYABLE_BACKOFF` value this branch used to return, so this
/// function's own public output for every input is unchanged — see the
/// `reconciler.rs` test
/// `next_requeue_via_from_retryable_reproduces_tiered_backoff_byte_for_byte`.
#[inline]
pub fn tiered_backoff(retryable: bool) -> Duration {
    next_requeue(RequeueOutcome::from_retryable(retryable), &RequeueCooldowns::default())
}

/// Run the standard error-policy effects: emit the per-controller
/// error counter, log a structured warning with the controller kind,
/// and return an `Action::requeue(backoff)`.
///
/// The single call site for every controller's `error_policy` callback —
/// guarantees that the `pangea_controller_reconciliations_total{result="error"}`
/// counter is incremented for every error path on every controller,
/// which the hand-rolled versions used to drift on.
pub fn run_error_policy<E: Display + ?Sized>(
    metrics: &Metrics,
    kind: ControllerKind,
    error: &E,
    backoff: Duration,
) -> Action {
    metrics.record_reconcile(kind, "error");
    // pangea_reconciliation_errors_total is queried by the shipped
    // PangeaReconciliationErrors alert but, before this, was never
    // recorded — the alert could never fire. The central error path is
    // the one place every controller funnels through, so incrementing
    // here covers the whole fleet with the controller-kind name as the
    // bounded `error_type` label value.
    metrics
        .reconciliation_errors_total
        .with_label_values(&[kind.name()])
        .inc();
    warn!(error = %error, controller = ?kind, "error policy triggered");
    Action::requeue(backoff)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::controller::reconciler::ERROR_REQUEUE_INTERVAL;

    #[test]
    fn tiered_backoff_picks_retryable_for_true() {
        assert_eq!(tiered_backoff(true), ERROR_REQUEUE_INTERVAL);
    }

    #[test]
    fn tiered_backoff_picks_non_retryable_for_false() {
        assert_eq!(tiered_backoff(false), NON_RETRYABLE_BACKOFF);
    }

    #[test]
    fn tiered_backoff_distinguishes_branches() {
        assert_ne!(tiered_backoff(true), tiered_backoff(false));
    }

    #[test]
    fn run_error_policy_increments_error_counter_and_returns_requeue() {
        // Spin up a fresh Metrics + check the counter goes from 0 → 1
        // after a single error_policy call. Validates that the helper
        // can never be called without recording (which was the drift
        // it was lifted to fix).
        let metrics = Metrics::default();
        struct E;
        impl Display for E {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "test error")
            }
        }
        let _action = run_error_policy(
            &metrics,
            ControllerKind::Template,
            &E,
            Duration::from_secs(7),
        );
        // Action requeue duration is internal to kube — we only assert
        // the counter side-effect happened. Looking up by the metric
        // labels we recorded.
        let text = metrics.gather();
        assert!(
            text.contains("controller=\"template\"") && text.contains("result=\"error\""),
            "counter did not record template/error label set:\n{text}"
        );
        // The central error path also feeds the shipped
        // PangeaReconciliationErrors alert's counter, keyed by the
        // controller-kind name as `error_type`.
        assert!(
            text.contains("pangea_reconciliation_errors_total"),
            "reconciliation_errors_total not recorded on the error path:\n{text}"
        );
        assert!(
            text.contains("error_type=\"template\""),
            "error_type label not set to the controller kind name:\n{text}"
        );
    }
}
