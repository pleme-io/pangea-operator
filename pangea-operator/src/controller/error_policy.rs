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
/// Routed through [`next_requeue`] (the pangea-operator port of breathe's
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
    next_requeue(
        RequeueOutcome::from_retryable(retryable),
        &RequeueCooldowns::default(),
    )
}

pub trait ErrorReason {
    fn reason(&self) -> &'static str;
}

pub fn run_error_policy<K, E>(
    metrics: &Metrics,
    kind: ControllerKind,
    object: &K,
    error: &E,
    backoff: Duration,
) -> Action
where
    K: kube::Resource,
    E: Display + ErrorReason + ?Sized,
{
    metrics.record_reconcile(kind, "error");
    let reason = error.reason();
    let meta = object.meta();
    let name = meta.name.as_deref().unwrap_or("");
    let namespace = meta.namespace.as_deref().unwrap_or("");
    metrics
        .reconciliation_errors_total
        .with_label_values(&[kind.name(), reason, name, namespace])
        .inc();
    warn!(
        error = %error,
        controller = ?kind,
        reason,
        name,
        namespace,
        "error policy triggered"
    );
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

    struct E;

    impl Display for E {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "test error")
        }
    }

    impl ErrorReason for E {
        fn reason(&self) -> &'static str {
            "test_reason"
        }
    }

    fn object(name: &str, namespace: &str) -> k8s_openapi::api::core::v1::ConfigMap {
        let mut cm = k8s_openapi::api::core::v1::ConfigMap::default();
        cm.metadata.name = Some(name.to_string());
        cm.metadata.namespace = Some(namespace.to_string());
        cm
    }

    fn error_line(text: &str) -> String {
        text.lines()
            .find(|l| l.starts_with("pangea_reconciliation_errors_total{"))
            .unwrap_or_else(|| panic!("no pangea_reconciliation_errors_total series:\n{text}"))
            .to_string()
    }

    #[test]
    fn run_error_policy_increments_error_counter_and_returns_requeue() {
        let metrics = Metrics::default();
        let _action = run_error_policy(
            &metrics,
            ControllerKind::Template,
            &object("t1", "ns1"),
            &E,
            Duration::from_secs(7),
        );
        let text = metrics.gather();
        assert!(
            text.contains("controller=\"template\"") && text.contains("result=\"error\""),
            "counter did not record template/error label set:\n{text}"
        );
        let line = error_line(&text);
        assert!(line.contains("error_type=\"template\""), "{line}");
        assert!(line.ends_with(" 1"), "{line}");
    }

    #[test]
    fn run_error_policy_labels_the_error_reason_and_the_failing_object() {
        let metrics = Metrics::default();
        run_error_policy(
            &metrics,
            ControllerKind::Template,
            &object("leaf-0001", "pleme"),
            &E,
            Duration::from_secs(7),
        );
        let line = error_line(&metrics.gather());
        assert!(line.contains("reason=\"test_reason\""), "{line}");
        assert!(line.contains("name=\"leaf-0001\""), "{line}");
        assert!(line.contains("target_namespace=\"pleme\""), "{line}");
    }

    #[test]
    fn errors_of_different_reasons_count_on_separate_series() {
        struct Other;
        impl Display for Other {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "other")
            }
        }
        impl ErrorReason for Other {
            fn reason(&self) -> &'static str {
                "other_reason"
            }
        }
        let metrics = Metrics::default();
        let t = object("t", "ns");
        run_error_policy(
            &metrics,
            ControllerKind::Template,
            &t,
            &E,
            Duration::from_secs(1),
        );
        run_error_policy(
            &metrics,
            ControllerKind::Template,
            &t,
            &E,
            Duration::from_secs(1),
        );
        run_error_policy(
            &metrics,
            ControllerKind::Template,
            &t,
            &Other,
            Duration::from_secs(1),
        );
        let text = metrics.gather();
        let series: Vec<&str> = text
            .lines()
            .filter(|l| l.starts_with("pangea_reconciliation_errors_total{"))
            .collect();
        assert_eq!(series.len(), 2, "{text}");
        assert!(
            series
                .iter()
                .any(|l| l.contains("reason=\"test_reason\"") && l.ends_with(" 2")),
            "{text}"
        );
        assert!(
            series
                .iter()
                .any(|l| l.contains("reason=\"other_reason\"") && l.ends_with(" 1")),
            "{text}"
        );
    }
}
