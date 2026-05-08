//! Status-patch helpers shared across controllers.
//!
//! Lifted during the 2026-05-03 review pass (C4). Two helpers
//! eliminate the byte-for-byte duplication between
//! `architecture_gem_controller.rs:369-388` and
//! `workspace_catalog_controller.rs:273-300`:
//!
//!   * [`merge_condition_transitions`] — preserves
//!     `lastTransitionTime` for conditions whose other fields haven't
//!     changed. Without this, every reconcile hot-loops the status
//!     timestamp + drives a downstream reconcile cascade.
//!   * [`patch_status`] — generic Helm-style patch wrapper that
//!     hides the `Api::patch_status + Patch::Merge + serde_json::json!`
//!     boilerplate.
//!
//! The per-type `status_content_equal` checks stay in each
//! controller because their semantics differ (which fields count
//! as "observable" varies by status struct shape).
//!
//! Future controllers adding status patching should use these
//! helpers rather than recopying the pattern.

use kube::api::{Api, Patch, PatchParams};
use kube::Resource;
use serde::de::DeserializeOwned;
use serde::Serialize;

/// Trait abstracting the standard Kubernetes Condition shape.
/// Each per-CRD `Condition` type (arch_gem, workspace_catalog, …)
/// implements this so the generic
/// [`merge_condition_transitions`] works on any of them.
///
/// The five accessors are the minimum needed for transition-time
/// preservation. Last-transition timestamps are typed as the consumer
/// Condition's own associated `Time` to avoid pulling in an extra
/// k8s_openapi dependency for callers that already have one.
pub trait ConditionLike {
    type Time: Clone;

    fn condition_type(&self) -> &str;
    fn status(&self) -> &str;
    fn reason(&self) -> &str;
    fn message(&self) -> &str;
    fn last_transition_time(&self) -> &Self::Time;
    fn set_last_transition_time(&mut self, t: Self::Time);
}

/// Preserve `lastTransitionTime` on conditions whose status, reason,
/// and message are unchanged from the previous reconcile.
///
/// Without this, every reconcile that builds a fresh `Vec<Condition>`
/// stamps each condition with `now()`, even when the condition's
/// content is unchanged. That timestamp churn makes Kubernetes
/// observers (and Flux's HelmRelease drift-detection) think
/// something changed — driving a hot reconcile loop.
///
/// Algorithm: for each condition in `new`, find a same-type condition
/// in `prev`; if all observable fields (status, reason, message)
/// match, copy `prev`'s `lastTransitionTime` into the new one.
/// Otherwise keep `new`'s timestamp.
///
/// Pure function — no I/O, no side effects, deterministic.
pub fn merge_condition_transitions<C: ConditionLike + Clone>(
    prev: &[C],
    new: Vec<C>,
) -> Vec<C> {
    new.into_iter()
        .map(|mut n| {
            if let Some(p) = prev.iter().find(|p| p.condition_type() == n.condition_type()) {
                if p.status() == n.status()
                    && p.reason() == n.reason()
                    && p.message() == n.message()
                {
                    n.set_last_transition_time(p.last_transition_time().clone());
                }
            }
            n
        })
        .collect()
}

/// Compare two condition lists semantically — `(type, status, reason,
/// message)` — ignoring `lastTransitionTime`.
///
/// Why: `create_condition()` (and equivalent constructors) restamp
/// `lastTransitionTime = Utc::now()` on every call, so a byte-equal
/// `Vec<Condition> == Vec<Condition>` check ALWAYS returns false.
/// Diff-gates that use it as a "did anything change?" check fall
/// through into the always-PATCH path and re-trigger their own
/// watches — the canonical self-trigger loop fixed across all
/// controllers in the rio firefighting 2026-05-07 wave.
///
/// This helper is the lifted, single-implementation version of the
/// condition-comparison pattern that was hand-rolled in 5+ controller
/// files (template, flow, compliance_binding, synthesizer_format,
/// fleet_status). Adding a new diff-gate? Use this instead of
/// reimplementing.
///
/// Returns true iff every condition in `new` has a same-typed
/// condition in `prev` with matching status + reason + message AND
/// the lengths match (so an extra new condition forces a patch).
/// Order-insensitive — finding a same-typed match anywhere in `prev`
/// is enough.
///
/// Pure function — no I/O, deterministic, easy to test.
pub fn conditions_observably_equal<C: ConditionLike>(prev: &[C], new: &[C]) -> bool {
    if prev.len() != new.len() {
        return false;
    }
    new.iter().all(|n| {
        prev.iter().any(|p| {
            p.condition_type() == n.condition_type()
                && p.status() == n.status()
                && p.reason() == n.reason()
                && p.message() == n.message()
        })
    })
}

/// Patch a Kubernetes resource's status sub-resource with the given
/// status struct. Wraps the typical `Api::patch_status + Patch::Merge
/// + serde_json::json!({"status": ...})` boilerplate.
///
/// Returns the kube error directly so callers can map it via `?`.
///
/// Use as the final step of a `patch_*_if_changed` helper after
/// running content-equality + condition-transition merging:
///
/// ```ignore
/// async fn patch_my_status_if_changed(...) -> Result<(), Error> {
///     if let Some(prev) = old {
///         new.conditions = merge_condition_transitions(&prev.conditions, new.conditions);
///         if my_status_content_equal(prev, &new) {
///             return Ok(());  // no-op patch
///         }
///     }
///     status::patch_status::<MyResource, _>(client, name, &new).await?;
///     Ok(())
/// }
/// ```
pub async fn patch_status<R, S>(
    client: &kube::Client,
    name: &str,
    new_status: &S,
) -> Result<(), kube::Error>
where
    R: Resource<DynamicType = ()> + Clone + DeserializeOwned + std::fmt::Debug,
    S: Serialize,
{
    let api: Api<R> = Api::all(client.clone());
    let pp = PatchParams::default();
    let patch = serde_json::json!({ "status": new_status });
    api.patch_status(name, &pp, &Patch::Merge(&patch)).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Minimal test fixture implementing ConditionLike. Real consumers
    /// (arch_gem::Condition, workspace_catalog::Condition) impl the
    /// trait on their own types; this fixture proves the generic
    /// algorithm is correct in isolation.
    #[derive(Clone, Debug, PartialEq)]
    struct TestCond {
        typ: String,
        status: String,
        reason: String,
        message: String,
        ts: i32,
    }
    impl ConditionLike for TestCond {
        type Time = i32;
        fn condition_type(&self) -> &str { &self.typ }
        fn status(&self) -> &str { &self.status }
        fn reason(&self) -> &str { &self.reason }
        fn message(&self) -> &str { &self.message }
        fn last_transition_time(&self) -> &i32 { &self.ts }
        fn set_last_transition_time(&mut self, t: i32) { self.ts = t; }
    }

    fn cond(typ: &str, status: &str, reason: &str, msg: &str, ts: i32) -> TestCond {
        TestCond { typ: typ.into(), status: status.into(), reason: reason.into(), message: msg.into(), ts }
    }

    #[test]
    fn merge_preserves_timestamp_when_content_unchanged() {
        let prev = vec![cond("Ready", "True", "Healthy", "all good", 2025)];
        let new = vec![cond("Ready", "True", "Healthy", "all good", 2026)];
        let merged = merge_condition_transitions(&prev, new);
        assert_eq!(merged[0].ts, prev[0].ts);
    }

    #[test]
    fn merge_advances_timestamp_when_status_flips() {
        let prev = vec![cond("Ready", "True", "Healthy", "all good", 2025)];
        let new = vec![cond("Ready", "False", "Sick", "down", 2026)];
        let merged = merge_condition_transitions(&prev, new);
        assert_ne!(merged[0].ts, prev[0].ts);
        assert_eq!(merged[0].status, "False");
    }

    #[test]
    fn merge_keeps_new_for_unmatched_types() {
        let prev = vec![cond("Ready", "True", "Healthy", "ok", 2025)];
        let new = vec![cond("Verified", "True", "Audited", "ok", 2026)];
        let merged = merge_condition_transitions(&prev, new.clone());
        assert_eq!(merged[0].ts, new[0].ts);
    }

    #[test]
    fn merge_handles_empty_prev() {
        let new = vec![cond("Ready", "True", "Healthy", "ok", 2026)];
        let merged = merge_condition_transitions(&[], new.clone());
        assert_eq!(merged.len(), 1);
        assert_eq!(merged[0].ts, new[0].ts);
    }

    // ── conditions_observably_equal — the lifted diff-gate primitive ─

    #[test]
    fn observable_equality_ignores_timestamp() {
        let prev = vec![cond("Ready", "True", "Healthy", "ok", 2025)];
        let new = vec![cond("Ready", "True", "Healthy", "ok", 2026)];
        assert!(
            conditions_observably_equal(&prev, &new),
            "byte-equal except for timestamp must read as equal — that's the whole point of the gate"
        );
    }

    #[test]
    fn observable_equality_detects_status_flip() {
        let prev = vec![cond("Ready", "True", "Healthy", "ok", 2025)];
        let new = vec![cond("Ready", "False", "Sick", "down", 2026)];
        assert!(!conditions_observably_equal(&prev, &new));
    }

    #[test]
    fn observable_equality_detects_reason_change() {
        let prev = vec![cond("Ready", "True", "Healthy", "ok", 2025)];
        let new = vec![cond("Ready", "True", "Verified", "ok", 2025)];
        assert!(!conditions_observably_equal(&prev, &new));
    }

    #[test]
    fn observable_equality_detects_message_change() {
        let prev = vec![cond("Ready", "True", "Healthy", "ok", 2025)];
        let new = vec![cond("Ready", "True", "Healthy", "still ok", 2025)];
        assert!(!conditions_observably_equal(&prev, &new));
    }

    #[test]
    fn observable_equality_detects_extra_new_condition() {
        let prev = vec![cond("Ready", "True", "Healthy", "ok", 2025)];
        let new = vec![
            cond("Ready", "True", "Healthy", "ok", 2025),
            cond("Verified", "True", "Audited", "ok", 2025),
        ];
        assert!(
            !conditions_observably_equal(&prev, &new),
            "extra new condition must force PATCH (length differs)"
        );
    }

    #[test]
    fn observable_equality_detects_missing_new_condition() {
        let prev = vec![
            cond("Ready", "True", "Healthy", "ok", 2025),
            cond("Verified", "True", "Audited", "ok", 2025),
        ];
        let new = vec![cond("Ready", "True", "Healthy", "ok", 2025)];
        assert!(
            !conditions_observably_equal(&prev, &new),
            "missing new condition must force PATCH"
        );
    }

    #[test]
    fn observable_equality_is_order_insensitive() {
        // Same conditions, different order. Order-insensitive matching
        // is intentional — controllers that sort conditions for display
        // shouldn't trip the gate.
        let prev = vec![
            cond("Ready", "True", "Healthy", "ok", 2025),
            cond("Verified", "True", "Audited", "ok", 2025),
        ];
        let new = vec![
            cond("Verified", "True", "Audited", "ok", 2026),
            cond("Ready", "True", "Healthy", "ok", 2026),
        ];
        assert!(conditions_observably_equal(&prev, &new));
    }

    #[test]
    fn observable_equality_empty_lists_match() {
        let prev: Vec<TestCond> = vec![];
        let new: Vec<TestCond> = vec![];
        assert!(conditions_observably_equal(&prev, &new));
    }
}
