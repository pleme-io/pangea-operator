//! Pre-reconcile policy pipeline — single ordered entry point for the
//! gates that fire before any controller's reconcile work begins.
//!
//! Lifted during the 2026-05-03 review pass (S2). Before this module,
//! the gate dispatch was scattered across:
//!
//!   * `policy_gate::check_operator_policy(state, kind)`
//!     — global/controller-class kill-switch
//!   * `policy_gate::check_template_workspace_policy(state, ns, name, parent)`
//!     — per-workspace tri-state pause (template/catalog/controller/global)
//!   * inline checks in `template_controller::reconcile_template:108-143`
//!     and similar hand-rolled blocks in 11+ controllers
//!
//! The state machine was correct but hard to trace — adding a new gate
//! required editing every controller. Now there is exactly one
//! `PreReconcilePipeline::run_*` per resource shape, and the order is
//! explicit + documented.
//!
//! The `reactive` and `policy_cascade` modules stay separate (those
//! fire AT or AFTER reconcile, not before) — those would consolidate
//! into a `post_reconcile_pipeline` module if scattering re-emerges.
//!
//! ## Three "policy" modules — what each one does
//!
//! Naming is overloaded; here is the precise carve-out:
//!
//! | module | when it fires | takes | purpose |
//! |---|---|---|---|
//! | `policy_pipeline` (this file) | **before** reconcile | a `ControllerState` or `OperatorPolicyCache` | single ordered entry point for every gate (kill-switch, per-workspace pause). Returns `Proceed` or `SkipWith(Action)`. |
//! | `policy_gate` | called by `policy_pipeline` | the same | decision primitives: `check_operator_policy`, `evaluate_against_cache`, `check_template_workspace_policy`. Pure, no-side-effect functions. |
//! | `policy_cascade` | **at apply-time** (after reconcile decided to proceed) | typed CRD specs | resolves the four-level Gem → Catalog → Template → Resource policy walk into a single `ResolvedPolicy`. Used by the apply path + admission webhook + `kubectl pangea policy explain`. |
//!
//! Rule of thumb: if you're answering "should we even reconcile?" use
//! `policy_pipeline`. If you're answering "given we're reconciling,
//! what policies apply?" use `policy_cascade`.

use crate::controller::operator_policy_cache::OperatorPolicyCache;
use crate::controller::{policy_gate, ControllerState};
use crate::crd::ControllerKind;
use kube::runtime::controller::Action;

/// Result of running the pre-reconcile gates: either "proceed with the
/// reconcile body" or "short-circuit with this requeue action".
///
/// Why an enum rather than `Option<Action>`: the caller's intent (skip
/// vs. proceed) is clearer at the call site, and a future variant
/// (e.g., `ProceedWithCarveOut`) can be added without breaking the
/// existing match arms.
#[must_use]
#[derive(Debug)]
pub enum PreReconcileDecision {
    /// All gates passed; controller should run the reconcile body.
    Proceed,
    /// A gate fired; controller should return this Action and skip
    /// the body entirely.
    SkipWith(Action),
}

impl PreReconcileDecision {
    /// Maps the decision to `Some(action)` when skipping or `None`
    /// when proceeding. Convenient for `if let Some(action) = ... { return Ok(action); }`
    /// call sites.
    pub fn into_skip_action(self) -> Option<Action> {
        match self {
            PreReconcileDecision::Proceed => None,
            PreReconcileDecision::SkipWith(a) => Some(a),
        }
    }
}

/// Run the full pre-reconcile pipeline for an `InfrastructureTemplate`.
///
/// Pipeline order (each gate runs only if the previous one passed):
///
///   1. Cluster-wide kill-switch (`OperatorPolicy.spec.globalSuspend` +
///      `controllerSuspend.template`).
///   2. Per-workspace tri-state pause (template → catalog cascade →
///      controller → global), resolved by
///      `policy_gate::check_template_workspace_policy`.
///
/// Per-CR `template.spec.suspend` and the ReactivePolicy auto-suspend
/// check are NOT run here — they need template-specific data and live
/// inside `template_controller::reconcile_template` after the
/// deletion-finalizer + parent-WorkspaceCatalog lookup.
pub fn run_for_template(
    state: &ControllerState,
    template_namespace: &str,
    template_name: &str,
    parent_catalog_name: Option<&str>,
) -> PreReconcileDecision {
    if let Some(action) = policy_gate::check_operator_policy(state, ControllerKind::Template) {
        return PreReconcileDecision::SkipWith(action);
    }
    if let Some(action) = policy_gate::check_template_workspace_policy(
        state,
        template_namespace,
        template_name,
        parent_catalog_name,
    ) {
        return PreReconcileDecision::SkipWith(action);
    }
    PreReconcileDecision::Proceed
}

/// Run the pre-reconcile pipeline for a controller that has no
/// per-workspace surface — i.e., every controller other than
/// `template_controller`. Currently just the global/controller-class
/// kill-switch; reserved as a stable entry point so future fleet-wide
/// gates can be added in one place.
pub fn run_for_controller(
    state: &ControllerState,
    controller: ControllerKind,
) -> PreReconcileDecision {
    if let Some(action) = policy_gate::check_operator_policy(state, controller) {
        return PreReconcileDecision::SkipWith(action);
    }
    PreReconcileDecision::Proceed
}

/// Cache-only variant of `run_for_controller` for controllers whose
/// context type is not `ControllerState` (`architecture_gem_controller`,
/// `workspace_catalog_controller`). Behaves identically; takes only
/// the cache. Front door for those two controllers' kill-switch check —
/// keeping them on the same entry point as the rest of the fleet so
/// that future pipeline gates apply uniformly.
pub fn run_for_controller_with_cache(
    cache: &OperatorPolicyCache,
    controller: ControllerKind,
) -> PreReconcileDecision {
    if let Some(action) = policy_gate::evaluate_against_cache(cache, controller) {
        return PreReconcileDecision::SkipWith(action);
    }
    PreReconcileDecision::Proceed
}

/// Cache-only variant of the workspace-catalog gate. Resolves the
/// per-catalog tri-state pause directive from `OperatorPolicy.spec
/// .workspaceSuspend.catalogs[<name>]`. Used exclusively by
/// `workspace_catalog_controller` (the catalog level of the
/// hierarchical pause cascade); other controllers bottom out at
/// `run_for_controller_with_cache`.
pub fn run_for_catalog_with_cache(
    cache: &OperatorPolicyCache,
    catalog_name: &str,
) -> PreReconcileDecision {
    // Front door consolidation — same shape as run_for_controller_with_cache,
    // but for the catalog tier. The kill-switch itself runs first; this
    // function only resolves the catalog-level override.
    if let Some(action) = policy_gate::evaluate_catalog_against_cache(cache, catalog_name) {
        return PreReconcileDecision::SkipWith(action);
    }
    PreReconcileDecision::Proceed
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crd::OperatorPolicySpec;
    use kube::runtime::controller::Action;

    #[test]
    fn into_skip_action_proceed_returns_none() {
        let d = PreReconcileDecision::Proceed;
        assert!(d.into_skip_action().is_none());
    }

    #[test]
    fn into_skip_action_skip_returns_some() {
        let d = PreReconcileDecision::SkipWith(Action::requeue(std::time::Duration::from_secs(30)));
        let a = d.into_skip_action();
        assert!(a.is_some());
    }

    // ── Cache-only entry point coverage (R4) ──

    #[test]
    fn cache_pipeline_proceeds_when_policy_is_permissive() {
        // Default-allow cache + arbitrary controller → no skip.
        let cache = OperatorPolicyCache::new_permissive();
        let decision = run_for_controller_with_cache(&cache, ControllerKind::ArchitectureGem);
        assert!(matches!(decision, PreReconcileDecision::Proceed));
    }

    #[test]
    fn cache_pipeline_skips_when_global_suspend_is_on() {
        let cache = OperatorPolicyCache::new_permissive();
        cache.store(OperatorPolicySpec {
            global_suspend: true,
            global_suspend_reason: Some("review pass".into()),
            ..Default::default()
        });
        let decision = run_for_controller_with_cache(&cache, ControllerKind::WorkspaceCatalog);
        assert!(
            matches!(decision, PreReconcileDecision::SkipWith(_)),
            "expected SkipWith when globalSuspend=true"
        );
    }

    #[test]
    fn catalog_cache_pipeline_proceeds_when_no_entry_for_catalog() {
        // Empty workspaceSuspend.catalogs → catalog falls through to
        // Inherit (not paused at the catalog tier).
        let cache = OperatorPolicyCache::new_permissive();
        let decision = run_for_catalog_with_cache(&cache, "any-catalog");
        assert!(matches!(decision, PreReconcileDecision::Proceed));
    }
}
