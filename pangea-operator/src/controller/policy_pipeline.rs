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

use crate::controller::{ControllerState, policy_gate};
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
    if let Some(action) =
        policy_gate::check_operator_policy(state, ControllerKind::Template)
    {
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

#[cfg(test)]
mod tests {
    use super::*;
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
}
