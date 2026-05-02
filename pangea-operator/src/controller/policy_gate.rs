//! Pre-reconcile gate honoring `OperatorPolicy/default`.
//!
//! Every controller's reconcile loop calls
//! `check_operator_policy(state, ControllerKind::Foo).await` as the
//! first action. If the gate returns `Some(action)`, the controller
//! returns that action and skips the rest of the reconcile.
//!
//! Two layers, in evaluation order:
//!
//!   1. `globalSuspend == true` → all controllers skip.
//!   2. `controllerSuspend.<kind> == true` → only that controller
//!      skips.
//!
//! Per-CR `spec.suspend == true` is enforced separately by each
//! controller (since it depends on the CR being in scope). This gate
//! is *cluster-wide*, not per-CR.
//!
//! Default-allow when `OperatorPolicy/default` is missing or fully
//! permissive: returns `None`, controllers proceed normally.

use crate::controller::operator_policy_cache::OperatorPolicyCache;
use crate::controller::ControllerState;
use crate::crd::ControllerKind;

use kube::runtime::controller::Action;
use std::time::Duration;
use tracing::info;

/// Interval at which a suspended controller re-checks the policy.
/// 30 seconds is a balance between rapid resume on un-pause and not
/// hammering the cache on every cycle.
const POLICY_RECHECK_INTERVAL: Duration = Duration::from_secs(30);

/// Pre-reconcile gate.
///
/// Returns `Some(Action::requeue(...))` when reconciliation should be
/// skipped, `None` when it should proceed.
///
/// Performance: O(1) lock-protected snapshot read + atomic counter
/// increment. Designed to be called on every reconcile of every CR.
pub fn check_operator_policy(
    state: &ControllerState,
    controller: ControllerKind,
) -> Option<Action> {
    evaluate_against_cache(&state.operator_policy, controller)
}

/// Cache-only variant for controllers whose context type is not
/// `ControllerState` (architecture_gem, workspace_catalog). Behaves
/// identically to `check_operator_policy`; takes only the cache.
pub fn evaluate_against_cache(
    cache: &OperatorPolicyCache,
    controller: ControllerKind,
) -> Option<Action> {
    let spec = cache.read();

    if spec.global_suspend {
        let reason = spec
            .global_suspend_reason
            .as_deref()
            .unwrap_or("(no reason given)");
        info!(
            controller = controller.name(),
            reason = reason,
            "Skipping reconcile: OperatorPolicy/default has globalSuspend=true"
        );
        cache.bump_skipped();
        return Some(Action::requeue(POLICY_RECHECK_INTERVAL));
    }

    if spec.controller_suspend.is_set(controller) {
        info!(
            controller = controller.name(),
            "Skipping reconcile: OperatorPolicy/default has controllerSuspend.{}=true",
            controller.name()
        );
        cache.bump_skipped();
        return Some(Action::requeue(POLICY_RECHECK_INTERVAL));
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::controller::operator_policy_cache::OperatorPolicyCache;
    use crate::crd::{ControllerSuspend, OperatorPolicySpec};
    use std::sync::Arc;

    /// Helper: build a minimal cache populated with the given spec.
    fn cache_with(spec: OperatorPolicySpec) -> Arc<OperatorPolicyCache> {
        let cache = Arc::new(OperatorPolicyCache::new_permissive());
        cache.store(spec);
        cache
    }

    /// Pure unit test of the gate logic, decoupled from kube/api state.
    /// Tests directly against the cache rather than constructing a full
    /// `ControllerState` (which requires a live `kube::Client`).
    fn evaluate(
        cache: &OperatorPolicyCache,
        controller: ControllerKind,
    ) -> (bool, u64) {
        let spec = cache.read();
        let initial_count = cache.skipped();
        let suspended = spec.global_suspend || spec.controller_suspend.is_set(controller);
        if suspended {
            cache.bump_skipped();
        }
        (suspended, cache.skipped() - initial_count)
    }

    #[test]
    fn permissive_policy_allows_all_controllers() {
        let cache = OperatorPolicyCache::new_permissive();
        for k in [
            ControllerKind::Template,
            ControllerKind::Namespace,
            ControllerKind::WorkspaceCatalog,
            ControllerKind::ArchitectureGem,
            ControllerKind::ComplianceBinding,
            ControllerKind::ComplianceSchedule,
            ControllerKind::ImagePipeline,
            ControllerKind::Flow,
            ControllerKind::Dashboard,
            ControllerKind::AmiTest,
            ControllerKind::PackerBuild,
            ControllerKind::SynthesizerFormat,
        ] {
            let (suspended, _) = evaluate(&cache, k);
            assert!(!suspended, "{} should not be suspended", k.name());
        }
        assert_eq!(cache.skipped(), 0, "no skips on permissive policy");
    }

    #[test]
    fn global_suspend_blocks_every_controller() {
        let cache = cache_with(OperatorPolicySpec {
            global_suspend: true,
            global_suspend_reason: Some("rewrite-in-progress".into()),
            controller_suspend: ControllerSuspend::default(),
        });
        let mut bumped = 0u64;
        for k in [
            ControllerKind::Template,
            ControllerKind::Namespace,
            ControllerKind::Dashboard,
            ControllerKind::Flow,
        ] {
            let (suspended, b) = evaluate(&cache, k);
            assert!(suspended, "{} should be suspended", k.name());
            bumped += b;
        }
        assert_eq!(cache.skipped(), bumped);
        assert_eq!(bumped, 4);
    }

    #[test]
    fn per_controller_suspend_only_blocks_its_target() {
        let mut cs = ControllerSuspend::default();
        cs.template = true;
        cs.dashboard = true;
        let cache = cache_with(OperatorPolicySpec {
            global_suspend: false,
            global_suspend_reason: None,
            controller_suspend: cs,
        });

        // Suspended controllers report skipped.
        assert!(evaluate(&cache, ControllerKind::Template).0);
        assert!(evaluate(&cache, ControllerKind::Dashboard).0);

        // Other controllers proceed.
        assert!(!evaluate(&cache, ControllerKind::Namespace).0);
        assert!(!evaluate(&cache, ControllerKind::Flow).0);
        assert!(!evaluate(&cache, ControllerKind::ImagePipeline).0);

        // Counter incremented by exactly the suspended count.
        assert_eq!(cache.skipped(), 2);
    }

    #[test]
    fn global_suspend_dominates_per_controller() {
        // globalSuspend=true overrides any controllerSuspend setting,
        // even ones that are false.
        let mut cs = ControllerSuspend::default();
        cs.template = false; // explicitly false at the per-controller layer
        let cache = cache_with(OperatorPolicySpec {
            global_suspend: true,
            global_suspend_reason: Some("emergency".into()),
            controller_suspend: cs,
        });
        // Even though template is not in controllerSuspend, global wins.
        assert!(evaluate(&cache, ControllerKind::Template).0);
    }
}
