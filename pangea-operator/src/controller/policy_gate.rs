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
use crate::crd::{
    ControllerKind, OperatorPolicySpec, SuspensionDecision, SuspensionSource, WorkspaceState,
    WorkspaceSuspend,
};

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
    let result = evaluate_against_cache(&state.operator_policy, controller);
    // When the gate fires, also bump the per-controller Prometheus
    // counter so dashboards can show "operator paused for X duration"
    // and "controller Y skipped N reconciles" without parsing logs.
    if result.is_some() {
        let spec = state.operator_policy.read();
        // Map binary global/controller flags to canonical
        // SuspensionSource label values so dashboards can pivot a
        // single sum across global/controller/workspace/catalog skips.
        let source = if spec.global_suspend {
            SuspensionSource::Global
        } else {
            SuspensionSource::Controller
        };
        // The cluster-wide gate has no per-workspace target; use empty
        // string so Prometheus emits a stable 3-label series.
        state
            .metrics
            .policy_skipped_total
            .with_label_values(&[controller.name(), source.name(), ""])
            .inc();
    }
    result
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

// ── Per-workspace tri-state precedence resolution ──────────────────
//
// Layered ladder (most specific wins, stop at first non-Inherit):
//
//   1. workspaceSuspend.templates["<ns>/<name>"]   (templates only)
//   2. workspaceSuspend.catalogs["<owner-catalog>"] (cascade for templates)
//   3. controllerSuspend.<kind>
//   4. globalSuspend
//
// Active at layers 1–2 means "specifically configured to reconcile
// during a more-general pause." Paused at layers 1–2 means "frozen
// regardless of more-general state." Layers 3–4 are binary; they only
// produce Paused or NotSuspended.
//
// All three resolve_*_state functions are pure: no I/O, no metric
// side effects, no logging. The metrics + log side effects live in the
// gate wrappers below. This keeps the precedence matrix unit-testable
// in isolation and makes the gate functions composable.

/// Resolve the effective suspension state for a single
/// `InfrastructureTemplate`, given the current `OperatorPolicy.spec`
/// and the template's parent catalog (if any).
///
/// `parent_catalog`:
///   * `Some(name)` when the template declares an owning
///     `WorkspaceCatalog` — the cascade layer applies.
///   * `None` for orphan templates (e.g. authored before
///     WorkspaceCatalog existed) — the cascade layer is skipped.
pub fn resolve_template_state(
    spec: &OperatorPolicySpec,
    namespace: &str,
    template_name: &str,
    parent_catalog: Option<&str>,
) -> SuspensionDecision {
    // Layer 1: most-specific per-template entry.
    if let Some(entry) = spec
        .workspace_suspend
        .template_entry(namespace, template_name)
    {
        if let Some(d) = decision_from_entry(entry, SuspensionSource::Workspace) {
            return d;
        }
    }

    // Layer 2: cascade from the parent catalog, if any.
    if let Some(catalog_name) = parent_catalog {
        if let Some(entry) = spec.workspace_suspend.catalog_entry(catalog_name) {
            if let Some(d) = decision_from_entry(entry, SuspensionSource::Catalog) {
                return d;
            }
        }
    }

    // Layer 3: controller-class binary suspend.
    if spec.controller_suspend.is_set(ControllerKind::Template) {
        return SuspensionDecision::Paused {
            reason: Some(format!("controllerSuspend.{}", ControllerKind::Template.name())),
            source: SuspensionSource::Controller,
        };
    }

    // Layer 4: global binary suspend.
    if spec.global_suspend {
        return SuspensionDecision::Paused {
            reason: spec.global_suspend_reason.clone(),
            source: SuspensionSource::Global,
        };
    }

    SuspensionDecision::NotSuspended
}

/// Resolve the effective suspension state for a `WorkspaceCatalog`
/// itself (not its child templates — those use `resolve_template_state`).
/// Skips the cascade layer (no parent above catalogs).
pub fn resolve_catalog_state(
    spec: &OperatorPolicySpec,
    catalog_name: &str,
) -> SuspensionDecision {
    // Layer 1: most-specific per-catalog entry.
    if let Some(entry) = spec.workspace_suspend.catalog_entry(catalog_name) {
        if let Some(d) = decision_from_entry(entry, SuspensionSource::Workspace) {
            return d;
        }
    }

    // Layer 3: controller-class binary suspend.
    if spec
        .controller_suspend
        .is_set(ControllerKind::WorkspaceCatalog)
    {
        return SuspensionDecision::Paused {
            reason: Some(format!(
                "controllerSuspend.{}",
                ControllerKind::WorkspaceCatalog.name()
            )),
            source: SuspensionSource::Controller,
        };
    }

    // Layer 4: global binary suspend.
    if spec.global_suspend {
        return SuspensionDecision::Paused {
            reason: spec.global_suspend_reason.clone(),
            source: SuspensionSource::Global,
        };
    }

    SuspensionDecision::NotSuspended
}

/// Map a `WorkspaceSuspendEntry` to a `SuspensionDecision`. Returns
/// `None` when the entry's state is `Inherit` (caller should fall
/// through to the next layer). Helper kept private to avoid leaking
/// an under-constrained shape into the public API.
fn decision_from_entry(
    entry: &crate::crd::WorkspaceSuspendEntry,
    source: SuspensionSource,
) -> Option<SuspensionDecision> {
    match entry.state {
        WorkspaceState::Inherit => None,
        WorkspaceState::Paused => Some(SuspensionDecision::Paused {
            reason: entry.reason.clone(),
            source,
        }),
        WorkspaceState::Active => Some(SuspensionDecision::Active {
            reason: entry.reason.clone(),
            source,
        }),
    }
}

/// Per-template gate. Wraps `resolve_template_state` with the side
/// effects (metric bump + log line) and translates the pure
/// `SuspensionDecision` to a kube `Action::requeue` when paused.
///
/// Returns `Some(Action)` to short-circuit the reconcile when paused;
/// `None` to proceed (covering both `NotSuspended` and `Active`).
pub fn check_template_workspace_policy(
    state: &ControllerState,
    namespace: &str,
    template_name: &str,
    parent_catalog: Option<&str>,
) -> Option<Action> {
    let spec = state.operator_policy.read();
    let decision = resolve_template_state(&spec, namespace, template_name, parent_catalog);

    match decision {
        SuspensionDecision::NotSuspended => None,
        SuspensionDecision::Active { reason, source } => {
            // Active carve-outs do NOT pause; they exempt the
            // workspace from a more-general pause. Surface this to
            // observability so dashboards can show "what's still
            // reconciling under a global pause".
            info!(
                namespace = %namespace,
                template = %template_name,
                source = source.name(),
                reason = reason.as_deref().unwrap_or("(none)"),
                "Workspace Active override — proceeding despite more-general pause"
            );
            None
        }
        SuspensionDecision::Paused { reason, source } => {
            let key = WorkspaceSuspend::template_key(namespace, template_name);
            info!(
                workspace = %key,
                source = source.name(),
                reason = reason.as_deref().unwrap_or("(no reason given)"),
                "Skipping reconcile: workspace paused"
            );
            state.operator_policy.bump_skipped();
            state
                .metrics
                .policy_skipped_total
                .with_label_values(&[
                    ControllerKind::Template.name(),
                    source.name(),
                    &key,
                ])
                .inc();
            Some(Action::requeue(POLICY_RECHECK_INTERVAL))
        }
    }
}

/// Per-catalog gate. Same shape as `check_template_workspace_policy`
/// but for `WorkspaceCatalog` reconciles. Skips the cascade layer.
pub fn check_catalog_workspace_policy(
    state: &ControllerState,
    catalog_name: &str,
) -> Option<Action> {
    let spec = state.operator_policy.read();
    let decision = resolve_catalog_state(&spec, catalog_name);

    match decision {
        SuspensionDecision::NotSuspended => None,
        SuspensionDecision::Active { reason, source } => {
            info!(
                catalog = %catalog_name,
                source = source.name(),
                reason = reason.as_deref().unwrap_or("(none)"),
                "Catalog Active override — proceeding despite more-general pause"
            );
            None
        }
        SuspensionDecision::Paused { reason, source } => {
            info!(
                catalog = %catalog_name,
                source = source.name(),
                reason = reason.as_deref().unwrap_or("(no reason given)"),
                "Skipping reconcile: catalog paused"
            );
            state.operator_policy.bump_skipped();
            state
                .metrics
                .policy_skipped_total
                .with_label_values(&[
                    ControllerKind::WorkspaceCatalog.name(),
                    source.name(),
                    catalog_name,
                ])
                .inc();
            Some(Action::requeue(POLICY_RECHECK_INTERVAL))
        }
    }
}

/// Cache-only catalog gate for controllers whose context type does
/// not carry the metrics handle (workspace_catalog_controller uses a
/// slim `Context` with `client + operator_policy` only). Mirrors
/// `evaluate_against_cache`'s role for the global/controller layers.
///
/// Skips the metric bump (no metrics handle available). The cache
/// `bump_skipped` is still called so `OperatorPolicyStatus.reconcilesSkipped`
/// stays accurate.
pub fn evaluate_catalog_against_cache(
    cache: &OperatorPolicyCache,
    catalog_name: &str,
) -> Option<Action> {
    let spec = cache.read();
    let decision = resolve_catalog_state(&spec, catalog_name);

    match decision {
        SuspensionDecision::NotSuspended => None,
        SuspensionDecision::Active { reason, source } => {
            info!(
                catalog = %catalog_name,
                source = source.name(),
                reason = reason.as_deref().unwrap_or("(none)"),
                "Catalog Active override — proceeding despite more-general pause"
            );
            None
        }
        SuspensionDecision::Paused { reason, source } => {
            info!(
                catalog = %catalog_name,
                source = source.name(),
                reason = reason.as_deref().unwrap_or("(no reason given)"),
                "Skipping reconcile: catalog paused"
            );
            cache.bump_skipped();
            Some(Action::requeue(POLICY_RECHECK_INTERVAL))
        }
    }
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
            ..Default::default()
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
            controller_suspend: cs,
            ..Default::default()
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
            ..Default::default()
        });
        // Even though template is not in controllerSuspend, global wins.
        assert!(evaluate(&cache, ControllerKind::Template).0);
    }

    // ── Per-workspace precedence ladder ───────────────────────────
    //
    // Pure-function tests against `resolve_template_state` and
    // `resolve_catalog_state`. No cache, no metrics, no ControllerState.
    // Every cell of the catalog × template × controller × global matrix
    // documented in CLAUDE.md is exercised.

    use crate::crd::{
        SuspensionDecision, SuspensionSource, WorkspaceState, WorkspaceSuspend,
        WorkspaceSuspendEntry,
    };

    /// Helper: build a spec with optional template + catalog entries.
    /// `(catalog_state, template_state)` choose what to insert; `None`
    /// means "no entry" (which serializes as the entry being absent =
    /// `Inherit` semantics).
    fn spec_with(
        global: bool,
        controller_template: bool,
        catalog_entry: Option<WorkspaceState>,
        template_entry: Option<WorkspaceState>,
    ) -> OperatorPolicySpec {
        let mut cs = ControllerSuspend::default();
        cs.template = controller_template;

        let mut ws = WorkspaceSuspend::default();
        if let Some(state) = catalog_entry {
            ws.catalogs.insert(
                "opensource".into(),
                WorkspaceSuspendEntry {
                    state,
                    reason: Some(format!("catalog={:?}", state)),
                },
            );
        }
        if let Some(state) = template_entry {
            ws.templates.insert(
                "ns/foo".into(),
                WorkspaceSuspendEntry {
                    state,
                    reason: Some(format!("template={:?}", state)),
                },
            );
        }

        OperatorPolicySpec {
            global_suspend: global,
            global_suspend_reason: if global {
                Some("global-test".into())
            } else {
                None
            },
            controller_suspend: cs,
            workspace_suspend: ws,
        }
    }

    fn resolve(spec: &OperatorPolicySpec) -> SuspensionDecision {
        resolve_template_state(spec, "ns", "foo", Some("opensource"))
    }

    // Precedence matrix from CLAUDE.md:
    // | Catalog state | Template state | Effective | Why |
    // | Inherit       | Inherit        | NotSusp.  | nothing applies |
    // | Inherit       | Paused         | Paused    | template-specific |
    // | Inherit       | Active         | Active    | template-specific |
    // | Paused        | Inherit        | Paused    | catalog cascade |
    // | Paused        | Active         | Active    | template overrides |
    // | Paused        | Paused         | Paused    | both agree |
    // | Active        | Inherit        | Active    | catalog carve-out |
    // | Active        | Paused         | Paused    | template overrides |
    // | Active        | Active         | Active    | both agree |

    #[test]
    fn precedence_inherit_inherit_no_pause() {
        let spec = spec_with(false, false, None, None);
        assert_eq!(resolve(&spec), SuspensionDecision::NotSuspended);
    }

    #[test]
    fn precedence_template_paused_wins_over_inherit_catalog() {
        let spec = spec_with(false, false, None, Some(WorkspaceState::Paused));
        let d = resolve(&spec);
        assert!(d.is_paused());
        assert_eq!(d.source(), Some(SuspensionSource::Workspace));
    }

    #[test]
    fn precedence_template_active_during_inherit_catalog() {
        // Just template Active, nothing else applies — same as
        // NotSuspended in observable behavior, but the source is
        // tracked so the metric label and gauge stay accurate.
        let spec = spec_with(false, false, None, Some(WorkspaceState::Active));
        let d = resolve(&spec);
        assert!(d.is_active_override());
        assert_eq!(d.source(), Some(SuspensionSource::Workspace));
    }

    #[test]
    fn precedence_catalog_paused_cascades_when_template_inherits() {
        let spec = spec_with(false, false, Some(WorkspaceState::Paused), None);
        let d = resolve(&spec);
        assert!(d.is_paused());
        assert_eq!(d.source(), Some(SuspensionSource::Catalog));
    }

    #[test]
    fn precedence_catalog_paused_template_active_template_wins() {
        let spec = spec_with(
            false,
            false,
            Some(WorkspaceState::Paused),
            Some(WorkspaceState::Active),
        );
        let d = resolve(&spec);
        assert!(d.is_active_override(), "template Active overrides catalog Paused");
        assert_eq!(d.source(), Some(SuspensionSource::Workspace));
    }

    #[test]
    fn precedence_catalog_paused_template_paused_pauses() {
        let spec = spec_with(
            false,
            false,
            Some(WorkspaceState::Paused),
            Some(WorkspaceState::Paused),
        );
        let d = resolve(&spec);
        assert!(d.is_paused());
        // Template wins because it's more specific and resolution
        // stops at first non-Inherit.
        assert_eq!(d.source(), Some(SuspensionSource::Workspace));
    }

    #[test]
    fn precedence_catalog_active_cascades_carveout() {
        // The user's killer use case: "monitoring github org during a
        // global pause". Catalog Active = whole catalog runs.
        let spec = spec_with(true, false, Some(WorkspaceState::Active), None);
        let d = resolve(&spec);
        assert!(d.is_active_override());
        assert_eq!(d.source(), Some(SuspensionSource::Catalog));
    }

    #[test]
    fn precedence_catalog_active_template_paused_template_wins() {
        let spec = spec_with(
            true,
            false,
            Some(WorkspaceState::Active),
            Some(WorkspaceState::Paused),
        );
        let d = resolve(&spec);
        assert!(d.is_paused(), "template Paused overrides catalog Active");
        assert_eq!(d.source(), Some(SuspensionSource::Workspace));
    }

    #[test]
    fn precedence_catalog_active_template_active() {
        let spec = spec_with(
            true,
            false,
            Some(WorkspaceState::Active),
            Some(WorkspaceState::Active),
        );
        let d = resolve(&spec);
        assert!(d.is_active_override());
        // Most specific wins for source attribution.
        assert_eq!(d.source(), Some(SuspensionSource::Workspace));
    }

    #[test]
    fn global_pause_with_no_overrides_pauses_template() {
        // Default behavior: no per-workspace override, global=paused →
        // template paused, attributed to global layer.
        let spec = spec_with(true, false, None, None);
        let d = resolve(&spec);
        assert!(d.is_paused());
        assert_eq!(d.source(), Some(SuspensionSource::Global));
        assert_eq!(d.reason(), Some("global-test"));
    }

    #[test]
    fn global_pause_with_template_active_carves_out() {
        // The other killer use case: global=paused but ONE template
        // is configured to keep running.
        let spec = spec_with(true, false, None, Some(WorkspaceState::Active));
        let d = resolve(&spec);
        assert!(d.is_active_override());
        assert_eq!(d.source(), Some(SuspensionSource::Workspace));
    }

    #[test]
    fn global_pause_with_catalog_active_carves_out_whole_catalog() {
        let spec = spec_with(true, false, Some(WorkspaceState::Active), None);
        let d = resolve(&spec);
        assert!(d.is_active_override());
        assert_eq!(d.source(), Some(SuspensionSource::Catalog));
    }

    #[test]
    fn controller_suspend_pauses_when_no_workspace_override() {
        let spec = spec_with(false, true, None, None);
        let d = resolve(&spec);
        assert!(d.is_paused());
        assert_eq!(d.source(), Some(SuspensionSource::Controller));
    }

    #[test]
    fn controller_suspend_yields_to_template_active() {
        let spec = spec_with(false, true, None, Some(WorkspaceState::Active));
        let d = resolve(&spec);
        assert!(d.is_active_override());
        assert_eq!(d.source(), Some(SuspensionSource::Workspace));
    }

    #[test]
    fn orphan_template_skips_catalog_layer() {
        // Template with no parent_catalog: cascade layer doesn't fire
        // even when catalogs map has entries.
        let mut ws = WorkspaceSuspend::default();
        ws.catalogs.insert(
            "opensource".into(),
            WorkspaceSuspendEntry {
                state: WorkspaceState::Paused,
                reason: None,
            },
        );
        let spec = OperatorPolicySpec {
            workspace_suspend: ws,
            ..Default::default()
        };
        let d = resolve_template_state(&spec, "ns", "orphan", None);
        // Catalog Paused must not apply; falls through to NotSuspended.
        assert_eq!(d, SuspensionDecision::NotSuspended);
    }

    #[test]
    fn reason_propagates_from_template_entry() {
        let mut ws = WorkspaceSuspend::default();
        ws.templates.insert(
            "ns/foo".into(),
            WorkspaceSuspendEntry {
                state: WorkspaceState::Paused,
                reason: Some("debugging compile error".into()),
            },
        );
        let spec = OperatorPolicySpec {
            workspace_suspend: ws,
            ..Default::default()
        };
        let d = resolve_template_state(&spec, "ns", "foo", None);
        assert_eq!(d.reason(), Some("debugging compile error"));
    }

    #[test]
    fn resolve_catalog_state_skips_cascade_layer() {
        // Catalog resolution must NOT consult template-level entries.
        let mut ws = WorkspaceSuspend::default();
        ws.templates.insert(
            "ns/foo".into(),
            WorkspaceSuspendEntry {
                state: WorkspaceState::Paused,
                reason: None,
            },
        );
        let spec = OperatorPolicySpec {
            workspace_suspend: ws,
            ..Default::default()
        };
        // Template entry does NOT pause the catalog itself.
        let d = resolve_catalog_state(&spec, "opensource");
        assert_eq!(d, SuspensionDecision::NotSuspended);
    }

    #[test]
    fn resolve_catalog_state_honors_own_entry() {
        let mut ws = WorkspaceSuspend::default();
        ws.catalogs.insert(
            "opensource".into(),
            WorkspaceSuspendEntry {
                state: WorkspaceState::Active,
                reason: Some("rebuilding manifest".into()),
            },
        );
        let spec = OperatorPolicySpec {
            global_suspend: true,
            global_suspend_reason: Some("rewrite".into()),
            workspace_suspend: ws,
            ..Default::default()
        };
        let d = resolve_catalog_state(&spec, "opensource");
        assert!(d.is_active_override());
        assert_eq!(d.source(), Some(SuspensionSource::Workspace));
        assert_eq!(d.reason(), Some("rebuilding manifest"));
    }

    #[test]
    fn resolve_catalog_state_honors_workspace_catalog_controller_suspend() {
        let mut cs = ControllerSuspend::default();
        cs.workspace_catalog = true;
        let spec = OperatorPolicySpec {
            controller_suspend: cs,
            ..Default::default()
        };
        let d = resolve_catalog_state(&spec, "opensource");
        assert!(d.is_paused());
        assert_eq!(d.source(), Some(SuspensionSource::Controller));
    }

    #[test]
    fn unrelated_template_still_inherits_cascade() {
        // Catalog Active applies to *every* template under that
        // catalog, not just specific ones.
        let mut ws = WorkspaceSuspend::default();
        ws.catalogs.insert(
            "opensource".into(),
            WorkspaceSuspendEntry {
                state: WorkspaceState::Active,
                reason: None,
            },
        );
        let spec = OperatorPolicySpec {
            global_suspend: true,
            workspace_suspend: ws,
            ..Default::default()
        };
        let d_a = resolve_template_state(&spec, "ns", "tmpl-a", Some("opensource"));
        let d_b = resolve_template_state(&spec, "ns", "tmpl-b", Some("opensource"));
        assert!(d_a.is_active_override());
        assert!(d_b.is_active_override());
        assert_eq!(d_a.source(), Some(SuspensionSource::Catalog));
        assert_eq!(d_b.source(), Some(SuspensionSource::Catalog));
    }

    #[test]
    fn pause_count_under_global_pause_carveout_pattern() {
        // Realistic scenario: globalSuspend=true with one catalog
        // carved out (Active) and one template inside that catalog
        // overridden back to Paused.
        let mut ws = WorkspaceSuspend::default();
        ws.catalogs.insert(
            "opensource".into(),
            WorkspaceSuspendEntry {
                state: WorkspaceState::Active,
                reason: Some("monitoring".into()),
            },
        );
        ws.templates.insert(
            "ns/broken".into(),
            WorkspaceSuspendEntry {
                state: WorkspaceState::Paused,
                reason: Some("compile error".into()),
            },
        );
        let spec = OperatorPolicySpec {
            global_suspend: true,
            global_suspend_reason: Some("rewrite".into()),
            workspace_suspend: ws,
            ..Default::default()
        };

        // tmpl-healthy (no per-template override) inherits catalog Active.
        let d_healthy =
            resolve_template_state(&spec, "ns", "tmpl-healthy", Some("opensource"));
        assert!(d_healthy.is_active_override());

        // tmpl-broken has explicit Paused — overrides catalog Active.
        let d_broken = resolve_template_state(&spec, "ns", "broken", Some("opensource"));
        assert!(d_broken.is_paused());
        assert_eq!(d_broken.reason(), Some("compile error"));

        // Template not under any catalog inherits global pause.
        let d_orphan = resolve_template_state(&spec, "ns", "orphan", None);
        assert!(d_orphan.is_paused());
        assert_eq!(d_orphan.source(), Some(SuspensionSource::Global));
    }

    #[test]
    fn count_active_overrides_one_carveout() {
        let mut ws = WorkspaceSuspend::default();
        ws.catalogs.insert(
            "opensource".into(),
            WorkspaceSuspendEntry {
                state: WorkspaceState::Active,
                reason: None,
            },
        );
        ws.templates.insert(
            "ns/foo".into(),
            WorkspaceSuspendEntry {
                state: WorkspaceState::Paused,
                reason: None,
            },
        );
        // 1 Active entry across both maps (catalogs.opensource).
        assert_eq!(ws.count_active_overrides(), 1);
    }

    // ── R5: decision_from_entry + cache-only side effects ──

    #[test]
    fn decision_from_entry_inherit_returns_none() {
        // Inherit means "fall through to next layer" — the helper must
        // return None so the caller continues the precedence ladder.
        let entry = WorkspaceSuspendEntry {
            state: WorkspaceState::Inherit,
            reason: Some("ignored".into()),
        };
        assert!(decision_from_entry(&entry, SuspensionSource::Workspace).is_none());
    }

    #[test]
    fn decision_from_entry_paused_carries_reason_and_source() {
        let entry = WorkspaceSuspendEntry {
            state: WorkspaceState::Paused,
            reason: Some("DR drill".into()),
        };
        let d = decision_from_entry(&entry, SuspensionSource::Catalog).unwrap();
        match d {
            SuspensionDecision::Paused { reason, source } => {
                assert_eq!(reason.as_deref(), Some("DR drill"));
                assert_eq!(source, SuspensionSource::Catalog);
            }
            _ => panic!("expected Paused decision"),
        }
    }

    #[test]
    fn decision_from_entry_active_carries_reason_and_source() {
        let entry = WorkspaceSuspendEntry {
            state: WorkspaceState::Active,
            reason: Some("on-call carve-out".into()),
        };
        let d = decision_from_entry(&entry, SuspensionSource::Workspace).unwrap();
        match d {
            SuspensionDecision::Active { reason, source } => {
                assert_eq!(reason.as_deref(), Some("on-call carve-out"));
                assert_eq!(source, SuspensionSource::Workspace);
            }
            _ => panic!("expected Active decision"),
        }
    }

    #[test]
    fn evaluate_against_cache_bumps_skipped_on_global_suspend() {
        // The cache-direct entry point used by architecture_gem +
        // workspace_catalog must increment the skipped counter on
        // every pause — observability for the kill-switch needs to
        // count actual reconciles dropped, not just the policy-state
        // boolean. Drift here would silently zero the dashboard.
        let cache = OperatorPolicyCache::new_permissive();
        cache.store(OperatorPolicySpec {
            global_suspend: true,
            global_suspend_reason: Some("R5 test".into()),
            ..Default::default()
        });
        assert_eq!(cache.skipped(), 0);
        let action = evaluate_against_cache(&cache, ControllerKind::ArchitectureGem);
        assert!(action.is_some(), "expected a skip action");
        assert_eq!(cache.skipped(), 1, "skipped counter must bump");
    }

    #[test]
    fn evaluate_against_cache_does_not_bump_skipped_on_proceed() {
        // Default-allow → no skip → counter stays at 0.
        let cache = OperatorPolicyCache::new_permissive();
        assert_eq!(cache.skipped(), 0);
        let action = evaluate_against_cache(&cache, ControllerKind::WorkspaceCatalog);
        assert!(action.is_none(), "permissive cache must not skip");
        assert_eq!(cache.skipped(), 0, "permissive must not bump");
    }

    #[test]
    fn evaluate_catalog_against_cache_paused_bumps_and_returns_action() {
        // Per-catalog tri-state Paused → must bump the skipped
        // counter and return Some(Action). Mirrors the architecture-
        // gem path but for the catalog-level cascade tier.
        let cache = OperatorPolicyCache::new_permissive();
        let mut spec = OperatorPolicySpec::default();
        spec.workspace_suspend.catalogs.insert(
            "shared-infra".into(),
            WorkspaceSuspendEntry {
                state: WorkspaceState::Paused,
                reason: Some("freeze for migration".into()),
            },
        );
        cache.store(spec);
        let before = cache.skipped();
        let action = evaluate_catalog_against_cache(&cache, "shared-infra");
        assert!(action.is_some());
        assert_eq!(cache.skipped(), before + 1);
    }

    #[test]
    fn evaluate_catalog_against_cache_active_does_not_skip() {
        // Active is a carve-out — do not pause, do not bump.
        let cache = OperatorPolicyCache::new_permissive();
        let mut spec = OperatorPolicySpec::default();
        spec.workspace_suspend.catalogs.insert(
            "shared-infra".into(),
            WorkspaceSuspendEntry {
                state: WorkspaceState::Active,
                reason: None,
            },
        );
        cache.store(spec);
        let before = cache.skipped();
        let action = evaluate_catalog_against_cache(&cache, "shared-infra");
        assert!(action.is_none(), "Active is a carve-out, not a pause");
        assert_eq!(cache.skipped(), before, "Active must not bump skipped");
    }
}
