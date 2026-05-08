//! Controller for `OperatorPolicy/default`.
//!
//! Two responsibilities:
//!
//!   1. **Cache propagation** — keep `state.operator_policy` in sync
//!      with the cluster's current `OperatorPolicy/default` spec, so
//!      every other controller's `policy_gate` read sees fresh data.
//!
//!   2. **Status mirror** — copy spec → `status.effective`, set
//!      `status.lastChangedAt`, and surface `status.reconcilesSkipped`
//!      from the in-memory counter.
//!
//! Singleton enforcement: only the resource named `default` is
//! honored. Any other name is logged at WARN and otherwise ignored —
//! the controller does not modify or fight non-default resources.
//!
//! Self-pause exception: this controller intentionally does NOT
//! consult `policy_gate`. If it did, `globalSuspend=true` would freeze
//! the very controller that surfaces the pause status — pause would
//! be invisible. The kill switch is one-way reversible by intent.

use crate::controller::{operator_policy_cache::OperatorPolicyCache, ControllerState};
use crate::crd::{
    OperatorPolicy, OperatorPolicySpec, OperatorPolicyStatus, OPERATOR_POLICY_SINGLETON,
};
use crate::error::{Error, Result};

use futures::StreamExt;
use kube::{
    api::{Api, Patch, PatchParams},
    runtime::controller::Action,
    ResourceExt,
};
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, error, info, instrument, warn};

const REQUEUE_INTERVAL: Duration = Duration::from_secs(60);
const ERROR_REQUEUE_INTERVAL: Duration = Duration::from_secs(15);

/// Top-level controller for `OperatorPolicy`.
pub struct OperatorPolicyController {
    state: ControllerState,
}

impl OperatorPolicyController {
    pub fn new(state: ControllerState) -> Self {
        Self { state }
    }

    /// Drive reconciliation. Call once at operator startup; runs until
    /// the controller stream terminates (operator shutdown).
    pub async fn run(self) -> Result<()> {
        let client = self.state.client.clone();
        let state = Arc::new(self.state);

        info!("Starting OperatorPolicy controller");

        crate::controller::generation_filter::filtered_controller::<OperatorPolicy>(client)
            .run(
                move |policy, ctx| {
                    let state = Arc::clone(&ctx);
                    async move { reconcile(policy, state).await }
                },
                error_policy,
                state,
            )
            .for_each(|result| async move {
                match result {
                    Ok((obj, action)) => {
                        debug!(name = %obj.name, ?action, "OperatorPolicy reconcile completed");
                    }
                    Err(e) => {
                        warn!(error = %e, "OperatorPolicy reconcile error");
                    }
                }
            })
            .await;

        Ok(())
    }
}

#[instrument(skip(state), fields(name = %policy.name_any()))]
async fn reconcile(
    policy: Arc<OperatorPolicy>,
    state: Arc<ControllerState>,
) -> std::result::Result<Action, Error> {
    let name = policy.name_any();

    // Singleton enforcement. Non-default OperatorPolicies are no-ops.
    if !OperatorPolicyCache::is_singleton_name(&name) {
        warn!(
            name = %name,
            singleton = OPERATOR_POLICY_SINGLETON,
            "Ignoring OperatorPolicy: only the singleton named '{}' is honored",
            OPERATOR_POLICY_SINGLETON,
        );
        return Ok(Action::requeue(REQUEUE_INTERVAL));
    }

    info!("Reconciling OperatorPolicy/default");

    // Layer 1: propagate spec into the in-memory cache so every
    // controller's `policy_gate` sees the new value on its next read.
    // This is cheap (atomic-pointer swap) and ALWAYS runs — the cache
    // must stay live regardless of whether we end up writing status.
    state.operator_policy.store(policy.spec.clone());

    // Update the workspace_active_override gauge from the spec.
    // Single-writer pattern: this controller owns the gauge, every
    // policy reconcile rebuilds the snapshot. No per-template or
    // per-catalog tracking required elsewhere. Cheap, always runs.
    publish_active_override_gauges(&state, &policy.spec);

    if policy.spec.global_suspend {
        let reason = policy
            .spec
            .global_suspend_reason
            .as_deref()
            .unwrap_or("(no reason given)");
        info!(reason = %reason, "Global suspend ACTIVE — every controller is paused");
    }

    // Layer 2: surface the resolved view + counter into status — but
    // diff-gate the PATCH to avoid the self-trigger watch loop.
    //
    // Why this gate matters: every status PATCH bumps
    // `metadata.resourceVersion`, the OperatorPolicy watch fires,
    // this controller re-runs reconcile, which previously ALWAYS
    // re-PATCHed status (because `last_changed_at = Utc::now()` and
    // `reconciles_skipped` from the live atomic counter both differed
    // on every read). That closed loop ran at ~76 reconciles/sec
    // observed on rio, persisting even with `globalSuspend = true`
    // because this controller intentionally does NOT consult
    // `policy_gate` (see header comment).
    //
    // Resolution: only PATCH when something *substantive* changed —
    // observedGeneration (spec was mutated), effective spec, or
    // the workspace_overrides count. The `reconciles_skipped`
    // counter is observable via the `pangea_policy_skipped_total`
    // Prometheus counter, so freezing its echo into `.status` to
    // the last-substantive-change snapshot is acceptable; live
    // counts live in metrics.
    let observed_gen = policy.metadata.generation.unwrap_or(0);
    let new_effective = Some(policy.spec.clone());
    let new_workspace_overrides = policy.spec.workspace_suspend.count_active_overrides();

    let needs_patch = status_needs_patch(
        policy.status.as_ref(),
        observed_gen,
        new_effective.as_ref(),
        new_workspace_overrides,
    );

    if !needs_patch {
        debug!(
            observed_gen,
            "OperatorPolicy effective state unchanged; skipping status patch \
             (avoids self-trigger watch loop). Live skip counts in pangea_policy_skipped_total."
        );
        return Ok(Action::requeue(REQUEUE_INTERVAL));
    }

    let status = OperatorPolicyStatus {
        observed_generation: observed_gen,
        last_changed_at: Some(chrono::Utc::now()),
        effective: new_effective,
        reconciles_skipped: state.operator_policy.skipped(),
        workspace_overrides: new_workspace_overrides,
    };

    let api: Api<OperatorPolicy> = Api::all(state.client.clone());
    let patch = serde_json::json!({
        "status": status,
    });
    let pp = PatchParams::apply("pangea-operator");

    // Use Merge (RFC 7396): no apiVersion/kind required, no field
    // ownership conflicts with kubectl-driven user patches against
    // spec. Status is exclusively operator-owned so merge is fine.
    if let Err(e) = api.patch_status(&name, &pp, &Patch::Merge(&patch)).await {
        error!(error = %e, "Failed to patch OperatorPolicy status");
        return Ok(Action::requeue(ERROR_REQUEUE_INTERVAL));
    }

    Ok(Action::requeue(REQUEUE_INTERVAL))
}

/// Diff-gate for the OperatorPolicy status PATCH.
///
/// Returns `true` only when at least one observable status field
/// would actually change. Without this gate the controller's own
/// status writes refire its watch and create a closed-loop
/// reconciliation at apiserver-event speed (~76/sec on rio,
/// 2026-05-07 firefighting). Specifically, the previous
/// implementation always set `last_changed_at = Utc::now()` and
/// echoed the live `reconciles_skipped` atomic — both differed on
/// every PATCH, so every reconcile bumped resourceVersion and
/// scheduled the next reconcile.
///
/// We intentionally exclude `reconciles_skipped` from this check —
/// its live count is observable via `pangea_policy_skipped_total`
/// (Prometheus). Echoing it into status drove the loop; freezing
/// the echo to last-substantive-change snapshots is acceptable.
fn status_needs_patch(
    prev: Option<&OperatorPolicyStatus>,
    observed_gen: i64,
    new_effective: Option<&OperatorPolicySpec>,
    new_workspace_overrides: u64,
) -> bool {
    let prev_observed_gen = prev.map(|s| s.observed_generation).unwrap_or(0);
    let prev_effective = prev.and_then(|s| s.effective.as_ref());
    let prev_workspace_overrides = prev.map(|s| s.workspace_overrides).unwrap_or(0);

    observed_gen != prev_observed_gen
        || prev_effective != new_effective
        || prev_workspace_overrides != new_workspace_overrides
}

fn error_policy(
    _obj: Arc<OperatorPolicy>,
    error: &Error,
    _ctx: Arc<ControllerState>,
) -> Action {
    error!(%error, "OperatorPolicy reconcile error");
    Action::requeue(ERROR_REQUEUE_INTERVAL)
}

/// Publish per-workspace `Active` carve-out gauges from the resolved
/// spec. Reset-on-each-reconcile semantics: drop every previous
/// label-set value to 0, then set the active ones to 1. Cheap because
/// the spec map is the source of truth and Prometheus retains zero
/// samples through the next scrape (gauge reset is idempotent).
///
/// Note: the cascade behavior (a template inheriting Active from its
/// parent catalog) is NOT reflected here — only direct entries with
/// `state == Active` get a gauge sample. The catalog itself emits
/// the carve-out signal; child templates running during a global
/// pause are inferred from `policy_skipped_total` staying flat for
/// templates under that catalog's selector. Keeps cardinality
/// bounded and the source-of-truth single.
fn publish_active_override_gauges(state: &ControllerState, spec: &OperatorPolicySpec) {
    use crate::crd::WorkspaceState;

    // Reset: clear every label-set we might have previously written.
    // `with_label_values` does NOT remove a series, so we explicitly
    // set existing template/catalog entries to 0 first, then set
    // the Actives to 1. Prometheus scrape sees the consistent view.
    state.metrics.workspace_active_override.reset();

    for (key, entry) in &spec.workspace_suspend.templates {
        if entry.state != WorkspaceState::Active {
            continue;
        }
        // key is "<namespace>/<name>" — split for the gauge labels so
        // dashboards can group by namespace.
        let (namespace, name) = key.split_once('/').unwrap_or(("", key.as_str()));
        state
            .metrics
            .workspace_active_override
            .with_label_values(&["template", namespace, name, "workspace"])
            .set(1);
    }

    for (name, entry) in &spec.workspace_suspend.catalogs {
        if entry.state != WorkspaceState::Active {
            continue;
        }
        // Catalogs are cluster-scoped (no namespace).
        state
            .metrics
            .workspace_active_override
            .with_label_values(&["catalog", "", name, "workspace"])
            .set(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crd::{ControllerSuspend, OperatorPolicySpec};

    #[test]
    fn singleton_name_match() {
        assert!(OperatorPolicyCache::is_singleton_name("default"));
        assert!(!OperatorPolicyCache::is_singleton_name("other"));
    }

    #[test]
    fn cache_store_round_trip_via_spec() {
        // Simulates what the reconciler does: take a spec from a CR
        // and store it; subsequent reads reflect it.
        let cache = OperatorPolicyCache::new_permissive();
        let mut cs = ControllerSuspend::default();
        cs.dashboard = true;
        let spec = OperatorPolicySpec {
            controller_suspend: cs,
            ..Default::default()
        };
        cache.store(spec.clone());
        let read = cache.read();
        assert_eq!(*read, spec);
    }

    #[test]
    fn status_struct_serializes_with_camelcase() {
        let status = OperatorPolicyStatus {
            observed_generation: 5,
            reconciles_skipped: 42,
            ..Default::default()
        };
        let json = serde_json::to_value(&status).unwrap();
        assert!(json.get("observedGeneration").is_some());
        assert!(json.get("reconcilesSkipped").is_some());
        assert_eq!(json.get("reconcilesSkipped").unwrap(), 42);
    }

    // ── Diff-gate (status_needs_patch) tests ─────────────────────
    //
    // Lock in the gate that broke the self-trigger watch loop
    // observed on rio (~76 reconciles/sec, 2026-05-07). Each test
    // pins one branch of the substantive-change predicate.

    fn spec_global_suspend(b: bool) -> OperatorPolicySpec {
        OperatorPolicySpec {
            global_suspend: b,
            ..Default::default()
        }
    }

    #[test]
    fn patch_required_when_status_is_absent_so_first_reconcile_initializes_it() {
        // Brand-new resource: status is None. First reconcile must
        // populate status.effective so observers can see what the
        // operator considers the resolved spec. The gate honors
        // this by reporting `None != Some(&spec)` for the
        // prev_effective check.
        let prev: Option<&OperatorPolicyStatus> = None;
        let spec = OperatorPolicySpec::default();
        assert!(
            status_needs_patch(prev, 0, Some(&spec), 0),
            "first reconcile after creation must initialize status"
        );
    }

    #[test]
    fn patch_required_when_observed_generation_advances() {
        let prev = OperatorPolicyStatus {
            observed_generation: 1,
            effective: Some(spec_global_suspend(true)),
            workspace_overrides: 0,
            ..Default::default()
        };
        // Spec mutated → metadata.generation bumped to 2.
        let spec = spec_global_suspend(true);
        assert!(
            status_needs_patch(Some(&prev), 2, Some(&spec), 0),
            "generation bump must always force a status PATCH"
        );
    }

    #[test]
    fn patch_required_when_effective_spec_differs() {
        // Spec mutated to flip globalSuspend; generation bumped too
        // in real life, but verify the effective-diff branch in
        // isolation (gen unchanged here).
        let prev = OperatorPolicyStatus {
            observed_generation: 3,
            effective: Some(spec_global_suspend(false)),
            workspace_overrides: 0,
            ..Default::default()
        };
        let spec = spec_global_suspend(true);
        assert!(
            status_needs_patch(Some(&prev), 3, Some(&spec), 0),
            "effective-spec diff alone must force a status PATCH"
        );
    }

    #[test]
    fn patch_required_when_workspace_overrides_count_changes() {
        let prev = OperatorPolicyStatus {
            observed_generation: 5,
            effective: Some(spec_global_suspend(true)),
            workspace_overrides: 0,
            ..Default::default()
        };
        let spec = spec_global_suspend(true);
        assert!(
            status_needs_patch(Some(&prev), 5, Some(&spec), 1),
            "workspace_overrides count change (e.g. operator added a carve-out) must force a PATCH"
        );
    }

    #[test]
    fn skip_patch_when_substantive_state_matches() {
        // The case that drove the rio hot loop: nothing of substance
        // has changed, only the live `reconciles_skipped` counter
        // and `last_changed_at` would advance. Gate must return
        // false to break the closed loop.
        let prev = OperatorPolicyStatus {
            observed_generation: 7,
            effective: Some(spec_global_suspend(true)),
            workspace_overrides: 1,
            reconciles_skipped: 1234, // live counter — should NOT trigger patch
            last_changed_at: Some(chrono::Utc::now()),
        };
        let spec = spec_global_suspend(true);
        assert!(
            !status_needs_patch(Some(&prev), 7, Some(&spec), 1),
            "must NOT patch on counter-only / timestamp-only churn — that's the loop bug"
        );
    }
}
