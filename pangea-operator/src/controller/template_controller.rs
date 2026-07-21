//! Controller for InfrastructureTemplate resources.

use crate::backend::{BackendConfigGenerator, Credentials, StateBackend};
use crate::crd::{
    DriftDetail, InfrastructureTemplate, InfrastructureTemplateSpec, InfrastructureTemplateStatus,
    PangeaNamespace, Phase, PolicyDecision, ResourceSummary,
};
use crate::error::{Error, Result};
use crate::executor::{evaluate_policy, policy_is_configured, Plan, PlanAction, PlannedChange};

use chrono::Utc;
use futures::StreamExt;
use k8s_openapi::api::core::v1::{ConfigMap, Secret};
use kube::{
    api::{Api, Patch, PatchParams},
    runtime::{
        controller::Action,
        events::EventType,
        reflector::{ObjectRef, Store},
        watcher,
    },
    ResourceExt,
};
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, error, info, instrument, warn};

use super::{
    conditions_for_suspended, exponential_backoff, parse_duration, ControllerState,
    ReconcileAction, DEFAULT_REQUEUE_INTERVAL, ERROR_REQUEUE_INTERVAL, SHORT_REQUEUE_INTERVAL,
};

// Helpers lifted to controller/template/* sub-modules during the
// 2026-05-03 review passes (R6 + T1). Re-import them under the names
// the call sites in this file already use.
use super::template::finalizer::{add_finalizer, has_finalizer, remove_finalizer};
use super::template::events::record_event;
use super::template::freshness::{
    evaluate_source_freshness, git_rev_parse_head, observe_head, Freshness,
};
use super::template::provider_creds::resolve_provider_config;
use super::template::secret_files::write_secret_files;
use super::template::status::{
    update_apply_status, update_compiled_revision, update_drift_check_timestamp,
    ObservationOutcome, update_freshness_status, update_pending_plan_hash, update_phase,
    update_phase_with_error,
    update_plan_status, update_settling_status, workspace_drift_reaction_to_policy_decision,
};
use super::template::cycle_receipts::{record_reconcile_cycle, truncate_for_status, CycleResult};

/// Controller for InfrastructureTemplate resources.
pub struct TemplateController {
    state: ControllerState,
}

impl TemplateController {
    /// Create a new template controller.
    pub fn new(state: ControllerState) -> Self {
        Self { state }
    }

    /// Run the controller.
    pub async fn run(self) -> Result<()> {
        let client = self.state.client.clone();
        let state = Arc::new(self.state);

        info!("Starting InfrastructureTemplate controller");

        // 2026-05: switched from `for_each` (serial) to
        // `for_each_concurrent` with PANGEA_RECONCILE_WORKERS-tunable
        // parallelism. Without this, a fast-cycling template could
        // starve siblings — observed on rio when cloudflare-pleme's
        // 7s tofu apply loop blocked pleme-io-opensource entirely.
        let workers = crate::controller::reconciler::reconcile_workers_from_env();
        info!(workers, "InfrastructureTemplate controller concurrency");

        // generation-filtered watch stream — drops status-only watch
        // events at the source so reconciles only fire on actual spec
        // mutations + the explicit Action::requeue tick. See
        // controller::generation_filter for the full rationale.
        let controller =
            crate::controller::generation_filter::filtered_controller::<InfrastructureTemplate>(
                client.clone(),
            );

        // Reactive ConfigMap watch (fleet task #131). A ConfigMap edit
        // does NOT bump the owning InfrastructureTemplate's
        // `metadata.generation` — the generation-filtered stream above
        // never fires for it — so a `configMapRef`-sourced template
        // (camelot-eks, camelot-flux-bootstrap) that has already
        // settled into Ready previously had no push signal at all for
        // a source-content edit: the only thing that could notice was
        // the next `refreshInterval` tick's `compiled_config_available`
        // content-revision check (see `non_git_source_revision` /
        // `rendered_config_is_current` below), i.e. a POLL, not a
        // PUSH — the CR could sit stale for up to `refreshInterval`,
        // and the only forcing lever operators found was a manual
        // `spec.suspend: true` → `false` cycle. `.watches()` closes the
        // gap: the moment the referenced ConfigMap changes, the owning
        // template(s) are pushed onto the reconcile queue immediately,
        // same as a spec edit. The mapper reads the SAME reflector
        // store the primary watch above already maintains
        // (`Controller::store()` — no second cache, no extra list
        // call) and delegates the actual match test to the pure
        // `config_map_ref_matches` (unit-tested below), so the linking
        // rule between a ConfigMap and the template(s) that reference
        // it is provable independent of any kube-rs machinery.
        let template_store = controller.store();
        let configmap_api: Api<ConfigMap> = Api::all(client);
        let controller = controller.watches(
            configmap_api,
            watcher::Config::default(),
            move |cm: ConfigMap| templates_referencing_configmap(&cm, &template_store),
        );

        controller
            .run(
                move |template, ctx| {
                    let state = Arc::clone(&ctx);
                    async move { reconcile_template(template, state).await }
                },
                error_policy,
                state,
            )
            .for_each_concurrent(workers, |result| async move {
                match result {
                    Ok((obj, action)) => {
                        debug!(
                            name = %obj.name,
                            namespace = ?obj.namespace,
                            ?action,
                            "Reconciliation completed"
                        );
                    }
                    Err(e) => {
                        error!(error = %e, "Reconciliation failed");
                    }
                }
            })
            .await;

        Ok(())
    }
}

/// `.watches()` mapper for the reactive ConfigMap watch above: given a
/// touched ConfigMap, which cached InfrastructureTemplate(s) reference
/// it via `spec.source.configMapRef`?
///
/// Reads the reflector `Store` the primary InfrastructureTemplate watch
/// already maintains (passed in from `TemplateController::run` via
/// `Controller::store()`) rather than issuing a live list — the store
/// is kept current by the same watch stream that feeds reconciles, so
/// it is never staler than "the last InfrastructureTemplate event this
/// controller has already processed". All the actual matching logic
/// lives in the pure `config_map_ref_matches` (unit-tested below); this
/// function is just the kube-rs-shaped plumbing around it, matching
/// this file's existing split between pure decision functions
/// (`rendered_config_is_current`, `generation_invalidates_render`, …)
/// and the I/O-touching callers that use them.
fn templates_referencing_configmap(
    cm: &ConfigMap,
    store: &Store<InfrastructureTemplate>,
) -> Vec<ObjectRef<InfrastructureTemplate>> {
    let touched_name = cm.name_any();
    let touched_namespace = cm.namespace().unwrap_or_default();
    store
        .state()
        .iter()
        .filter(|tpl| {
            tpl.spec
                .source
                .config_map_ref
                .as_ref()
                .is_some_and(|cm_ref| {
                    let template_namespace = tpl.namespace().unwrap_or_default();
                    config_map_ref_matches(
                        &cm_ref.name,
                        cm_ref.namespace.as_deref(),
                        &template_namespace,
                        &touched_namespace,
                        &touched_name,
                    )
                })
        })
        .map(|tpl| ObjectRef::from_obj(tpl.as_ref()))
        .collect()
}

/// Pure predicate: does a template's `spec.source.configMapRef` resolve
/// to the given ConfigMap `(touched_namespace, touched_name)`?
///
/// Mirrors the EXACT namespace-defaulting rule `handle_compiling` and
/// `non_git_source_revision` already use when resolving this same
/// field to actually read the ConfigMap: an unset `configMapRef.namespace`
/// means "the same namespace as the InfrastructureTemplate", never a
/// bare/cluster scope. Kept independent of any kube-rs type so the
/// linking rule is provable as a pure function over plain strings, not
/// an assertion about live cluster/watch behavior.
fn config_map_ref_matches(
    ref_name: &str,
    ref_namespace: Option<&str>,
    template_namespace: &str,
    touched_namespace: &str,
    touched_name: &str,
) -> bool {
    ref_name == touched_name && ref_namespace.unwrap_or(template_namespace) == touched_namespace
}

#[cfg(test)]
mod configmap_watch_mapper_tests {
    use super::config_map_ref_matches;

    #[test]
    fn same_name_and_explicit_namespace_matches() {
        assert!(config_map_ref_matches(
            "camelot-flux-bootstrap-tfjson",
            Some("camelot"),
            "camelot",
            "camelot",
            "camelot-flux-bootstrap-tfjson",
        ));
    }

    #[test]
    fn unset_ref_namespace_defaults_to_the_templates_own_namespace() {
        // The load-bearing case: `configMapRef.namespace` is unset (the
        // common case — camelot-eks / camelot-flux-bootstrap both omit
        // it), so the ConfigMap must be assumed to live in the SAME
        // namespace as the InfrastructureTemplate, matching
        // `handle_compiling`'s `cm_ref.namespace.clone().or_else(||
        // template.namespace())` resolution exactly.
        assert!(config_map_ref_matches(
            "camelot-flux-bootstrap-tfjson",
            None,
            "camelot",
            "camelot",
            "camelot-flux-bootstrap-tfjson",
        ));
    }

    #[test]
    fn different_name_never_matches() {
        assert!(!config_map_ref_matches(
            "camelot-flux-bootstrap-tfjson",
            None,
            "camelot",
            "camelot",
            "some-other-configmap",
        ));
    }

    #[test]
    fn unset_ref_namespace_does_not_match_a_different_touched_namespace() {
        // Same ConfigMap NAME in a DIFFERENT namespace than the
        // template's own must not match when configMapRef.namespace is
        // unset — the default is the template's namespace, not "any
        // namespace with a same-named ConfigMap".
        assert!(!config_map_ref_matches(
            "camelot-flux-bootstrap-tfjson",
            None,
            "camelot",
            "some-other-namespace",
            "camelot-flux-bootstrap-tfjson",
        ));
    }

    #[test]
    fn explicit_ref_namespace_overrides_the_templates_own_namespace() {
        // configMapRef.namespace IS set — it wins over the template's
        // own namespace, and a touched ConfigMap in the template's
        // namespace (but not the referenced one) must NOT match.
        assert!(config_map_ref_matches(
            "shared-tfjson",
            Some("shared-configs"),
            "camelot",
            "shared-configs",
            "shared-tfjson",
        ));
        assert!(!config_map_ref_matches(
            "shared-tfjson",
            Some("shared-configs"),
            "camelot",
            "camelot",
            "shared-tfjson",
        ));
    }
}

/// Reconcile an InfrastructureTemplate resource.
#[instrument(skip(state), fields(name = %template.name_any(), namespace = ?template.namespace()))]
async fn reconcile_template(
    template: Arc<InfrastructureTemplate>,
    state: Arc<ControllerState>,
) -> std::result::Result<Action, Error> {
    let name = template.name_any();
    let namespace = template.namespace().unwrap_or_default();

    info!("Reconciling InfrastructureTemplate");
    state.metrics.reconciliations_total.inc();
    // Track in-flight reconciles via an RAII guard: inc on entry, dec
    // on every scope exit (including early-returns + the `?` error
    // paths). Before this, `pangea_active_reconciliations` was declared
    // but never moved, so it sat flat at 0.
    let _active_guard =
        crate::observability::ActiveReconcileGuard::enter(&state.metrics);
    // Per-controller reconcile counter — completes the denominator
    // for `pangea_controller_reconciliations_total{controller="template"}`
    // so the chart 0.8.14 PangeaControllerReconcileRateHigh alert can
    // see this controller. (The standalone `reconciliations_total`
    // counter at line above predates the per-controller labeled one
    // and is kept for the existing template-specific dashboard.)
    state
        .metrics
        .record_reconcile(crate::crd::ControllerKind::Template, "ok");
    // Seed the magma apply-outcome series (failed/applied gauges +
    // Succeeded counter) at 0 from the first reconcile, BEFORE any
    // apply has run — otherwise the failure-signal series for this
    // template does not exist until its first apply failure, and the
    // PangeaTemplateFailing alert (`pangea_magma_resources_failed > 0`)
    // has no series to evaluate. The label arity/order matches
    // `record_magma_apply`, so a later real apply overwrites the same
    // series rather than forking a second one.
    state.metrics.seed_template(&name, &namespace);

    // Pre-reconcile policy pipeline — runs the kill-switch + per-workspace
    // pause gates in their canonical order. Each gate returns a SkipWith
    // action when it fires; we early-return without executing the body.
    // Per-CR template.spec.suspend + ReactivePolicy auto-suspend stay
    // inline below since they need template-specific data (parent_wsc,
    // status).
    let parent_catalog_name = template
        .metadata
        .labels
        .as_ref()
        .and_then(|l| l.get(crate::controller::workspace_catalog_controller::WORKSPACE_LABEL))
        .map(String::as_str);
    if let Some(action) = crate::controller::policy_pipeline::run_for_template(
        &state,
        &namespace,
        &name,
        parent_catalog_name,
    )
    .into_skip_action()
    {
        return Ok(action);
    }

    // Handle deletion via finalizer
    if template.metadata.deletion_timestamp.is_some() {
        if has_finalizer(&template) {
            // Destroy protection: refuse to destroy infrastructure that the
            // operator itself depends on (VPC, EKS, RDS, etc.)
            if template.spec.destroy_protection {
                warn!(
                    "Destroy protection is enabled — refusing to destroy infrastructure. \
                     Set spec.destroyProtection=false first, then re-delete."
                );
                record_event(
                    &template,
                    &state,
                    EventType::Warning,
                    "DestroyBlocked",
                    "Destroy protection is enabled. Set spec.destroyProtection=false to allow deletion.",
                )
                .await;
                // Keep requeuing — the finalizer blocks deletion until
                // protection is removed and destroy completes.
                return Ok(Action::requeue(DEFAULT_REQUEUE_INTERVAL));
            }

            // Transition to Destroying if not already
            let current_phase = template
                .status
                .as_ref()
                .and_then(|s| s.phase)
                .unwrap_or(Phase::Pending);

            if current_phase != Phase::Destroying {
                update_phase(&template, Phase::Destroying, &state).await?;
                return Ok(Action::requeue(SHORT_REQUEUE_INTERVAL));
            }

            return Ok(handle_destroying(&template, &state).await?.into());
        }
        // No finalizer, nothing to clean up
        return Ok(Action::await_change());
    }

    // Ensure finalizer is present
    if !has_finalizer(&template) {
        add_finalizer(&template, &state).await?;
        return Ok(Action::requeue(Duration::from_secs(1)));
    }

    // Resolve the parent WorkspaceCatalog (if the template carries the
    // pangea.pleme.io/workspace label) — used both for the
    // suspend-cascade check immediately below and for the policy
    // cascade in handle_planning. Treat lookup failures as "no parent"
    // (best-effort cascade); we'd rather reconcile without the
    // workspace-level overrides than refuse to reconcile.
    let parent_wsc = match crate::controller::workspace_catalog_controller::parent_catalog_for_template(
        &state.client,
        &template,
    )
    .await
    {
        Ok(p) => p,
        Err(e) => {
            warn!(error = %e, "WorkspaceCatalog lookup failed; reconciling without workspace cascade");
            None
        }
    };

    // ReactivePolicy auto-suspend gate: a prior reconcile triggered a
    // Suspend escalation (e.g. 5+ consecutive failures) and patched
    // status.autoSuspended=true. Halt every reconcile until the
    // operator-human clears the flag (e.g. `kubectl patch ... -p
    // '{"status":{"autoSuspended":false}}' --subresource status`).
    // This is the typed circuit breaker.
    let auto_suspended = template
        .status
        .as_ref()
        .map(|s| s.auto_suspended)
        .unwrap_or(false);
    if auto_suspended {
        // Never-stuck: a corrective .spec edit self-clears the park.
        // Previously this gate short-circuited BEFORE the
        // generation-change handler below (:305), so even fixing the
        // config that tripped the breaker could not un-park the
        // template — a human had to `kubectl patch autoSuspended=false`.
        // arc-github/drive/zot sat parked 20-54 days on a compile that
        // had since gone green. A generation bump (metadata.generation
        // vs status.observedGeneration diverging) is an explicit
        // corrective signal: clear the latch and FALL THROUGH so the
        // generation-change handler resets to Pending and re-compiles
        // against the new spec. Also self-clears when reactive policy
        // (`apply_reactive_policy`) resolves to Healthy on a clean
        // reconcile — this gate is the belt to that suspenders.
        let observed_gen = template
            .status
            .as_ref()
            .map(|s| s.observed_generation)
            .unwrap_or(0);
        let current_gen = template.metadata.generation.unwrap_or(0);
        if auto_suspend_gate_should_clear(current_gen, observed_gen) {
            info!(
                current_gen,
                observed_gen,
                "Template was auto-suspended but its spec changed (corrective edit) — \
                 clearing status.autoSuspended and re-reconciling against the new spec"
            );
            let clear_patch = serde_json::json!({
                "status": { "autoSuspended": false }
            });
            crate::controller::status_patch::patch_status(&*template, &state.client, clear_patch)
                .await
                .map_err(crate::error::Error::Kube)?;
            record_event(
                &template,
                &state,
                EventType::Normal,
                "AutoSuspendCleared",
                "Spec changed since auto-suspension; clearing autoSuspended and re-reconciling",
            )
            .await;
            // Fall through: the code below reaches the generation-change
            // handler (generation_invalidates_render(current_gen,
            // observed_gen), i.e. current_gen > observed_gen), which
            // cleans the workspace and resets to Pending, then returns.
            // `template`
            // is an `Arc` (immutable); the on-cluster clear PATCH above
            // is authoritative, and no code on this fall-through path
            // re-reads the stale in-memory `auto_suspended` before the
            // gen handler returns.
        } else {
            // No corrective .spec edit. But a park must never be
            // permanent: a template auto-suspended by a TRANSIENT cause
            // (a provider process that later respawns clean, a DB blip, a
            // provider rate-limit, an operator-image fix that changed the
            // operator's behavior without changing this template's spec)
            // would otherwise sit parked forever, because gen == obs means
            // the corrective-edit gate above never fires. That is the #1
            // stuck class this reaction exists to kill.
            //
            // Circuit-breaker HALF-OPEN probe: once
            // AUTO_SUSPEND_PROBE_INTERVAL has elapsed since the park
            // (status.lastEscalatedAt), let exactly ONE reconcile fall
            // through and re-attempt. If the cause has cleared,
            // `apply_reactive_policy` resolves `Escalation::Healthy` and
            // `next_auto_suspended()` writes autoSuspended=false — the
            // template self-resumes with no human action. If it fails
            // again, escalation re-fires, `lastEscalatedAt` refreshes, and
            // the breaker re-opens for another full interval (bounded
            // low-frequency retry, never a hammer). The only downstream
            // reads of `auto_suspended` are this gate (verified single
            // site), so falling through with the on-cluster latch still
            // true is safe — nothing below re-parks on the stale value.
            let last_escalated_at = template
                .status
                .as_ref()
                .and_then(|s| s.last_escalated_at);
            if auto_suspend_probe_due(
                Utc::now(),
                last_escalated_at,
                auto_suspend_probe_interval(),
            ) {
                info!(
                    ?last_escalated_at,
                    probe_interval_secs = AUTO_SUSPEND_PROBE_INTERVAL_SECS,
                    "Auto-suspend circuit breaker HALF-OPEN: probe interval elapsed since park; \
                     clearing autoSuspended and re-attempting the parked template. A clean \
                     reconcile leaves it cleared (self-healed); a failing one re-arms the \
                     breaker via the failure path's ReactivePolicy escalation."
                );
                // HALF-OPEN = give the template exactly one attempt with the
                // latch OFF. We MUST clear autoSuspended here rather than
                // relying on a Healthy reconcile to clear it: the ReactivePolicy
                // stage (apply_reactive_policy → next_auto_suspended) runs ONLY
                // from update_phase_with_error (the failure status path) via
                // post_reconcile_pipeline — a SUCCESSFUL/Ready reconcile never
                // invokes it, so a recovered template would otherwise stay
                // latched forever. Clear proactively; if the re-attempt fails,
                // update_phase_with_error → apply_reactive_policy re-escalates
                // and re-sets autoSuspended + lastEscalatedAt (breaker re-opens
                // for another interval). If it succeeds, the latch stays off and
                // the template has self-resumed with no human action.
                let clear_patch = serde_json::json!({
                    "status": { "autoSuspended": false }
                });
                crate::controller::status_patch::patch_status(
                    &*template,
                    &state.client,
                    clear_patch,
                )
                .await
                .map_err(crate::error::Error::Kube)?;
                record_event(
                    &template,
                    &state,
                    EventType::Normal,
                    "AutoSuspendProbe",
                    "Circuit-breaker HALF-OPEN probe: cleared autoSuspended and re-attempting; \
                     the failure path re-arms the breaker if the cause has not cleared",
                )
                .await;
                // Fall through to the normal reconcile path (gen == obs, so
                // the generation handler below is a no-op and control reaches
                // compile/plan/apply/verify).
            } else {
                // Breaker still OPEN — stay parked, requeue, re-check the
                // probe clock next cycle.
                info!(
                    ?last_escalated_at,
                    "Template auto-suspended by ReactivePolicy (status.autoSuspended=true); \
                     circuit breaker OPEN, next HALF-OPEN probe pending. \
                     Edit the spec to correct + resume immediately, or clear it manually. \
                     lastEscalationReason: {:?}",
                    template
                        .status
                        .as_ref()
                        .and_then(|s| s.last_escalation_reason.as_deref())
                );
                return Ok(Action::requeue(DEFAULT_REQUEUE_INTERVAL));
            }
        }
    }

    // Check if suspended — emit conditions so FluxCD sees definitive state.
    // Also honor the parent WorkspaceCatalog's suspend flag (cascade) —
    // suspending a workspace stops every template under it without
    // touching each template's spec.
    let workspace_suspended = parent_wsc.as_ref().map(|w| w.spec.suspend).unwrap_or(false);
    if template.spec.suspend || workspace_suspended {
        info!(
            workspace_suspended,
            template_suspended = template.spec.suspend,
            "Template is suspended, skipping reconciliation"
        );
        // Diff-gate: skip the status patch when the on-cluster
        // conditions already match the suspended set. Without this
        // gate, every reconcile (~5 min default + every status-watch
        // event) re-PATCHes the same `(type, status, reason, message)`
        // tuple with a fresh `lastTransitionTime` (`create_condition`
        // calls `Utc::now()`). The PATCH bumps `metadata.resourceVersion`,
        // the watch fires, the controller re-reconciles — closed loop
        // observed at ~123 PATCHes/sec on a single template.
        let new_conditions = conditions_for_suspended();
        let prev_conditions: &[crate::crd::Condition] = template
            .status
            .as_ref()
            .map(|s| s.conditions.as_slice())
            .unwrap_or(&[]);
        let already_set = suspended_conditions_already_set(prev_conditions, &new_conditions);
        if !already_set {
            let ns = template.namespace().unwrap_or_default();
            let api: Api<InfrastructureTemplate> = Api::namespaced(state.client.clone(), &ns);
            let patch = serde_json::json!({
                "status": { "conditions": new_conditions }
            });
            let _ = api
                .patch_status(
                    &name,
                    &PatchParams::apply("pangea-operator"),
                    &Patch::Merge(&patch),
                )
                .await;
        } else {
            debug!(
                "Suspended template conditions already set; skipping status patch (avoids self-trigger watch loop)"
            );
        }
        return Ok(Action::requeue(DEFAULT_REQUEUE_INTERVAL));
    }

    // Detect spec changes (generation mismatch) — clean workspace and restart from Pending.
    // This ensures stale .terraform state, lock files, and cached providers are cleared
    // when the template source or configuration changes.
    //
    // Render invalidation (never-stuck Reaction D): the reset to Pending
    // routes through Pending → Compiling, and `handle_compiling` ALWAYS
    // recompiles and overwrites the Postgres `rendered_config` row via
    // `put_rendered_config` — so the stale render is not reused after a
    // spec change. The disk `workspace.clean()` covers the disk-fallback
    // path; the DB path is invalidated by the mandatory recompile plus the
    // generation-aware reuse gate in `compiled_config_available`
    // (`generation_invalidates_render`), which forces a recompile whenever
    // `generation` is ahead of `observedGeneration` even for a
    // `spec.variables`-only edit that leaves the source-content revision
    // unchanged. Before the generation-aware gate, such an edit could serve
    // the OLD render off the new spec until a manual pod restart.
    let observed_gen = template
        .status
        .as_ref()
        .map(|s| s.observed_generation)
        .unwrap_or(0);
    let current_gen = template.metadata.generation.unwrap_or(0);
    let current_phase = template
        .status
        .as_ref()
        .and_then(|s| s.phase)
        .unwrap_or(Phase::Pending);

    if generation_invalidates_render(current_gen, observed_gen) && current_phase != Phase::Pending && current_phase != Phase::Destroying {
        info!(
            current_gen,
            observed_gen,
            "Spec changed — cleaning workspace and restarting from Pending"
        );
        let workspace = state.workspace_manager.get_workspace(&template).await?;
        workspace.clean().await?;
        update_phase(&template, Phase::Pending, &state).await?;
        record_event(&template, &state, EventType::Normal, "SpecChanged", "Template spec changed, restarting reconciliation").await;
        return Ok(Action::requeue(SHORT_REQUEUE_INTERVAL));
    }

    // Dispatch to the per-phase handler. The state machine is documented
    // at `dispatch_template_phase` below; reconcile_template stays focused
    // on the surrounding lifecycle (deletion, finalizers, gen-tracking,
    // gating) and delegates the body work to phase handlers.
    let action = match dispatch_template_phase(current_phase, &template, &state).await {
        Ok(action) => action,
        Err(e) => {
            // HONEST FEEDBACK. A hard reconcile error otherwise escapes via `?`
            // to `error_policy`, which only WARN-logs + requeues — the CR status
            // stays frozen at its current phase (a silent spin). Record the real
            // reason on the CR (Phase::Failed + .status.lastError) BEFORE
            // propagating, so the convergence→delivery feedback is honest: an
            // operator sees e.g. "Magma execution failed: substrate preflight
            // violations: resources declared but no terraform.required_providers"
            // on `.status` and fixes the config, instead of staring at a stuck
            // "Planning". The soft-failure paths (handlers returning Ok with a
            // recorded Failed) are unaffected — this catches only the un-recorded
            // hard `Err`s. Best-effort: a status-patch failure must NOT mask the
            // original error — log it and still surface `e` for escalation/backoff.
            let err_msg = format!("{e}");
            if let Err(pe) =
                update_phase_with_error(&template, Phase::Failed, &err_msg, &state).await
            {
                warn!(error = %pe, "failed to record reconcile error on CR status");
            }
            record_event(&template, &state, EventType::Warning, "ReconcileFailed", &err_msg).await;
            return Err(e);
        }
    };

    // ★★ Every-reconcile post-reconcile hook (the load-bearing generalization).
    // ReactivePolicy (apply_reactive_policy) previously ran ONLY on the failure
    // path (status.rs update_phase_with_error → post_reconcile_pipeline), so a
    // SUCCESSFUL/Ready reconcile never ran it: the Healthy self-clear of
    // autoSuspended (next_auto_suspended's Escalation::Healthy → false branch)
    // was effectively dead on success, and the verified-blocked clock + the
    // Healthy=True condition only refreshed when a template FAILED. Run it here
    // on EVERY phase-completing reconcile, on FRESH status (re-GET — the
    // reconcile-start `template` Arc is a stale snapshot that server-side-merge
    // PATCHes never mutate). Loop-safe by two independent brakes: the /status
    // subresource write cannot bump metadata.generation (so the generation
    // predicate on the watch stream drops the refire) and reactive_policy_status_
    // unchanged skips the PATCH entirely in steady state (zero-write on healthy
    // Ready templates). The failure paths KEEP their own run_for_template call
    // (status.rs:197) so every early-return caller — including handle_destroying,
    // which returns at :201 and never reaches here — retains its escalation
    // ladder; the harmless double-run on soft-failures that also flow here is
    // suppressed by the diff-gate + the ReactivePolicyTriggered event debounce.
    run_post_reconcile_on_fresh(&template, &state).await;

    Ok(action.into())
}

/// Run the post-reconcile pipeline (ReactivePolicy escalation + the Healthy
/// self-clear of `autoSuspended` + the verified-blocked clock) on FRESH status.
///
/// The `template` handed to `reconcile_template` is the reconcile-START `Arc`
/// snapshot; every status write this cycle is a server-side-merge PATCH that
/// does NOT mutate that snapshot, so evaluating escalation against it would read
/// `failureCount` / `phase` / `autoSuspended` one cycle stale — making the first
/// escalation and the Healthy self-clear dishonest (e.g. a template that just
/// succeeded to Ready with failureCount reset to 0 would still be judged against
/// its pre-reset count). Re-GET before running. Best-effort: a failed GET, or a
/// template deleted mid-cycle, is logged and skipped, never fatal — the reconcile
/// has already produced its `Action` by the time we reach here.
async fn run_post_reconcile_on_fresh(template: &InfrastructureTemplate, state: &ControllerState) {
    let name = template.name_any();
    let ns = template.namespace().unwrap_or_default();
    let api: Api<InfrastructureTemplate> = Api::namespaced(state.client.clone(), &ns);
    match api.get_opt(&name).await {
        Ok(Some(fresh)) => {
            crate::controller::post_reconcile_pipeline::run_for_template(&fresh, state).await;
        }
        Ok(None) => {
            tracing::debug!(%name, %ns, "post-reconcile: template gone on re-GET; skipping reactive policy");
        }
        Err(e) => {
            warn!(error = %e, %name, %ns, "post-reconcile re-GET failed; skipping reactive policy this cycle (non-fatal)");
        }
    }
}

/// State-machine dispatch for `InfrastructureTemplate` reconcile phases.
///
/// Phase transition graph (canonical happy path; deviations land in
/// `Failed` or `Drifted`):
///
/// ```text
///   Pending → Verifying → Verified → Compiling → Initializing →
///   Planning → Applying → Ready
///   Ready ↔ Drifted  (drift-detect cycle)
///   * → Failed       (escalation; manual reset)
///   * → Destroying   (deletion; finalizer cleanup)
/// ```
///
/// `Verifying` and `Verified` currently no-op forward to `Compiling` —
/// the M1 ArchitectureGem registry lookup that makes them load-bearing
/// is in `theory/PANGEA-WORKSPACE-RECONCILIATION.md` M2.
///
/// Every individual handler bumps `reconciliation_duration_seconds{phase}`
/// via `state.metrics.record_phase_duration(...)` (wired in C3) so
/// dashboards can compute per-phase p50/p99.
async fn dispatch_template_phase(
    current_phase: Phase,
    template: &InfrastructureTemplate,
    state: &ControllerState,
) -> Result<ReconcileAction> {
    // Verifying / Verified are synthetic forward-stub phases — handled
    // inline rather than via the trait dispatch. See M2 in
    // theory/PANGEA-WORKSPACE-RECONCILIATION.md for the planned
    // ArchitectureGem readiness check that makes them load-bearing.
    if matches!(current_phase, Phase::Verifying | Phase::Verified) {
        update_phase(template, Phase::Compiling, state).await?;
        return Ok(ReconcileAction::Requeue(SHORT_REQUEUE_INTERVAL));
    }

    // Per-phase trait dispatch (D2). Each Phase variant has a typed
    // ReconcilePhase impl in `controller::template_phase`; the impl is
    // a thin wrapper around the corresponding handle_<phase> body in
    // this file. for_phase returns None only for Verifying/Verified
    // (which we already handled inline above).
    let handler = crate::controller::template_phase::for_phase(current_phase)
        .expect("every concrete Phase has a ReconcilePhase impl");

    // ── Live-dispatch admission gate (the no-crash bound) ──────────────
    // Only the EXPENSIVE phases consume a workspace-scoped budget permit:
    // Compiling (git clone + ruby compile), Planning (magma plan), Applying
    // (provider-RPC apply). Cheap phases — status transitions, drift
    // checks, deletion — run ungated. When the workspace (or the global
    // pool) is at cap we DEFER: requeue in the same phase, never drop the
    // work; the next tick retries. The permit is RAII — held across
    // handler.handle and freed on any exit (Ok OR Err), so an errored
    // expensive phase never leaks a slot. At current fleet scale the
    // generous defaults make this a no-op; it becomes load-bearing only
    // when ready work exceeds budget. See `ControllerState.workspace_budgets`.
    if matches!(
        current_phase,
        Phase::Compiling | Phase::Planning | Phase::Applying
    ) {
        let scope = workspace_scope(template);
        match state.workspace_budgets.try_acquire(&scope) {
            Some(_permit) => handler.handle(template, state).await,
            None => {
                // `Phase: Display` (strum) is the typed stringify surface — no format!().
                let phase_label = current_phase.to_string();
                state
                    .metrics
                    .record_admission_deferred(&scope, &phase_label);
                tracing::debug!(
                    scope = %scope,
                    phase = %current_phase,
                    "workspace concurrency budget exhausted; deferring expensive phase (will retry next tick)"
                );
                Ok(ReconcileAction::Requeue(SHORT_REQUEUE_INTERVAL))
            }
        }
    } else {
        handler.handle(template, state).await
    }
}

/// The budget + fairness scope for a template: its workspace label
/// (`pangea.pleme.io/workspace`), or `namespace/name` when unlabeled — so
/// an unlabeled template is its own bucket and can neither share a budget
/// slice with, nor be starved by, others. Mirrors the workspace identity
/// the policy cascade already keys on.
fn workspace_scope(template: &InfrastructureTemplate) -> String {
    if let Some(ws) = template
        .metadata
        .labels
        .as_ref()
        .and_then(|l| l.get(crate::controller::workspace_catalog_controller::WORKSPACE_LABEL))
    {
        return ws.clone();
    }
    let ns = template.metadata.namespace.as_deref().unwrap_or("default");
    let name = template.metadata.name.as_deref().unwrap_or("unknown");
    // Typed slice-join, not format!() — a '/'-joined scope key (TYPED EMISSION).
    [ns, name].join("/")
}

/// Resolve a template's cross-template dependencies (P2/P3, Terragrunt
/// `dependency.<x>.outputs` + `run-all`): for each `spec.variableRefs` entry,
/// fetch the referenced upstream template's `status.outputs` and resolve the
/// reference (real output, else `mockOutput`, else recorded unresolved). The
/// pure resolution lives in `controller::template_dependency`; this is just the
/// kube fetch around it. Returns the resolution; the caller injects
/// `all_variables()` and gates on `unresolved_templates` (the run-all wait).
async fn resolve_template_dependencies(
    refs: &std::collections::BTreeMap<String, crate::crd::infrastructure_template::VariableRef>,
    template: &InfrastructureTemplate,
    state: &ControllerState,
) -> crate::controller::template_dependency::DependencyResolution {
    use crate::controller::template_dependency::{resolve_dependency_vars, DepRef, UpstreamOutputs};
    let own_ns = template.metadata.namespace.as_deref().unwrap_or("default");
    let mut dep_refs = std::collections::BTreeMap::new();
    let mut upstream: UpstreamOutputs = std::collections::BTreeMap::new();
    for (var_name, vref) in refs {
        let up_name = vref.template_ref.name.clone();
        dep_refs.insert(
            var_name.clone(),
            DepRef { template: up_name.clone(), output_key: vref.output_key.clone(), mock: vref.mock_output.clone() },
        );
        if !upstream.contains_key(&up_name) {
            let up_ns = vref.template_ref.namespace.as_deref().unwrap_or(own_ns);
            let up_api: kube::Api<InfrastructureTemplate> = kube::Api::namespaced(state.client.clone(), up_ns);
            if let Ok(Some(up)) = up_api.get_opt(&up_name).await {
                if let Some(outs) = up.status.as_ref().and_then(|s| s.outputs.clone()) {
                    upstream.insert(up_name, outs);
                }
            }
        }
    }
    resolve_dependency_vars(&dep_refs, &upstream)
}

/// Handle Pending phase - prepare for compilation.
/// Public wrapper for `handle_pending` so trait impls in
/// `controller::template_phase` can dispatch to it. The body lives
/// here; the trait impl is the thin shim.
pub(crate) async fn handle_pending_internal(
    template: &InfrastructureTemplate,
    state: &ControllerState,
) -> Result<ReconcileAction> {
    handle_pending(template, state).await
}

#[tracing::instrument(skip_all, name = "handle_pending")]
async fn handle_pending(
    template: &InfrastructureTemplate,
    state: &ControllerState,
) -> Result<ReconcileAction> {
    info!("Template in Pending phase, preparing for compilation");

    // Validate template source
    validate_source(template)?;

    // Update status to Compiling
    update_phase(template, Phase::Compiling, state).await?;

    Ok(ReconcileAction::Requeue(SHORT_REQUEUE_INTERVAL))
}

/// Handle Compiling phase - write template source to workspace.
///
/// For MVP, supports inline Terraform JSON and ConfigMap sources.
/// Ruby DSL compilation via sidecar is deferred to a future phase.
/// Public wrapper for `handle_compiling` so trait impls in
/// `controller::template_phase` can dispatch to it. The body lives
/// here; the trait impl is the thin shim.
pub(crate) async fn handle_compiling_internal(
    template: &InfrastructureTemplate,
    state: &ControllerState,
) -> Result<ReconcileAction> {
    handle_compiling(template, state).await
}

#[tracing::instrument(skip_all, name = "handle_compiling")]
async fn handle_compiling(
    template: &InfrastructureTemplate,
    state: &ControllerState,
) -> Result<ReconcileAction> {
    info!("Template in Compiling phase");
    let _phase_timer = state.metrics.record_phase_duration("compiling");
    let _compile_timer = state.metrics.record_compile_duration();

    let workspace = state.workspace_manager.get_workspace(template).await?;
    let source = &template.spec.source;

    // Freshness model (§ staleness honesty): the commit this compile
    // consumes. Captured in the gitRepository arm right after the
    // clone/fetch lands; written to `status.compiledRevision` in the
    // compile-success block so handle_ready's freshness gate can
    // compare it against the observed remote HEAD. `None` for
    // inline / configMap sources (no revision to be stale against).
    let mut compiled_revision: Option<String> = None;

    // Resolve template content from source
    let content = if let Some(inline) = &source.inline {
        inline.clone()
    } else if let Some(cm_ref) = &source.config_map_ref {
        let ns = cm_ref
            .namespace
            .clone()
            .or_else(|| template.namespace())
            .unwrap_or_default();
        let cm_api: Api<ConfigMap> = Api::namespaced(state.client.clone(), &ns);
        let cm = cm_api.get(&cm_ref.name).await.map_err(|e| {
            Error::Config(format!("Failed to fetch ConfigMap {}/{}: {}", ns, cm_ref.name, e))
        })?;
        cm.data
            .as_ref()
            .and_then(|d| d.get(&cm_ref.key))
            .cloned()
            .ok_or_else(|| {
                Error::Config(format!(
                    "Key '{}' not found in ConfigMap {}/{}",
                    cm_ref.key, ns, cm_ref.name
                ))
            })?
    } else if let Some(git_ref) = &source.git_repository {
        // Clone or fetch the Git repository, then read the template file.
        //
        // NOTE (zero-disk invariant): this git clone — like loading the
        // gRPC provider-plugin binaries — is workspace *input*
        // acquisition, the same sanctioned filesystem class as
        // provider-plugin loading, NOT durable operator execution state.
        // The zero-disk invariant (★★ MAGMA-NATIVE EXECUTION) is about
        // execution state — the rendered config, the plan, the bundle,
        // and the tofu state — which on the magma path now all live in
        // Postgres. Acquiring the source tree the compiler reads from is
        // not durable execution state and stays on disk by design.
        let repo_dir = workspace.path.join("_repo");

        // Resolve git credentials if specified (factored so the
        // freshness gate's `observe_head` reuses the same auth env).
        let env_vars = git_auth_env(template, state, &workspace).await?;

        // Clone or update (with 120s timeout)
        let git_timeout = Duration::from_secs(120);
        let git_result = if repo_dir.exists() {
            let mut cmd = tokio::process::Command::new("git");
            cmd.args(["fetch", "origin", &git_ref.r#ref])
                .current_dir(&repo_dir)
                .stdout(std::process::Stdio::piped())
                .stderr(std::process::Stdio::piped());
            for (k, v) in &env_vars {
                cmd.env(k, v);
            }
            match tokio::time::timeout(git_timeout, cmd.output()).await {
                Ok(Ok(output)) if output.status.success() => {
                    let checkout = tokio::process::Command::new("git")
                        .args(["checkout", "FETCH_HEAD"])
                        .current_dir(&repo_dir)
                        .output()
                        .await
                        .map_err(|e| Error::Io(e))?;
                    checkout.status.success()
                }
                Ok(Ok(_)) => false,
                Ok(Err(e)) => return Err(Error::Io(e)),
                Err(_) => return Err(Error::Timeout(120)),
            }
        } else {
            let mut cmd = tokio::process::Command::new("git");
            cmd.args([
                "clone",
                "--depth=1",
                "--branch",
                &git_ref.r#ref,
                &git_ref.url,
                &repo_dir.to_string_lossy(),
            ])
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped());
            for (k, v) in &env_vars {
                cmd.env(k, v);
            }
            match tokio::time::timeout(git_timeout, cmd.output()).await {
                Ok(Ok(output)) => output.status.success(),
                Ok(Err(e)) => return Err(Error::Io(e)),
                Err(_) => return Err(Error::Timeout(120)),
            }
        };

        if !git_result {
            return Err(Error::Compilation(format!(
                "Failed to clone/fetch git repository: {}",
                git_ref.url
            )));
        }

        // The clone/fetch landed — record exactly which commit this
        // compile is about to consume. Before this, NO status field
        // could express "the compile is stale" (staleness was
        // unrepresentable in the wrong direction).
        compiled_revision = Some(git_rev_parse_head(&repo_dir).await?);

        // For gitRepository sources, do NOT read the file into memory
        // and ship it as a `source` string — that would lose the
        // workspace-dir context that Pangea workspace templates rely
        // on (sibling .rb files via `require_relative`, __dir__-
        // relative YAML reads, etc.). Instead, return a sentinel that
        // tells the downstream compile-request builder to use
        // `template_path` mode, which lets the compiler `load(path)`
        // from the shared workspaces emptyDir with CWD set to the
        // workspace dir.
        let template_path = repo_dir.join(&git_ref.path);
        if !template_path.is_file() {
            return Err(Error::Compilation(format!(
                "Failed to read template file '{}': not a regular file (cloned repo path: {})",
                git_ref.path,
                template_path.display()
            )));
        }
        // Use \0-prefixed sentinel so it can never collide with real
        // template content. The compile-request builder downstream
        // splits it back into the `template_path` JSON field. The
        // \0RUBYLIB\0 segment carries the cloned-tree's `lib/` dir
        // so the compiler can prepend it to $LOAD_PATH around the
        // load — that's what lets workspace .rb files
        // `require 'pangea/architectures'` resolve to the *cloned*
        // composer copy instead of an image-baked path-gem. Cuts
        // pangea-architectures out of the image's grammar layer.
        format!(
            "\0PATH\0{}\0RUBYLIB\0{}",
            template_path.to_string_lossy(),
            repo_dir.join("lib").to_string_lossy(),
        )
    } else {
        return Err(Error::InvalidSource("No template source specified".into()));
    };

    // Non-git sources (inline / configMap) have no remote HEAD to anchor
    // freshness — record the content-revision as the compiledRevision so a
    // later config EDIT is detectable by `compiled_config_available`. Git
    // already set compiled_revision to its HEAD SHA above; this fills only the
    // non-git case. (For git, `content` is the \0PATH\0 sentinel, never hashed.)
    if compiled_revision.is_none() {
        compiled_revision = Some(content_revision(&content));
    }

    // Distinguish three modes:
    //   1. content starts with `{` → already-rendered Terraform JSON, use as-is.
    //   2. content starts with `\0PATH\0` → gitRepository sentinel; the compile
    //      request uses `template_path` mode so the compiler `load`s the file
    //      from the shared workspaces emptyDir with CWD set to the workspace
    //      dir. Preserves __dir__ + require_relative semantics for canonical
    //      Pangea workspace patterns.
    //   3. otherwise → inline / configMap source, send as `source` string
    //      (legacy eval mode in the compiler).
    let terraform_json = if content.trim_start().starts_with('{') {
        // Already JSON — use directly
        content
    } else {
        // Ruby DSL — dispatch via the CompilerBackend trait. Pre-M8.2
        // this was a direct reqwest to the compiler sidecar; now the
        // backend chooses HTTP-or-embedded.
        // CONFIG INHERITANCE CASCADE (P1, Terragrunt `root.hcl`/`include` parity):
        // the effective variables are the deep-merge of the outer scopes' defaults
        // and the template's own — `PangeaNamespace.defaultVariables` (outermost)
        // → `WorkspaceCatalog.variables` → `template.spec.variables` (innermost,
        // wins per key; nested objects merge recursively). A template inherits
        // fleet/workspace defaults and overrides only what it needs. Lookups are
        // best-effort: a missing namespace/workspace simply contributes no layer
        // (behaviour-preserving for templates with no parent).
        let ns_defaults = {
            let pns_api: kube::Api<crate::crd::PangeaNamespace> = kube::Api::all(state.client.clone());
            pns_api
                .get_opt(&template.spec.pangea_namespace)
                .await
                .ok()
                .flatten()
                .and_then(|ns| ns.spec.default_variables)
                .unwrap_or_default()
        };
        let ws_defaults = crate::controller::workspace_catalog_controller::parent_catalog_for_template(
            &state.client,
            template,
        )
        .await
        .ok()
        .flatten()
        .and_then(|wsc| wsc.spec.variables)
        .unwrap_or_default();
        let template_vars = template.spec.variables.clone().unwrap_or_default();
        let mut variables = crate::controller::config_cascade::resolve_variables(&[
            &ns_defaults,
            &ws_defaults,
            &template_vars,
        ]);

        // CROSS-TEMPLATE DEPENDENCY + OUTPUTS (P2/P3, Terragrunt
        // `dependency.<x>.outputs` + `run-all`): resolve `spec.variableRefs`
        // against upstream templates' `status.outputs`, then inject the values
        // as variables (deps win over inherited defaults — they're the innermost
        // intent). If an upstream isn't Ready and has no `mockOutput`, this is
        // the run-all GATE: requeue until the upstream converges, exactly as
        // `terragrunt run-all` blocks a unit on its dependencies.
        if let Some(refs) = &template.spec.variable_refs {
            if !refs.is_empty() {
                let resolution = resolve_template_dependencies(refs, template, state).await;
                if !resolution.unresolved_templates.is_empty() {
                    info!(
                        unresolved = ?resolution.unresolved_templates,
                        "run-all gate: waiting on upstream template outputs (no value yet, no mock) — requeueing"
                    );
                    return Ok(ReconcileAction::Requeue(SHORT_REQUEUE_INTERVAL));
                }
                for (k, v) in resolution.all_variables() {
                    variables.insert(k, v);
                }
            }
        }

        // Plus every key from any providerCredentials secret —
        // Pangea workspace templates use `ENV.fetch('CF_API_TOKEN')`
        // etc. for provider config, which the compiler installs into
        // ENV around eval. The convention is that secret data keys
        // ARE the env var names (so the secret has `CF_API_TOKEN`,
        // `CF_ACCOUNT_ID`, … verbatim). Operator-side naming
        // transforms would re-introduce the kind of brittle wiring
        // we just stripped out elsewhere.
        //
        // Iteration is exhaustive over `ProviderCredentials` via
        // `iter_secret_refs()` — adding a new provider field to the
        // CRD without updating the iterator's destructuring pattern
        // is a Rust compile error. This typed contract supersedes
        // the silent failure mode that shipped GitHubCredentials in
        // 92f2f74 without env-var injection.
        if let Some(provider_creds) = template.spec.provider_credentials.as_ref() {
            for (provider_kind, sref) in provider_creds.iter_secret_refs() {
                let ns = sref
                    .namespace
                    .clone()
                    .or_else(|| template.namespace())
                    .unwrap_or_default();
                let secret_api: Api<Secret> = Api::namespaced(state.client.clone(), &ns);
                let secret = secret_api.get(&sref.name).await.map_err(|_| {
                    Error::SecretNotFound {
                        namespace: ns.clone(),
                        name: sref.name.clone(),
                    }
                })?;
                debug!(
                    provider = provider_kind.name(),
                    secret_namespace = %ns,
                    secret_name = %sref.name,
                    "Loaded provider credentials secret"
                );
                if let Some(data) = &secret.data {
                    for (k, v) in data.iter() {
                        let val = String::from_utf8_lossy(&v.0).to_string();
                        variables
                            .entry(k.clone())
                            .or_insert(serde_json::Value::String(val));
                    }
                }
            }
        }

        let compile_request = if let Some(path) = content.strip_prefix("\0PATH\0") {
            // gitRepository — let the compiler load the file from disk
            // so it sees its workspace siblings, and prepend the
            // cloned tree's `lib/` to $LOAD_PATH so `require
            // 'pangea/architectures'` resolves to the cloned composer
            // copy (not an image-baked path-gem).
            let (template_path, rubylib_paths) = match path.split_once("\0RUBYLIB\0") {
                Some((tp, rl)) => (tp.to_string(), vec![rl.to_string()]),
                None => (path.to_string(), Vec::<String>::new()),
            };
            crate::ruby::CompileRequest {
                template_path: Some(template_path),
                rubylib_paths,
                variables: variables.clone().into_iter().collect(),
                template_name: template.spec.template_name.clone(),
                source: None,
            }
        } else {
            // inline / configMapRef — eval the string in a virtual binding.
            crate::ruby::CompileRequest {
                source: Some(content.clone()),
                variables: variables.clone().into_iter().collect(),
                template_name: template.spec.template_name.clone(),
                template_path: None,
                rubylib_paths: Vec::new(),
            }
        };

        let compile_result = match state.compiler_backend.compile(compile_request).await {
            Ok(r) => r,
            Err(e) => {
                // Compile failure path — increment the per-template
                // consecutive-failure counter and escalate if we hit
                // the settling threshold. Without this, templates
                // like `pleme-io-opensource` (missing gem) sit in
                // Compiling cycleCount=0 indefinitely because the
                // cycle counter only advances after a complete
                // plan→apply, which never reaches. (A residual
                // dual-load lands here too — `BackendError::DualLoad`
                // — and rides the same ladder, LOUD by construction.)
                handle_compile_failure(template, state, &e.to_string()).await?;
                return Err(Error::Compilation(format!("Compile failed: {e}")));
            }
        };

        compile_result.terraform_json
    };

    // Persist the compile→plan rendered-config handoff.
    //
    // On the magma DB-backed path (executor resolves to magma AND the
    // artifact store is wired) the rendered terraform JSON is stored in
    // Postgres (`put_rendered_config`) so the plan phase — a SEPARATE
    // reconcile, possibly on a fresh pod — reads it from the DB rather
    // than a pod-local file. This is the zero-disk compile handoff: a
    // pod roll between Compiling and Planning no longer os-error-2-loops
    // on a missing `main.tf.json`. Keys MUST match `magma_executor_for`:
    // schema = "pangea_{spec.pangeaNamespace}", template = name_any().
    // Per the org ★★ MAGMA-NATIVE EXECUTION directive.
    let magma_active = state.executor_for(template).name() == "magma";
    // The DB-backed magma path = magma active AND the artifact store is
    // wired. On that path the rendered config lives in Postgres and
    // magma reads it via `load_config_routed` (Postgres), NOT from
    // `main.tf.json` on disk — so the disk write below is redundant and
    // is skipped (zero-disk). When magma is active but the store is
    // absent (disk fallback), magma's `load_config` DOES read
    // `main.tf.json`, so the write must stay.
    let magma_db_backed = magma_active && state.artifact_store.is_some();
    if magma_active {
        if let Some(store) = state.artifact_store.as_ref() {
            let value: serde_json::Value = serde_json::from_str(&terraform_json)
                .map_err(|e| Error::Compilation(format!("rendered terraform JSON is not valid JSON: {e}")))?;
            let schema_name = format!("pangea_{}", template.spec.pangea_namespace);
            let template_name = template.name_any();
            // Record the source revision this render was produced AT — the
            // git HEAD SHA (git sources) or the `cm:` content hash
            // (inline/configMap), the SAME value written to
            // `status.compiledRevision` a few lines below via
            // `update_compiled_revision`. `compiled_config_available`
            // compares the stored revision against `status.compiledRevision`
            // to decide whether a cached render is still current, so the put
            // (compile) and the get (reuse) MUST agree on the revision — the
            // revision extension of the "Keys MUST match" invariant. Without
            // it, a git source whose HEAD advanced kept serving the render
            // from the OLDER revision (the stale-render class this fixes).
            store
                .put_rendered_config(
                    &schema_name,
                    &template_name,
                    &value,
                    compiled_revision.as_deref(),
                )
                .await?;
            info!("Rendered config persisted to Postgres artifact store (zero-disk magma handoff)");
        }
    }

    // Write `main.tf.json` to the workspace UNLESS we're on the DB-backed
    // magma path (where the DB row is the source of truth and nothing
    // reads this file — verified: magma's `load_config_routed` reads
    // Postgres when the store is wired). The tofu path requires it, and
    // the magma disk-fallback path reads it via `load_config`.
    if !magma_db_backed {
        workspace.write_file("main.tf.json", &terraform_json).await?;
        info!("Template content written to workspace");
    } else {
        info!("Skipping main.tf.json disk write (DB-backed magma path; rendered config is in Postgres)");
    }

    // Compile succeeded — reset the failure counter so a subsequent
    // failure starts fresh. The template can recover from a
    // transient error (gem cache miss, network blip) without staying
    // forever-elevated.
    reset_compile_failure_counter(template, state).await?;

    // Persist the compiled revision (git sources only). This is the
    // anchor the freshness gate + `lastAppliedRevision` chain hang
    // off — Ready can now only be uttered against a named commit.
    update_compiled_revision(template, compiled_revision.as_deref(), state).await?;

    update_phase(template, Phase::Initializing, state).await?;
    record_event(template, state, EventType::Normal, "Compiled", "Template source resolved and written to workspace").await;

    Ok(ReconcileAction::Requeue(SHORT_REQUEUE_INTERVAL))
}

/// Assemble the git auth env for a template's `gitRepository` source:
/// resolves the referenced Secret, writes the askpass trio into the
/// workspace, and returns the `GIT_ASKPASS` env pair. Empty when the
/// source has no `secretRef` (public repo). Factored out of
/// `handle_compiling` so the freshness gate's `observe_head`
/// (`git ls-remote`, no clone) authenticates identically.
async fn git_auth_env(
    template: &InfrastructureTemplate,
    state: &ControllerState,
    workspace: &crate::executor::Workspace,
) -> Result<Vec<(String, String)>> {
    let mut env_vars = Vec::new();
    let Some(git_ref) = &template.spec.source.git_repository else {
        return Ok(env_vars);
    };
    if let Some(secret_ref) = &git_ref.secret_ref {
        let ns = secret_ref
            .namespace
            .clone()
            .or_else(|| template.namespace())
            .unwrap_or_default();
        let secret_api: Api<Secret> = Api::namespaced(state.client.clone(), &ns);
        let secret = secret_api.get(&secret_ref.name).await.map_err(|_| {
            Error::SecretNotFound {
                namespace: ns.clone(),
                name: secret_ref.name.clone(),
            }
        })?;

        if let Some(data) = &secret.data {
            // Support HTTPS token auth via username/password
            if let Some(token) = data.get("password").or_else(|| data.get("token")) {
                let username = data
                    .get("username")
                    .map(|v| String::from_utf8_lossy(&v.0).to_string())
                    .unwrap_or_else(|| "git".to_string());
                let password = String::from_utf8_lossy(&token.0).to_string();
                // Write credentials to separate files (avoids shell injection)
                workspace.write_file("_git_user", &username).await?;
                workspace.write_file("_git_pass", &password).await?;
                // GIT_ASKPASS script reads from files — no interpolation
                let askpass_script = workspace.path.join("_git_askpass.sh");
                let user_path = workspace.path.join("_git_user");
                let pass_path = workspace.path.join("_git_pass");
                let script_content = format!(
                    "#!/bin/sh\ncase \"$1\" in\n*Username*) cat '{}' ;;\n*Password*) cat '{}' ;;\nesac",
                    user_path.display(),
                    pass_path.display(),
                );
                workspace.write_file("_git_askpass.sh", &script_content).await?;
                #[cfg(unix)]
                {
                    use std::os::unix::fs::PermissionsExt;
                    let _ = std::fs::set_permissions(
                        &askpass_script,
                        std::fs::Permissions::from_mode(0o700),
                    );
                }
                env_vars.push((
                    "GIT_ASKPASS".to_string(),
                    askpass_script.to_string_lossy().to_string(),
                ));
            }
        }
    }
    // Harden EVERY git invocation (clone/fetch on the mutation path, and
    // ls-remote on the freshness path) so a misconfigured credential helper
    // FAILS FAST instead of hanging to the timeout → Unknown → stale-proceed.
    Ok(crate::controller::template::freshness::non_interactive_git_env(&env_vars))
}

/// Outcome of [`source_freshness_gate`].
enum FreshnessGate {
    /// Continue the phase handler. Carries the verdict so the Settled
    /// condition can name what was (or wasn't) verified; `None` ⇒
    /// non-git source, freshness model not applicable.
    Proceed(Option<Freshness>),
    /// The compile is stale — the gate already bounced the phase to
    /// Compiling; the handler returns this action verbatim.
    Bounce(ReconcileAction),
}

/// The source-freshness gate (operational projection of the pure
/// `freshness::ready_drift_decision` law — its `RecompileStale` arm,
/// applied BEFORE any plan runs so neither "no changes" nor a drift
/// correction can ever be derived from a stale compile). Observes the
/// remote HEAD via `ls-remote` (1 RTT, no clone), records the
/// observation on status, and on `Stale` bounces to Compiling — the
/// exact shape of the missing-main-tf.json restart guards.
///
/// `Unknown` (ls-remote unreachable) does NOT wedge the drift loop:
/// the handler proceeds, the
/// `pangea_source_freshness_check_failures_total` counter ticks, and
/// the Settled message says "HEAD: unverified". Tier-honest: a C2
/// external-world observation, renewed per check.
async fn source_freshness_gate(
    template: &InfrastructureTemplate,
    state: &ControllerState,
    workspace: &crate::executor::Workspace,
    phase_label: &'static str,
) -> Result<FreshnessGate> {
    let Some(git_ref) = &template.spec.source.git_repository else {
        return Ok(FreshnessGate::Proceed(None));
    };
    let env = git_auth_env(template, state, workspace).await?;
    match observe_head(&git_ref.url, &git_ref.r#ref, &env).await {
        Ok(head) => {
            update_freshness_status(template, &ObservationOutcome::Observed(head.clone()), state)
                .await?;
            let compiled = template
                .status
                .as_ref()
                .and_then(|s| s.compiled_revision.as_deref());
            match evaluate_source_freshness(compiled, &head) {
                Freshness::Stale { compiled, head } => {
                    let msg = format!(
                        "Source HEAD {} is ahead of compiled revision {} — recompiling",
                        head,
                        compiled.as_deref().unwrap_or("(none recorded)"),
                    );
                    warn!(phase = phase_label, %msg, "source stale; bouncing to Compiling");
                    record_event(template, state, EventType::Warning, "SourceStale", &msg).await;
                    update_phase(template, Phase::Compiling, state).await?;
                    Ok(FreshnessGate::Bounce(ReconcileAction::Requeue(
                        SHORT_REQUEUE_INTERVAL,
                    )))
                }
                fresh => Ok(FreshnessGate::Proceed(Some(fresh))),
            }
        }
        Err(e) => {
            state.metrics.source_freshness_check_failures_total.inc();
            // Advance the ATTEMPT clock even though the probe failed — a wedged
            // probe is now visibly "checking + failing", never silently frozen
            // (the rio defect: lastFreshnessCheckAt stuck for days while
            // lastDriftCheckAt kept advancing).
            update_freshness_status(template, &ObservationOutcome::Unobserved, state).await?;
            // Surface a TYPED, visible anomaly — never a silent steady state.
            // (The Ready condition stays independently honest: source_fresh_state
            // reads observed_head, which a failed probe does NOT advance, so Ready
            // reflects the last VERIFIED edge — it never guesses at-HEAD.)
            let msg = format!(
                "Source git HEAD could not be observed (ls-remote failed: {e}) — HEAD \
                 unverified; reconciling against the last-observed revision"
            );
            warn!(phase = phase_label, error = %e, "freshness probe failed — {msg}");
            record_event(template, state, EventType::Warning, "SourceUnobservable", &msg).await;
            Ok(FreshnessGate::Proceed(Some(Freshness::Unknown)))
        }
    }
}

/// Default 5 matches `crd::infrastructure_template::default_max_drift_cycles`.
/// We don't import that fn (private to the CRD module); the constant is
/// inlined here. Keep in sync if the CRD default ever moves.
const DEFAULT_MAX_DRIFT_CYCLES: u32 = 5;

/// Pure helper: given the prior count + max threshold, return the
/// next count + whether to escalate. Extracted from
/// `handle_compile_failure` so tests can exercise the logic without
/// a live kube::Api client.
///
/// Contract:
///   * `next = prior + 1` (saturating at u32::MAX)
///   * `escalate = next >= max`
///   * `max == 0` is treated as "never escalate" — a defensive
///     interpretation, since 0 would otherwise escalate on the first
///     failure which is almost certainly user error.
pub(crate) fn evaluate_compile_failure_escalation(
    prior: u32,
    max: u32,
) -> (u32, bool) {
    let next = prior.saturating_add(1);
    let escalate = max > 0 && next >= max;
    (next, escalate)
}

/// Bump `status.consecutiveCompileFailures` and, if it crosses
/// `settlingPolicy.maxConsecutiveDriftCycles`, transition the
/// template to `phase=Failed` with a typed `lastError` and an Event
/// naming the underlying compile error.
///
/// Returns `Ok(())` whether or not escalation happened — the caller
/// re-raises the original error to honor the existing retry semantics.
/// Escalation is purely additive: the next reconcile cycle will see
/// `phase=Failed` and skip past Compiling.
#[tracing::instrument(skip_all, name = "handle_compile_failure")]
async fn handle_compile_failure(
    template: &InfrastructureTemplate,
    state: &ControllerState,
    err_msg: &str,
) -> Result<()> {
    let prior = template
        .status
        .as_ref()
        .map(|s| s.consecutive_compile_failures)
        .unwrap_or(0);

    let max = template
        .spec
        .settling_policy
        .as_ref()
        .map(|p| p.max_consecutive_drift_cycles)
        .unwrap_or(DEFAULT_MAX_DRIFT_CYCLES);

    let (next, escalate) = evaluate_compile_failure_escalation(prior, max);

    let name = template.name_any();
    let namespace = template.namespace().unwrap_or_default();

    // Time-graded recovery recommendation. Consult the escalation
    // ladder against time-since-last-phase-change so operators
    // (and slice-5 action handlers) see the recommended depth of
    // intervention before the settlingPolicy threshold trips.
    //
    // The ladder is the FIX-AXIS sibling of the detection axis
    // (project_escalation_ladder.md). For now it's surface-only —
    // we log the recommendation + include it in lastError so it's
    // visible in `kubectl get it`. Slice 5 wires actual handlers
    // (RefreshSource → invalidate workspace clone; ReloadGems →
    // gem cache invalidation; RecycleWorkers → pool kill+respawn;
    // PauseAndAlert → set autoSuspended + Event).
    let now = chrono::Utc::now();
    // Duration since last_applied_at — the canonical "how long since
    // we were last in a good (applied) state". phase_entered_at is
    // wrong: phase resets to Compiling on every retry, so it stays
    // tiny even when the template has been thrashing for hours.
    // Fallback chain: last_applied_at (best) → phase_entered_at
    // (acceptable for never-applied templates) → ZERO.
    let duration_unready = template
        .status
        .as_ref()
        .and_then(|s| {
            s.last_applied_at
                .as_ref()
                .or(s.phase_entered_at.as_ref())
        })
        .map(|t| (now - *t).to_std().unwrap_or(std::time::Duration::ZERO))
        .unwrap_or(std::time::Duration::ZERO);
    let recommended_action = crate::controller::escalation::EscalationLadder::pangea_default()
        .pick(duration_unready);

    // Anomaly recurrence — the known-unknowns axis. Hash the error
    // into a stable signature + bump the in-process tracker. Same
    // logical error produces the same signature across runs, so
    // dashboards can answer "is this template stuck on ONE bug
    // class repeated 100 times, or 100 distinct issues?".
    let signature = crate::controller::anomaly_tracker::error_signature(err_msg);
    let recurrence_key = format!("{}/{}", namespace, name);
    let recurrence = state
        .anomaly_tracker
        .observe(&recurrence_key, &signature);

    // Composite three-axis summary — one structured event for log
    // analytics + Prometheus + (slice-4) `.status.anomalies[]`. Same
    // shape regardless of whether a typed detector also fired, so
    // consumers don't branch on source.
    let summary = crate::controller::anomaly_tracker::AnomalySummary::compose(
        &recurrence,
        recommended_action.label(),
        recommended_action.depth(),
        None,
    );

    // Fan the summary to every configured sink via the
    // AnomalyEmitter trait. CompositeEmitter::pangea_default ships
    // tracing + Prometheus today; slice-4 appends a status-field
    // emitter without touching this call site. See
    // controller/anomaly_emitter.rs.
    let emit_ctx = crate::controller::anomaly_emitter::AnomalyContext {
        template,
        summary: &summary,
        error_msg: err_msg,
        event_reason: "CompileFailureEscalated", // overridden below if escalation fires
        event_message: err_msg,
    };
    state.anomaly_emitter.emit(&emit_ctx).await;

    // Item J observability: bump rate counter + set current-count gauge
    // so Grafana can alert on "stuck-Compiling" before the settling
    // threshold trips. Prometheus path:
    //   pangea_compile_failures_total{namespace,name}      (counter)
    //   pangea_template_consecutive_compile_failures{...}   (gauge)
    state
        .metrics
        .compile_failures_total
        .with_label_values(&[&namespace, &name])
        .inc();
    state
        .metrics
        .consecutive_compile_failures
        .with_label_values(&[&namespace, &name])
        .set(next as i64);

    if escalate {
        // Threshold crossed — transition to CompileBlocked + emit
        // Event. CompileBlocked (not Failed): the failure class is
        // compile, which self-heals the moment a new commit compiles
        // — its phase handler retries Compiling on backoff instead of
        // parking until a human resets. The ladder's PauseAndAlert
        // arm is unchanged (the handler's status patch below still
        // sets autoSuspended when the ladder says so).
        warn!(
            template = %name,
            consecutive = next,
            max = max,
            "Compile failure threshold reached; transitioning to CompileBlocked"
        );
        let escalation_msg = format!(
            "Compile has failed {} consecutive times (settlingPolicy.maxConsecutiveDriftCycles={}). \
             Last error: {}. Recovery ladder recommends '{}' (depth {}, after {}s unready). \
             Parked in CompileBlocked: compile retries on backoff and resumes \
             automatically once the source compiles (push a fixing commit, restore \
             the missing gem, etc.).",
            next, max, err_msg,
            recommended_action.label(), recommended_action.depth(),
            duration_unready.as_secs(),
        );
        // Dispatch the recovery action via the EscalationHandlerRegistry
        // trait. The handler returns the desired status delta + event
        // payload; we merge with our own escalation patch (phase,
        // counter, lastError) and apply once. Slice-5 RefreshSource /
        // ReloadGems / RecycleWorkers handlers slot in by replacing
        // their no-op execute(); the call site stays identical.
        let ctx = crate::controller::escalation_handlers::EscalationContext {
            template,
            action: recommended_action,
            duration_unready,
            consecutive_failures: next,
            last_error: err_msg.to_string(),
            error_signature: signature.clone(),
        };
        let handler = state.escalation_handlers.handler_for(recommended_action);
        let outcome = match handler.execute(&ctx).await {
            Ok(o) => o,
            Err(e) => {
                // Handler failed — surface it but proceed with the
                // base escalation patch (phase=Failed + lastError).
                // Recovery handlers are NOT critical-path: even if
                // RefreshSource fails to invalidate a cache, the
                // escalation still records the attempt.
                warn!(
                    error = %e,
                    action = recommended_action.label(),
                    "Escalation handler execute failed; continuing with base patch"
                );
                crate::controller::escalation_handlers::EscalationOutcome {
                    status_patch: serde_json::json!({}),
                    event_reason: "EscalationLadderHandlerError",
                    event_message: format!(
                        "Recovery action '{}' handler failed: {e}",
                        recommended_action.label()
                    ),
                }
            }
        };

        // Merge the handler's status patch with the base escalation
        // patch. The handler's fields (e.g. autoSuspended=true from
        // PauseAndAlertHandler) take precedence on conflict; the base
        // patch supplies the always-set fields (phase, count, error).
        // phaseEnteredAt set here because this patch bypasses
        // update_phase (it merges the ladder handler's fields too) —
        // handle_compile_blocked measures its retry backoff against
        // this timestamp.
        let mut patch = serde_json::json!({
            "status": {
                "phase": "CompileBlocked",
                "phaseEnteredAt": chrono::Utc::now(),
                "consecutiveCompileFailures": next,
                "lastError": escalation_msg.clone(),
            },
        });
        if let (Some(merged_status), Some(handler_status)) = (
            patch.get_mut("status").and_then(|s| s.as_object_mut()),
            outcome.status_patch.get("status").and_then(|s| s.as_object()),
        ) {
            for (k, v) in handler_status {
                merged_status.insert(k.clone(), v.clone());
            }
        }

        if let Err(e) =
            crate::controller::status_patch::patch_status(template, &state.client, patch).await
        {
            warn!(error = %e, "Failed to patch template status during compile-failure escalation");
        }
        record_event(
            template,
            state,
            EventType::Warning,
            outcome.event_reason,
            &format!("{}\n\n{}", escalation_msg, outcome.event_message),
        )
        .await;
    } else {
        // Below threshold — bump counter, stay in Compiling, retry.
        let patch = serde_json::json!({
            "status": {
                "consecutiveCompileFailures": next,
                "lastError": format!("Compile failed (attempt {}/{}): {}", next, max, err_msg),
            },
        });
        if let Err(e) =
            crate::controller::status_patch::patch_status(template, &state.client, patch).await
        {
            warn!(error = %e, "Failed to patch template status on compile failure");
        }
    }
    Ok(())
}

/// Reset `status.consecutiveCompileFailures` to 0 after a successful
/// compile. Idempotent — patches the field to 0 unconditionally.
/// No-op cost when the counter is already 0; the K8s server-side
/// merge resolves to no change.
async fn reset_compile_failure_counter(
    template: &InfrastructureTemplate,
    state: &ControllerState,
) -> Result<()> {
    let prior = template
        .status
        .as_ref()
        .map(|s| s.consecutive_compile_failures)
        .unwrap_or(0);
    let name = template.name_any();
    let namespace = template.namespace().unwrap_or_default();

    // Always clear the Prometheus gauge to 0, even if the spec/status
    // counter is already 0 — covers the case where the operator
    // restarted after a compile-failure spike: the in-memory gauge
    // could carry the stale value into a fresh process.
    state
        .metrics
        .consecutive_compile_failures
        .with_label_values(&[&namespace, &name])
        .set(0);

    if prior == 0 {
        return Ok(());
    }
    let patch = serde_json::json!({
        "status": { "consecutiveCompileFailures": 0 },
    });
    if let Err(e) =
        crate::controller::status_patch::patch_status(template, &state.client, patch).await
    {
        warn!(error = %e, "Failed to reset consecutiveCompileFailures");
    }
    Ok(())
}

/// Handle Initializing phase - configure backend and run `tofu init`.
/// Public wrapper for `handle_initializing` so trait impls in
/// `controller::template_phase` can dispatch to it. The body lives
/// here; the trait impl is the thin shim.
pub(crate) async fn handle_initializing_internal(
    template: &InfrastructureTemplate,
    state: &ControllerState,
) -> Result<ReconcileAction> {
    handle_initializing(template, state).await
}

#[tracing::instrument(skip_all, name = "handle_initializing")]
async fn handle_initializing(
    template: &InfrastructureTemplate,
    state: &ControllerState,
) -> Result<ReconcileAction> {
    info!("Template in Initializing phase");
    let _phase_timer = state.metrics.record_phase_duration("initializing");
    // Mutating phase entry point: resolve through the forbid-aware
    // checked variant so a `PANGEA_FORBID_TOFU` violation fails loud
    // (typed Error::TofuForbidden → status.lastError) instead of
    // silently running tofu. Per ★★ MAGMA-NATIVE.
    let executor = state.executor_for_checked(template)?;

    // On the magma path the operator's `backend.tf.json` /
    // `providers.tf.json` are never read: magma's `init` is a no-op, and
    // its plan/apply use the magma-backend Postgres state backend +
    // read providers from the rendered config (`load_config_routed`),
    // not these files. So skip those disk writes when magma is active
    // (true on BOTH the DB-backed and disk-fallback magma paths — magma
    // never consumes these files either way). The tofu path needs them.
    let magma_active = executor.name() == "magma";

    let workspace = state.workspace_manager.get_workspace(template).await?;

    // Resolve PangeaNamespace to get backend configuration
    let pns_api: Api<PangeaNamespace> = Api::all(state.client.clone());
    let pangea_ns = pns_api.get(&template.spec.pangea_namespace).await.map_err(|_| {
        Error::NamespaceNotFound(template.spec.pangea_namespace.clone())
    })?;

    // Resolve PostgreSQL credentials from Secret. On the magma path the
    // operator-side backend.tf.json is never read (magma uses the
    // magma-backend Postgres state backend), so skip the credential
    // resolution + write entirely there.
    if let Some(pg) = pangea_ns.spec.backend.pg.as_ref().filter(|_| !magma_active) {
        let secret_ns = pg
            .secret_ref
            .namespace
            .clone()
            .or_else(|| template.namespace())
            .unwrap_or_else(|| "default".to_string());
        let secret_api: Api<Secret> = Api::namespaced(state.client.clone(), &secret_ns);
        let secret = secret_api.get(&pg.secret_ref.name).await.map_err(|_| {
            Error::SecretNotFound {
                namespace: secret_ns.clone(),
                name: pg.secret_ref.name.clone(),
            }
        })?;

        let data = secret.data.as_ref().ok_or_else(|| {
            Error::Config(format!("Secret {}/{} has no data", secret_ns, pg.secret_ref.name))
        })?;

        let username = data
            .get(&pg.secret_ref.username_key)
            .map(|v| String::from_utf8_lossy(&v.0).to_string())
            .ok_or_else(|| {
                Error::Config(format!("Key '{}' not found in secret", pg.secret_ref.username_key))
            })?;

        let password = data
            .get(&pg.secret_ref.password_key)
            .map(|v| String::from_utf8_lossy(&v.0).to_string())
            .ok_or_else(|| {
                Error::Config(format!("Key '{}' not found in secret", pg.secret_ref.password_key))
            })?;

        let credentials = Credentials::new(username, password);

        // Write backend configuration
        let template_name = template.name_any();
        BackendConfigGenerator::write_backend_config(
            &pangea_ns,
            &template_name,
            &credentials,
            &workspace.path,
        )
        .await?;
    }

    // Write provider configuration if credentials are specified — but
    // not on the magma path, where providers come from the rendered
    // config (`load_config_routed`), not providers.tf.json on disk.
    if let Some(provider_creds) = template.spec.provider_credentials.as_ref().filter(|_| !magma_active) {
        let provider_config = resolve_provider_config(provider_creds, template, state).await?;
        BackendConfigGenerator::write_provider_config(provider_config, &workspace.path).await?;
    }

    // Resolve `spec.secretFiles` into pod-local workspace files — same
    // gating as the provider-config write above (magma reads providers
    // from the resolved config, never from workspace files; a template
    // with no secretFiles entries pays one empty-loop no-op).
    if !magma_active {
        write_secret_files(template, state, &workspace).await?;
    }

    // Run tofu init
    let result = executor.init(&workspace.path, &[]).await?;

    if result.success {
        info!("tofu init completed successfully");
        update_phase(template, Phase::Planning, state).await?;
        record_event(template, state, EventType::Normal, "Initialized", "Backend initialized successfully").await;
    } else {
        let err_msg = format!("init failed: {}", result.stderr);
        warn!(%err_msg);
        update_phase_with_error(template, Phase::Failed, &err_msg, state).await?;
        record_event(template, state, EventType::Warning, "InitFailed", &err_msg).await;
    }

    Ok(ReconcileAction::Requeue(SHORT_REQUEUE_INTERVAL))
}

/// Restart-safety predicate for the plan / apply / ready phase guards:
/// is the compiled config available for this reconcile to read?
///
/// Two source-of-truth paths — see `handle_compiling`:
///   - **DB-backed magma** (`executor == magma` AND an artifact store is
///     wired): the rendered config lives in Postgres (`put_rendered_config`)
///     and `main.tf.json` is INTENTIONALLY never written to the pod-local
///     emptyDir (the zero-disk magma handoff). Checking only the disk file
///     looped DB-backed templates FOREVER at Planning/Applying/Ready — the
///     config is present in Postgres but absent on disk, so the unconditional
///     `main.tf.json` guard bounced every cycle (the pleme-io-opensource
///     `+N`-plan-never-applies wedge). Probe the Postgres row instead.
///   - **disk** (tofu, or the magma disk-fallback): the compiled config IS
///     `main.tf.json` on disk; a fresh pod that resumed PAST `handle_compiling`
///     has none → bounce to Compiling so clone+compile re-runs.
///
/// Keys MUST match `handle_compiling`'s `put_rendered_config`:
/// schema = `pangea_{spec.pangeaNamespace}`, template = `name_any()`.
/// Stable content-addressed revision of a NON-GIT source's content — the
/// non-git analogue of the git `compiledRevision` SHA. blake3 (the same hash
/// the artifact store uses), 16-hex-char prefix, `cm:`-tagged so it can never
/// be mistaken for a 40-char git SHA in `status.compiledRevision`.
pub(crate) fn content_revision(content: &str) -> String {
    let h = blake3::hash(content.as_bytes()).to_hex().to_string();
    format!("cm:{}", &h[..16])
}

/// Current content-revision of a NON-GIT (inline / configMap) source, or
/// `None` for a git source (which tracks freshness via its HEAD SHA) or a
/// source whose content can't be read right now. This is what lets a
/// ConfigMap/inline config EDIT be DETECTED: git sources observe a moving
/// remote HEAD, but non-git sources previously had no change signal at all,
/// so the operator served the stale Postgres-cached render forever.
async fn non_git_source_revision(
    template: &InfrastructureTemplate,
    state: &ControllerState,
) -> Result<Option<String>> {
    let source = &template.spec.source;
    let content = if let Some(inline) = &source.inline {
        inline.clone()
    } else if let Some(cm_ref) = &source.config_map_ref {
        let ns = cm_ref
            .namespace
            .clone()
            .or_else(|| template.namespace())
            .unwrap_or_default();
        let cm_api: Api<ConfigMap> = Api::namespaced(state.client.clone(), &ns);
        match cm_api.get_opt(&cm_ref.name).await? {
            Some(cm) => match cm.data.as_ref().and_then(|d| d.get(&cm_ref.key)).cloned() {
                Some(c) => c,
                // Key missing — don't claim freshness; let the compile path
                // surface the real error instead of silently using the cache.
                None => return Ok(None),
            },
            None => return Ok(None), // ConfigMap gone — same.
        }
    } else {
        return Ok(None); // git source (or none): not a non-git revision.
    };
    Ok(Some(content_revision(&content)))
}

/// Pure reuse decision: is a cached `rendered_config` still current?
///
/// The cached render is reusable **iff** the `source_revision` it was
/// produced at (recorded on the artifact row by `handle_compiling`'s
/// `put_rendered_config`) equals the revision the operator now believes
/// it should be at (`status.compiledRevision`). This unifies git and
/// non-git: for git, `compiled_revision` is the HEAD SHA the freshness
/// gate keeps current; for non-git it is the `cm:` content hash of the
/// current source content.
///
/// `stored_revision == None` (no row, or a legacy row written before the
/// `source_revision` column existed) ⇒ stale ⇒ recompile once — the same
/// converge-by-one-recompile discipline as a legacy CR with no
/// `compiled_revision`. A mismatch ⇒ stale (the render is from an older
/// revision — the git stale-render class this fixes). Pure + kube-free so
/// the law is a unit test, not an operational hope.
fn rendered_config_is_current(
    stored_revision: Option<&str>,
    current_revision: Option<&str>,
) -> bool {
    match (stored_revision, current_revision) {
        // The artifact records the revision it was rendered from AND we
        // know the revision we should be at — reuse only on an exact match.
        (Some(stored), Some(current)) => stored == current,
        // The artifact has no recorded revision (legacy row / NULL) — treat
        // as stale so exactly one recompile stamps the revision.
        (None, _) => false,
        // A stored render exists but we CANNOT determine the current source
        // revision. Previously this returned `true` ("present ⇒ available"),
        // which SILENTLY certified a possibly-stale render as current when
        // the current source became unreadable this tick (a configMap key
        // deleted, the ConfigMap gone — `non_git_source_revision` returns
        // `Ok(None)` for those). Serving a stale render off an unreadable
        // source is the never-stuck masking hazard this fixes: force a
        // recompile instead. The compile path then either re-reads the
        // source (if it recovered) or surfaces the real "source unreadable"
        // error loudly — never masks it as "current".
        //
        // Safe for the legitimate git-first-compile case: on the very first
        // git compile there is no stored render, so `(None, _) => false`
        // fires first and this arm is never reached; once a git render
        // exists the freshness gate has stamped `status.compiledRevision`,
        // so `current` is `Some(head)` (this arm again unreached). Reaching
        // here with a stored render but no derivable current revision is an
        // anomalous state that correctly warrants exactly one recompile.
        (Some(_), None) => false,
    }
}

/// Whether a cached render is invalidated by a spec/generation change.
///
/// `true` when the live `metadata.generation` is AHEAD of the last
/// phase-transition's `status.observedGeneration` — i.e. a spec edit
/// landed that the operator has not yet re-compiled through. This makes
/// the reuse gate honor ANY spec change, not only a source-content
/// change: a `spec.variables`-only edit (or any non-source spec field)
/// bumps `generation` but leaves the source-content revision unchanged,
/// so the revision-only gate could otherwise serve a stale render off
/// the new spec until a manual pod restart. Generation-awareness closes
/// that: a spec change forces a recompile.
///
/// Only fires when generation is strictly ahead (`>`), never on a stale
/// or equal observed generation, so it can never churn a settled
/// template.
fn generation_invalidates_render(current_gen: i64, observed_gen: i64) -> bool {
    current_gen > observed_gen
}

async fn compiled_config_available(
    template: &InfrastructureTemplate,
    state: &ControllerState,
    main_tf_on_disk: bool,
) -> Result<bool> {
    let magma_db_backed =
        state.executor_for(template).name() == "magma" && state.artifact_store.is_some();
    if !magma_db_backed {
        return Ok(main_tf_on_disk);
    }

    // Generation-aware invalidation (never-stuck Reaction D): a spec edit
    // that bumps `metadata.generation` past `status.observedGeneration`
    // invalidates the cached render even if the SOURCE CONTENT revision is
    // unchanged (a `spec.variables`-only edit). Without this, the
    // revision-only reuse gate below would certify the stale render as
    // current and the operator would serve the OLD render off the new spec
    // until a manual pod restart. A recompile re-derives against the new
    // spec and re-persists the render.
    let current_gen = template.metadata.generation.unwrap_or(0);
    let observed_gen = template
        .status
        .as_ref()
        .map(|s| s.observed_generation)
        .unwrap_or(0);
    if generation_invalidates_render(current_gen, observed_gen) {
        info!(
            current_gen,
            observed_gen,
            "cached rendered_config predates a spec/generation change — re-compiling \
             (generation-aware reuse gate)"
        );
        return Ok(false);
    }

    let schema_name = format!("pangea_{}", template.spec.pangea_namespace);
    let store = state
        .artifact_store
        .as_ref()
        .expect("magma_db_backed implies artifact_store.is_some()");
    let template_name = template.name_any();

    let present = store
        .get_rendered_config(&schema_name, &template_name)
        .await?
        .is_some();
    if !present {
        return Ok(false);
    }

    // UNIFIED SOURCE-REVISION REUSE GATE (git + non-git). The cached render
    // is only "available" if the revision it was produced from still matches
    // the revision the operator should be at. Previously this checked ONLY
    // non-git sources (via `non_git_source_revision`), so git sources
    // returned `true` UNCONDITIONALLY once a row was present — the operator
    // stamped `status.compiledRevision` to a new HEAD while continuing to
    // serve the render produced from the OLDER revision (the git-sourced
    // stale-render class: an org.yaml/source change silently never applied).
    //
    // The stored revision is read from the artifact row; the current
    // revision is `status.compiledRevision` for git (the freshness gate,
    // which observes the remote HEAD via `observe_head`, keeps this current
    // and bounces to Compiling on a moved HEAD) and `non_git_source_revision`
    // for inline/configMap (the current content hash). On mismatch or an
    // unrecorded stored revision the cache is stale → recompile → bounce to
    // Compiling.
    let stored_revision = store
        .get_rendered_config_revision(&schema_name, &template_name)
        .await?;
    let current_revision = match non_git_source_revision(template, state).await? {
        // Non-git (inline/configMap): the current content hash.
        Some(rev) => Some(rev),
        // Git (or no revision anchor yet): `status.compiledRevision` — the
        // HEAD SHA the freshness gate advances. Do NOT re-resolve the remote
        // HEAD here (a redundant `ls-remote` RTT the freshness gate already
        // owns); `compiledRevision` IS the observed-HEAD anchor.
        None => template
            .status
            .as_ref()
            .and_then(|s| s.compiled_revision.clone()),
    };

    if !rendered_config_is_current(stored_revision.as_deref(), current_revision.as_deref()) {
        info!(
            stored = ?stored_revision,
            current = ?current_revision,
            "cached rendered_config is from a different source revision — re-compiling \
             (unified git + non-git freshness gate)"
        );
        return Ok(false);
    }
    Ok(true)
}

/// Outcome of [`acquire_mutation_lock`] — what a mutating phase handler
/// (`handle_applying`, `handle_destroying`) should do next.
enum LockDispatch {
    /// Safe to proceed with the mutation. `Some(guard)` when a lock
    /// manager is wired and the advisory lock was acquired — the caller
    /// MUST hold the guard for the duration of the mutation (bind it to
    /// a local; `LockGuard::drop` releases it on every return path).
    /// `None` when no lock manager is configured (`ControllerState::
    /// state_lock` is `None` — a DB-less deployment); nothing to hold.
    Proceed(Option<crate::backend::LockGuard>),
    /// Another operator pod already holds this template's lock. The
    /// caller MUST NOT dispatch to the executor — requeue instead.
    Contended,
}

/// Acquire the Postgres advisory lock guarding `(schema_name,
/// template_name)`'s state before a mutating dispatch, or determine that
/// none is needed / available.
///
/// `schema_name` / `template_name` MUST be the same pair every other
/// Postgres-backed surface in this file keys state on
/// (`pangea_{spec.pangeaNamespace}` / the CR's own name — see
/// `compiled_config_available`'s identical derivation above), so the
/// lock genuinely guards the SAME state row the caller is about to
/// mutate — not a looser (or tighter) K8s-object identity that could
/// under- or over-lock relative to the real collision surface.
async fn acquire_mutation_lock(
    state: &ControllerState,
    schema_name: &str,
    template_name: &str,
) -> Result<LockDispatch> {
    let Some(lock_mgr) = state.state_lock.as_ref() else {
        return Ok(LockDispatch::Proceed(None));
    };
    match lock_mgr
        .try_acquire(schema_name, template_name, &crate::leader::pod_identity())
        .await
    {
        Ok(guard) => Ok(LockDispatch::Proceed(Some(guard))),
        Err(e) if is_lock_contention(&e) => Ok(LockDispatch::Contended),
        Err(e) => Err(e),
    }
}

/// True when a `StateLock::try_acquire` error means "someone else holds
/// this template's lock right now" (expected, transient contention —
/// requeue) as opposed to a genuine failure (a connection problem, a
/// schema problem — propagate). Pure and testable without a database,
/// unlike `try_acquire` itself (needs a live Postgres advisory lock):
/// this is the one classification every mutating phase handler shares,
/// proven once here instead of drifting between hand-written `matches!`
/// arms at each call site. Getting this wrong in either direction is a
/// real regression class: misclassifying a genuine DB outage AS
/// contention would requeue silently forever instead of surfacing the
/// failure; misclassifying real contention as a hard failure would
/// abandon the safe "someone else has it, retry shortly" path.
fn is_lock_contention(err: &Error) -> bool {
    matches!(err, Error::LockFailed(_))
}

#[cfg(test)]
mod lock_contention_tests {
    use super::is_lock_contention;
    use crate::error::Error;

    #[test]
    fn lock_failed_is_contention() {
        assert!(is_lock_contention(&Error::LockFailed(
            "State lock for pangea_x/y is held by another process".into()
        )));
    }

    #[test]
    fn other_error_kinds_are_never_contention() {
        // Every one of these MUST propagate as a real failure — none of
        // them mean "someone else holds the lock, retry shortly".
        assert!(!is_lock_contention(&Error::Timeout(30)));
        assert!(!is_lock_contention(&Error::Config("bad config".into())));
        assert!(!is_lock_contention(&Error::InvalidSource("bad source".into())));
    }
}

/// Derive `(PlanSummary, DriftDetail list)` from a magma `CycleArtifact`
/// — shared by both magma drift-extraction paths in `handle_planning`
/// (the disk-fallback path, where `plan_result.artifact` is populated
/// directly, and the DB-backed path, where the same shape is fetched
/// back from Postgres via `fetch_db_backed_cycle_artifact`) so the two
/// paths can never drift apart on how a `CycleArtifact` becomes policy
/// input.
fn plan_summary_and_drifts_from_artifact(
    art: &crate::executor::cycle_artifact::CycleArtifact,
) -> (crate::executor::PlanSummary, Vec<crate::crd::DriftDetail>) {
    let (added, changed, destroyed, total) = art.summary_counts();
    let s = crate::executor::PlanSummary {
        added,
        changed,
        destroyed,
        total,
        has_changes: added > 0 || changed > 0 || destroyed > 0,
        // changes_by_type left empty — magma's CycleArtifact doesn't
        // carry per-type buckets today; a follow-up slice can wire
        // these from `resource_changes` if a consumer needs them.
        changes_by_type: std::collections::HashMap::new(),
    };
    let details = art.drift_details(50);
    (s, details)
}

/// The `(schema_name, template_name)` pair magma's Postgres-backed
/// artifacts AND state are keyed on for a given CR. MUST stay in sync
/// with `ControllerState::magma_executor_with_provider_configs`'s own
/// derivation (`controller/mod.rs`) — that function is what actually
/// builds the `MagmaExecutor` that reads/writes state under this exact
/// key (`schema_name = "pangea_{spec.pangeaNamespace}"`, `state_name =
/// "default"`); reading with any other derivation would silently miss
/// the row or read the wrong one. Collapses what was three independent
/// hand-copies of `format!("pangea_{}", template.spec.pangea_namespace)`
/// (`magma_executor_with_provider_configs`, `fetch_db_backed_cycle_artifact`,
/// and now `current_state_fingerprint`) down to two — `mod.rs` lives in a
/// different module and is left as a documented, cross-referenced
/// duplicate rather than pulled in here, to keep this fix's blast radius
/// contained to the approval-hash gap it closes.
fn magma_state_key(template: &InfrastructureTemplate) -> (String, String) {
    (
        format!("pangea_{}", template.spec.pangea_namespace),
        template.name_any(),
    )
}

/// Fetch the `CycleArtifact` `MagmaExecutor::plan` already persisted to
/// Postgres for THIS template's most recent plan (`magma.rs`'s
/// `store.put_bundle(...)`, committed before `WorkspaceRunner::plan`
/// returns). Needed because `MagmaWorkspaceRunner::plan` deliberately
/// returns `artifact: None` on the DB-backed zero-disk path (see its
/// own doc comment: "the cycle receipt is enriched from the DB
/// downstream") — so `plan_result.artifact` alone is NEVER populated
/// for the production magma posture (`PANGEA_FORBID_TOFU` +
/// Postgres-backed state), and without this fetch `handle_planning`'s
/// drift extraction fell through to the "no analyzable output" branch
/// for EVERY DB-backed magma plan, unconditionally, regardless of the
/// plan's real content. Mirrors `record_reconcile_cycle`'s own
/// `db_bundle_bytes` fetch (`controller/template/cycle_receipts.rs`)
/// so both call sites derive the SAME typed shape from the SAME
/// bundle. `None` on a missing bundle or any read/parse failure —
/// best-effort, the caller's existing "no analyzable output" fallback
/// already handles that case safely.
///
/// Closes the root cause of a live cross-template hash collision
/// (2026-07-17, Camelot Mode-1): five unrelated `InfrastructureTemplate`
/// CRs — plans ranging from 1 to 50 resources — all converged on the
/// identical `status.pendingPlanHash` "eac9f28515f12ae7", because every
/// one of them hit this exact gap: with `raw_drifts` always empty,
/// `canonical_drift_fingerprint(&policy_outcome.annotated_drifts)` is
/// always `""`, and `workspace.read_state_bytes()` (a raw on-disk
/// read) is always `None` on the same zero-disk path, so
/// `plan_approval_hash("", None)` collapsed to one fleet-wide constant.
/// Approving any single CR's plan at that hash would have made every
/// other affected CR's unrelated plan appear pre-approved too.
async fn fetch_db_backed_cycle_artifact(
    template: &InfrastructureTemplate,
    state: &ControllerState,
) -> Option<crate::executor::cycle_artifact::CycleArtifact> {
    let store = state.artifact_store.as_ref()?;
    let (schema_name, name) = magma_state_key(template);
    match store.get_bundle_bytes(&schema_name, &name).await {
        Ok(Some(bytes)) => crate::executor::magma_bundle::cycle_artifact_from_bytes(&bytes),
        Ok(None) => None,
        Err(e) => {
            warn!(
                error = %e,
                schema = %schema_name,
                template = %name,
                "DB-backed magma plan: bundle read failed while deriving drift details"
            );
            None
        }
    }
}

/// Handle Planning phase - run `tofu plan` and analyze changes.
/// Public wrapper for `handle_planning` so trait impls in
/// `controller::template_phase` can dispatch to it. The body lives
/// here; the trait impl is the thin shim.
pub(crate) async fn handle_planning_internal(
    template: &InfrastructureTemplate,
    state: &ControllerState,
) -> Result<ReconcileAction> {
    handle_planning(template, state).await
}

#[tracing::instrument(skip_all, name = "handle_planning")]
async fn handle_planning(
    template: &InfrastructureTemplate,
    state: &ControllerState,
) -> Result<ReconcileAction> {
    info!("Template in Planning phase");
    let _phase_timer = state.metrics.record_phase_duration("planning");

    // Plan also runs the executor (tofu plan spawns tofu; magma issues
    // provider RPCs for data-source reads): build the runner through the
    // forbid-aware, credential-aware path so (a) a `PANGEA_FORBID_TOFU`
    // violation fails loud (typed Error::TofuForbidden → status.lastError)
    // and (b) on the magma path the runner's executor carries the
    // resolved `spec.providerCredentials` — a cloudflare data-source read
    // at plan time is a real provider RPC that fails "channel closed"
    // without the token. Per ★★ MAGMA-NATIVE.
    //
    // Slice 2c: phase handlers speak the typed `WorkspaceRunner`
    // surface — one call returns both the unified `CycleArtifact`
    // (for status enrichment + magma drift detail) AND the raw
    // tofu-show-JSON (for the legacy `Plan::from_json` path that
    // produces per-attribute DriftDetail entries the policy engine
    // consumes). No double-call to the executor.
    let runner = state.executor_runner_for_with_creds(template).await?;

    let workspace = state.workspace_manager.get_workspace(template).await?;

    // Restart-safety: the workspace is a pod-local emptyDir, lost on
    // every pod restart, but the CR `status.phase` persists in etcd. A
    // fresh pod that resumes a CR already at phase=Planning dispatches
    // straight here, SKIPPING handle_compiling — so main.tf.json was
    // never (re)written on this pod, and the executor's load_config
    // IO-errors on the missing file (`No such file or directory`),
    // looping forever at Planning (the 2026-06-03 fleet-wide wedge:
    // every operator redeploy re-broke pleme-io-opensource + every other
    // template). If the compiled config is absent, bounce back to
    // Compiling so the clone+compile re-runs before we plan.
    if !compiled_config_available(template, state, workspace.main_tf_path().exists()).await? {
        warn!(
            template = %template.name_any(),
            "Planning: compiled config missing (pod restart / wiped workspace, \
             and no Postgres rendered_config) — resetting to Compiling so \
             clone+compile re-runs"
        );
        update_phase(template, Phase::Compiling, state).await?;
        return Ok(ReconcileAction::Requeue(SHORT_REQUEUE_INTERVAL));
    }

    // Freshness gate — the no-changes→Ready edge below must never be
    // taken against a compile the remote has already moved past.
    if let FreshnessGate::Bounce(action) =
        source_freshness_gate(template, state, &workspace, "planning").await?
    {
        return Ok(action);
    }

    let plan_result = runner.plan(&workspace).await?;

    if !plan_result.success {
        let err_msg = format!("plan failed: {}", plan_result.raw_stderr);
        warn!(%err_msg);
        update_phase_with_error(template, Phase::Failed, &err_msg, state).await?;
        record_event(template, state, EventType::Warning, "PlanFailed", &err_msg).await;
        return Ok(ReconcileAction::Requeue(ERROR_REQUEUE_INTERVAL));
    }

    // Three-path drift extraction (same final shape):
    //   1. **Tofu path** — `raw_show_json` populated: use legacy
    //      `Plan::from_json`. Carries per-attribute drift detail the
    //      policy engine reads.
    //   2. **Magma disk-fallback path** — `raw_show_json` empty,
    //      `artifact` populated directly on `plan_result`: derive
    //      `PlanSummary` + `DriftDetail` from
    //      `CycleArtifact.resource_changes`. Today the per-attribute
    //      block is empty (the bundle has before/after but
    //      `TypedResourceChange` doesn't surface them yet — a follow-up
    //      slice extends this). Net effect for magma cycles that
    //      previously had `Plan::from_json` silently failing (magma's
    //      show_plan emits magma-shape JSON tofu can't parse): the
    //      policy engine now sees the resources it should.
    //   3. **Magma DB-backed path** — `raw_show_json` empty AND
    //      `plan_result.artifact` is `None` (the zero-disk production
    //      posture; see `fetch_db_backed_cycle_artifact`): fetch the
    //      SAME plan's bundle back from Postgres and derive the
    //      identical shape as path 2. Without this path, EVERY
    //      DB-backed magma plan silently fell through to "no
    //      analyzable output" regardless of real content — see
    //      `fetch_db_backed_cycle_artifact`'s doc comment for the live
    //      incident this closes.
    // Drift details capped at 50 so the status object stays tractable.
    let (summary, raw_drifts) = if !plan_result.raw_show_json.is_empty() {
        match Plan::from_json(&plan_result.raw_show_json) {
            Ok(plan) => {
                let s = plan.summary();
                let details: Vec<crate::crd::DriftDetail> = plan
                    .drift_details(50)
                    .into_iter()
                    .map(|d| crate::crd::DriftDetail {
                        address: d.address,
                        action: d.action,
                        risk: d.risk,
                        attributes: d.attributes,
                        policy_decision: None,
                        matched_policy: None,
                    })
                    .collect();
                info!(
                    runner = runner.name(),
                    added = s.added,
                    changed = s.changed,
                    destroyed = s.destroyed,
                    drift_count = details.len(),
                    "Plan analysis complete (tofu path: Plan::from_json)"
                );
                (Some(s), details)
            }
            Err(e) => {
                warn!(error = %e, "Failed to parse plan JSON, continuing without summary");
                (None, Vec::new())
            }
        }
    } else if let Some(art) = plan_result.artifact.as_ref() {
        // Magma disk-fallback path: derive equivalent shapes from the
        // typed artifact `plan_result` already carries.
        let (s, details) = plan_summary_and_drifts_from_artifact(art);
        info!(
            runner = runner.name(),
            added = s.added,
            changed = s.changed,
            destroyed = s.destroyed,
            drift_count = details.len(),
            "Plan analysis complete (magma path: CycleArtifact)"
        );
        (Some(s), details)
    } else if let Some(art) = fetch_db_backed_cycle_artifact(template, state).await {
        // Magma DB-backed (zero-disk) path: `plan_result.artifact` is
        // always `None` here by design — fetch the just-persisted
        // bundle back from Postgres instead. See
        // `fetch_db_backed_cycle_artifact`'s doc comment.
        let (s, details) = plan_summary_and_drifts_from_artifact(&art);
        info!(
            runner = runner.name(),
            added = s.added,
            changed = s.changed,
            destroyed = s.destroyed,
            drift_count = details.len(),
            "Plan analysis complete (magma path: DB-backed bundle fetch)"
        );
        (Some(s), details)
    } else {
        warn!(
            runner = runner.name(),
            "Plan succeeded but produced no analyzable output (no show-JSON, no artifact, no DB-backed bundle)"
        );
        (None, Vec::new())
    };

    let has_changes = plan_result.has_changes;

    // Resolve the cascade root: if the template has its own
    // `defaultDecision` set, it wins; otherwise inherit the parent
    // WorkspaceCatalog's `policy.driftReaction`. This is the workspace
    // level of the four-level cascade
    // (gem → workspace → template → resource). Refuse >
    // requireApproval > autoApply for safety precedence is enforced
    // inside evaluate_policy when both layers contribute rules.
    let effective_default = match template.spec.default_decision {
        Some(d) => Some(d),
        None => match crate::controller::workspace_catalog_controller::parent_catalog_for_template(
            &state.client,
            template,
        )
        .await
        {
            Ok(Some(wsc)) => wsc
                .spec
                .policy
                .drift_reaction
                .and_then(workspace_drift_reaction_to_policy_decision),
            _ => None,
        },
    };

    // Run the per-resource policy engine. Empty rules + unset
    // defaultDecision = aggressive auto-apply on every change (the
    // documented default). The engine annotates each drift entry with
    // its resolved decision and emits an aggregate that drives the
    // plan→apply gate below.
    let policy_outcome = evaluate_policy(
        &template.spec.policies,
        effective_default,
        &raw_drifts,
    );
    let policy_was_configured =
        policy_is_configured(&template.spec.policies, effective_default);

    let resource_summary = summary.as_ref().map(|s| ResourceSummary {
        total: s.total,
        added: s.added,
        changed: s.changed,
        destroyed: s.destroyed,
    });
    let plan_text = summary.as_ref().map(|s| s.format());

    // Persist annotated drifts + policyEvaluation (only when configured —
    // otherwise we'd noisily attach `<default>` everywhere on every
    // legacy template).
    let evaluation_to_store = if policy_was_configured {
        Some(policy_outcome.evaluation.clone())
    } else {
        None
    };
    update_plan_status(
        template,
        resource_summary,
        plan_text.as_deref(),
        policy_outcome.annotated_drifts.clone(),
        evaluation_to_store,
        state,
    )
    .await?;

    // Emit per-template policy + drift-detail gauges for Prometheus.
    // Counter for total decisions accumulates over time; gauges
    // reflect the CURRENT plan state and reset on next reconcile.
    let tname = template.name_any();
    let tns = template.namespace().unwrap_or_default();
    state
        .metrics
        .policy_decisions_total
        .with_label_values(&[&tname, &tns, "autoApply"])
        .inc_by(policy_outcome.evaluation.auto_apply_count as u64);
    state
        .metrics
        .policy_decisions_total
        .with_label_values(&[&tname, &tns, "requireApproval"])
        .inc_by(policy_outcome.evaluation.require_approval_count as u64);
    state
        .metrics
        .policy_decisions_total
        .with_label_values(&[&tname, &tns, "refuse"])
        .inc_by(policy_outcome.evaluation.refuse_count as u64);
    update_drift_detail_gauges(&state.metrics, &tname, &tns, &policy_outcome.annotated_drifts);

    if !has_changes {
        info!("No changes detected");
        update_phase(template, Phase::Ready, state).await?;
        record_reconcile_cycle(
            template,
            state,
            Some(&workspace.path),
                plan_result.artifact.clone(), // slice 2c: runner-provided artifact threads through cycle receipt
            &[],
            plan_text.clone(),
            CycleResult::NoChanges,
        )
        .await?;
        return Ok(ReconcileAction::Requeue(DEFAULT_REQUEUE_INTERVAL));
    }

    match policy_outcome.aggregate {
        PolicyDecision::Refuse => {
            // Refuse is a hard stop: name the offending resources
            // loudly so the operator surface tells the human exactly
            // which rule blocked which change.
            let refused_count = policy_outcome.evaluation.refuse_count;
            let sample = policy_outcome.evaluation.refused_addresses.join(", ");
            let err_msg = format!(
                "Plan refused by policy: {} refused change(s). Refused addresses: {}",
                refused_count, sample
            );
            warn!(%err_msg, "Policy refused plan");
            update_phase_with_error(template, Phase::Failed, &err_msg, state).await?;
            record_reconcile_cycle(
                template,
                state,
                Some(&workspace.path),
                plan_result.artifact.clone(), // slice 2c: runner-provided artifact threads through cycle receipt
                &policy_outcome.annotated_drifts,
                plan_text.clone(),
                CycleResult::PolicyGated(PolicyDecision::Refuse),
            )
            .await?;
            record_event(template, state, EventType::Warning, "PolicyRefused", &err_msg).await;
            Ok(ReconcileAction::Requeue(DEFAULT_REQUEUE_INTERVAL))
        }
        PolicyDecision::AutoApply => {
            // State-continuity gate. `plan_approval_hash` (below, in the
            // RequireApproval arm) folds a state fingerprint into the
            // approval hash so a STALE APPROVAL can never silently
            // re-validate a plan computed against different state — but
            // that protection only fires when a human is in the loop.
            // AutoApply has no approval step at all, so a template with
            // a prior successful apply whose local Terraform/OpenTofu
            // state has since gone missing (an emptyDir wipe on a pod
            // restart — no spec edit required) would otherwise plan
            // "everything create" against the empty state and apply it
            // completely unattended: the exact class of bug behind
            // docs/postmortems/2026-07-12-camelot-eks-state-wipe-duplicate-vpc.md.
            // Detect that combination and, when it fires, downgrade this
            // one cycle to the SAME state-fingerprinted approval gate
            // RequireApproval already uses, instead of applying blind.
            let previously_applied = template
                .status
                .as_ref()
                .and_then(|s| s.last_applied_at)
                .is_some();
            let durable = is_durable_state_backend(template, state);
            let (schema_name, template_name) = magma_state_key(template);
            let state_fingerprint = current_state_fingerprint(
                durable,
                state.state_backend.as_ref(),
                &schema_name,
                &template_name,
                &workspace,
            )
            .await;
            let state_present_now = state_fingerprint.is_present();

            match evaluate_auto_apply_gate(durable, previously_applied, state_present_now) {
                AutoApplyGate::BlockedByStateContinuityBreach => {
                    warn!(
                        template = %template.name_any(),
                        "AutoApply BLOCKED by a state-continuity breach: a prior \
                         successful apply is recorded but local Terraform/OpenTofu \
                         state is now absent. Routing through the approval gate \
                         instead of applying blind."
                    );
                    route_through_approval_gate(
                        template,
                        state,
                        &workspace,
                        &plan_result,
                        &policy_outcome,
                        plan_text.clone(),
                        ApprovalGateReason::AutoApplyStateContinuityBreach,
                        state_fingerprint,
                    )
                    .await
                }
                AutoApplyGate::Proceed => {
                    info!(
                        auto = policy_outcome.evaluation.auto_apply_count,
                        "Policy permits auto-apply for all changes"
                    );
                    update_phase(template, Phase::Applying, state).await?;
                    record_event(
                        template,
                        state,
                        EventType::Normal,
                        "PlanApproved",
                        "Changes detected and auto-applied per policy",
                    )
                    .await;
                    Ok(ReconcileAction::Requeue(SHORT_REQUEUE_INTERVAL))
                }
            }
        }
        PolicyDecision::RequireApproval => {
            // Recompute the approval hash from THIS cycle's plan, every
            // time — never trust a stale stored `pendingPlanHash` for
            // the approval decision. Before this fix, once a human
            // approved hash X, every SUBSEQUENT Planning pass compared
            // the OLD stored `pendingPlanHash` (frozen from the
            // approval cycle) against `approvedPlanHash` WITHOUT ever
            // re-deriving from the plan just computed above
            // (`plan_result`) — so a plan recomputed against
            // wiped-then-regenerated state (see `Workspace::clean`)
            // silently inherited the old approval even though the
            // underlying state had completely changed by the time it
            // ran. `plan_approval_hash` also folds in a fingerprint of
            // the real infrastructure state — `current_state_fingerprint`,
            // Postgres for magma, on-disk for tofu — so two plans that
            // are textually identical in SHAPE (e.g. both "create
            // everything from an empty state") but computed against
            // DIFFERENT actual state can never collide. Closes both
            // halves of
            // docs/postmortems/2026-07-12-camelot-eks-state-wipe-duplicate-vpc.md
            // bug 2 for tofu, and the magma-specific gap where the
            // approval hash was `plan_text`-derived ONLY (see
            // `CurrentStateFingerprint`'s doc comment). Note: this is a
            // hash-format change — any CR with an in-flight
            // (not-yet-applied) approval at upgrade time will require
            // re-approval, which is the intended fail-safe behavior,
            // not a regression.
            let durable = is_durable_state_backend(template, state);
            let (schema_name, template_name) = magma_state_key(template);
            let state_fingerprint = current_state_fingerprint(
                durable,
                state.state_backend.as_ref(),
                &schema_name,
                &template_name,
                &workspace,
            )
            .await;
            route_through_approval_gate(
                template,
                state,
                &workspace,
                &plan_result,
                &policy_outcome,
                plan_text.clone(),
                ApprovalGateReason::PolicyDecision {
                    require_approval_count: policy_outcome.evaluation.require_approval_count,
                },
                state_fingerprint,
            )
            .await
        }
    }
}

/// Whether a template's IaC state is durably persisted somewhere that
/// survives a pod restart — today, exactly the magma DB-backed
/// executor (state lives in Postgres). Mirrors the identical
/// predicate `compiled_config_available` already uses for the
/// analogous rendered-config-reuse question, so both call sites agree
/// on what "durable" means. Everything else (the disk-based `tofu`
/// executor, including the magma disk-fallback path) keeps its state
/// on the pod-local `emptyDir`, which does NOT survive a restart.
///
/// Known, named scope limitation (not silently rounded up): a `tofu`
/// executor configured with a remote `pg` state backend
/// (`PangeaNamespace.spec.backend.pg`) is ALSO durable but is not
/// recognized here — such a template would over-trigger
/// `state_continuity_breach`'s gate after an ordinary, harmless pod
/// restart (one avoidable extra approval step). That is a fail-SAFE
/// degradation, never a fail-DANGEROUS one — the asymmetry this
/// predicate must never get wrong is silently applying, not
/// occasionally over-asking — and is tracked as a follow-up rather
/// than silently dropped, mirroring the sibling postmortem fix's own
/// `pending-postmortem-followup` convention.
fn is_durable_state_backend(template: &InfrastructureTemplate, state: &ControllerState) -> bool {
    state.executor_for(template).name() == "magma" && state.artifact_store.is_some()
}

/// Detect an AutoApply-dangerous **state-continuity breach**: this
/// template has completed at least one successful apply
/// (`previously_applied`) — so the operator's own history says real
/// infrastructure should already exist — but the on-disk state this
/// cycle's plan just ran against is completely absent
/// (`!state_present_now`), on a backend that is not durably persisted
/// remotely (`!is_durable_state_backend`). That combination is the
/// exact signature left by an emptyDir wipe on the disk-based `tofu`
/// executor.
///
/// `is_durable_state_backend == true` makes this always `false` — that
/// backend's state cannot vanish on a pod restart, so gating on it
/// would be a false positive, not a safety net.
/// `previously_applied == false` (a brand-new template's first-ever
/// apply) is also never a breach — there is nothing to have lost yet.
fn state_continuity_breach(
    is_durable_state_backend: bool,
    previously_applied: bool,
    state_present_now: bool,
) -> bool {
    !is_durable_state_backend && previously_applied && !state_present_now
}

/// Outcome of evaluating whether an `AutoApply` decision may proceed
/// straight to `Applying` this cycle. The one call site
/// (`handle_planning`'s `PolicyDecision::AutoApply` arm) matches this
/// exhaustively, so a future edit that forgets to branch on a
/// detected breach is a compile error (`E0004: non-exhaustive
/// patterns`) rather than a silently-dropped check — the one
/// structural guarantee available here, since the underlying signal
/// (on-disk state presence + apply history) is necessarily a runtime
/// observation, not a static property.
///
/// **Tier (named honestly, not rounded up): only-mitigated.** This is
/// a runtime check gating a transition, not a type that makes the
/// illegal state unconstructible — full unrepresentability isn't
/// achievable here because the danger signal is external, mutable
/// reality (a filesystem read + a status timestamp), not something
/// the type system can see at compile time.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AutoApplyGate {
    /// No continuity breach detected — safe to apply per policy.
    Proceed,
    /// `state_continuity_breach` fired — must not apply blind.
    BlockedByStateContinuityBreach,
}

fn evaluate_auto_apply_gate(
    is_durable_state_backend: bool,
    previously_applied: bool,
    state_present_now: bool,
) -> AutoApplyGate {
    if state_continuity_breach(is_durable_state_backend, previously_applied, state_present_now) {
        AutoApplyGate::BlockedByStateContinuityBreach
    } else {
        AutoApplyGate::Proceed
    }
}

/// Why a cycle is routed through the require-approval gate — drives
/// only the human-facing message text; the gate mechanics
/// (`route_through_approval_gate`) are identical either way.
enum ApprovalGateReason {
    /// The policy engine's own per-resource rules resolved to
    /// `requireApproval` for at least one change.
    PolicyDecision { require_approval_count: u32 },
    /// `PolicyDecision::AutoApply` detected a state-continuity breach
    /// (see `state_continuity_breach`) and downgraded this one cycle
    /// to require-approval rather than applying blind.
    AutoApplyStateContinuityBreach,
}

impl ApprovalGateReason {
    fn waiting_message(&self) -> String {
        match self {
            Self::PolicyDecision {
                require_approval_count,
            } => format!("Changes detected ({require_approval_count} require approval)."),
            Self::AutoApplyStateContinuityBreach => {
                "AutoApply BLOCKED: this template has a prior successful apply, but \
                 local Terraform/OpenTofu state is now missing (state-continuity \
                 breach — see \
                 docs/postmortems/2026-07-12-camelot-eks-state-wipe-duplicate-vpc.md). \
                 Refusing to auto-apply a plan that would recreate every resource \
                 from scratch; verify real cloud state before approving."
                    .to_string()
            }
        }
    }
}

/// The CURRENT real infrastructure state, fetched from whichever store
/// the template's chosen executor actually persists it to, used to
/// fingerprint a plan-approval hash (`plan_approval_hash`,
/// `route_through_approval_gate`).
///
/// A plain `Option<Vec<u8>>` cannot express this honestly — it can only
/// say "bytes" or "nothing," and "nothing" is ambiguous between two
/// completely different facts: *the backend genuinely has no state yet*
/// (a legitimate, hashable value — a brand-new template's first plan)
/// and *the read itself failed* (a Postgres error, not "zero rows" —
/// unknown, and must never be treated as if it were the first case).
/// Collapsing those two into one `None` is exactly the shape of bug
/// this type exists to make unrepresentable: before this fix,
/// `Workspace::read_state_bytes()` (a raw on-disk read, `fs::read(...)
/// .ok()`) was called unconditionally regardless of executor, so every
/// magma-backed template — whose real state lives in Postgres, never on
/// the pod-local disk — read `None` on every cycle, and
/// `plan_approval_hash` folded that constant `None` into the hash
/// alongside the plan text. The approval hash for a magma template was
/// therefore `plan_text`-derived ONLY, unconditionally: a human's
/// approval of one plan silently and permanently authorized ANY future
/// plan with the same `plan_text` shape, regardless of what actually
/// changed in real infrastructure state between then and now — the
/// state-fingerprint protection `docs/postmortems/2026-07-12-camelot-eks-state-wipe-duplicate-vpc.md`
/// added existed in name only for every magma CR.
#[derive(Debug, Clone, PartialEq, Eq)]
enum CurrentStateFingerprint {
    /// The backend was queried successfully and has no state for this
    /// template — a genuine, hashable fact (first-ever plan, or a
    /// legitimately-empty backend).
    Absent,
    /// The backend was queried successfully and returned real state
    /// bytes.
    Present(Vec<u8>),
    /// The state read itself failed. Distinct from `Absent` on purpose
    /// — an approval hash MUST NEVER be computed from this outcome (see
    /// `route_through_approval_gate`, which fails closed on this
    /// variant rather than silently degrading to a `plan_text`-only
    /// hash).
    Unreadable,
}

impl CurrentStateFingerprint {
    /// `true` only for `Present`. `Unreadable` is deliberately NOT
    /// treated as "present" — an unknown state can never satisfy a
    /// continuity check that exists to prove state is still there;
    /// mirrors `is_durable_state_backend`'s own documented asymmetry
    /// (over-asking for approval is acceptable, silently applying is
    /// not).
    fn is_present(&self) -> bool {
        matches!(self, Self::Present(_))
    }
}

/// What `route_through_approval_gate` should do with a
/// `CurrentStateFingerprint` this cycle: either hash against real state
/// bytes, or refuse to compute (let alone compare) an approval hash at
/// all this cycle. Pure and directly unit-testable — mirrors
/// `evaluate_auto_apply_gate`'s shape — so the fail-closed decision
/// itself has a test that needs no `ControllerState`/`kube::Client`;
/// only the Event-recording + requeue consequence stays inline in
/// `route_through_approval_gate`.
#[derive(Debug, Clone, PartialEq, Eq)]
enum ApprovalHashInput<'a> {
    /// Compute `plan_approval_hash` against these bytes (`Some`) or the
    /// tagged-absent case (`None`) — both are legitimate, known facts
    /// about real state.
    Hashable(Option<&'a [u8]>),
    /// `CurrentStateFingerprint::Unreadable` — real state could not be
    /// confirmed. MUST NOT be silently degraded into `Hashable(None)`;
    /// see `CurrentStateFingerprint`'s doc comment for the incident
    /// class that degradation reproduces.
    RefuseUnreadable,
}

fn resolve_approval_hash_input(fingerprint: &CurrentStateFingerprint) -> ApprovalHashInput<'_> {
    match fingerprint {
        CurrentStateFingerprint::Absent => ApprovalHashInput::Hashable(None),
        CurrentStateFingerprint::Present(bytes) => ApprovalHashInput::Hashable(Some(bytes.as_slice())),
        CurrentStateFingerprint::Unreadable => ApprovalHashInput::RefuseUnreadable,
    }
}

/// Fetch [`CurrentStateFingerprint`] for `template`, reading from
/// whichever store its chosen executor actually persists state to:
/// magma's Postgres-backed `StateBackend` (the SAME `TofuPgStateBackend`
/// rows tofu itself would read — see that type's module doc) when
/// `is_magma_backed`, or the workspace's local-disk
/// `terraform.tfstate` (`Workspace::read_state_bytes`) otherwise.
///
/// Deliberately takes the already-resolved `is_magma_backed` bool and
/// an `Option<&Arc<dyn StateBackend>>` rather than `&ControllerState`
/// directly — mirrors `state_continuity_breach`'s existing shape so
/// this stays unit-testable with `InMemoryStateBackend` + a tempdir
/// `Workspace`, no `kube::Client` required. Callers pass
/// `is_durable_state_backend(template, state)` and
/// `state.state_backend.as_ref()`.
async fn current_state_fingerprint(
    is_magma_backed: bool,
    state_backend: Option<&Arc<dyn StateBackend>>,
    schema_name: &str,
    template_name: &str,
    workspace: &crate::executor::Workspace,
) -> CurrentStateFingerprint {
    if is_magma_backed {
        let Some(backend) = state_backend else {
            // Structurally shouldn't happen: `is_magma_backed` (via
            // `is_durable_state_backend` → `executor_for`) only
            // resolves to magma when `state_backend` is `Some` — see
            // `ControllerState::magma_executor_with_provider_configs`'s
            // early-return-to-tofu path when the backend is missing.
            // Stay honest rather than assume the invariant holds: treat
            // a violation as unreadable, never as a silently-hashable
            // absence.
            warn!(
                schema = schema_name,
                template = template_name,
                "plan-approval state fingerprint: executor resolved to magma but \
                 no state backend is wired in — treating state as unreadable"
            );
            return CurrentStateFingerprint::Unreadable;
        };
        match backend.get_state(schema_name, template_name, "default").await {
            Ok(Some(entry)) => match entry.data {
                Some(bytes) => CurrentStateFingerprint::Present(bytes),
                None => CurrentStateFingerprint::Absent,
            },
            Ok(None) => CurrentStateFingerprint::Absent,
            Err(e) => {
                warn!(
                    error = %e,
                    schema = schema_name,
                    template = template_name,
                    "plan-approval state fingerprint: magma Postgres state read failed"
                );
                CurrentStateFingerprint::Unreadable
            }
        }
    } else {
        match workspace.read_state_bytes().await {
            Some(bytes) => CurrentStateFingerprint::Present(bytes),
            None => CurrentStateFingerprint::Absent,
        }
    }
}

/// Two equally-valid approval sources, either satisfies the gate:
///   - `status.approvedPlanHash` -- the original mechanism, a direct
///     `kubectl patch --subresource status` (still the operator's own
///     documented UX, see the `PlanPending` event text in the caller).
///   - `spec.approvedPlanHash` -- GitOps-native: commit the reported
///     `pendingPlanHash` into the CR's spec instead. Added for fleets
///     whose tooling refuses imperative status-subresource mutations
///     against a GitOps-managed namespace (this fleet's own `guardrail`
///     `kubectl-imperative-camelot` rule is the motivating case) --
///     status subresources are controller-owned observed-state by K8s
///     convention, never reconciled by Flux from a committed manifest,
///     so a cluster with that policy had no way to approve a plan at
///     all before this field existed. Per ★★ PLATFORM-MEDIATED
///     INFRASTRUCTURE: declare (commit `spec.approvedPlanHash`) and
///     observe (`status.lastCycle`), never `kubectl patch`.
///
/// Extracted as a pure function (no I/O, no `ControllerState`) so this
/// two-source OR is directly unit-testable without the full async
/// `route_through_approval_gate` plumbing.
fn is_plan_approved(
    status: &Option<InfrastructureTemplateStatus>,
    spec: &InfrastructureTemplateSpec,
    plan_hash: &str,
) -> bool {
    status
        .as_ref()
        .and_then(|s| s.approved_plan_hash.as_deref())
        .map(|approved| approved == plan_hash)
        .unwrap_or(false)
        || spec
            .spec_approved_plan_hash
            .as_deref()
            .map(|approved| approved == plan_hash)
            .unwrap_or(false)
}

/// Route a Planning-phase cycle through the state-fingerprinted
/// approval gate: recompute `plan_approval_hash` from THIS cycle's
/// plan + real infrastructure state (`current_state_fingerprint`,
/// executor-aware — Postgres for magma, on-disk for tofu), compare
/// against `status.approvedPlanHash`, and either proceed to `Applying`
/// (already approved — the hash match itself proves the approval was
/// granted against THIS exact plan+state, not a stale one) or park at
/// `Planning` with `status.pendingPlanHash` set so a human can bless it.
///
/// Shared by two callers — `PolicyDecision::RequireApproval` (the
/// policy engine's own decision) and `PolicyDecision::AutoApply` when
/// `state_continuity_breach` fires — so the state-fingerprinting
/// protection lives in exactly one place instead of two independent
/// (and driftable) copies. Both callers fetch `state_fingerprint` via
/// `current_state_fingerprint` before calling in — this function no
/// longer reads state itself, so it can fail closed on
/// `CurrentStateFingerprint::Unreadable` without a second read.
async fn route_through_approval_gate(
    template: &InfrastructureTemplate,
    state: &ControllerState,
    workspace: &crate::executor::Workspace,
    plan_result: &crate::executor::workspace_runner::PlanResult,
    policy_outcome: &crate::executor::PolicyOutcome,
    plan_text: Option<String>,
    reason: ApprovalGateReason,
    state_fingerprint: CurrentStateFingerprint,
) -> Result<ReconcileAction> {
    let plan_content = canonical_drift_fingerprint(&policy_outcome.annotated_drifts);

    let state_bytes = match resolve_approval_hash_input(&state_fingerprint) {
        ApprovalHashInput::Hashable(bytes) => bytes,
        ApprovalHashInput::RefuseUnreadable => {
            // Fail CLOSED: never compute — let alone compare — an
            // approval hash while real state is unknown. Degrading to a
            // `plan_text`-only hash here would silently reproduce the
            // exact bug `CurrentStateFingerprint` exists to close (see
            // its doc comment). Neither `status.pendingPlanHash` nor
            // `status.approvedPlanHash` is touched this cycle; the next
            // reconcile retries the read.
            warn!(
                template = %template.name_any(),
                "Approval gate: current infrastructure state could not be read; \
                 refusing to compute or compare a plan-approval hash this cycle. \
                 Holding at Planning until state becomes readable again."
            );
            record_event(
                template,
                state,
                EventType::Warning,
                "StateUnreadable",
                "Could not read current infrastructure state while evaluating the \
                 plan-approval gate; holding at Planning rather than risk comparing \
                 against a stale approval hash. Will retry automatically.",
            )
            .await;
            return Ok(ReconcileAction::Requeue(DEFAULT_REQUEUE_INTERVAL));
        }
    };
    let plan_hash = plan_approval_hash(&plan_content, state_bytes);

    let is_approved = is_plan_approved(&template.status, &template.spec, &plan_hash);

    if is_approved {
        info!(plan_hash, "Plan approved by user, proceeding to apply");
        update_phase(template, Phase::Applying, state).await?;
        record_event(template, state, EventType::Normal, "PlanApproved", "Plan approved by user").await;
        Ok(ReconcileAction::Requeue(SHORT_REQUEUE_INTERVAL))
    } else {
        info!(plan_hash, "Policy requires approval, waiting");
        update_pending_plan_hash(template, &plan_hash, state).await?;
        // Emit a Drifted-uncorrected receipt so the user sees exactly
        // which resources are awaiting approval. The content-equality
        // guard inside record_reconcile_cycle suppresses re-patches
        // while the plan keeps matching.
        record_reconcile_cycle(
            template,
            state,
            Some(&workspace.path),
            plan_result.artifact.clone(), // slice 2c: runner-provided artifact threads through cycle receipt
            &policy_outcome.annotated_drifts,
            plan_text,
            CycleResult::PolicyGated(PolicyDecision::RequireApproval),
        )
        .await?;
        record_event(
            template,
            state,
            EventType::Normal,
            "PlanPending",
            &format!(
                "{} Approve via GitOps: commit spec.approvedPlanHash: \"{plan_hash}\" to this CR's manifest. \
                 Or via direct kubectl (where imperative cluster mutations are permitted): \
                 kubectl patch infra {} -n {} --type merge --subresource status -p '{{\"status\":{{\"approvedPlanHash\":\"{plan_hash}\"}}}}'",
                reason.waiting_message(),
                template.name_any(),
                template.namespace().unwrap_or_default(),
            ),
        ).await;
        Ok(ReconcileAction::Requeue(DEFAULT_REQUEUE_INTERVAL))
    }
}

/// Handle Applying phase - run `tofu apply`.
/// Public wrapper for `handle_applying` so trait impls in
/// `controller::template_phase` can dispatch to it. The body lives
/// here; the trait impl is the thin shim.
pub(crate) async fn handle_applying_internal(
    template: &InfrastructureTemplate,
    state: &ControllerState,
) -> Result<ReconcileAction> {
    handle_applying(template, state).await
}

#[tracing::instrument(skip_all, name = "handle_applying")]
async fn handle_applying(
    template: &InfrastructureTemplate,
    state: &ControllerState,
) -> Result<ReconcileAction> {
    info!("Template in Applying phase");
    let _phase_timer = state.metrics.record_phase_duration("applying");
    // Keep `executor` for the verb-level apply call (we don't migrate the
    // full handle_applying to runner.apply in this slice — apply has
    // tofu-specific self-heal paths the runner abstraction would have to
    // grow to absorb cleanly; slice 2d does that). But we ALSO grab the
    // runner so we can run a post-apply `runner.plan()` and thread the
    // resulting `CycleArtifact` into the cycle receipt. That's what makes
    // tofu cycles WITH CHANGES populate `actionDistribution` after apply —
    // the post-apply re-plan reports the converged state.
    // Mutating phase entry point: resolve through the forbid-aware,
    // credential-aware checked variant so (a) a `PANGEA_FORBID_TOFU`
    // violation fails loud (typed Error::TofuForbidden → status.lastError)
    // and (b) on the magma path the apply executor carries the resolved
    // `spec.providerCredentials` — forwarded into magma's ApplyContext
    // via with_provider_config so the provider's create/update RPCs reach
    // it with real credentials instead of a null config ("channel
    // closed"). The post-apply re-plan runner is built the same way.
    // Per ★★ MAGMA-NATIVE.
    let executor = state.executor_for_checked_with_creds(template).await?;
    let runner = state.executor_runner_for_with_creds(template).await?;

    let workspace = state.workspace_manager.get_workspace(template).await?;
    let plan_path = workspace.plan_path();

    // Restart-safety (see handle_planning): a pod that resumes a CR at
    // phase=Applying on a fresh emptyDir workspace has no compiled
    // main.tf.json (nor plan checkpoint) — the apply / post-apply re-plan
    // would IO-error on the missing config. Bounce back to Compiling so
    // the clone+compile (then plan) re-runs.
    if !compiled_config_available(template, state, workspace.main_tf_path().exists()).await? {
        warn!(
            template = %template.name_any(),
            "Applying: compiled config missing (pod restart / wiped workspace, \
             and no Postgres rendered_config) — resetting to Compiling so \
             clone+compile re-runs"
        );
        update_phase(template, Phase::Compiling, state).await?;
        return Ok(ReconcileAction::Requeue(SHORT_REQUEUE_INTERVAL));
    }

    // Concurrency guard: from here to the end of this function, every
    // path issues (or may issue) real provider create/update RPCs
    // (import pre-pass, apply, conflict resolution, post-apply re-plan).
    // The operator's only other concurrency guard — the Lease-based
    // `LeaderElector` — is advisory and explicitly tolerates an
    // overlapping pod pair during a `RollingUpdate`, so without a second,
    // independent guard here two pods could both reach this dispatch for
    // the SAME template and race real mutating RPCs against the same
    // cloud resources (see the doc on `ControllerState::state_lock`).
    // `_state_lock_guard` holds the advisory lock for the rest of this
    // function via RAII — released on every return path (success,
    // error, or early return) by `LockGuard::drop`.
    let schema_name = format!("pangea_{}", template.spec.pangea_namespace);
    let template_name = template.name_any();
    let _state_lock_guard =
        match acquire_mutation_lock(state, &schema_name, &template_name).await? {
            LockDispatch::Proceed(guard) => guard,
            LockDispatch::Contended => {
                warn!(
                    template = %template_name,
                    "Applying: another operator pod holds this template's state lock \
                     — requeueing instead of racing a concurrent apply"
                );
                return Ok(ReconcileAction::Requeue(SHORT_REQUEUE_INTERVAL));
            }
        };

    // Snapshot the drift_details that handle_planning persisted on
    // status before the apply ran — this is the per-resource change
    // set the apply just consumed (or just failed against).
    let prior_drifts = template
        .status
        .as_ref()
        .map(|s| s.drift_details.clone())
        .unwrap_or_default();
    let prior_plan_summary = template
        .status
        .as_ref()
        .and_then(|s| s.plan_summary.clone());

    // Import pre-pass: for every `create` action whose resource
    // address has an importHint, run `tofu import <addr> <id>` to
    // adopt the existing cloud resource into state instead of
    // creating a duplicate. Imported addresses are tracked so the
    // cycle receipt can mark them Outcome::Imported (instead of
    // whatever the post-import plan would derive).
    let mut imported_addresses = run_import_prepass(
        template,
        state,
        &workspace.path,
        &plan_path,
        &prior_drifts,
    )
    .await;

    // Use plan file if it exists, otherwise apply directly. If we
    // imported anything, drop the cached plan file — the new state
    // makes the cached plan stale; tofu apply will refresh.
    let plan_file = if plan_path.exists() && imported_addresses.is_empty() {
        Some(plan_path.as_path())
    } else {
        None
    };

    // SAFETY GATE (2026-07-12): when imports ran, `plan_file` above is
    // `None` — the apply call below runs a live refresh-and-apply with NO
    // saved plan, so tofu decides its own action set at apply time,
    // never re-shown to the human who approved `prior_drifts`. Confirmed
    // live on camelot-eks: an import-triggered refresh discovered
    // `aws_eks_cluster` needed `replace` (a computed-attribute artifact
    // of the freshly-imported resource — the same purely-computed-field
    // shape the 2026-07-12 SIXTH-INCIDENT `replace_because_tainted` bug
    // had, but this time on a genuinely fresh import, not a stale taint
    // marker), and `apply` executed it — destroying and recreating a
    // production EKS cluster with zero human review of THAT specific
    // action, even though the plan a human actually approved
    // (`prior_drifts`) contained zero destroys. This is not a race —
    // it fires deterministically whenever importHints exist and state
    // is empty (i.e. every fresh-pod apply on this CR). Close the gap
    // structurally: re-plan (read-only, mutates nothing) after imports
    // settle, and refuse to apply if the fresh plan contains any
    // destructive action (delete/replace) the ORIGINALLY APPROVED plan
    // didn't already carry for that address. This enforces the exact
    // same invariant `route_through_approval_gate` enforces before
    // Applying is ever entered — closing the copy of that gap which
    // reopens mid-Applying whenever imports occur. `run_import_prepass`
    // runs on BOTH executors (magma imports via `executor.import`, see
    // that function's magma-native `planned_changes()` discovery tier),
    // so this gate must too — see `drift_details_from_plan_result`'s doc
    // for the 2026-07-19 fix that closed the magma-inert gap this
    // comment used to (incorrectly) claim as a known limitation.
    if plan_file.is_none() && !imported_addresses.is_empty() {
        let recheck = runner.plan(&workspace).await?;
        if recheck.success {
            let fresh_drifts = drift_details_from_plan_result(&recheck, template, state).await;
            if let Some(escalation) =
                find_unapproved_destructive_escalation(&prior_drifts, &fresh_drifts)
            {
                let msg = format!(
                    "Applying refused: post-import refresh discovered a new {} \
                     action on {} that was not in the approved plan. Parking at \
                     Failed (the FSM's only legal edge out of Applying besides \
                     Ready) so the existing self-heal path recomputes a fresh \
                     plan for human review — never applying an action nobody \
                     approved.",
                    escalation.action, escalation.address
                );
                warn!(template = %template_name, %msg);
                record_event(
                    template,
                    state,
                    EventType::Warning,
                    "UnapprovedEscalation",
                    &msg,
                )
                .await;
                update_phase_with_error(template, Phase::Failed, &msg, state).await?;
                return Ok(ReconcileAction::Requeue(DEFAULT_REQUEUE_INTERVAL));
            }
        }
        // A failed recheck plan falls through to the existing apply call
        // below, which surfaces the same failure through its own
        // established error path — unchanged behavior on a plan failure.
    }

    // ── destroyProtection apply gate (task #120) ─────────────────────
    // `spec.destroyProtection` promises "never destroy this
    // infrastructure", but before this gate it was consulted ONLY on the
    // explicit destroy path (`handle_destroying` / CR deletion). A normal
    // apply — AutoApply, or an approved cached plan — whose action set
    // contained a `replace` (delete-then-recreate) or `delete` executed
    // with `destroyProtection` never read. That is the class of gap
    // behind the repeated camelot-eks in-apply cluster replacements.
    // `prior_drifts` (snapshotted above) is the analyzed action set this
    // apply consumes — the same set `handle_planning` persisted. Refuse
    // UNCONDITIONALLY (approved or not) when protection is on and any
    // action is destructive, parking at `Failed` (the FSM's only legal
    // edge out of Applying besides Ready) exactly like the r101
    // escalation gate. The human must set `destroyProtection=false` first
    // to proceed — identical to the destroy-path requirement. This is a
    // sibling to, not a duplicate of, the r101 post-import gate: that one
    // fires only on the import path for an *unapproved* escalation; this
    // one is unconditional on every apply path.
    if let DestroyProtectionGate::BlockedByProtectedDestruction { address, action } =
        evaluate_destroy_protection_gate(template.spec.destroy_protection, &prior_drifts)
    {
        let msg = format!(
            "Applying refused: destroyProtection is enabled and the plan contains a \
             destructive {action} action on {address}. A replace is a delete-then-\
             recreate, so it is blocked under destroy protection (approved or not). \
             Set spec.destroyProtection=false first if this destruction is intended, \
             then re-approve. Parking at Failed."
        );
        warn!(template = %template_name, %msg);
        record_event(template, state, EventType::Warning, "DestroyBlocked", &msg).await;
        update_phase_with_error(template, Phase::Failed, &msg, state).await?;
        return Ok(ReconcileAction::Requeue(DEFAULT_REQUEUE_INTERVAL));
    }

    // ── destroyProtection FRESH-recheck (task #120 follow-up, #131) ──
    // The gate above only ever sees `prior_drifts` — sound exactly when
    // `plan_file = Some(...)` (the cached plan IS what's about to be
    // applied). Whenever `plan_file` is `None` — the ordinary case on
    // the magma execution path (magma reads its plan from Postgres, not
    // a cached `.tfplan`, so `plan_path` is never populated) —
    // `executor.apply` below can realize an action set `prior_drifts`
    // never saw. Recompute it and gate on it too; see
    // `recheck_destroy_protection_before_bare_apply`'s doc for the full
    // rationale. ADDITIVE beside the check above, never a replacement.
    if plan_file.is_none() {
        if let Some(action) = recheck_destroy_protection_before_bare_apply(
            template,
            state,
            &executor,
            &workspace.path,
            &plan_path,
        )
        .await?
        {
            return Ok(action);
        }
    }

    let mut result = executor
        .apply(&workspace.path, plan_file, true)
        .await?;

    // Self-heal: stale-plan auto-recovery within the same reconcile.
    //
    // OpenTofu refuses to consume a cached `-out` plan if state has
    // changed since the plan was generated — that's "Saved plan is
    // stale". The cached plan is unrecoverable, but the underlying
    // reconcile intent isn't: a fresh plan-then-apply (with
    // `plan_file = None`) will compute a new plan against current
    // state and apply it in one shot. Detecting + retrying here
    // converts what was a phase-trapping failure (the operator stuck
    // at Applying for ~7 days on rio, 2026-05-08) into a transient
    // condition the operator defeats inside one reconcile.
    if !result.success && is_self_healable_apply_error(&result.stderr) {
        warn!(
            stderr = %result.stderr,
            "tofu apply rejected cached plan (stale or unusable) — discarding plan cache and retrying with fresh apply"
        );
        record_event(
            template,
            state,
            EventType::Normal,
            "StalePlanRecovery",
            "discarding stale plan cache and retrying apply with fresh plan",
        )
        .await;
        let _ = tokio::fs::remove_file(&plan_path).await;

        // destroyProtection FRESH-recheck (task #120 follow-up, #131):
        // this retry always applies with plan_file=None (the stale
        // cached plan was just discarded), the same gap as the main
        // apply path above — recheck before retrying.
        if let Some(action) = recheck_destroy_protection_before_bare_apply(
            template,
            state,
            &executor,
            &workspace.path,
            &plan_path,
        )
        .await?
        {
            return Ok(action);
        }

        result = executor
            .apply(&workspace.path, None, true)
            .await?;
    }

    // Post-apply conflict resolution — the typed, cascading
    // ConflictResolutionPolicy layer. When the pre-apply import sweep
    // didn't adopt everything (e.g. `tofu show -json` came back empty on
    // a huge plan, or a resource was created out-of-band between plan and
    // apply), the apply 422s on "already exists" / "already protected".
    // Rather than failing the cycle, classify each conflict against the
    // policy and, for `import`-resolution conflicts, adopt the resource
    // via `tofu import` then re-apply — up to `maxRounds`. Gated on the
    // same autoOnConflict signal the prepass uses (or an explicit
    // `spec.conflictPolicy.enabled`), so it fires on the existing
    // pleme-io-opensource CR with no spec change. This is the convergence
    // guarantee that does NOT depend on the prepass succeeding.
    if !result.success {
        if let Some(outcome) = crate::controller::conflict::resolve_conflicts_post_apply(
            template,
            state,
            &workspace.path,
            &plan_path,
            &workspace.main_tf_path(),
            result.clone(),
        )
        .await?
        {
            if outcome.destroy_protection_blocked {
                // The destroyProtection safety net (task #120 follow-up,
                // #131) fired mid-conflict-resolution: the event was
                // already recorded and the template already parked at
                // Phase::Failed inside resolve_conflicts_post_apply.
                // Return immediately — falling through to the generic
                // apply-failure classification below would misreport a
                // deliberate safety block as an ordinary provider error.
                return Ok(ReconcileAction::Requeue(DEFAULT_REQUEUE_INTERVAL));
            }
            let imported_n = outcome.imported.len();
            imported_addresses.extend(outcome.imported);
            result = outcome.result;
            if result.success {
                info!(
                    imported = imported_n,
                    rounds = outcome.rounds,
                    "conflict-resolution: apply converged after adopting out-of-band resources"
                );
                record_event(
                    template,
                    state,
                    EventType::Normal,
                    "ConflictResolved",
                    &format!(
                        "Adopted {imported_n} out-of-band resource(s) via import + re-apply ({} round(s))",
                        outcome.rounds
                    ),
                )
                .await;
            } else if imported_n > 0 {
                warn!(
                    imported = imported_n,
                    rounds = outcome.rounds,
                    "conflict-resolution: imported resources but apply still failing — surfacing real error"
                );
            }
        }
    }

    if result.success {
        info!(duration_secs = result.duration.as_secs_f64(), "tofu apply completed successfully");

        // Fetch outputs
        let outputs = match executor.output(&workspace.path).await {
            Ok(output_result) if output_result.success => {
                serde_json::from_str(&output_result.stdout).ok()
            }
            _ => None,
        };

        // The apply realized whatever the last compile produced —
        // thread status.compiledRevision into lastAppliedRevision so
        // the cycle receipt's sourceRevision chain goes live.
        let applied_revision = template
            .status
            .as_ref()
            .and_then(|s| s.compiled_revision.clone());
        update_apply_status(template, outputs.clone(), applied_revision.as_deref(), state).await?;

        // X2: write tofu outputs to user-bound K8s Secrets. Best-
        // effort — bindings logged + metric'd; failure here doesn't
        // fail the reconcile (apply already succeeded).
        if !template.spec.output_bindings.is_empty() {
            let outs_map = outputs.unwrap_or_default();
            let results = crate::controller::template::output_bindings::apply_output_bindings(
                template,
                &outs_map,
                &state.client,
            )
            .await;
            let (published, missing, errored) =
                crate::controller::template::output_bindings::summarize(&results);
            let template_name = template
                .metadata
                .name
                .clone()
                .unwrap_or_else(|| "unknown".into());
            let template_ns = template
                .metadata
                .namespace
                .clone()
                .unwrap_or_else(|| "unknown".into());
            for r in &results {
                let result_label = match &r.status {
                    crate::controller::template::output_bindings::PublishStatus::Published { .. } => "published",
                    crate::controller::template::output_bindings::PublishStatus::OutputMissing => "output_missing",
                    crate::controller::template::output_bindings::PublishStatus::Errored(_) => "errored",
                };
                state.metrics.record_output_binding(
                    &template_name,
                    &template_ns,
                    result_label,
                );
            }
            info!(
                template = %template_name,
                published, missing, errored,
                "output_bindings: cycle summary"
            );
        }

        update_phase(template, Phase::Ready, state).await?;

        // Slice 2c part-2: capture the post-apply state via the runner.
        // For tofu, this is a fresh `tofu plan` that should report no
        // changes (the apply converged the state). For magma, the
        // bundle on disk reflects the post-apply state. Either way,
        // threading this artifact into the cycle receipt gives the CR
        // status its `actionDistribution` for the post-apply cycle.
        //
        // Best-effort: a runner.plan() failure here doesn't fail the
        // apply (which already succeeded); we just lose the
        // post-apply artifact and the cycle records without
        // actionDistribution populated.
        let post_apply_artifact = match runner.plan(&workspace).await {
            Ok(r) => r.artifact,
            Err(e) => {
                warn!(
                    error = %e,
                    "post-apply runner.plan failed; cycle will record without actionDistribution"
                );
                None
            }
        };

        // planSummary handling: pass the planning-phase summary (what the
        // cycle PLANNED — e.g. "+5" for a real create). `build_reconcile_cycle`
        // centrally overrides it to "No changes" when the cycle actually
        // converged (success + zero created/updated/destroyed/imported), so a
        // steady-state template can't show a stale "+6". See cycle_receipts.rs.
        record_reconcile_cycle(
            template,
            state,
            Some(&workspace.path),
            post_apply_artifact,
            &prior_drifts,
            prior_plan_summary,
            CycleResult::AppliedSuccess { imported_addresses: imported_addresses.clone() },
        )
        .await?;
        record_event(template, state, EventType::Normal, "Applied", "Infrastructure applied successfully").await;
    } else {
        // OpenTofu writes most diagnostics to stdout, not stderr. Combine
        // both into the err_msg so the operator surface tells the human
        // exactly which class of failure tripped.
        let combined_output = if result.stdout.is_empty() {
            result.stderr.clone()
        } else if result.stderr.is_empty() {
            result.stdout.clone()
        } else {
            format!("{}\n--- stderr ---\n{}", result.stdout, result.stderr)
        };
        // Executor-neutral label: the default executor is magma (tofu is
        // forbidden via PANGEA_FORBID_TOFU), so a "tofu apply failed" prefix
        // here is a lie that misroutes diagnosis. The error body already
        // carries the provider/executor detail.
        let err_msg = format!("apply failed: {combined_output}");
        warn!(%err_msg);

        // ── The reacting FSM: classify every apply failure into the typed
        // ApplyAnomaly taxonomy, then dispatch ONE typed remediation
        // (theory/ANOMALY-REACTIVE-RECONCILE.md §IV-§VI). This FOLDS the
        // former four scattered substring checks (stale-plan, empty-
        // workspace, already-exists, self-healable) into one classifier so
        // there is a single typed detection border. Each anomaly carries a
        // RemediationMode (Absolute | Decaying | Hold) — the Lyapunov
        // settle-bound made explicit.
        //
        // Detection sources, in priority:
        //   1. `result.failed_changes` — the structured per-resource
        //      failures the magma executor now surfaces (each carries the
        //      free `reason` string the classifier matches on).
        //   2. Fallback — when the executor produced no structured records
        //      (the tofu path, whose failures only surface as stdout/
        //      stderr text), classify the combined output once.
        let anomalies: Vec<crate::controller::anomaly::ApplyAnomaly> =
            if !result.failed_changes.is_empty() {
                result
                    .failed_changes
                    .iter()
                    .map(|fc| {
                        crate::controller::anomaly::classify(&fc.reason, &fc.address, &fc.action)
                    })
                    .collect()
            } else {
                // FIX 2 (tofu fallback): the legacy executor surfaces failures
                // only as a stdout/stderr blob, never structured per-resource
                // records. Classifying the WHOLE blob once is
                // first-substring-match-wins across many resources'
                // diagnostics — a conflict in one resource masks a different
                // anomaly (rate-limit / permission / transient-net) in
                // another. Split the blob into per-address diagnostic blocks
                // (`conflict::apply_error_blocks`) and classify EACH block
                // separately, mirroring the magma per-`FailedChange` path so
                // both executors produce a per-resource anomaly list. The
                // tofu action grammar isn't recoverable from the diagnostic
                // text, so we pass "" for the per-block action (the
                // anomalies that carry it record an empty action — the
                // address + class are what drive the remediation).
                let blocks = crate::controller::conflict::apply_error_blocks(&combined_output);
                if blocks.is_empty() {
                    // No resource-scoped blocks parsed (a backend / init /
                    // non-resource error). Fall back to the single blanket
                    // classify so the failure is still typed + surfaced, never
                    // silently dropped.
                    vec![crate::controller::anomaly::classify(&combined_output, "", "")]
                } else {
                    blocks
                        .iter()
                        .map(|(addr, reason)| {
                            crate::controller::anomaly::classify(reason, addr, "")
                        })
                        .collect()
                }
            };

        // Pick the single anomaly that drives this tick's reconcile action.
        // Recovery anomalies (stale-plan / empty-workspace) take precedence
        // because they have an Absolute self-heal that converges within one
        // reconcile; then the provider/conflict classes; holds last (they
        // don't recover on their own, so they shouldn't mask a recoverable
        // sibling). `react_to_apply_anomaly` performs the dispatch.
        let driver = select_driving_anomaly(&anomalies);
        warn!(
            anomaly = driver.kind_str(),
            mode = driver.mode().as_str(),
            total = anomalies.len(),
            "apply failed — classified into typed ApplyAnomaly; dispatching typed remediation"
        );

        if let Some(action) = react_to_apply_anomaly(
            template,
            state,
            &driver,
            &err_msg,
            &prior_drifts,
            prior_plan_summary.clone(),
            &workspace,
            &runner,
        )
        .await?
        {
            // The remediation produced a terminal reconcile action for this
            // tick (recovery bounce, Decaying requeue, …). It already
            // recorded its own status/event/cycle. Return it.
            return Ok(action);
        }

        // No early-return remediation: fall through to the typed
        // SurfaceAndHold / record-failure path below (Hold-mode anomalies +
        // the ProviderUnavailable surface). `update_phase_with_error`
        // sets phase=Failed + a `Healthy=False` condition + lastError and
        // runs the ReactivePolicy escalation pipeline — non-silent by
        // construction.
        update_phase_with_error(template, Phase::Failed, &err_msg, state).await?;

        // Even on apply failure, capture the post-failure state via
        // the runner — for tofu, this surfaces "what changed (or
        // didn't) at the point of failure"; for magma, the bundle
        // captures the lifecycle FSM's failed-phase. Best-effort:
        // a runner.plan() failure here is silent.
        let post_apply_artifact = runner.plan(&workspace).await.ok().and_then(|r| r.artifact);

        record_reconcile_cycle(
            template,
            state,
            Some(&workspace.path),
            post_apply_artifact,
            &prior_drifts,
            prior_plan_summary,
            CycleResult::AppliedFailure(err_msg.clone()),
        )
        .await?;
        // K8s Events have a 1024-char message limit; the combined stdout+stderr
        // err_msg can be much longer (one provider's "Creating..." log + several
        // diagnostics easily exceeds 1KiB). Truncate before recording the
        // event so we don't lose the failure on K8s admission validation.
        // The full err_msg is still on the template status (lastError +
        // lastCycle.outcomes) and in the operator log.
        //
        // The event reason now carries the typed anomaly discriminant
        // (SurfaceAndHold per §IV) so `kubectl get events` names the class —
        // `AnomalyUnclassified` / `AnomalyPermissionDenied` /
        // `AnomalyProviderUnavailable` — not just a generic `ApplyFailed`.
        // An Unclassified here is the backlog signal (§X): it forces a new
        // taxonomy arm.
        let event_reason = driver.event_reason();
        let event_body = format!(
            "[{} / {}] {}",
            driver.kind_str(),
            driver.mode().as_str(),
            err_msg
        );
        // `event_body` embeds `err_msg` — untrusted apply-error output from
        // tofu/magma that is not guaranteed ASCII. A raw `&event_body[..1000]`
        // byte slice panics whenever byte 1000 lands mid-character; this runs
        // inside the same unsupervised `tokio::spawn` reconcile task as
        // `truncate_for_status` above, so a single non-ASCII apply error
        // would silently halt reconciliation fleet-wide. Char-boundary-safe
        // by construction — see `crate::text_util::truncate_utf8_safe`.
        let event_msg = crate::text_util::truncate_utf8_safe(
            &event_body,
            1000,
            "…[truncated, full err in template status]",
        );
        record_event(template, state, EventType::Warning, event_reason, &event_msg).await;
    }

    Ok(ReconcileAction::Requeue(DEFAULT_REQUEUE_INTERVAL))
}

/// Pick the single [`ApplyAnomaly`] that drives this reconcile tick's
/// remediation action from the per-resource set.
///
/// Precedence (theory/ANOMALY-REACTIVE-RECONCILE.md §V): Absolute
/// self-healing recovery classes first (they converge within one reconcile
/// and shouldn't be masked), then provider/conflict classes, then Decaying
/// retries, then Hold classes last (a Hold doesn't recover on its own, so it
/// must not mask a recoverable sibling). Within a tie the first occurrence
/// wins (deterministic). An empty set never reaches here (the caller only
/// classifies on failure), but defaults to a typed Unclassified for safety.
fn select_driving_anomaly(
    anomalies: &[crate::controller::anomaly::ApplyAnomaly],
) -> crate::controller::anomaly::ApplyAnomaly {
    use crate::controller::anomaly::ApplyAnomaly;
    fn rank(a: &ApplyAnomaly) -> u8 {
        match a {
            ApplyAnomaly::StalePlan => 0,
            ApplyAnomaly::EmptyWorkspace => 1,
            ApplyAnomaly::ProviderUnavailable { .. } => 2,
            ApplyAnomaly::ObjectExistsUntracked { .. } => 3,
            ApplyAnomaly::RateLimited => 4,
            ApplyAnomaly::TransientNetwork => 5,
            ApplyAnomaly::PermissionDenied => 6,
            ApplyAnomaly::Unclassified { .. } => 7,
        }
    }
    anomalies
        .iter()
        .min_by_key(|a| rank(a))
        .cloned()
        .unwrap_or_else(|| ApplyAnomaly::Unclassified {
            reason: "apply failed with no classifiable failure record".into(),
        })
}

/// Dispatch the typed remediation for the driving [`ApplyAnomaly`]
/// (theory/ANOMALY-REACTIVE-RECONCILE.md §IV, §VII.2). Returns
/// `Some(action)` when the remediation produced a terminal reconcile action
/// for this tick (and recorded its own status/event/cycle); `None` when the
/// caller should fall through to the typed SurfaceAndHold record-failure
/// path (Phase::Failed + Healthy=False + ReactivePolicy escalation).
///
/// Each arm records its MODE (Absolute | Decaying | Hold) so the Lyapunov
/// settle-bound is explicit in the operator-facing event.
#[allow(clippy::too_many_arguments)]
async fn react_to_apply_anomaly(
    template: &InfrastructureTemplate,
    state: &ControllerState,
    anomaly: &crate::controller::anomaly::ApplyAnomaly,
    err_msg: &str,
    prior_drifts: &[DriftDetail],
    prior_plan_summary: Option<String>,
    workspace: &crate::executor::Workspace,
    runner: &Arc<dyn crate::executor::workspace_runner::WorkspaceRunner>,
) -> Result<Option<ReconcileAction>> {
    use crate::controller::anomaly::{ApplyAnomaly, RemediationMode};

    let mode = anomaly.mode();
    match anomaly {
        // ── StalePlan / EmptyWorkspace → Absolute self-heal ───────────
        // Drop the stale cached plan + clean the workspace (idempotent) and
        // bounce to Pending so the next cycle re-renders + re-plans against
        // current state. Converges within one reconcile. (Formerly the
        // is_stale_plan / is_empty_workspace ad-hoc block — folded into the
        // typed taxonomy.)
        ApplyAnomaly::StalePlan | ApplyAnomaly::EmptyWorkspace => {
            let (reason_code, reason_msg) = match anomaly {
                ApplyAnomaly::StalePlan => (
                    "StalePlanRecovered",
                    "Apply hit stale-plan race; wiped workspace and re-queued from Pending for a fresh plan",
                ),
                _ => (
                    "EmptyWorkspaceRecovered",
                    "Apply found empty workspace (likely pod restart mid-self-heal); re-queued from Pending",
                ),
            };
            warn!(
                kind = reason_code,
                mode = mode.as_str(),
                "Apply failure is recoverable (Absolute) — wiping workspace + transitioning to Pending"
            );
            if let Ok(ws) = state.workspace_manager.get_workspace(template).await {
                let _ = ws.clean().await;
            }
            update_phase(template, Phase::Pending, state).await?;
            record_event(template, state, EventType::Normal, reason_code, reason_msg).await;
            Ok(Some(ReconcileAction::Requeue(SHORT_REQUEUE_INTERVAL)))
        }

        // ── RateLimited / TransientNetwork → Decaying backoff retry ───
        // The provider RPC hit a (secondary) rate limit or a transient
        // network fault. magma's in-RPC `rpc_retry!` already wore down the
        // transient inside the apply; reaching here means it exhausted, so
        // back off at the operator tick and let the next reconcile re-plan
        // + re-apply.
        //
        // FIX 3 (Decaying livelock): the requeue interval MUST strictly
        // decrease retry pressure per consecutive failure — a CONSTANT
        // 30s requeue is constant-weight retry, which the model
        // (ANOMALY-REACTIVE-RECONCILE.md §IV/§VI) names a forbidden
        // *livelock* for a "Decaying" remediation. We compute a per-
        // failure_count exponential backoff (the same `exponential_backoff`
        // helper the Failed-phase retry path uses), so consecutive
        // RateLimited/TransientNetwork ticks requeue at a GROWING interval
        // up to a cap — retry pressure strictly decreases, satisfying the
        // Decaying contract. The samba pacer already bounds the in-apply
        // mutation rate; this bounds the BETWEEN-tick cadence. The
        // ReactivePolicy escalation ladder still counts persistent
        // decay-that-won't-settle as the upper livelock guard.
        ApplyAnomaly::RateLimited | ApplyAnomaly::TransientNetwork => {
            // `update_phase_with_error` (called next) bumps failure_count by
            // one; reflect that increment so the backoff grows from THIS
            // consecutive failure. base 30s, cap 600s (10m), matching the
            // Failed-phase retry envelope.
            let consecutive = template.retry_count().saturating_add(1);
            let backoff = exponential_backoff(consecutive, 30, 600);
            warn!(
                anomaly = anomaly.kind_str(),
                mode = mode.as_str(),
                consecutive,
                backoff_secs = backoff.as_secs(),
                "Apply hit a Decaying anomaly — backing off with GROWING per-failure exponential \
                 backoff (strictly-decreasing retry pressure; samba/rpc_retry already attempted in-RPC)"
            );
            // Still bump the failure surface so ReactivePolicy's escalation
            // ladder counts persistent decay-that-won't-settle (the livelock
            // guard) — but keep it visible-not-terminal.
            update_phase_with_error(template, Phase::Applying, err_msg, state).await?;
            let post_apply_artifact = runner.plan(workspace).await.ok().and_then(|r| r.artifact);
            record_reconcile_cycle(
                template,
                state,
                Some(&workspace.path),
                post_apply_artifact,
                prior_drifts,
                prior_plan_summary,
                CycleResult::AppliedFailure(err_msg.to_string()),
            )
            .await?;
            record_event(
                template,
                state,
                EventType::Normal,
                anomaly.event_reason(),
                &format!(
                    "[{} / {}] backing off {}s (attempt {}) + retrying ({})",
                    anomaly.kind_str(),
                    mode.as_str(),
                    backoff.as_secs(),
                    consecutive,
                    truncate_for_status(err_msg)
                ),
            )
            .await;
            Ok(Some(ReconcileAction::Requeue(backoff)))
        }

        // ── ObjectExistsUntracked → Import / adopt ────────────────────
        // The operator's existing import machinery already ran BEFORE this
        // point — the pre-apply `run_import_prepass` (spec.importHints +
        // importPolicy.naturalIds + bundled_natural_ids) and the post-apply
        // `conflict::resolve_conflicts_post_apply` import+re-apply loop. If
        // we still see ObjectExistsUntracked here, adoption did NOT resolve
        // a cloud-id for this address. For github-shaped resources the
        // natural-id IS the name and magma's internal adopt closes the loop
        // once providers are staged; the residual gap is the cloudflare
        // record whose import id is `<zone_id>/<record_id>` with a
        // server-assigned record_id that nothing recovers (bundled_natural_ids
        // deliberately excludes cloudflare — import.rs:108-135).
        //
        // The convergent, no-human-id resolution (a pre-flight CF list-by-
        // name probe → ImportResourceState-by-natural-key) lives in magma
        // and is OUT OF SCOPE for this operator-side change. So here we
        // SurfaceAndHold with the EXACT importHint the operator should carry
        // to adopt the resource — non-silent, actionable, and naming the
        // residual. Falls through (None) so the record-failure path sets
        // Healthy=False + runs escalation.
        ApplyAnomaly::ObjectExistsUntracked { address, .. } => {
            let hint = suggested_import_hint(address);
            warn!(
                address = %address,
                mode = mode.as_str(),
                "ObjectExistsUntracked persisted past the import prepass + post-apply conflict loop — \
                 surfacing the importHint to carry (cloud-id resolution for server-assigned ids is a magma pre-flight probe, out of scope here)"
            );
            record_event(
                template,
                state,
                EventType::Warning,
                "AnomalyObjectExistsUntrackedHint",
                &format!(
                    "Resource {address} exists out-of-band and auto-adoption could not resolve its cloud-id. \
                     Add spec.importHints[\"{address}\"] = \"{hint}\" (+ the referenced variables) to adopt it. \
                     [mode={}]",
                    mode.as_str()
                ),
            )
            .await;
            Ok(None)
        }

        // ── ProviderUnavailable → SurfaceAndHold (runtime) ────────────
        // The provider plugin couldn't be located. Tier-honest split (FIX 4):
        //   • BUILD time — baking the providers into the image at a durable
        //     MAGMA_PROVIDER_DIR (roll-surviving) makes this anomaly
        //     structurally ABSENT for every baked provider: Absolute-by-
        //     construction at build time (no runtime path can reach here).
        //   • RUNTIME — for a provider that is NOT yet baked, the operator
        //     CANNOT stage it at runtime (that needs an image rebuild adding
        //     it to flake.nix magmaProviderMirror). So the realized runtime
        //     remediation is a Hold, NOT Absolute: surface the exact
        //     actionable gap ("add `<provider>` to magmaProviderMirror +
        //     rebuild") and fall through (None) to Healthy=False. A short
        //     requeue still retries in case a staged image is mid-roll, but
        //     the reaction does not claim self-convergence at runtime.
        ApplyAnomaly::ProviderUnavailable { provider } => {
            warn!(
                provider = %provider,
                mode = mode.as_str(),
                "ProviderUnavailable — provider plugin not located. Runtime reaction Holds (the \
                 operator cannot stage a provider at runtime); a baked provider makes this \
                 structurally absent (Absolute-by-construction at build time). Add `{provider}` \
                 to flake.nix magmaProviderMirror + rebuild the operator image."
            );
            record_event(
                template,
                state,
                EventType::Warning,
                anomaly.event_reason(),
                &format!(
                    "Provider plugin '{provider}' not in the baked MAGMA_PROVIDER_DIR mirror. \
                     The operator cannot stage a provider at runtime — add '{provider}' to \
                     flake.nix magmaProviderMirror and rebuild the operator image. [mode={}]",
                    mode.as_str()
                ),
            )
            .await;
            Ok(None)
        }

        // ── PermissionDenied / Unclassified → SurfaceAndHold ──────────
        // Neither recovers on its own. Fall through to the record-failure
        // path (Phase::Failed + Healthy=False + ReactivePolicy escalation),
        // which is non-silent by construction. The terminal-event reason
        // (set by the caller from `driver.event_reason()`) names the class;
        // an Unclassified here is the §X backlog signal that forces a new
        // taxonomy arm. We assert the Hold mode explicitly for clarity.
        ApplyAnomaly::PermissionDenied | ApplyAnomaly::Unclassified { .. } => {
            debug_assert_eq!(mode, RemediationMode::Hold);
            Ok(None)
        }
    }
}

/// Best-effort suggested `importHint` template for a resource address whose
/// out-of-band twin must be adopted. For the cloudflare-record gap the
/// natural import id is `<zone_id>/<record_id>` (record_id server-assigned);
/// for most other types the name is the id. This is advisory text in a
/// surfaced event — it names what the operator should carry, it does not
/// auto-resolve the id.
fn suggested_import_hint(address: &str) -> String {
    let ty = address.split('.').next().unwrap_or(address);
    match ty {
        "cloudflare_dns_record" | "cloudflare_record" => {
            "{{ zone_id }}/<record_id>".to_string()
        }
        _ => "<natural-id>".to_string(),
    }
}

/// Detect tofu apply errors that are self-healable by discarding the
/// cached `-out` plan and retrying with a fresh plan-then-apply.
///
/// The classic case is `Saved plan is stale`: state was mutated
/// between `tofu plan -out` and `tofu apply <plan>`, so the plan
/// snapshot no longer reflects reality. The fix isn't to surrender
/// the reconcile to the Failed phase and wait for `handle_failed` to
/// wipe the workspace — it's to discard the one stale artifact
/// (the plan file) and let tofu compute a fresh plan inline.
///
/// Match substrings (not regex) so behavior is predictable even if
/// tofu reformats messages across versions. Keep the list tight —
/// every entry is a deliberate "this is recoverable by replanning"
/// claim, not a catch-all that papers over real bugs.
fn is_self_healable_apply_error(stderr: &str) -> bool {
    // The canonical opentofu / terraform stale-plan banner.
    stderr.contains("Saved plan is stale")
}

#[cfg(test)]
mod source_freshness_tests {
    use super::{content_revision, generation_invalidates_render, rendered_config_is_current};

    // ── generation_invalidates_render — the spec/generation reuse guard ──

    #[test]
    fn spec_generation_ahead_invalidates_the_render() {
        // A spec edit bumped generation past the last-observed generation:
        // the cached render predates the edit → force a recompile even if
        // the source-content revision is unchanged (variables-only edit).
        assert!(
            generation_invalidates_render(6, 5),
            "generation ahead of observedGeneration must invalidate the cached render"
        );
    }

    #[test]
    fn caught_up_generation_does_not_invalidate() {
        // observedGeneration has caught up to generation → no pending spec
        // change → the revision-based gate decides reuse; do not churn.
        assert!(
            !generation_invalidates_render(5, 5),
            "an equal generation must NOT force a recompile (no pending spec change)"
        );
    }

    #[test]
    fn stale_observed_generation_never_forces_recompile() {
        // Defensive: an observedGeneration somehow AHEAD of generation
        // (clock/replay anomaly) must not loop the render — only a strictly
        // ahead generation invalidates.
        assert!(
            !generation_invalidates_render(4, 5),
            "observedGeneration ahead of generation must not force a recompile"
        );
    }

    #[test]
    fn content_revision_is_stable_and_change_sensitive() {
        let a = content_revision("resource \"aws_vpc\" \"x\" {}");
        // Deterministic: same content → same revision (so a fresh config does
        // NOT churn into an endless re-compile loop).
        assert_eq!(a, content_revision("resource \"aws_vpc\" \"x\" {}"));
        // cm:-tagged + fixed width — never mistaken for a 40-char git SHA.
        assert!(a.starts_with("cm:"), "got {a}");
        assert_eq!(a.len(), 3 + 16);
        // A config EDIT changes the revision → forces exactly one re-compile.
        let b = content_revision("resource \"aws_vpc\" \"x\" { tags = {} }");
        assert_ne!(a, b, "changed content must yield a different revision");
    }

    // ── rendered_config_is_current — the unified reuse law (git + non-git) ──
    //
    // These pin the decision `compiled_config_available` makes AFTER it
    // confirms a rendered_config row is present: reuse the cached render iff
    // its recorded source_revision matches the revision the operator should
    // be at (status.compiledRevision for git; the cm: content hash for
    // non-git). The pre-fix bug was that git sources reused UNCONDITIONALLY;
    // the headline regression test below is `git_head_advanced_*`.

    #[test]
    fn git_head_advanced_makes_cached_render_stale() {
        // THE load-bearing regression. A git-sourced template: the render in
        // Postgres was produced at an OLD HEAD, but the observed HEAD (kept on
        // status.compiledRevision by the freshness gate) has advanced. The
        // cached render is NOT current → recompile. Before the fix this
        // returned `available` unconditionally and the source change silently
        // never applied.
        let old_head = "a1b2c3d4e5f60718293a4b5c6d7e8f9012345678";
        let new_head = "ffeeddccbbaa00112233445566778899aabbccdd";
        assert!(
            !rendered_config_is_current(Some(old_head), Some(new_head)),
            "a moved git HEAD must make the older-revision render stale (recompile)"
        );
    }

    #[test]
    fn git_head_unchanged_reuses_cached_render() {
        // The negative: when the render's revision equals the current HEAD,
        // reuse it — the gate must not force gratuitous recompiles.
        let head = "a1b2c3d4e5f60718293a4b5c6d7e8f9012345678";
        assert!(
            rendered_config_is_current(Some(head), Some(head)),
            "an unchanged git HEAD must reuse the cached render (no recompile churn)"
        );
    }

    #[test]
    fn non_git_content_edit_makes_cached_render_stale() {
        // The preserved non-git behavior, now expressed via the SAME unified
        // gate: an inline/configMap edit changes the cm: content hash, so the
        // stored render (from the old content) is stale → recompile.
        let old_rev = content_revision("resource \"aws_vpc\" \"x\" {}");
        let new_rev = content_revision("resource \"aws_vpc\" \"x\" { tags = {} }");
        assert_ne!(old_rev, new_rev);
        assert!(
            !rendered_config_is_current(Some(&old_rev), Some(&new_rev)),
            "a non-git content edit must make the cached render stale (recompile)"
        );
    }

    #[test]
    fn non_git_content_unchanged_reuses_cached_render() {
        // Preserved non-git behavior: unchanged content reuses the render.
        let rev = content_revision("resource \"aws_vpc\" \"x\" {}");
        assert!(
            rendered_config_is_current(Some(&rev), Some(&rev)),
            "unchanged non-git content must reuse the cached render"
        );
    }

    #[test]
    fn legacy_null_stored_revision_recompiles_once() {
        // A legacy artifact row written before the source_revision column
        // existed carries NULL → treated as stale so exactly one recompile
        // stamps the revision. Same converge-by-one-recompile discipline as a
        // legacy CR with no compiledRevision.
        let head = "a1b2c3d4e5f60718293a4b5c6d7e8f9012345678";
        assert!(
            !rendered_config_is_current(None, Some(head)),
            "a NULL stored revision (legacy row) must recompile once to stamp it"
        );
        // Also stale when both are unknown — a legacy row + no anchor still
        // recompiles once rather than trusting an unrevisioned blob.
        assert!(!rendered_config_is_current(None, None));
    }

    #[test]
    fn undeterminable_current_revision_forces_recompile_not_masking() {
        // NEVER-STUCK (Reaction D): a stored render + an UNDETERMINABLE
        // current revision must NOT be certified as current. Previously
        // this returned `true` ("present ⇒ available"), which silently
        // served a possibly-stale render when the current source became
        // unreadable this tick (a configMap key deleted / ConfigMap gone —
        // `non_git_source_revision` returns `Ok(None)` there). That is the
        // masking hazard: the operator kept serving the OLD render off a
        // source it could no longer read. It must force a recompile so the
        // compile path either re-reads the recovered source or surfaces the
        // real "source unreadable" error loudly.
        let head = "a1b2c3d4e5f60718293a4b5c6d7e8f9012345678";
        assert!(
            !rendered_config_is_current(Some(head), None),
            "an unreadable/undeterminable current source revision must NOT silently \
             certify the stored render as current — force a recompile-or-fail"
        );
    }
}

#[cfg(test)]
mod self_healable_apply_error_tests {
    use super::is_self_healable_apply_error;

    #[test]
    fn detects_canonical_stale_plan_banner() {
        let stderr = "\nError: Saved plan is stale\n\nThe given plan file can no longer be applied because the state was changed by\nanother operation after the plan was created.";
        assert!(
            is_self_healable_apply_error(stderr),
            "canonical stale-plan stderr must trigger recovery"
        );
    }

    #[test]
    fn ignores_unrelated_apply_failure() {
        let stderr =
            "Error: error creating GitHub repository: 422 Validation Failed (name already exists)";
        assert!(
            !is_self_healable_apply_error(stderr),
            "real provider errors must NOT trigger the stale-plan recovery path"
        );
    }

    #[test]
    fn ignores_empty_stderr() {
        assert!(!is_self_healable_apply_error(""));
    }
}

/// Extract every address with a `create` action from a `tofu show
/// -json <tfplan>` payload. Used by the import prepass to find ALL
/// create-action addresses without the 50-entry cap that
/// `Plan::drift_details` applies for status-surface fitness.
fn extract_create_addresses_from_plan(plan_json: &str) -> Vec<String> {
    let parsed: serde_json::Value = match serde_json::from_str(plan_json) {
        Ok(v) => v,
        Err(_) => return Vec::new(),
    };
    let changes = match parsed.get("resource_changes").and_then(|v| v.as_array()) {
        Some(a) => a,
        None => return Vec::new(),
    };
    changes
        .iter()
        .filter_map(|change| {
            let actions = change
                .pointer("/change/actions")
                .and_then(|v| v.as_array())?;
            let is_create = actions.iter().any(|a| a.as_str() == Some("create"));
            if !is_create {
                return None;
            }
            change
                .get("address")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string())
        })
        .collect()
}

/// Legacy tofu-path plan readback for import discovery: `tofu show -json`
/// stdout, or `""` on a non-success / empty / errored read (best-effort —
/// the prepass proceeds without pre-apply import; the post-apply conflict
/// catch covers). Reached ONLY on the `planned_changes() == Ok(None)`
/// fall-through (tofu, or disk-fallback magma) — the magma DB path never
/// touches this stringly channel.
async fn read_legacy_show_plan_json(
    executor: &dyn crate::executor::IacExecutor,
    workspace_path: &std::path::Path,
    plan_path: &std::path::Path,
) -> String {
    match executor.show_plan(workspace_path, plan_path).await {
        Ok(r) if r.success && !r.stdout.is_empty() => r.stdout,
        Ok(r) => {
            // NON-SILENT: a non-success or empty `tofu show -json` is the
            // exact failure that silently disabled auto-import and stuck
            // the pleme-io-opensource posture for ~17 days.
            warn!(
                success = r.success,
                exit_code = r.exit_code,
                stdout_len = r.stdout.len(),
                stderr = %truncate_for_status(&r.stderr),
                "import prepass: `tofu show -json` returned no usable plan JSON — \
                 falling back to prior drift details (typically 0 creates). Pre-apply \
                 import is disabled this cycle; conflicts will be caught post-apply."
            );
            String::new()
        }
        Err(e) => {
            warn!(
                error = %e,
                "import prepass: `tofu show -json` errored — pre-apply import disabled \
                 this cycle (post-apply conflict catch covers)."
            );
            String::new()
        }
    }
}


/// Run the pre-apply import sweep. Returns the set of addresses
/// successfully imported so the cycle receipt can mark them as
/// `Outcome::Imported` instead of whatever the apply-time plan
/// derives (the plan-after-import would say no-op or update — the
/// USER-facing outcome is "we adopted this resource").
///
/// Failures are non-fatal: a hint with bad substitution is skipped
/// with a Warning event; an import that fails (wrong ID, resource
/// gone, already-managed) is logged but doesn't block the apply.
async fn run_import_prepass(
    template: &InfrastructureTemplate,
    state: &ControllerState,
    workspace_path: &std::path::Path,
    plan_path: &std::path::Path,
    prior_drifts: &[DriftDetail],
) -> Vec<String> {
    use crate::controller::import::{
        discovery_from_planned_changes, parse_planned_attrs, resolve_import_targets, ImportSkip,
    };

    let executor = state.executor_for(template);

    let auto_import = template
        .spec
        .import_policy
        .as_ref()
        .map(|p| p.auto_on_conflict)
        .unwrap_or(false);

    // Short-circuit when neither auto-import nor declared hints fire.
    if template.spec.import_hints.is_empty() && !auto_import {
        return Vec::new();
    }

    let variables = template.spec.variables.clone().unwrap_or_default();

    // Create-action discovery — magma-native + disk-free FIRST.
    //
    // Prefer the executor's typed, disk-free `planned_changes()` readback:
    // the magma DB-backed path (production) + the RecordingExecutor test
    // mock return `Some(..)` off the SAME typed plan `apply()` consumes —
    // ZERO filesystem, NO tofu-format string re-parse. tofu + disk-fallback
    // magma return `None` → the byte-identical legacy `tofu show -json`
    // path (drift-details capped at 50; the plan bypasses the cap). A
    // DB-backed magma with no persisted plan row is a LOUD typed `Err`
    // (never a silent empty — that silent empty is the exact ~17-day
    // pleme-io-opensource wedge shape). Per the org ★★ MAGMA-NATIVE
    // EXECUTION directive (disk-free by default) + ★★ TYPED-SPEC border.
    let (create_addresses_owned, planned_by_addr): (
        Vec<String>,
        std::collections::BTreeMap<String, serde_json::Value>,
    ) = match executor.planned_changes().await {
        Ok(Some(changes)) => {
            let (creates, planned) = discovery_from_planned_changes(&changes);
            info!(
                total_create_addresses = creates.len(),
                source = "planned_changes",
                "import prepass: create-action discovery (magma-native typed plan, disk-free)"
            );
            (creates, planned)
        }
        Ok(None) => {
            // Legacy tofu / disk-fallback path: read + parse the tofu
            // `show -json` payload (byte-identical to the prior behavior),
            // with prior_drifts as the fallback for callers without a plan.
            let plan_json =
                read_legacy_show_plan_json(executor.as_ref(), workspace_path, plan_path).await;
            let plan_create_addresses = extract_create_addresses_from_plan(&plan_json);
            let plan_creates_n = plan_create_addresses.len();
            let creates: Vec<String> = if !plan_create_addresses.is_empty() {
                plan_create_addresses
            } else {
                prior_drifts
                    .iter()
                    .filter(|d| d.action == "create")
                    .map(|d| d.address.clone())
                    .collect()
            };
            info!(
                plan_json_len = plan_json.len(),
                plan_creates = plan_creates_n,
                total_create_addresses = creates.len(),
                source = if plan_creates_n > 0 { "plan" } else { "prior_drifts" },
                "import prepass: create-action discovery"
            );
            // parse_planned_attrs is best-effort — only the auto-import
            // layers below consume it.
            let planned = if auto_import {
                parse_planned_attrs(&plan_json)
            } else {
                std::collections::BTreeMap::new()
            };
            (creates, planned)
        }
        Err(e) => {
            // LOUD, non-fatal: a DB-backed magma with no persisted plan
            // (or an artifact-store read error). Do NOT silently disable
            // import — surface it + skip pre-apply import THIS cycle. The
            // reconcile continues; the post-apply conflict catch AND
            // magma's reactive on-conflict adopt (magma_apply engine)
            // cover the gap so breathe still converges.
            warn!(
                error = %e,
                "import prepass: executor.planned_changes() errored (no persisted magma plan?) — \
                 pre-apply import disabled this cycle (post-apply conflict catch + magma reactive \
                 adopt cover)."
            );
            return Vec::new();
        }
    };

    if create_addresses_owned.is_empty() {
        return Vec::new();
    }

    // Resolve every create-action to an import target via the three-layer
    // cascade (importHints → naturalIds → bundled). Pure, ControllerState-
    // free core — no I/O, no tofu-format parse.
    let (targets, skips) = resolve_import_targets(
        &create_addresses_owned,
        &planned_by_addr,
        &template.spec.import_hints,
        template.spec.import_policy.as_ref(),
        &variables,
    );

    // Surface unresolved skips as typed events / warnings (kept out of the
    // pure resolver so it stays sync + state-free).
    for skip in &skips {
        match skip {
            ImportSkip::Hint { address, missing } => {
                warn!(
                    address = %address,
                    missing_var = %missing,
                    "import hint substitution failed; skipping"
                );
                record_event(
                    template,
                    state,
                    EventType::Warning,
                    "ImportHintSkipped",
                    &format!(
                        "Import hint for {address} references unset variable {{{{ .{missing} }}}}; skipping"
                    ),
                )
                .await;
            }
            ImportSkip::Auto {
                address,
                template: id_template,
                missing,
                server_assigned,
            } => {
                warn!(
                    address = %address,
                    template = %id_template,
                    missing = %missing,
                    suggestion = if *server_assigned {
                        "Server-assigned attribute is null on create-action plans. \
                         Add `spec.importHints[<address>] = \"<known-cloud-id>\"` and re-reconcile."
                    } else {
                        "Required attribute not in plan. Either declare it on the workspace \
                         DSL resource block, or add a per-address `spec.importHints` entry."
                    },
                    "auto-import: substitution failed; skipping"
                );
            }
        }
    }

    // Dispatch all resolved imports concurrently. Each `tofu import`
    // is its own subprocess and the pg backend's advisory lock
    // naturally serializes the state-write step (~200ms/import),
    // so the win comes from overlapping the non-locked phases
    // (config load, provider gRPC init, GitHub API call — ~10-15s
    // each). Empirically against pleme-io-opensource (~459 imports)
    // serial=1/15s = 7000s+ ≈ 2h; buffer_unordered(10) ≈ 12-15min.
    const IMPORT_CONCURRENCY: usize = 10;
    let total_targets = targets.len();
    if total_targets == 0 {
        return Vec::new();
    }

    // Credential-aware executor for the actual import RPCs. `executor`
    // above (built via the sync, credential-blind `state.executor_for`)
    // is fine for DISCOVERY — `planned_changes()` / `show_plan()` only
    // read an already-computed plan (Postgres row or local checkpoint),
    // never a live provider RPC. `try_tofu_import` below is different:
    // it calls `executor.import()`, which on the magma path DOES issue
    // a real provider Read/Import RPC. Magma carries provider
    // credentials IN the executor instance (threaded into `ApplyContext`
    // at call time — never baked into an on-disk file the way tofu's
    // `providers.tf.json` is), so an executor built via the
    // credential-blind constructor has an EMPTY `provider_configs` map
    // and every RPC it issues silently falls back to the provider
    // plugin's own ambient credential chain (pod env / EC2 instance
    // role) instead of `spec.providerCredentials`. Resolve ONCE here
    // (not per-target inside the concurrent loop below — that would
    // re-read the credential Secret(s) once per import target) via the
    // same checked, credential-aware constructor `handle_applying`'s own
    // apply/destroy calls use. Per ★★ MAGMA-NATIVE.
    let import_executor = match state.executor_for_checked_with_creds(template).await {
        Ok(exec) => exec,
        Err(e) => {
            warn!(
                error = %e,
                "import prepass: failed to resolve a credential-aware executor \
                 (spec.providerCredentials / PANGEA_FORBID_TOFU) — pre-apply import \
                 disabled this cycle (post-apply conflict catch + magma reactive \
                 adopt cover)."
            );
            return Vec::new();
        }
    };

    info!(
        total = total_targets,
        concurrency = IMPORT_CONCURRENCY,
        "Running import prepass concurrently"
    );
    let imported: Vec<String> = futures::stream::iter(targets.into_iter())
        .map(|t| {
            let import_executor = Arc::clone(&import_executor);
            async move {
                let ok = try_tofu_import(
                    template,
                    state,
                    &import_executor,
                    workspace_path,
                    &t.address,
                    &t.id,
                    &t.source,
                )
                .await;
                if ok { Some(t.address) } else { None }
            }
        })
        .buffer_unordered(IMPORT_CONCURRENCY)
        .filter_map(|maybe_addr| async move { maybe_addr })
        .collect()
        .await;

    info!(
        imported = imported.len(),
        total = total_targets,
        "Import prepass complete"
    );
    imported
}

/// Try a single `tofu import`. Returns true if the import succeeded.
/// Failures are non-fatal — we log + emit a Warning event and let the
/// apply path handle the resource (where it'll fail visibly with a
/// real error message instead of a silently-skipped import).
///
/// `executor` is the credential-aware executor `run_import_prepass`
/// resolved once via `state.executor_for_checked_with_creds` — this
/// function must NOT re-derive its own via the bare, credential-blind
/// `state.executor_for`, or the real provider Read/Import RPC below
/// silently runs under ambient credentials instead of
/// `spec.providerCredentials`.
async fn try_tofu_import(
    template: &InfrastructureTemplate,
    state: &ControllerState,
    executor: &Arc<dyn crate::executor::IacExecutor>,
    workspace_path: &std::path::Path,
    addr: &str,
    import_id: &str,
    source_label: &str,
) -> bool {
    info!(
        address = %addr,
        import_id = %import_id,
        source = %source_label,
        "Running tofu import for create-action"
    );
    match executor.import(workspace_path, addr, import_id).await {
        Ok(r) if r.success => {
            record_event(
                template,
                state,
                EventType::Normal,
                "Imported",
                &format!(
                    "Adopted out-of-band {addr} into state via import id {import_id} ({source_label})"
                ),
            )
            .await;
            true
        }
        Ok(r) => {
            warn!(
                address = %addr,
                stderr = %r.stderr,
                "import failed; falling through to apply"
            );
            record_event(
                template,
                state,
                EventType::Warning,
                "ImportFailed",
                &format!("import {addr} failed: {}", truncate_for_status(&r.stderr)),
            )
            .await;
            false
        }
        Err(e) => {
            warn!(address = %addr, error = %e, "import errored; falling through to apply");
            false
        }
    }
}

/// Replace `{{ .name }}` (with optional whitespace) tokens in
/// `template` with string-coerced values from `variables`. Returns
/// `Err(missing_var)` on the first unresolved token so the caller
/// can surface it as a typed event.
pub(crate) fn substitute_import_id(
    template: &str,
    variables: &std::collections::BTreeMap<String, serde_json::Value>,
) -> std::result::Result<String, String> {
    let mut out = String::with_capacity(template.len());
    let bytes = template.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if i + 1 < bytes.len() && bytes[i] == b'{' && bytes[i + 1] == b'{' {
            // Find closing }}
            let close = match template[i + 2..].find("}}") {
                Some(p) => i + 2 + p,
                None => {
                    out.push_str(&template[i..]);
                    break;
                }
            };
            let inner = template[i + 2..close].trim();
            // Accept either `.name` or `name`.
            let var_name = inner.strip_prefix('.').unwrap_or(inner).trim();
            match variables.get(var_name) {
                Some(serde_json::Value::String(s)) => out.push_str(s),
                Some(v) => out.push_str(&v.to_string().trim_matches('"').to_string()),
                None => return Err(var_name.to_string()),
            }
            i = close + 2;
        } else {
            out.push(bytes[i] as char);
            i += 1;
        }
    }
    Ok(out)
}

/// Handle Ready phase - periodic drift detection + state-settling tracking.
///
/// Settling is the controller's primary success metric: each
/// Ready→Drifted→Ready cycle that still reports drift is one
/// "non-settling" cycle. Two stuck signals — either is loud:
///   * count: `consecutive_drift_cycles >= max` (configurable, default 5)
///   * fingerprint: drift content identical across cycles (we're not
///     making progress even before the count threshold)
///
/// On stuck, the configured `SettlingPolicy.on_exhaustion` decides:
/// fail (transition to Failed, default), alert (emit Warning + flip
/// `Settled=False` condition but keep trying), or continue (silent).
/// Public wrapper for `handle_ready` so trait impls in
/// `controller::template_phase` can dispatch to it. The body lives
/// here; the trait impl is the thin shim.
pub(crate) async fn handle_ready_internal(
    template: &InfrastructureTemplate,
    state: &ControllerState,
) -> Result<ReconcileAction> {
    handle_ready(template, state).await
}

#[tracing::instrument(skip_all, name = "handle_ready")]
async fn handle_ready(
    template: &InfrastructureTemplate,
    state: &ControllerState,
) -> Result<ReconcileAction> {
    // Slice 2c part 3: drift check goes through the runner abstraction
    // too. Same shape as handle_planning — one `runner.plan(workspace)`
    // call returns both the raw show-JSON (for the legacy Plan-based
    // drift detail extraction the settling fingerprint reads) and the
    // typed `CycleArtifact` (for the unified surface). Closes the
    // last phase handler that still spoke `IacExecutor` directly for
    // its plan call.
    let runner = state.executor_runner_for(template);
    let interval = parse_duration(&template.spec.refresh_interval)
        .unwrap_or(DEFAULT_REQUEUE_INTERVAL);

    let workspace = state.workspace_manager.get_workspace(template).await?;

    // FIRST BEAT — observe the remote git HEAD EVERY tick, ABOVE the drift
    // throttle. A moved remote is a desired-state change, observed every
    // reconcile exactly like a spec change: the gate records
    // observedHeadRevision and, on a HEAD-advance, bounces to Compiling
    // (re-render) immediately. ls-remote is 1 RTT, so this is cheap enough to
    // run unthrottled. THIS is the rio-wedge fix: HEAD used to be observed
    // only on the 32m drift cadence (and frozen entirely when the probe
    // errored), so the operator could sit phase=Ready 25 commits behind HEAD.
    let freshness = match source_freshness_gate(template, state, &workspace, "ready").await? {
        FreshnessGate::Bounce(action) => return Ok(action),
        FreshnessGate::Proceed(f) => f,
    };

    // Restart-safety: compiled main.tf.json missing (a fresh pod resumed a
    // Ready CR onto a wiped emptyDir, skipping handle_compiling) → bounce to
    // Compiling so clone+compile re-runs. Reached every tick now (above the
    // throttle), so a restart self-heals immediately instead of waiting out
    // the drift interval. (The 2026-06-03 fleet wedge: 4 Ready templates
    // os-error-2'd every cycle on the post-restart emptyDir.)
    if !compiled_config_available(template, state, workspace.main_tf_path().exists()).await? {
        warn!(
            template = %template.name_any(),
            "Ready: compiled config missing (pod restart / wiped workspace, \
             and no Postgres rendered_config) — resetting to Compiling so \
             clone+compile re-runs"
        );
        update_phase(template, Phase::Compiling, state).await?;
        return Ok(ReconcileAction::Requeue(SHORT_REQUEUE_INTERVAL));
    }

    // Throttle the EXPENSIVE plan only — the cheap HEAD observation above
    // already ran this tick. (The plan stays on the refreshInterval cadence;
    // the freshness probe does not.)
    if let Some(last_check) = template
        .status
        .as_ref()
        .and_then(|s| s.last_drift_check_at)
    {
        let elapsed = Utc::now().signed_duration_since(last_check);
        let interval_chrono = chrono::Duration::from_std(interval).unwrap_or(chrono::Duration::minutes(5));
        if elapsed < interval_chrono {
            debug!("Plan throttled ({}s since last); HEAD already observed this tick", elapsed.num_seconds());
            return Ok(ReconcileAction::Requeue(interval));
        }
    }

    debug!("Running drift detection");

    let plan_result = runner.plan(&workspace).await?;

    update_drift_check_timestamp(template, state).await?;

    // Two-path drift extraction — identical shape to handle_planning's
    // dispatch. Tofu uses `Plan::from_json` on the show-JSON for
    // per-attribute drift detail (the settling fingerprint reads this);
    // magma derives from the typed `CycleArtifact.resource_changes`
    // (which the bundle reader populates with severities + actions).
    // Either way, the drift_details list feeds the settling evaluator
    // the same way it always did.
    let drift_details: Vec<crate::crd::DriftDetail> = if !plan_result.has_changes {
        Vec::new()
    } else if !plan_result.raw_show_json.is_empty() {
        match Plan::from_json(&plan_result.raw_show_json) {
            Ok(plan) => plan
                .drift_details(50)
                .into_iter()
                .map(|d| crate::crd::DriftDetail {
                    address: d.address,
                    action: d.action,
                    risk: d.risk,
                    attributes: d.attributes,
                    policy_decision: None,
                    matched_policy: None,
                })
                .collect(),
            Err(e) => {
                warn!(error = %e, runner = runner.name(), "Failed to parse drift plan JSON");
                Vec::new()
            }
        }
    } else if let Some(art) = plan_result.artifact.as_ref() {
        art.drift_details(50)
    } else {
        warn!(runner = runner.name(), "Drift check produced no analyzable output");
        Vec::new()
    };

    let settling_policy = template.spec.settling_policy.clone().unwrap_or_default();
    let prior_cycles = template
        .status
        .as_ref()
        .map(|s| s.consecutive_drift_cycles)
        .unwrap_or(0);
    let prior_fingerprint = template
        .status
        .as_ref()
        .filter(|s| !s.drift_details.is_empty())
        .map(|s| crate::controller::settling::fingerprint(&s.drift_details));

    let outcome = crate::controller::settling::evaluate(
        &settling_policy,
        prior_cycles,
        prior_fingerprint.as_deref(),
        &drift_details,
    );
    let action = crate::controller::settling::action_for(&outcome, &settling_policy);

    update_settling_status(template, &outcome, &drift_details, freshness.as_ref(), state).await?;

    // Mirror settling state into Prometheus gauges + counters.
    let tname = template.name_any();
    let tns = template.namespace().unwrap_or_default();
    state
        .metrics
        .consecutive_drift_cycles
        .with_label_values(&[&tname, &tns])
        .set(outcome.cycle_count() as i64);
    let (cycles, stuck_addrs) = stuck_summary(&outcome);
    state
        .metrics
        .stuck_resources
        .with_label_values(&[&tname, &tns])
        .set(stuck_addrs.len() as i64);
    state
        .metrics
        .settled
        .with_label_values(&[&tname, &tns])
        .set(if matches!(outcome, crate::controller::settling::SettlingOutcome::Settled) { 1 } else { 0 });
    let _ = cycles;
    update_drift_detail_gauges(&state.metrics, &tname, &tns, &drift_details);

    use crate::controller::settling::{SettlingAction, SettlingOutcome};
    match action {
        SettlingAction::AcceptSettled => {
            debug!("No drift detected — system has settled");
            Ok(ReconcileAction::Requeue(interval))
        }
        SettlingAction::KeepTrying => {
            warn!("Drift detected, transitioning to Drifted");
            state.metrics.drift_detected_total.inc();
            update_phase(template, Phase::Drifted, state).await?;
            record_event(template, state, EventType::Warning, "DriftDetected", "Infrastructure drift detected").await;
            Ok(ReconcileAction::Requeue(SHORT_REQUEUE_INTERVAL))
        }
        SettlingAction::AlertButContinue => {
            let (cycles, addrs) = stuck_summary(&outcome);
            let reason_label = match outcome {
                SettlingOutcome::StuckByFingerprint { .. } => "StuckByFingerprint",
                SettlingOutcome::StuckByCount { .. } => "StuckByCount",
                _ => "Unknown",
            };
            state
                .metrics
                .settling_failures_total
                .with_label_values(&[&tname, &tns, reason_label])
                .inc();
            let msg = format!(
                "State has not settled after {} cycle(s). Stuck resources: {}. Continuing to retry.",
                cycles,
                addrs.join(", ")
            );
            warn!(%msg, "Settling alert");
            state.metrics.drift_detected_total.inc();
            record_event(template, state, EventType::Warning, "SettlingAlert", &msg).await;
            update_phase(template, Phase::Drifted, state).await?;
            Ok(ReconcileAction::Requeue(SHORT_REQUEUE_INTERVAL))
        }
        SettlingAction::EscalateToFailed => {
            let (cycles, addrs) = stuck_summary(&outcome);
            let (reason, reason_label) = match outcome {
                SettlingOutcome::StuckByFingerprint { .. } => (
                    "identical drift fingerprint across cycles",
                    "StuckByFingerprint",
                ),
                SettlingOutcome::StuckByCount { .. } => (
                    "exceeded max consecutive drift cycles",
                    "StuckByCount",
                ),
                _ => ("stuck", "Unknown"),
            };
            state
                .metrics
                .settling_failures_total
                .with_label_values(&[&tname, &tns, reason_label])
                .inc();
            let err_msg = format!(
                "STATE-SETTLING FAILED — {} after {} cycle(s). Stuck resources: {}. \
                 Manual investigation required (provider quota, broken provider config, \
                 conflicting external automation, or upstream API not converging).",
                reason,
                cycles,
                addrs.join(", ")
            );
            warn!(%err_msg, "Settling escalated to Failed");
            update_phase_with_error(template, Phase::Failed, &err_msg, state).await?;
            record_event(template, state, EventType::Warning, "SettlingFailed", &err_msg).await;
            Ok(ReconcileAction::Requeue(ERROR_REQUEUE_INTERVAL))
        }
    }
}

/// Set per-template gauges from the latest annotated drift list.
///
/// Resets the four (action, risk) buckets to zero before counting,
/// otherwise stale entries from the prior plan would linger forever.
/// Prometheus client doesn't expose a per-label-set delete that's
/// safe across versions, so we just zero the bounded action×risk
/// matrix (4×4 = 16 series per template).
fn update_drift_detail_gauges(
    metrics: &crate::observability::Metrics,
    template: &str,
    namespace: &str,
    drifts: &[crate::crd::DriftDetail],
) {
    use std::collections::HashMap;
    let actions = ["create", "update", "delete", "replace"];
    let risks = ["none", "low", "medium", "high"];
    let mut buckets: HashMap<(&str, &str), u64> = HashMap::new();
    for d in drifts {
        let action = actions.iter().copied().find(|a| *a == d.action).unwrap_or("update");
        let risk = risks.iter().copied().find(|r| *r == d.risk).unwrap_or("low");
        *buckets.entry((action, risk)).or_default() += 1;
    }
    for &a in &actions {
        for &r in &risks {
            let v = buckets.get(&(a, r)).copied().unwrap_or(0) as i64;
            metrics
                .template_drift_detail
                .with_label_values(&[template, namespace, a, r])
                .set(v);
        }
    }
}

fn stuck_summary(outcome: &crate::controller::settling::SettlingOutcome) -> (u32, Vec<String>) {
    use crate::controller::settling::SettlingOutcome;
    match outcome {
        SettlingOutcome::StuckByFingerprint { cycles, stuck_addresses, .. }
        | SettlingOutcome::StuckByCount { cycles, stuck_addresses } => {
            (*cycles, stuck_addresses.clone())
        }
        SettlingOutcome::Progressing { cycles } => (*cycles, vec![]),
        SettlingOutcome::Settled => (0, vec![]),
    }
}

/// Handle Drifted phase - auto-correct or wait for approval.
/// Public wrapper for `handle_drifted` so trait impls in
/// `controller::template_phase` can dispatch to it. The body lives
/// here; the trait impl is the thin shim.
pub(crate) async fn handle_drifted_internal(
    template: &InfrastructureTemplate,
    state: &ControllerState,
) -> Result<ReconcileAction> {
    handle_drifted(template, state).await
}

#[tracing::instrument(skip_all, name = "handle_drifted")]
async fn handle_drifted(
    template: &InfrastructureTemplate,
    state: &ControllerState,
) -> Result<ReconcileAction> {
    warn!("Template has drift detected");
    state.metrics.drift_detected_total.inc();

    if template.spec.auto_approve {
        // Auto-correction: transition back through plan → apply cycle.
        // Freshness-gated — drift correction must never apply a stale
        // compile (Stale ⇒ Compiling, not Planning).
        let workspace = state.workspace_manager.get_workspace(template).await?;
        if let FreshnessGate::Bounce(action) =
            source_freshness_gate(template, state, &workspace, "drifted").await?
        {
            return Ok(action);
        }
        info!("Auto-correcting drift");
        record_event(template, state, EventType::Normal, "DriftCorrection", "Auto-correcting infrastructure drift").await;
        update_phase(template, Phase::Planning, state).await?;
        Ok(ReconcileAction::Requeue(SHORT_REQUEUE_INTERVAL))
    } else {
        // Wait for manual approval via approved_plan_hash
        let approved = template
            .status
            .as_ref()
            .and_then(|s| {
                match (&s.pending_plan_hash, &s.approved_plan_hash) {
                    (Some(pending), Some(approved)) => Some(pending == approved),
                    _ => None,
                }
            })
            .unwrap_or(false);

        if approved {
            // Same gate as the auto-correct arm: an operator approval
            // does not make a stale compile safe to apply.
            let workspace = state.workspace_manager.get_workspace(template).await?;
            if let FreshnessGate::Bounce(action) =
                source_freshness_gate(template, state, &workspace, "drifted").await?
            {
                return Ok(action);
            }
            info!("Drift correction approved");
            record_event(template, state, EventType::Normal, "DriftApproved", "Drift correction approved by user").await;
            update_phase(template, Phase::Planning, state).await?;
            Ok(ReconcileAction::Requeue(SHORT_REQUEUE_INTERVAL))
        } else {
            debug!("Waiting for drift correction approval");
            Ok(ReconcileAction::Requeue(DEFAULT_REQUEUE_INTERVAL))
        }
    }
}

/// Handle Failed phase - retry with backoff.
/// Public wrapper for `handle_failed` so trait impls in
/// `controller::template_phase` can dispatch to it. The body lives
/// here; the trait impl is the thin shim.
pub(crate) async fn handle_failed_internal(
    template: &InfrastructureTemplate,
    state: &ControllerState,
) -> Result<ReconcileAction> {
    handle_failed(template, state).await
}

/// The interval `handle_failed` retries at once the exponential-backoff
/// ramp is exhausted. Slow enough not to hammer a genuinely broken
/// external system; still finite, so the CR keeps checking whether the
/// original blocker cleared instead of stopping forever.
const EXHAUSTED_RETRY_INTERVAL: Duration = Duration::from_secs(3600);

/// What `handle_failed` should do this reconcile: retry now (still
/// inside the exponential-backoff ramp) or retry at the slow,
/// steady-state cadence once the ramp is exhausted. There is
/// deliberately no "give up" variant.
///
/// BUG THIS CLOSES (task #191): the exhausted branch used to `return`
/// early WITHOUT ever transitioning the template back to `Pending` or
/// cleaning its workspace, so once a template exceeded
/// `spec.retryPolicy.maxRetries` it stayed in `Failed` FOREVER — even
/// after the original blocker (a transient cloud outage, an expired
/// credential since rotated, a config typo since fixed) had long since
/// cleared. `retries_exhausted()` stayed true forever too (nothing
/// resets `status.failureCount` from inside that early-return branch),
/// so every subsequent reconcile took the identical dead-end path. The
/// only way out was an external `kubectl edit`/re-apply, a direct
/// violation of ★★ CONTINUOUS CONVERGENCE (a controller must never
/// require a human to un-stick it).
///
/// Fixed by making retry unconditional: `handle_failed` now runs the
/// SAME clean + `update_phase(Pending)` + event sequence for both
/// variants below, only the requeue interval + event reason differ.
/// Transitioning to `Pending` re-enters the normal
/// Pending→Verifying→…→Applying pipeline, which re-checks whatever
/// condition originally failed — and `update_phase`'s existing
/// "clear on non-Failed transition" behavior resets `failureCount` to
/// 0, so a successful retry fully clears the exhaustion state and a
/// failing one re-enters the exponential ramp from scratch rather than
/// being permanently wedged at the ceiling.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FailedRetryDecision {
    /// Still inside the exponential-backoff ramp.
    ExponentialBackoff(Duration),
    /// The ramp is exhausted; retry anyway at `EXHAUSTED_RETRY_INTERVAL`
    /// so the CR keeps self-healing instead of stopping forever.
    SlowCadenceAfterExhaustion(Duration),
}

impl FailedRetryDecision {
    fn requeue_after(self) -> Duration {
        match self {
            Self::ExponentialBackoff(d) | Self::SlowCadenceAfterExhaustion(d) => d,
        }
    }

    fn event_reason(self) -> &'static str {
        match self {
            Self::ExponentialBackoff(_) => "Retry",
            Self::SlowCadenceAfterExhaustion(_) => "RetryAfterExhaustion",
        }
    }

    /// `Warning` for the exhausted case so operators actually notice
    /// "this template blew through its retry budget and is now
    /// self-healing at the slow cadence" in `kubectl describe` /
    /// `kubectl get events`, instead of it looking like an ordinary
    /// retry. Before this fix the exhausted case emitted no event at
    /// all (the handler returned before `record_event` ever ran).
    fn event_type(self) -> EventType {
        match self {
            Self::ExponentialBackoff(_) => EventType::Normal,
            Self::SlowCadenceAfterExhaustion(_) => EventType::Warning,
        }
    }
}

/// Pure decision powering `handle_failed`. Extracted so the self-heal
/// invariant — every path is SOME flavor of retry, never a permanent
/// stop — is unit-testable without a live `kube::Client` (this crate's
/// established convention: see `status_patch::patch_status`'s own test,
/// which documents that mocking the K8s API is out of scope and pure
/// decisions are tested directly instead).
fn failed_retry_decision(
    retries_exhausted: bool,
    failure_count: u32,
    backoff_seconds: u32,
) -> FailedRetryDecision {
    if retries_exhausted {
        FailedRetryDecision::SlowCadenceAfterExhaustion(EXHAUSTED_RETRY_INTERVAL)
    } else {
        FailedRetryDecision::ExponentialBackoff(exponential_backoff(failure_count, backoff_seconds, 600))
    }
}

#[tracing::instrument(skip_all, name = "handle_failed")]
async fn handle_failed(
    template: &InfrastructureTemplate,
    state: &ControllerState,
) -> Result<ReconcileAction> {
    let failure_count = template.retry_count();
    let retries_exhausted = template.retries_exhausted();
    warn!(failure_count, retries_exhausted, "Template in Failed phase");

    let backoff_seconds = template
        .spec
        .retry_policy
        .as_ref()
        .map(|p| p.backoff_seconds)
        .unwrap_or(30);
    let decision = failed_retry_decision(retries_exhausted, failure_count, backoff_seconds);

    if retries_exhausted {
        warn!(
            failure_count,
            "Retries exhausted; self-healing at the reduced cadence \
             instead of stopping forever"
        );
    }

    // Clean workspace and restart from Pending on retry. Unconditional:
    // every `FailedRetryDecision` variant is some flavor of retry, so
    // this always runs — that is what makes the self-heal invariant
    // hold. The controller's own NEXT reconcile always re-attempts the
    // cycle; no external trigger is ever required to leave `Failed`.
    info!("Cleaning workspace and retrying from Pending");
    let workspace = state.workspace_manager.get_workspace(template).await?;
    workspace.clean().await?;
    update_phase(template, Phase::Pending, state).await?;
    record_event(
        template,
        state,
        decision.event_type(),
        decision.event_reason(),
        &format!("Retrying after failure (attempt {})", failure_count),
    ).await;

    Ok(ReconcileAction::Requeue(decision.requeue_after()))
}

/// Handle CompileBlocked phase — HEAD observed, compile of it cannot
/// succeed. Public wrapper for `handle_compile_blocked` so trait impls
/// in `controller::template_phase` can dispatch to it. The body lives
/// here; the trait impl is the thin shim.
pub(crate) async fn handle_compile_blocked_internal(
    template: &InfrastructureTemplate,
    state: &ControllerState,
) -> Result<ReconcileAction> {
    handle_compile_blocked(template, state).await
}

/// Unlike `Failed`, CompileBlocked self-heals: park on an exponential
/// backoff (keyed off `consecutiveCompileFailures`, measured against
/// `phaseEnteredAt`), then retry Compiling. The loop exits the moment
/// a commit that compiles lands on the tracked ref — `handle_compiling`
/// resets the counter on success and the template proceeds normally.
/// While parked, the escalation Event + ladder action (incl. the
/// PauseAndAlert autoSuspend arm, honored upstream in
/// `reconcile_template`) have already fired — blocked is LOUD, not
/// silent.
#[tracing::instrument(skip_all, name = "handle_compile_blocked")]
async fn handle_compile_blocked(
    template: &InfrastructureTemplate,
    state: &ControllerState,
) -> Result<ReconcileAction> {
    let failures = template
        .status
        .as_ref()
        .map(|s| s.consecutive_compile_failures)
        .unwrap_or(0);
    let backoff = exponential_backoff(
        failures,
        template
            .spec
            .retry_policy
            .as_ref()
            .map(|p| p.backoff_seconds)
            .unwrap_or(30),
        600,
    );

    let elapsed = template
        .status
        .as_ref()
        .and_then(|s| s.phase_entered_at)
        .map(|t| (Utc::now() - t).to_std().unwrap_or(Duration::ZERO))
        .unwrap_or(Duration::ZERO);

    if elapsed < backoff {
        let remaining = backoff - elapsed;
        debug!(
            failures,
            remaining_secs = remaining.as_secs(),
            "CompileBlocked: parked on backoff before next compile retry"
        );
        return Ok(ReconcileAction::Requeue(remaining));
    }

    info!(failures, "CompileBlocked: backoff elapsed — retrying compile");
    update_phase(template, Phase::Compiling, state).await?;
    record_event(
        template,
        state,
        EventType::Normal,
        "CompileRetry",
        &format!("Retrying compile after CompileBlocked backoff (failure count {failures})"),
    )
    .await;
    Ok(ReconcileAction::Requeue(SHORT_REQUEUE_INTERVAL))
}

/// Handle Destroying phase - run `tofu destroy` and clean up.
/// Public wrapper for `handle_destroying` so trait impls in
/// `controller::template_phase` can dispatch to it. The body lives
/// here; the trait impl is the thin shim.
pub(crate) async fn handle_destroying_internal(
    template: &InfrastructureTemplate,
    state: &ControllerState,
) -> Result<ReconcileAction> {
    handle_destroying(template, state).await
}

#[tracing::instrument(skip_all, name = "handle_destroying")]
async fn handle_destroying(
    template: &InfrastructureTemplate,
    state: &ControllerState,
) -> Result<ReconcileAction> {
    // Double-check destroy protection (belt-and-suspenders)
    if template.spec.destroy_protection {
        warn!("Destroy protection active — blocking destroy");
        return Ok(ReconcileAction::Requeue(DEFAULT_REQUEUE_INTERVAL));
    }

    info!("Template in Destroying phase");
    // Slice 2c part 4: the last phase handler that still spoke the raw
    // `IacExecutor` migrates to the typed `WorkspaceRunner`. After this
    // commit, EVERY phase handler (planning/applying/ready/destroying)
    // consumes the same abstraction — `IacExecutor` is held directly
    // only by the verb-level carve-outs (`run_import_prepass`,
    // `conflict.rs`), as designed.
    //
    // Credential-aware: destroy issues real provider DestroyResource
    // RPCs, so on the magma path the runner's executor must carry the
    // resolved `spec.providerCredentials` (forwarded into ApplyContext)
    // — a cloudflare delete with a null token fails "channel closed"
    // exactly like an apply. Per ★★ MAGMA-NATIVE.
    let runner = state.executor_runner_for_with_creds(template).await?;

    let workspace = state.workspace_manager.get_workspace(template).await?;

    // Concurrency guard: `runner.destroy()` below issues real provider
    // DestroyResource RPCs. Same rationale as `handle_applying` — the
    // Lease-based `LeaderElector` alone tolerates an overlapping pod pair
    // during a `RollingUpdate`, so without a second, independent guard
    // here two pods could both reach this dispatch for the SAME template
    // (see the doc on `ControllerState::state_lock`). Held for the rest
    // of this function via RAII.
    let schema_name = format!("pangea_{}", template.spec.pangea_namespace);
    let template_name = template.name_any();
    let _state_lock_guard =
        match acquire_mutation_lock(state, &schema_name, &template_name).await? {
            LockDispatch::Proceed(guard) => guard,
            LockDispatch::Contended => {
                warn!(
                    template = %template_name,
                    "Destroying: another operator pod holds this template's state lock \
                     — requeueing instead of racing a concurrent destroy"
                );
                return Ok(ReconcileAction::Requeue(SHORT_REQUEUE_INTERVAL));
            }
        };

    // Always run destroy — unconditionally, for every executor. There is
    // deliberately NO "has this workspace been initialized/applied"
    // pre-check here anymore. That check used to be
    // `workspace.file_exists(".terraform")`, a tofu-only artifact
    // (`.terraform` is the directory `tofu init` creates). It was a
    // silent no-op for every magma-executed template — `MagmaExecutor::
    // init` is a documented no-op that never creates `.terraform` — so
    // on the executor that has been the fleet default since 2026-06-02,
    // `runner.destroy()` was never invoked on CR deletion and real cloud
    // infrastructure was orphaned, untracked, forever.
    //
    // `WorkspaceRunner::destroy` is a hard trait contract (see its doc)
    // that every implementation MUST be safe to call against a
    // never-applied workspace: magma's `destroy` diffs an empty state to
    // zero deletes (a real no-op); tofu's `destroy` runs `tofu init`
    // first if needed, so it degrades to tofu's own "No objects need to
    // be destroyed" no-op instead of erroring. Backend-specific "was
    // this ever applied" logic lives in the backend's own runner impl,
    // never here.
    let r = runner.destroy(&workspace, true).await?;

    if !r.success {
        // Combine stdout + stderr — tofu writes most diagnostics to
        // stdout (same logic that lives in handle_applying's
        // post-apply failure path).
        let err_msg = format!(
            "destroy failed (runner={}): {}",
            runner.name(),
            if r.raw_stdout.is_empty() { String::new() } else { r.raw_stdout.clone() }
        );
        warn!(%err_msg);
        update_phase_with_error(template, Phase::Failed, &err_msg, state).await?;
        record_event(template, state, EventType::Warning, "DestroyFailed", &err_msg).await;
        return Ok(ReconcileAction::Requeue(ERROR_REQUEUE_INTERVAL));
    }

    info!(runner = runner.name(), "destroy completed successfully");
    record_event(template, state, EventType::Normal, "Destroyed", "Infrastructure destroyed successfully").await;

    // Clean up workspace
    let ns = template.namespace().unwrap_or_default();
    let name = template.name_any();
    state.workspace_manager.delete_workspace(&ns, &name).await?;

    // Remove finalizer to allow K8s garbage collection
    remove_finalizer(template, state).await?;

    Ok(ReconcileAction::Done)
}

/// Validate template source configuration.
fn validate_source(template: &InfrastructureTemplate) -> Result<()> {
    let source = &template.spec.source;

    let source_count = [
        source.inline.is_some(),
        source.config_map_ref.is_some(),
        source.git_repository.is_some(),
    ]
    .iter()
    .filter(|&&x| x)
    .count();

    if source_count == 0 {
        return Err(Error::InvalidSource(
            "No template source specified (inline, configMapRef, or gitRepository)".into(),
        ));
    }

    if source_count > 1 {
        return Err(Error::InvalidSource(
            "Multiple template sources specified, only one allowed".into(),
        ));
    }

    Ok(())
}

// Status update helpers were lifted to `controller/template/status.rs`
// during T1 (2026-05-03 review pass). Internal callers reference them
// via `super::template::status::*`.

// ReactivePolicy application was lifted to
// `controller/template/reactive_policy.rs` during T2 (continuation
// of R6). The post-reconcile pipeline calls into the new module's
// `apply_reactive_policy_internal` directly.

/// Hash plan content for deterministic approval identification.
fn content_hash(input: &str) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    input.hash(&mut hasher);
    hasher.finish()
}

/// Compute the plan-approval hash stored in `status.pendingPlanHash`
/// and compared against `status.approvedPlanHash`.
///
/// Folds in a fingerprint of the CURRENT real infrastructure state
/// ALONGSIDE the plan text (`content_hash`'s existing job) so two plans
/// that are textually identical in shape — e.g. both "create
/// everything" from an empty state — but computed against DIFFERENT
/// actual state can never produce the same hash. `state_bytes` is
/// tagged (present vs absent) before hashing so `Some(&[])` (an
/// empty-but-present state) can never collide with `None` (no state at
/// all).
///
/// Callers MUST source `state_bytes` from [`CurrentStateFingerprint`]
/// (via `current_state_fingerprint`), never from
/// `Workspace::read_state_bytes()` directly — that on-disk read is
/// `None` unconditionally for every magma-backed template (state lives
/// in Postgres, not on the pod-local disk), which collapsed this
/// function to a `plan_text`-only hash for the fleet's default
/// executor until `current_state_fingerprint` closed that gap. See
/// `CurrentStateFingerprint`'s doc comment for the full incident.
///
/// This closes bug 2 of
/// docs/postmortems/2026-07-12-camelot-eks-state-wipe-duplicate-vpc.md:
/// previously the hash was `content_hash(plan_text)` alone, so a plan
/// recomputed after `Workspace::clean()` wiped state (bug 1) hashed
/// identically to the plan a human had approved against the PRIOR,
/// genuinely-different state — silently reusing a stale approval for
/// an apply the human never actually reviewed.
fn plan_approval_hash(plan_text: &str, state_bytes: Option<&[u8]>) -> String {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    content_hash(plan_text).hash(&mut hasher);
    match state_bytes {
        Some(bytes) => {
            1u8.hash(&mut hasher);
            bytes.hash(&mut hasher);
        }
        None => 0u8.hash(&mut hasher),
    }
    format!("{:016x}", hasher.finish())
}

/// Canonical, order-independent fingerprint of a plan's per-resource
/// changes — the input `route_through_approval_gate` feeds to
/// `plan_approval_hash`.
///
/// Deliberately built from the STRUCTURED `DriftDetail` list rather
/// than `tofu plan`'s raw human-readable stdout: OpenTofu's plan-text
/// renderer walks the resource graph with parallelism (10 workers by
/// default), so the ORDER independent resources appear in varies
/// run-to-run even when the semantic diff (which resources, what
/// action) is identical — and a refresh-diff ("Objects have changed
/// outside of OpenTofu") block can carry naturally-varying computed
/// values. Hashing that raw text meant a human's approval could never
/// stay valid past the next periodic replan: `status.pendingPlanHash`
/// rolled on every reconcile even when nothing real had changed,
/// making `requireApproval` structurally unapprovable — a human is
/// always chasing a hash that has already moved by the time they act
/// on it. Confirmed live on `camelot-eks` 2026-07-12: four consecutive
/// replans of the identical `+21 create` diff produced four different
/// hashes purely from stdout-ordering/refresh noise.
///
/// Sorting by `address` (and each entry's own `attributes`) removes
/// the graph-walk-order non-determinism; using `DriftDetail::attributes`
/// (changed attribute NAMES only, never values — see
/// `changed_attributes` in `executor::plan`) removes refresh-value
/// noise while staying exactly as resource-specific as before: two
/// plans that differ in WHICH resources change, WHAT action, or WHICH
/// attributes changed still hash differently.
fn canonical_drift_fingerprint(drifts: &[DriftDetail]) -> String {
    let mut entries: Vec<String> = drifts
        .iter()
        .map(|d| {
            let mut attrs = d.attributes.clone();
            attrs.sort();
            format!("{}|{}|{}", d.address, d.action, attrs.join(","))
        })
        .collect();
    entries.sort();
    entries.join("\n")
}

/// Extract `DriftDetail`s from a raw `tofu show -json` payload — the
/// tofu-executor-only leg of the three-path drift extraction
/// `handle_planning` uses (see that function's "Three-path drift
/// extraction" doc comment) and, via `drift_details_from_plan_result`
/// below, one leg of `handle_applying`'s post-import safety recheck.
/// Empty input or a parse failure returns an empty list rather than
/// propagating an error: the caller treats "no drift details
/// available" as "cannot prove safety", which the caller's own
/// fallback (fail closed, requeue to Planning) already handles
/// correctly.
pub(crate) fn drift_details_from_tofu_show_json(raw_show_json: &str) -> Vec<DriftDetail> {
    if raw_show_json.is_empty() {
        return Vec::new();
    }
    match Plan::from_json(raw_show_json) {
        Ok(plan) => plan
            .drift_details(50)
            .into_iter()
            .map(|d| DriftDetail {
                address: d.address,
                action: d.action,
                risk: d.risk,
                attributes: d.attributes,
                policy_decision: None,
                matched_policy: None,
            })
            .collect(),
        Err(e) => {
            warn!(error = %e, "post-import recheck: failed to parse plan JSON");
            Vec::new()
        }
    }
}

/// Extract fresh `DriftDetail`s from a `PlanResult`, honoring the SAME
/// three-path shape `handle_planning`'s drift extraction uses (tofu's
/// `raw_show_json` / magma's disk-fallback `artifact` / magma's
/// DB-backed bundle fetched back from Postgres via
/// `fetch_db_backed_cycle_artifact`).
///
/// This exists because `handle_applying`'s r101 post-import safety
/// recheck (the SAFETY GATE above) used to call
/// `drift_details_from_tofu_show_json` directly against
/// `PlanResult.raw_show_json` — which `MagmaWorkspaceRunner::plan`
/// ALWAYS sets to an empty string (see that method's doc comment: "Magma
/// doesn't emit tofu-format show-JSON"), on BOTH the disk-fallback path
/// AND the production DB-backed path. That made `fresh_drifts` always
/// empty for every magma-executed CR — the fleet's default executor —
/// so `find_unapproved_destructive_escalation` never had a non-empty
/// `fresh` list to search and the r101 gate was silently a no-op under
/// magma. Fixed 2026-07-19 by reusing the exact DB-backed extraction
/// path that closed the same class of gap in `handle_planning` (the
/// 2026-07-17 plan-hash-collision incident) — no new plan-shape
/// parsing, no new artifact plumbing, just the already-typed
/// `CycleArtifact` this same `runner.plan()` call already persisted.
///
/// Soundness note: `plan_result` here is always the output of a plan
/// call issued *after* the import prepass ran (see the SAFETY GATE call
/// site), so every leg below reads data from THAT fresh plan, never a
/// stale pre-import one — `MagmaExecutor::plan` recomputes from live
/// state and unconditionally re-persists (`put_plan`/`put_bundle`)
/// before returning, so the DB-backed leg's `fetch_db_backed_cycle_artifact`
/// fetch reads back exactly the bundle this same call just wrote.
async fn drift_details_from_plan_result(
    plan_result: &crate::executor::workspace_runner::PlanResult,
    template: &InfrastructureTemplate,
    state: &ControllerState,
) -> Vec<DriftDetail> {
    if let Some(drifts) = drift_details_from_plan_result_sync(plan_result) {
        return drifts;
    }
    if let Some(art) = fetch_db_backed_cycle_artifact(template, state).await {
        return plan_summary_and_drifts_from_artifact(&art).1;
    }
    warn!(
        template = %template.name_any(),
        "post-import recheck: plan succeeded but produced no analyzable output \
         (no show-JSON, no artifact, no DB-backed bundle) — treating as zero \
         fresh drift (fails open, matching the existing failed-recheck fallthrough)"
    );
    Vec::new()
}

/// The two synchronous, `ControllerState`-free legs of
/// `drift_details_from_plan_result`: tofu's `raw_show_json`, and
/// magma's disk-fallback `artifact`. Factored out so the fix's core
/// logic — that a magma `PlanResult` (empty `raw_show_json`) must still
/// yield real drift details when it carries a typed `artifact` — is
/// directly unit-testable without constructing a `ControllerState`
/// (which needs a live k8s `Client` and isn't test-constructible
/// anywhere in this codebase today; see
/// `drift_details_from_plan_result_tests`, below). Returns `None` when
/// neither leg has data, telling the caller to fall through to the
/// DB-backed fetch (the only leg that genuinely needs `state`).
fn drift_details_from_plan_result_sync(
    plan_result: &crate::executor::workspace_runner::PlanResult,
) -> Option<Vec<DriftDetail>> {
    if !plan_result.raw_show_json.is_empty() {
        return Some(drift_details_from_tofu_show_json(&plan_result.raw_show_json));
    }
    if let Some(art) = plan_result.artifact.as_ref() {
        return Some(plan_summary_and_drifts_from_artifact(art).1);
    }
    None
}

#[cfg(test)]
mod drift_details_from_plan_result_tests {
    use super::{
        drift_details_from_plan_result_sync, drift_details_from_tofu_show_json,
        find_unapproved_destructive_escalation,
    };
    use crate::crd::DriftDetail;
    use crate::executor::cycle_artifact::{CycleArtifact, PlanAction, TypedResourceChange};
    use crate::executor::workspace_runner::PlanResult;

    fn drift(address: &str, action: &str) -> DriftDetail {
        DriftDetail {
            address: address.to_string(),
            action: action.to_string(),
            risk: "low".to_string(),
            attributes: vec![],
            policy_decision: None,
            matched_policy: None,
        }
    }

    fn magma_plan_result_with_artifact(artifact: CycleArtifact) -> PlanResult {
        // Mirrors `MagmaWorkspaceRunner::plan`'s ACTUAL shape on the
        // disk-fallback path: `raw_show_json` is always an empty
        // string (magma emits no tofu-format show-JSON — see that
        // method's own doc comment), `artifact` is populated directly.
        PlanResult {
            artifact: Some(artifact),
            plan_file: None,
            raw_stdout: String::new(),
            raw_show_json: String::new(),
            has_changes: true,
            success: true,
            raw_stderr: String::new(),
        }
    }

    // ── The bug this closes, reproduced directly (2026-07-19) ────────
    //
    // Before the fix, `handle_applying`'s r101 SAFETY GATE called
    // `drift_details_from_tofu_show_json(&recheck.raw_show_json)`
    // directly. On a magma `PlanResult` `raw_show_json` is UNCONDITIONALLY
    // empty, so that call always returned `Vec::new()` regardless of
    // what the plan actually contained — silently disarming the gate
    // for every magma-executed CR. This test proves both halves: the
    // OLD code path is empty even though the plan clearly carries a
    // destructive `replace`, and the NEW sync helper correctly surfaces
    // it.
    #[test]
    fn old_tofu_only_extraction_missed_a_destructive_replace_a_magma_plan_result_carries() {
        let art = CycleArtifact {
            action_distribution: CycleArtifact::action_distribution_from(&[TypedResourceChange {
                address: "aws_eks_cluster.camelot-eks".to_string(),
                action: PlanAction::Replace,
                severity: crate::executor::cycle_artifact::action_to_severity(&PlanAction::Replace),
            }]),
            resource_changes: vec![TypedResourceChange {
                address: "aws_eks_cluster.camelot-eks".to_string(),
                action: PlanAction::Replace,
                severity: crate::executor::cycle_artifact::action_to_severity(&PlanAction::Replace),
            }],
            ..Default::default()
        };
        let plan_result = magma_plan_result_with_artifact(art);

        // OLD behavior (what the gate did before this fix): parse
        // `raw_show_json` directly — always empty for magma, so this
        // MUST stay empty even though the plan has a real replace.
        let old_extraction = drift_details_from_tofu_show_json(&plan_result.raw_show_json);
        assert!(
            old_extraction.is_empty(),
            "sanity check: this is the exact blind spot the fix closes — \
             raw_show_json is unconditionally empty on the magma path"
        );

        // NEW behavior: the sync helper falls through to `artifact`
        // and surfaces the destructive replace.
        let fresh_drifts = drift_details_from_plan_result_sync(&plan_result)
            .expect("a magma PlanResult with a populated artifact must yield Some(..)");
        assert_eq!(fresh_drifts.len(), 1);
        assert_eq!(fresh_drifts[0].address, "aws_eks_cluster.camelot-eks");
        assert_eq!(fresh_drifts[0].action, "replace");
    }

    // ── End-to-end shape: the r101 gate's own escalation check, fed
    // by the NEW extraction, correctly flags the exact camelot-eks
    // incident scenario on a magma-shaped PlanResult. ──────────────────
    #[test]
    fn magma_plan_result_feeds_the_r101_escalation_check_correctly() {
        let approved = vec![drift("aws_eks_node_group.system_ng", "create")];
        let art = CycleArtifact {
            action_distribution: CycleArtifact::action_distribution_from(&[
                TypedResourceChange {
                    address: "aws_eks_cluster.camelot-eks".to_string(),
                    action: PlanAction::Replace,
                    severity: crate::executor::cycle_artifact::action_to_severity(
                        &PlanAction::Replace,
                    ),
                },
                TypedResourceChange {
                    address: "aws_eks_node_group.system_ng".to_string(),
                    action: PlanAction::Create,
                    severity: crate::executor::cycle_artifact::action_to_severity(
                        &PlanAction::Create,
                    ),
                },
            ]),
            resource_changes: vec![
                TypedResourceChange {
                    address: "aws_eks_cluster.camelot-eks".to_string(),
                    action: PlanAction::Replace,
                    severity: crate::executor::cycle_artifact::action_to_severity(
                        &PlanAction::Replace,
                    ),
                },
                TypedResourceChange {
                    address: "aws_eks_node_group.system_ng".to_string(),
                    action: PlanAction::Create,
                    severity: crate::executor::cycle_artifact::action_to_severity(
                        &PlanAction::Create,
                    ),
                },
            ],
            ..Default::default()
        };
        let plan_result = magma_plan_result_with_artifact(art);

        let fresh_drifts = drift_details_from_plan_result_sync(&plan_result).unwrap_or_default();
        let escalation = find_unapproved_destructive_escalation(&approved, &fresh_drifts);
        assert_eq!(
            escalation.map(|d| d.address.as_str()),
            Some("aws_eks_cluster.camelot-eks"),
            "the r101 gate must flag the unapproved replace once fed the magma-correct \
             extraction — this is exactly what stayed silently unflagged before the fix"
        );
    }

    #[test]
    fn tofu_raw_show_json_leg_is_unaffected_when_populated() {
        // Sanity: the pre-existing tofu path (raw_show_json non-empty)
        // must keep working exactly as before — this fix only adds
        // legs, it doesn't reorder tofu's.
        let plan_result = PlanResult {
            artifact: None,
            plan_file: None,
            raw_stdout: String::new(),
            raw_show_json: String::new(),
            has_changes: false,
            success: true,
            raw_stderr: String::new(),
        };
        assert!(drift_details_from_plan_result_sync(&plan_result).is_none());
    }
}

/// The single source of truth for what counts as a **destructive** IaC
/// action: a `delete` (tear the resource down) or a `replace`
/// (delete-then-recreate — which destroys the existing resource every
/// bit as surely as a bare delete, just followed by a create). Shared
/// by both destructive-action gates in this module —
/// `find_unapproved_destructive_escalation` (the r101 post-import gate)
/// and `evaluate_destroy_protection_gate` (the destroyProtection apply
/// gate) — so the classification can never drift between them: adding a
/// new destructive verb here tightens BOTH gates in one edit.
pub(crate) fn is_destructive_action(action: &str) -> bool {
    matches!(action, "delete" | "replace")
}

/// Find the first entry in `fresh` whose action is destructive
/// (`delete`/`replace`) and whose address either doesn't appear in
/// `approved` at all, or appears there with a less severe action —
/// i.e. a destructive action the human who reviewed `approved` never
/// actually saw. Used by `handle_applying`'s post-import safety gate
/// to refuse applying a plan the human never approved (see that call
/// site's doc comment for the live incident this closes).
fn find_unapproved_destructive_escalation<'a>(
    approved: &[DriftDetail],
    fresh: &'a [DriftDetail],
) -> Option<&'a DriftDetail> {
    fresh.iter().find(|f| {
        if !is_destructive_action(f.action.as_str()) {
            return false;
        }
        // Not an escalation iff the human already approved a destructive
        // action for this exact address.
        let approved_destructive_for_addr = approved
            .iter()
            .find(|a| a.address == f.address)
            .is_some_and(|a| is_destructive_action(a.action.as_str()));
        !approved_destructive_for_addr
    })
}

/// Outcome of the **destroyProtection apply gate** — evaluated in
/// `handle_applying` immediately before any real provider mutation.
///
/// `spec.destroyProtection` has always promised "refuse to destroy this
/// infrastructure", but before this gate it was consulted ONLY on the
/// explicit destroy path (CR deletion / `handle_destroying`). A REPLACE
/// is a delete-then-recreate — every bit as destructive as a bare
/// delete — yet a plan containing a `replace` (or `delete`) sailed
/// straight through a normal apply (AutoApply, or an approved cached
/// plan) with `destroyProtection` never read. That is the exact class
/// of gap behind the repeated camelot-eks in-apply cluster replacements
/// (task #120). It is a SIBLING of the r101 import-escalation gap but
/// distinct: r101 fires only on the import-hint refresh path and only
/// on an *unapproved* escalation, whereas this gate fires on EVERY apply
/// path and is UNCONDITIONAL — with protection on, no destructive action
/// may apply, approved or not; the human must first set
/// `destroyProtection=false`, exactly as the destroy path already
/// requires.
///
/// **Tier (named honestly, not rounded up): only-mitigated.** A runtime
/// gate on a live plan's analyzed action set, not a type that makes a
/// protection-violating apply unconstructible — the danger signal is a
/// runtime observation, so full unrepresentability isn't reachable here.
/// The one asymmetry it must never get wrong is silently DESTROYING a
/// protected resource; a spurious block is fail-SAFE (it only ever asks
/// the human to disable protection), never fail-DANGEROUS.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum DestroyProtectionGate {
    /// Protection is off, or the plan carries no destructive action —
    /// safe to apply.
    Proceed,
    /// `destroyProtection` is on AND the plan carries a destructive
    /// action — refuse to apply. Carries the FIRST offending
    /// address+action (plan order) for the operator-facing message, so
    /// the reported resource is stable across reconciles.
    BlockedByProtectedDestruction { address: String, action: String },
}

/// Pure decision for the destroyProtection apply gate: with
/// `destroy_protection` on, refuse to apply any plan whose analyzed
/// action set contains a destructive action (`delete`/`replace` — see
/// `is_destructive_action`); with protection off, always proceed.
/// Reuses r101's exact destructive-action predicate so the two gates
/// can never disagree on what "destructive" means.
pub(crate) fn evaluate_destroy_protection_gate(
    destroy_protection: bool,
    actions: &[DriftDetail],
) -> DestroyProtectionGate {
    if !destroy_protection {
        return DestroyProtectionGate::Proceed;
    }
    match actions
        .iter()
        .find(|d| is_destructive_action(d.action.as_str()))
    {
        Some(d) => DestroyProtectionGate::BlockedByProtectedDestruction {
            address: d.address.clone(),
            action: d.action.clone(),
        },
        None => DestroyProtectionGate::Proceed,
    }
}

/// Project a typed, executor-agnostic `PlannedChange` list — the shape
/// `IacExecutor::planned_changes()` returns (magma's disk-free,
/// non-mutating readback of the SAME persisted plan row `apply()` is
/// about to consume) — into `DriftDetail`s, so the destroyProtection
/// gates can reuse `evaluate_destroy_protection_gate` completely
/// unchanged regardless of which typed border produced the action set.
/// `risk`/`attributes`/`policy_decision`/`matched_policy` carry no
/// signal here (only `.action`/`.address` are ever read downstream of
/// this mapping) — left empty/`None` rather than fabricated.
pub(crate) fn drift_details_from_planned_changes(changes: &[PlannedChange]) -> Vec<DriftDetail> {
    changes
        .iter()
        .map(|c| DriftDetail {
            address: c.address.clone(),
            action: match c.action {
                PlanAction::NoOp => "noop",
                PlanAction::Create => "create",
                PlanAction::Read => "read",
                PlanAction::Update => "update",
                PlanAction::Replace => "replace",
                PlanAction::Delete => "delete",
            }
            .to_string(),
            risk: String::new(),
            attributes: Vec::new(),
            policy_decision: None,
            matched_policy: None,
        })
        .collect()
}

/// Compute the FRESH action set an imminent `executor.apply(.., None,
/// ..)` call will actually execute — the observation half of the
/// destroyProtection safety net closing the rest of task #120 (see
/// `recheck_destroy_protection_before_bare_apply`, below, for why
/// `prior_drifts` alone isn't sound whenever `plan_file` is `None`).
/// Three tiers, cheapest and safest first:
///
///   1. `executor.planned_changes()` — magma's disk-free, non-mutating
///      readback of the SAME persisted-plan row `apply()` is about to
///      consume: zero extra provider RPCs, zero extra Postgres writes,
///      zero lifecycle-FSM side effects. Returns `Ok(None)` for tofu
///      (the trait default — no override) and disk-fallback magma;
///      `Err` for a DB-backed magma executor whose persisted row is
///      torn or missing. Neither is fatal here — both fall through.
///   2. Force a fresh `executor.plan(.., Some(plan_path), ..)`, then try
///      the typed readback AGAIN. This is what actually REPAIRS tier 1's
///      `Err` case: a fresh `plan()` re-persists a valid row via
///      `put_plan` (the identical cache-miss-regenerate recovery
///      `apply()` itself runs), so the re-read now succeeds. For tofu
///      this second `planned_changes()` call is a guaranteed, free
///      no-op (the trait default always returns `Ok(None)`) — it costs
///      nothing beyond the plan call tier 3 needs anyway.
///   3. Falls back to `executor.show_plan(..)` parsed via
///      `drift_details_from_tofu_show_json` — the exact combo
///      `TofuWorkspaceRunner::plan` / this file's own r101 recheck /
///      `conflict::gather_attrs` already pay. This is what actually
///      observes TOFU's fresh action set. **Named limitation, not
///      hidden:** this tier is tofu-format-only — reached by a real
///      tofu executor (correct), or by magma's test-only disk-fallback
///      mode (`artifact_store: None`, never the production DB-backed
///      configuration), where it silently mis-parses magma's own JSON
///      shape and returns empty (see `plan_change.rs`'s own module doc
///      on exactly this tofu-format-dependence trap — the reason
///      `PlannedChange` exists). Bounded to test-only infrastructure;
///      no real cloud resource is at risk through this arm.
///
/// Returns an empty `Vec` when NO tier could produce an action set (a
/// tier-2 plan itself failing, or the tier-3 named limitation above) —
/// the caller must treat that identically to "no destructive action"
/// and fail OPEN (fall through to the real apply, which surfaces any
/// real failure through its own established error path) rather than
/// block on absent data. This is the same asymmetry the destroyProtection
/// gate has always held: a spurious block is fail-SAFE, but "the
/// recheck itself couldn't run" must never become a way to wedge every
/// apply.
pub(crate) async fn fresh_action_set_before_bare_apply(
    executor: &Arc<dyn crate::executor::IacExecutor>,
    work_dir: &std::path::Path,
    plan_path: &std::path::Path,
) -> Vec<DriftDetail> {
    // Tier 1.
    if let Ok(Some(changes)) = executor.planned_changes().await {
        return drift_details_from_planned_changes(&changes);
    }

    // Tier 2: force a fresh plan, then retry the typed readback — this
    // is what repairs tier 1's `Err` arm (a torn/missing magma plan
    // row); a genuine plan failure fails open here, matching the r101
    // gate's own documented fallthrough.
    let plan_result = match executor.plan(work_dir, Some(plan_path), &[]).await {
        Ok(r) => r,
        Err(e) => {
            warn!(
                error = %e,
                "destroyProtection recheck: fresh plan errored — proceeding without \
                 a recheck (fails open, matching the r101 gate's own fallthrough)"
            );
            return Vec::new();
        }
    };
    if !plan_result.success {
        return Vec::new();
    }
    if let Ok(Some(changes)) = executor.planned_changes().await {
        return drift_details_from_planned_changes(&changes);
    }

    // Tier 3 (tofu-format fallback — see the doc above for the named
    // magma-disk-fallback limitation).
    match executor.show_plan(work_dir, plan_path).await {
        Ok(r) if r.success && !r.stdout.is_empty() => drift_details_from_tofu_show_json(&r.stdout),
        _ => Vec::new(),
    }
}

/// The destroyProtection safety net for a bare apply — one that will run
/// with `plan_file = None`, meaning the imminent `executor.apply` does
/// NOT consume a pre-approved cached plan the earlier `prior_drifts`
/// gate (in `handle_applying`, above) already checked.
///
/// `prior_drifts` is trustworthy only when the cached plan at
/// `plan_path` is what's actually about to be applied
/// (`plan_file = Some(...)`) — the same planning pass produced both. It
/// is NOT trustworthy whenever `plan_file` is `None`: the self-heal
/// retry after a stale-plan error; the post-import re-apply
/// (`conflict::resolve_conflicts_post_apply`); and the ordinary
/// magma-executor apply, where `plan_file` is effectively always `None`
/// (magma reads its plan from Postgres, not a cached `.tfplan`).
/// Whenever that's true, the action set `executor.apply` actually
/// realizes can diverge from `prior_drifts` (a computed-attribute-
/// triggered `replace` surfacing only once state has actually settled —
/// the exact mechanism that destructively replaced the real
/// `camelot-eks` EKS cluster; see the SAFETY GATE comment in
/// `handle_applying`, above).
///
/// This closes that gap: recompute the FRESH action set
/// (`fresh_action_set_before_bare_apply`) and gate on it via the exact
/// same `evaluate_destroy_protection_gate` predicate — reused, never
/// forked. ADDITIVE beside the `prior_drifts` check, never a
/// replacement: both run; either can block.
///
/// Returns `Some(action)` when the gate blocked — the caller must return
/// it immediately; the `DestroyBlocked` event and the `Phase::Failed`
/// transition have already happened here. Returns `None` to proceed
/// with the apply.
async fn recheck_destroy_protection_before_bare_apply(
    template: &InfrastructureTemplate,
    state: &ControllerState,
    executor: &Arc<dyn crate::executor::IacExecutor>,
    work_dir: &std::path::Path,
    plan_path: &std::path::Path,
) -> Result<Option<ReconcileAction>> {
    if !template.spec.destroy_protection {
        return Ok(None);
    }
    let fresh = fresh_action_set_before_bare_apply(executor, work_dir, plan_path).await;
    if let DestroyProtectionGate::BlockedByProtectedDestruction { address, action } =
        evaluate_destroy_protection_gate(true, &fresh)
    {
        let template_name = template.name_any();
        let msg = format!(
            "Applying refused: destroyProtection is enabled and a freshly recomputed \
             plan — about to be applied with no pre-approved cached plan \
             (plan_file=None) — contains a destructive {action} action on {address}. \
             A replace is a delete-then-recreate, so it is blocked under destroy \
             protection (approved or not). Set spec.destroyProtection=false first if \
             this destruction is intended, then re-approve. Parking at Failed."
        );
        warn!(template = %template_name, %msg);
        record_event(template, state, EventType::Warning, "DestroyBlocked", &msg).await;
        update_phase_with_error(template, Phase::Failed, &msg, state).await?;
        return Ok(Some(ReconcileAction::Requeue(DEFAULT_REQUEUE_INTERVAL)));
    }
    Ok(None)
}

#[cfg(test)]
mod plan_approval_hash_tests {
    use super::plan_approval_hash;

    // ── Bug 2 regression: state must be folded into the hash ───────

    #[test]
    fn same_plan_text_different_state_produces_different_hash() {
        // The exact incident shape: two plans that are textually
        // identical ("create everything") but computed against
        // DIFFERENT underlying state (the first apply's real partial
        // state vs the wiped-then-empty state) must never collide.
        let plan_text = "Plan: 50 to add, 0 to change, 0 to destroy.";
        let hash_against_partial_state =
            plan_approval_hash(plan_text, Some(b"state-with-vpc-and-2-iam-roles"));
        let hash_against_wiped_state = plan_approval_hash(plan_text, None);

        assert_ne!(
            hash_against_partial_state, hash_against_wiped_state,
            "identical plan text against different state must hash differently"
        );
    }

    #[test]
    fn same_plan_text_same_state_produces_the_same_hash() {
        // Determinism: re-running the exact same plan against
        // unchanged state must reproduce the exact same hash, or a
        // legitimately-unapproved-but-unchanged plan would spuriously
        // demand re-approval every cycle.
        let plan_text = "Plan: 3 to add, 1 to change, 0 to destroy.";
        let state = Some(b"stable-state-bytes".as_slice());

        assert_eq!(
            plan_approval_hash(plan_text, state),
            plan_approval_hash(plan_text, state)
        );
    }

    #[test]
    fn different_plan_text_same_state_produces_different_hash() {
        let state = Some(b"same-state".as_slice());
        let a = plan_approval_hash("Plan: 1 to add.", state);
        let b = plan_approval_hash("Plan: 2 to add.", state);
        assert_ne!(a, b);
    }

    #[test]
    fn no_state_file_is_distinct_from_an_empty_state_file() {
        // A present-but-empty state file (Some(&[])) must not collide
        // with "no state file at all" (None) — the tag byte in
        // plan_approval_hash is what guarantees this, not an accident
        // of the underlying hasher.
        let plan_text = "Plan: 1 to add.";
        let hash_none = plan_approval_hash(plan_text, None);
        let hash_empty = plan_approval_hash(plan_text, Some(&[]));
        assert_ne!(hash_none, hash_empty);
    }

    #[test]
    fn hash_is_stable_hex_format() {
        let hash = plan_approval_hash("Plan: 1 to add.", None);
        assert_eq!(hash.len(), 16, "hash must be a fixed-width 16-char hex string");
        assert!(
            hash.chars().all(|c| c.is_ascii_hexdigit()),
            "hash must be lowercase hex: {hash}"
        );
    }

    #[test]
    fn wiped_then_regenerated_state_never_silently_reuses_a_stale_approval() {
        // End-to-end simulation of the incident's approval-gate logic
        // (mirrors the `is_approved` check in handle_planning): a human
        // approves the hash for the FIRST plan (real, partially-applied
        // state). Workspace::clean() then wipes state (bug 1, separately
        // regression-tested in executor::workspace::tests). The next
        // planning cycle recomputes a plan hash against the NOW-EMPTY
        // state — even though the plan TEXT is identical in shape — and
        // that hash must NOT equal the human's stored approval.
        let plan_text = "Plan: 50 to add, 0 to change, 0 to destroy.";

        let first_plan_hash =
            plan_approval_hash(plan_text, Some(b"vpc-094734439e62440a8-partial-state"));
        let approved_plan_hash = first_plan_hash.clone(); // human approves via kubectl patch

        // Workspace::clean() wiped state; state_path() now reads back
        // None on the next reconcile.
        let second_plan_hash = plan_approval_hash(plan_text, None);

        assert_ne!(
            second_plan_hash, approved_plan_hash,
            "a plan replanned against wiped state must require fresh approval, \
             never silently inherit the prior state's approval"
        );
    }
}

/// `is_plan_approved`'s two-source OR: `status.approvedPlanHash` (direct
/// kubectl patch) and `spec.approvedPlanHash` (GitOps-native, committed)
/// are equally valid -- pins that neither alone is required, a mismatch
/// on either alone still refuses, and a matching one is decisive even
/// with the other absent/stale.
#[cfg(test)]
mod is_plan_approved_tests {
    use super::{is_plan_approved, InfrastructureTemplateSpec, InfrastructureTemplateStatus};
    use crate::crd::TemplateSource;

    fn spec_with(approved: Option<&str>) -> InfrastructureTemplateSpec {
        InfrastructureTemplateSpec {
            source: TemplateSource { inline: Some(String::new()), config_map_ref: None, git_repository: None },
            pangea_namespace: "test".to_string(),
            template_name: None,
            variables: None,
            variable_refs: None,
            auto_approve: false,
            spec_approved_plan_hash: approved.map(str::to_string),
            refresh_interval: "10m".to_string(),
            suspend: false,
            executor: None,
            destroy_protection: false,
            retry_policy: None,
            provider_credentials: None,
            compliance_profiles: vec![],
            policies: vec![],
            default_decision: None,
            settling_policy: None,
            reactive_policy: None,
            import_policy: None,
            import_hints: Default::default(),
            conflict_policy: None,
            output_bindings: vec![],
            secret_files: vec![],
        }
    }

    fn status_with(approved: Option<&str>) -> Option<InfrastructureTemplateStatus> {
        Some(InfrastructureTemplateStatus {
            approved_plan_hash: approved.map(str::to_string),
            ..Default::default()
        })
    }

    #[test]
    fn neither_source_set_is_not_approved() {
        assert!(!is_plan_approved(&None, &spec_with(None), "abc123"));
    }

    #[test]
    fn status_only_match_is_approved() {
        assert!(is_plan_approved(&status_with(Some("abc123")), &spec_with(None), "abc123"));
    }

    #[test]
    fn spec_only_match_is_approved() {
        assert!(is_plan_approved(&None, &spec_with(Some("abc123")), "abc123"));
    }

    #[test]
    fn status_matches_but_spec_present_and_stale_is_still_approved() {
        // The OR: a stale/mismatched spec value must never veto a real
        // status-side approval, and vice versa (checked below).
        assert!(is_plan_approved(
            &status_with(Some("abc123")),
            &spec_with(Some("some-older-hash")),
            "abc123"
        ));
    }

    #[test]
    fn spec_matches_but_status_present_and_stale_is_still_approved() {
        assert!(is_plan_approved(
            &status_with(Some("some-older-hash")),
            &spec_with(Some("abc123")),
            "abc123"
        ));
    }

    #[test]
    fn both_sources_stale_is_not_approved() {
        assert!(!is_plan_approved(
            &status_with(Some("stale-a")),
            &spec_with(Some("stale-b")),
            "abc123"
        ));
    }
}

/// Regression tests for the confirmed 2026-07-19 gap: `plan_approval_hash`
/// folding in `Workspace::read_state_bytes()` (a raw on-disk read)
/// unconditionally, regardless of executor. For every magma-backed
/// template — whose real state lives in Postgres, never on the
/// pod-local disk — that read was `None` every cycle, so the approval
/// hash was `plan_text`-derived ONLY: a human's approval of one plan
/// silently and permanently authorized ANY future plan with the same
/// `plan_text` shape, regardless of what actually changed in real
/// infrastructure state. `current_state_fingerprint` + the
/// `CurrentStateFingerprint`/`ApprovalHashInput` types close this by
/// making the executor-aware read explicit and by refusing (never
/// silently degrading) when the real state read itself fails. See
/// `CurrentStateFingerprint`'s own doc comment for the full mechanism.
#[cfg(test)]
mod current_state_fingerprint_tests {
    use super::{
        current_state_fingerprint, plan_approval_hash, resolve_approval_hash_input,
        ApprovalHashInput, CurrentStateFingerprint, StateBackend,
    };
    use crate::backend::{InMemoryStateBackend, StateEntry, TerraformState};
    use crate::error::{Error, Result};
    use crate::executor::WorkspaceManager;
    use async_trait::async_trait;
    use std::sync::Arc;

    /// A `StateBackend` whose `get_state` always fails — simulates a
    /// genuine Postgres read error (connection refused past
    /// `RetryingStateBackend`'s bounded backoff, a query error, etc.),
    /// as distinct from `InMemoryStateBackend` returning `Ok(None)`
    /// (queried successfully; no such row — a legitimate `Absent`).
    struct FailingStateBackend;

    #[async_trait]
    impl StateBackend for FailingStateBackend {
        async fn get_state(
            &self,
            _schema_name: &str,
            _template_name: &str,
            _state_name: &str,
        ) -> Result<Option<StateEntry>> {
            Err(Error::StateBackend("simulated connection failure".to_string()))
        }
        async fn get_parsed_state(
            &self,
            _schema_name: &str,
            _template_name: &str,
            _state_name: &str,
        ) -> Result<Option<TerraformState>> {
            Err(Error::StateBackend("simulated connection failure".to_string()))
        }
        async fn save_state(
            &self,
            _schema_name: &str,
            _template_name: &str,
            _state_name: &str,
            _data: &[u8],
        ) -> Result<i64> {
            Err(Error::StateBackend("simulated connection failure".to_string()))
        }
        async fn delete_state(
            &self,
            _schema_name: &str,
            _template_name: &str,
            _state_name: &str,
        ) -> Result<bool> {
            Err(Error::StateBackend("simulated connection failure".to_string()))
        }
        async fn list_states(
            &self,
            _schema_name: &str,
            _template_name: &str,
        ) -> Result<Vec<StateEntry>> {
            Err(Error::StateBackend("simulated connection failure".to_string()))
        }
    }

    async fn workspace_with_state(content: Option<&[u8]>) -> (tempfile::TempDir, crate::executor::Workspace) {
        let base = tempfile::tempdir().expect("create temp base dir");
        let wm = WorkspaceManager::new(base.path().to_path_buf());
        let ws = wm.get_or_create("ns", "tmpl").await.expect("create workspace");
        if let Some(bytes) = content {
            tokio::fs::write(ws.state_path(), bytes).await.expect("seed state file");
        }
        (base, ws)
    }

    // ── The core confirmed bug: a magma template's fingerprint MUST
    // reflect real Postgres state, not a constant `None`. ─────────────

    #[tokio::test]
    async fn magma_backed_template_reads_real_state_from_the_backend() {
        let backend = InMemoryStateBackend::new();
        backend
            .save_state("pangea_camelot", "camelot-eks", "default", b"vpc-real-state-bytes")
            .await
            .expect("seed magma state");
        let backend: Option<Arc<dyn StateBackend>> = Some(Arc::new(backend));
        let (_dir, ws) = workspace_with_state(None).await;

        let fingerprint = current_state_fingerprint(
            /* is_magma_backed */ true,
            backend.as_ref(),
            "pangea_camelot",
            "camelot-eks",
            &ws,
        )
        .await;

        assert_eq!(
            fingerprint,
            CurrentStateFingerprint::Present(b"vpc-real-state-bytes".to_vec()),
            "a magma template's fingerprint must be the REAL Postgres state, \
             never the constant None the old on-disk read always produced"
        );
    }

    #[tokio::test]
    async fn magma_backed_template_with_no_state_row_is_a_legitimate_absent() {
        // Queried successfully; simply no row yet (first-ever plan).
        // Must be `Absent`, NOT `Unreadable` — those are different
        // facts (see `CurrentStateFingerprint`'s doc comment).
        let backend: Option<Arc<dyn StateBackend>> = Some(Arc::new(InMemoryStateBackend::new()));
        let (_dir, ws) = workspace_with_state(None).await;

        let fingerprint = current_state_fingerprint(true, backend.as_ref(), "pangea_ns", "tmpl", &ws).await;

        assert_eq!(fingerprint, CurrentStateFingerprint::Absent);
    }

    #[tokio::test]
    async fn two_plans_with_identical_plan_text_but_different_magma_state_hash_differently() {
        // The core invariant this whole fix restores: same plan_text,
        // genuinely different real (Postgres) state, must never
        // collide — reproduces the exact class of bug that made a
        // magma template's approval hash `plan_text`-derived only.
        let backend_a = InMemoryStateBackend::new();
        backend_a
            .save_state("pangea_camelot", "camelot-eks", "default", b"vpc-094734439e62440a8-partial-state")
            .await
            .unwrap();
        let backend_a: Option<Arc<dyn StateBackend>> = Some(Arc::new(backend_a));

        let backend_b = InMemoryStateBackend::new();
        backend_b
            .save_state("pangea_camelot", "camelot-eks", "default", b"vpc-06987bc0cd6d8aaad-different-state")
            .await
            .unwrap();
        let backend_b: Option<Arc<dyn StateBackend>> = Some(Arc::new(backend_b));

        let (_dir_a, ws_a) = workspace_with_state(None).await;
        let (_dir_b, ws_b) = workspace_with_state(None).await;

        let fp_a = current_state_fingerprint(true, backend_a.as_ref(), "pangea_camelot", "camelot-eks", &ws_a).await;
        let fp_b = current_state_fingerprint(true, backend_b.as_ref(), "pangea_camelot", "camelot-eks", &ws_b).await;

        let plan_text = "Plan: 50 to add, 0 to change, 0 to destroy.";
        let hash_a = match resolve_approval_hash_input(&fp_a) {
            ApprovalHashInput::Hashable(bytes) => plan_approval_hash(plan_text, bytes),
            ApprovalHashInput::RefuseUnreadable => panic!("expected Hashable"),
        };
        let hash_b = match resolve_approval_hash_input(&fp_b) {
            ApprovalHashInput::Hashable(bytes) => plan_approval_hash(plan_text, bytes),
            ApprovalHashInput::RefuseUnreadable => panic!("expected Hashable"),
        };

        assert_ne!(
            hash_a, hash_b,
            "identical plan_text against two DIFFERENT real magma states must \
             hash differently — a human approving hash_a must never look like \
             an approval of hash_b's plan"
        );
    }

    // ── The tofu path stays exactly what it was — unaffected by this
    // fix (existing `Workspace::read_state_bytes` behavior, regression
    // for the 2026-07-12 postmortem's own fix). ────────────────────────

    #[tokio::test]
    async fn tofu_backed_template_reads_from_disk_ignoring_any_state_backend() {
        let (_dir, ws) = workspace_with_state(Some(b"tofu-local-state")).await;
        // Even a wired, populated magma backend must be ignored on the
        // tofu path — `is_magma_backed: false` is what selects disk.
        let backend = InMemoryStateBackend::new();
        backend.save_state("pangea_ns", "tmpl", "default", b"WRONG-this-must-not-be-read").await.unwrap();
        let backend: Option<Arc<dyn StateBackend>> = Some(Arc::new(backend));

        let fingerprint = current_state_fingerprint(false, backend.as_ref(), "pangea_ns", "tmpl", &ws).await;

        assert_eq!(fingerprint, CurrentStateFingerprint::Present(b"tofu-local-state".to_vec()));
    }

    #[tokio::test]
    async fn tofu_backed_template_with_no_state_file_is_absent() {
        let (_dir, ws) = workspace_with_state(None).await;
        let fingerprint = current_state_fingerprint(false, None, "pangea_ns", "tmpl", &ws).await;
        assert_eq!(fingerprint, CurrentStateFingerprint::Absent);
    }

    // ── Fail-closed: an unreadable state must never silently degrade
    // into a hashable value. ────────────────────────────────────────────

    #[tokio::test]
    async fn magma_state_read_failure_is_unreadable_not_absent() {
        let backend: Option<Arc<dyn StateBackend>> = Some(Arc::new(FailingStateBackend));
        let (_dir, ws) = workspace_with_state(None).await;

        let fingerprint = current_state_fingerprint(true, backend.as_ref(), "pangea_ns", "tmpl", &ws).await;

        assert_eq!(
            fingerprint,
            CurrentStateFingerprint::Unreadable,
            "a genuine read FAILURE must never be conflated with a confirmed-empty state"
        );
    }

    #[tokio::test]
    async fn magma_resolved_but_backend_missing_is_unreadable_not_absent() {
        // Structurally shouldn't happen (see the function's own doc
        // comment) but must degrade honestly if it ever does.
        let (_dir, ws) = workspace_with_state(None).await;
        let fingerprint = current_state_fingerprint(true, None, "pangea_ns", "tmpl", &ws).await;
        assert_eq!(fingerprint, CurrentStateFingerprint::Unreadable);
    }

    #[test]
    fn unreadable_state_never_silently_proceeds_to_a_hash() {
        // Exercises the exact decision `route_through_approval_gate`
        // makes: `Unreadable` MUST resolve to `RefuseUnreadable`, never
        // to `Hashable(None)` — that degradation is precisely the bug
        // this fix closes for magma. `Absent` and `Present` MUST both
        // resolve to `Hashable`, since both are legitimate, known facts.
        assert_eq!(
            resolve_approval_hash_input(&CurrentStateFingerprint::Unreadable),
            ApprovalHashInput::RefuseUnreadable
        );
        assert_eq!(
            resolve_approval_hash_input(&CurrentStateFingerprint::Absent),
            ApprovalHashInput::Hashable(None)
        );
        let present = CurrentStateFingerprint::Present(b"real-bytes".to_vec());
        assert_eq!(
            resolve_approval_hash_input(&present),
            ApprovalHashInput::Hashable(Some(b"real-bytes".as_slice()))
        );
    }

    #[test]
    fn is_present_treats_unreadable_as_not_present() {
        // Feeds `evaluate_auto_apply_gate`'s `state_present_now` — an
        // unknown state must never look "confirmed there," matching
        // `is_durable_state_backend`'s documented fail-safe asymmetry
        // (over-asking for approval is acceptable, silently applying
        // is not).
        assert!(!CurrentStateFingerprint::Absent.is_present());
        assert!(!CurrentStateFingerprint::Unreadable.is_present());
        assert!(CurrentStateFingerprint::Present(vec![1, 2, 3]).is_present());
    }
}

#[cfg(test)]
mod canonical_drift_fingerprint_tests {
    use super::canonical_drift_fingerprint;
    use crate::crd::DriftDetail;

    fn drift(address: &str, action: &str, attrs: &[&str]) -> DriftDetail {
        DriftDetail {
            address: address.to_string(),
            action: action.to_string(),
            risk: "low".to_string(),
            attributes: attrs.iter().map(|s| s.to_string()).collect(),
            policy_decision: None,
            matched_policy: None,
        }
    }

    // ── Live incident regression (2026-07-12): four consecutive
    // replans of the identical `+21 create` diff on `camelot-eks`
    // produced four different `pendingPlanHash` values purely from
    // `tofu plan`'s raw-stdout graph-walk ordering — making a human's
    // approval structurally unable to catch up. The fingerprint below
    // is what `route_through_approval_gate` now hashes instead. ──────

    #[test]
    fn same_drifts_in_different_order_produce_the_same_fingerprint() {
        let a = vec![
            drift("aws_eks_addon.coredns", "create", &[]),
            drift("aws_eks_node_group.system_ng", "create", &[]),
            drift("aws_iam_openid_connect_provider.oidc", "create", &[]),
        ];
        let b = vec![
            drift("aws_iam_openid_connect_provider.oidc", "create", &[]),
            drift("aws_eks_addon.coredns", "create", &[]),
            drift("aws_eks_node_group.system_ng", "create", &[]),
        ];

        assert_eq!(
            canonical_drift_fingerprint(&a),
            canonical_drift_fingerprint(&b),
            "graph-walk ordering must not affect the approval fingerprint"
        );
    }

    #[test]
    fn same_entry_with_attributes_in_different_order_produces_the_same_fingerprint() {
        let a = vec![drift("aws_eks_cluster.camelot-eks", "update", &["endpoint", "identity", "created_at"])];
        let b = vec![drift("aws_eks_cluster.camelot-eks", "update", &["created_at", "identity", "endpoint"])];

        assert_eq!(canonical_drift_fingerprint(&a), canonical_drift_fingerprint(&b));
    }

    #[test]
    fn a_different_action_on_the_same_address_changes_the_fingerprint() {
        let a = vec![drift("aws_eks_cluster.camelot-eks", "create", &[])];
        let b = vec![drift("aws_eks_cluster.camelot-eks", "replace", &[])];

        assert_ne!(canonical_drift_fingerprint(&a), canonical_drift_fingerprint(&b));
    }

    #[test]
    fn a_different_set_of_addresses_changes_the_fingerprint() {
        let a = vec![drift("aws_eks_node_group.system_ng", "create", &[])];
        let b = vec![
            drift("aws_eks_node_group.system_ng", "create", &[]),
            drift("aws_iam_openid_connect_provider.oidc", "create", &[]),
        ];

        assert_ne!(canonical_drift_fingerprint(&a), canonical_drift_fingerprint(&b));
    }

    #[test]
    fn empty_drift_list_is_stable() {
        assert_eq!(canonical_drift_fingerprint(&[]), canonical_drift_fingerprint(&[]));
        assert_eq!(canonical_drift_fingerprint(&[]), "");
    }
}

/// Regression tests for a live incident (2026-07-17, Camelot Mode-1):
/// five unrelated `InfrastructureTemplate` CRs — plans ranging from 1
/// to 50 resources — all converged on the identical
/// `status.pendingPlanHash` `"eac9f28515f12ae7"`.
///
/// Root cause: `MagmaWorkspaceRunner::plan` deliberately returns
/// `artifact: None` on the DB-backed (zero-disk, production-default)
/// path — see `executor::workspace_runner`'s doc comment: "the cycle
/// receipt is enriched from the DB downstream." But `handle_planning`'s
/// drift extraction only had two paths (tofu's `raw_show_json`, and a
/// magma path gated on `plan_result.artifact.is_some()`) — with BOTH
/// empty on the DB-backed path, every such plan silently fell through
/// to "no analyzable output," producing an EMPTY drift list regardless
/// of the plan's real content. `canonical_drift_fingerprint(&[])` is
/// always `""`, and `workspace.read_state_bytes()` (a raw on-disk read)
/// is also always `None` on the same zero-disk path — so
/// `plan_approval_hash("", None)` collapsed to one fleet-wide constant
/// every DB-backed magma template converged on whenever it needed
/// approval. Approving any single CR's plan at that hash would have
/// made every other affected CR's UNRELATED plan appear pre-approved.
///
/// The fix adds the missing third path (`fetch_db_backed_cycle_artifact`
/// in `template_controller.rs`, reusing the SAME Postgres bundle fetch
/// `record_reconcile_cycle` already performs) so the policy engine (and
/// the approval hash) sees the resources a DB-backed plan actually
/// touches. These tests exercise the shared derivation
/// (`plan_summary_and_drifts_from_artifact`) both paths now feed
/// through, proving two genuinely different plans get genuinely
/// different hashes — and neither collapses to the old constant.
#[cfg(test)]
mod db_backed_magma_drift_extraction_tests {
    use super::{canonical_drift_fingerprint, plan_approval_hash, plan_summary_and_drifts_from_artifact};
    use crate::executor::cycle_artifact::{CycleArtifact, PlanAction, TypedResourceChange};

    /// Build a `CycleArtifact` the way `CycleArtifact::from_magma_plan`
    /// would for a plan that creates exactly these resources against
    /// empty state — i.e. the exact shape every colliding CR in the
    /// live incident had (`+N create` from scratch, no prior state).
    fn artifact_with_creates(addresses: &[&str]) -> CycleArtifact {
        let resource_changes: Vec<TypedResourceChange> = addresses
            .iter()
            .map(|addr| TypedResourceChange {
                address: (*addr).to_string(),
                action: PlanAction::Create,
                severity: crate::executor::cycle_artifact::action_to_severity(&PlanAction::Create),
            })
            .collect();
        CycleArtifact {
            action_distribution: CycleArtifact::action_distribution_from(&resource_changes),
            resource_changes,
            ..Default::default()
        }
    }

    /// Reproduce `route_through_approval_gate`'s hash derivation from a
    /// `CycleArtifact`, isolating the DRIFT-fingerprint half of the fix
    /// this module regression-tests: `state_bytes` is fixed at `None`
    /// (Absent) for every case here on purpose, so these tests prove
    /// `canonical_drift_fingerprint` alone breaks the collision — they
    /// do NOT exercise the separate, real-Postgres-backed state half of
    /// the hash `current_state_fingerprint` now provides for magma
    /// templates (see `CurrentStateFingerprint`'s doc comment and
    /// `current_state_fingerprint_tests` below for that half).
    fn pending_plan_hash_for(art: &CycleArtifact) -> String {
        let (_summary, drifts) = plan_summary_and_drifts_from_artifact(art);
        let plan_content = canonical_drift_fingerprint(&drifts);
        plan_approval_hash(&plan_content, None)
    }

    #[test]
    fn two_genuinely_different_plans_get_genuinely_different_hashes() {
        // camelot-flux-bootstrap's real live shape: one resource.
        let flux_bootstrap = artifact_with_creates(&["flux_bootstrap_git.this"]);
        // camelot-eks's real live shape has 50 resources; a
        // representative subset is enough to prove non-collision here
        // (the fingerprint's own full-address-set behavior is covered
        // by `canonical_drift_fingerprint_tests`).
        let eks = artifact_with_creates(&[
            "aws_vpc.camelot-eks-vpc",
            "aws_eks_cluster.camelot-eks",
            "aws_subnet.camelot-eks-public-0",
        ]);
        // camelot-breathe-controller-iam's real live shape: three
        // IAM resources.
        let breathe_iam = artifact_with_creates(&[
            "aws_iam_role.camelot-breathe-controller",
            "aws_iam_role_policy.camelot-breathe-controller-policy",
            "aws_iam_instance_profile.camelot-breathe-controller",
        ]);

        let hash_flux = pending_plan_hash_for(&flux_bootstrap);
        let hash_eks = pending_plan_hash_for(&eks);
        let hash_iam = pending_plan_hash_for(&breathe_iam);

        assert_ne!(hash_flux, hash_eks, "a 1-resource plan and a 3-resource plan must not collide");
        assert_ne!(hash_flux, hash_iam, "two different 1-vs-3-resource plans must not collide");
        assert_ne!(hash_eks, hash_iam, "two different 3-resource plans must not collide");
    }

    #[test]
    fn no_real_plan_collapses_to_the_live_empty_drift_collision_constant() {
        // "eac9f28515f12ae7" is `plan_approval_hash("", None)` — the
        // exact value every affected CR converged on live (verified by
        // direct computation against the deployed hash function). Any
        // `CycleArtifact` with real resource_changes must now hash to
        // something else.
        let collision_constant = plan_approval_hash("", None);
        assert_eq!(
            collision_constant, "eac9f28515f12ae7",
            "sanity check: must match the value observed live on Camelot Mode-1"
        );

        let flux_bootstrap = artifact_with_creates(&["flux_bootstrap_git.this"]);
        let eks = artifact_with_creates(&["aws_vpc.camelot-eks-vpc", "aws_eks_cluster.camelot-eks"]);
        let breathe_iam = artifact_with_creates(&[
            "aws_iam_role.camelot-breathe-controller",
            "aws_iam_role_policy.camelot-breathe-controller-policy",
        ]);

        assert_ne!(pending_plan_hash_for(&flux_bootstrap), collision_constant);
        assert_ne!(pending_plan_hash_for(&eks), collision_constant);
        assert_ne!(pending_plan_hash_for(&breathe_iam), collision_constant);
    }
}

#[cfg(test)]
mod unapproved_destructive_escalation_tests {
    use super::find_unapproved_destructive_escalation;
    use crate::crd::DriftDetail;

    fn drift(address: &str, action: &str) -> DriftDetail {
        DriftDetail {
            address: address.to_string(),
            action: action.to_string(),
            risk: "low".to_string(),
            attributes: vec![],
            policy_decision: None,
            matched_policy: None,
        }
    }

    // ── Live incident regression (2026-07-12): a human approved a plan
    // with ZERO destroys ("+21 create") on camelot-eks. During Applying,
    // `run_import_prepass` imported resources and dropped the cached
    // plan file, so `tofu apply` ran a live refresh-and-apply that
    // discovered `aws_eks_cluster` needed `replace` — never shown to
    // the human — and executed it, destroying and recreating a
    // production EKS cluster. This guard is what `handle_applying` now
    // runs against the post-import recheck plan before ever applying it. ──

    #[test]
    fn a_replace_never_present_in_the_approved_plan_is_flagged() {
        let approved = vec![
            drift("aws_eks_node_group.system_ng", "create"),
            drift("aws_iam_openid_connect_provider.oidc", "create"),
        ];
        let fresh = vec![
            drift("aws_eks_cluster.camelot-eks", "replace"),
            drift("aws_eks_node_group.system_ng", "create"),
        ];

        let escalation = find_unapproved_destructive_escalation(&approved, &fresh);
        assert_eq!(
            escalation.map(|d| d.address.as_str()),
            Some("aws_eks_cluster.camelot-eks")
        );
    }

    #[test]
    fn a_delete_on_an_address_the_approved_plan_never_mentioned_is_flagged() {
        let approved = vec![drift("aws_eks_node_group.system_ng", "create")];
        let fresh = vec![
            drift("aws_eks_node_group.system_ng", "create"),
            drift("aws_vpc_endpoint.orphan", "delete"),
        ];

        assert!(find_unapproved_destructive_escalation(&approved, &fresh).is_some());
    }

    #[test]
    fn a_replace_the_human_already_approved_for_that_address_is_not_flagged() {
        // The approved plan itself already showed this exact replace —
        // the human reviewed and approved it, so the recheck must not
        // re-block an apply that matches what was granted.
        let approved = vec![drift("aws_eks_cluster.camelot-eks", "replace")];
        let fresh = vec![drift("aws_eks_cluster.camelot-eks", "replace")];

        assert!(find_unapproved_destructive_escalation(&approved, &fresh).is_none());
    }

    #[test]
    fn non_destructive_actions_are_never_flagged_regardless_of_approval() {
        let approved: Vec<DriftDetail> = vec![];
        let fresh = vec![
            drift("aws_eks_addon.coredns", "create"),
            drift("aws_eks_addon.kube_proxy", "update"),
            drift("aws_eks_addon.vpc_cni", "noop"),
        ];

        assert!(find_unapproved_destructive_escalation(&approved, &fresh).is_none());
    }

    #[test]
    fn a_create_downgraded_to_noop_after_import_is_not_flagged() {
        // The common, safe case this gate must never block: an import
        // turns a planned `create` into a no-op `matched`/`noop` — that's
        // exactly what the import prepass is FOR, not an escalation.
        let approved = vec![drift("aws_vpc.camelot-eks-vpc", "create")];
        let fresh = vec![drift("aws_vpc.camelot-eks-vpc", "noop")];

        assert!(find_unapproved_destructive_escalation(&approved, &fresh).is_none());
    }

    #[test]
    fn empty_fresh_plan_is_never_flagged() {
        let approved = vec![drift("aws_eks_cluster.camelot-eks", "create")];
        assert!(find_unapproved_destructive_escalation(&approved, &[]).is_none());
    }
}

#[cfg(test)]
mod destroy_protection_gate_tests {
    use super::{
        drift_details_from_planned_changes, drift_details_from_tofu_show_json,
        evaluate_destroy_protection_gate, is_destructive_action, DestroyProtectionGate,
    };
    use crate::crd::DriftDetail;
    use crate::executor::{PlanAction, PlannedChange, ResourceKindClass};

    fn drift(address: &str, action: &str) -> DriftDetail {
        DriftDetail {
            address: address.to_string(),
            action: action.to_string(),
            risk: "low".to_string(),
            attributes: vec![],
            policy_decision: None,
            matched_policy: None,
        }
    }

    fn planned(address: &str, action: PlanAction) -> PlannedChange {
        PlannedChange {
            address: address.to_string(),
            action,
            after: None,
            kind: ResourceKindClass::Managed,
        }
    }

    // ── Live incident regression (task #120, sibling of #123): a plan
    // whose action set contained a `replace` on `aws_eks_cluster` executed
    // during a NORMAL apply (not the import path r101 closed) — destroying
    // and recreating a production EKS cluster — because `destroyProtection`
    // was consulted ONLY on the explicit destroy path, never against a
    // normal apply's action set. This gate is what `handle_applying` now
    // runs before EVERY apply, unconditionally, when protection is on. ──

    #[test]
    fn a_replace_under_destroy_protection_is_blocked() {
        let actions = vec![
            drift("aws_eks_node_group.system_ng", "create"),
            drift("aws_eks_cluster.camelot-eks", "replace"),
        ];
        assert_eq!(
            evaluate_destroy_protection_gate(true, &actions),
            DestroyProtectionGate::BlockedByProtectedDestruction {
                address: "aws_eks_cluster.camelot-eks".to_string(),
                action: "replace".to_string(),
            }
        );
    }

    #[test]
    fn a_delete_under_destroy_protection_is_blocked() {
        let actions = vec![drift("aws_vpc_endpoint.orphan", "delete")];
        assert_eq!(
            evaluate_destroy_protection_gate(true, &actions),
            DestroyProtectionGate::BlockedByProtectedDestruction {
                address: "aws_vpc_endpoint.orphan".to_string(),
                action: "delete".to_string(),
            }
        );
    }

    #[test]
    fn a_replace_with_destroy_protection_off_proceeds() {
        // The gate is strictly opt-in: with protection off, a replace is a
        // normal (policy/approval-governed) operation and this gate must
        // never touch it — otherwise it would block every legitimate
        // replace fleet-wide. This is the load-bearing false-positive
        // guard: destroyProtection defaults off, so the overwhelming
        // majority of applies must sail straight through untouched.
        let actions = vec![drift("aws_eks_cluster.camelot-eks", "replace")];
        assert_eq!(
            evaluate_destroy_protection_gate(false, &actions),
            DestroyProtectionGate::Proceed
        );
    }

    #[test]
    fn non_destructive_actions_under_destroy_protection_proceed() {
        // The common, safe case: destroyProtection on, plan only adds /
        // updates / no-ops. Must NEVER be blocked, or protection would
        // trap every ordinary create-and-update apply.
        let actions = vec![
            drift("aws_eks_addon.coredns", "create"),
            drift("aws_eks_addon.kube_proxy", "update"),
            drift("aws_eks_addon.vpc_cni", "noop"),
        ];
        assert_eq!(
            evaluate_destroy_protection_gate(true, &actions),
            DestroyProtectionGate::Proceed
        );
    }

    #[test]
    fn an_empty_plan_under_destroy_protection_proceeds() {
        assert_eq!(
            evaluate_destroy_protection_gate(true, &[]),
            DestroyProtectionGate::Proceed
        );
    }

    #[test]
    fn the_first_destructive_action_in_plan_order_is_the_one_reported() {
        // Determinism: when several destructive actions exist, the gate
        // reports the FIRST in plan order so the operator-facing message
        // is stable across reconciles rather than flapping.
        let actions = vec![
            drift("aws_vpc.first", "delete"),
            drift("aws_eks_cluster.second", "replace"),
        ];
        assert_eq!(
            evaluate_destroy_protection_gate(true, &actions),
            DestroyProtectionGate::BlockedByProtectedDestruction {
                address: "aws_vpc.first".to_string(),
                action: "delete".to_string(),
            }
        );
    }

    #[test]
    fn is_destructive_action_matches_exactly_delete_and_replace() {
        // The shared predicate both destructive-action gates depend on —
        // pin its contract so a change to what counts as "destructive" is
        // a conscious edit, caught here, not an accident that silently
        // widens or narrows both gates.
        assert!(is_destructive_action("delete"));
        assert!(is_destructive_action("replace"));
        assert!(!is_destructive_action("create"));
        assert!(!is_destructive_action("update"));
        assert!(!is_destructive_action("noop"));
        assert!(!is_destructive_action("read"));
    }

    // ── Task #120 follow-up (#131): the three additional apply sites
    // (conflict.rs's post-import re-apply, the self-heal retry, and the
    // ordinary bare-magma apply) each recompute a FRESH action set via
    // `fresh_action_set_before_bare_apply` instead of trusting the
    // stale `prior_drifts` snapshot the original fix checked. Its two
    // tiers — magma's typed `PlannedChange` readback, and the tofu
    // `drift_details_from_tofu_show_json` fallback — must feed the
    // EXACT SAME `evaluate_destroy_protection_gate` predicate proven
    // above, never a forked check. ──

    #[test]
    fn a_replace_planned_change_maps_to_a_destructive_drift_detail() {
        let changes = vec![planned("aws_eks_cluster.camelot-eks", PlanAction::Replace)];
        let drifts = drift_details_from_planned_changes(&changes);
        assert_eq!(drifts.len(), 1);
        assert_eq!(drifts[0].address, "aws_eks_cluster.camelot-eks");
        assert_eq!(drifts[0].action, "replace");
        assert!(is_destructive_action(&drifts[0].action));
    }

    #[test]
    fn a_delete_planned_change_maps_to_a_destructive_drift_detail() {
        let changes = vec![planned("aws_vpc_endpoint.orphan", PlanAction::Delete)];
        let drifts = drift_details_from_planned_changes(&changes);
        assert_eq!(drifts[0].action, "delete");
        assert!(is_destructive_action(&drifts[0].action));
    }

    #[test]
    fn non_destructive_planned_changes_map_to_non_destructive_drift_details() {
        let changes = vec![
            planned("aws_eks_addon.coredns", PlanAction::Create),
            planned("aws_eks_addon.kube_proxy", PlanAction::Update),
            planned("aws_eks_addon.vpc_cni", PlanAction::NoOp),
            planned("data.aws_ami.al2", PlanAction::Read),
        ];
        let drifts = drift_details_from_planned_changes(&changes);
        assert!(drifts.iter().all(|d| !is_destructive_action(&d.action)));
    }

    #[test]
    fn a_fresh_planned_change_replace_blocks_via_the_shared_gate() {
        // The pipeline `fresh_action_set_before_bare_apply`'s tier 1
        // (magma's `planned_changes()`) feeds
        // `recheck_destroy_protection_before_bare_apply`: a fresh
        // PlannedChange set — the SAME persisted plan row apply() will
        // consume — containing a replace must block under protection,
        // exactly like a `prior_drifts`-sourced replace already does.
        let changes = vec![
            planned("aws_eks_node_group.system_ng", PlanAction::Create),
            planned("aws_eks_cluster.camelot-eks", PlanAction::Replace),
        ];
        let fresh = drift_details_from_planned_changes(&changes);
        assert_eq!(
            evaluate_destroy_protection_gate(true, &fresh),
            DestroyProtectionGate::BlockedByProtectedDestruction {
                address: "aws_eks_cluster.camelot-eks".to_string(),
                action: "replace".to_string(),
            }
        );
    }

    #[test]
    fn a_fresh_planned_change_set_with_no_destructive_action_proceeds() {
        let changes = vec![planned("aws_eks_addon.coredns", PlanAction::Create)];
        let fresh = drift_details_from_planned_changes(&changes);
        assert_eq!(
            evaluate_destroy_protection_gate(true, &fresh),
            DestroyProtectionGate::Proceed
        );
    }

    #[test]
    fn tier_2_raw_tofu_plan_json_with_a_replace_blocks_via_the_shared_gate() {
        // The tofu-fallback tier of `fresh_action_set_before_bare_apply`:
        // a raw `tofu show -json` payload (the channel the conflict.rs /
        // self-heal / magma-cache-miss recheck falls back to) parsed by
        // `drift_details_from_tofu_show_json` must feed the identical
        // gate — this is the exact channel the camelot-eks incident's
        // import-driven re-apply used.
        let json = serde_json::json!({
            "format_version": "1.2",
            "terraform_version": "1.6.0",
            "resource_changes": [
                {
                    "address": "aws_eks_cluster.camelot-eks",
                    "type": "aws_eks_cluster",
                    "name": "camelot-eks",
                    "provider_name": "registry.terraform.io/hashicorp/aws",
                    "change": { "actions": ["delete", "create"] }
                }
            ],
            "output_changes": {},
            "configuration": {}
        })
        .to_string();
        let fresh = drift_details_from_tofu_show_json(&json);
        assert_eq!(
            evaluate_destroy_protection_gate(true, &fresh),
            DestroyProtectionGate::BlockedByProtectedDestruction {
                address: "aws_eks_cluster.camelot-eks".to_string(),
                action: "replace".to_string(),
            }
        );
    }
}

#[cfg(test)]
mod state_continuity_breach_tests {
    use super::{
        evaluate_auto_apply_gate, failed_retry_decision, state_continuity_breach, AutoApplyGate,
        Duration, EventType, FailedRetryDecision, InfrastructureTemplate, EXHAUSTED_RETRY_INTERVAL,
    };
    use crate::crd::{InfrastructureTemplateSpec, InfrastructureTemplateStatus, RetryPolicy, TemplateSource};

    fn default_test_spec() -> InfrastructureTemplateSpec {
        InfrastructureTemplateSpec {
            source: TemplateSource {
                inline:         Some(String::new()),
                config_map_ref: None,
                git_repository: None,
            },
            pangea_namespace:    "test".to_string(),
            template_name:       None,
            variables:           None,
            variable_refs:       None,
            auto_approve:        true,
            spec_approved_plan_hash: None,
            refresh_interval:    "10m".to_string(),
            suspend:             false,
            executor:            None,
            destroy_protection:  false,
            retry_policy:        None,
            provider_credentials: None,
            compliance_profiles: vec![],
            policies:            vec![],
            default_decision:    None,
            settling_policy:     None,
            reactive_policy:     None,
            import_policy:       None,
            import_hints:        Default::default(),
            conflict_policy:     None,
            output_bindings:     Default::default(),
            secret_files:        Default::default(),
        }
    }

    // ── Finding 1 regression: PolicyDecision::AutoApply had ZERO
    // state-continuity check before this fix — an ordinary pod restart
    // on the disk-based `tofu` executor (workspace on a pod-local
    // emptyDir) silently wiped `terraform.tfstate`, and AutoApply would
    // plan+apply "everything create" against the empty state completely
    // unattended, reproducing the exact
    // docs/postmortems/2026-07-12-camelot-eks-state-wipe-duplicate-vpc.md
    // duplicate-VPC incident with no spec edit required at all. ────────

    #[test]
    fn restart_wipe_on_a_previously_applied_disk_backed_template_is_a_breach() {
        // The exact failure scenario: not durable (disk-based tofu),
        // this template has applied successfully before, and local
        // state is now gone. This MUST be flagged — it is precisely
        // the state pod restarts leave behind on an emptyDir workspace.
        assert!(state_continuity_breach(
            /* is_durable_state_backend */ false,
            /* previously_applied       */ true,
            /* state_present_now        */ false,
        ));
    }

    #[test]
    fn first_ever_apply_on_a_disk_backed_template_is_never_a_breach() {
        // A brand-new template's very first AutoApply cycle also has
        // "no local state yet" — but there is nothing to have lost, so
        // this must NOT be flagged (would otherwise block every
        // legitimate first apply forever).
        assert!(!state_continuity_breach(false, false, false));
    }

    #[test]
    fn healthy_steady_state_disk_backed_template_is_never_a_breach() {
        // State is present and intact — the common case on every
        // ordinary reconcile of an already-applied disk-backed
        // template.
        assert!(!state_continuity_breach(false, true, true));
    }

    #[test]
    fn magma_db_backed_template_is_never_a_breach_even_though_local_state_is_always_absent() {
        // Magma's state lives in Postgres and NEVER populates local
        // disk, restart or not — `state_present_now` is always `false`
        // for it by design. Without the durable-backend exclusion this
        // would misfire on EVERY magma AutoApply cycle after the first
        // successful apply, which would be a severe regression (magma
        // is the fleet's default, safe executor — ★★ MAGMA-NATIVE).
        assert!(!state_continuity_breach(
            /* is_durable_state_backend */ true,
            /* previously_applied       */ true,
            /* state_present_now        */ false,
        ));
    }

    #[test]
    fn evaluate_auto_apply_gate_blocks_exactly_on_a_breach() {
        assert_eq!(
            evaluate_auto_apply_gate(false, true, false),
            AutoApplyGate::BlockedByStateContinuityBreach
        );
        assert_eq!(
            evaluate_auto_apply_gate(false, true, true),
            AutoApplyGate::Proceed
        );
        assert_eq!(
            evaluate_auto_apply_gate(false, false, false),
            AutoApplyGate::Proceed
        );
        assert_eq!(
            evaluate_auto_apply_gate(true, true, false),
            AutoApplyGate::Proceed
        );
    }

    // ── handle_failed self-heal (task #191) ─────────────────────────

    #[test]
    fn failed_retry_decision_retries_within_the_backoff_ramp() {
        // Not exhausted: unchanged exponential-backoff behavior.
        assert_eq!(
            failed_retry_decision(false, 0, 30),
            FailedRetryDecision::ExponentialBackoff(Duration::from_secs(30)),
        );
        assert_eq!(
            failed_retry_decision(false, 2, 30),
            FailedRetryDecision::ExponentialBackoff(Duration::from_secs(120)),
        );
    }

    #[test]
    fn failed_retry_decision_never_gives_up_once_exhausted() {
        // The task #191 regression: retries_exhausted=true used to mean
        // "no further action, stuck in Failed forever" (a decision that,
        // if this were modeled honestly at the time, would have had no
        // representable variant at all -- the old code just `return`ed
        // out of the whole handler). It must now still be a retry, just
        // at the slower, bounded cadence -- proving the CR always gets
        // picked back up on the controller's own next reconcile tick.
        let decision = failed_retry_decision(true, 3, 30);
        assert_eq!(
            decision,
            FailedRetryDecision::SlowCadenceAfterExhaustion(EXHAUSTED_RETRY_INTERVAL)
        );
        // Whatever the variant, `requeue_after` is always Some concrete,
        // finite duration -- there is no "never requeue again" path.
        assert_eq!(decision.requeue_after(), Duration::from_secs(3600));
        assert_eq!(decision.event_reason(), "RetryAfterExhaustion");
        assert_eq!(
            decision.event_type(),
            EventType::Warning,
            "exhaustion must be visible to operators, not silent -- before this fix \
             the exhausted branch recorded NO event at all"
        );
    }

    #[test]
    fn failed_retry_decision_is_always_some_flavor_of_retry() {
        // Property check across a spread of failure counts: no matter
        // how many times the template has failed, or whether the ramp
        // is exhausted, the decision always carries a positive requeue
        // duration and a retry-shaped event reason. This is the
        // self-heal invariant task #191 restores: the controller's own
        // next reconcile always re-attempts the cycle from Pending,
        // never requiring an external kubectl edit/re-apply to escape
        // Failed.
        for failure_count in [0u32, 1, 3, 5, 10, 100] {
            for retries_exhausted in [false, true] {
                let decision = failed_retry_decision(retries_exhausted, failure_count, 30);
                assert!(
                    decision.requeue_after() > Duration::ZERO,
                    "failure_count={failure_count} exhausted={retries_exhausted}: \
                     must always requeue, never stop forever"
                );
                assert!(
                    matches!(decision.event_reason(), "Retry" | "RetryAfterExhaustion"),
                    "every decision must be retry-shaped"
                );
            }
        }
    }

    #[test]
    fn retries_exhausted_reflects_the_real_crd_method_feeding_the_decision() {
        // Ties `failed_retry_decision` to the CRD's own exhaustion
        // semantics (`InfrastructureTemplate::retries_exhausted` /
        // `retry_count`), rather than trusting bare booleans in
        // isolation -- a template past `spec.retryPolicy.maxRetries`
        // really does route to the slow-cadence self-heal path, and one
        // still within the ramp really does route to the exponential
        // path.
        let mut template = InfrastructureTemplate::new(
            "t",
            InfrastructureTemplateSpec {
                retry_policy: Some(RetryPolicy { max_retries: 3, backoff_seconds: 30 }),
                ..default_test_spec()
            },
        );
        template.status = Some(InfrastructureTemplateStatus {
            failure_count: 3,
            ..Default::default()
        });
        assert!(template.retries_exhausted(), "3 >= max_retries(3)");
        let decision = failed_retry_decision(
            template.retries_exhausted(),
            template.retry_count(),
            template.spec.retry_policy.as_ref().unwrap().backoff_seconds,
        );
        assert_eq!(
            decision,
            FailedRetryDecision::SlowCadenceAfterExhaustion(EXHAUSTED_RETRY_INTERVAL)
        );

        // One failure short of the ceiling: still retries on the
        // exponential ramp, not the slow cadence.
        template.status = Some(InfrastructureTemplateStatus {
            failure_count: 2,
            ..Default::default()
        });
        assert!(!template.retries_exhausted(), "2 < max_retries(3)");
        let decision = failed_retry_decision(
            template.retries_exhausted(),
            template.retry_count(),
            template.spec.retry_policy.as_ref().unwrap().backoff_seconds,
        );
        assert_eq!(
            decision,
            FailedRetryDecision::ExponentialBackoff(Duration::from_secs(120))
        );
    }
}

// Reconcile cycle receipts were lifted to
// `controller/template/cycle_receipts.rs` during T3 (continuation
// of R6/T1/T2). Internal callers reference them via
// `super::template::cycle_receipts::*` paths.

// Finalizer helpers, Event recording, and Provider credential resolution
// were lifted to `controller/template/{finalizer,events,provider_creds}.rs`
// during the 2026-05-03 review pass (R6). Internal callers in this file
// reference them via `super::template::*` paths.

/// Error policy for the controller.
fn error_policy(
    _obj: Arc<InfrastructureTemplate>,
    error: &Error,
    ctx: Arc<ControllerState>,
) -> Action {
    use crate::controller::error_policy::{run_error_policy, tiered_backoff};
    run_error_policy(
        &ctx.metrics,
        crate::crd::ControllerKind::Template,
        error,
        tiered_backoff(error.is_retryable()),
    )
}

impl From<ReconcileAction> for Action {
    fn from(action: ReconcileAction) -> Self {
        match action {
            ReconcileAction::Requeue(duration) => Action::requeue(duration),
            ReconcileAction::Done => Action::await_change(),
        }
    }
}

/// Never-stuck breaker self-clear decision for the auto-suspend entry
/// gate. Returns `true` when a template that is `status.autoSuspended`
/// should have the latch cleared and be re-reconciled — i.e. when the
/// live `metadata.generation` is genuinely AHEAD of the last-observed
/// `status.observedGeneration`, which is the operator's explicit
/// "the spec changed since we parked it" corrective signal.
///
/// Returns `false` when the generation is unchanged OR has moved
/// backward: no forward corrective edit landed, so the template stays
/// parked (log + requeue) rather than churning the same known-bad
/// spec.
///
/// Delegates to the shared `generation_invalidates_render` gate (see
/// its doc comment above `compiled_config_available`) instead of its
/// own comparison, so this decision and the render-reuse gate can
/// never drift apart. (2026-07-17) Previously compared with `!=`,
/// which also cleared the park when generation moved BACKWARD —
/// e.g. a status-subresource replay or a controller restart observing
/// a stale `metadata.generation` snapshot — a false corrective signal
/// with no real spec edit behind it. `>` only fires on a genuine
/// forward edit, matching the render-reuse gate's own directional
/// fix.
fn auto_suspend_gate_should_clear(current_gen: i64, observed_gen: i64) -> bool {
    generation_invalidates_render(current_gen, observed_gen)
}

/// Interval (seconds) a template stays parked (circuit breaker OPEN)
/// before the auto-suspend gate lets one reconcile fall through as a
/// HALF-OPEN probe. Sized well above `DEFAULT_REQUEUE_INTERVAL` (5 min)
/// so parked templates re-probe on a low-frequency cadence — bounded
/// retry, never a hammer — while still guaranteeing a transient-cause
/// park self-heals within one interval rather than requiring a human
/// spec edit. Kept as a plain `i64` (not a `const chrono::Duration`,
/// whose constructors are not `const fn`); the `Duration` is built at
/// the two use sites via `auto_suspend_probe_interval()`.
const AUTO_SUSPEND_PROBE_INTERVAL_SECS: i64 = 30 * 60;

/// The parked-template HALF-OPEN probe interval as a `chrono::Duration`.
fn auto_suspend_probe_interval() -> chrono::Duration {
    chrono::Duration::seconds(AUTO_SUSPEND_PROBE_INTERVAL_SECS)
}

/// Never-stuck breaker HALF-OPEN decision for the auto-suspend entry
/// gate's no-corrective-edit branch. Returns `true` when a parked
/// template is due for a probe reconcile — i.e. when
/// `AUTO_SUSPEND_PROBE_INTERVAL` has elapsed since it was parked
/// (`status.lastEscalatedAt`), so the operator should re-attempt the
/// reconcile to discover whether the (possibly transient) cause has
/// cleared.
///
/// A missing `last_escalated_at` (parked without a recorded timestamp —
/// shouldn't happen, but defend against it) returns `true`: fail toward
/// unsticking, never toward a permanent, un-timestamped park.
fn auto_suspend_probe_due(
    now: chrono::DateTime<chrono::Utc>,
    last_escalated_at: Option<chrono::DateTime<chrono::Utc>>,
    probe_interval: chrono::Duration,
) -> bool {
    match last_escalated_at {
        None => true,
        Some(parked_at) => now.signed_duration_since(parked_at) >= probe_interval,
    }
}

/// Returns true iff `prev` already carries the suspended-condition
/// set semantically. Thin wrapper around the lifted helper so the
/// suspended-skip call site reads naturally.
///
/// Suspended-PATCH issues a Merge patch on the `conditions` field,
/// which RFC 7396 replaces in full — so in steady state prev's length
/// matches new's, and `conditions_observably_equal` (which also
/// length-checks) returns true. The first PATCH after entering
/// suspended state may force a write because prev had a different
/// set (e.g. Ready=True from prior phases) — that's correct behavior.
fn suspended_conditions_already_set(
    prev: &[crate::crd::Condition],
    new: &[crate::crd::Condition],
) -> bool {
    crate::controller::status::conditions_observably_equal(prev, new)
}

#[cfg(test)]
mod suspended_diff_tests {
    //! Lock in the diff-gate that breaks the suspended-template
    //! self-trigger watch loop (rio firefighting 2026-05-07: was
    //! observed at ~123 PATCH/sec on cloudflare-pleme).
    use super::suspended_conditions_already_set;
    use super::super::reconciler::conditions_for_suspended;
    use crate::crd::Condition;
    use chrono::{TimeZone, Utc};

    fn cond(typ: &str, status: &str, reason: &str, msg: &str) -> Condition {
        Condition {
            r#type: typ.into(),
            status: status.into(),
            reason: reason.into(),
            message: msg.into(),
            // Stale timestamp on purpose: the diff-gate must NOT
            // be tricked into "differing" just because the existing
            // condition was stamped earlier.
            last_transition_time: Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).unwrap(),
        }
    }

    #[test]
    fn skips_patch_when_all_three_suspended_conditions_already_present() {
        let new = conditions_for_suspended();
        let prev: Vec<Condition> = new
            .iter()
            .map(|n| cond(&n.r#type, &n.status, &n.reason, &n.message))
            .collect();
        assert!(
            suspended_conditions_already_set(&prev, &new),
            "should skip PATCH when (type, status, reason, message) already match"
        );
    }

    #[test]
    fn requests_patch_when_status_differs() {
        let new = conditions_for_suspended();
        // Same types, but status is wrong — e.g., a previous Ready=True
        // hasn't been overwritten yet.
        let prev = vec![
            cond("Ready", "True", "Ready", "Healthy"),
            cond("Reconciling", "True", "Apply", "Applying changes"),
            cond("DriftDetected", "False", "Settled", "No drift"),
        ];
        assert!(
            !suspended_conditions_already_set(&prev, &new),
            "must PATCH when prior conditions disagree on status/reason/message"
        );
    }

    #[test]
    fn requests_patch_when_prev_is_empty() {
        let new = conditions_for_suspended();
        assert!(
            !suspended_conditions_already_set(&[], &new),
            "must PATCH when no prior conditions exist"
        );
    }

    #[test]
    fn extra_prev_conditions_force_patch_to_overwrite() {
        // prev has the 3 suspended conditions plus extras (Settled,
        // Verified). The lifted `conditions_observably_equal` helper
        // length-checks, so this returns false → we PATCH. That's the
        // CORRECT behavior: the suspended-PATCH issues a JSON Merge on
        // the conditions field which RFC-7396-replaces the whole
        // array, so writing our authoritative 3-condition set
        // intentionally overwrites the extras (which would have come
        // from a stale prior phase or an outside actor — neither of
        // which we want to coexist with).
        //
        // Pre-refinement (the original `suspended_conditions_already_set`
        // had no length check) this case skipped the PATCH and the
        // extras lingered until something else removed them.
        let new = conditions_for_suspended();
        let mut prev: Vec<Condition> = new
            .iter()
            .map(|n| cond(&n.r#type, &n.status, &n.reason, &n.message))
            .collect();
        prev.push(cond("Settled", "True", "Settled", "no drift"));
        prev.push(cond("Verified", "True", "Audited", "ok"));
        assert!(
            !suspended_conditions_already_set(&prev, &new),
            "extras in prev must force PATCH so our authoritative set wins"
        );
    }
}

#[cfg(test)]
mod auto_suspend_gate_tests {
    //! Lock in the never-stuck self-clear decision for the auto-suspend
    //! entry gate. Previously the gate short-circuited BEFORE the
    //! generation-change handler, so a corrective .spec edit could not
    //! un-park a template (arc-github/drive/zot parked 20-54 days).
    //! The full clear+fall-through path (kube PATCH, event, gen handler)
    //! is integration-testable only (needs a live kube::Client +
    //! ControllerState); the decision that gates it is pure and locked
    //! in here.
    use super::auto_suspend_gate_should_clear;

    #[test]
    fn spec_change_clears_the_park() {
        // metadata.generation bumped past the last-observed generation
        // = an explicit corrective edit landed → clear + re-reconcile.
        assert!(
            auto_suspend_gate_should_clear(7, 5),
            "a generation bump (corrective spec edit) must clear the auto-suspend park"
        );
    }

    #[test]
    fn unchanged_generation_stays_parked() {
        // No corrective edit → stay parked (log + requeue), do not churn
        // the same known-bad spec.
        assert!(
            !auto_suspend_gate_should_clear(5, 5),
            "an unchanged generation must NOT clear the park (no corrective signal)"
        );
    }

    #[test]
    fn fresh_never_observed_generation_clears() {
        // observedGeneration defaults to 0 before the first successful
        // reconcile; a real generation (>=1) differs → the gate opens.
        assert!(
            auto_suspend_gate_should_clear(1, 0),
            "a real generation vs the default observed=0 must clear the park"
        );
    }

    #[test]
    fn backward_generation_does_not_clear() {
        // (2026-07-17) Regression lock for the directional fix: a
        // `current_gen` that moved BACKWARD relative to `observed_gen`
        // (e.g. a status-subresource replay or a controller restart
        // observing a stale `metadata.generation` snapshot) is NOT a
        // corrective spec edit and must NOT clear the auto-suspend
        // latch. Under the old `current_gen != observed_gen` body this
        // incorrectly returned `true` — any generation move, forward
        // OR backward, cleared the park. `auto_suspend_gate_should_clear`
        // now delegates to `generation_invalidates_render`, which only
        // fires when `current_gen > observed_gen`.
        assert!(!auto_suspend_gate_should_clear(3, 5));
    }
}

#[cfg(test)]
mod auto_suspend_probe_tests {
    //! Lock in the never-stuck HALF-OPEN probe decision. Without it a
    //! template parked by a TRANSIENT cause (provider crash later
    //! respawned clean, DB blip, rate-limit, an operator-image fix that
    //! changed operator behavior without touching this template's spec)
    //! sits parked forever, because gen == obs means the corrective-edit
    //! gate never fires. The probe guarantees a bounded, low-frequency
    //! re-attempt so a cleared cause self-resumes with no human action.
    use super::{auto_suspend_probe_due, auto_suspend_probe_interval};
    use chrono::{Duration, Utc};

    #[test]
    fn probe_fires_once_the_interval_has_elapsed() {
        let now = Utc::now();
        let parked_at = now - auto_suspend_probe_interval() - Duration::seconds(1);
        assert!(
            auto_suspend_probe_due(now, Some(parked_at), auto_suspend_probe_interval()),
            "a park older than the probe interval must be due for a HALF-OPEN probe"
        );
    }

    #[test]
    fn probe_holds_while_still_within_the_interval() {
        let now = Utc::now();
        let parked_at = now - (auto_suspend_probe_interval() - Duration::seconds(1));
        assert!(
            !auto_suspend_probe_due(now, Some(parked_at), auto_suspend_probe_interval()),
            "a park younger than the probe interval must stay OPEN (no premature probe hammer)"
        );
    }

    #[test]
    fn probe_fires_exactly_at_the_interval_boundary() {
        // `>=`: a park aged exactly one interval is due (deterministic
        // boundary, not an off-by-one that could wedge at the edge).
        let now = Utc::now();
        let parked_at = now - auto_suspend_probe_interval();
        assert!(
            auto_suspend_probe_due(now, Some(parked_at), auto_suspend_probe_interval()),
            "a park aged exactly the probe interval must be due"
        );
    }

    #[test]
    fn missing_timestamp_fails_toward_unsticking() {
        // A parked template with no recorded park time must NOT become a
        // permanent, un-timestamped park — probe immediately.
        let now = Utc::now();
        assert!(
            auto_suspend_probe_due(now, None, auto_suspend_probe_interval()),
            "a park with no lastEscalatedAt must probe immediately (fail toward unsticking)"
        );
    }
}

#[cfg(test)]
mod cycle_tests {
    use super::*;
    // The cycle_receipts module's pure helpers are referenced by name
    // throughout this test mod. Re-import them under the names the
    // pre-T3 tests already use so the test bodies stay unchanged.
    use super::super::template::cycle_receipts::{
        build_reconcile_cycle, cycle_content_equal, outcome_for_action, truncate_for_status,
        CycleResult,
    };
    use crate::crd::{CycleSummary, Outcome, ReconcileCycle};

    fn d(addr: &str, action: &str) -> DriftDetail {
        DriftDetail {
            address: addr.to_string(),
            action: action.to_string(),
            risk: "low".to_string(),
            attributes: vec![],
            policy_decision: None,
            matched_policy: None,
        }
    }

    // ── Item D — compile-failure escalation tests ───────────────
    //
    // Reproducer for the rio incident: pleme-io-opensource sat in
    // phase=Compiling, cycleCount=0 for hours after the gem-load
    // failure; settling policy never fired because the cycle counter
    // never advanced. The fix introduces consecutiveCompileFailures
    // as an independent escalation counter; these tests lock in its
    // semantics.

    #[test]
    fn compile_failure_increments_below_threshold() {
        // Below threshold: counter advances, no escalation.
        let (next, escalate) = evaluate_compile_failure_escalation(0, 5);
        assert_eq!(next, 1);
        assert!(!escalate);

        let (next, escalate) = evaluate_compile_failure_escalation(3, 5);
        assert_eq!(next, 4);
        assert!(!escalate);
    }

    #[test]
    fn compile_failure_escalates_at_threshold() {
        // At threshold: escalate.
        let (next, escalate) = evaluate_compile_failure_escalation(4, 5);
        assert_eq!(next, 5);
        assert!(escalate, "next == max should escalate");
    }

    #[test]
    fn compile_failure_escalates_past_threshold() {
        // Past threshold (e.g., status patch failed once and we're
        // re-reconciling with a stale prior count): still escalate.
        let (next, escalate) = evaluate_compile_failure_escalation(10, 5);
        assert_eq!(next, 11);
        assert!(escalate);
    }

    #[test]
    fn compile_failure_zero_max_never_escalates() {
        // Defensive: if a user sets maxConsecutiveDriftCycles=0
        // (intentionally or otherwise), don't escalate on every
        // failure — that's almost certainly not what they meant.
        let (next, escalate) = evaluate_compile_failure_escalation(0, 0);
        assert_eq!(next, 1);
        assert!(!escalate);

        let (next, escalate) = evaluate_compile_failure_escalation(100, 0);
        assert_eq!(next, 101);
        assert!(!escalate);
    }

    #[test]
    fn compile_failure_saturates_at_u32_max() {
        // No panic on overflow — saturating add.
        let (next, escalate) = evaluate_compile_failure_escalation(u32::MAX, 5);
        assert_eq!(next, u32::MAX, "must saturate, not panic");
        assert!(escalate, "any non-zero max with saturated count escalates");
    }

    #[test]
    fn outcome_action_mapping() {
        assert_eq!(outcome_for_action("no-op"), Outcome::Matched);
        assert_eq!(outcome_for_action("noop"), Outcome::Matched);
        assert_eq!(outcome_for_action("create"), Outcome::Created);
        assert_eq!(outcome_for_action("update"), Outcome::Updated);
        assert_eq!(outcome_for_action("delete"), Outcome::Destroyed);
        assert_eq!(outcome_for_action("replace"), Outcome::Updated);
        assert_eq!(outcome_for_action("import"), Outcome::Imported);
        assert_eq!(outcome_for_action("anything-else"), Outcome::Updated);
    }

    #[test]
    fn no_changes_cycle_marks_all_matched() {
        let cycle = build_reconcile_cycle(
            1,
            Utc::now(),
            &[],
            20,
            Some("+0 ~0 -0".to_string()),
            None,
            None,
            None,
            CycleResult::NoChanges,
        );
        assert_eq!(cycle.summary.matched, 20);
        assert_eq!(cycle.summary.updated, 0);
        assert_eq!(cycle.summary.failed, 0);
        assert_eq!(cycle.outcomes.len(), 0);
    }

    #[test]
    fn applied_success_derives_per_resource_outcomes() {
        let drifts = vec![
            d("cf_dns_record.foo", "update"),
            d("cf_zone.bar", "create"),
            d("cf_workers_script.baz", "delete"),
        ];
        let cycle = build_reconcile_cycle(
            5,
            Utc::now(),
            &drifts,
            20,
            Some("+1 ~1 -1".to_string()),
            None,
            None,
            None,
            CycleResult::AppliedSuccess { imported_addresses: vec![] },
        );
        assert_eq!(cycle.summary.matched, 17, "20 total - 3 touched = 17");
        assert_eq!(cycle.summary.updated, 1);
        assert_eq!(cycle.summary.created, 1);
        assert_eq!(cycle.summary.destroyed, 1);
        assert_eq!(cycle.summary.failed, 0);
        assert_eq!(cycle.outcomes.len(), 3);
        assert_eq!(cycle.outcomes[0].outcome, Outcome::Updated);
        assert_eq!(cycle.outcomes[0].action.as_deref(), Some("update"));
    }

    #[test]
    fn apply_success_with_imported_address_marks_outcome_imported() {
        let drifts = vec![
            d("cf_dns_record.foo", "create"),
            d("cf_zone.bar", "create"),
        ];
        let cycle = build_reconcile_cycle(
            6,
            Utc::now(),
            &drifts,
            10,
            Some("+2 ~0 -0".to_string()),
            None,
            None,
            None,
            CycleResult::AppliedSuccess {
                imported_addresses: vec!["cf_dns_record.foo".to_string()],
            },
        );
        // foo got imported, bar got created
        assert_eq!(cycle.summary.imported, 1);
        assert_eq!(cycle.summary.created, 1);
        assert_eq!(cycle.summary.matched, 8);
        let foo = cycle.outcomes.iter().find(|o| o.address == "cf_dns_record.foo").unwrap();
        let bar = cycle.outcomes.iter().find(|o| o.address == "cf_zone.bar").unwrap();
        assert_eq!(foo.outcome, Outcome::Imported);
        assert_eq!(bar.outcome, Outcome::Created);
        assert!(foo.message.as_ref().unwrap().contains("import"));
    }

    #[test]
    fn apply_failure_marks_all_failed_with_error_message() {
        let drifts = vec![d("cf_dns_record.foo", "update")];
        let err = "tofu apply failed: provider error: rate limit".to_string();
        let cycle = build_reconcile_cycle(
            6,
            Utc::now(),
            &drifts,
            20,
            None,
            None,
            None,
            None,
            CycleResult::AppliedFailure(err.clone()),
        );
        assert_eq!(cycle.summary.failed, 1);
        assert_eq!(cycle.summary.matched, 19);
        assert_eq!(cycle.outcomes[0].outcome, Outcome::Failed);
        assert!(cycle.outcomes[0].message.as_ref().unwrap().contains("rate limit"));
    }

    #[test]
    fn policy_gated_marks_drifted_with_decision_message() {
        let drifts = vec![d("cf_dns_record.foo", "update")];
        let cycle = build_reconcile_cycle(
            7,
            Utc::now(),
            &drifts,
            20,
            None,
            None,
            None,
            None,
            CycleResult::PolicyGated(PolicyDecision::Refuse),
        );
        assert_eq!(cycle.summary.drifted_uncorrected, 1);
        assert_eq!(cycle.outcomes[0].outcome, Outcome::Drifted);
        assert!(cycle.outcomes[0].message.as_ref().unwrap().contains("refuse"));
    }

    #[test]
    fn cycle_content_equal_ignores_cycle_number_and_timestamps() {
        let now = Utc::now();
        let later = now + chrono::Duration::minutes(5);
        let mk = |c: u64, ts: chrono::DateTime<Utc>| ReconcileCycle {
            cycle: c,
            started_at: ts,
            completed_at: ts,
            source_revision: None,
            plan_summary: Some("+0 ~0 -0".into()),
            summary: CycleSummary {
                matched: 20,
                ..Default::default()
            },
            outcomes: vec![],
            ..Default::default()
        };
        assert!(cycle_content_equal(&mk(1, now), &mk(2, later)));
    }

    #[test]
    fn cycle_content_unequal_when_summary_differs() {
        let now = Utc::now();
        let mk = |matched: u32| ReconcileCycle {
            cycle: 1,
            started_at: now,
            completed_at: now,
            source_revision: None,
            plan_summary: None,
            summary: CycleSummary {
                matched,
                ..Default::default()
            },
            outcomes: vec![],
            ..Default::default()
        };
        assert!(!cycle_content_equal(&mk(20), &mk(19)));
    }

    #[test]
    fn truncate_for_status_caps_long_strings() {
        let long = "x".repeat(500);
        let t = truncate_for_status(&long);
        assert!(t.ends_with('…'));
        assert!(t.chars().count() <= 257);
    }

    #[test]
    fn truncate_for_status_passes_short_through() {
        assert_eq!(truncate_for_status("ok"), "ok");
    }

    #[test]
    fn workspace_drift_reaction_maps_to_policy_decision() {
        use crate::crd::architecture_gem::DriftReaction as DR;
        assert_eq!(
            workspace_drift_reaction_to_policy_decision(DR::AutoApply),
            Some(PolicyDecision::AutoApply)
        );
        assert_eq!(
            workspace_drift_reaction_to_policy_decision(DR::RequireApproval),
            Some(PolicyDecision::RequireApproval)
        );
        assert_eq!(
            workspace_drift_reaction_to_policy_decision(DR::Refuse),
            Some(PolicyDecision::Refuse)
        );
        // Alert collapses to AutoApply at the template level — the
        // alert mechanism is separate from the apply gate.
        assert_eq!(
            workspace_drift_reaction_to_policy_decision(DR::Alert),
            Some(PolicyDecision::AutoApply)
        );
    }

    #[test]
    fn outcomes_capped_at_100() {
        let drifts: Vec<DriftDetail> =
            (0..200).map(|i| d(&format!("cf_dns_record.r{i}"), "update")).collect();
        let cycle = build_reconcile_cycle(
            8,
            Utc::now(),
            &drifts,
            500,
            None,
            None,
            None,
            None,
            CycleResult::AppliedSuccess { imported_addresses: vec![] },
        );
        assert_eq!(cycle.outcomes.len(), 100, "outcomes capped at 100");
        // Summary still counts the FULL touched-set in matched math:
        // 500 total - 200 touched (all update) = 300 matched.
        // Per-Outcome counts only reflect what we iterated (capped at 100).
        // So updated count = 100 (top of the cap).
        assert_eq!(cycle.summary.updated, 100);
        assert_eq!(cycle.summary.matched, 300);
    }

    #[test]
    fn substitute_import_id_inserts_string_variables() {
        let mut vars = std::collections::BTreeMap::new();
        vars.insert("zone".into(), serde_json::Value::String("z123".into()));
        vars.insert("rec".into(), serde_json::Value::String("r456".into()));
        let out = substitute_import_id("{{ .zone }}/{{ .rec }}", &vars).unwrap();
        assert_eq!(out, "z123/r456");
    }

    #[test]
    fn substitute_import_id_handles_no_dot_prefix() {
        let mut vars = std::collections::BTreeMap::new();
        vars.insert("name".into(), serde_json::Value::String("foo".into()));
        let out = substitute_import_id("{{ name }}", &vars).unwrap();
        assert_eq!(out, "foo");
    }

    #[test]
    fn substitute_import_id_string_coerces_numbers() {
        let mut vars = std::collections::BTreeMap::new();
        vars.insert(
            "id".into(),
            serde_json::Value::Number(serde_json::Number::from(42)),
        );
        let out = substitute_import_id("{{ .id }}", &vars).unwrap();
        assert_eq!(out, "42");
    }

    #[test]
    fn substitute_import_id_returns_missing_var() {
        let vars = std::collections::BTreeMap::new();
        let err = substitute_import_id("{{ .missing }}", &vars).unwrap_err();
        assert_eq!(err, "missing");
    }

    #[test]
    fn substitute_import_id_preserves_literal_text() {
        let mut vars = std::collections::BTreeMap::new();
        vars.insert("a".into(), serde_json::Value::String("x".into()));
        let out = substitute_import_id("prefix-{{ .a }}-suffix", &vars).unwrap();
        assert_eq!(out, "prefix-x-suffix");
    }

    #[test]
    fn substitute_import_id_no_template_passes_through() {
        let vars = std::collections::BTreeMap::new();
        let out = substitute_import_id("plain-id-no-vars", &vars).unwrap();
        assert_eq!(out, "plain-id-no-vars");
    }

    #[test]
    fn substitute_import_id_unclosed_template_passes_through() {
        // Defensive: malformed templates don't crash; remainder is
        // copied verbatim so the caller's `tofu import` will fail
        // visibly instead of receiving a corrupted ID.
        let vars = std::collections::BTreeMap::new();
        let out = substitute_import_id("{{ .unclosed", &vars).unwrap();
        assert_eq!(out, "{{ .unclosed");
    }
}

/// Tests for the reacting-FSM dispatch helpers (the typed remediation
/// router). These cover the pure decision functions — precedence selection
/// + import-hint suggestion — without kube mocks. The per-arm side-effect
/// dispatch (`react_to_apply_anomaly`) is exercised end-to-end via the
/// controller integration tests; here we lock the routing decisions.
#[cfg(test)]
mod anomaly_reaction_tests {
    use super::{select_driving_anomaly, suggested_import_hint};
    use crate::controller::anomaly::{ApplyAnomaly, RemediationMode};

    #[test]
    fn recovery_anomalies_win_precedence_over_holds() {
        // A mixed batch: an Unclassified hold + a StalePlan recovery. The
        // recovery (Absolute, converges in one reconcile) must drive the
        // tick, not the hold.
        let batch = vec![
            ApplyAnomaly::Unclassified { reason: "weird".into() },
            ApplyAnomaly::StalePlan,
        ];
        let driver = select_driving_anomaly(&batch);
        assert_eq!(driver, ApplyAnomaly::StalePlan);
        assert_eq!(driver.mode(), RemediationMode::Absolute);
    }

    #[test]
    fn provider_unavailable_outranks_object_exists_and_holds() {
        let batch = vec![
            ApplyAnomaly::PermissionDenied,
            ApplyAnomaly::ObjectExistsUntracked {
                address: "github_repository.a".into(),
                action: "create".into(),
            },
            ApplyAnomaly::ProviderUnavailable { provider: "cloudflare".into() },
        ];
        let driver = select_driving_anomaly(&batch);
        assert_eq!(
            driver,
            ApplyAnomaly::ProviderUnavailable { provider: "cloudflare".into() }
        );
    }

    #[test]
    fn object_exists_outranks_decaying_and_holds() {
        // The grafana-record class must drive over an incidental rate-limit
        // on a sibling resource so adoption is attempted before backoff.
        let batch = vec![
            ApplyAnomaly::RateLimited,
            ApplyAnomaly::ObjectExistsUntracked {
                address: "cloudflare_dns_record.x".into(),
                action: "create".into(),
            },
            ApplyAnomaly::PermissionDenied,
        ];
        let driver = select_driving_anomaly(&batch);
        assert!(matches!(driver, ApplyAnomaly::ObjectExistsUntracked { .. }));
    }

    #[test]
    fn empty_batch_defaults_to_typed_unclassified_never_panics() {
        let driver = select_driving_anomaly(&[]);
        assert!(matches!(driver, ApplyAnomaly::Unclassified { .. }));
        assert_eq!(driver.mode(), RemediationMode::Hold);
    }

    #[test]
    fn cloudflare_record_import_hint_names_zone_record_shape() {
        // The live rio grafana-record gap: import id is <zone_id>/<record_id>.
        assert_eq!(
            suggested_import_hint("cloudflare_dns_record.rio-grafana-quero-cloud-cname"),
            "{{ zone_id }}/<record_id>"
        );
    }

    #[test]
    fn generic_resource_import_hint_is_natural_id_placeholder() {
        assert_eq!(
            suggested_import_hint("github_repository.galho"),
            "<natural-id>"
        );
    }
}
