//! Status-patching helpers for `InfrastructureTemplate` reconciliation.
//!
//! Lifted from `template_controller.rs` during T1 (continuation of R6).
//! Each function takes a `&InfrastructureTemplate` + new field values
//! and applies a server-side merge patch on `.status`. The patches
//! preserve fields not named in the patch (since the kube-rs Merge
//! patch is a shallow JSON merge, not a strategic merge).
//!
//! Why these belong in their own module: every reconcile path ends in
//! one of these functions, so they form a natural API surface. Group-
//! ing them together makes drift across patches (e.g. one fn forgets
//! to bump observedGeneration) immediately visible.

use chrono::Utc;
use kube::api::{Api, Patch, PatchParams};
use kube::ResourceExt;
use tracing::info;

use crate::controller::reconciler::conditions_for_phase;
use crate::controller::ControllerState;
use crate::crd::{
    InfrastructureTemplate, Phase, PolicyDecision, PolicyEvaluation, ResourceSummary,
};
use crate::error::Result;

/// Update the phase in the template status.
///
/// Bumps `phase_entered_at` only on real transitions (the
/// ReactivePolicy phaseTimeout escalation measures against this).
/// Clears `last_error` + `failure_count` on non-Failed transitions.
pub async fn update_phase(
    template: &InfrastructureTemplate,
    phase: Phase,
    state: &ControllerState,
) -> Result<()> {
    let name = template.name_any();
    let namespace = template.namespace().unwrap_or_default();

    let api: Api<InfrastructureTemplate> = Api::namespaced(state.client.clone(), &namespace);

    let mut status = template.status.clone().unwrap_or_default();
    let phase_changed = status.phase != Some(phase);
    status.phase = Some(phase);
    status.observed_generation = template.metadata.generation.unwrap_or(0);
    // Always set conditions so FluxCD healthChecks see current state
    status.conditions = conditions_for_phase(phase, None);
    // ReactivePolicy: bump phase_entered_at only on real transitions
    // — that's what phaseTimeout escalation measures against.
    if phase_changed {
        status.phase_entered_at = Some(Utc::now());
    }

    // Clear error on non-Failed transitions
    if phase != Phase::Failed {
        status.last_error = None;
        status.failure_count = 0;
    }

    let patch = serde_json::json!({ "status": status });

    api.patch_status(
        &name,
        &PatchParams::apply("pangea-operator"),
        &Patch::Merge(&patch),
    )
    .await?;

    state
        .metrics
        .templates_by_phase
        .with_label_values(&[&phase.to_string()])
        .inc();

    info!(?phase, "Updated template phase");
    Ok(())
}

/// Update phase to a Failed-class state with an error message and
/// run the post-reconcile pipeline (ReactivePolicy escalation).
pub async fn update_phase_with_error(
    template: &InfrastructureTemplate,
    phase: Phase,
    error_msg: &str,
    state: &ControllerState,
) -> Result<()> {
    let name = template.name_any();
    let namespace = template.namespace().unwrap_or_default();

    let api: Api<InfrastructureTemplate> = Api::namespaced(state.client.clone(), &namespace);

    let mut status = template.status.clone().unwrap_or_default();
    let phase_changed = status.phase != Some(phase);
    status.phase = Some(phase);
    status.observed_generation = template.metadata.generation.unwrap_or(0);
    status.last_error = Some(error_msg.to_string());
    status.failure_count = status.failure_count.saturating_add(1);
    status.conditions = conditions_for_phase(phase, Some(error_msg));
    if phase_changed {
        status.phase_entered_at = Some(Utc::now());
    }

    let patch = serde_json::json!({ "status": status });

    api.patch_status(
        &name,
        &PatchParams::apply("pangea-operator"),
        &Patch::Merge(&patch),
    )
    .await?;

    // Post-reconcile pipeline — single ordered entry point for hooks
    // that fire AFTER status is patched. Today's pipeline runs the
    // ReactivePolicy stage (failure escalation / phase timeout /
    // verified-blocked); future hooks (audit emission, notification
    // routing) land in `controller::post_reconcile_pipeline`.
    crate::controller::post_reconcile_pipeline::run_for_template(template, state).await;

    Ok(())
}

/// Update status after a successful plan.
pub async fn update_plan_status(
    template: &InfrastructureTemplate,
    resources: Option<ResourceSummary>,
    plan_summary: Option<&str>,
    drift_details: Vec<crate::crd::DriftDetail>,
    policy_evaluation: Option<PolicyEvaluation>,
    state: &ControllerState,
) -> Result<()> {
    let name = template.name_any();
    let namespace = template.namespace().unwrap_or_default();

    let api: Api<InfrastructureTemplate> = Api::namespaced(state.client.clone(), &namespace);

    let mut status = template.status.clone().unwrap_or_default();
    status.resources = resources;
    status.plan_summary = plan_summary.map(|s| s.to_string());
    status.last_planned_at = Some(Utc::now());
    status.drift_details = drift_details;
    status.policy_evaluation = policy_evaluation;

    let patch = serde_json::json!({ "status": status });

    api.patch_status(
        &name,
        &PatchParams::apply("pangea-operator"),
        &Patch::Merge(&patch),
    )
    .await?;

    Ok(())
}

/// Update status after a successful apply.
/// Clears the pending/approved plan hashes and forces the conditions
/// to the Ready set so external observers see the new posture.
pub async fn update_apply_status(
    template: &InfrastructureTemplate,
    outputs: Option<std::collections::BTreeMap<String, serde_json::Value>>,
    state: &ControllerState,
) -> Result<()> {
    let name = template.name_any();
    let namespace = template.namespace().unwrap_or_default();

    let api: Api<InfrastructureTemplate> = Api::namespaced(state.client.clone(), &namespace);

    let mut status = template.status.clone().unwrap_or_default();
    status.outputs = outputs;
    status.last_applied_at = Some(Utc::now());
    // Clear approval hashes after successful apply
    status.pending_plan_hash = None;
    status.approved_plan_hash = None;
    status.conditions = conditions_for_phase(Phase::Ready, None);

    let patch = serde_json::json!({ "status": status });

    api.patch_status(
        &name,
        &PatchParams::apply("pangea-operator"),
        &Patch::Merge(&patch),
    )
    .await?;

    Ok(())
}

/// Persist state-settling tracking fields to status.
///
/// Updates `consecutive_drift_cycles`, `stuck_resources`, and (when
/// drift was detected) `drift_details`. Also flips the `Settled`
/// condition to reflect the current outcome — this is what an
/// external observer (Flux healthCheck, Prometheus alert, kubectl
/// describe) reads to know whether the system has actually converged.
pub async fn update_settling_status(
    template: &InfrastructureTemplate,
    outcome: &crate::controller::settling::SettlingOutcome,
    drift_details: &[crate::crd::DriftDetail],
    state: &ControllerState,
) -> Result<()> {
    use crate::controller::settling::SettlingOutcome;
    let name = template.name_any();
    let namespace = template.namespace().unwrap_or_default();
    let api: Api<InfrastructureTemplate> = Api::namespaced(state.client.clone(), &namespace);

    let cycles = outcome.cycle_count();
    let stuck_addresses: Vec<String> = match outcome {
        SettlingOutcome::StuckByFingerprint { stuck_addresses, .. }
        | SettlingOutcome::StuckByCount { stuck_addresses, .. } => stuck_addresses.clone(),
        _ => Vec::new(),
    };

    let (settled_status, settled_reason, settled_msg) = match outcome {
        SettlingOutcome::Settled => (
            "True",
            "Settled".to_string(),
            "Drift check found no changes — desired state matches actual state.".to_string(),
        ),
        SettlingOutcome::Progressing { cycles } => (
            "False",
            "Reconciling".to_string(),
            format!("Drift detected; reconciling (cycle {}).", cycles),
        ),
        SettlingOutcome::StuckByFingerprint { cycles, fingerprint, .. } => (
            "False",
            "StuckByFingerprint".to_string(),
            format!(
                "Drift fingerprint {} unchanged across {} cycle(s) — system is not converging.",
                fingerprint, cycles
            ),
        ),
        SettlingOutcome::StuckByCount { cycles, .. } => (
            "False",
            "StuckByCount".to_string(),
            format!(
                "Exceeded max consecutive drift cycles ({}) without settling.",
                cycles
            ),
        ),
    };

    let mut status = template.status.clone().unwrap_or_default();
    status.consecutive_drift_cycles = cycles;
    status.stuck_resources = stuck_addresses;
    if !drift_details.is_empty() {
        status.drift_details = drift_details.to_vec();
    } else if matches!(outcome, SettlingOutcome::Settled) {
        // Clear stale drift details once we've settled.
        status.drift_details = Vec::new();
    }
    status.last_drift_check_at = Some(Utc::now());

    // Replace any prior `Settled` condition; preserve other types.
    let now = Utc::now();
    status.conditions.retain(|c| c.r#type != "Settled");
    status.conditions.push(crate::crd::Condition {
        r#type: "Settled".to_string(),
        status: settled_status.to_string(),
        last_transition_time: now,
        reason: settled_reason,
        message: settled_msg,
    });

    let patch = serde_json::json!({ "status": status });
    api.patch_status(
        &name,
        &PatchParams::apply("pangea-operator"),
        &Patch::Merge(&patch),
    )
    .await?;

    Ok(())
}

/// Update only the last drift check timestamp.
pub async fn update_drift_check_timestamp(
    template: &InfrastructureTemplate,
    state: &ControllerState,
) -> Result<()> {
    let name = template.name_any();
    let namespace = template.namespace().unwrap_or_default();

    let api: Api<InfrastructureTemplate> = Api::namespaced(state.client.clone(), &namespace);

    let patch = serde_json::json!({
        "status": {
            "lastDriftCheckAt": Utc::now()
        }
    });

    api.patch_status(
        &name,
        &PatchParams::apply("pangea-operator"),
        &Patch::Merge(&patch),
    )
    .await?;

    Ok(())
}

/// Update the pending plan hash for approval workflow.
/// Always nulls the approved hash so the operator can't accidentally
/// race a stale approval against a new plan.
pub async fn update_pending_plan_hash(
    template: &InfrastructureTemplate,
    plan_hash: &str,
    state: &ControllerState,
) -> Result<()> {
    let name = template.name_any();
    let namespace = template.namespace().unwrap_or_default();

    let api: Api<InfrastructureTemplate> = Api::namespaced(state.client.clone(), &namespace);

    let patch = serde_json::json!({
        "status": {
            "pendingPlanHash": plan_hash,
            "approvedPlanHash": null
        }
    });

    api.patch_status(
        &name,
        &PatchParams::apply("pangea-operator"),
        &Patch::Merge(&patch),
    )
    .await?;

    Ok(())
}

/// Map the WorkspaceCatalog's `DriftReaction` enum (gem-cascade
/// shape) to the InfrastructureTemplate's `PolicyDecision` enum. The
/// two enums describe the same intent at different cascade levels but
/// have slightly different vocabularies — `Alert` exists at the
/// workspace level (notify-but-don't-block) but maps to `AutoApply`
/// at the template level because the alerting mechanism is separate
/// from the apply gate.
pub fn workspace_drift_reaction_to_policy_decision(
    dr: crate::crd::architecture_gem::DriftReaction,
) -> Option<PolicyDecision> {
    use crate::crd::architecture_gem::DriftReaction as DR;
    Some(match dr {
        DR::AutoApply => PolicyDecision::AutoApply,
        DR::RequireApproval => PolicyDecision::RequireApproval,
        DR::Refuse => PolicyDecision::Refuse,
        DR::Alert => PolicyDecision::AutoApply,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crd::architecture_gem::DriftReaction;

    #[test]
    fn workspace_drift_reaction_alert_collapses_to_auto_apply() {
        // The vocab carve-out: workspace-level Alert means
        // "notify-but-don't-block". At the template level there's no
        // direct equivalent — Alert collapses to AutoApply because
        // the alert mechanism (ReactivePolicy notifications) is
        // separate from the apply gate. Drift here would silently
        // upgrade Alert → Refuse.
        assert_eq!(
            workspace_drift_reaction_to_policy_decision(DriftReaction::Alert),
            Some(PolicyDecision::AutoApply)
        );
    }

    #[test]
    fn workspace_drift_reaction_passes_through_other_decisions() {
        assert_eq!(
            workspace_drift_reaction_to_policy_decision(DriftReaction::AutoApply),
            Some(PolicyDecision::AutoApply)
        );
        assert_eq!(
            workspace_drift_reaction_to_policy_decision(DriftReaction::RequireApproval),
            Some(PolicyDecision::RequireApproval)
        );
        assert_eq!(
            workspace_drift_reaction_to_policy_decision(DriftReaction::Refuse),
            Some(PolicyDecision::Refuse)
        );
    }
}
