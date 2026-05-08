//! Controller for ComplianceBinding resources.
//!
//! Watches ComplianceSchedule status and enforces compliance on bound targets.
//! When compliance state changes, the controller executes configured reactions:
//! suspending/resuming targets, posting webhooks, updating sekiban gates.

use crate::crd::{
    BindingComplianceState, ComplianceBinding, ComplianceBindingStatus, ComplianceSchedule,
    ComplianceSchedulePhase, EnforcementLevel, ImagePipeline, InfrastructureFlow,
    InfrastructureTemplate, PackerBuild, ReactionAction, TargetKind, TargetStatus,
};
use crate::error::Error;

use futures::StreamExt;
use kube::{
    api::{Api, Patch, PatchParams},
    runtime::controller::Action,
    ResourceExt,
};
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, error, info, instrument, warn};

use super::{
    create_condition, ControllerState, DEFAULT_REQUEUE_INTERVAL, ERROR_REQUEUE_INTERVAL,
    SHORT_REQUEUE_INTERVAL,
};

pub struct ComplianceBindingController {
    state: ControllerState,
}

impl ComplianceBindingController {
    pub fn new(state: ControllerState) -> Self {
        Self { state }
    }

    pub async fn run(self) -> crate::error::Result<()> {
        let client = self.state.client.clone();
        let state = Arc::new(self.state);

        info!("Starting ComplianceBinding controller");

        crate::controller::generation_filter::filtered_controller::<ComplianceBinding>(client)
            .run(
                move |binding, ctx| {
                    let state = Arc::clone(&ctx);
                    async move { reconcile(binding, state).await }
                },
                error_policy,
                state,
            )
            .for_each(|result| async move {
                match result {
                    Ok((obj, action)) => {
                        debug!(
                            name = %obj.name,
                            namespace = ?obj.namespace,
                            ?action,
                            "ComplianceBinding reconciliation completed"
                        );
                    }
                    Err(e) => {
                        error!(error = %e, "ComplianceBinding reconciliation failed");
                    }
                }
            })
            .await;

        Ok(())
    }
}

#[instrument(skip(state), fields(name = %binding.name_any(), namespace = ?binding.namespace()))]
async fn reconcile(
    binding: Arc<ComplianceBinding>,
    state: Arc<ControllerState>,
) -> std::result::Result<Action, Error> {
    state
        .metrics
        .record_reconcile(crate::crd::ControllerKind::ComplianceBinding, "ok");
    let _name = binding.name_any();
    let namespace = binding.namespace().unwrap_or_default();

    info!("Reconciling ComplianceBinding");

    // Cluster-wide kill-switch — honor `OperatorPolicy/default`.
    if let Some(action) = crate::controller::policy_pipeline::run_for_controller(
        &state,
        crate::crd::ControllerKind::ComplianceBinding,
    )
    .into_skip_action()
    {
        return Ok(action);
    }

    if binding.spec.suspend {
        update_compliance_state(&binding, BindingComplianceState::Suspended, &state).await?;
        return Ok(Action::requeue(DEFAULT_REQUEUE_INTERVAL));
    }

    // Read the referenced ComplianceSchedule
    let cs_ns = binding
        .spec
        .compliance_ref
        .namespace
        .as_deref()
        .unwrap_or(&namespace);
    let cs_name = &binding.spec.compliance_ref.name;

    let cs_api: Api<ComplianceSchedule> = Api::namespaced(state.client.clone(), cs_ns);
    let cs = match cs_api.get_opt(cs_name).await.map_err(Error::Kube)? {
        Some(cs) => cs,
        None => {
            warn!(schedule = %cs_name, "Referenced ComplianceSchedule not found");
            update_compliance_state(&binding, BindingComplianceState::Unknown, &state).await?;
            return Ok(Action::requeue(SHORT_REQUEUE_INTERVAL));
        }
    };

    let cs_phase = cs
        .status
        .as_ref()
        .and_then(|s| s.phase)
        .unwrap_or(ComplianceSchedulePhase::Idle);

    let compliance_state = match cs_phase {
        ComplianceSchedulePhase::Compliant => BindingComplianceState::Compliant,
        ComplianceSchedulePhase::NonCompliant => BindingComplianceState::NonCompliant,
        _ => BindingComplianceState::Unknown,
    };

    let previous_state = binding
        .status
        .as_ref()
        .and_then(|s| s.compliance_state)
        .unwrap_or(BindingComplianceState::Unknown);

    // Detect state transition
    let state_changed = compliance_state != previous_state;

    if state_changed {
        info!(
            from = %previous_state,
            to = %compliance_state,
            "Compliance state changed"
        );

        // Execute reactions
        for reaction in &binding.spec.reactions {
            let event_matches = match (&reaction.event, &compliance_state) {
                (crate::crd::ComplianceEvent::NonCompliant, BindingComplianceState::NonCompliant) => true,
                (crate::crd::ComplianceEvent::Compliant, BindingComplianceState::Compliant) => true,
                (crate::crd::ComplianceEvent::Error, BindingComplianceState::Unknown) => true,
                _ => false,
            };

            if event_matches {
                execute_reaction(&binding, reaction, &state).await?;
            }
        }

        // Enforce on targets
        if matches!(
            binding.spec.enforcement,
            EnforcementLevel::Gate | EnforcementLevel::Rollback
        ) {
            enforce_on_targets(&binding, &compliance_state, &state).await?;
        }
    }

    // Update binding status
    let compliance_hash = cs
        .status
        .as_ref()
        .and_then(|s| s.compliance_hash.clone());

    let target_statuses: Vec<TargetStatus> = binding
        .spec
        .targets
        .iter()
        .map(|t| TargetStatus {
            kind: t.kind.clone(),
            name: t.name.clone(),
            gated: compliance_state == BindingComplianceState::NonCompliant
                && matches!(binding.spec.enforcement, EnforcementLevel::Gate | EnforcementLevel::Rollback),
            last_action: None,
        })
        .collect();

    update_full_status(&binding, compliance_state, &target_statuses, compliance_hash.as_deref(), &state).await?;

    Ok(Action::requeue(SHORT_REQUEUE_INTERVAL))
}

// ---------------------------------------------------------------------------
// Enforcement
// ---------------------------------------------------------------------------

async fn enforce_on_targets(
    binding: &ComplianceBinding,
    compliance_state: &BindingComplianceState,
    state: &ControllerState,
) -> std::result::Result<(), Error> {
    let namespace = binding.namespace().unwrap_or_default();
    let should_suspend = *compliance_state == BindingComplianceState::NonCompliant;

    for target in &binding.spec.targets {
        let target_ns = target.namespace.as_deref().unwrap_or(&namespace);

        let suspend_patch = serde_json::json!({
            "spec": { "suspend": should_suspend }
        });

        let pp = PatchParams::apply("pangea-operator");

        match target.kind {
            TargetKind::InfrastructureTemplate => {
                let api: Api<InfrastructureTemplate> =
                    Api::namespaced(state.client.clone(), target_ns);
                api.patch(&target.name, &pp, &Patch::Merge(&suspend_patch))
                    .await
                    .map_err(Error::Kube)?;
            }
            TargetKind::InfrastructureFlow => {
                let api: Api<InfrastructureFlow> =
                    Api::namespaced(state.client.clone(), target_ns);
                api.patch(&target.name, &pp, &Patch::Merge(&suspend_patch))
                    .await
                    .map_err(Error::Kube)?;
            }
            TargetKind::ImagePipeline => {
                let api: Api<ImagePipeline> =
                    Api::namespaced(state.client.clone(), target_ns);
                api.patch(&target.name, &pp, &Patch::Merge(&suspend_patch))
                    .await
                    .map_err(Error::Kube)?;
            }
            TargetKind::PackerBuild => {
                let api: Api<PackerBuild> =
                    Api::namespaced(state.client.clone(), target_ns);
                api.patch(&target.name, &pp, &Patch::Merge(&suspend_patch))
                    .await
                    .map_err(Error::Kube)?;
            }
        }

        let action = if should_suspend { "suspended" } else { "resumed" };
        info!(target = %target.name, kind = %target.kind, action, "Enforcement applied");
    }

    Ok(())
}

async fn execute_reaction(
    binding: &ComplianceBinding,
    reaction: &crate::crd::Reaction,
    _state: &ControllerState,
) -> std::result::Result<(), Error> {
    match reaction.action {
        ReactionAction::Webhook => {
            if let Some(ref url) = reaction.webhook_url {
                let message = reaction
                    .message_template
                    .as_deref()
                    .unwrap_or("Compliance state changed");

                let payload = serde_json::json!({
                    "binding": binding.name_any(),
                    "event": format!("{}", reaction.event),
                    "message": message,
                    "timestamp": chrono::Utc::now(),
                });

                let http = reqwest::Client::new();
                match http
                    .post(url)
                    .json(&payload)
                    .timeout(Duration::from_secs(10))
                    .send()
                    .await
                {
                    Ok(resp) => {
                        info!(url = %url, status = resp.status().as_u16(), "Webhook sent");
                    }
                    Err(e) => {
                        warn!(url = %url, error = %e, "Webhook failed");
                    }
                }
            }
        }
        ReactionAction::SuspendTarget | ReactionAction::ResumeTarget => {
            // Handled by enforce_on_targets
        }
        ReactionAction::Event => {
            // TODO: emit K8s Event on target resources
        }
        ReactionAction::Reconcile => {
            // TODO: trigger reconciliation by touching annotation
        }
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Status helpers
// ---------------------------------------------------------------------------

async fn update_compliance_state(
    binding: &ComplianceBinding,
    state_val: BindingComplianceState,
    state: &ControllerState,
) -> std::result::Result<(), Error> {
    let patch = serde_json::json!({
        "status": {
            "complianceState": state_val,
            "observedGeneration": binding.metadata.generation.unwrap_or(0),
        }
    });

    crate::controller::status_patch::patch_status(binding, &state.client, patch)
        .await
        .map_err(Error::Kube)?;

    Ok(())
}

async fn update_full_status(
    binding: &ComplianceBinding,
    compliance_state: BindingComplianceState,
    targets: &[TargetStatus],
    compliance_hash: Option<&str>,
    state: &ControllerState,
) -> std::result::Result<(), Error> {
    let ready = compliance_state == BindingComplianceState::Compliant;
    let conditions = vec![create_condition(
        "Ready",
        ready,
        &format!("{compliance_state}"),
        &format!("{compliance_state}"),
    )];

    let new_observed_gen = binding.metadata.generation.unwrap_or(0);

    // Diff-gate: skip the PATCH when nothing observable would change.
    // `complianceHash` is the canonical fingerprint of the rendered
    // target set, so when it (and the scalar state fields) haven't
    // moved, the targets list — even if reordered — represents the
    // same posture and we can skip without missing semantic drift.
    // Conditions compared semantically (`create_condition` restamps
    // lastTransitionTime on every call).
    let needs_patch = binding_status_needs_patch(
        binding.status.as_ref(),
        compliance_state,
        targets.len() as u32,
        compliance_hash,
        &conditions,
        new_observed_gen,
    );

    if needs_patch {
        let patch = serde_json::json!({
            "status": {
                "complianceState": compliance_state,
                "targetCount": targets.len(),
                "targets": targets,
                "complianceHash": compliance_hash,
                "conditions": conditions,
                "observedGeneration": new_observed_gen,
            }
        });

        crate::controller::status_patch::patch_status(binding, &state.client, patch)
            .await
            .map_err(Error::Kube)?;
    } else {
        debug!(
            "ComplianceBinding status unchanged; skipping patch (avoids self-trigger watch loop)"
        );
    }

    // Emit pangea_compliance_bindings_gated_targets so the
    // ComplianceBindingGating alert can fire (was silently inert
    // pre-U3). Counts the number of targets currently gated due to
    // non-compliance.
    let gated_count = targets.iter().filter(|t| t.gated).count() as i64;
    state.metrics.set_compliance_binding_gated_targets(
        &binding.namespace().unwrap_or_default(),
        &binding.name_any(),
        gated_count,
    );

    Ok(())
}

fn error_policy(
    _binding: Arc<ComplianceBinding>,
    error: &Error,
    ctx: Arc<ControllerState>,
) -> Action {
    crate::controller::error_policy::run_error_policy(
        &ctx.metrics,
        crate::crd::ControllerKind::ComplianceBinding,
        error,
        ERROR_REQUEUE_INTERVAL,
    )
}

/// Diff-gate for the ComplianceBinding `update_full_status` PATCH.
///
/// Returns `true` only when at least one observable status field
/// would change. Without this gate, every reconcile re-PATCHes the
/// same `(state, hash, count, conditions)` tuple with a fresh
/// `lastTransitionTime` — `create_condition` calls `Utc::now()` on
/// every invocation — which refires the watch and creates the same
/// closed-loop reconcile cycle the operator hit on rio.
///
/// The targets list itself is intentionally omitted from the gate:
/// `complianceHash` is the canonical fingerprint of the rendered
/// target set, so a hash match implies a posture match even if the
/// list reorders. Skipping the per-target compare also avoids needing
/// `PartialEq` on `TargetStatus` (which carries free-form fields).
fn binding_status_needs_patch(
    prev: Option<&ComplianceBindingStatus>,
    new_state: BindingComplianceState,
    new_target_count: u32,
    new_hash: Option<&str>,
    new_conditions: &[crate::crd::Condition],
    new_observed_gen: i64,
) -> bool {
    let prev_state: Option<BindingComplianceState> = prev.and_then(|s| s.compliance_state);
    let prev_target_count = prev.map(|s| s.target_count).unwrap_or(0);
    let prev_hash = prev.and_then(|s| s.compliance_hash.as_deref());
    let prev_observed_gen = prev.map(|s| s.observed_generation).unwrap_or(0);
    let prev_conditions: &[crate::crd::Condition] =
        prev.map(|s| s.conditions.as_slice()).unwrap_or(&[]);
    let conditions_match = prev_conditions.len() == new_conditions.len()
        && new_conditions.iter().all(|n| {
            prev_conditions.iter().any(|p| {
                p.r#type == n.r#type
                    && p.status == n.status
                    && p.reason == n.reason
                    && p.message == n.message
            })
        });
    !conditions_match
        || prev_state != Some(new_state)
        || prev_target_count != new_target_count
        || prev_hash != new_hash
        || prev_observed_gen != new_observed_gen
        || prev.is_none()
}

#[cfg(test)]
mod tests {
    use super::binding_status_needs_patch;
    use crate::crd::compliance_binding::{
        BindingComplianceState, ComplianceBinding, ComplianceBindingStatus,
    };
    use crate::crd::Condition;
    use kube::CustomResourceExt;

    fn cond(typ: &str, status: &str, reason: &str, msg: &str) -> Condition {
        Condition {
            r#type: typ.into(),
            status: status.into(),
            reason: reason.into(),
            message: msg.into(),
            last_transition_time: chrono::TimeZone::with_ymd_and_hms(
                &chrono::Utc, 2025, 1, 1, 0, 0, 0
            ).unwrap(),
        }
    }

    fn compliant_status(observed_gen: i64, hash: &str) -> ComplianceBindingStatus {
        ComplianceBindingStatus {
            compliance_state: Some(BindingComplianceState::Compliant),
            target_count: 5,
            targets: vec![],
            compliance_hash: Some(hash.into()),
            conditions: vec![cond("Ready", "True", "Compliant", "Compliant")],
            observed_generation: observed_gen,
        }
    }

    #[test]
    fn binding_first_reconcile_must_patch() {
        let new_conds = vec![cond("Ready", "True", "Compliant", "Compliant")];
        assert!(
            binding_status_needs_patch(
                None,
                BindingComplianceState::Compliant,
                5,
                Some("h1"),
                &new_conds,
                1,
            ),
            "missing prev status must always force a PATCH"
        );
    }

    #[test]
    fn binding_compliance_state_change_must_patch() {
        let prev = compliant_status(2, "h1");
        let new_conds = vec![cond("Ready", "False", "NonCompliant", "NonCompliant")];
        assert!(
            binding_status_needs_patch(
                Some(&prev),
                BindingComplianceState::NonCompliant,
                5,
                Some("h1"),
                &new_conds,
                2,
            ),
            "compliance state flip must force a PATCH"
        );
    }

    #[test]
    fn binding_hash_change_must_patch() {
        let prev = compliant_status(2, "h1");
        let new_conds = vec![cond("Ready", "True", "Compliant", "Compliant")];
        assert!(
            binding_status_needs_patch(
                Some(&prev),
                BindingComplianceState::Compliant,
                5,
                Some("h2"),
                &new_conds,
                2,
            ),
            "hash change (target set re-rendered) must force a PATCH"
        );
    }

    #[test]
    fn binding_steady_state_skips_patch() {
        let prev = compliant_status(7, "h1");
        // Same posture, same hash, same generation — only fresh
        // `lastTransitionTime` would differ (`create_condition` always
        // stamps Utc::now()). Gate must skip.
        let new_conds = vec![cond("Ready", "True", "Compliant", "Compliant")];
        assert!(
            !binding_status_needs_patch(
                Some(&prev),
                BindingComplianceState::Compliant,
                5,
                Some("h1"),
                &new_conds,
                7,
            ),
            "must NOT patch on timestamp-only churn"
        );
    }

    #[test]
    fn binding_observed_gen_advance_must_patch() {
        let prev = compliant_status(7, "h1");
        let new_conds = vec![cond("Ready", "True", "Compliant", "Compliant")];
        assert!(
            binding_status_needs_patch(
                Some(&prev),
                BindingComplianceState::Compliant,
                5,
                Some("h1"),
                &new_conds,
                8,
            ),
            "observed generation bump must force a PATCH"
        );
    }

    #[test]
    fn status_default_round_trips() {
        let s = ComplianceBindingStatus::default();
        let j = serde_json::to_string(&s).unwrap();
        let back: ComplianceBindingStatus = serde_json::from_str(&j).unwrap();
        assert_eq!(format!("{:?}", s), format!("{:?}", back));
    }

    #[test]
    fn crd_yaml_renders_cleanly() {
        let yaml = serde_yaml::to_string(&ComplianceBinding::crd()).expect("CRD serializes");
        assert!(yaml.contains("compliancebindings.pangea.pleme.io"));
        assert!(yaml.contains("- pangea"));
    }

    #[test]
    fn status_default_initializes_to_no_compliance_state() {
        // The status is "unknown" until the binding observes its first
        // schedule evaluation — verifies the default isn't accidentally
        // claiming Compliant.
        let s = ComplianceBindingStatus::default();
        assert!(s.compliance_state.is_none());
    }

    // ── D4 deep tests — CRD shape + enum coverage ──

    #[test]
    fn crd_namespaced_scope() {
        // Compliance bindings are inherently namespaced: a binding controls
        // resources within a namespace. A scope drift to Cluster would be a
        // breaking change for every existing binding.
        let yaml = serde_yaml::to_string(&ComplianceBinding::crd()).expect("CRD serializes");
        assert!(yaml.contains("scope: Namespaced"));
    }

    #[test]
    fn compliance_state_enum_serializes_known_variants() {
        use crate::crd::compliance_binding::BindingComplianceState;
        // Drift in these tags would silently break operator-emitted status
        // patches against existing CRs.
        let variants = [
            BindingComplianceState::Compliant,
            BindingComplianceState::NonCompliant,
            BindingComplianceState::Unknown,
            BindingComplianceState::Suspended,
        ];
        for v in variants {
            let s = serde_json::to_string(&v).unwrap();
            let back: BindingComplianceState = serde_json::from_str(&s).unwrap();
            assert_eq!(format!("{:?}", v), format!("{:?}", back));
        }
        // Default is Unknown — the binding starts in observation mode.
        assert_eq!(BindingComplianceState::default(), BindingComplianceState::Unknown);
    }
}
