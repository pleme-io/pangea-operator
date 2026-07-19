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
use k8s_openapi::api::core::v1::ObjectReference;
use kube::{
    api::{Api, Patch, PatchParams},
    runtime::controller::Action,
    runtime::events::{Event, EventType, Recorder, Reporter},
    Resource, ResourceExt,
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
    state: &ControllerState,
) -> std::result::Result<(), Error> {
    match reaction.action {
        ReactionAction::Webhook => {
            if let Some(ref url) = reaction.webhook_url {
                let message = reaction_message(reaction);

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
            emit_target_events(binding, reaction, state).await;
        }
        ReactionAction::Reconcile => {
            request_target_reconcile(binding, state).await;
        }
    }

    Ok(())
}

/// Default message body shared by every reaction kind that surfaces
/// text to a human or an event stream (`Webhook`, `Event`).
fn reaction_message(reaction: &crate::crd::Reaction) -> &str {
    reaction
        .message_template
        .as_deref()
        .unwrap_or("Compliance state changed")
}

/// Build the `ObjectReference` for a compliance-binding target.
///
/// Constructed directly from the typed `TargetKind` (group/version/kind
/// come from each CRD's own `#[kube(...)]` registration) rather than a
/// live GET — the same 4 kinds `enforce_on_targets` already patches
/// without fetching first, and a target that doesn't exist yet would
/// fail there before an Event ever needed to be emitted on it.
fn target_object_ref(target: &crate::crd::BindingTarget, namespace: &str) -> ObjectReference {
    let (api_version, kind) = match target.kind {
        TargetKind::InfrastructureTemplate => (
            <InfrastructureTemplate as Resource>::api_version(&()),
            <InfrastructureTemplate as Resource>::kind(&()),
        ),
        TargetKind::InfrastructureFlow => (
            <InfrastructureFlow as Resource>::api_version(&()),
            <InfrastructureFlow as Resource>::kind(&()),
        ),
        TargetKind::ImagePipeline => (
            <ImagePipeline as Resource>::api_version(&()),
            <ImagePipeline as Resource>::kind(&()),
        ),
        TargetKind::PackerBuild => (
            <PackerBuild as Resource>::api_version(&()),
            <PackerBuild as Resource>::kind(&()),
        ),
    };
    ObjectReference {
        api_version: Some(api_version.into_owned()),
        kind: Some(kind.into_owned()),
        name: Some(target.name.clone()),
        namespace: Some(namespace.to_string()),
        ..Default::default()
    }
}

/// `EventType` + PascalCase `reason` for a `ReactionAction::Event`
/// reaction, derived from the `ComplianceEvent` that triggered it.
/// Pure so the mapping is unit-testable without a live client.
fn reaction_event_type_and_reason(reaction: &crate::crd::Reaction) -> (EventType, String) {
    let event_type = match reaction.event {
        crate::crd::ComplianceEvent::NonCompliant
        | crate::crd::ComplianceEvent::ControlFailed
        | crate::crd::ComplianceEvent::Error => EventType::Warning,
        crate::crd::ComplianceEvent::Compliant | crate::crd::ComplianceEvent::HashChanged => {
            EventType::Normal
        }
    };
    (event_type, format!("Compliance{}", reaction.event))
}

/// `ReactionAction::Event`: emit a K8s Event on each of the binding's
/// targets (per the CRD doc comment: "Emit a K8s Event on the target
/// resource"). Best-effort — mirrors `template::events::record_event`'s
/// swallow-and-warn contract, since Events are observability, not
/// load-bearing; a publish failure never fails the reconcile.
async fn emit_target_events(
    binding: &ComplianceBinding,
    reaction: &crate::crd::Reaction,
    state: &ControllerState,
) {
    let namespace = binding.namespace().unwrap_or_default();
    let (event_type, reason) = reaction_event_type_and_reason(reaction);
    let message = reaction_message(reaction).to_string();

    let reporter = Reporter {
        controller: "pangea-operator".into(),
        instance: std::env::var("POD_NAME").ok(),
    };
    let recorder = Recorder::new(state.client.clone(), reporter);

    for target in &binding.spec.targets {
        let target_ns = target.namespace.as_deref().unwrap_or(&namespace);
        let obj_ref = target_object_ref(target, target_ns);
        let event = Event {
            type_: event_type,
            reason: reason.clone(),
            note: Some(message.clone()),
            action: reason.clone(),
            secondary: None,
        };
        if let Err(e) = recorder.publish(&event, &obj_ref).await {
            warn!(
                binding = %binding.name_any(),
                target = %target.name,
                kind = %target.kind,
                error = %e,
                "Failed to record compliance-binding reaction Event"
            );
        }
    }
}

/// Annotation a `ReactionAction::Reconcile` request patches onto each
/// target. Reuses the exact key the GraphQL `apply` mutation already
/// patches onto `InfrastructureTemplate`
/// (`api::graphql::resolvers::MutationRoot::apply`) rather than minting
/// a second "force reconcile" convention.
const RECONCILE_REQUESTED_ANNOTATION: &str = "pangea.pleme.io/reconcile-requested";

/// Build the merge-patch body for a `ReactionAction::Reconcile`
/// request. Pure so the patch shape is unit-testable without a live
/// client.
fn reconcile_request_patch(requested_at: chrono::DateTime<chrono::Utc>) -> serde_json::Value {
    serde_json::json!({
        "metadata": {
            "annotations": {
                RECONCILE_REQUESTED_ANNOTATION: requested_at.to_rfc3339(),
            }
        }
    })
}

/// `ReactionAction::Reconcile`: patch a reconcile-requested annotation
/// onto each of the binding's targets (per the CRD doc comment:
/// "Trigger a reconciliation of the target"). Best-effort, matching
/// every other reaction kind in this function — a patch failure logs
/// and is dropped, it never fails the reconcile.
///
/// Scope note: every target controller (`InfrastructureTemplate`,
/// `InfrastructureFlow`, `ImagePipeline`, `PackerBuild`) runs behind
/// `generation_filter::filtered_controller`, which drops watch events
/// whose `metadata.generation` is unchanged — an annotation-only patch
/// never bumps `generation`, so this does not jump the target ahead of
/// its own periodic `Action::requeue` tick (same characteristic the
/// existing GraphQL `reconcile-requested`/`approved`/`destroy-requested`
/// annotations already have). What it fixes is the reported gap: the
/// request becomes observable (`kubectl get -o yaml` on the target, plus
/// a log line) instead of a silent no-op. An immediate-reconcile trigger
/// would need a spec-level field (bumps `generation`) — a schema change
/// across 4 CRDs, out of scope for this fix.
async fn request_target_reconcile(binding: &ComplianceBinding, state: &ControllerState) {
    let namespace = binding.namespace().unwrap_or_default();
    let patch = reconcile_request_patch(chrono::Utc::now());
    let pp = PatchParams::apply("pangea-operator");

    for target in &binding.spec.targets {
        let target_ns = target.namespace.as_deref().unwrap_or(&namespace);

        let result: std::result::Result<(), kube::Error> = match target.kind {
            TargetKind::InfrastructureTemplate => {
                let api: Api<InfrastructureTemplate> =
                    Api::namespaced(state.client.clone(), target_ns);
                api.patch(&target.name, &pp, &Patch::Merge(&patch))
                    .await
                    .map(|_| ())
            }
            TargetKind::InfrastructureFlow => {
                let api: Api<InfrastructureFlow> =
                    Api::namespaced(state.client.clone(), target_ns);
                api.patch(&target.name, &pp, &Patch::Merge(&patch))
                    .await
                    .map(|_| ())
            }
            TargetKind::ImagePipeline => {
                let api: Api<ImagePipeline> = Api::namespaced(state.client.clone(), target_ns);
                api.patch(&target.name, &pp, &Patch::Merge(&patch))
                    .await
                    .map(|_| ())
            }
            TargetKind::PackerBuild => {
                let api: Api<PackerBuild> = Api::namespaced(state.client.clone(), target_ns);
                api.patch(&target.name, &pp, &Patch::Merge(&patch))
                    .await
                    .map(|_| ())
            }
        };

        match result {
            Ok(()) => {
                info!(
                    binding = %binding.name_any(),
                    target = %target.name,
                    kind = %target.kind,
                    "Reconcile requested on target (annotation patched)"
                );
            }
            Err(e) => {
                warn!(
                    binding = %binding.name_any(),
                    target = %target.name,
                    kind = %target.kind,
                    error = %e,
                    "Failed to patch reconcile-requested annotation"
                );
            }
        }
    }
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
    let conditions_match = crate::controller::status::conditions_observably_equal(
        prev_conditions,
        new_conditions,
    );
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

    // ── ReactionAction::Event / ::Reconcile — was a silent no-op TODO,
    // now builds a real ObjectReference / Event payload / annotation
    // patch. These functions didn't exist before the fix (the match
    // arms were empty), so these tests could not have compiled, let
    // alone passed, prior to it. ──

    use super::{
        reaction_event_type_and_reason, reaction_message, reconcile_request_patch,
        target_object_ref, RECONCILE_REQUESTED_ANNOTATION,
    };
    use crate::crd::compliance_binding::{BindingTarget, ComplianceEvent, Reaction, TargetKind};
    use crate::crd::{ImagePipeline, InfrastructureFlow, InfrastructureTemplate, PackerBuild};
    use kube::runtime::events::EventType;

    fn target(kind: TargetKind, name: &str) -> BindingTarget {
        BindingTarget {
            kind,
            name: name.into(),
            namespace: None,
        }
    }

    fn reaction(event: ComplianceEvent, action: crate::crd::ReactionAction) -> Reaction {
        Reaction {
            event,
            action,
            webhook_url: None,
            message_template: None,
        }
    }

    #[test]
    fn target_object_ref_matches_each_crd_registration() {
        // Each arm must resolve to the SAME group/version/kind the CRD
        // itself is registered under (`#[kube(group, version, kind)]`) —
        // a mismatch here would emit an Event `regarding` an object
        // reference the apiserver can't resolve.
        let cases: [(TargetKind, &str); 4] = [
            (TargetKind::InfrastructureTemplate, "InfrastructureTemplate"),
            (TargetKind::InfrastructureFlow, "InfrastructureFlow"),
            (TargetKind::ImagePipeline, "ImagePipeline"),
            (TargetKind::PackerBuild, "PackerBuild"),
        ];
        for (kind, expected_kind) in cases {
            let t = target(kind, "my-target");
            let obj_ref = target_object_ref(&t, "my-ns");
            assert_eq!(obj_ref.api_version.as_deref(), Some("pangea.pleme.io/v1alpha1"));
            assert_eq!(obj_ref.kind.as_deref(), Some(expected_kind));
            assert_eq!(obj_ref.name.as_deref(), Some("my-target"));
            assert_eq!(obj_ref.namespace.as_deref(), Some("my-ns"));
        }

        // Cross-check against each CRD's own kube() registration directly,
        // so this test breaks if a CRD's kind/group/version ever drifts
        // independently of this match.
        assert_eq!(
            <InfrastructureTemplate as kube::Resource>::kind(&()).as_ref(),
            "InfrastructureTemplate"
        );
        assert_eq!(
            <InfrastructureFlow as kube::Resource>::kind(&()).as_ref(),
            "InfrastructureFlow"
        );
        assert_eq!(
            <ImagePipeline as kube::Resource>::kind(&()).as_ref(),
            "ImagePipeline"
        );
        assert_eq!(
            <PackerBuild as kube::Resource>::kind(&()).as_ref(),
            "PackerBuild"
        );
    }

    #[test]
    fn reaction_event_maps_noncompliant_to_warning() {
        let r = reaction(ComplianceEvent::NonCompliant, crate::crd::ReactionAction::Event);
        let (event_type, reason) = reaction_event_type_and_reason(&r);
        assert_eq!(event_type, EventType::Warning);
        assert_eq!(reason, "ComplianceNonCompliant");
    }

    #[test]
    fn reaction_event_maps_compliant_to_normal() {
        let r = reaction(ComplianceEvent::Compliant, crate::crd::ReactionAction::Event);
        let (event_type, reason) = reaction_event_type_and_reason(&r);
        assert_eq!(event_type, EventType::Normal);
        assert_eq!(reason, "ComplianceCompliant");
    }

    #[test]
    fn reaction_event_maps_error_to_warning() {
        let r = reaction(ComplianceEvent::Error, crate::crd::ReactionAction::Event);
        let (event_type, _reason) = reaction_event_type_and_reason(&r);
        assert_eq!(event_type, EventType::Warning);
    }

    #[test]
    fn reaction_message_falls_back_to_default() {
        let r = reaction(ComplianceEvent::NonCompliant, crate::crd::ReactionAction::Event);
        assert_eq!(reaction_message(&r), "Compliance state changed");
    }

    #[test]
    fn reaction_message_uses_template_when_set() {
        let mut r = reaction(ComplianceEvent::NonCompliant, crate::crd::ReactionAction::Event);
        r.message_template = Some("custom message".into());
        assert_eq!(reaction_message(&r), "custom message");
    }

    #[test]
    fn reconcile_request_patch_carries_the_shared_annotation_key() {
        // Same annotation key the GraphQL `apply` mutation already
        // patches onto InfrastructureTemplate — one convention, not two.
        assert_eq!(RECONCILE_REQUESTED_ANNOTATION, "pangea.pleme.io/reconcile-requested");

        let ts = chrono::TimeZone::with_ymd_and_hms(&chrono::Utc, 2026, 7, 19, 0, 0, 0).unwrap();
        let patch = reconcile_request_patch(ts);
        let annotated = &patch["metadata"]["annotations"][RECONCILE_REQUESTED_ANNOTATION];
        assert_eq!(annotated.as_str(), Some(ts.to_rfc3339().as_str()));
    }
}
