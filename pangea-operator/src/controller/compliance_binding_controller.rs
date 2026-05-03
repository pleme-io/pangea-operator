//! Controller for ComplianceBinding resources.
//!
//! Watches ComplianceSchedule status and enforces compliance on bound targets.
//! When compliance state changes, the controller executes configured reactions:
//! suspending/resuming targets, posting webhooks, updating sekiban gates.

use crate::crd::{
    BindingComplianceState, ComplianceBinding, ComplianceSchedule, ComplianceSchedulePhase,
    EnforcementLevel, ImagePipeline, InfrastructureFlow, InfrastructureTemplate, PackerBuild,
    ReactionAction, TargetKind, TargetStatus,
};
use crate::error::Error;

use futures::StreamExt;
use kube::{
    api::{Api, Patch, PatchParams},
    runtime::{
        controller::{Action, Controller},
        watcher::Config,
    },
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
        let api: Api<ComplianceBinding> = Api::all(client.clone());
        let state = Arc::new(self.state);

        info!("Starting ComplianceBinding controller");

        Controller::new(api, Config::default())
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
    let name = binding.name_any();
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
    let namespace = binding.namespace().unwrap_or_default();
    let name = binding.name_any();
    let api: Api<ComplianceBinding> = Api::namespaced(state.client.clone(), &namespace);

    let patch = serde_json::json!({
        "status": {
            "complianceState": state_val,
            "observedGeneration": binding.metadata.generation.unwrap_or(0),
        }
    });

    api.patch_status(&name, &PatchParams::apply("pangea-operator"), &Patch::Merge(&patch))
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
    let namespace = binding.namespace().unwrap_or_default();
    let name = binding.name_any();
    let api: Api<ComplianceBinding> = Api::namespaced(state.client.clone(), &namespace);

    let ready = compliance_state == BindingComplianceState::Compliant;
    let conditions = vec![create_condition(
        "Ready",
        ready,
        &format!("{compliance_state}"),
        &format!("{compliance_state}"),
    )];

    let patch = serde_json::json!({
        "status": {
            "complianceState": compliance_state,
            "targetCount": targets.len(),
            "targets": targets,
            "complianceHash": compliance_hash,
            "conditions": conditions,
            "observedGeneration": binding.metadata.generation.unwrap_or(0),
        }
    });

    api.patch_status(&name, &PatchParams::apply("pangea-operator"), &Patch::Merge(&patch))
        .await
        .map_err(Error::Kube)?;

    Ok(())
}

fn error_policy(
    _binding: Arc<ComplianceBinding>,
    error: &Error,
    _ctx: Arc<ControllerState>,
) -> Action {
    _ctx
        .metrics
        .record_reconcile(crate::crd::ControllerKind::ComplianceBinding, "error");
    warn!(error = %error, "ComplianceBinding error policy triggered");
    Action::requeue(ERROR_REQUEUE_INTERVAL)
}

#[cfg(test)]
mod tests {
    use crate::crd::compliance_binding::{ComplianceBinding, ComplianceBindingStatus};
    use kube::CustomResourceExt;

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
}
