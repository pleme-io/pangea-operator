//! Controller for InfrastructureTemplate resources.

use crate::crd::{InfrastructureTemplate, InfrastructureTemplateStatus, Phase};
use crate::error::{Error, Result};
use crate::observability::Metrics;

use futures::StreamExt;
use kube::{
    api::{Api, Patch, PatchParams},
    runtime::{
        controller::{Action, Controller},
        watcher::Config,
    },
    Client, ResourceExt,
};
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, error, info, instrument, warn};

use super::{
    exponential_backoff, parse_duration, ControllerState, ReconcileAction,
    DEFAULT_REQUEUE_INTERVAL, ERROR_REQUEUE_INTERVAL, SHORT_REQUEUE_INTERVAL,
};

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
        let api: Api<InfrastructureTemplate> = Api::all(client.clone());
        let state = Arc::new(self.state);

        info!("Starting InfrastructureTemplate controller");

        Controller::new(api, Config::default())
            .run(
                move |template, ctx| {
                    let state = Arc::clone(&ctx);
                    async move { reconcile_template(template, state).await }
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

    // Check if suspended
    if template.spec.suspend {
        info!("Template is suspended, skipping reconciliation");
        return Ok(Action::requeue(DEFAULT_REQUEUE_INTERVAL));
    }

    // Determine current phase
    let current_phase = template
        .status
        .as_ref()
        .and_then(|s| s.phase)
        .unwrap_or(Phase::Pending);

    let action = match current_phase {
        Phase::Pending => handle_pending(&template, &state).await?,
        Phase::Compiling => handle_compiling(&template, &state).await?,
        Phase::Initializing => handle_initializing(&template, &state).await?,
        Phase::Planning => handle_planning(&template, &state).await?,
        Phase::Applying => handle_applying(&template, &state).await?,
        Phase::Ready => handle_ready(&template, &state).await?,
        Phase::Drifted => handle_drifted(&template, &state).await?,
        Phase::Failed => handle_failed(&template, &state).await?,
        Phase::Destroying => handle_destroying(&template, &state).await?,
    };

    Ok(action.into())
}

/// Handle Pending phase - prepare for compilation.
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

/// Handle Compiling phase - compile Ruby DSL to Terraform JSON.
async fn handle_compiling(
    template: &InfrastructureTemplate,
    state: &ControllerState,
) -> Result<ReconcileAction> {
    info!("Template in Compiling phase");

    // TODO: Call Ruby compiler sidecar via gRPC
    // For now, transition to next phase
    update_phase(template, Phase::Initializing, state).await?;

    Ok(ReconcileAction::Requeue(SHORT_REQUEUE_INTERVAL))
}

/// Handle Initializing phase - run `tofu init`.
async fn handle_initializing(
    template: &InfrastructureTemplate,
    state: &ControllerState,
) -> Result<ReconcileAction> {
    info!("Template in Initializing phase");

    // TODO: Execute `tofu init` with PostgreSQL backend config
    update_phase(template, Phase::Planning, state).await?;

    Ok(ReconcileAction::Requeue(SHORT_REQUEUE_INTERVAL))
}

/// Handle Planning phase - run `tofu plan`.
async fn handle_planning(
    template: &InfrastructureTemplate,
    state: &ControllerState,
) -> Result<ReconcileAction> {
    info!("Template in Planning phase");

    // TODO: Execute `tofu plan` and parse output
    // If auto_approve, transition to Applying
    // Otherwise, stay in Planning and wait for approval

    if template.spec.auto_approve {
        update_phase(template, Phase::Applying, state).await?;
        Ok(ReconcileAction::Requeue(SHORT_REQUEUE_INTERVAL))
    } else {
        // Stay in Planning, wait for manual approval
        Ok(ReconcileAction::Requeue(DEFAULT_REQUEUE_INTERVAL))
    }
}

/// Handle Applying phase - run `tofu apply`.
async fn handle_applying(
    template: &InfrastructureTemplate,
    state: &ControllerState,
) -> Result<ReconcileAction> {
    info!("Template in Applying phase");

    // TODO: Execute `tofu apply -auto-approve`
    update_phase(template, Phase::Ready, state).await?;

    Ok(ReconcileAction::Requeue(DEFAULT_REQUEUE_INTERVAL))
}

/// Handle Ready phase - periodic drift detection.
async fn handle_ready(
    template: &InfrastructureTemplate,
    state: &ControllerState,
) -> Result<ReconcileAction> {
    debug!("Template in Ready phase, checking for drift");

    // Parse refresh interval
    let interval = parse_duration(&template.spec.refresh_interval)
        .unwrap_or(DEFAULT_REQUEUE_INTERVAL);

    // TODO: Run `tofu plan` for drift detection
    // If drift detected, update to Drifted phase

    Ok(ReconcileAction::Requeue(interval))
}

/// Handle Drifted phase - changes detected.
async fn handle_drifted(
    template: &InfrastructureTemplate,
    state: &ControllerState,
) -> Result<ReconcileAction> {
    warn!("Template has drift detected");
    state.metrics.drift_detected_total.inc();

    if template.spec.auto_approve {
        update_phase(template, Phase::Planning, state).await?;
        Ok(ReconcileAction::Requeue(SHORT_REQUEUE_INTERVAL))
    } else {
        // Wait for manual approval
        Ok(ReconcileAction::Requeue(DEFAULT_REQUEUE_INTERVAL))
    }
}

/// Handle Failed phase - retry with backoff.
async fn handle_failed(
    template: &InfrastructureTemplate,
    state: &ControllerState,
) -> Result<ReconcileAction> {
    let failure_count = template.retry_count();
    warn!(failure_count, "Template in Failed phase");

    if template.retries_exhausted() {
        warn!("Retries exhausted, not requeuing");
        return Ok(ReconcileAction::Requeue(Duration::from_secs(3600))); // Check hourly
    }

    let backoff = exponential_backoff(
        failure_count,
        template
            .spec
            .retry_policy
            .as_ref()
            .map(|p| p.backoff_seconds)
            .unwrap_or(30),
        600,
    );

    Ok(ReconcileAction::Requeue(backoff))
}

/// Handle Destroying phase - run `tofu destroy`.
async fn handle_destroying(
    template: &InfrastructureTemplate,
    state: &ControllerState,
) -> Result<ReconcileAction> {
    info!("Template in Destroying phase");

    // TODO: Execute `tofu destroy -auto-approve`
    // Remove finalizer when complete

    Ok(ReconcileAction::Requeue(SHORT_REQUEUE_INTERVAL))
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

/// Update the phase in the template status.
async fn update_phase(
    template: &InfrastructureTemplate,
    phase: Phase,
    state: &ControllerState,
) -> Result<()> {
    let name = template.name_any();
    let namespace = template.namespace().unwrap_or_default();

    let api: Api<InfrastructureTemplate> = Api::namespaced(state.client.clone(), &namespace);

    let status = InfrastructureTemplateStatus {
        phase: Some(phase),
        observed_generation: template.metadata.generation.unwrap_or(0),
        ..template.status.clone().unwrap_or_default()
    };

    let patch = serde_json::json!({ "status": status });

    api.patch_status(
        &name,
        &PatchParams::apply("pangea-operator"),
        &Patch::Merge(&patch),
    )
    .await?;

    state.metrics.templates_by_phase
        .with_label_values(&[&phase.to_string()])
        .inc();

    info!(?phase, "Updated template phase");
    Ok(())
}

/// Error policy for the controller.
fn error_policy(
    _obj: Arc<InfrastructureTemplate>,
    error: &Error,
    _ctx: Arc<ControllerState>,
) -> Action {
    error!(%error, "Reconciliation error");

    if error.is_retryable() {
        Action::requeue(ERROR_REQUEUE_INTERVAL)
    } else {
        // Non-retryable errors get longer backoff
        Action::requeue(Duration::from_secs(300))
    }
}

impl From<ReconcileAction> for Action {
    fn from(action: ReconcileAction) -> Self {
        match action {
            ReconcileAction::Requeue(duration) => Action::requeue(duration),
            ReconcileAction::Done => Action::await_change(),
        }
    }
}
