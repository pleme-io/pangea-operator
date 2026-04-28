//! Controller for InfrastructureTemplate resources.

use crate::backend::{BackendConfigGenerator, Credentials};
use crate::crd::{
    InfrastructureTemplate, PangeaNamespace, Phase, PolicyDecision, PolicyEvaluation,
    ResourceSummary, SettlingExhaustionAction, SettlingPolicy,
};
use crate::error::{Error, Result};
use crate::executor::{evaluate_policy, policy_is_configured, Plan};

use chrono::Utc;
use futures::StreamExt;
use k8s_openapi::api::core::v1::{ConfigMap, Secret};
use kube::{
    api::{Api, Patch, PatchParams},
    runtime::{
        controller::{Action, Controller},
        events::{Event, EventType, Recorder, Reporter},
        watcher::Config,
    },
    Resource, ResourceExt,
};
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, error, info, instrument, warn};

use super::{
    conditions_for_phase, conditions_for_suspended, exponential_backoff, parse_duration,
    ControllerState, ReconcileAction, DEFAULT_REQUEUE_INTERVAL, ERROR_REQUEUE_INTERVAL,
    SHORT_REQUEUE_INTERVAL,
};

/// Finalizer name for cleanup on deletion.
const FINALIZER_NAME: &str = "pangea.pleme.io/cleanup";

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

    // Check if suspended — emit conditions so FluxCD sees definitive state
    if template.spec.suspend {
        info!("Template is suspended, skipping reconciliation");
        let ns = template.namespace().unwrap_or_default();
        let api: Api<InfrastructureTemplate> = Api::namespaced(state.client.clone(), &ns);
        let patch = serde_json::json!({
            "status": { "conditions": conditions_for_suspended() }
        });
        let _ = api
            .patch_status(
                &name,
                &PatchParams::apply("pangea-operator"),
                &Patch::Merge(&patch),
            )
            .await;
        return Ok(Action::requeue(DEFAULT_REQUEUE_INTERVAL));
    }

    // Detect spec changes (generation mismatch) — clean workspace and restart from Pending.
    // This ensures stale .terraform state, lock files, and cached providers are cleared
    // when the template source or configuration changes.
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

    if current_gen != observed_gen && current_phase != Phase::Pending && current_phase != Phase::Destroying {
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

/// Handle Compiling phase - write template source to workspace.
///
/// For MVP, supports inline Terraform JSON and ConfigMap sources.
/// Ruby DSL compilation via sidecar is deferred to a future phase.
async fn handle_compiling(
    template: &InfrastructureTemplate,
    state: &ControllerState,
) -> Result<ReconcileAction> {
    info!("Template in Compiling phase");

    let workspace = state.workspace_manager.get_workspace(template).await?;
    let source = &template.spec.source;

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
        // Clone or fetch the Git repository, then read the template file
        let repo_dir = workspace.path.join("_repo");

        // Resolve git credentials if specified
        let mut env_vars = Vec::new();
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
                    env_vars.push(("GIT_ASKPASS".to_string(), askpass_script.to_string_lossy().to_string()));
                }
            }
        }

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

        // Read the template file from the cloned repo
        let template_path = repo_dir.join(&git_ref.path);
        tokio::fs::read_to_string(&template_path)
            .await
            .map_err(|e| {
                Error::Compilation(format!(
                    "Failed to read template file '{}': {}",
                    git_ref.path, e
                ))
            })?
    } else {
        return Err(Error::InvalidSource("No template source specified".into()));
    };

    // If content looks like Ruby DSL (not JSON), compile via sidecar
    let terraform_json = if content.trim_start().starts_with('{') {
        // Already JSON — use directly
        content
    } else {
        // Ruby DSL — call compiler sidecar
        let compiler_url = std::env::var("COMPILER_ENDPOINT")
            .unwrap_or_else(|_| "http://localhost:8082".to_string());

        // Variables from spec.variables (explicit, plain values)
        let mut variables = template
            .spec
            .variables
            .clone()
            .unwrap_or_default();

        // Plus every key from any providerCredentials secret —
        // Pangea workspace templates use `ENV.fetch('CF_API_TOKEN')`
        // etc. for provider config, which the compiler installs into
        // ENV around eval. The convention is that secret data keys
        // ARE the env var names (so the secret has `CF_API_TOKEN`,
        // `CF_ACCOUNT_ID`, … verbatim). Operator-side naming
        // transforms would re-introduce the kind of brittle wiring
        // we just stripped out elsewhere.
        if let Some(provider_creds) = template.spec.provider_credentials.as_ref() {
            let provider_secret_refs: Vec<&crate::crd::SecretRef> = [
                provider_creds.aws.as_ref().map(|c| &c.secret_ref),
                provider_creds.cloudflare.as_ref().map(|c| &c.secret_ref),
            ]
            .into_iter()
            .flatten()
            .collect();

            for sref in provider_secret_refs {
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

        let compile_request = serde_json::json!({
            "source": content,
            "variables": variables,
            "template_name": template.spec.template_name,
        });

        let http = reqwest::Client::new();
        let resp = http
            .post(format!("{}/compile", compiler_url))
            .json(&compile_request)
            .timeout(Duration::from_secs(120))
            .send()
            .await
            .map_err(|e| Error::Compilation(format!("Compiler sidecar request failed: {}", e)))?;

        if !resp.status().is_success() {
            let error_body: String = resp.text().await.unwrap_or_default();
            return Err(Error::Compilation(format!(
                "Compiler returned error: {}",
                error_body
            )));
        }

        let compile_result: serde_json::Value = resp
            .json::<serde_json::Value>()
            .await
            .map_err(|e| Error::Compilation(format!("Failed to parse compiler response: {}", e)))?;

        compile_result["terraform_json"]
            .as_str()
            .ok_or_else(|| Error::Compilation("Compiler response missing terraform_json field".into()))?
            .to_string()
    };

    // Write template content to workspace
    workspace.write_file("main.tf.json", &terraform_json).await?;
    info!("Template content written to workspace");

    update_phase(template, Phase::Initializing, state).await?;
    record_event(template, state, EventType::Normal, "Compiled", "Template source resolved and written to workspace").await;

    Ok(ReconcileAction::Requeue(SHORT_REQUEUE_INTERVAL))
}

/// Handle Initializing phase - configure backend and run `tofu init`.
async fn handle_initializing(
    template: &InfrastructureTemplate,
    state: &ControllerState,
) -> Result<ReconcileAction> {
    info!("Template in Initializing phase");

    let workspace = state.workspace_manager.get_workspace(template).await?;

    // Resolve PangeaNamespace to get backend configuration
    let pns_api: Api<PangeaNamespace> = Api::all(state.client.clone());
    let pangea_ns = pns_api.get(&template.spec.pangea_namespace).await.map_err(|_| {
        Error::NamespaceNotFound(template.spec.pangea_namespace.clone())
    })?;

    // Resolve PostgreSQL credentials from Secret
    if let Some(pg) = &pangea_ns.spec.backend.pg {
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

    // Write provider configuration if credentials are specified
    if let Some(provider_creds) = &template.spec.provider_credentials {
        let provider_config = resolve_provider_config(provider_creds, template, state).await?;
        BackendConfigGenerator::write_provider_config(provider_config, &workspace.path).await?;
    }

    // Run tofu init
    let result = state.executor.init(&workspace.path, &[]).await?;

    if result.success {
        info!("tofu init completed successfully");
        update_phase(template, Phase::Planning, state).await?;
        record_event(template, state, EventType::Normal, "Initialized", "Backend initialized successfully").await;
    } else {
        let err_msg = format!("tofu init failed: {}", result.stderr);
        warn!(%err_msg);
        update_phase_with_error(template, Phase::Failed, &err_msg, state).await?;
        record_event(template, state, EventType::Warning, "InitFailed", &err_msg).await;
    }

    Ok(ReconcileAction::Requeue(SHORT_REQUEUE_INTERVAL))
}

/// Handle Planning phase - run `tofu plan` and analyze changes.
async fn handle_planning(
    template: &InfrastructureTemplate,
    state: &ControllerState,
) -> Result<ReconcileAction> {
    info!("Template in Planning phase");

    let workspace = state.workspace_manager.get_workspace(template).await?;
    let plan_path = workspace.plan_path();

    // Run tofu plan
    let result = state
        .executor
        .plan(&workspace.path, Some(&plan_path), &[])
        .await?;

    if !result.success {
        let err_msg = format!("tofu plan failed: {}", result.stderr);
        warn!(%err_msg);
        update_phase_with_error(template, Phase::Failed, &err_msg, state).await?;
        record_event(template, state, EventType::Warning, "PlanFailed", &err_msg).await;
        return Ok(ReconcileAction::Requeue(ERROR_REQUEUE_INTERVAL));
    }

    // Parse plan output for resource summary + per-resource drift detail.
    // Drift details are capped at 50 entries so the K8s status object
    // stays tractable; full per-plan list is available via GraphQL.
    let (summary, raw_drifts) = if plan_path.exists() {
        let show_result = state.executor.show_plan(&workspace.path, &plan_path).await?;
        if show_result.success {
            match Plan::from_json(&show_result.stdout) {
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
                        added = s.added,
                        changed = s.changed,
                        destroyed = s.destroyed,
                        drift_count = details.len(),
                        "Plan analysis complete"
                    );
                    (Some(s), details)
                }
                Err(e) => {
                    warn!(error = %e, "Failed to parse plan JSON, continuing without summary");
                    (None, Vec::new())
                }
            }
        } else {
            (None, Vec::new())
        }
    } else {
        (None, Vec::new())
    };

    let has_changes = result.has_changes();

    // Run the per-resource policy engine. Empty rules + unset
    // defaultDecision = aggressive auto-apply on every change (the
    // documented default). The engine annotates each drift entry with
    // its resolved decision and emits an aggregate that drives the
    // plan→apply gate below.
    let policy_outcome = evaluate_policy(
        &template.spec.policies,
        template.spec.default_decision,
        &raw_drifts,
    );
    let policy_was_configured =
        policy_is_configured(&template.spec.policies, template.spec.default_decision);

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
            record_event(template, state, EventType::Warning, "PolicyRefused", &err_msg).await;
            Ok(ReconcileAction::Requeue(DEFAULT_REQUEUE_INTERVAL))
        }
        PolicyDecision::AutoApply => {
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
        PolicyDecision::RequireApproval => {
            // Standard pendingPlanHash / approvedPlanHash gate.
            let is_approved = template
                .status
                .as_ref()
                .and_then(|s| match (&s.pending_plan_hash, &s.approved_plan_hash) {
                    (Some(pending), Some(approved)) if !pending.is_empty() => {
                        Some(pending == approved)
                    }
                    _ => None,
                })
                .unwrap_or(false);

            if is_approved {
                info!("Plan approved by user, proceeding to apply");
                update_phase(template, Phase::Applying, state).await?;
                record_event(template, state, EventType::Normal, "PlanApproved", "Plan approved by user").await;
                Ok(ReconcileAction::Requeue(SHORT_REQUEUE_INTERVAL))
            } else {
                let plan_content = result.stdout.as_str();
                let plan_hash = format!("{:016x}", content_hash(plan_content));
                info!(
                    plan_hash,
                    require_approval_count = policy_outcome.evaluation.require_approval_count,
                    "Policy requires approval, waiting"
                );
                update_pending_plan_hash(template, &plan_hash, state).await?;
                record_event(
                    template,
                    state,
                    EventType::Normal,
                    "PlanPending",
                    &format!(
                        "Changes detected ({} require approval). Approve with: kubectl patch infra {} -n {} --type merge --subresource status -p '{{\"status\":{{\"approvedPlanHash\":\"{}\"}}}}'",
                        policy_outcome.evaluation.require_approval_count,
                        template.name_any(),
                        template.namespace().unwrap_or_default(),
                        plan_hash
                    ),
                ).await;
                Ok(ReconcileAction::Requeue(DEFAULT_REQUEUE_INTERVAL))
            }
        }
    }
}

/// Handle Applying phase - run `tofu apply`.
async fn handle_applying(
    template: &InfrastructureTemplate,
    state: &ControllerState,
) -> Result<ReconcileAction> {
    info!("Template in Applying phase");

    let workspace = state.workspace_manager.get_workspace(template).await?;
    let plan_path = workspace.plan_path();

    // Use plan file if it exists, otherwise apply directly
    let plan_file = if plan_path.exists() {
        Some(plan_path.as_path())
    } else {
        None
    };

    let result = state
        .executor
        .apply(&workspace.path, plan_file, true)
        .await?;

    if result.success {
        info!(duration_secs = result.duration.as_secs_f64(), "tofu apply completed successfully");

        // Fetch outputs
        let outputs = match state.executor.output(&workspace.path).await {
            Ok(output_result) if output_result.success => {
                serde_json::from_str(&output_result.stdout).ok()
            }
            _ => None,
        };

        update_apply_status(template, outputs, state).await?;
        update_phase(template, Phase::Ready, state).await?;
        record_event(template, state, EventType::Normal, "Applied", "Infrastructure applied successfully").await;
    } else {
        let err_msg = format!("tofu apply failed: {}", result.stderr);
        warn!(%err_msg);
        update_phase_with_error(template, Phase::Failed, &err_msg, state).await?;
        record_event(template, state, EventType::Warning, "ApplyFailed", &err_msg).await;
    }

    Ok(ReconcileAction::Requeue(DEFAULT_REQUEUE_INTERVAL))
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
async fn handle_ready(
    template: &InfrastructureTemplate,
    state: &ControllerState,
) -> Result<ReconcileAction> {
    let interval = parse_duration(&template.spec.refresh_interval)
        .unwrap_or(DEFAULT_REQUEUE_INTERVAL);

    if let Some(last_check) = template
        .status
        .as_ref()
        .and_then(|s| s.last_drift_check_at)
    {
        let elapsed = Utc::now().signed_duration_since(last_check);
        let interval_chrono = chrono::Duration::from_std(interval).unwrap_or(chrono::Duration::minutes(5));
        if elapsed < interval_chrono {
            debug!("Skipping drift check, last check was {}s ago", elapsed.num_seconds());
            return Ok(ReconcileAction::Requeue(interval));
        }
    }

    debug!("Running drift detection");

    let workspace = state.workspace_manager.get_workspace(template).await?;

    // We need structured drift data (for fingerprinting / settling)
    // so save the plan to a side file and parse it. If JSON parsing
    // fails we fall back to the boolean has_changes signal — better
    // a missed fingerprint than a controller wedge.
    let drift_plan_path = workspace.path.join("_drift_plan.tfplan");
    let result = state
        .executor
        .plan(&workspace.path, Some(&drift_plan_path), &[])
        .await?;

    update_drift_check_timestamp(template, state).await?;

    let drift_details: Vec<crate::crd::DriftDetail> = if result.has_changes() && drift_plan_path.exists() {
        let show = state.executor.show_plan(&workspace.path, &drift_plan_path).await?;
        if show.success {
            match Plan::from_json(&show.stdout) {
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
                    warn!(error = %e, "Failed to parse drift plan JSON");
                    Vec::new()
                }
            }
        } else {
            Vec::new()
        }
    } else {
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

    update_settling_status(template, &outcome, &drift_details, state).await?;

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
async fn handle_drifted(
    template: &InfrastructureTemplate,
    state: &ControllerState,
) -> Result<ReconcileAction> {
    warn!("Template has drift detected");
    state.metrics.drift_detected_total.inc();

    if template.spec.auto_approve {
        // Auto-correction: transition back through plan → apply cycle
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
async fn handle_failed(
    template: &InfrastructureTemplate,
    state: &ControllerState,
) -> Result<ReconcileAction> {
    let failure_count = template.retry_count();
    warn!(failure_count, "Template in Failed phase");

    if template.retries_exhausted() {
        warn!("Retries exhausted, not requeuing");
        return Ok(ReconcileAction::Requeue(Duration::from_secs(3600)));
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

    // Clean workspace and restart from Pending on retry
    info!("Cleaning workspace and retrying from Pending");
    let workspace = state.workspace_manager.get_workspace(template).await?;
    workspace.clean().await?;
    update_phase(template, Phase::Pending, state).await?;
    record_event(template, state, EventType::Normal, "Retry", &format!("Retrying after failure (attempt {})", failure_count)).await;

    Ok(ReconcileAction::Requeue(backoff))
}

/// Handle Destroying phase - run `tofu destroy` and clean up.
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

    let workspace = state.workspace_manager.get_workspace(template).await?;

    // Only run destroy if workspace has been initialized
    if workspace.file_exists(".terraform") {
        let result = state.executor.destroy(&workspace.path, true).await?;

        if !result.success {
            let err_msg = format!("tofu destroy failed: {}", result.stderr);
            warn!(%err_msg);
            update_phase_with_error(template, Phase::Failed, &err_msg, state).await?;
            record_event(template, state, EventType::Warning, "DestroyFailed", &err_msg).await;
            return Ok(ReconcileAction::Requeue(ERROR_REQUEUE_INTERVAL));
        }

        info!("tofu destroy completed successfully");
        record_event(template, state, EventType::Normal, "Destroyed", "Infrastructure destroyed successfully").await;
    }

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

// ---------------------------------------------------------------------------
// Status update helpers
// ---------------------------------------------------------------------------

/// Update the phase in the template status.
async fn update_phase(
    template: &InfrastructureTemplate,
    phase: Phase,
    state: &ControllerState,
) -> Result<()> {
    let name = template.name_any();
    let namespace = template.namespace().unwrap_or_default();

    let api: Api<InfrastructureTemplate> = Api::namespaced(state.client.clone(), &namespace);

    let mut status = template.status.clone().unwrap_or_default();
    status.phase = Some(phase);
    status.observed_generation = template.metadata.generation.unwrap_or(0);
    // Always set conditions so FluxCD healthChecks see current state
    status.conditions = conditions_for_phase(phase, None);

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

/// Update phase to Failed with an error message.
async fn update_phase_with_error(
    template: &InfrastructureTemplate,
    phase: Phase,
    error_msg: &str,
    state: &ControllerState,
) -> Result<()> {
    let name = template.name_any();
    let namespace = template.namespace().unwrap_or_default();

    let api: Api<InfrastructureTemplate> = Api::namespaced(state.client.clone(), &namespace);

    let mut status = template.status.clone().unwrap_or_default();
    status.phase = Some(phase);
    status.observed_generation = template.metadata.generation.unwrap_or(0);
    status.last_error = Some(error_msg.to_string());
    status.failure_count = status.failure_count.saturating_add(1);
    status.conditions = conditions_for_phase(phase, Some(error_msg));

    let patch = serde_json::json!({ "status": status });

    api.patch_status(
        &name,
        &PatchParams::apply("pangea-operator"),
        &Patch::Merge(&patch),
    )
    .await?;

    Ok(())
}

/// Update status after a successful plan.
async fn update_plan_status(
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
async fn update_apply_status(
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
async fn update_settling_status(
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

/// Update the last drift check timestamp.
async fn update_drift_check_timestamp(
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
async fn update_pending_plan_hash(
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

/// Hash plan content for deterministic approval identification.
fn content_hash(input: &str) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    input.hash(&mut hasher);
    hasher.finish()
}

// ---------------------------------------------------------------------------
// Finalizer helpers
// ---------------------------------------------------------------------------

/// Check if the finalizer is present.
fn has_finalizer(template: &InfrastructureTemplate) -> bool {
    template
        .metadata
        .finalizers
        .as_ref()
        .map(|f| f.contains(&FINALIZER_NAME.to_string()))
        .unwrap_or(false)
}

/// Add the finalizer to the template.
async fn add_finalizer(
    template: &InfrastructureTemplate,
    state: &ControllerState,
) -> Result<()> {
    let name = template.name_any();
    let namespace = template.namespace().unwrap_or_default();
    let api: Api<InfrastructureTemplate> = Api::namespaced(state.client.clone(), &namespace);

    let patch = serde_json::json!({
        "metadata": {
            "finalizers": [FINALIZER_NAME]
        }
    });

    api.patch(
        &name,
        &PatchParams::apply("pangea-operator"),
        &Patch::Merge(&patch),
    )
    .await?;

    debug!("Finalizer added");
    Ok(())
}

/// Remove the finalizer from the template.
async fn remove_finalizer(
    template: &InfrastructureTemplate,
    state: &ControllerState,
) -> Result<()> {
    let name = template.name_any();
    let namespace = template.namespace().unwrap_or_default();
    let api: Api<InfrastructureTemplate> = Api::namespaced(state.client.clone(), &namespace);

    let finalizers: Vec<String> = template
        .metadata
        .finalizers
        .as_ref()
        .map(|f| f.iter().filter(|s| s.as_str() != FINALIZER_NAME).cloned().collect())
        .unwrap_or_default();

    let patch = serde_json::json!({
        "metadata": {
            "finalizers": finalizers
        }
    });

    api.patch(
        &name,
        &PatchParams::apply("pangea-operator"),
        &Patch::Merge(&patch),
    )
    .await?;

    info!("Finalizer removed");
    Ok(())
}

// ---------------------------------------------------------------------------
// Event recording
// ---------------------------------------------------------------------------

/// Record a Kubernetes event on the template.
async fn record_event(
    template: &InfrastructureTemplate,
    state: &ControllerState,
    event_type: EventType,
    reason: &str,
    message: &str,
) {
    let reporter = Reporter {
        controller: "pangea-operator".into(),
        instance: std::env::var("POD_NAME").ok(),
    };
    let recorder = Recorder::new(state.client.clone(), reporter);
    let obj_ref = template.object_ref(&());
    let event = Event {
        type_: event_type,
        reason: reason.into(),
        note: Some(message.into()),
        action: reason.into(),
        secondary: None,
    };
    if let Err(e) = recorder.publish(&event, &obj_ref).await {
        warn!(error = %e, "Failed to record event");
    }
}

// ---------------------------------------------------------------------------
// Provider credential resolution
// ---------------------------------------------------------------------------

/// Resolve provider credentials from Kubernetes Secrets.
async fn resolve_provider_config(
    provider_creds: &crate::crd::ProviderCredentials,
    template: &InfrastructureTemplate,
    state: &ControllerState,
) -> Result<serde_json::Value> {
    let default_ns = template.namespace().unwrap_or_else(|| "default".to_string());

    let aws_creds = if let Some(aws) = &provider_creds.aws {
        let ns = aws
            .secret_ref
            .namespace
            .as_deref()
            .unwrap_or(&default_ns);
        let secret_api: Api<Secret> = Api::namespaced(state.client.clone(), ns);
        let secret = secret_api.get(&aws.secret_ref.name).await.map_err(|_| {
            Error::SecretNotFound {
                namespace: ns.to_string(),
                name: aws.secret_ref.name.clone(),
            }
        })?;

        let data = secret.data.as_ref().ok_or_else(|| {
            Error::Config("AWS credentials secret has no data".into())
        })?;

        let access_key = data
            .get("access_key")
            .or_else(|| data.get("AWS_ACCESS_KEY_ID"))
            .map(|v| String::from_utf8_lossy(&v.0).to_string())
            .ok_or_else(|| Error::Config("access_key not found in AWS secret".into()))?;

        let secret_key = data
            .get("secret_key")
            .or_else(|| data.get("AWS_SECRET_ACCESS_KEY"))
            .map(|v| String::from_utf8_lossy(&v.0).to_string())
            .ok_or_else(|| Error::Config("secret_key not found in AWS secret".into()))?;

        let session_token = data
            .get("session_token")
            .or_else(|| data.get("AWS_SESSION_TOKEN"))
            .map(|v| String::from_utf8_lossy(&v.0).to_string());

        Some(crate::backend::AwsCredentialsConfig {
            access_key,
            secret_key,
            session_token,
        })
    } else {
        None
    };

    let cf_creds = if let Some(cf) = &provider_creds.cloudflare {
        let ns = cf
            .secret_ref
            .namespace
            .as_deref()
            .unwrap_or(&default_ns);
        let secret_api: Api<Secret> = Api::namespaced(state.client.clone(), ns);
        let secret = secret_api.get(&cf.secret_ref.name).await.map_err(|_| {
            Error::SecretNotFound {
                namespace: ns.to_string(),
                name: cf.secret_ref.name.clone(),
            }
        })?;

        let data = secret.data.as_ref().ok_or_else(|| {
            Error::Config("Cloudflare credentials secret has no data".into())
        })?;

        // Several legacy + current key names — be tolerant so the
        // operator works with secrets that follow either the
        // pangea-CLI naming convention (api_token / CLOUDFLARE_API_TOKEN)
        // or the workspace-template ENV-fetch convention
        // (CF_API_TOKEN). If none are present we skip writing a
        // backend-managed provider block — the template's inline
        // `provider :cloudflare, …` (with ENV.fetch) already covers
        // that case via the new compile-time variables injection.
        let api_token = data
            .get("api_token")
            .or_else(|| data.get("CLOUDFLARE_API_TOKEN"))
            .or_else(|| data.get("CF_API_TOKEN"))
            .map(|v| String::from_utf8_lossy(&v.0).to_string());

        api_token.map(|t| crate::backend::CloudflareCredentialsConfig { api_token: t })
    } else {
        None
    };

    let aws_region = provider_creds
        .aws
        .as_ref()
        .and_then(|a| a.region.as_deref());

    Ok(BackendConfigGenerator::generate_provider_config(
        aws_region,
        aws_creds.as_ref(),
        cf_creds.as_ref(),
    ))
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
