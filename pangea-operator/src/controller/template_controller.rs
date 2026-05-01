//! Controller for InfrastructureTemplate resources.

use crate::backend::{BackendConfigGenerator, Credentials};
use crate::crd::{
    CycleSummary, DriftDetail, InfrastructureTemplate, InfrastructureTemplateStatus, Outcome,
    PangeaNamespace, Phase, PolicyDecision, PolicyEvaluation, ReconcileCycle, ResourceOutcome,
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
        // M2 — Verifying / Verified phases. Skeleton: drop straight to
        // Compiling. Once M1's ArchitectureGem registry lookup is
        // wired into the operator's runtime context, `Verifying`
        // queries the registry and only advances to `Verified` when
        // every required gem is `Loaded`. Until then, fall through.
        // See theory/PANGEA-WORKSPACE-RECONCILIATION.md M2.
        Phase::Verifying | Phase::Verified => {
            update_phase(&template, Phase::Compiling, &state).await?;
            ReconcileAction::Requeue(SHORT_REQUEUE_INTERVAL)
        }
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

        let compile_result = state
            .compiler_backend
            .compile(compile_request)
            .await
            .map_err(|e| Error::Compilation(format!("Compile failed: {e}")))?;

        compile_result.terraform_json
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
        record_reconcile_cycle(
            template,
            state,
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
                &policy_outcome.annotated_drifts,
                plan_text.clone(),
                CycleResult::PolicyGated(PolicyDecision::Refuse),
            )
            .await?;
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
                // Emit a Drifted-uncorrected receipt so the user sees
                // exactly which resources are awaiting approval. The
                // content-equality guard inside record_reconcile_cycle
                // suppresses re-patches while the plan keeps matching.
                record_reconcile_cycle(
                    template,
                    state,
                    &policy_outcome.annotated_drifts,
                    plan_text.clone(),
                    CycleResult::PolicyGated(PolicyDecision::RequireApproval),
                )
                .await?;
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
        record_reconcile_cycle(
            template,
            state,
            &prior_drifts,
            prior_plan_summary,
            CycleResult::AppliedSuccess,
        )
        .await?;
        record_event(template, state, EventType::Normal, "Applied", "Infrastructure applied successfully").await;
    } else {
        let err_msg = format!("tofu apply failed: {}", result.stderr);
        warn!(%err_msg);
        update_phase_with_error(template, Phase::Failed, &err_msg, state).await?;
        record_reconcile_cycle(
            template,
            state,
            &prior_drifts,
            prior_plan_summary,
            CycleResult::AppliedFailure(err_msg.clone()),
        )
        .await?;
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
// Reconcile cycle receipts
// ---------------------------------------------------------------------------

/// What triggered the cycle emission — drives outcome derivation when
/// translating drift_details into per-resource ResourceOutcome entries.
#[derive(Debug, Clone)]
pub(crate) enum CycleResult {
    /// Plan reported no changes — every managed resource matched
    /// declared state. Apply did not run.
    NoChanges,
    /// Apply ran successfully on every change in `drift_details`.
    /// Each entry's terraform action becomes the per-resource outcome.
    AppliedSuccess,
    /// Apply errored. Every change in `drift_details` becomes
    /// `Failed`; the apply error is attached as `message`.
    AppliedFailure(String),
    /// Policy gated this cycle (refuse / requireApproval). Apply did
    /// NOT run; every change is `Drifted` (uncorrected) with the
    /// policy decision as `message`.
    PolicyGated(PolicyDecision),
}

/// Build a typed receipt summarizing one reconcile cycle.
///
/// `drifts` is the set of resources the plan reported a change on
/// (already annotated with policy decisions when relevant).
/// `total` is the total managed-resource count; `total - drifts.len()`
/// becomes `summary.matched`. `next_cycle` is the cycle number for
/// THIS cycle (caller has already incremented `status.cycle_count`).
fn build_reconcile_cycle(
    next_cycle: u64,
    started_at: chrono::DateTime<Utc>,
    drifts: &[DriftDetail],
    total: u32,
    plan_summary: Option<String>,
    source_revision: Option<String>,
    result: CycleResult,
) -> ReconcileCycle {
    let mut summary = CycleSummary::default();

    let outcomes: Vec<ResourceOutcome> = drifts
        .iter()
        .take(100)
        .map(|d| {
            let (outcome, message) = match result {
                CycleResult::AppliedFailure(ref err) => {
                    (Outcome::Failed, Some(truncate_for_status(err)))
                }
                CycleResult::PolicyGated(decision) => (
                    Outcome::Drifted,
                    Some(format!("policy decision: {}", decision.as_str())),
                ),
                CycleResult::NoChanges => {
                    // Defensive: a NoChanges cycle should have empty
                    // drifts. If we got here, treat as Matched.
                    (Outcome::Matched, None)
                }
                CycleResult::AppliedSuccess => match outcome_for_action(&d.action) {
                    o @ Outcome::Matched => (o, None),
                    o => (o, None),
                },
            };
            match outcome {
                Outcome::Matched => summary.matched = summary.matched.saturating_add(1),
                Outcome::Updated => summary.updated = summary.updated.saturating_add(1),
                Outcome::Created => summary.created = summary.created.saturating_add(1),
                Outcome::Destroyed => summary.destroyed = summary.destroyed.saturating_add(1),
                Outcome::Imported => summary.imported = summary.imported.saturating_add(1),
                Outcome::Drifted => {
                    summary.drifted_uncorrected = summary.drifted_uncorrected.saturating_add(1)
                }
                Outcome::Failed => summary.failed = summary.failed.saturating_add(1),
            }
            ResourceOutcome {
                address: d.address.clone(),
                outcome,
                action: Some(d.action.clone()),
                message,
            }
        })
        .collect();

    // matched aggregate = (total - touched). For NoChanges cycles
    // drifts is empty so this equals `total`.
    let touched_count = drifts.len() as u32;
    let untouched = total.saturating_sub(touched_count);
    summary.matched = summary.matched.saturating_add(untouched);

    ReconcileCycle {
        cycle: next_cycle,
        started_at,
        completed_at: Utc::now(),
        source_revision,
        plan_summary,
        summary,
        outcomes,
    }
}

/// Map the terraform action vocabulary to the typed `Outcome` the
/// operator surfaces. The mapping is deliberately conservative:
/// replaces collapse to `Updated` (net effect = matches declared);
/// unknown actions land on `Updated` so we never silently lose a
/// signal.
fn outcome_for_action(action: &str) -> Outcome {
    match action {
        "no-op" | "noop" => Outcome::Matched,
        "create" => Outcome::Created,
        "update" => Outcome::Updated,
        "delete" => Outcome::Destroyed,
        "replace" => Outcome::Updated,
        "import" => Outcome::Imported,
        _ => Outcome::Updated,
    }
}

fn truncate_for_status(s: &str) -> String {
    const MAX: usize = 256;
    if s.len() <= MAX {
        s.to_string()
    } else {
        let mut t = s[..MAX].to_string();
        t.push('…');
        t
    }
}

/// Patch `status.lastCycle` + bump `status.cycleCount`. Skips the
/// patch entirely if the receipt is content-equal to the prior one
/// (only the timestamps differ) — keeps reconcile-loop chatter off
/// etcd for steady-state Ready→Ready flows.
async fn record_reconcile_cycle(
    template: &InfrastructureTemplate,
    state: &ControllerState,
    drifts: &[DriftDetail],
    plan_summary: Option<String>,
    result: CycleResult,
) -> Result<()> {
    let name = template.name_any();
    let namespace = template.namespace().unwrap_or_default();
    let api: Api<InfrastructureTemplate> = Api::namespaced(state.client.clone(), &namespace);

    let prior_status = template.status.clone().unwrap_or_default();
    let prior_cycle_count = prior_status.cycle_count;
    let next_cycle = prior_cycle_count.saturating_add(1);

    let total = prior_status
        .resources
        .as_ref()
        .map(|r| r.total)
        .unwrap_or(0);
    let started_at = prior_status
        .last_planned_at
        .unwrap_or_else(Utc::now);
    let source_revision = prior_status.last_applied_revision.clone();

    let new_cycle = build_reconcile_cycle(
        next_cycle,
        started_at,
        drifts,
        total,
        plan_summary,
        source_revision,
        result,
    );

    if let Some(prev) = prior_status.last_cycle.as_ref() {
        if cycle_content_equal(prev, &new_cycle) {
            debug!(
                template = %name,
                cycle = prior_cycle_count,
                "Reconcile cycle content unchanged; skipping status patch"
            );
            return Ok(());
        }
    }

    let mut new_status = prior_status;
    new_status.cycle_count = next_cycle;
    new_status.last_cycle = Some(new_cycle.clone());

    let patch = serde_json::json!({
        "status": {
            "cycleCount": new_status.cycle_count,
            "lastCycle": new_status.last_cycle,
        }
    });
    api.patch_status(
        &name,
        &PatchParams::apply("pangea-operator"),
        &Patch::Merge(&patch),
    )
    .await?;

    info!(
        template = %name,
        cycle = next_cycle,
        matched = new_cycle.summary.matched,
        updated = new_cycle.summary.updated,
        created = new_cycle.summary.created,
        destroyed = new_cycle.summary.destroyed,
        drifted_uncorrected = new_cycle.summary.drifted_uncorrected,
        failed = new_cycle.summary.failed,
        "ReconcileCycle recorded"
    );
    Ok(())
}

/// Two cycles are content-equal when summary, source_revision,
/// plan_summary, and outcomes match. Cycle number and timestamps are
/// deliberately ignored — they always differ between successive
/// reconciles, and skipping the patch when nothing else changed is
/// the whole point (no etcd churn for Matched-only steady state).
fn cycle_content_equal(a: &ReconcileCycle, b: &ReconcileCycle) -> bool {
    if a.summary.matched != b.summary.matched
        || a.summary.updated != b.summary.updated
        || a.summary.created != b.summary.created
        || a.summary.destroyed != b.summary.destroyed
        || a.summary.imported != b.summary.imported
        || a.summary.drifted_uncorrected != b.summary.drifted_uncorrected
        || a.summary.failed != b.summary.failed
    {
        return false;
    }
    if a.source_revision != b.source_revision || a.plan_summary != b.plan_summary {
        return false;
    }
    if a.outcomes.len() != b.outcomes.len() {
        return false;
    }
    for (ao, bo) in a.outcomes.iter().zip(b.outcomes.iter()) {
        if ao.address != bo.address
            || ao.outcome != bo.outcome
            || ao.action != bo.action
            || ao.message != bo.message
        {
            return false;
        }
    }
    true
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

#[cfg(test)]
mod cycle_tests {
    use super::*;

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
            CycleResult::AppliedSuccess,
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
            CycleResult::AppliedSuccess,
        );
        assert_eq!(cycle.outcomes.len(), 100, "outcomes capped at 100");
        // Summary still counts the FULL touched-set in matched math:
        // 500 total - 200 touched (all update) = 300 matched.
        // Per-Outcome counts only reflect what we iterated (capped at 100).
        // So updated count = 100 (top of the cap).
        assert_eq!(cycle.summary.updated, 100);
        assert_eq!(cycle.summary.matched, 300);
    }
}
