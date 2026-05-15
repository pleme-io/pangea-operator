//! Controller for InfrastructureTemplate resources.

use crate::backend::{BackendConfigGenerator, Credentials};
use crate::crd::{
    DriftDetail, InfrastructureTemplate, PangeaNamespace, Phase, PolicyDecision, ResourceSummary,
};
use crate::error::{Error, Result};
use crate::executor::{evaluate_policy, policy_is_configured, Plan};

use chrono::Utc;
use futures::StreamExt;
use k8s_openapi::api::core::v1::{ConfigMap, Secret};
use kube::{
    api::{Api, Patch, PatchParams},
    runtime::{
        controller::Action,
        events::EventType,
    },
    ResourceExt,
};
use std::collections::HashSet;
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
use super::template::provider_creds::resolve_provider_config;
use super::template::status::{
    update_apply_status, update_drift_check_timestamp, update_pending_plan_hash, update_phase,
    update_phase_with_error, update_plan_status, update_settling_status,
    workspace_drift_reaction_to_policy_decision,
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
        crate::controller::generation_filter::filtered_controller::<InfrastructureTemplate>(client)
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
    // Per-controller reconcile counter — completes the denominator
    // for `pangea_controller_reconciliations_total{controller="template"}`
    // so the chart 0.8.14 PangeaControllerReconcileRateHigh alert can
    // see this controller. (The standalone `reconciliations_total`
    // counter at line above predates the per-controller labeled one
    // and is kept for the existing template-specific dashboard.)
    state
        .metrics
        .record_reconcile(crate::crd::ControllerKind::Template, "ok");

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
        info!(
            "Template auto-suspended by ReactivePolicy (status.autoSuspended=true); \
             clear it manually to resume. lastEscalationReason: {:?}",
            template
                .status
                .as_ref()
                .and_then(|s| s.last_escalation_reason.as_deref())
        );
        return Ok(Action::requeue(DEFAULT_REQUEUE_INTERVAL));
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

    // Dispatch to the per-phase handler. The state machine is documented
    // at `dispatch_template_phase` below; reconcile_template stays focused
    // on the surrounding lifecycle (deletion, finalizers, gen-tracking,
    // gating) and delegates the body work to phase handlers.
    let action = dispatch_template_phase(current_phase, &template, &state).await?;

    Ok(action.into())
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
    handler.handle(template, state).await
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
                // consecutive-failure counter and escalate to Failed
                // if we hit the settling threshold. Without this,
                // templates like `pleme-io-opensource` (missing gem)
                // sit in Compiling cycleCount=0 indefinitely because
                // the cycle counter only advances after a complete
                // plan→apply, which never reaches.
                handle_compile_failure(template, state, &e.to_string()).await?;
                return Err(Error::Compilation(format!("Compile failed: {e}")));
            }
        };

        compile_result.terraform_json
    };

    // Write template content to workspace
    workspace.write_file("main.tf.json", &terraform_json).await?;
    info!("Template content written to workspace");

    // Compile succeeded — reset the failure counter so a subsequent
    // failure starts fresh. The template can recover from a
    // transient error (gem cache miss, network blip) without staying
    // forever-elevated.
    reset_compile_failure_counter(template, state).await?;

    update_phase(template, Phase::Initializing, state).await?;
    record_event(template, state, EventType::Normal, "Compiled", "Template source resolved and written to workspace").await;

    Ok(ReconcileAction::Requeue(SHORT_REQUEUE_INTERVAL))
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
        // Threshold crossed — transition to Failed + emit Event.
        warn!(
            template = %name,
            consecutive = next,
            max = max,
            "Compile failure threshold reached; transitioning to Failed"
        );
        let escalation_msg = format!(
            "Compile has failed {} consecutive times (settlingPolicy.maxConsecutiveDriftCycles={}). \
             Last error: {}. Resolve the underlying compile issue (missing gem, syntax error, \
             unresolved provider, etc.) and the next reconcile will resume.",
            next, max, err_msg
        );
        let patch = serde_json::json!({
            "status": {
                "phase": "Failed",
                "consecutiveCompileFailures": next,
                "lastError": escalation_msg.clone(),
            },
        });
        if let Err(e) =
            crate::controller::status_patch::patch_status(template, &state.client, patch).await
        {
            warn!(error = %e, "Failed to patch template status during compile-failure escalation");
        }
        record_event(
            template,
            state,
            EventType::Warning,
            "CompileFailureEscalated",
            &escalation_msg,
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

    let workspace = state.workspace_manager.get_workspace(template).await?;
    let plan_path = workspace.plan_path();

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
    let imported_addresses = run_import_prepass(
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

    let mut result = state
        .executor
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
        result = state
            .executor
            .apply(&workspace.path, None, true)
            .await?;
    }

    if result.success {
        info!(duration_secs = result.duration.as_secs_f64(), "tofu apply completed successfully");

        // Fetch outputs
        let outputs = match state.executor.output(&workspace.path).await {
            Ok(output_result) if output_result.success => {
                serde_json::from_str(&output_result.stdout).ok()
            }
            _ => None,
        };

        update_apply_status(template, outputs.clone(), state).await?;

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
        record_reconcile_cycle(
            template,
            state,
            &prior_drifts,
            prior_plan_summary,
            CycleResult::AppliedSuccess { imported_addresses: imported_addresses.clone() },
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
        bundled_natural_ids, parse_planned_attrs, resolve_natural_id, substitute_with_planned,
    };

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

    let create_addresses: Vec<&str> = prior_drifts
        .iter()
        .filter(|d| d.action == "create")
        .map(|d| d.address.as_str())
        .collect();
    if create_addresses.is_empty() {
        return Vec::new();
    }

    let variables = template.spec.variables.clone().unwrap_or_default();
    let mut imported: Vec<String> = Vec::new();
    let mut covered: HashSet<String> = HashSet::new();

    // Layer 1: per-address importHints (existing behaviour, highest
    // priority — the user explicitly named these resources).
    for (addr, id_template) in &template.spec.import_hints {
        if !create_addresses.contains(&addr.as_str()) {
            continue;
        }
        let import_id = match substitute_import_id(id_template, &variables) {
            Ok(id) => id,
            Err(missing) => {
                warn!(
                    address = %addr,
                    missing_var = %missing,
                    "import hint substitution failed; skipping"
                );
                record_event(
                    template,
                    state,
                    EventType::Warning,
                    "ImportHintSkipped",
                    &format!(
                        "Import hint for {addr} references unset variable {{{{ .{missing} }}}}; skipping"
                    ),
                )
                .await;
                continue;
            }
        };
        if try_tofu_import(template, state, workspace_path, addr, &import_id, "hint").await {
            imported.push(addr.clone());
        }
        covered.insert(addr.clone());
    }

    // Layer 2 + 3: auto-import via importPolicy.naturalIds (or
    // bundled defaults) for every create-action not already covered
    // by an explicit hint. Only fires when autoOnConflict is true.
    if auto_import {
        // Snapshot the plan once. parse_planned_attrs is best-effort —
        // an empty map just means substitution will fail per-address
        // and the address gets skipped (we fall back to the apply,
        // which then fails in a debuggable way).
        let plan_json = match state.executor.show_plan(workspace_path, plan_path).await {
            Ok(r) if r.success => r.stdout,
            _ => String::new(),
        };
        let planned_by_addr = parse_planned_attrs(&plan_json);

        let user_natural_ids = template
            .spec
            .import_policy
            .as_ref()
            .map(|p| p.natural_ids.clone())
            .unwrap_or_default();

        for addr in &create_addresses {
            if covered.contains(*addr) {
                continue;
            }
            let id_template = match resolve_natural_id(addr, &user_natural_ids) {
                Some(t) => t,
                None => {
                    debug!(
                        address = %addr,
                        "auto-import: no naturalIds rule for resource type; skipping"
                    );
                    continue;
                }
            };
            let planned_attrs = planned_by_addr
                .get(*addr)
                .cloned()
                .unwrap_or(serde_json::Value::Null);
            let import_id = match substitute_with_planned(&id_template, &planned_attrs, &variables) {
                Ok(id) => id,
                Err(missing) => {
                    // Common cause: the template references a
                    // server-assigned attribute (e.g. `planned.id`,
                    // `planned.arn`) that's null on create-action
                    // plans. The fix is per-address `spec.importHints`
                    // with the cloud-side ID, looked up out-of-band.
                    let server_assigned_hint = matches!(
                        missing.as_str(),
                        "planned.id" | "planned.arn" | "planned.self_link"
                    );
                    warn!(
                        address = %addr,
                        template = %id_template,
                        missing = %missing,
                        suggestion = if server_assigned_hint {
                            "Server-assigned attribute is null on create-action plans. \
                             Add `spec.importHints[<address>] = \"<known-cloud-id>\"` and re-reconcile."
                        } else {
                            "Required attribute not in plan. Either declare it on the workspace \
                             DSL resource block, or add a per-address `spec.importHints` entry."
                        },
                        "auto-import: substitution failed; skipping"
                    );
                    continue;
                }
            };
            if try_tofu_import(template, state, workspace_path, addr, &import_id, "auto").await {
                imported.push((*addr).to_string());
            }
        }
        // Replace bundled_natural_ids fn-pointer warning with explicit use to avoid
        // dead_code on the import. (The fn is invoked transitively via resolve_natural_id.)
        let _ = bundled_natural_ids;
    }

    imported
}

/// Try a single `tofu import`. Returns true if the import succeeded.
/// Failures are non-fatal — we log + emit a Warning event and let the
/// apply path handle the resource (where it'll fail visibly with a
/// real error message instead of a silently-skipped import).
async fn try_tofu_import(
    template: &InfrastructureTemplate,
    state: &ControllerState,
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
    match state.executor.import(workspace_path, addr, import_id).await {
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
                "tofu import failed; falling through to apply"
            );
            record_event(
                template,
                state,
                EventType::Warning,
                "ImportFailed",
                &format!("tofu import {addr} failed: {}", truncate_for_status(&r.stderr)),
            )
            .await;
            false
        }
        Err(e) => {
            warn!(address = %addr, error = %e, "tofu import errored; falling through to apply");
            false
        }
    }
}

/// Replace `{{ .name }}` (with optional whitespace) tokens in
/// `template` with string-coerced values from `variables`. Returns
/// `Err(missing_var)` on the first unresolved token so the caller
/// can surface it as a typed event.
fn substitute_import_id(
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
/// Public wrapper for `handle_failed` so trait impls in
/// `controller::template_phase` can dispatch to it. The body lives
/// here; the trait impl is the thin shim.
pub(crate) async fn handle_failed_internal(
    template: &InfrastructureTemplate,
    state: &ControllerState,
) -> Result<ReconcileAction> {
    handle_failed(template, state).await
}

#[tracing::instrument(skip_all, name = "handle_failed")]
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
