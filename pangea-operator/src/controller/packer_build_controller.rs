//! Controller for PackerBuild resources.
//!
//! Manages the lifecycle of Packer builds: compile Ruby DSL → validate →
//! build → extract AMI ID from manifest.

use crate::crd::{PackerBuild, PackerBuildPhase};
use crate::error::Error;
use crate::executor::packer;

use chrono::Utc;
use futures::StreamExt;
use k8s_openapi::api::core::v1::ConfigMap;
use kube::{api::Api, runtime::controller::Action, ResourceExt};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, error, info, instrument, warn};

use super::{
    create_condition, ControllerState, DEFAULT_REQUEUE_INTERVAL, ERROR_REQUEUE_INTERVAL,
    SHORT_REQUEUE_INTERVAL,
};

const FINALIZER_NAME: &str = "pangea.pleme.io/packer-cleanup";

/// Controller for PackerBuild resources.
pub struct PackerBuildController {
    state: ControllerState,
}

impl PackerBuildController {
    pub fn new(state: ControllerState) -> Self {
        Self { state }
    }

    pub async fn run(self) -> crate::error::Result<()> {
        let client = self.state.client.clone();
        let state = Arc::new(self.state);

        info!("Starting PackerBuild controller");

        crate::controller::generation_filter::filtered_controller::<PackerBuild>(client)
            .run(
                move |build, ctx| {
                    let state = Arc::clone(&ctx);
                    async move { reconcile_packer_build(build, state).await }
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
                            "PackerBuild reconciliation completed"
                        );
                    }
                    Err(e) => {
                        error!(error = %e, "PackerBuild reconciliation failed");
                    }
                }
            })
            .await;

        Ok(())
    }
}

// `skip_all`, never an enumerated `skip(...)`. `#[instrument]` Debug-records
// every argument it is not told to skip, and the JSON formatter stamps the
// resulting span fields onto EVERY event inside the span. A large CR (a
// workspace carrying hundreds of DriftDetails) turned each ordinary INFO line
// into ~49KB; tracing's stdout writer is a synchronous mutex, so the reconcile
// path blocked tokio workers on log I/O until the liveness handler could not
// be scheduled inside the kubelet's 5s timeout and the pod was restarted
// mid-cycle, discarding ~22 minutes of plan+apply. See tests/
// instrument_never_dumps_a_cr.rs for the full account.
//
// An enumerated skip-list is not enough: it is a snapshot of today's
// arguments, so the next parameter added here silently reintroduces the bug.
// `skip_all` is total — nothing is ever auto-recorded — and the explicit
// `fields(...)` below carry the only identity anyone wanted.
#[instrument(skip_all, fields(name = %build.name_any(), namespace = ?build.namespace()))]
async fn reconcile_packer_build(
    build: Arc<PackerBuild>,
    state: Arc<ControllerState>,
) -> std::result::Result<Action, Error> {
    state
        .metrics
        .record_reconcile(crate::crd::ControllerKind::PackerBuild, "ok");
    let name = build.name_any();
    let namespace = build.namespace().unwrap_or_default();

    info!("Reconciling PackerBuild");

    // Cluster-wide kill-switch — honor `OperatorPolicy/default`.
    if let Some(action) = crate::controller::policy_pipeline::run_for_controller(
        &state,
        crate::crd::ControllerKind::PackerBuild,
    )
    .into_skip_action()
    {
        return Ok(action);
    }

    // Handle deletion
    if build.metadata.deletion_timestamp.is_some() {
        if has_finalizer(&build) {
            // Clean up workspace
            let workspace = state
                .workspace_manager
                .get_or_create(&namespace, &format!("packer-{name}"))
                .await?;
            let _ = workspace.clean().await;
            remove_finalizer(&build, &state).await?;
        }
        return Ok(Action::await_change());
    }

    // Ensure finalizer
    if !has_finalizer(&build) {
        add_finalizer(&build, &state).await?;
        return Ok(Action::requeue(Duration::from_secs(1)));
    }

    // Handle suspension
    if build.spec.suspend {
        info!("PackerBuild is suspended");
        return Ok(Action::requeue(DEFAULT_REQUEUE_INTERVAL));
    }

    // Detect spec changes
    let current_gen = build.metadata.generation.unwrap_or(0);
    let observed_gen = build
        .status
        .as_ref()
        .map(|s| s.observed_generation)
        .unwrap_or(0);

    if current_gen != observed_gen && observed_gen > 0 {
        info!(
            current_gen,
            observed_gen, "Spec changed, restarting from Pending"
        );
        update_phase(&build, PackerBuildPhase::Pending, None, &state).await?;
        return Ok(Action::requeue(SHORT_REQUEUE_INTERVAL));
    }

    // Phase dispatch
    let current_phase = build
        .status
        .as_ref()
        .and_then(|s| s.phase)
        .unwrap_or(PackerBuildPhase::Pending);

    let action = match current_phase {
        PackerBuildPhase::Pending => handle_pending(&build, &state).await,
        PackerBuildPhase::Compiling => handle_compiling(&build, &state).await,
        PackerBuildPhase::Validating => handle_validating(&build, &state).await,
        PackerBuildPhase::Building => handle_building(&build, &state).await,
        PackerBuildPhase::Extracting => handle_extracting(&build, &state).await,
        PackerBuildPhase::Ready => Ok(Action::requeue(DEFAULT_REQUEUE_INTERVAL)),
        PackerBuildPhase::Failed => {
            let retry_count = build.status.as_ref().map(|s| s.retry_count).unwrap_or(0);
            let backoff = super::exponential_backoff(retry_count as u32, 60, 600);
            Ok(Action::requeue(backoff))
        }
    };

    match action {
        Ok(a) => Ok(a),
        Err(e) => {
            warn!(error = %e, "PackerBuild reconciliation error");
            let err_msg = e.to_string();
            let _ =
                update_phase_with_error(&build, PackerBuildPhase::Failed, &err_msg, &state).await;
            Ok(Action::requeue(ERROR_REQUEUE_INTERVAL))
        }
    }
}

// ---------------------------------------------------------------------------
// Phase handlers
// ---------------------------------------------------------------------------

async fn handle_pending(
    build: &PackerBuild,
    state: &ControllerState,
) -> std::result::Result<Action, Error> {
    info!("PackerBuild pending, validating source");

    // Validate that at least one source is specified
    let source = &build.spec.source;
    if source.inline.is_none() && source.config_map_ref.is_none() && source.git_repository.is_none()
    {
        return Err(Error::InvalidSource(
            "PackerBuild source must specify inline, configMapRef, or gitRepository".into(),
        ));
    }

    update_phase(build, PackerBuildPhase::Compiling, None, state).await?;
    Ok(Action::requeue(SHORT_REQUEUE_INTERVAL))
}

async fn handle_compiling(
    build: &PackerBuild,
    state: &ControllerState,
) -> std::result::Result<Action, Error> {
    info!("Compiling Packer template");

    let name = build.name_any();
    let namespace = build.namespace().unwrap_or_default();

    // Get or create workspace
    let workspace = state
        .workspace_manager
        .get_or_create(&namespace, &format!("packer-{name}"))
        .await?;

    // Resolve source content
    let content = resolve_source(build, state).await?;

    // Detect if content is already JSON or needs compilation
    let packer_json =
        if content.trim_start().starts_with('{') || content.trim_start().starts_with('[') {
            // Already compiled JSON — use directly
            info!("Source is pre-compiled Packer JSON");
            content
        } else {
            // Ruby DSL — dispatch via the CompilerBackend trait.
            // /compile-packer collapses to /compile-any with format=packer.
            info!("Compiling Ruby DSL via compiler backend");
            let variables: HashMap<String, serde_json::Value> =
                build.spec.variables.clone().into_iter().collect();

            let compile_result = state
                .compiler_backend
                .compile_any(crate::ruby::CompileAnyRequest {
                    source: content,
                    variables,
                    format: Some("packer".to_string()),
                    format_definition: None,
                })
                .await
                .map_err(|e| Error::Compilation(format!("Compile failed: {e}")))?;

            compile_result.output_json
        };

    // Ensure manifest post-processor is present
    let packer_json = ensure_manifest_post_processor(&packer_json, &build.spec.manifest_path)?;

    // Write to workspace
    workspace
        .write_file("template.pkr.json", &packer_json)
        .await?;

    update_phase(build, PackerBuildPhase::Validating, None, state).await?;
    Ok(Action::requeue(SHORT_REQUEUE_INTERVAL))
}

async fn handle_validating(
    build: &PackerBuild,
    state: &ControllerState,
) -> std::result::Result<Action, Error> {
    info!("Validating Packer template");

    let name = build.name_any();
    let namespace = build.namespace().unwrap_or_default();
    let workspace = state
        .workspace_manager
        .get_or_create(&namespace, &format!("packer-{name}"))
        .await?;

    // Init first (download required plugins)
    let init_result = state.packer_executor.init(&workspace.path, &[]).await?;

    if !init_result.success {
        return Err(Error::PackerExecution(format!(
            "packer init failed: {}",
            init_result.stderr
        )));
    }

    // Validate
    let validate_result = state.packer_executor.validate(&workspace.path).await?;

    if !validate_result.success {
        return Err(Error::PackerExecution(format!(
            "packer validate failed: {}",
            validate_result.stderr
        )));
    }

    update_phase(build, PackerBuildPhase::Building, None, state).await?;
    Ok(Action::requeue(SHORT_REQUEUE_INTERVAL))
}

async fn handle_building(
    build: &PackerBuild,
    state: &ControllerState,
) -> std::result::Result<Action, Error> {
    info!("Running Packer build");

    let name = build.name_any();
    let namespace = build.namespace().unwrap_or_default();
    let workspace = state
        .workspace_manager
        .get_or_create(&namespace, &format!("packer-{name}"))
        .await?;

    // Record build start time
    update_started_at(build, state).await?;

    // Prepare variables
    let vars: HashMap<String, String> = build
        .spec
        .variables
        .iter()
        .map(|(k, v)| {
            let val = match v {
                serde_json::Value::String(s) => s.clone(),
                other => other.to_string(),
            };
            (k.clone(), val)
        })
        .collect();

    // Run packer build
    let build_result = state
        .packer_executor
        .build(&workspace.path, &vars, &[], &build.spec.on_error)
        .await?;

    if !build_result.success {
        return Err(Error::PackerExecution(format!(
            "packer build failed (exit {}): {}",
            build_result.exit_code, build_result.stderr
        )));
    }

    info!(
        duration_secs = build_result.duration.as_secs_f64(),
        "Packer build completed"
    );

    update_phase(build, PackerBuildPhase::Extracting, None, state).await?;
    Ok(Action::requeue(SHORT_REQUEUE_INTERVAL))
}

async fn handle_extracting(
    build: &PackerBuild,
    state: &ControllerState,
) -> std::result::Result<Action, Error> {
    info!("Extracting AMI ID from packer manifest");

    let name = build.name_any();
    let namespace = build.namespace().unwrap_or_default();
    let workspace = state
        .workspace_manager
        .get_or_create(&namespace, &format!("packer-{name}"))
        .await?;

    let manifest_path = workspace.path.join(&build.spec.manifest_path);
    let ami_id = packer::parse_packer_manifest(&manifest_path)?;
    let ami_region = packer::parse_packer_manifest_region(&manifest_path)?;

    // Read raw manifest for status
    let raw_manifest = tokio::fs::read_to_string(&manifest_path).await.ok();

    info!(ami = %ami_id, region = ?ami_region, "AMI extracted from manifest");

    // Update status with AMI ID and transition to Ready
    update_status_with_ami(
        build,
        &ami_id,
        ami_region.as_deref(),
        raw_manifest.as_deref(),
        state,
    )
    .await?;

    Ok(Action::requeue(DEFAULT_REQUEUE_INTERVAL))
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Resolve the template source content from inline, ConfigMap, or Git.
async fn resolve_source(
    build: &PackerBuild,
    state: &ControllerState,
) -> std::result::Result<String, Error> {
    let source = &build.spec.source;
    let namespace = build.namespace().unwrap_or_default();

    if let Some(ref inline) = source.inline {
        return Ok(inline.clone());
    }

    if let Some(ref cm_ref) = source.config_map_ref {
        let cm_ns = cm_ref.namespace.as_deref().unwrap_or(&namespace);
        let api: Api<ConfigMap> = Api::namespaced(state.client.clone(), cm_ns);
        let cm = api.get(&cm_ref.name).await.map_err(Error::Kube)?;

        let data = cm
            .data
            .as_ref()
            .and_then(|d| d.get(&cm_ref.key))
            .ok_or_else(|| {
                Error::InvalidSource(format!(
                    "ConfigMap {}/{} key {} not found",
                    cm_ns, cm_ref.name, cm_ref.key
                ))
            })?;

        return Ok(data.clone());
    }

    if let Some(ref _git_ref) = source.git_repository {
        // Git clone is a TODO — for now, return an error directing users to
        // use inline or ConfigMap source
        return Err(Error::InvalidSource(
            "Git repository source for PackerBuild is not yet implemented. \
             Use inline or configMapRef."
                .into(),
        ));
    }

    Err(Error::InvalidSource(
        "No source specified in PackerBuild".into(),
    ))
}

/// Ensure the compiled Packer JSON includes a manifest post-processor.
fn ensure_manifest_post_processor(
    json_str: &str,
    manifest_path: &str,
) -> std::result::Result<String, Error> {
    let mut json: serde_json::Value = serde_json::from_str(json_str)
        .map_err(|e| Error::Compilation(format!("Invalid Packer JSON: {}", e)))?;

    // Check if post-processors array has a manifest entry
    let has_manifest = json
        .get("post-processors")
        .and_then(|pp| pp.as_array())
        .map(|arr| {
            arr.iter().any(|p| {
                p.get("type")
                    .and_then(|t| t.as_str())
                    .map(|t| t == "manifest")
                    .unwrap_or(false)
            })
        })
        .unwrap_or(false);

    if !has_manifest {
        let manifest_pp = serde_json::json!({
            "type": "manifest",
            "output": manifest_path,
            "strip_path": true,
        });

        let pp = json
            .as_object_mut()
            .ok_or_else(|| Error::Compilation("Packer JSON is not an object".into()))?
            .entry("post-processors")
            .or_insert_with(|| serde_json::json!([]));

        if let Some(arr) = pp.as_array_mut() {
            arr.push(manifest_pp);
        }
    }

    serde_json::to_string_pretty(&json).map_err(|e| Error::Compilation(e.to_string()))
}

// ---------------------------------------------------------------------------
// Status update helpers
// ---------------------------------------------------------------------------

async fn update_phase(
    build: &PackerBuild,
    phase: PackerBuildPhase,
    error_msg: Option<&str>,
    state: &ControllerState,
) -> std::result::Result<(), Error> {
    let ready = phase == PackerBuildPhase::Ready;
    let reconciling = matches!(
        phase,
        PackerBuildPhase::Compiling
            | PackerBuildPhase::Validating
            | PackerBuildPhase::Building
            | PackerBuildPhase::Extracting
    );

    let conditions = vec![
        create_condition(
            "Ready",
            ready,
            &format!("{}", phase),
            error_msg.unwrap_or(&format!("{}", phase)),
        ),
        create_condition(
            "Reconciling",
            reconciling,
            &format!("{}", phase),
            &format!("{}", phase),
        ),
    ];

    let patch = serde_json::json!({
        "status": {
            "phase": phase,
            "conditions": conditions,
            "observedGeneration": build.metadata.generation.unwrap_or(0),
        }
    });

    crate::controller::status_patch::patch_status(build, &state.client, patch)
        .await
        .map_err(Error::Kube)?;

    // Emit pangea_packer_builds_by_phase{phase=…} so the
    // PackerBuildFailed alert in the chart actually has data to
    // alert on (was silently inert pre-U3).
    state.metrics.set_packer_build_phase(
        &build.namespace().unwrap_or_default(),
        &build.name_any(),
        &phase,
    );

    info!(%phase, "PackerBuild phase updated");
    Ok(())
}

async fn update_phase_with_error(
    build: &PackerBuild,
    phase: PackerBuildPhase,
    error_msg: &str,
    state: &ControllerState,
) -> std::result::Result<(), Error> {
    let retry_count = build.status.as_ref().map(|s| s.retry_count).unwrap_or(0) + 1;

    let conditions = vec![
        create_condition("Ready", false, "Failed", error_msg),
        create_condition("Reconciling", false, "Failed", error_msg),
    ];

    let patch = serde_json::json!({
        "status": {
            "phase": phase,
            "conditions": conditions,
            "errorMessage": error_msg,
            "retryCount": retry_count,
            "observedGeneration": build.metadata.generation.unwrap_or(0),
        }
    });

    crate::controller::status_patch::patch_status(build, &state.client, patch)
        .await
        .map_err(Error::Kube)?;

    Ok(())
}

async fn update_started_at(
    build: &PackerBuild,
    state: &ControllerState,
) -> std::result::Result<(), Error> {
    let patch = serde_json::json!({
        "status": {
            "startedAt": Utc::now(),
        }
    });

    crate::controller::status_patch::patch_status(build, &state.client, patch)
        .await
        .map_err(Error::Kube)?;

    Ok(())
}

async fn update_status_with_ami(
    build: &PackerBuild,
    ami_id: &str,
    ami_region: Option<&str>,
    raw_manifest: Option<&str>,
    state: &ControllerState,
) -> std::result::Result<(), Error> {
    let phase = PackerBuildPhase::Ready;
    let conditions = vec![
        create_condition("Ready", true, "BuildComplete", &format!("AMI: {ami_id}")),
        create_condition("Reconciling", false, "BuildComplete", "Build finished"),
    ];

    let patch = serde_json::json!({
        "status": {
            "phase": phase,
            "amiId": ami_id,
            "amiRegion": ami_region,
            "packerManifest": raw_manifest,
            "completedAt": Utc::now(),
            "conditions": conditions,
            "observedGeneration": build.metadata.generation.unwrap_or(0),
            "errorMessage": null,
            "retryCount": 0,
        }
    });

    crate::controller::status_patch::patch_status(build, &state.client, patch)
        .await
        .map_err(Error::Kube)?;

    info!(ami = %ami_id, "PackerBuild completed successfully");
    Ok(())
}

// Finalizer helpers — generic plumbing lives in
// `crate::controller::finalizer`. Wrappers preserve the pre-T5 names.

fn has_finalizer(build: &PackerBuild) -> bool {
    crate::controller::finalizer::has(build, FINALIZER_NAME)
}

async fn add_finalizer(
    build: &PackerBuild,
    state: &ControllerState,
) -> std::result::Result<(), Error> {
    crate::controller::finalizer::add(build, FINALIZER_NAME, &state.client)
        .await
        .map_err(Error::Kube)
}

async fn remove_finalizer(
    build: &PackerBuild,
    state: &ControllerState,
) -> std::result::Result<(), Error> {
    crate::controller::finalizer::remove(build, FINALIZER_NAME, &state.client)
        .await
        .map_err(Error::Kube)
}

// ---------------------------------------------------------------------------
// Error policy
// ---------------------------------------------------------------------------

fn error_policy(_build: Arc<PackerBuild>, error: &Error, ctx: Arc<ControllerState>) -> Action {
    use crate::controller::error_policy::{run_error_policy, tiered_backoff};
    run_error_policy(
        &ctx.metrics,
        crate::crd::ControllerKind::PackerBuild,
        error,
        tiered_backoff(error.is_retryable()),
    )
}

#[cfg(test)]
mod tests {
    use crate::crd::{PackerBuild, PackerBuildPhase, PackerBuildStatus};
    use kube::CustomResourceExt;

    #[test]
    fn phase_serde_round_trip() {
        for p in [
            PackerBuildPhase::Pending,
            PackerBuildPhase::Compiling,
            PackerBuildPhase::Validating,
            PackerBuildPhase::Building,
            PackerBuildPhase::Extracting,
            PackerBuildPhase::Ready,
            PackerBuildPhase::Failed,
        ] {
            let s = serde_json::to_string(&p).unwrap();
            let back: PackerBuildPhase = serde_json::from_str(&s).unwrap();
            assert_eq!(format!("{:?}", p), format!("{:?}", back));
        }
    }

    #[test]
    fn status_default_round_trips() {
        let s = PackerBuildStatus::default();
        let j = serde_json::to_string(&s).unwrap();
        let back: PackerBuildStatus = serde_json::from_str(&j).unwrap();
        assert_eq!(format!("{:?}", s), format!("{:?}", back));
    }

    #[test]
    fn crd_yaml_renders_cleanly() {
        let yaml = serde_yaml::to_string(&PackerBuild::crd()).expect("CRD serializes");
        assert!(yaml.contains("packerbuilds.pangea.pleme.io"));
        assert!(yaml.contains("- pangea"));
    }

    // ── D4 deep tests — pure helper coverage ──

    use super::{has_finalizer, FINALIZER_NAME};

    fn fake_build(finalizers: Option<Vec<String>>) -> PackerBuild {
        let mut b: PackerBuild = serde_json::from_value(serde_json::json!({
            "apiVersion": "pangea.pleme.io/v1alpha1",
            "kind": "PackerBuild",
            "metadata": { "name": "x" },
            "spec": { "source": { "raw": "" } }
        }))
        .unwrap();
        b.metadata.finalizers = finalizers;
        b
    }

    #[test]
    fn has_finalizer_detects_canonical_name() {
        assert!(!has_finalizer(&fake_build(None)));
        assert!(has_finalizer(&fake_build(Some(vec![
            FINALIZER_NAME.to_string()
        ]))));
        assert!(!has_finalizer(&fake_build(Some(vec![
            "unrelated/finalizer".to_string()
        ]))));
    }

    #[test]
    fn has_finalizer_handles_finalizer_among_several() {
        // Mixed list — ours is present, returns true.
        let mixed = vec![
            "other.io/cleanup".to_string(),
            FINALIZER_NAME.to_string(),
            "yet-another/finalizer".to_string(),
        ];
        assert!(has_finalizer(&fake_build(Some(mixed))));
        // Mixed without ours → false.
        let only_others = vec![
            "other.io/cleanup".to_string(),
            "yet-another/finalizer".to_string(),
        ];
        assert!(!has_finalizer(&fake_build(Some(only_others))));
    }
}
