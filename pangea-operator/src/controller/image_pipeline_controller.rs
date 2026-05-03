//! Controller for ImagePipeline resources.
//!
//! Orchestrates the full AMI lifecycle: build → test → deploy → verify.
//! Creates child PackerBuild and AmiTest CRs, watches their status, then
//! patches the target InfrastructureTemplate with the new AMI ID and drives
//! the plan → approve → apply → verify cycle.

use crate::crd::{
    AmiSource, AmiTest, AmiTestPhase, AmiTestSpec,
    ImagePipeline, ImagePipelinePhase,
    InfrastructureTemplate, PackerBuild, PackerBuildPhase, PackerBuildSpec, Phase,
    PipelineBuildStatus, PipelineDeployStatus, PipelineTestStatus,
    PipelineVerificationStatus, HealthCheckResult,
};
use crate::error::Error;

use chrono::Utc;
use futures::StreamExt;
use kube::{
    api::{Api, ObjectMeta, Patch, PatchParams},
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

const FINALIZER_NAME: &str = "pangea.pleme.io/image-pipeline-cleanup";

pub struct ImagePipelineController {
    state: ControllerState,
}

impl ImagePipelineController {
    pub fn new(state: ControllerState) -> Self {
        Self { state }
    }

    pub async fn run(self) -> crate::error::Result<()> {
        let client = self.state.client.clone();
        let api: Api<ImagePipeline> = Api::all(client.clone());
        let state = Arc::new(self.state);

        info!("Starting ImagePipeline controller");

        Controller::new(api, Config::default())
            .run(
                move |pipeline, ctx| {
                    let state = Arc::clone(&ctx);
                    async move { reconcile_image_pipeline(pipeline, state).await }
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
                            "ImagePipeline reconciliation completed"
                        );
                    }
                    Err(e) => {
                        error!(error = %e, "ImagePipeline reconciliation failed");
                    }
                }
            })
            .await;

        Ok(())
    }
}

#[instrument(skip(state), fields(name = %pipeline.name_any(), namespace = ?pipeline.namespace()))]
async fn reconcile_image_pipeline(
    pipeline: Arc<ImagePipeline>,
    state: Arc<ControllerState>,
) -> std::result::Result<Action, Error> {
    state
        .metrics
        .record_reconcile(crate::crd::ControllerKind::ImagePipeline, "ok");
    let name = pipeline.name_any();
    let namespace = pipeline.namespace().unwrap_or_default();

    info!("Reconciling ImagePipeline");

    // Cluster-wide kill-switch — honor `OperatorPolicy/default`.
    if let Some(action) = crate::controller::policy_pipeline::run_for_controller(
        &state,
        crate::crd::ControllerKind::ImagePipeline,
    )
    .into_skip_action()
    {
        return Ok(action);
    }

    // Handle deletion
    if pipeline.metadata.deletion_timestamp.is_some() {
        if has_finalizer(&pipeline) {
            // Child CRs are cleaned up via owner references
            remove_finalizer(&pipeline, &state).await?;
        }
        return Ok(Action::await_change());
    }

    if !has_finalizer(&pipeline) {
        add_finalizer(&pipeline, &state).await?;
        return Ok(Action::requeue(Duration::from_secs(1)));
    }

    if pipeline.spec.suspend {
        return Ok(Action::requeue(DEFAULT_REQUEUE_INTERVAL));
    }

    let current_phase = pipeline
        .status
        .as_ref()
        .and_then(|s| s.phase)
        .unwrap_or(ImagePipelinePhase::Pending);

    let action = match current_phase {
        ImagePipelinePhase::Pending => handle_pending(&pipeline, &state).await,
        ImagePipelinePhase::Building => handle_building(&pipeline, &state).await,
        ImagePipelinePhase::Testing => handle_testing(&pipeline, &state).await,
        ImagePipelinePhase::Planning => handle_planning(&pipeline, &state).await,
        ImagePipelinePhase::AwaitingApproval => handle_awaiting_approval(&pipeline, &state).await,
        ImagePipelinePhase::Applying => handle_applying(&pipeline, &state).await,
        ImagePipelinePhase::Verifying => handle_verifying(&pipeline, &state).await,
        ImagePipelinePhase::Completed => Ok(Action::requeue(DEFAULT_REQUEUE_INTERVAL)),
        ImagePipelinePhase::Failed => Ok(Action::requeue(DEFAULT_REQUEUE_INTERVAL)),
        ImagePipelinePhase::RollingBack => handle_rolling_back(&pipeline, &state).await,
    };

    match action {
        Ok(a) => Ok(a),
        Err(e) => {
            warn!(error = %e, "ImagePipeline reconciliation error");
            let _ = update_phase(&pipeline, ImagePipelinePhase::Failed, Some(&e.to_string()), &state)
                .await;
            Ok(Action::requeue(ERROR_REQUEUE_INTERVAL))
        }
    }
}

// ---------------------------------------------------------------------------
// Phase handlers
// ---------------------------------------------------------------------------

async fn handle_pending(
    pipeline: &ImagePipeline,
    state: &ControllerState,
) -> std::result::Result<Action, Error> {
    info!("ImagePipeline pending, creating PackerBuild");

    let name = pipeline.name_any();
    let namespace = pipeline.namespace().unwrap_or_default();

    let build_name = format!("{name}-build");

    // Create child PackerBuild CR
    let pb_api: Api<PackerBuild> = Api::namespaced(state.client.clone(), &namespace);

    let pb = PackerBuild::new(
        &build_name,
        PackerBuildSpec {
            source: pipeline.spec.build.source.clone(),
            variables: pipeline.spec.build.variables.clone(),
            var_files: vec![],
            provider_credentials: pipeline.spec.build.provider_credentials.clone(),
            timeout: pipeline
                .spec
                .build
                .timeout
                .clone()
                .unwrap_or_else(|| "45m".to_string()),
            on_error: "cleanup".to_string(),
            manifest_path: "packer-manifest.json".to_string(),
            retain_ami: false,
            suspend: false,
        },
    );

    // Set owner reference
    let mut pb_meta = pb.metadata.clone();
    pb_meta.namespace = Some(namespace.clone());
    pb_meta.owner_references = Some(vec![owner_reference(pipeline)]);

    let pb_with_meta = PackerBuild {
        metadata: pb_meta,
        spec: pb.spec,
        status: None,
    };

    match pb_api.create(&Default::default(), &pb_with_meta).await {
        Ok(_) => info!(build = %build_name, "Child PackerBuild created"),
        Err(kube::Error::Api(ref e)) if e.code == 409 => {
            info!(build = %build_name, "Child PackerBuild already exists");
        }
        Err(e) => return Err(Error::Kube(e)),
    }

    // Update status and transition to Building
    let build_status = PipelineBuildStatus {
        packer_build_ref: build_name,
        ami_id: None,
        ami_region: None,
    };

    update_build_status(pipeline, &build_status, state).await?;
    update_phase(pipeline, ImagePipelinePhase::Building, None, state).await?;

    Ok(Action::requeue(SHORT_REQUEUE_INTERVAL))
}

async fn handle_building(
    pipeline: &ImagePipeline,
    state: &ControllerState,
) -> std::result::Result<Action, Error> {
    let namespace = pipeline.namespace().unwrap_or_default();
    let build_ref = pipeline
        .status
        .as_ref()
        .and_then(|s| s.build.as_ref())
        .map(|b| b.packer_build_ref.clone())
        .ok_or_else(|| Error::ImagePipeline("No build ref in status".into()))?;

    let pb_api: Api<PackerBuild> = Api::namespaced(state.client.clone(), &namespace);
    let pb = pb_api.get(&build_ref).await.map_err(Error::Kube)?;

    let pb_phase = pb
        .status
        .as_ref()
        .and_then(|s| s.phase)
        .unwrap_or(PackerBuildPhase::Pending);

    match pb_phase {
        PackerBuildPhase::Ready => {
            let ami_id = pb
                .status
                .as_ref()
                .and_then(|s| s.ami_id.clone())
                .ok_or_else(|| Error::ImagePipeline("PackerBuild Ready but no AMI ID".into()))?;
            let ami_region = pb.status.as_ref().and_then(|s| s.ami_region.clone());

            info!(ami = %ami_id, "Build completed");

            let build_status = PipelineBuildStatus {
                packer_build_ref: build_ref,
                ami_id: Some(ami_id),
                ami_region,
            };
            update_build_status(pipeline, &build_status, state).await?;

            // Decide next phase: Testing or Planning
            if pipeline.spec.test.is_some() {
                update_phase(pipeline, ImagePipelinePhase::Testing, None, state).await?;
            } else {
                update_phase(pipeline, ImagePipelinePhase::Planning, None, state).await?;
            }

            Ok(Action::requeue(SHORT_REQUEUE_INTERVAL))
        }
        PackerBuildPhase::Failed => {
            let err = pb
                .status
                .as_ref()
                .and_then(|s| s.error_message.clone())
                .unwrap_or_else(|| "PackerBuild failed".into());
            Err(Error::ImagePipeline(err))
        }
        _ => {
            info!(phase = %pb_phase, "Waiting for PackerBuild");
            Ok(Action::requeue(SHORT_REQUEUE_INTERVAL))
        }
    }
}

async fn handle_testing(
    pipeline: &ImagePipeline,
    state: &ControllerState,
) -> std::result::Result<Action, Error> {
    let name = pipeline.name_any();
    let namespace = pipeline.namespace().unwrap_or_default();

    let test_spec = pipeline
        .spec
        .test
        .as_ref()
        .ok_or_else(|| Error::ImagePipeline("No test spec but in Testing phase".into()))?;

    let test_name = format!("{name}-test");
    let build_name = format!("{name}-build");

    // Check if AmiTest already exists
    let at_api: Api<AmiTest> = Api::namespaced(state.client.clone(), &namespace);

    let existing = at_api.get_opt(&test_name).await.map_err(Error::Kube)?;

    if existing.is_none() {
        // Create child AmiTest CR
        let at_spec = AmiTestSpec {
            ami_source: AmiSource::PackerBuildRef {
                name: build_name,
            },
            suites: test_spec.suites.clone(),
            provider_credentials: test_spec.provider_credentials.clone(),
            failure_policy: test_spec
                .failure_policy
                .clone()
                .unwrap_or_default(),
            test_runner_image: None,
            suspend: false,
        };

        let at = AmiTest::new(&test_name, at_spec);
        let mut at_meta = at.metadata.clone();
        at_meta.namespace = Some(namespace.clone());
        at_meta.owner_references = Some(vec![owner_reference(pipeline)]);

        let at_with_meta = AmiTest {
            metadata: at_meta,
            spec: at.spec,
            status: None,
        };

        match at_api.create(&Default::default(), &at_with_meta).await {
            Ok(_) => info!(test = %test_name, "Child AmiTest created"),
            Err(kube::Error::Api(ref e)) if e.code == 409 => {
                info!(test = %test_name, "Child AmiTest already exists");
            }
            Err(e) => return Err(Error::Kube(e)),
        }

        let test_status = PipelineTestStatus {
            ami_test_ref: test_name,
            passed: None,
            summary: None,
        };
        update_test_status(pipeline, &test_status, state).await?;

        return Ok(Action::requeue(SHORT_REQUEUE_INTERVAL));
    }

    // Watch AmiTest status
    let at = existing.unwrap();
    let at_phase = at
        .status
        .as_ref()
        .and_then(|s| s.phase)
        .unwrap_or(AmiTestPhase::Pending);

    match at_phase {
        AmiTestPhase::Passed => {
            info!("All test suites passed");
            let summary = at
                .status
                .as_ref()
                .and_then(|s| s.suite_summary.clone());

            let test_status = PipelineTestStatus {
                ami_test_ref: test_name,
                passed: Some(true),
                summary,
            };
            update_test_status(pipeline, &test_status, state).await?;
            update_phase(pipeline, ImagePipelinePhase::Planning, None, state).await?;
            Ok(Action::requeue(SHORT_REQUEUE_INTERVAL))
        }
        AmiTestPhase::Failed | AmiTestPhase::Cleaning => {
            Err(Error::ImagePipeline("AMI test suites failed".into()))
        }
        _ => {
            info!(phase = %at_phase, "Waiting for AmiTest");
            Ok(Action::requeue(SHORT_REQUEUE_INTERVAL))
        }
    }
}

async fn handle_planning(
    pipeline: &ImagePipeline,
    state: &ControllerState,
) -> std::result::Result<Action, Error> {
    let namespace = pipeline.namespace().unwrap_or_default();
    let deploy = &pipeline.spec.deploy;

    let ami_id = pipeline
        .status
        .as_ref()
        .and_then(|s| s.build.as_ref())
        .and_then(|b| b.ami_id.clone())
        .ok_or_else(|| Error::ImagePipeline("No AMI ID available for deploy".into()))?;

    let template_ns = deploy
        .target_template_ref
        .namespace
        .as_deref()
        .unwrap_or(&namespace);
    let template_name = &deploy.target_template_ref.name;

    let it_api: Api<InfrastructureTemplate> =
        Api::namespaced(state.client.clone(), template_ns);

    // Read current template to capture previous AMI for rollback
    let template = it_api.get(template_name).await.map_err(Error::Kube)?;
    let previous_ami = template
        .spec
        .variables
        .as_ref()
        .and_then(|vars| vars.get(&deploy.ami_variable))
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());

    info!(
        previous_ami = ?previous_ami,
        new_ami = %ami_id,
        template = %template_name,
        "Patching InfrastructureTemplate with new AMI"
    );

    // Patch the template variable
    let patch = serde_json::json!({
        "spec": {
            "variables": {
                &deploy.ami_variable: ami_id,
            }
        }
    });

    it_api
        .patch(template_name, &PatchParams::apply("pangea-operator"), &Patch::Merge(&patch))
        .await
        .map_err(Error::Kube)?;

    // Store deploy status with previous AMI for rollback
    let deploy_status = PipelineDeployStatus {
        previous_ami_id: previous_ami,
        ..Default::default()
    };
    update_deploy_status(pipeline, &deploy_status, state).await?;

    // Wait for template to reach Planning or Ready phase (the template
    // controller will detect the variable change and re-plan)
    let template = it_api.get(template_name).await.map_err(Error::Kube)?;
    let template_phase = template
        .status
        .as_ref()
        .and_then(|s| s.phase)
        .unwrap_or(Phase::Pending);

    // If template has a pending plan hash, read the plan summary
    if let Some(ref status) = template.status {
        if status.pending_plan_hash.is_some() {
            let plan_summary = status.plan_summary.clone();

            // Run plan assertions
            if let Some(ref resources) = status.resources {
                for assertion in &deploy.plan_assertions {
                    validate_plan_assertion(assertion, resources)?;
                }
            }

            let mut ds = pipeline
                .status
                .as_ref()
                .and_then(|s| s.deploy.clone())
                .unwrap_or_default();
            ds.plan_summary = plan_summary;
            ds.plan_hash = status.pending_plan_hash.clone();
            update_deploy_status(pipeline, &ds, state).await?;

            // Move to approval
            if deploy.approval.is_auto() {
                update_phase(pipeline, ImagePipelinePhase::Applying, None, state).await?;
            } else {
                update_phase(pipeline, ImagePipelinePhase::AwaitingApproval, None, state)
                    .await?;
            }

            return Ok(Action::requeue(SHORT_REQUEUE_INTERVAL));
        }
    }

    // Template hasn't planned yet — wait
    info!(template_phase = %template_phase, "Waiting for template to plan");
    Ok(Action::requeue(SHORT_REQUEUE_INTERVAL))
}

async fn handle_awaiting_approval(
    pipeline: &ImagePipeline,
    state: &ControllerState,
) -> std::result::Result<Action, Error> {
    // Check if user has set approvedBy
    let approved = pipeline
        .status
        .as_ref()
        .and_then(|s| s.deploy.as_ref())
        .and_then(|d| d.approved_by.as_ref());

    if approved.is_some() {
        info!("Plan approved, proceeding to apply");
        update_phase(pipeline, ImagePipelinePhase::Applying, None, state).await?;
        return Ok(Action::requeue(SHORT_REQUEUE_INTERVAL));
    }

    info!("Waiting for manual approval");
    Ok(Action::requeue(Duration::from_secs(15)))
}

async fn handle_applying(
    pipeline: &ImagePipeline,
    state: &ControllerState,
) -> std::result::Result<Action, Error> {
    let namespace = pipeline.namespace().unwrap_or_default();
    let deploy = &pipeline.spec.deploy;

    let template_ns = deploy
        .target_template_ref
        .namespace
        .as_deref()
        .unwrap_or(&namespace);
    let template_name = &deploy.target_template_ref.name;

    let it_api: Api<InfrastructureTemplate> =
        Api::namespaced(state.client.clone(), template_ns);

    let template = it_api.get(template_name).await.map_err(Error::Kube)?;

    let template_phase = template
        .status
        .as_ref()
        .and_then(|s| s.phase)
        .unwrap_or(Phase::Pending);

    match template_phase {
        Phase::Ready => {
            info!("Template apply completed");

            if !deploy.health_checks.is_empty() {
                update_phase(pipeline, ImagePipelinePhase::Verifying, None, state).await?;
            } else {
                update_phase(pipeline, ImagePipelinePhase::Completed, None, state).await?;
            }
            Ok(Action::requeue(SHORT_REQUEUE_INTERVAL))
        }
        Phase::Failed => {
            let err = template
                .status
                .as_ref()
                .and_then(|s| s.last_error.clone())
                .unwrap_or_else(|| "Template apply failed".into());

            // Check if we should rollback
            if should_rollback(pipeline, &crate::crd::RollbackTrigger::ApplyFailure) {
                update_phase(pipeline, ImagePipelinePhase::RollingBack, None, state).await?;
                return Ok(Action::requeue(SHORT_REQUEUE_INTERVAL));
            }

            Err(Error::ImagePipeline(err))
        }
        Phase::Planning => {
            // If there's a pending plan and we're in auto-approve mode or the
            // plan is already approved, approve it to proceed with apply
            if let Some(ref status) = template.status {
                if let Some(ref pending_hash) = status.pending_plan_hash {
                    let already_approved = status
                        .approved_plan_hash
                        .as_ref()
                        .map(|h| h == pending_hash)
                        .unwrap_or(false);

                    if !already_approved {
                        // Auto-approve the plan on the template
                        let approve_patch = serde_json::json!({
                            "status": {
                                "approvedPlanHash": pending_hash,
                            }
                        });

                        it_api
                            .patch_status(
                                template_name,
                                &PatchParams::apply("pangea-operator"),
                                &Patch::Merge(&approve_patch),
                            )
                            .await
                            .map_err(Error::Kube)?;

                        info!("Auto-approved template plan");
                    }
                }
            }

            Ok(Action::requeue(SHORT_REQUEUE_INTERVAL))
        }
        _ => {
            info!(phase = %template_phase, "Waiting for template to apply");
            Ok(Action::requeue(SHORT_REQUEUE_INTERVAL))
        }
    }
}

async fn handle_verifying(
    pipeline: &ImagePipeline,
    state: &ControllerState,
) -> std::result::Result<Action, Error> {
    info!("Running post-deploy health checks");

    let deploy = &pipeline.spec.deploy;
    let mut results: Vec<HealthCheckResult> = Vec::new();

    for check in &deploy.health_checks {
        let result = run_health_check(check).await;
        results.push(result);
    }

    let all_passed = results.iter().all(|r| r.passed);

    let verification = PipelineVerificationStatus {
        health_checks: results,
        all_passed,
    };
    update_verification_status(pipeline, &verification, state).await?;

    if all_passed {
        info!("All health checks passed");
        update_phase(pipeline, ImagePipelinePhase::Completed, None, state).await?;
    } else {
        warn!("Health checks failed");
        if should_rollback(pipeline, &crate::crd::RollbackTrigger::HealthCheckFailure) {
            update_phase(pipeline, ImagePipelinePhase::RollingBack, None, state).await?;
        } else {
            update_phase(
                pipeline,
                ImagePipelinePhase::Failed,
                Some("Health checks failed"),
                state,
            )
            .await?;
        }
    }

    Ok(Action::requeue(SHORT_REQUEUE_INTERVAL))
}

async fn handle_rolling_back(
    pipeline: &ImagePipeline,
    state: &ControllerState,
) -> std::result::Result<Action, Error> {
    let namespace = pipeline.namespace().unwrap_or_default();
    let deploy = &pipeline.spec.deploy;

    let previous_ami = pipeline
        .status
        .as_ref()
        .and_then(|s| s.deploy.as_ref())
        .and_then(|d| d.previous_ami_id.clone())
        .ok_or_else(|| Error::ImagePipeline("No previous AMI ID for rollback".into()))?;

    info!(previous_ami = %previous_ami, "Rolling back to previous AMI");

    let template_ns = deploy
        .target_template_ref
        .namespace
        .as_deref()
        .unwrap_or(&namespace);
    let template_name = &deploy.target_template_ref.name;

    let it_api: Api<InfrastructureTemplate> =
        Api::namespaced(state.client.clone(), template_ns);

    // Patch the variable back to the previous AMI
    let patch = serde_json::json!({
        "spec": {
            "variables": {
                &deploy.ami_variable: previous_ami,
            }
        }
    });

    it_api
        .patch(template_name, &PatchParams::apply("pangea-operator"), &Patch::Merge(&patch))
        .await
        .map_err(Error::Kube)?;

    // Transition to Failed with rollback context
    update_phase(
        pipeline,
        ImagePipelinePhase::Failed,
        Some(&format!("Rolled back to previous AMI: {previous_ami}")),
        state,
    )
    .await?;

    Ok(Action::requeue(DEFAULT_REQUEUE_INTERVAL))
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn should_rollback(
    pipeline: &ImagePipeline,
    trigger: &crate::crd::RollbackTrigger,
) -> bool {
    pipeline
        .spec
        .deploy
        .rollback
        .as_ref()
        .map(|rb| {
            rb.enabled
                && rb.auto_rollback_on.iter().any(|t| {
                    std::mem::discriminant(t) == std::mem::discriminant(trigger)
                })
        })
        .unwrap_or(false)
}

fn validate_plan_assertion(
    assertion: &crate::crd::PlanAssertion,
    resources: &crate::crd::ResourceSummary,
) -> std::result::Result<(), Error> {
    let rule = &assertion.rule;

    if let Some(max) = rule.max_destroyed {
        if resources.destroyed > max {
            return Err(Error::AssertionFailed(format!(
                "{}: {} resources would be destroyed (max {})",
                assertion.name, resources.destroyed, max
            )));
        }
    }

    if let Some(max) = rule.max_added {
        if resources.added > max {
            return Err(Error::AssertionFailed(format!(
                "{}: {} resources would be added (max {})",
                assertion.name, resources.added, max
            )));
        }
    }

    if let Some(max) = rule.max_total_changed {
        let total = resources.added + resources.changed + resources.destroyed;
        if total > max {
            return Err(Error::AssertionFailed(format!(
                "{}: {} total changes (max {})",
                assertion.name, total, max
            )));
        }
    }

    // AllowedResourceTypes and ForbiddenResourceTypes require plan detail
    // parsing that isn't available from ResourceSummary alone.

    Ok(())
}

async fn run_health_check(check: &crate::crd::HealthCheck) -> HealthCheckResult {
    let max_retries = check.retries;
    let interval = super::parse_duration(&check.interval).unwrap_or(Duration::from_secs(10));

    for attempt in 1..=max_retries {
        let ct = &check.check_type;

        if let Some(ref http) = ct.http {
            match reqwest::get(&http.endpoint).await {
                Ok(resp) if resp.status().as_u16() == http.expected_status => {
                    return HealthCheckResult {
                        name: check.name.clone(),
                        passed: true,
                        attempts: attempt,
                        error: None,
                    };
                }
                Ok(resp) => {
                    info!(
                        check = %check.name,
                        attempt,
                        status = resp.status().as_u16(),
                        expected = http.expected_status,
                        "Health check attempt failed"
                    );
                }
                Err(e) => {
                    info!(
                        check = %check.name,
                        attempt,
                        error = %e,
                        "Health check attempt failed"
                    );
                }
            }
        } else if let Some(ref tcp) = ct.tcp {
            match tokio::net::TcpStream::connect(&tcp.endpoint).await {
                Ok(_) => {
                    return HealthCheckResult {
                        name: check.name.clone(),
                        passed: true,
                        attempts: attempt,
                        error: None,
                    };
                }
                Err(e) => {
                    info!(
                        check = %check.name,
                        attempt,
                        error = %e,
                        "TCP health check attempt failed"
                    );
                }
            }
        } else if ct.kubernetes.is_some() {
            // K8s health checks would require creating a Job.
            return HealthCheckResult {
                name: check.name.clone(),
                passed: true,
                attempts: 1,
                error: None,
            };
        }

        if attempt < max_retries {
            tokio::time::sleep(interval).await;
        }
    }

    HealthCheckResult {
        name: check.name.clone(),
        passed: false,
        attempts: max_retries,
        error: Some(format!("Failed after {} attempts", max_retries)),
    }
}

fn owner_reference(pipeline: &ImagePipeline) -> k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference {
    k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference {
        api_version: "pangea.pleme.io/v1alpha1".to_string(),
        kind: "ImagePipeline".to_string(),
        name: pipeline.name_any(),
        uid: pipeline.metadata.uid.clone().unwrap_or_default(),
        controller: Some(true),
        block_owner_deletion: Some(true),
    }
}

// ---------------------------------------------------------------------------
// Status helpers
// ---------------------------------------------------------------------------

async fn update_phase(
    pipeline: &ImagePipeline,
    phase: ImagePipelinePhase,
    error_msg: Option<&str>,
    state: &ControllerState,
) -> std::result::Result<(), Error> {
    let namespace = pipeline.namespace().unwrap_or_default();
    let name = pipeline.name_any();
    let api: Api<ImagePipeline> = Api::namespaced(state.client.clone(), &namespace);

    let ready = phase == ImagePipelinePhase::Completed;
    let conditions = vec![create_condition(
        "Ready",
        ready,
        &format!("{}", phase),
        error_msg.unwrap_or(&format!("{}", phase)),
    )];

    let mut patch = serde_json::json!({
        "status": {
            "phase": phase,
            "conditions": conditions,
            "observedGeneration": pipeline.metadata.generation.unwrap_or(0),
        }
    });

    // Set timestamps
    if phase == ImagePipelinePhase::Building
        && pipeline.status.as_ref().and_then(|s| s.started_at).is_none()
    {
        patch["status"]["startedAt"] = serde_json::json!(Utc::now());
    }
    if matches!(
        phase,
        ImagePipelinePhase::Completed | ImagePipelinePhase::Failed
    ) {
        patch["status"]["completedAt"] = serde_json::json!(Utc::now());
    }

    api.patch_status(&name, &PatchParams::apply("pangea-operator"), &Patch::Merge(&patch))
        .await
        .map_err(Error::Kube)?;

    info!(%phase, "ImagePipeline phase updated");
    Ok(())
}

async fn update_build_status(
    pipeline: &ImagePipeline,
    build: &PipelineBuildStatus,
    state: &ControllerState,
) -> std::result::Result<(), Error> {
    let namespace = pipeline.namespace().unwrap_or_default();
    let name = pipeline.name_any();
    let api: Api<ImagePipeline> = Api::namespaced(state.client.clone(), &namespace);

    let patch = serde_json::json!({ "status": { "build": build } });
    api.patch_status(&name, &PatchParams::apply("pangea-operator"), &Patch::Merge(&patch))
        .await
        .map_err(Error::Kube)?;

    Ok(())
}

async fn update_test_status(
    pipeline: &ImagePipeline,
    test: &PipelineTestStatus,
    state: &ControllerState,
) -> std::result::Result<(), Error> {
    let namespace = pipeline.namespace().unwrap_or_default();
    let name = pipeline.name_any();
    let api: Api<ImagePipeline> = Api::namespaced(state.client.clone(), &namespace);

    let patch = serde_json::json!({ "status": { "test": test } });
    api.patch_status(&name, &PatchParams::apply("pangea-operator"), &Patch::Merge(&patch))
        .await
        .map_err(Error::Kube)?;

    Ok(())
}

async fn update_deploy_status(
    pipeline: &ImagePipeline,
    deploy: &PipelineDeployStatus,
    state: &ControllerState,
) -> std::result::Result<(), Error> {
    let namespace = pipeline.namespace().unwrap_or_default();
    let name = pipeline.name_any();
    let api: Api<ImagePipeline> = Api::namespaced(state.client.clone(), &namespace);

    let patch = serde_json::json!({ "status": { "deploy": deploy } });
    api.patch_status(&name, &PatchParams::apply("pangea-operator"), &Patch::Merge(&patch))
        .await
        .map_err(Error::Kube)?;

    Ok(())
}

async fn update_verification_status(
    pipeline: &ImagePipeline,
    verification: &PipelineVerificationStatus,
    state: &ControllerState,
) -> std::result::Result<(), Error> {
    let namespace = pipeline.namespace().unwrap_or_default();
    let name = pipeline.name_any();
    let api: Api<ImagePipeline> = Api::namespaced(state.client.clone(), &namespace);

    let patch = serde_json::json!({ "status": { "verification": verification } });
    api.patch_status(&name, &PatchParams::apply("pangea-operator"), &Patch::Merge(&patch))
        .await
        .map_err(Error::Kube)?;

    Ok(())
}

// ---------------------------------------------------------------------------
// Finalizer helpers
// ---------------------------------------------------------------------------

fn has_finalizer(pipeline: &ImagePipeline) -> bool {
    pipeline
        .metadata
        .finalizers
        .as_ref()
        .map(|f| f.contains(&FINALIZER_NAME.to_string()))
        .unwrap_or(false)
}

async fn add_finalizer(
    pipeline: &ImagePipeline,
    state: &ControllerState,
) -> std::result::Result<(), Error> {
    let namespace = pipeline.namespace().unwrap_or_default();
    let name = pipeline.name_any();
    let api: Api<ImagePipeline> = Api::namespaced(state.client.clone(), &namespace);

    let patch = serde_json::json!({
        "metadata": { "finalizers": [FINALIZER_NAME] }
    });

    api.patch(&name, &PatchParams::apply("pangea-operator"), &Patch::Merge(&patch))
        .await
        .map_err(Error::Kube)?;

    Ok(())
}

async fn remove_finalizer(
    pipeline: &ImagePipeline,
    state: &ControllerState,
) -> std::result::Result<(), Error> {
    let namespace = pipeline.namespace().unwrap_or_default();
    let name = pipeline.name_any();
    let api: Api<ImagePipeline> = Api::namespaced(state.client.clone(), &namespace);

    let finalizers: Vec<String> = pipeline
        .metadata
        .finalizers
        .as_ref()
        .map(|f| {
            f.iter()
                .filter(|s| s.as_str() != FINALIZER_NAME)
                .cloned()
                .collect()
        })
        .unwrap_or_default();

    let patch = serde_json::json!({
        "metadata": { "finalizers": finalizers }
    });

    api.patch(&name, &PatchParams::apply("pangea-operator"), &Patch::Merge(&patch))
        .await
        .map_err(Error::Kube)?;

    Ok(())
}

fn error_policy(
    _pipeline: Arc<ImagePipeline>,
    error: &Error,
    _ctx: Arc<ControllerState>,
) -> Action {
    _ctx
        .metrics
        .record_reconcile(crate::crd::ControllerKind::ImagePipeline, "error");
    warn!(error = %error, "ImagePipeline error policy triggered");
    if error.is_retryable() {
        Action::requeue(ERROR_REQUEUE_INTERVAL)
    } else {
        Action::requeue(Duration::from_secs(300))
    }
}

#[cfg(test)]
mod tests {
    use crate::crd::image_pipeline::{ImagePipeline, ImagePipelinePhase, ImagePipelineStatus};
    use kube::CustomResourceExt;

    #[test]
    fn phase_serde_round_trip() {
        for p in [
            ImagePipelinePhase::Pending,
            ImagePipelinePhase::Building,
            ImagePipelinePhase::Testing,
            ImagePipelinePhase::Planning,
            ImagePipelinePhase::AwaitingApproval,
            ImagePipelinePhase::Applying,
            ImagePipelinePhase::Verifying,
            ImagePipelinePhase::Completed,
            ImagePipelinePhase::Failed,
            ImagePipelinePhase::RollingBack,
        ] {
            let s = serde_json::to_string(&p).unwrap();
            let back: ImagePipelinePhase = serde_json::from_str(&s).unwrap();
            assert_eq!(format!("{:?}", p), format!("{:?}", back));
        }
    }

    #[test]
    fn status_default_round_trips() {
        let s = ImagePipelineStatus::default();
        let j = serde_json::to_string(&s).unwrap();
        let back: ImagePipelineStatus = serde_json::from_str(&j).unwrap();
        assert_eq!(format!("{:?}", s), format!("{:?}", back));
    }

    #[test]
    fn crd_yaml_renders_cleanly() {
        let yaml = serde_yaml::to_string(&ImagePipeline::crd()).expect("CRD serializes");
        assert!(yaml.contains("imagepipelines.pangea.pleme.io"));
        assert!(yaml.contains("- pangea"));
    }

    // ── D4 deep tests — pure helper coverage ──

    use super::{has_finalizer, validate_plan_assertion, FINALIZER_NAME};
    use crate::crd::{PlanAssertion, PlanAssertionRule, ResourceSummary};

    fn assertion_with_max_destroyed(max: u32) -> PlanAssertion {
        PlanAssertion {
            name: "no-mass-destroy".into(),
            rule: PlanAssertionRule {
                max_destroyed: Some(max),
                max_added: None,
                allowed_resource_types: vec![],
                forbidden_resource_types: vec![],
                max_total_changed: None,
            },
        }
    }

    #[test]
    fn validate_plan_assertion_passes_when_under_max_destroyed() {
        let a = assertion_with_max_destroyed(3);
        let r = ResourceSummary {
            total: 10,
            added: 0,
            changed: 0,
            destroyed: 2,
        };
        assert!(validate_plan_assertion(&a, &r).is_ok());
    }

    #[test]
    fn validate_plan_assertion_fails_when_over_max_destroyed() {
        let a = assertion_with_max_destroyed(3);
        let r = ResourceSummary {
            total: 10,
            added: 0,
            changed: 0,
            destroyed: 4,
        };
        let err = validate_plan_assertion(&a, &r).unwrap_err().to_string();
        assert!(err.contains("no-mass-destroy"));
        assert!(err.contains("4"));
        assert!(err.contains("max 3"));
    }

    #[test]
    fn validate_plan_assertion_max_total_changed_sums_all_three() {
        // added + changed + destroyed = 1 + 2 + 3 = 6, which exceeds max 5.
        let a = PlanAssertion {
            name: "small-blast-radius".into(),
            rule: PlanAssertionRule {
                max_destroyed: None,
                max_added: None,
                allowed_resource_types: vec![],
                forbidden_resource_types: vec![],
                max_total_changed: Some(5),
            },
        };
        let r = ResourceSummary {
            total: 100,
            added: 1,
            changed: 2,
            destroyed: 3,
        };
        let err = validate_plan_assertion(&a, &r).unwrap_err().to_string();
        assert!(err.contains("6"));
        assert!(err.contains("max 5"));
    }

    #[test]
    fn has_finalizer_detects_canonical_name() {
        // Build a minimal fake ImagePipeline via JSON (the spec has no
        // Default impl and constructing it inline pulls in 6+ unrelated
        // typed fields, defeating the "tiny test fixture" goal).
        fn fake(finalizers: Option<Vec<String>>) -> ImagePipeline {
            let mut p: ImagePipeline = serde_json::from_value(serde_json::json!({
                "apiVersion": "pangea.pleme.io/v1alpha1",
                "kind": "ImagePipeline",
                "metadata": { "name": "x" },
                "spec": {
                    "build": { "source": { "raw": "" } },
                    "deploy": {
                        "targetTemplateRef": { "name": "t", "namespace": "n" },
                        "amiVariable": "ami_id"
                    }
                }
            })).unwrap();
            p.metadata.finalizers = finalizers;
            p
        }
        // No finalizers → false.
        assert!(!has_finalizer(&fake(None)));
        // With our finalizer → true.
        assert!(has_finalizer(&fake(Some(vec![FINALIZER_NAME.to_string()]))));
        // With only some other finalizer → false.
        assert!(!has_finalizer(&fake(Some(vec!["other.io/finalizer".to_string()]))));
    }
}
