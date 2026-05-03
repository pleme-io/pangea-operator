//! Controller for AmiTest resources.
//!
//! Validates AMIs through tiered test suites executed as K8s Jobs.
//! Suites form a DAG — the controller resolves dependencies and launches
//! suites whose dependencies have all succeeded.

use crate::crd::{
    AmiSource, AmiTest, AmiTestPhase, PackerBuild, PackerBuildPhase, SuitePhase, SuiteResult,
};
use crate::error::Error;

use chrono::Utc;
use futures::StreamExt;
use k8s_openapi::api::batch::v1::{Job, JobSpec, JobStatus};
use k8s_openapi::api::core::v1::{Container, EnvVar, PodSpec, PodTemplateSpec};
use kube::{
    api::{Api, ListParams, ObjectMeta, Patch, PatchParams},
    runtime::{
        controller::{Action, Controller},
        watcher::Config,
    },
    Resource, ResourceExt,
};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, error, info, instrument, warn};

use super::{
    create_condition, ControllerState, DEFAULT_REQUEUE_INTERVAL, ERROR_REQUEUE_INTERVAL,
    SHORT_REQUEUE_INTERVAL,
};

const FINALIZER_NAME: &str = "pangea.pleme.io/ami-test-cleanup";

pub struct AmiTestController {
    state: ControllerState,
}

impl AmiTestController {
    pub fn new(state: ControllerState) -> Self {
        Self { state }
    }

    pub async fn run(self) -> crate::error::Result<()> {
        let client = self.state.client.clone();
        let api: Api<AmiTest> = Api::all(client.clone());
        let state = Arc::new(self.state);

        info!("Starting AmiTest controller");

        Controller::new(api, Config::default())
            .run(
                move |test, ctx| {
                    let state = Arc::clone(&ctx);
                    async move { reconcile_ami_test(test, state).await }
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
                            "AmiTest reconciliation completed"
                        );
                    }
                    Err(e) => {
                        error!(error = %e, "AmiTest reconciliation failed");
                    }
                }
            })
            .await;

        Ok(())
    }
}

#[instrument(skip(state), fields(name = %test.name_any(), namespace = ?test.namespace()))]
async fn reconcile_ami_test(
    test: Arc<AmiTest>,
    state: Arc<ControllerState>,
) -> std::result::Result<Action, Error> {
    state
        .metrics
        .record_reconcile(crate::crd::ControllerKind::AmiTest, "ok");
    let name = test.name_any();
    let namespace = test.namespace().unwrap_or_default();

    info!("Reconciling AmiTest");

    // Cluster-wide kill-switch — honor `OperatorPolicy/default`.
    if let Some(action) = crate::controller::policy_pipeline::run_for_controller(
        &state,
        crate::crd::ControllerKind::AmiTest,
    )
    .into_skip_action()
    {
        return Ok(action);
    }

    // Handle deletion
    if test.metadata.deletion_timestamp.is_some() {
        if has_finalizer(&test) {
            // Delete owned Jobs
            let job_api: Api<Job> = Api::namespaced(state.client.clone(), &namespace);
            let lp = ListParams::default().labels(&format!("pangea.pleme.io/ami-test={name}"));
            if let Ok(jobs) = job_api.list(&lp).await {
                for job in jobs.items {
                    let jn = job.metadata.name.unwrap_or_default();
                    let _ = job_api
                        .delete(&jn, &Default::default())
                        .await;
                }
            }
            remove_finalizer(&test, &state).await?;
        }
        return Ok(Action::await_change());
    }

    if !has_finalizer(&test) {
        add_finalizer(&test, &state).await?;
        return Ok(Action::requeue(Duration::from_secs(1)));
    }

    if test.spec.suspend {
        return Ok(Action::requeue(DEFAULT_REQUEUE_INTERVAL));
    }

    let current_phase = test
        .status
        .as_ref()
        .and_then(|s| s.phase)
        .unwrap_or(AmiTestPhase::Pending);

    let action = match current_phase {
        AmiTestPhase::Pending => handle_pending(&test, &state).await,
        AmiTestPhase::Resolving => handle_resolving(&test, &state).await,
        AmiTestPhase::Running => handle_running(&test, &state).await,
        AmiTestPhase::Passed => Ok(Action::requeue(DEFAULT_REQUEUE_INTERVAL)),
        AmiTestPhase::Failed => Ok(Action::requeue(DEFAULT_REQUEUE_INTERVAL)),
        AmiTestPhase::Cleaning => handle_cleaning(&test, &state).await,
    };

    match action {
        Ok(a) => Ok(a),
        Err(e) => {
            warn!(error = %e, "AmiTest reconciliation error");
            let _ = update_phase(&test, AmiTestPhase::Failed, Some(&e.to_string()), &state).await;
            Ok(Action::requeue(ERROR_REQUEUE_INTERVAL))
        }
    }
}

// ---------------------------------------------------------------------------
// Phase handlers
// ---------------------------------------------------------------------------

async fn handle_pending(
    test: &AmiTest,
    state: &ControllerState,
) -> std::result::Result<Action, Error> {
    info!("AmiTest pending, resolving AMI");

    if test.spec.suites.is_empty() {
        return Err(Error::AmiTestFailed(
            "AmiTest must have at least one suite".into(),
        ));
    }

    // Validate DAG (no cycles, no missing dependencies)
    validate_suite_dag(&test.spec.suites)?;

    update_phase(test, AmiTestPhase::Resolving, None, state).await?;
    Ok(Action::requeue(SHORT_REQUEUE_INTERVAL))
}

async fn handle_resolving(
    test: &AmiTest,
    state: &ControllerState,
) -> std::result::Result<Action, Error> {
    let namespace = test.namespace().unwrap_or_default();

    let (ami_id, ami_region) = match &test.spec.ami_source {
        AmiSource::Inline { ami_id, region } => (ami_id.clone(), Some(region.clone())),
        AmiSource::PackerBuildRef { name } => {
            let pb_api: Api<PackerBuild> = Api::namespaced(state.client.clone(), &namespace);
            let pb = pb_api.get(name).await.map_err(Error::Kube)?;

            let pb_phase = pb
                .status
                .as_ref()
                .and_then(|s| s.phase)
                .unwrap_or(PackerBuildPhase::Pending);

            if pb_phase == PackerBuildPhase::Failed {
                return Err(Error::AmiTestFailed(format!(
                    "Referenced PackerBuild '{}' is in Failed state",
                    name
                )));
            }

            if pb_phase != PackerBuildPhase::Ready {
                info!(packer_build = %name, phase = %pb_phase, "Waiting for PackerBuild to be Ready");
                return Ok(Action::requeue(SHORT_REQUEUE_INTERVAL));
            }

            let ami = pb
                .status
                .as_ref()
                .and_then(|s| s.ami_id.clone())
                .ok_or_else(|| {
                    Error::AmiTestFailed(format!(
                        "PackerBuild '{}' is Ready but has no AMI ID",
                        name
                    ))
                })?;

            let region = pb.status.as_ref().and_then(|s| s.ami_region.clone());
            (ami, region)
        }
    };

    info!(ami = %ami_id, "AMI resolved");

    // Initialize suite results
    let suites: Vec<SuiteResult> = test
        .spec
        .suites
        .iter()
        .map(|s| SuiteResult {
            name: s.name.clone(),
            phase: SuitePhase::Pending,
            job_name: None,
            duration: None,
            message: None,
            started_at: None,
            completed_at: None,
        })
        .collect();

    update_status_with_ami(test, &ami_id, ami_region.as_deref(), &suites, state).await?;
    Ok(Action::requeue(SHORT_REQUEUE_INTERVAL))
}

async fn handle_running(
    test: &AmiTest,
    state: &ControllerState,
) -> std::result::Result<Action, Error> {
    let name = test.name_any();
    let namespace = test.namespace().unwrap_or_default();

    let ami_id = test
        .status
        .as_ref()
        .and_then(|s| s.ami_id.clone())
        .ok_or_else(|| Error::AmiTestFailed("No AMI ID in status".into()))?;

    let mut suites: Vec<SuiteResult> = test
        .status
        .as_ref()
        .map(|s| s.suites.clone())
        .unwrap_or_default();

    // Build completed set
    let succeeded: HashSet<String> = suites
        .iter()
        .filter(|s| s.phase == SuitePhase::Succeeded)
        .map(|s| s.name.clone())
        .collect();

    let failed: HashSet<String> = suites
        .iter()
        .filter(|s| s.phase == SuitePhase::Failed)
        .map(|s| s.name.clone())
        .collect();

    // Check for failed suites — skip dependents
    if !failed.is_empty() {
        // Mark all suites depending on failed ones as Skipped
        for suite_result in &mut suites {
            if suite_result.phase == SuitePhase::Pending {
                let spec_suite = test
                    .spec
                    .suites
                    .iter()
                    .find(|s| s.name == suite_result.name);

                if let Some(spec) = spec_suite {
                    if spec.depends_on.iter().any(|d| failed.contains(d)) {
                        suite_result.phase = SuitePhase::Skipped;
                        suite_result.message =
                            Some("Skipped: dependency failed".into());
                    }
                }
            }
        }

        // Check if all suites are terminal
        let all_terminal = suites
            .iter()
            .all(|s| matches!(s.phase, SuitePhase::Succeeded | SuitePhase::Failed | SuitePhase::Skipped));

        if all_terminal {
            update_suites(test, &suites, state).await?;

            if test.spec.failure_policy.deregister_ami {
                update_phase(test, AmiTestPhase::Cleaning, None, state).await?;
            } else {
                update_phase(test, AmiTestPhase::Failed, Some("One or more suites failed"), state)
                    .await?;
            }
            return Ok(Action::requeue(SHORT_REQUEUE_INTERVAL));
        }
    }

    // Find ready suites (all deps succeeded, suite is Pending)
    let job_api: Api<Job> = Api::namespaced(state.client.clone(), &namespace);

    for suite_result in &mut suites {
        if suite_result.phase != SuitePhase::Pending {
            continue;
        }

        let spec_suite = test
            .spec
            .suites
            .iter()
            .find(|s| s.name == suite_result.name);
        let spec = match spec_suite {
            Some(s) => s,
            None => continue,
        };

        // Check if all dependencies succeeded
        let deps_ready = spec.depends_on.iter().all(|d| succeeded.contains(d));
        if !deps_ready {
            continue;
        }

        // Launch Job for this suite
        let job_name = format!("{}-{}", name, suite_result.name);
        info!(suite = %suite_result.name, job = %job_name, "Launching test suite Job");

        let timeout_secs = super::parse_duration(&spec.timeout)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(900);

        let job = build_test_job(
            &job_name,
            &namespace,
            &name,
            &suite_result.name,
            &ami_id,
            &spec.suite_type,
            spec.config.as_ref(),
            test.spec.test_runner_image.as_deref(),
            timeout_secs,
        );

        match job_api.create(&Default::default(), &job).await {
            Ok(_) => {
                suite_result.phase = SuitePhase::Running;
                suite_result.job_name = Some(job_name);
                suite_result.started_at = Some(Utc::now());
            }
            Err(kube::Error::Api(ref e)) if e.code == 409 => {
                // Job already exists — check its status
                suite_result.phase = SuitePhase::Running;
                suite_result.job_name = Some(job_name);
            }
            Err(e) => {
                warn!(suite = %suite_result.name, error = %e, "Failed to create test Job");
                suite_result.phase = SuitePhase::Failed;
                suite_result.message = Some(format!("Failed to create Job: {e}"));
            }
        }
    }

    // Check running suites for completion
    for suite_result in &mut suites {
        if suite_result.phase != SuitePhase::Running {
            continue;
        }

        let job_name = match &suite_result.job_name {
            Some(jn) => jn.clone(),
            None => continue,
        };

        match job_api.get(&job_name).await {
            Ok(job) => {
                if let Some(ref status) = job.status {
                    if is_job_succeeded(status) {
                        suite_result.phase = SuitePhase::Succeeded;
                        suite_result.completed_at = Some(Utc::now());
                        suite_result.message = Some("Suite passed".into());
                        info!(suite = %suite_result.name, "Test suite succeeded");
                    } else if is_job_failed(status) {
                        suite_result.phase = SuitePhase::Failed;
                        suite_result.completed_at = Some(Utc::now());
                        suite_result.message = Some("Suite failed".into());
                        warn!(suite = %suite_result.name, "Test suite failed");
                    }
                }
            }
            Err(e) => {
                warn!(job = %job_name, error = %e, "Failed to get Job status");
            }
        }
    }

    // Update suite results in status
    update_suites(test, &suites, state).await?;

    // Check if all suites are terminal
    let all_terminal = suites
        .iter()
        .all(|s| matches!(s.phase, SuitePhase::Succeeded | SuitePhase::Failed | SuitePhase::Skipped));

    if all_terminal {
        let all_passed = suites.iter().all(|s| s.phase == SuitePhase::Succeeded);
        if all_passed {
            update_phase(test, AmiTestPhase::Passed, None, state).await?;
        } else if test.spec.failure_policy.deregister_ami {
            update_phase(test, AmiTestPhase::Cleaning, None, state).await?;
        } else {
            update_phase(test, AmiTestPhase::Failed, Some("One or more suites failed"), state)
                .await?;
        }
        return Ok(Action::requeue(SHORT_REQUEUE_INTERVAL));
    }

    // Still running — requeue to check again
    Ok(Action::requeue(Duration::from_secs(15)))
}

async fn handle_cleaning(
    test: &AmiTest,
    state: &ControllerState,
) -> std::result::Result<Action, Error> {
    info!("Cleaning up failed AMI");

    // In a full implementation, this would create a Job to run `ami-forge rotate`
    // to deregister the AMI and clean up snapshots. For now, we just transition
    // to Failed with a message.

    update_phase(
        test,
        AmiTestPhase::Failed,
        Some("Suites failed; AMI cleanup requested (not yet implemented)"),
        state,
    )
    .await?;

    Ok(Action::requeue(DEFAULT_REQUEUE_INTERVAL))
}

// ---------------------------------------------------------------------------
// Job builder
// ---------------------------------------------------------------------------

fn build_test_job(
    job_name: &str,
    namespace: &str,
    test_name: &str,
    suite_name: &str,
    ami_id: &str,
    suite_type: &crate::crd::TestSuiteType,
    config: Option<&serde_json::Value>,
    runner_image: Option<&str>,
    timeout_secs: i64,
) -> Job {
    let image = runner_image.unwrap_or("ghcr.io/pleme-io/ami-forge:latest");

    let command = match suite_type {
        crate::crd::TestSuiteType::PackerTest => {
            vec!["ami-forge".to_string(), "boot-check".to_string()]
        }
        crate::crd::TestSuiteType::ClusterTest => {
            vec![
                "ami-forge".to_string(),
                "cluster-test".to_string(),
                "--ami-id".to_string(),
                ami_id.to_string(),
            ]
        }
        crate::crd::TestSuiteType::InSpec => {
            let profile = config
                .and_then(|c| c.get("profile"))
                .and_then(|p| p.as_str())
                .unwrap_or("default");
            vec![
                "inspec".to_string(),
                "exec".to_string(),
                profile.to_string(),
            ]
        }
        crate::crd::TestSuiteType::Script => {
            config
                .and_then(|c| c.get("command"))
                .and_then(|cmd| cmd.as_array())
                .map(|arr| {
                    arr.iter()
                        .filter_map(|v| v.as_str().map(String::from))
                        .collect()
                })
                .unwrap_or_else(|| vec!["echo".to_string(), "no command specified".to_string()])
        }
    };

    let env = vec![
        EnvVar {
            name: "AMI_ID".to_string(),
            value: Some(ami_id.to_string()),
            ..Default::default()
        },
        EnvVar {
            name: "SUITE_NAME".to_string(),
            value: Some(suite_name.to_string()),
            ..Default::default()
        },
    ];

    let mut labels = std::collections::BTreeMap::new();
    labels.insert(
        "pangea.pleme.io/ami-test".to_string(),
        test_name.to_string(),
    );
    labels.insert(
        "pangea.pleme.io/suite".to_string(),
        suite_name.to_string(),
    );

    Job {
        metadata: ObjectMeta {
            name: Some(job_name.to_string()),
            namespace: Some(namespace.to_string()),
            labels: Some(labels.clone()),
            ..Default::default()
        },
        spec: Some(JobSpec {
            active_deadline_seconds: Some(timeout_secs),
            backoff_limit: Some(0),
            template: PodTemplateSpec {
                metadata: Some(ObjectMeta {
                    labels: Some(labels),
                    ..Default::default()
                }),
                spec: Some(PodSpec {
                    restart_policy: Some("Never".to_string()),
                    containers: vec![Container {
                        name: "test-runner".to_string(),
                        image: Some(image.to_string()),
                        command: Some(command),
                        env: Some(env),
                        ..Default::default()
                    }],
                    ..Default::default()
                }),
            },
            ..Default::default()
        }),
        ..Default::default()
    }
}

fn is_job_succeeded(status: &JobStatus) -> bool {
    status.succeeded.unwrap_or(0) > 0
}

fn is_job_failed(status: &JobStatus) -> bool {
    status.failed.unwrap_or(0) > 0
        || status
            .conditions
            .as_ref()
            .map(|cs| {
                cs.iter()
                    .any(|c| c.type_ == "Failed" && c.status == "True")
            })
            .unwrap_or(false)
}

// ---------------------------------------------------------------------------
// DAG validation
// ---------------------------------------------------------------------------

fn validate_suite_dag(
    suites: &[crate::crd::TestSuite],
) -> std::result::Result<(), Error> {
    let names: HashSet<&str> = suites.iter().map(|s| s.name.as_str()).collect();

    for suite in suites {
        for dep in &suite.depends_on {
            if !names.contains(dep.as_str()) {
                return Err(Error::AmiTestFailed(format!(
                    "Suite '{}' depends on '{}' which does not exist",
                    suite.name, dep
                )));
            }
            if dep == &suite.name {
                return Err(Error::AmiTestFailed(format!(
                    "Suite '{}' depends on itself",
                    suite.name
                )));
            }
        }
    }

    // Simple cycle detection via topological sort
    let mut in_degree: HashMap<&str, usize> = HashMap::new();
    let mut adj: HashMap<&str, Vec<&str>> = HashMap::new();

    for suite in suites {
        in_degree.entry(suite.name.as_str()).or_insert(0);
        for dep in &suite.depends_on {
            adj.entry(dep.as_str())
                .or_default()
                .push(suite.name.as_str());
            *in_degree.entry(suite.name.as_str()).or_insert(0) += 1;
        }
    }

    let mut queue: Vec<&str> = in_degree
        .iter()
        .filter(|(_, &d)| d == 0)
        .map(|(&n, _)| n)
        .collect();

    let mut visited = 0;
    while let Some(node) = queue.pop() {
        visited += 1;
        if let Some(neighbors) = adj.get(node) {
            for &neighbor in neighbors {
                if let Some(degree) = in_degree.get_mut(neighbor) {
                    *degree -= 1;
                    if *degree == 0 {
                        queue.push(neighbor);
                    }
                }
            }
        }
    }

    if visited != suites.len() {
        return Err(Error::AmiTestFailed(
            "Cycle detected in suite dependency graph".into(),
        ));
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Status helpers
// ---------------------------------------------------------------------------

async fn update_phase(
    test: &AmiTest,
    phase: AmiTestPhase,
    error_msg: Option<&str>,
    state: &ControllerState,
) -> std::result::Result<(), Error> {
    let namespace = test.namespace().unwrap_or_default();
    let name = test.name_any();
    let api: Api<AmiTest> = Api::namespaced(state.client.clone(), &namespace);

    let ready = phase == AmiTestPhase::Passed;
    let conditions = vec![
        create_condition(
            "Ready",
            ready,
            &format!("{}", phase),
            error_msg.unwrap_or(&format!("{}", phase)),
        ),
    ];

    let patch = serde_json::json!({
        "status": {
            "phase": phase,
            "conditions": conditions,
            "observedGeneration": test.metadata.generation.unwrap_or(0),
        }
    });

    api.patch_status(&name, &PatchParams::apply("pangea-operator"), &Patch::Merge(&patch))
        .await
        .map_err(Error::Kube)?;

    Ok(())
}

async fn update_status_with_ami(
    test: &AmiTest,
    ami_id: &str,
    ami_region: Option<&str>,
    suites: &[SuiteResult],
    state: &ControllerState,
) -> std::result::Result<(), Error> {
    let namespace = test.namespace().unwrap_or_default();
    let name = test.name_any();
    let api: Api<AmiTest> = Api::namespaced(state.client.clone(), &namespace);

    let patch = serde_json::json!({
        "status": {
            "phase": AmiTestPhase::Running,
            "amiId": ami_id,
            "amiRegion": ami_region,
            "suites": suites,
            "startedAt": Utc::now(),
            "observedGeneration": test.metadata.generation.unwrap_or(0),
        }
    });

    api.patch_status(&name, &PatchParams::apply("pangea-operator"), &Patch::Merge(&patch))
        .await
        .map_err(Error::Kube)?;

    Ok(())
}

async fn update_suites(
    test: &AmiTest,
    suites: &[SuiteResult],
    state: &ControllerState,
) -> std::result::Result<(), Error> {
    let namespace = test.namespace().unwrap_or_default();
    let name = test.name_any();
    let api: Api<AmiTest> = Api::namespaced(state.client.clone(), &namespace);

    let passed = suites
        .iter()
        .filter(|s| s.phase == SuitePhase::Succeeded)
        .count();
    let total = suites.len();
    let summary = format!("{passed}/{total} passed");

    let patch = serde_json::json!({
        "status": {
            "suites": suites,
            "suiteSummary": summary,
        }
    });

    api.patch_status(&name, &PatchParams::apply("pangea-operator"), &Patch::Merge(&patch))
        .await
        .map_err(Error::Kube)?;

    Ok(())
}

// ---------------------------------------------------------------------------
// Finalizer helpers
// ---------------------------------------------------------------------------

fn has_finalizer(test: &AmiTest) -> bool {
    test.metadata
        .finalizers
        .as_ref()
        .map(|f| f.contains(&FINALIZER_NAME.to_string()))
        .unwrap_or(false)
}

async fn add_finalizer(
    test: &AmiTest,
    state: &ControllerState,
) -> std::result::Result<(), Error> {
    let namespace = test.namespace().unwrap_or_default();
    let name = test.name_any();
    let api: Api<AmiTest> = Api::namespaced(state.client.clone(), &namespace);

    let patch = serde_json::json!({
        "metadata": { "finalizers": [FINALIZER_NAME] }
    });

    api.patch(&name, &PatchParams::apply("pangea-operator"), &Patch::Merge(&patch))
        .await
        .map_err(Error::Kube)?;

    Ok(())
}

async fn remove_finalizer(
    test: &AmiTest,
    state: &ControllerState,
) -> std::result::Result<(), Error> {
    let namespace = test.namespace().unwrap_or_default();
    let name = test.name_any();
    let api: Api<AmiTest> = Api::namespaced(state.client.clone(), &namespace);

    let finalizers: Vec<String> = test
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
    _test: Arc<AmiTest>,
    error: &Error,
    ctx: Arc<ControllerState>,
) -> Action {
    use crate::controller::error_policy::{run_error_policy, tiered_backoff};
    run_error_policy(
        &ctx.metrics,
        crate::crd::ControllerKind::AmiTest,
        error,
        tiered_backoff(error.is_retryable()),
    )
}

#[cfg(test)]
mod tests {
    use crate::crd::ami_test::{AmiTest, AmiTestPhase, AmiTestStatus};
    use kube::CustomResourceExt;

    #[test]
    fn phase_serde_round_trip() {
        for p in [
            AmiTestPhase::Pending,
            AmiTestPhase::Resolving,
            AmiTestPhase::Running,
            AmiTestPhase::Passed,
            AmiTestPhase::Failed,
            AmiTestPhase::Cleaning,
        ] {
            let s = serde_json::to_string(&p).unwrap();
            let back: AmiTestPhase = serde_json::from_str(&s).unwrap();
            assert_eq!(format!("{:?}", p), format!("{:?}", back));
        }
    }

    #[test]
    fn status_default_round_trips() {
        let s = AmiTestStatus::default();
        let j = serde_json::to_string(&s).unwrap();
        let back: AmiTestStatus = serde_json::from_str(&j).unwrap();
        assert_eq!(format!("{:?}", s), format!("{:?}", back));
    }

    #[test]
    fn crd_yaml_renders_cleanly() {
        let yaml = serde_yaml::to_string(&AmiTest::crd()).expect("CRD serializes");
        assert!(yaml.contains("amitests.pangea.pleme.io"));
        assert!(yaml.contains("- pangea"));
    }
}

#[cfg(test)]
mod deep_tests {
    use super::{is_job_failed, is_job_succeeded, validate_suite_dag};
    use crate::crd::TestSuite;
    use k8s_openapi::api::batch::v1::JobStatus;
    use k8s_openapi::apimachinery::pkg::apis::meta::v1::Condition;

    #[test]
    fn job_succeeded_when_succeeded_ge_1() {
        let mut s = JobStatus::default();
        assert!(!is_job_succeeded(&s));
        s.succeeded = Some(1);
        assert!(is_job_succeeded(&s));
        s.succeeded = Some(0);
        assert!(!is_job_succeeded(&s));
    }

    #[test]
    fn job_failed_via_count_or_condition() {
        let mut s = JobStatus::default();
        assert!(!is_job_failed(&s));
        s.failed = Some(2);
        assert!(is_job_failed(&s));

        // Reset count, fail via condition
        s.failed = None;
        s.conditions = Some(vec![k8s_openapi::api::batch::v1::JobCondition {
            type_: "Failed".into(),
            status: "True".into(),
            reason: Some("BackoffLimitExceeded".into()),
            message: Some("exceeded".into()),
            last_probe_time: None,
            last_transition_time: None,
        }]);
        assert!(is_job_failed(&s));
    }

    fn ts(name: &str, deps: &[&str]) -> TestSuite {
        TestSuite {
            name: name.into(),
            suite_type: crate::crd::ami_test::TestSuiteType::PackerTest,
            depends_on: deps.iter().map(|s| (*s).into()).collect(),
            timeout: "5m".into(),
            config: None,
        }
    }

    #[test]
    fn dag_validation_rejects_self_dep() {
        let suites = vec![ts("a", &["a"])];
        let r = validate_suite_dag(&suites);
        assert!(r.is_err());
        assert!(format!("{:?}", r.unwrap_err()).contains("itself"));
    }

    #[test]
    fn dag_validation_rejects_missing_dep() {
        let suites = vec![ts("a", &["nonexistent"])];
        let r = validate_suite_dag(&suites);
        assert!(r.is_err());
        assert!(format!("{:?}", r.unwrap_err()).contains("does not exist"));
    }

    #[test]
    fn dag_validation_accepts_proper_chain() {
        let suites = vec![ts("a", &[]), ts("b", &["a"]), ts("c", &["a", "b"])];
        assert!(validate_suite_dag(&suites).is_ok());
    }
}
