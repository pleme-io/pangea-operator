//! Controller for ComplianceSchedule resources.
//!
//! Runs compliance suites on schedule or infrastructure change, creates K8s
//! Jobs for kensa/InSpec execution, collects results, emits HeartbeatChain
//! events, and feeds the observability pipeline.

use crate::crd::{
    ComplianceSchedule, ComplianceSchedulePhase, ComplianceSuiteResult,
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
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, error, info, instrument, warn};

use super::{
    create_condition, ControllerState, DEFAULT_REQUEUE_INTERVAL, ERROR_REQUEUE_INTERVAL,
    SHORT_REQUEUE_INTERVAL,
};

const FINALIZER_NAME: &str = "pangea.pleme.io/compliance-schedule-cleanup";

pub struct ComplianceScheduleController {
    state: ControllerState,
}

impl ComplianceScheduleController {
    pub fn new(state: ControllerState) -> Self {
        Self { state }
    }

    pub async fn run(self) -> crate::error::Result<()> {
        let client = self.state.client.clone();
        let api: Api<ComplianceSchedule> = Api::all(client.clone());
        let state = Arc::new(self.state);

        info!("Starting ComplianceSchedule controller");

        Controller::new(api, Config::default())
            .run(
                move |schedule, ctx| {
                    let state = Arc::clone(&ctx);
                    async move { reconcile(schedule, state).await }
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
                            "ComplianceSchedule reconciliation completed"
                        );
                    }
                    Err(e) => {
                        error!(error = %e, "ComplianceSchedule reconciliation failed");
                    }
                }
            })
            .await;

        Ok(())
    }
}

#[instrument(skip(state), fields(name = %schedule.name_any(), namespace = ?schedule.namespace()))]
async fn reconcile(
    schedule: Arc<ComplianceSchedule>,
    state: Arc<ControllerState>,
) -> std::result::Result<Action, Error> {
    let name = schedule.name_any();
    let namespace = schedule.namespace().unwrap_or_default();

    info!("Reconciling ComplianceSchedule");

    // Cluster-wide kill-switch — honor `OperatorPolicy/default`.
    if let Some(action) = crate::controller::policy_gate::check_operator_policy(
        &state,
        crate::crd::ControllerKind::ComplianceSchedule,
    ) {
        return Ok(action);
    }

    // Handle deletion
    if schedule.metadata.deletion_timestamp.is_some() {
        if has_finalizer(&schedule) {
            cleanup_jobs(&name, &namespace, &state).await?;
            remove_finalizer(&schedule, &state).await?;
        }
        return Ok(Action::await_change());
    }

    if !has_finalizer(&schedule) {
        add_finalizer(&schedule, &state).await?;
        return Ok(Action::requeue(Duration::from_secs(1)));
    }

    if schedule.spec.suspend {
        return Ok(Action::requeue(DEFAULT_REQUEUE_INTERVAL));
    }

    let current_phase = schedule
        .status
        .as_ref()
        .and_then(|s| s.phase)
        .unwrap_or(ComplianceSchedulePhase::Idle);

    let action = match current_phase {
        ComplianceSchedulePhase::Idle => handle_idle(&schedule, &state).await,
        ComplianceSchedulePhase::Running => handle_running(&schedule, &state).await,
        ComplianceSchedulePhase::Compliant => handle_scheduled(&schedule, &state).await,
        ComplianceSchedulePhase::NonCompliant => handle_scheduled(&schedule, &state).await,
        ComplianceSchedulePhase::Error => handle_scheduled(&schedule, &state).await,
    };

    match action {
        Ok(a) => Ok(a),
        Err(e) => {
            warn!(error = %e, "ComplianceSchedule reconciliation error");
            let _ = update_phase(&schedule, ComplianceSchedulePhase::Error, Some(&e.to_string()), &state).await;
            Ok(Action::requeue(ERROR_REQUEUE_INTERVAL))
        }
    }
}

// ---------------------------------------------------------------------------
// Phase handlers
// ---------------------------------------------------------------------------

async fn handle_idle(
    schedule: &ComplianceSchedule,
    state: &ControllerState,
) -> std::result::Result<Action, Error> {
    // First run — launch suites immediately
    info!("ComplianceSchedule idle, starting first compliance run");
    launch_suites(schedule, state).await?;
    update_phase(schedule, ComplianceSchedulePhase::Running, None, state).await?;
    Ok(Action::requeue(SHORT_REQUEUE_INTERVAL))
}

async fn handle_running(
    schedule: &ComplianceSchedule,
    state: &ControllerState,
) -> std::result::Result<Action, Error> {
    let name = schedule.name_any();
    let namespace = schedule.namespace().unwrap_or_default();

    let job_api: Api<Job> = Api::namespaced(state.client.clone(), &namespace);
    let lp = ListParams::default().labels(&format!("pangea.pleme.io/compliance-schedule={name}"));
    let jobs = job_api.list(&lp).await.map_err(Error::Kube)?;

    let mut results: Vec<ComplianceSuiteResult> = Vec::new();
    let mut all_done = true;
    let mut any_failed = false;

    for suite_spec in &schedule.spec.suites {
        let job_name = format!("{}-{}", name, suite_spec.name);
        let job = jobs.items.iter().find(|j| {
            j.metadata.name.as_deref() == Some(&job_name)
        });

        match job {
            Some(j) => {
                let status = j.status.as_ref();
                if status.map(|s| s.succeeded.unwrap_or(0) > 0).unwrap_or(false) {
                    results.push(ComplianceSuiteResult {
                        name: suite_spec.name.clone(),
                        runner: suite_spec.runner.clone(),
                        passed: true,
                        controls_assessed: 0, // TODO: parse from Job logs
                        controls_passed: 0,
                        controls_failed: 0,
                        controls_skipped: 0,
                        failed_controls: vec![],
                        result_hash: None,
                        job_name: Some(job_name),
                        duration: None,
                        completed_at: Some(Utc::now()),
                    });
                } else if status.map(|s| s.failed.unwrap_or(0) > 0).unwrap_or(false) {
                    any_failed = true;
                    results.push(ComplianceSuiteResult {
                        name: suite_spec.name.clone(),
                        runner: suite_spec.runner.clone(),
                        passed: false,
                        controls_assessed: 0,
                        controls_passed: 0,
                        controls_failed: 0,
                        controls_skipped: 0,
                        failed_controls: vec![],
                        result_hash: None,
                        job_name: Some(job_name),
                        duration: None,
                        completed_at: Some(Utc::now()),
                    });
                } else {
                    all_done = false;
                }
            }
            None => {
                all_done = false;
            }
        }
    }

    if all_done {
        let passed = results.iter().filter(|r| r.passed).count();
        let total = results.len();
        let summary = format!("{passed}/{total} suites passed");

        update_suite_results(schedule, &results, &summary, state).await?;

        let phase = if any_failed {
            ComplianceSchedulePhase::NonCompliant
        } else {
            ComplianceSchedulePhase::Compliant
        };

        update_phase(schedule, phase, None, state).await?;

        // TODO: emit HeartbeatChain events
        // TODO: emit Vector/Shinryu events
        // TODO: update sekiban SignatureGate

        return Ok(Action::requeue(SHORT_REQUEUE_INTERVAL));
    }

    Ok(Action::requeue(Duration::from_secs(15)))
}

async fn handle_scheduled(
    schedule: &ComplianceSchedule,
    state: &ControllerState,
) -> std::result::Result<Action, Error> {
    // Check if it's time for the next run based on cron schedule
    // For now, just requeue at the default interval
    // TODO: parse cron expression and compute next run time

    Ok(Action::requeue(DEFAULT_REQUEUE_INTERVAL))
}

// ---------------------------------------------------------------------------
// Suite launching
// ---------------------------------------------------------------------------

async fn launch_suites(
    schedule: &ComplianceSchedule,
    state: &ControllerState,
) -> std::result::Result<(), Error> {
    let name = schedule.name_any();
    let namespace = schedule.namespace().unwrap_or_default();
    let job_api: Api<Job> = Api::namespaced(state.client.clone(), &namespace);

    let runner_image = schedule
        .spec
        .runner_image
        .as_deref()
        .unwrap_or("ghcr.io/pleme-io/kensa:latest");

    for suite in &schedule.spec.suites {
        let job_name = format!("{}-{}", name, suite.name);

        let command = match suite.runner {
            crate::crd::ComplianceRunner::Kensa => {
                let profile = suite.profile.as_deref().unwrap_or("default");
                vec![
                    "kensa".to_string(),
                    "run".to_string(),
                    "--profile".to_string(),
                    profile.to_string(),
                    "--reporter".to_string(),
                    "json".to_string(),
                ]
            }
            crate::crd::ComplianceRunner::InSpec => {
                let profile = suite.profile.as_deref().unwrap_or(".");
                vec![
                    "inspec".to_string(),
                    "exec".to_string(),
                    profile.to_string(),
                    "--reporter".to_string(),
                    "json".to_string(),
                ]
            }
            crate::crd::ComplianceRunner::Script => {
                suite
                    .config
                    .as_ref()
                    .and_then(|c| c.get("command"))
                    .and_then(|cmd| cmd.as_array())
                    .map(|arr| arr.iter().filter_map(|v| v.as_str().map(String::from)).collect())
                    .unwrap_or_else(|| vec!["echo".to_string(), "no command".to_string()])
            }
        };

        let timeout_secs = super::parse_duration(&suite.timeout)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(900);

        let mut labels = std::collections::BTreeMap::new();
        labels.insert(
            "pangea.pleme.io/compliance-schedule".to_string(),
            name.clone(),
        );
        labels.insert("pangea.pleme.io/suite".to_string(), suite.name.clone());

        let job = Job {
            metadata: ObjectMeta {
                name: Some(job_name.clone()),
                namespace: Some(namespace.clone()),
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
                            name: "compliance-runner".to_string(),
                            image: Some(runner_image.to_string()),
                            command: Some(command),
                            env: Some(vec![
                                EnvVar {
                                    name: "SUITE_NAME".to_string(),
                                    value: Some(suite.name.clone()),
                                    ..Default::default()
                                },
                            ]),
                            ..Default::default()
                        }],
                        ..Default::default()
                    }),
                },
                ..Default::default()
            }),
            ..Default::default()
        };

        match job_api.create(&Default::default(), &job).await {
            Ok(_) => info!(suite = %suite.name, job = %job_name, "Compliance suite Job created"),
            Err(kube::Error::Api(ref e)) if e.code == 409 => {
                info!(job = %job_name, "Compliance Job already exists");
            }
            Err(e) => {
                warn!(suite = %suite.name, error = %e, "Failed to create compliance Job");
            }
        }
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Status helpers
// ---------------------------------------------------------------------------

async fn update_phase(
    schedule: &ComplianceSchedule,
    phase: ComplianceSchedulePhase,
    error_msg: Option<&str>,
    state: &ControllerState,
) -> std::result::Result<(), Error> {
    let namespace = schedule.namespace().unwrap_or_default();
    let name = schedule.name_any();
    let api: Api<ComplianceSchedule> = Api::namespaced(state.client.clone(), &namespace);

    let ready = phase == ComplianceSchedulePhase::Compliant;
    let conditions = vec![create_condition(
        "Ready",
        ready,
        &format!("{phase}"),
        error_msg.unwrap_or(&format!("{phase}")),
    )];

    let patch = serde_json::json!({
        "status": {
            "phase": phase,
            "conditions": conditions,
            "observedGeneration": schedule.metadata.generation.unwrap_or(0),
            "lastRunAt": Utc::now(),
        }
    });

    api.patch_status(&name, &PatchParams::apply("pangea-operator"), &Patch::Merge(&patch))
        .await
        .map_err(Error::Kube)?;

    Ok(())
}

async fn update_suite_results(
    schedule: &ComplianceSchedule,
    results: &[ComplianceSuiteResult],
    summary: &str,
    state: &ControllerState,
) -> std::result::Result<(), Error> {
    let namespace = schedule.namespace().unwrap_or_default();
    let name = schedule.name_any();
    let api: Api<ComplianceSchedule> = Api::namespaced(state.client.clone(), &namespace);

    let total_runs = schedule.status.as_ref().map(|s| s.total_runs).unwrap_or(0) + 1;

    let patch = serde_json::json!({
        "status": {
            "suites": results,
            "controlSummary": summary,
            "totalRuns": total_runs,
        }
    });

    api.patch_status(&name, &PatchParams::apply("pangea-operator"), &Patch::Merge(&patch))
        .await
        .map_err(Error::Kube)?;

    Ok(())
}

// ---------------------------------------------------------------------------
// Cleanup
// ---------------------------------------------------------------------------

async fn cleanup_jobs(
    name: &str,
    namespace: &str,
    state: &ControllerState,
) -> std::result::Result<(), Error> {
    let job_api: Api<Job> = Api::namespaced(state.client.clone(), namespace);
    let lp = ListParams::default().labels(&format!("pangea.pleme.io/compliance-schedule={name}"));
    if let Ok(jobs) = job_api.list(&lp).await {
        for job in jobs.items {
            let jn = job.metadata.name.unwrap_or_default();
            let _ = job_api.delete(&jn, &Default::default()).await;
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Finalizer helpers
// ---------------------------------------------------------------------------

fn has_finalizer(schedule: &ComplianceSchedule) -> bool {
    schedule
        .metadata
        .finalizers
        .as_ref()
        .map(|f| f.contains(&FINALIZER_NAME.to_string()))
        .unwrap_or(false)
}

async fn add_finalizer(
    schedule: &ComplianceSchedule,
    state: &ControllerState,
) -> std::result::Result<(), Error> {
    let namespace = schedule.namespace().unwrap_or_default();
    let name = schedule.name_any();
    let api: Api<ComplianceSchedule> = Api::namespaced(state.client.clone(), &namespace);
    let patch = serde_json::json!({ "metadata": { "finalizers": [FINALIZER_NAME] } });
    api.patch(&name, &PatchParams::apply("pangea-operator"), &Patch::Merge(&patch))
        .await
        .map_err(Error::Kube)?;
    Ok(())
}

async fn remove_finalizer(
    schedule: &ComplianceSchedule,
    state: &ControllerState,
) -> std::result::Result<(), Error> {
    let namespace = schedule.namespace().unwrap_or_default();
    let name = schedule.name_any();
    let api: Api<ComplianceSchedule> = Api::namespaced(state.client.clone(), &namespace);
    let finalizers: Vec<String> = schedule
        .metadata
        .finalizers
        .as_ref()
        .map(|f| f.iter().filter(|s| s.as_str() != FINALIZER_NAME).cloned().collect())
        .unwrap_or_default();
    let patch = serde_json::json!({ "metadata": { "finalizers": finalizers } });
    api.patch(&name, &PatchParams::apply("pangea-operator"), &Patch::Merge(&patch))
        .await
        .map_err(Error::Kube)?;
    Ok(())
}

fn error_policy(
    _schedule: Arc<ComplianceSchedule>,
    error: &Error,
    _ctx: Arc<ControllerState>,
) -> Action {
    warn!(error = %error, "ComplianceSchedule error policy triggered");
    Action::requeue(ERROR_REQUEUE_INTERVAL)
}
