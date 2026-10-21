use std::collections::BTreeMap;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use k8s_openapi::api::batch::v1::{CronJob, Job};
use k8s_openapi::api::core::v1::Pod;
use kube::api::{Api, ListParams};
use kube::Client;
use prometheus::{IntGaugeVec, Opts, Registry};
use tracing::{info, warn};

use crate::crd::InfrastructureTemplate;

pub const CRONJOB_OWNER_LABEL: &str = "pangea.pleme.io/owner";
pub const TICK: Duration = Duration::from_secs(30);
pub const DEFAULT_WEDGE_SECONDS: i64 = 1200;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TemplateRow {
    pub template: String,
    pub namespace: String,
    pub phase: String,
    pub phase_age_seconds: i64,
    pub generations_behind: i64,
    pub pending_since: Option<DateTime<Utc>>,
    pub conditions: Vec<ConditionRow>,
    pub pass_kind: Option<String>,
    pub stall: Option<String>,
    pub heartbeat_age_seconds: Option<i64>,
    pub in_flight: Option<(String, i64)>,
    pub reactions: BTreeMap<String, u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConditionRow {
    pub kind: String,
    pub status: String,
    pub generations_behind: i64,
}

pub const PROJECTED_CONDITIONS: [&str; 2] = ["Ready", "Settled"];

fn age(now: DateTime<Utc>, at: DateTime<Utc>) -> i64 {
    (now - at).num_seconds().max(0)
}

pub fn template_row(t: &InfrastructureTemplate, now: DateTime<Utc>) -> Option<TemplateRow> {
    if t.spec.suspend || t.metadata.deletion_timestamp.is_some() {
        return None;
    }
    let template = t.metadata.name.clone()?;
    let namespace = t.metadata.namespace.clone().unwrap_or_default();
    let generation = t.metadata.generation.unwrap_or(0);
    let status = t.status.clone().unwrap_or_default();
    let observed = status.observed_generation;
    let generations_behind = (generation - observed).max(0);
    let phase = status.phase.unwrap_or_default();
    let entered = status
        .progress
        .as_ref()
        .filter(|p| p.phase == phase && p.finished_at.is_none())
        .map(|p| p.phase_started_at)
        .or(status.phase_entered_at);
    let pending_since = if generations_behind > 0 {
        Some(status.generation_lag_since.or(entered).unwrap_or_else(|| {
            t.metadata
                .creation_timestamp
                .as_ref()
                .map(|c| c.0)
                .unwrap_or(now)
        }))
    } else {
        None
    };
    let conditions = PROJECTED_CONDITIONS
        .iter()
        .filter_map(|kind| {
            status
                .conditions
                .iter()
                .find(|c| c.r#type == *kind)
                .map(|c| ConditionRow {
                    kind: (*kind).to_string(),
                    status: c.status.clone(),
                    generations_behind: (generation - c.observed_generation.unwrap_or(observed))
                        .max(0),
                })
        })
        .collect();
    let pass = status.pass.as_ref();
    let in_flight = status
        .progress
        .as_ref()
        .filter(|p| p.finished_at.is_none())
        .and_then(|p| p.in_flight.as_ref())
        .map(|f| {
            (
                f.rpc.clone().unwrap_or_else(|| "unnamed".into()),
                age(now, f.since),
            )
        });
    Some(TemplateRow {
        template,
        namespace,
        phase: phase.to_string(),
        phase_age_seconds: entered.map(|e| age(now, e)).unwrap_or(0),
        generations_behind,
        pending_since,
        conditions,
        pass_kind: pass.map(|p| p.kind.to_string()),
        stall: pass.and_then(stall_of),
        heartbeat_age_seconds: pass.and_then(|p| p.heartbeat_at).map(|h| age(now, h)),
        in_flight,
        reactions: status.reactions.map(|r| r.counts).unwrap_or_default(),
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CronRow {
    pub namespace: String,
    pub cronjob: String,
    pub last_success: Option<i64>,
    pub last_failure: Option<i64>,
    pub consecutive_failures: i64,
}

enum JobOutcome {
    Succeeded,
    Failed,
    Running,
}

fn job_outcome(job: &Job) -> JobOutcome {
    let status = job.status.as_ref();
    let condition = |kind: &str| {
        status
            .and_then(|s| s.conditions.as_ref())
            .is_some_and(|cs| cs.iter().any(|c| c.type_ == kind && c.status == "True"))
    };
    if condition("Complete") || status.and_then(|s| s.succeeded).unwrap_or(0) > 0 {
        JobOutcome::Succeeded
    } else if condition("Failed") {
        JobOutcome::Failed
    } else {
        JobOutcome::Running
    }
}

pub fn cron_rows(cronjobs: &[CronJob], jobs: &[Job]) -> Vec<CronRow> {
    cronjobs
        .iter()
        .filter_map(|cj| {
            let cronjob = cj.metadata.name.clone()?;
            let namespace = cj.metadata.namespace.clone().unwrap_or_default();
            let mut owned: Vec<(i64, &Job)> = jobs
                .iter()
                .filter(|j| j.metadata.namespace.as_deref() == Some(namespace.as_str()))
                .filter(|j| {
                    j.metadata.owner_references.as_ref().is_some_and(|refs| {
                        refs.iter()
                            .any(|r| r.kind == "CronJob" && r.name == cronjob)
                    })
                })
                .map(|j| {
                    let at = j
                        .status
                        .as_ref()
                        .and_then(|s| s.completion_time.as_ref().or(s.start_time.as_ref()))
                        .or(j.metadata.creation_timestamp.as_ref())
                        .map(|t| t.0.timestamp())
                        .unwrap_or(0);
                    (at, j)
                })
                .collect();
            owned.sort_by(|a, b| b.0.cmp(&a.0));
            let mut last_success = None;
            let mut last_failure = None;
            let mut consecutive_failures = 0;
            let mut streak_open = true;
            for (at, job) in &owned {
                match job_outcome(job) {
                    JobOutcome::Succeeded => {
                        last_success = last_success.or(Some(*at));
                        streak_open = false;
                    }
                    JobOutcome::Failed => {
                        last_failure = last_failure.or(Some(*at));
                        if streak_open {
                            consecutive_failures += 1;
                        }
                    }
                    JobOutcome::Running => {}
                }
            }
            Some(CronRow {
                namespace,
                cronjob,
                last_success,
                last_failure,
                consecutive_failures,
            })
        })
        .collect()
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContainerRow {
    pub namespace: String,
    pub pod: String,
    pub app: String,
    pub container: String,
    pub restarts: i64,
    pub ready: bool,
}

pub fn container_rows(pods: &[Pod]) -> Vec<ContainerRow> {
    pods.iter()
        .flat_map(|p| {
            let namespace = p.metadata.namespace.clone().unwrap_or_default();
            let pod = p.metadata.name.clone().unwrap_or_default();
            let labels = p.metadata.labels.clone().unwrap_or_default();
            let app = labels
                .get("app.kubernetes.io/name")
                .or_else(|| labels.get("app"))
                .cloned()
                .unwrap_or_else(|| pod.clone());
            p.status
                .as_ref()
                .and_then(|s| s.container_statuses.clone())
                .unwrap_or_default()
                .into_iter()
                .map(move |c| ContainerRow {
                    namespace: namespace.clone(),
                    pod: pod.clone(),
                    app: app.clone(),
                    container: c.name,
                    restarts: i64::from(c.restart_count),
                    ready: c.ready,
                })
        })
        .collect()
}

pub fn stall_of(pass: &crate::crd::PassStatus) -> Option<String> {
    stall_in(&serde_json::to_value(pass).ok()?)
}

pub fn stall_in(pass: &serde_json::Value) -> Option<String> {
    pass.get("stall")?.as_str().map(str::to_string)
}

pub fn latest_running_heartbeat(rows: &[TemplateRow], now: DateTime<Utc>) -> Option<DateTime<Utc>> {
    rows.iter()
        .filter(|r| matches!(r.pass_kind.as_deref(), Some("planning") | Some("applying")))
        .filter_map(|r| r.heartbeat_age_seconds)
        .min()
        .map(|age| now - chrono::Duration::seconds(age))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Progress {
    Idle,
    Moving,
    Wedged {
        pending_seconds: i64,
        idle_seconds: i64,
    },
}

#[derive(Debug, Default)]
pub struct ProgressGate {
    last_reconcile: AtomicI64,
    oldest_pending: AtomicI64,
    started: AtomicI64,
}

impl ProgressGate {
    pub fn new(now: DateTime<Utc>) -> Self {
        let gate = Self::default();
        gate.started.store(now.timestamp(), Ordering::Relaxed);
        gate
    }

    pub fn reconciled(&self, now: DateTime<Utc>) {
        self.last_reconcile
            .fetch_max(now.timestamp(), Ordering::Relaxed);
    }

    pub fn pending_since(&self, oldest: Option<DateTime<Utc>>) {
        self.oldest_pending.store(
            oldest.map(|t| t.timestamp()).unwrap_or(0),
            Ordering::Relaxed,
        );
    }

    pub fn last_reconcile(&self) -> i64 {
        self.last_reconcile.load(Ordering::Relaxed)
    }

    pub fn verdict(&self, now: DateTime<Utc>, window_seconds: i64) -> Progress {
        let pending = self.oldest_pending.load(Ordering::Relaxed);
        if pending == 0 {
            return Progress::Idle;
        }
        let since_activity = self
            .last_reconcile
            .load(Ordering::Relaxed)
            .max(self.started.load(Ordering::Relaxed));
        let now = now.timestamp();
        let pending_seconds = (now - pending).max(0);
        let idle_seconds = (now - since_activity).max(0);
        if window_seconds > 0 && pending_seconds > window_seconds && idle_seconds > window_seconds {
            Progress::Wedged {
                pending_seconds,
                idle_seconds,
            }
        } else {
            Progress::Moving
        }
    }
}

pub fn wedge_window_from_env() -> i64 {
    std::env::var("PANGEA_LIVENESS_WEDGE_SECONDS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(DEFAULT_WEDGE_SECONDS)
}

#[derive(Clone)]
pub struct ProjectionMetrics {
    pub template_phase: IntGaugeVec,
    pub phase_age_seconds: IntGaugeVec,
    pub generations_behind: IntGaugeVec,
    pub condition: IntGaugeVec,
    pub condition_generations_behind: IntGaugeVec,
    pub pass: IntGaugeVec,
    pub stall: IntGaugeVec,
    pub heartbeat_age_seconds: IntGaugeVec,
    pub in_flight_rpc_age_seconds: IntGaugeVec,
    pub reactions: IntGaugeVec,
    pub cronjob_last_success: IntGaugeVec,
    pub cronjob_last_failure: IntGaugeVec,
    pub cronjob_consecutive_failures: IntGaugeVec,
    pub container_restarts: IntGaugeVec,
    pub container_ready: IntGaugeVec,
    pub operator: IntGaugeVec,
}

fn gauge(registry: &Registry, name: &str, help: &str, labels: &[&str]) -> IntGaugeVec {
    let g = IntGaugeVec::new(Opts::new(name, help), labels).expect("metric can be created");
    registry
        .register(Box::new(g.clone()))
        .expect("metric can be registered");
    g
}

const T: &[&str] = &["template", "namespace"];

impl ProjectionMetrics {
    pub fn register(registry: &Registry) -> Self {
        Self {
            template_phase: gauge(registry, "pangea_template_phase", "1 for the phase status.phase names", &["template", "namespace", "phase"]),
            phase_age_seconds: gauge(registry, "pangea_template_phase_age_seconds", "Seconds the template has been in its current phase", T),
            generations_behind: gauge(registry, "pangea_template_generations_behind", "metadata.generation minus status.observedGeneration", T),
            condition: gauge(registry, "pangea_template_condition", "1 for each projected condition's current status", &["template", "namespace", "type", "status"]),
            condition_generations_behind: gauge(registry, "pangea_template_condition_generations_behind", "metadata.generation minus the generation the condition was computed for", &["template", "namespace", "type"]),
            pass: gauge(registry, "pangea_template_pass", "1 for the kind status.pass names", &["template", "namespace", "kind"]),
            stall: gauge(registry, "pangea_template_stall", "1 for the stall state status.pass.stall names", &["template", "namespace", "state"]),
            heartbeat_age_seconds: gauge(registry, "pangea_template_pass_heartbeat_age_seconds", "Seconds since status.pass.heartbeatAt", T),
            in_flight_rpc_age_seconds: gauge(registry, "pangea_template_in_flight_rpc_age_seconds", "Seconds the provider RPC in status.progress.inFlight has been outstanding", &["template", "namespace", "rpc"]),
            reactions: gauge(registry, "pangea_template_reactions", "Reactions recorded in status.reactions.counts, by trigger", &["template", "namespace", "trigger"]),
            cronjob_last_success: gauge(registry, "pangea_cronjob_last_success_timestamp_seconds", "Unix time of the newest succeeded Job a CronJob owns", &["namespace", "cronjob"]),
            cronjob_last_failure: gauge(registry, "pangea_cronjob_last_failure_timestamp_seconds", "Unix time of the newest failed Job a CronJob owns", &["namespace", "cronjob"]),
            cronjob_consecutive_failures: gauge(registry, "pangea_cronjob_consecutive_failures", "Failed Jobs since the newest succeeded Job a CronJob owns", &["namespace", "cronjob"]),
            container_restarts: gauge(registry, "pangea_stack_container_restarts", "restartCount of each container in the operator's namespace", &["namespace", "pod", "app", "container"]),
            container_ready: gauge(registry, "pangea_stack_container_ready", "1 while the container reports ready", &["namespace", "pod", "app", "container"]),
            operator: gauge(registry, "pangea_operator_progress", "Operator progress gate readings: last_reconcile_timestamp_seconds, oldest_pending_age_seconds, wedged, projection_timestamp_seconds", &["reading"]),
        }
    }

    pub fn publish_templates(&self, rows: &[TemplateRow]) {
        for g in [
            &self.template_phase,
            &self.phase_age_seconds,
            &self.generations_behind,
            &self.condition,
            &self.condition_generations_behind,
            &self.pass,
            &self.stall,
            &self.heartbeat_age_seconds,
            &self.in_flight_rpc_age_seconds,
            &self.reactions,
        ] {
            g.reset();
        }
        for r in rows {
            let t = [r.template.as_str(), r.namespace.as_str()];
            self.template_phase
                .with_label_values(&[t[0], t[1], r.phase.as_str()])
                .set(1);
            self.phase_age_seconds
                .with_label_values(&t)
                .set(r.phase_age_seconds);
            self.generations_behind
                .with_label_values(&t)
                .set(r.generations_behind);
            for c in &r.conditions {
                self.condition
                    .with_label_values(&[t[0], t[1], c.kind.as_str(), c.status.as_str()])
                    .set(1);
                self.condition_generations_behind
                    .with_label_values(&[t[0], t[1], c.kind.as_str()])
                    .set(c.generations_behind);
            }
            if let Some(kind) = &r.pass_kind {
                self.pass
                    .with_label_values(&[t[0], t[1], kind.as_str()])
                    .set(1);
            }
            if let Some(state) = &r.stall {
                self.stall
                    .with_label_values(&[t[0], t[1], state.as_str()])
                    .set(1);
            }
            if let Some(h) = r.heartbeat_age_seconds {
                self.heartbeat_age_seconds.with_label_values(&t).set(h);
            }
            if let Some((rpc, secs)) = &r.in_flight {
                self.in_flight_rpc_age_seconds
                    .with_label_values(&[t[0], t[1], rpc.as_str()])
                    .set(*secs);
            }
            for (trigger, count) in &r.reactions {
                self.reactions
                    .with_label_values(&[t[0], t[1], trigger.as_str()])
                    .set(i64::try_from(*count).unwrap_or(i64::MAX));
            }
        }
    }

    pub fn publish_cronjobs(&self, rows: &[CronRow]) {
        self.cronjob_last_success.reset();
        self.cronjob_last_failure.reset();
        self.cronjob_consecutive_failures.reset();
        for r in rows {
            let l = [r.namespace.as_str(), r.cronjob.as_str()];
            if let Some(at) = r.last_success {
                self.cronjob_last_success.with_label_values(&l).set(at);
            }
            if let Some(at) = r.last_failure {
                self.cronjob_last_failure.with_label_values(&l).set(at);
            }
            self.cronjob_consecutive_failures
                .with_label_values(&l)
                .set(r.consecutive_failures);
        }
    }

    pub fn publish_containers(&self, rows: &[ContainerRow]) {
        self.container_restarts.reset();
        self.container_ready.reset();
        for r in rows {
            let l = [
                r.namespace.as_str(),
                r.pod.as_str(),
                r.app.as_str(),
                r.container.as_str(),
            ];
            self.container_restarts
                .with_label_values(&l)
                .set(r.restarts);
            self.container_ready
                .with_label_values(&l)
                .set(i64::from(r.ready));
        }
    }

    pub fn publish_progress(&self, gate: &ProgressGate, now: DateTime<Utc>, window_seconds: i64) {
        let verdict = gate.verdict(now, window_seconds);
        let oldest = gate.oldest_pending.load(Ordering::Relaxed);
        self.operator
            .with_label_values(&["last_reconcile_timestamp_seconds"])
            .set(gate.last_reconcile());
        self.operator
            .with_label_values(&["oldest_pending_age_seconds"])
            .set(if oldest == 0 {
                0
            } else {
                (now.timestamp() - oldest).max(0)
            });
        self.operator
            .with_label_values(&["wedged"])
            .set(i64::from(matches!(verdict, Progress::Wedged { .. })));
        self.operator
            .with_label_values(&["projection_timestamp_seconds"])
            .set(now.timestamp());
    }
}

pub async fn run(client: Client, metrics: Arc<super::Metrics>, operator_namespace: String) {
    let window = wedge_window_from_env();
    info!(window_seconds = window, namespace = %operator_namespace, "status projection running");
    let templates: Api<InfrastructureTemplate> = Api::all(client.clone());
    let cronjobs: Api<CronJob> = Api::all(client.clone());
    let jobs: Api<Job> = Api::all(client.clone());
    let pods: Api<Pod> = Api::namespaced(client, &operator_namespace);
    let owned = ListParams::default().labels(CRONJOB_OWNER_LABEL);
    let mut tick = tokio::time::interval(TICK);
    loop {
        tick.tick().await;
        let now = Utc::now();
        match templates.list(&ListParams::default()).await {
            Ok(list) => {
                let rows: Vec<TemplateRow> = list
                    .items
                    .iter()
                    .filter_map(|t| template_row(t, now))
                    .collect();
                metrics
                    .progress_gate
                    .pending_since(rows.iter().filter_map(|r| r.pending_since).min());
                if let Some(beat) = latest_running_heartbeat(&rows, now) {
                    metrics.progress_gate.reconciled(beat);
                }
                metrics.projection.publish_templates(&rows);
            }
            Err(e) => warn!(error = %e, "status projection could not list templates"),
        }
        match (
            cronjobs.list(&owned).await,
            jobs.list(&ListParams::default()).await,
        ) {
            (Ok(c), Ok(j)) => metrics
                .projection
                .publish_cronjobs(&cron_rows(&c.items, &j.items)),
            (Err(e), _) | (_, Err(e)) => {
                warn!(error = %e, "status projection could not list cronjobs or jobs")
            }
        }
        match pods.list(&ListParams::default()).await {
            Ok(list) => metrics
                .projection
                .publish_containers(&container_rows(&list.items)),
            Err(e) => warn!(error = %e, "status projection could not list pods"),
        }
        metrics
            .projection
            .publish_progress(&metrics.progress_gate, now, window);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crd::live_status::{InFlightCall, LiveProgress, ReactionLedger};
    use crate::crd::pass_status::PassStatus;
    use crate::crd::{Condition, InfrastructureTemplateStatus, Phase};
    use k8s_openapi::api::batch::v1::{JobCondition, JobStatus};
    use k8s_openapi::apimachinery::pkg::apis::meta::v1::{ObjectMeta, OwnerReference, Time};

    fn at(s: i64) -> DateTime<Utc> {
        DateTime::<Utc>::from_timestamp(s, 0).unwrap()
    }

    fn condition(kind: &str, status: &str, observed: Option<i64>) -> Condition {
        Condition {
            r#type: kind.into(),
            status: status.into(),
            last_transition_time: at(0),
            reason: "r".into(),
            message: "m".into(),
            observed_generation: observed,
        }
    }

    fn template(generation: i64, status: InfrastructureTemplateStatus) -> InfrastructureTemplate {
        let mut t: InfrastructureTemplate = serde_json::from_value(serde_json::json!({
            "apiVersion": "pangea.pleme.io/v1alpha1",
            "kind": "InfrastructureTemplate",
            "metadata": {"name": "t", "namespace": "ns", "generation": generation},
            "spec": {"source": {"inline": "ignored"}, "pangeaNamespace": "ns"}
        }))
        .unwrap();
        t.status = Some(status);
        t
    }

    #[test]
    fn a_ready_condition_computed_for_an_older_generation_reads_as_behind() {
        let status = InfrastructureTemplateStatus {
            phase: Some(Phase::Ready),
            observed_generation: 6,
            phase_entered_at: Some(at(100)),
            generation_lag_since: Some(at(400)),
            conditions: vec![
                condition("Ready", "True", Some(6)),
                condition("Settled", "True", None),
            ],
            ..Default::default()
        };
        let row = template_row(&template(7, status), at(1_840)).unwrap();
        assert_eq!(row.generations_behind, 1);
        assert_eq!(row.pending_since, Some(at(400)));
        assert_eq!(row.phase, "Ready");
        assert_eq!(row.phase_age_seconds, 1_740);
        assert_eq!(
            row.conditions,
            vec![
                ConditionRow {
                    kind: "Ready".into(),
                    status: "True".into(),
                    generations_behind: 1
                },
                ConditionRow {
                    kind: "Settled".into(),
                    status: "True".into(),
                    generations_behind: 1
                },
            ]
        );
    }

    #[test]
    fn a_stalled_planning_pass_projects_its_state_heartbeat_and_rpc() {
        let mut progress = LiveProgress::new(Phase::Planning, at(1_000), None);
        progress.in_flight = Some(InFlightCall {
            address: "a.b".into(),
            rpc: Some("PlanResourceChange".into()),
            since: at(1_100),
        });
        let mut counts = BTreeMap::new();
        counts.insert("noProgress".to_string(), 2);
        let status = InfrastructureTemplateStatus {
            phase: Some(Phase::Planning),
            observed_generation: 3,
            progress: Some(progress),
            pass: Some(
                serde_json::from_value::<PassStatus>(serde_json::json!({
                    "kind": "stalled",
                    "generation": 3,
                    "since": "1970-01-01T00:16:40Z",
                    "heartbeatAt": "1970-01-01T00:20:00Z",
                    "stall": "noProgress"
                }))
                .unwrap(),
            ),
            reactions: Some(ReactionLedger {
                counts,
                ..Default::default()
            }),
            ..Default::default()
        };
        let row = template_row(&template(3, status), at(1_900)).unwrap();
        assert_eq!(row.phase_age_seconds, 900);
        assert_eq!(row.pass_kind.as_deref(), Some("stalled"));
        assert_eq!(row.heartbeat_age_seconds, Some(700));
        assert_eq!(row.in_flight, Some(("PlanResourceChange".into(), 800)));
        assert_eq!(row.reactions.get("noProgress"), Some(&2));
        assert_eq!(row.pending_since, None);
    }

    #[test]
    fn the_stall_state_is_read_from_the_pass_as_written() {
        assert_eq!(
            stall_in(&serde_json::json!({"kind": "stalled", "stall": "heartbeatLost"})).as_deref(),
            Some("heartbeatLost")
        );
        assert_eq!(stall_in(&serde_json::json!({"kind": "planning"})), None);
    }

    #[test]
    fn a_suspended_template_is_not_projected() {
        let mut t = template(1, InfrastructureTemplateStatus::default());
        t.spec.suspend = true;
        assert!(template_row(&t, at(0)).is_none());
    }

    fn job(name: &str, created: i64, kind: &str) -> Job {
        Job {
            metadata: ObjectMeta {
                name: Some(name.into()),
                namespace: Some("default".into()),
                creation_timestamp: Some(Time(at(created))),
                owner_references: Some(vec![OwnerReference {
                    api_version: "batch/v1".into(),
                    kind: "CronJob".into(),
                    name: "vars".into(),
                    uid: "u".into(),
                    ..Default::default()
                }]),
                ..Default::default()
            },
            status: Some(JobStatus {
                conditions: Some(vec![JobCondition {
                    type_: kind.into(),
                    status: "True".into(),
                    ..Default::default()
                }]),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    #[test]
    fn cronjob_failures_count_back_to_the_newest_success() {
        let cj = CronJob {
            metadata: ObjectMeta {
                name: Some("vars".into()),
                namespace: Some("default".into()),
                ..Default::default()
            },
            ..Default::default()
        };
        let jobs = vec![
            job("a", 100, "Complete"),
            job("b", 200, "Failed"),
            job("c", 300, "Failed"),
            job("d", 400, "Failed"),
        ];
        let rows = cron_rows(&[cj.clone()], &jobs);
        assert_eq!(
            rows,
            vec![CronRow {
                namespace: "default".into(),
                cronjob: "vars".into(),
                last_success: Some(100),
                last_failure: Some(400),
                consecutive_failures: 3,
            }]
        );
        let mut recovered = jobs.clone();
        recovered.push(job("e", 500, "Complete"));
        assert_eq!(cron_rows(&[cj], &recovered)[0].consecutive_failures, 0);
    }

    #[test]
    fn the_gate_is_wedged_only_when_work_waits_and_nothing_reconciles() {
        let gate = ProgressGate::new(at(0));
        assert_eq!(gate.verdict(at(10_000), 1_200), Progress::Idle);
        gate.pending_since(Some(at(1_000)));
        gate.reconciled(at(9_500));
        assert_eq!(gate.verdict(at(10_000), 1_200), Progress::Moving);
        assert_eq!(
            gate.verdict(at(12_000), 1_200),
            Progress::Wedged {
                pending_seconds: 11_000,
                idle_seconds: 2_500
            }
        );
        assert_eq!(gate.verdict(at(12_000), 0), Progress::Moving);
        gate.reconciled(at(100));
        assert_eq!(gate.last_reconcile(), 9_500);
    }

    #[test]
    fn a_running_pass_with_a_live_heartbeat_counts_as_progress() {
        let row = |kind: &str, beat: i64| TemplateRow {
            template: "t".into(),
            namespace: "ns".into(),
            phase: "Applying".into(),
            phase_age_seconds: 0,
            generations_behind: 1,
            pending_since: Some(at(0)),
            conditions: vec![],
            pass_kind: Some(kind.into()),
            stall: None,
            heartbeat_age_seconds: Some(beat),
            in_flight: None,
            reactions: BTreeMap::new(),
        };
        let rows = vec![row("applying", 40), row("settled", 5), row("planning", 90)];
        assert_eq!(latest_running_heartbeat(&rows, at(5_000)), Some(at(4_960)));
        assert_eq!(
            latest_running_heartbeat(&[row("stalled", 5)], at(5_000)),
            None
        );
    }

    #[test]
    fn every_projection_reaches_the_registry() {
        let registry = Registry::new();
        let m = ProjectionMetrics::register(&registry);
        m.publish_templates(&[TemplateRow {
            template: "t".into(),
            namespace: "ns".into(),
            phase: "Planning".into(),
            phase_age_seconds: 60,
            generations_behind: 1,
            pending_since: None,
            conditions: vec![ConditionRow {
                kind: "Ready".into(),
                status: "True".into(),
                generations_behind: 1,
            }],
            pass_kind: Some("stalled".into()),
            stall: Some("heartbeatLost".into()),
            heartbeat_age_seconds: Some(300),
            in_flight: Some(("ApplyResourceChange".into(), 120)),
            reactions: BTreeMap::new(),
        }]);
        m.publish_containers(&[ContainerRow {
            namespace: "pangea-system".into(),
            pod: "p".into(),
            app: "pangea-postgres".into(),
            container: "postgres".into(),
            restarts: 34,
            ready: true,
        }]);
        let text = {
            use prometheus::Encoder;
            let mut buf = Vec::new();
            prometheus::TextEncoder::new()
                .encode(&registry.gather(), &mut buf)
                .unwrap();
            String::from_utf8(buf).unwrap()
        };
        for needle in [
            "pangea_template_generations_behind{namespace=\"ns\",template=\"t\"} 1",
            "pangea_template_condition_generations_behind{namespace=\"ns\",template=\"t\",type=\"Ready\"} 1",
            "pangea_template_stall{namespace=\"ns\",state=\"heartbeatLost\",template=\"t\"} 1",
            "pangea_template_in_flight_rpc_age_seconds{namespace=\"ns\",rpc=\"ApplyResourceChange\",template=\"t\"} 120",
            "pangea_stack_container_restarts{app=\"pangea-postgres\",container=\"postgres\",namespace=\"pangea-system\",pod=\"p\"} 34",
        ] {
            assert!(text.contains(needle), "missing {needle}");
        }
    }
}
