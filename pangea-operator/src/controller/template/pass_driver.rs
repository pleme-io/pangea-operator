use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration as StdDuration;

use chrono::Utc;
use kube::api::{Api, ListParams, Patch, PatchParams};
use kube::runtime::events::EventType;
use kube::ResourceExt;
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};

use super::events::record_event;
use super::pass::{project_status, projection_patch, stored_pass, Deadlines, PassPhase};
use super::pass_registry::{pass_key, PassRegistry};
use crate::controller::ControllerState;
use crate::crd::{InfrastructureTemplate, PassKind, PassStatus, Phase, StallState};

pub const HEARTBEAT_PERIOD: StdDuration = StdDuration::from_secs(30);
pub const WATCHDOG_TICK: StdDuration = StdDuration::from_secs(15);

pub async fn fresh_template(
    template: &InfrastructureTemplate,
    state: &ControllerState,
) -> Option<InfrastructureTemplate> {
    let api: Api<InfrastructureTemplate> = Api::namespaced(
        state.client.clone(),
        &template.namespace().unwrap_or_default(),
    );
    match api.get_opt(&template.name_any()).await {
        Ok(found) => found,
        Err(e) => {
            warn!(error = %e, "live template read failed; using the reconcile-start snapshot");
            None
        }
    }
}

pub async fn live_generation(template: &InfrastructureTemplate, state: &ControllerState) -> i64 {
    let snapshot = template.metadata.generation.unwrap_or(0);
    fresh_template(template, state)
        .await
        .and_then(|f| f.metadata.generation)
        .unwrap_or(snapshot)
        .max(snapshot)
}

pub fn requested_value(template: &InfrastructureTemplate) -> Option<&str> {
    if !crate::controller::generation_filter::honors_reconcile_requests() {
        return None;
    }
    template
        .metadata
        .annotations
        .as_ref()
        .and_then(|a| a.get(crate::controller::generation_filter::RECONCILE_REQUESTED_ANNOTATION))
        .map(String::as_str)
}

pub fn unhandled_request(template: &InfrastructureTemplate) -> Option<&str> {
    let handled = template
        .status
        .as_ref()
        .and_then(|s| s.last_reconcile_request_handled.as_deref());
    requested_value(template).filter(|v| Some(*v) != handled)
}

pub fn reconcile_request_pending(template: &InfrastructureTemplate) -> bool {
    unhandled_request(template).is_some()
}

pub fn settled_base(
    previous: &[crate::crd::Condition],
    legacy_ready: Vec<crate::crd::Condition>,
) -> Vec<crate::crd::Condition> {
    let owned: std::collections::BTreeSet<String> =
        legacy_ready.iter().map(|c| c.r#type.clone()).collect();
    let mut base = legacy_ready;
    base.extend(
        previous
            .iter()
            .filter(|c| !owned.contains(&c.r#type) && c.r#type != "Stalled")
            .cloned(),
    );
    base
}

pub async fn finish_settled_pass(template: &InfrastructureTemplate, state: &ControllerState) {
    let now = Utc::now();
    let generation = template.metadata.generation.unwrap_or(0);
    let fresh = fresh_template(template, state).await;
    let live = fresh
        .as_ref()
        .and_then(|f| f.metadata.generation)
        .unwrap_or(generation)
        .max(generation);
    let base = fresh
        .as_ref()
        .and_then(|f| f.status.clone())
        .or_else(|| template.status.clone())
        .unwrap_or_default();
    let queued_since = base
        .pass
        .as_ref()
        .map(|p| PassPhase::from_status(p, now))
        .and_then(|p| p.queued().map(|q| q.since));
    let mut pass = PassPhase::Settled { generation };
    if live > generation {
        pass = PassPhase::Pending {
            generation: live,
            since: queued_since.unwrap_or(now),
        };
    }
    let observed = PassPhase::Settled { generation }.observed_generation(base.observed_generation);
    let legacy = crate::controller::reconciler::conditions_for_phase(
        Phase::Ready,
        None,
        super::status::source_fresh_state(template),
    );
    let conditions = super::pass::ConditionSet::project(
        settled_base(&base.conditions, legacy),
        &pass,
        observed,
        live,
        now,
    );
    let conditions = crate::controller::status::merge_condition_transitions(
        &base.conditions,
        conditions.into_vec(),
    );
    let mut status = serde_json::json!({
        "pass": pass.to_status(),
        "observedGeneration": observed,
        "conditions": conditions,
        "generationLagSince": pass.lag_since(),
    });
    if let Some(v) = unhandled_request(template) {
        status["lastReconcileRequestHandled"] = serde_json::json!(v);
    }
    let patch = serde_json::json!({ "status": status });
    if let Err(e) =
        crate::controller::status_patch::patch_status(template, &state.client, patch).await
    {
        warn!(error = %e, "could not record the completed pass");
    }
}

pub struct PassGuard {
    beat: JoinHandle<()>,
}

impl Drop for PassGuard {
    fn drop(&mut self) {
        self.beat.abort();
    }
}

impl PassGuard {
    pub async fn begin(
        template: &InfrastructureTemplate,
        state: &ControllerState,
        kind: Phase,
    ) -> Self {
        let now = Utc::now();
        let namespace = template.namespace().unwrap_or_default();
        let name = template.name_any();
        let previous = template.status.as_ref().map(|s| stored_pass(s, now));
        let generation = template.metadata.generation.unwrap_or(0);
        let live = live_generation(template, state).await;
        let pass =
            PassPhase::entering(kind, generation, None, previous.as_ref(), now).observe(live, now);
        let patch = serde_json::json!({
            "status": {
                "pass": pass.to_status(),
                "generationLagSince": pass.lag_since(),
            }
        });
        let api: Api<InfrastructureTemplate> = Api::namespaced(state.client.clone(), &namespace);
        if let Err(e) = api
            .patch_status(
                &name,
                &PatchParams::apply("pangea-operator"),
                &Patch::Merge(&patch),
            )
            .await
        {
            warn!(error = %e, %name, "could not record the pass start in status.pass");
        } else {
            state.passes.beat_landed(&pass_key(&namespace, &name), now);
        }
        let key = pass_key(&namespace, &name);
        let passes = Arc::clone(&state.passes);
        let beat_api = api.clone();
        let beat = tokio::spawn(async move {
            let mut tick = tokio::time::interval(HEARTBEAT_PERIOD);
            tick.tick().await;
            loop {
                tick.tick().await;
                let now = Utc::now();
                if beat_once(&beat_api, &name, &pass, now, resumes_on_heartbeat()).await {
                    passes.beat_landed(&key, now);
                }
            }
        });
        Self { beat }
    }
}

static RESUME_ON_HEARTBEAT: AtomicBool = AtomicBool::new(true);
static PREEMPT_STALLED_PASS: AtomicBool = AtomicBool::new(true);

pub fn set_resume_on_heartbeat(on: bool) {
    RESUME_ON_HEARTBEAT.store(on, Ordering::Relaxed);
}

#[must_use]
pub fn resumes_on_heartbeat() -> bool {
    RESUME_ON_HEARTBEAT.load(Ordering::Relaxed)
}

pub fn set_preempt_stalled_pass(on: bool) {
    PREEMPT_STALLED_PASS.store(on, Ordering::Relaxed);
}

#[must_use]
pub fn preempts_stalled_pass() -> bool {
    PREEMPT_STALLED_PASS.load(Ordering::Relaxed)
}

#[must_use]
pub fn withdraws_heartbeat_lost(stored: Option<&PassStatus>) -> bool {
    stored
        .is_some_and(|p| p.kind == PassKind::Stalled && p.stall == Some(StallState::HeartbeatLost))
}

pub async fn beat_once(
    api: &Api<InfrastructureTemplate>,
    name: &str,
    owned: &PassPhase,
    now: chrono::DateTime<Utc>,
    resume: bool,
) -> bool {
    let patch = serde_json::json!({
        "status": { "pass": { "heartbeatAt": now } }
    });
    let stored = match api
        .patch_status(
            name,
            &PatchParams::apply("pangea-operator"),
            &Patch::Merge(&patch),
        )
        .await
    {
        Ok(stored) => stored,
        Err(e) => {
            debug!(error = %e, %name, "pass heartbeat write failed");
            return false;
        }
    };
    let stored_pass = stored.status.as_ref().and_then(|s| s.pass.as_ref());
    if resume && withdraws_heartbeat_lost(stored_pass) {
        let patch = serde_json::json!({
            "metadata": { "resourceVersion": stored.resource_version() },
            "status": { "pass": owned.clone().beat(now).to_status() }
        });
        match api
            .patch_status(
                name,
                &PatchParams::apply("pangea-operator"),
                &Patch::Merge(&patch),
            )
            .await
        {
            Ok(_) => {
                info!(%name, kind = %owned.kind(), "pass heartbeat landed again; the heartbeatLost stall is withdrawn")
            }
            Err(e) => {
                debug!(error = %e, %name, "heartbeatLost withdrawal not written; the next beat retries")
            }
        }
    }
    true
}

pub async fn redispatch(
    api: &Api<InfrastructureTemplate>,
    passes: &PassRegistry,
    namespace: &str,
    name: &str,
    stall: StallState,
    now: chrono::DateTime<Utc>,
    heartbeat: chrono::Duration,
    preempt: bool,
) -> kube::Result<bool> {
    let preempted = preempt && passes.preempt(&pass_key(namespace, name), stall, now, heartbeat);
    if preempted {
        warn!(%name, %namespace, state = %stall, "stalled pass: preempting the in-process pass so the re-dispatch starts a fresh one");
    }
    let patch = serde_json::json!({
        "metadata": { "annotations": {
            crate::controller::generation_filter::RECONCILE_REQUESTED_ANNOTATION:
                format!("watchdog-{}", now.timestamp_millis())
        }}
    });
    api.patch(name, &PatchParams::default(), &Patch::Merge(&patch))
        .await?;
    Ok(preempted)
}

pub const MAX_NUDGES: u32 = 5;

pub fn nudge_backoff(attempts: u32) -> chrono::Duration {
    chrono::Duration::seconds((60i64 << attempts.min(5)).min(1800))
}

pub fn nudge_due(
    attempts: u32,
    last: Option<chrono::DateTime<Utc>>,
    now: chrono::DateTime<Utc>,
) -> bool {
    attempts < MAX_NUDGES && last.map_or(true, |l| now - l >= nudge_backoff(attempts))
}

#[derive(Default)]
pub struct Nudges(std::collections::HashMap<String, (u32, chrono::DateTime<Utc>)>);

pub async fn run_watchdog(state: Arc<ControllerState>) {
    let deadlines = Deadlines::from_env();
    info!(
        heartbeat_secs = deadlines.heartbeat.num_seconds(),
        pending_start_secs = deadlines.pending_start.num_seconds(),
        "template pass watchdog running"
    );
    let api: Api<InfrastructureTemplate> = Api::all(state.client.clone());
    let mut tick = tokio::time::interval(WATCHDOG_TICK);
    let mut nudges = Nudges::default();
    loop {
        tick.tick().await;
        let templates = match api.list(&ListParams::default()).await {
            Ok(list) => list.items,
            Err(e) => {
                warn!(error = %e, "pass watchdog could not list templates");
                continue;
            }
        };
        for template in templates {
            watch_one(&template, &state, &deadlines, &mut nudges).await;
        }
    }
}

async fn watch_one(
    template: &InfrastructureTemplate,
    state: &ControllerState,
    d: &Deadlines,
    nudges: &mut Nudges,
) {
    if template.metadata.deletion_timestamp.is_some() || template.spec.suspend {
        return;
    }
    let Some(status) = template.status.as_ref() else {
        return;
    };
    let now = Utc::now();
    let live = template.metadata.generation.unwrap_or(0);
    let projection = project_status(status, live, now, d);
    let name = template.name_any();
    let namespace = template.namespace().unwrap_or_default();
    let labels = [name.as_str(), namespace.as_str()];
    let was_stalled = status
        .pass
        .as_ref()
        .is_some_and(|p| p.kind == PassKind::Stalled);
    let is_stalled = projection.pass.kind() == PassKind::Stalled;
    state
        .metrics
        .live
        .stalled
        .with_label_values(&labels)
        .set(i64::from(is_stalled));
    state
        .metrics
        .live
        .generation_lag_seconds
        .with_label_values(&labels)
        .set(
            projection
                .lag_since
                .map(|s| (now - s).num_seconds().max(0))
                .unwrap_or(0),
        );
    let key = format!("{namespace}/{name}");
    let stall = match &projection.pass {
        PassPhase::Stalled { state, reason, .. } => Some((*state, reason.clone())),
        _ => None,
    };
    if stall.is_none() {
        nudges.0.remove(&key);
    }
    let api: Api<InfrastructureTemplate> = Api::namespaced(state.client.clone(), &namespace);
    if let Some(mut patch) = projection_patch(status, &projection) {
        if let (Some((kind, _)), true) = (&stall, !was_stalled) {
            patch["status"]["reactions"] = ledger_with(status, *kind, now);
        }
        if let Err(e) = api
            .patch_status(
                &name,
                &PatchParams::apply("pangea-operator"),
                &Patch::Merge(&patch),
            )
            .await
        {
            warn!(error = %e, %name, "pass watchdog status write failed");
            return;
        }
    }
    let Some((kind, reason)) = stall else {
        return;
    };
    if !was_stalled {
        warn!(%name, %namespace, state = %kind, %reason, "template pass stalled");
        let label = kind.to_string();
        state
            .metrics
            .live
            .stalls_total
            .with_label_values(&[name.as_str(), namespace.as_str(), label.as_str()])
            .inc();
        record_event(
            template,
            state,
            EventType::Warning,
            &format!("Stalled{}", capitalize(&label)),
            &format!("{reason}. Remedy: {}.", kind.remedy()),
        )
        .await;
    }
    if !kind.redispatches() {
        return;
    }
    let (attempts, last) = nudges
        .0
        .get(&key)
        .map(|(a, l)| (*a, Some(*l)))
        .unwrap_or((0, None));
    if nudge_due(attempts, last, now) {
        match redispatch(
            &api,
            &state.passes,
            &namespace,
            &name,
            kind,
            now,
            d.heartbeat,
            preempts_stalled_pass(),
        )
        .await
        {
            Ok(preempted) => {
                info!(%name, attempt = attempts + 1, state = %kind, preempted, "stalled pass: re-dispatching the reconcile");
                nudges.0.insert(key, (attempts + 1, now));
            }
            Err(e) => warn!(error = %e, %name, "stalled pass: re-dispatch failed"),
        }
    } else if attempts == MAX_NUDGES {
        nudges
            .0
            .insert(key, (attempts, now + chrono::Duration::days(3650)));
        record_event(
            template,
            state,
            EventType::Warning,
            "StallEscalated",
            &format!(
                "{reason}. {MAX_NUDGES} re-dispatches did not move it; a human or the reactive policy must act."
            ),
        )
        .await;
    }
}

fn capitalize(s: &str) -> String {
    let mut c = s.chars();
    match c.next() {
        Some(f) => f.to_uppercase().collect::<String>() + c.as_str(),
        None => String::new(),
    }
}

fn ledger_with(
    status: &crate::crd::InfrastructureTemplateStatus,
    kind: crate::crd::StallState,
    now: chrono::DateTime<Utc>,
) -> serde_json::Value {
    let mut ledger = status.reactions.clone().unwrap_or_default();
    let trigger = format!("Stalled:{kind}");
    *ledger.counts.entry(trigger.clone()).or_insert(0) += 1;
    ledger.active_since.insert(trigger.clone(), now);
    ledger.last = Some(crate::crd::ReactionRecord {
        trigger,
        at: now,
        age_seconds: 0,
        notified: false,
    });
    serde_json::json!(ledger)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::controller::generation_filter::{
        set_honor_reconcile_requests, RECONCILE_REQUESTED_ANNOTATION,
    };
    use std::sync::Mutex;

    static FLAG: Mutex<()> = Mutex::new(());

    fn template(requested: Option<&str>, handled: Option<&str>) -> InfrastructureTemplate {
        let mut annotations = serde_json::Map::new();
        if let Some(r) = requested {
            annotations.insert(RECONCILE_REQUESTED_ANNOTATION.into(), r.into());
        }
        let mut status = serde_json::Map::new();
        if let Some(h) = handled {
            status.insert("lastReconcileRequestHandled".into(), h.into());
        }
        serde_json::from_value(serde_json::json!({
            "apiVersion": "pangea.pleme.io/v1alpha1",
            "kind": "InfrastructureTemplate",
            "metadata": { "name": "t", "namespace": "default", "annotations": annotations },
            "spec": { "pangeaNamespace": "n", "source": { "inline": "" } },
            "status": status,
        }))
        .expect("a minimal InfrastructureTemplate parses")
    }

    #[test]
    fn a_changed_request_annotation_is_pending_until_recorded_handled() {
        let _g = FLAG.lock().unwrap_or_else(|p| p.into_inner());
        set_honor_reconcile_requests(true);
        assert!(reconcile_request_pending(&template(Some("2"), None)));
        assert!(reconcile_request_pending(&template(Some("2"), Some("1"))));
        assert!(!reconcile_request_pending(&template(Some("2"), Some("2"))));
        assert!(!reconcile_request_pending(&template(None, Some("2"))));
        assert_eq!(
            unhandled_request(&template(Some("2"), Some("1"))),
            Some("2")
        );
        set_honor_reconcile_requests(false);
    }

    #[test]
    fn the_request_annotation_is_inert_while_not_honored() {
        let _g = FLAG.lock().unwrap_or_else(|p| p.into_inner());
        set_honor_reconcile_requests(false);
        assert!(!reconcile_request_pending(&template(Some("2"), None)));
        assert_eq!(requested_value(&template(Some("2"), None)), None);
    }

    #[test]
    fn spec_variables_are_part_of_the_material_spec_digest() {
        let digest = |vars: serde_json::Value| {
            let t: InfrastructureTemplate = serde_json::from_value(serde_json::json!({
                "apiVersion": "pangea.pleme.io/v1alpha1",
                "kind": "InfrastructureTemplate",
                "metadata": { "name": "t", "namespace": "default" },
                "spec": {
                    "pangeaNamespace": "n",
                    "source": { "inline": "" },
                    "variables": vars,
                },
            }))
            .unwrap();
            crate::controller::template_controller::material_spec_digest(&t.spec)
        };
        let a = digest(serde_json::json!({ "org": "a" }));
        assert_eq!(a, digest(serde_json::json!({ "org": "a" })));
        assert_ne!(a, digest(serde_json::json!({ "org": "b" })));
    }

    #[test]
    fn the_crd_prints_phase_generation_observed_and_age_in_phase() {
        use kube::CustomResourceExt;
        let crd = InfrastructureTemplate::crd();
        let columns: Vec<(String, String)> = crd.spec.versions[0]
            .additional_printer_columns
            .clone()
            .unwrap_or_default()
            .into_iter()
            .map(|c| (c.name, c.json_path))
            .collect();
        for (name, path) in [
            ("Phase", ".status.phase"),
            ("Generation", ".metadata.generation"),
            ("Observed", ".status.observedGeneration"),
            ("Age-In-Phase", ".status.phaseEnteredAt"),
        ] {
            assert!(
                columns.iter().any(|(n, p)| n == name && p == path),
                "missing printer column {name} at {path}: {columns:?}"
            );
        }
    }

    #[test]
    fn the_crd_schema_carries_pass_acknowledged_and_condition_observed_generation() {
        use kube::CustomResourceExt;
        let crd = serde_json::to_value(InfrastructureTemplate::crd()).unwrap();
        let status = &crd["spec"]["versions"][0]["schema"]["openAPIV3Schema"]["properties"]
            ["status"]["properties"];
        for field in [
            "pass",
            "acknowledgedGeneration",
            "observedGeneration",
            "lastReconcileRequestHandled",
            "generationLagSince",
        ] {
            assert!(status.get(field).is_some(), "status.{field} missing");
        }
        let cond = &status["conditions"]["items"]["properties"];
        assert!(cond.get("observedGeneration").is_some());
    }

    #[test]
    fn nudges_back_off_and_stop_at_the_cap() {
        let t0 = Utc::now();
        assert!(nudge_due(0, None, t0));
        assert!(!nudge_due(1, Some(t0), t0 + chrono::Duration::seconds(60)));
        assert!(nudge_due(1, Some(t0), t0 + chrono::Duration::seconds(120)));
        assert_eq!(nudge_backoff(0), chrono::Duration::seconds(60));
        assert_eq!(nudge_backoff(4), chrono::Duration::seconds(960));
        assert_eq!(nudge_backoff(30), chrono::Duration::seconds(1800));
        assert!(!nudge_due(
            MAX_NUDGES,
            Some(t0),
            t0 + chrono::Duration::days(1)
        ));
    }

    #[test]
    fn every_named_stall_has_a_remedy_and_only_the_redispatchable_are_nudged() {
        use crate::crd::StallState;
        for s in StallState::ALL {
            assert!(!s.remedy().is_empty());
        }
        assert!(!StallState::QueuedBehind.redispatches());
        assert!(!StallState::Unnamed.redispatches());
        assert!(StallState::OrphanedPhase.redispatches());
        assert!(StallState::HeartbeatLost.redispatches());
    }

    mod fake_api {
        use super::*;
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, Request, ResponseTemplate};

        pub const TEMPLATE_PATH: &str =
            "/apis/pangea.pleme.io/v1alpha1/namespaces/default/infrastructuretemplates/t";

        pub fn body(pass: serde_json::Value) -> serde_json::Value {
            serde_json::json!({
                "apiVersion": "pangea.pleme.io/v1alpha1",
                "kind": "InfrastructureTemplate",
                "metadata": { "name": "t", "namespace": "default", "resourceVersion": "7", "generation": 28 },
                "spec": { "pangeaNamespace": "n", "source": { "inline": "" } },
                "status": { "pass": pass },
            })
        }

        pub async fn server(stored_pass: serde_json::Value) -> MockServer {
            let server = MockServer::start().await;
            let reply = body(stored_pass);
            Mock::given(method("PATCH"))
                .and(path(format!("{TEMPLATE_PATH}/status")))
                .respond_with(ResponseTemplate::new(200).set_body_json(reply.clone()))
                .mount(&server)
                .await;
            Mock::given(method("PATCH"))
                .and(path(TEMPLATE_PATH))
                .respond_with(ResponseTemplate::new(200).set_body_json(reply))
                .mount(&server)
                .await;
            server
        }

        pub fn api(server: &MockServer) -> Api<InfrastructureTemplate> {
            let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
            let config = kube::Config::new(server.uri().parse().expect("a mock uri"));
            let client = kube::Client::try_from(config).expect("a client for the mock");
            Api::namespaced(client, "default")
        }

        pub async fn patches(server: &MockServer, suffix: &str) -> Vec<serde_json::Value> {
            let want = format!("{TEMPLATE_PATH}{suffix}");
            server
                .received_requests()
                .await
                .unwrap_or_default()
                .iter()
                .filter(|r: &&Request| r.method.as_str() == "PATCH" && r.url.path() == want)
                .map(|r| serde_json::from_slice(&r.body).expect("a json patch body"))
                .collect()
        }
    }

    fn stalled_wire(stall: &str) -> serde_json::Value {
        serde_json::json!({
            "kind": "stalled",
            "generation": 28,
            "since": "2026-10-27T12:05:20.663013Z",
            "stall": stall,
            "reason": "Planning pass for generation 28 started 2026-10-27T11:55:39.523447+00:00 has no heartbeat for 310s",
            "heartbeatAt": "2026-10-27T12:17:44.539569Z",
            "incarnation": "operator-1793096482",
        })
    }

    fn owned_planning() -> PassPhase {
        let started = chrono::DateTime::parse_from_rfc3339("2026-10-27T11:55:39.523447Z")
            .unwrap()
            .with_timezone(&Utc);
        PassPhase::Planning {
            generation: 28,
            started_at: started,
            heartbeat: started,
            queued: None,
        }
    }

    #[tokio::test]
    async fn receipt_2026_10_27_a_landed_beat_withdraws_the_heartbeat_lost_stall_of_its_own_pass() {
        let server = fake_api::server(stalled_wire("heartbeatLost")).await;
        let api = fake_api::api(&server);
        let now = Utc::now();
        assert!(beat_once(&api, "t", &owned_planning(), now, true).await);
        let writes = fake_api::patches(&server, "/status").await;
        assert_eq!(
            writes.len(),
            2,
            "heartbeat, then the withdrawal: {writes:?}"
        );
        let restore = &writes[1];
        assert_eq!(restore["metadata"]["resourceVersion"], "7");
        assert_eq!(restore["status"]["pass"]["kind"], "planning");
        assert_eq!(restore["status"]["pass"]["generation"], 28);
        assert_eq!(
            restore["status"]["pass"]["since"],
            serde_json::json!(owned_planning().to_status().since)
        );
        assert!(restore["status"]["pass"]["stall"].is_null());
        assert!(restore["status"]["pass"]["reason"].is_null());
    }

    #[tokio::test]
    async fn a_beat_never_withdraws_a_stall_it_does_not_disprove() {
        for stall in ["noProgress", "queuedBehind", "orphanedPhase", "unnamed"] {
            let server = fake_api::server(stalled_wire(stall)).await;
            let api = fake_api::api(&server);
            assert!(beat_once(&api, "t", &owned_planning(), Utc::now(), true).await);
            assert_eq!(
                fake_api::patches(&server, "/status").await.len(),
                1,
                "{stall}"
            );
        }
    }

    #[tokio::test]
    async fn resume_off_leaves_the_stall_in_place() {
        let server = fake_api::server(stalled_wire("heartbeatLost")).await;
        let api = fake_api::api(&server);
        assert!(beat_once(&api, "t", &owned_planning(), Utc::now(), false).await);
        assert_eq!(fake_api::patches(&server, "/status").await.len(), 1);
    }

    async fn hung_pass(
        passes: &Arc<PassRegistry>,
        budgets: &Arc<crate::controller::scheduling::FairBudget>,
    ) -> tokio::task::JoinHandle<Option<()>> {
        let runner = Arc::clone(passes);
        let held = Arc::clone(budgets);
        let pass = tokio::spawn(async move {
            runner
                .run(pass_key("default", "t"), async move {
                    let _permit = held.try_acquire("ws").expect("a free slot");
                    futures::future::pending::<()>().await;
                })
                .await
        });
        while !passes.holds("default/t") || budgets.in_flight("ws") == 0 {
            tokio::task::yield_now().await;
        }
        pass
    }

    fn one_slot() -> Arc<crate::controller::scheduling::FairBudget> {
        crate::controller::scheduling::FairBudget::new(
            crate::controller::scheduling::BudgetConfig {
                per_scope: 1,
                global: 1,
            },
        )
    }

    #[tokio::test]
    async fn receipt_2026_10_27_redispatch_preempts_the_hung_pass_so_the_nudge_starts_a_fresh_one()
    {
        let server = fake_api::server(stalled_wire("heartbeatLost")).await;
        let api = fake_api::api(&server);
        let passes = Arc::new(PassRegistry::default());
        let budgets = one_slot();
        let pass = hung_pass(&passes, &budgets).await;
        let now = Utc::now() + chrono::Duration::minutes(10);
        let preempted = redispatch(
            &api,
            &passes,
            "default",
            "t",
            StallState::HeartbeatLost,
            now,
            chrono::Duration::minutes(5),
            true,
        )
        .await
        .expect("the nudge lands");
        assert!(preempted);
        let out = tokio::time::timeout(StdDuration::from_secs(5), pass)
            .await
            .expect("the hung pass returns once preempted")
            .expect("the task joins");
        assert_eq!(out, None);
        assert_eq!(budgets.in_flight("ws"), 0, "the budget slot is released");
        assert!(!passes.holds("default/t"), "the registry entry is released");
        let nudges = fake_api::patches(&server, "").await;
        assert_eq!(nudges.len(), 1);
        assert!(
            nudges[0]["metadata"]["annotations"][RECONCILE_REQUESTED_ANNOTATION]
                .as_str()
                .is_some_and(|v| v.starts_with("watchdog-"))
        );
    }

    #[tokio::test]
    async fn preempt_off_only_nudges() {
        let server = fake_api::server(stalled_wire("heartbeatLost")).await;
        let api = fake_api::api(&server);
        let passes = Arc::new(PassRegistry::default());
        let budgets = one_slot();
        let pass = hung_pass(&passes, &budgets).await;
        let now = Utc::now() + chrono::Duration::minutes(10);
        let preempted = redispatch(
            &api,
            &passes,
            "default",
            "t",
            StallState::HeartbeatLost,
            now,
            chrono::Duration::minutes(5),
            false,
        )
        .await
        .expect("the nudge lands");
        assert!(!preempted);
        assert!(passes.holds("default/t"));
        assert_eq!(budgets.in_flight("ws"), 1);
        assert_eq!(fake_api::patches(&server, "").await.len(), 1);
        pass.abort();
    }

    #[test]
    fn the_stall_switches_default_on() {
        assert!(resumes_on_heartbeat());
        assert!(preempts_stalled_pass());
        assert!(withdraws_heartbeat_lost(Some(
            &PassPhase::Stalled {
                generation: 1,
                since: Utc::now(),
                state: StallState::HeartbeatLost,
                reason: String::new(),
            }
            .to_status()
        )));
        assert!(!withdraws_heartbeat_lost(Some(
            &owned_planning().to_status()
        )));
        assert!(!withdraws_heartbeat_lost(None));
    }

    #[test]
    fn the_ledger_counts_each_named_stall_apart() {
        let status = crate::crd::InfrastructureTemplateStatus::default();
        let l = ledger_with(&status, crate::crd::StallState::OrphanedPhase, Utc::now());
        assert_eq!(l["counts"]["Stalled:orphanedPhase"], 1);
        assert_eq!(l["last"]["trigger"], "Stalled:orphanedPhase");
    }
}
