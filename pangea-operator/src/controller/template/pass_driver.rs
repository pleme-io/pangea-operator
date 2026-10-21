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
use crate::controller::ControllerState;
use crate::crd::{InfrastructureTemplate, PassKind, Phase};

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
    let conditions =
        super::pass::ConditionSet::project(base.conditions.clone(), &pass, observed, live, now);
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
        }
        let beat_api = api.clone();
        let beat = tokio::spawn(async move {
            let mut tick = tokio::time::interval(HEARTBEAT_PERIOD);
            tick.tick().await;
            loop {
                tick.tick().await;
                let patch = serde_json::json!({
                    "status": { "pass": { "heartbeatAt": Utc::now() } }
                });
                if let Err(e) = beat_api
                    .patch_status(
                        &name,
                        &PatchParams::apply("pangea-operator"),
                        &Patch::Merge(&patch),
                    )
                    .await
                {
                    debug!(error = %e, %name, "pass heartbeat write failed");
                }
            }
        });
        Self { beat }
    }
}

pub async fn run_watchdog(state: Arc<ControllerState>) {
    let deadlines = Deadlines::from_env();
    info!(
        heartbeat_secs = deadlines.heartbeat.num_seconds(),
        pending_start_secs = deadlines.pending_start.num_seconds(),
        "template pass watchdog running"
    );
    let api: Api<InfrastructureTemplate> = Api::all(state.client.clone());
    let mut tick = tokio::time::interval(WATCHDOG_TICK);
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
            watch_one(&template, &state, &deadlines).await;
        }
    }
}

async fn watch_one(template: &InfrastructureTemplate, state: &ControllerState, d: &Deadlines) {
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
    let Some(patch) = projection_patch(status, &projection) else {
        return;
    };
    let api: Api<InfrastructureTemplate> = Api::namespaced(state.client.clone(), &namespace);
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
    if is_stalled && !was_stalled {
        let reason = match &projection.pass {
            PassPhase::Stalled { reason, .. } => reason.clone(),
            _ => String::new(),
        };
        warn!(%name, %namespace, %reason, "template pass stalled");
        state
            .metrics
            .live
            .stalls_total
            .with_label_values(&labels)
            .inc();
        record_event(template, state, EventType::Warning, "Stalled", &reason).await;
    }
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
}
