//! PangeaDashboard controller — synthesizes Grafana dashboards from Ruby DSL.
//!
//! Reconciliation loop:
//! 1. Read PangeaDashboard spec (inline Ruby or ConfigMap ref).
//! 2. Render the Ruby through the embedded magnus backend → Grafana JSON.
//! 3. Diff-gate on `sourceHash`: if unchanged AND the ConfigMap already
//!    exists, skip (no churn).
//! 4. Upsert a sidecar-labelled ConfigMap carrying the dashboard JSON,
//!    owned by the PangeaDashboard (ownerReference → GC on CR delete).
//! 5. Patch status (phase, configMapName, dashboardUid, sourceHash,
//!    observedGeneration, Ready condition) through a `*_status_needs_patch`
//!    diff-gate per the operator's status-write-loop discipline.
//!
//! Delivery target: rio runs the victoria-metrics-k8s-stack bundled
//! Grafana with the dashboard SIDECAR (`label grafana_dashboard="1"`,
//! `searchNamespace: ALL`) — NOT grafana-operator. So the controller's
//! delivery surface is the sidecar-labelled ConfigMap; it does NOT emit a
//! grafana-operator `GrafanaDashboard` CR. The `DashboardDelivery` enum
//! below is the typed seam where a future grafana-operator target would
//! slot in (config-selectable per the design doc) — `SidecarConfigMap` is
//! the default and only implemented arm.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use k8s_openapi::api::core::v1::ConfigMap;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference;
use kube::{
    api::{Api, Patch, PatchParams},
    runtime::controller::Action,
    ResourceExt,
};
use tracing::{debug, error, info, instrument, warn};

use crate::crd::{Condition, DashboardSource, PangeaDashboard, PangeaDashboardPhase};
use crate::error::Error;

use super::{create_condition, ControllerState, DEFAULT_REQUEUE_INTERVAL, SHORT_REQUEUE_INTERVAL};

/// How a rendered dashboard is delivered to Grafana. Typed seam so a
/// future grafana-operator target is a config-selectable arm, never a
/// fork of the controller. `SidecarConfigMap` is the default + the only
/// implemented path (rio runs the vm-stack Grafana dashboard sidecar,
/// not grafana-operator).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DashboardDelivery {
    /// Sidecar-labelled ConfigMap (`grafana_dashboard="1"`). The vm-stack
    /// Grafana sidecar (`searchNamespace: ALL`) loads it.
    SidecarConfigMap,
}

impl DashboardDelivery {
    /// The only delivery path implemented today. A future
    /// grafana-operator arm would be selected from `spec.delivery` here.
    fn for_dashboard(_pd: &PangeaDashboard) -> Self {
        DashboardDelivery::SidecarConfigMap
    }
}

/// Controller for PangeaDashboard resources.
pub struct DashboardController {
    state: ControllerState,
}

impl DashboardController {
    pub fn new(state: ControllerState) -> Self {
        Self { state }
    }

    /// Start the PangeaDashboard controller.
    pub async fn run(self) -> crate::error::Result<()> {
        let client = self.state.client.clone();
        let state = Arc::new(self.state);

        info!("Starting PangeaDashboard controller");

        crate::controller::generation_filter::filtered_controller::<PangeaDashboard>(client)
            .run(
                move |pd, ctx| {
                    let state = Arc::clone(&ctx);
                    async move { reconcile(pd, state).await }
                },
                error_policy,
                state,
            )
            .for_each(|result| async move {
                match result {
                    Ok((obj, action)) => {
                        debug!(name = %obj.name, ?action, "PangeaDashboard reconciliation completed");
                    }
                    Err(e) => {
                        error!(error = %e, "PangeaDashboard reconciliation failed");
                    }
                }
            })
            .await;

        Ok(())
    }
}

fn error_policy(
    _pd: Arc<PangeaDashboard>,
    error: &Error,
    ctx: Arc<ControllerState>,
) -> Action {
    crate::controller::error_policy::run_error_policy(
        &ctx.metrics,
        crate::crd::ControllerKind::Dashboard,
        error,
        Duration::from_secs(30),
    )
}

#[instrument(skip_all, fields(name = %pd.name_any()))]
async fn reconcile(
    pd: Arc<PangeaDashboard>,
    state: Arc<ControllerState>,
) -> Result<Action, Error> {
    state
        .metrics
        .record_reconcile(crate::crd::ControllerKind::Dashboard, "ok");
    let name = pd.name_any();
    let namespace = pd.namespace().unwrap_or_else(|| "default".to_string());

    info!(%namespace, "Reconciling PangeaDashboard");

    // Cluster-wide kill-switch — honor `OperatorPolicy/default`.
    if let Some(action) = crate::controller::policy_pipeline::run_for_controller(
        &state,
        crate::crd::ControllerKind::Dashboard,
    )
    .into_skip_action()
    {
        return Ok(action);
    }

    // Per-CR suspend gate (the cluster-scoped policy is checked above).
    if pd.spec.suspend {
        info!(%name, %namespace, "PangeaDashboard suspended; skipping reconcile");
        return Ok(Action::requeue(Duration::from_secs(30)));
    }

    // Extract Ruby source.
    let ruby_source = match &pd.spec.source {
        DashboardSource::Inline { ruby } => ruby.clone(),
        DashboardSource::ConfigMapRef {
            name: cm_name, key, ..
        } => {
            let cm_api: Api<ConfigMap> = Api::namespaced(state.client.clone(), &namespace);
            match cm_api.get(cm_name).await {
                Ok(cm) => cm
                    .data
                    .and_then(|d| d.get(key).cloned())
                    .unwrap_or_default(),
                Err(e) => {
                    warn!(%name, error = %e, "Failed to read source ConfigMap");
                    return Ok(Action::requeue(Duration::from_secs(30)));
                }
            }
        }
    };

    let source_hash = source_hash_hex(&ruby_source);
    let cm_name = config_map_name(&name);
    let folder = pd.spec.folder.clone().unwrap_or_else(|| "General".to_string());

    // Diff-gate (a): if sourceHash matches the last successful render AND
    // the rendered ConfigMap is still present, the dashboard is already
    // converged — skip render + write + status churn. A missing CM (an
    // out-of-band delete) forces a re-render even on a hash match.
    let prev_hash = pd.status.as_ref().and_then(|s| s.source_hash.as_deref());
    if prev_hash == Some(source_hash.as_str()) {
        let cm_api: Api<ConfigMap> = Api::namespaced(state.client.clone(), &namespace);
        if cm_api.get_opt(&cm_name).await.map_err(Error::Kube)?.is_some() {
            debug!(%name, "sourceHash unchanged + ConfigMap present; skipping render");
            return Ok(Action::requeue(DEFAULT_REQUEUE_INTERVAL));
        }
    }

    // (a) Render the inline Ruby → Grafana dashboard JSON string.
    let dashboard_json = match state
        .compiler_backend
        .render_dashboard(ruby_source.clone(), pd.spec.extend_modules.clone())
        .await
    {
        Ok(j) => j,
        Err(e) => {
            let msg = format!("dashboard render failed: {e}");
            warn!(%name, error = %msg, "PangeaDashboard render failed");
            patch_failed(&pd, &state, &msg).await?;
            return Ok(Action::requeue(SHORT_REQUEUE_INTERVAL));
        }
    };

    // (d) Extract the dashboard UID from the rendered JSON for status.
    let dashboard_uid = extract_dashboard_uid(&dashboard_json);

    // (c) Upsert the sidecar-labelled ConfigMap, owned by the CR.
    match DashboardDelivery::for_dashboard(&pd) {
        DashboardDelivery::SidecarConfigMap => {
            if let Err(e) = upsert_dashboard_config_map(
                &pd,
                &namespace,
                &cm_name,
                &name,
                &folder,
                &dashboard_json,
                &state,
            )
            .await
            {
                let msg = format!("ConfigMap upsert failed: {e}");
                warn!(%name, error = %msg, "PangeaDashboard delivery failed");
                patch_failed(&pd, &state, &msg).await?;
                return Ok(Action::requeue(SHORT_REQUEUE_INTERVAL));
            }
        }
    }

    // (e) Patch status (Ready) through the diff-gate.
    let new_observed_gen = pd.metadata.generation.unwrap_or(0);
    let conditions = vec![create_condition(
        "Ready",
        true,
        "Synced",
        "Dashboard rendered and delivered to the Grafana sidecar ConfigMap",
    )];

    let needs_patch = dashboard_status_needs_patch(
        pd.status.as_ref(),
        PangeaDashboardPhase::Ready,
        Some(cm_name.as_str()),
        dashboard_uid.as_deref(),
        Some(source_hash.as_str()),
        &conditions,
        new_observed_gen,
    );

    if needs_patch {
        let patch = serde_json::json!({
            "status": {
                "phase": PangeaDashboardPhase::Ready,
                "configMapName": cm_name,
                "dashboardUid": dashboard_uid,
                "sourceHash": source_hash,
                "conditions": conditions,
                "observedGeneration": new_observed_gen,
                // Clear any stale error from a prior Failed cycle.
                "error": serde_json::Value::Null,
            }
        });
        crate::controller::status_patch::patch_status(pd.as_ref(), &state.client, patch)
            .await
            .map_err(Error::Kube)?;
        info!(
            %name,
            config_map = %cm_name,
            uid = dashboard_uid.as_deref().unwrap_or("<none>"),
            "PangeaDashboard Ready"
        );
    } else {
        debug!(
            %name,
            "PangeaDashboard status unchanged; skipping patch (avoids self-trigger watch loop)"
        );
    }

    Ok(Action::requeue(DEFAULT_REQUEUE_INTERVAL))
}

// ---------------------------------------------------------------------------
// ConfigMap delivery
// ---------------------------------------------------------------------------

/// The name of the sidecar ConfigMap a PangeaDashboard renders into.
fn config_map_name(dashboard_name: &str) -> String {
    format!("pangea-dashboard-{dashboard_name}")
}

/// Owner reference back to the PangeaDashboard so deleting the CR
/// garbage-collects its rendered ConfigMap.
fn owner_reference(pd: &PangeaDashboard) -> OwnerReference {
    OwnerReference {
        api_version: "pangea.pleme.io/v1alpha1".to_string(),
        kind: "PangeaDashboard".to_string(),
        name: pd.name_any(),
        uid: pd.metadata.uid.clone().unwrap_or_default(),
        controller: Some(true),
        block_owner_deletion: Some(true),
    }
}

/// Build the sidecar ConfigMap object for a rendered dashboard.
///
/// Shape:
///   - `metadata.labels.grafana_dashboard = "1"` — the sidecar selector.
///   - `metadata.labels."pleme.io/pangea-dashboard" = <name>` — provenance.
///   - `metadata.annotations.grafana_folder = <folder>` — sidecar folder.
///   - `metadata.ownerReferences = [<PangeaDashboard>]` — GC on delete.
///   - `data["<name>.json"] = <grafana json>` — the dashboard payload.
fn build_dashboard_config_map(
    pd: &PangeaDashboard,
    namespace: &str,
    cm_name: &str,
    dashboard_name: &str,
    folder: &str,
    dashboard_json: &str,
) -> ConfigMap {
    let mut labels = BTreeMap::new();
    labels.insert("grafana_dashboard".to_string(), "1".to_string());
    labels.insert(
        "pleme.io/pangea-dashboard".to_string(),
        dashboard_name.to_string(),
    );

    let mut annotations = BTreeMap::new();
    annotations.insert("grafana_folder".to_string(), folder.to_string());

    let mut data = BTreeMap::new();
    data.insert(format!("{dashboard_name}.json"), dashboard_json.to_string());

    ConfigMap {
        metadata: kube::api::ObjectMeta {
            name: Some(cm_name.to_string()),
            namespace: Some(namespace.to_string()),
            labels: Some(labels),
            annotations: Some(annotations),
            owner_references: Some(vec![owner_reference(pd)]),
            ..Default::default()
        },
        data: Some(data),
        ..Default::default()
    }
}

/// Upsert the dashboard ConfigMap: create it if absent, else converge
/// its data/labels/annotations with a merge patch. Idempotent — the CM
/// is fully owned by this controller (ownerReference + the
/// `pleme.io/pangea-dashboard` label), so a merge never fights another
/// field manager. Matches the operator's create-then-merge idiom
/// (`Patch::Merge`, not server-side apply).
async fn upsert_dashboard_config_map(
    pd: &PangeaDashboard,
    namespace: &str,
    cm_name: &str,
    dashboard_name: &str,
    folder: &str,
    dashboard_json: &str,
    state: &ControllerState,
) -> Result<(), Error> {
    let cm = build_dashboard_config_map(
        pd,
        namespace,
        cm_name,
        dashboard_name,
        folder,
        dashboard_json,
    );
    let cm_api: Api<ConfigMap> = Api::namespaced(state.client.clone(), namespace);

    match cm_api.create(&Default::default(), &cm).await {
        Ok(_) => {
            info!(config_map = %cm_name, "Dashboard ConfigMap created");
            Ok(())
        }
        // Already exists — converge its content with a merge patch. The
        // labels/annotations/data/ownerReferences are all set by us, so
        // a merge is sufficient + idempotent.
        Err(kube::Error::Api(ae)) if ae.code == 409 => {
            let pp = PatchParams::apply("pangea-operator");
            cm_api
                .patch(cm_name, &pp, &Patch::Merge(&cm))
                .await
                .map_err(Error::Kube)?;
            debug!(config_map = %cm_name, "Dashboard ConfigMap converged");
            Ok(())
        }
        Err(e) => Err(Error::Kube(e)),
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Hex-encoded BLAKE3 of the Ruby source — the change-detection key
/// stored in `status.sourceHash`. The `sourceHash` field's job is
/// change detection (the diff-gate), not a cryptographic compliance
/// claim, so we use the operator's existing content-hash idiom
/// (`blake3`, already a direct dep — see `anomaly_tracker`) and its
/// typed `to_hex()` renderer rather than pulling in `sha2`. The CRD
/// field name predates this and is kept for schema stability.
fn source_hash_hex(source: &str) -> String {
    blake3::hash(source.as_bytes()).to_hex().to_string()
}

/// Pull the `"uid"` field out of the rendered Grafana JSON (top-level).
/// Returns `None` when the dashboard didn't declare a UID (Grafana
/// then assigns one) or the JSON didn't parse.
fn extract_dashboard_uid(dashboard_json: &str) -> Option<String> {
    let v: serde_json::Value = serde_json::from_str(dashboard_json).ok()?;
    // Grafana dashboards carry the uid at the top level; some wrappers
    // nest it under "dashboard". Check both.
    v.get("uid")
        .or_else(|| v.get("dashboard").and_then(|d| d.get("uid")))
        .and_then(|u| u.as_str())
        .map(|s| s.to_string())
}

/// Patch the CR to `Failed` with a typed error + a `Ready=False`
/// condition, through the diff-gate (so a repeated identical failure
/// doesn't churn the watch).
async fn patch_failed(
    pd: &PangeaDashboard,
    state: &ControllerState,
    error_msg: &str,
) -> Result<(), Error> {
    let new_observed_gen = pd.metadata.generation.unwrap_or(0);
    let conditions = vec![create_condition("Ready", false, "RenderFailed", error_msg)];

    let prev_hash = pd.status.as_ref().and_then(|s| s.source_hash.clone());
    let needs_patch = dashboard_status_needs_patch(
        pd.status.as_ref(),
        PangeaDashboardPhase::Failed,
        pd.status.as_ref().and_then(|s| s.config_map_name.as_deref()),
        pd.status.as_ref().and_then(|s| s.dashboard_uid.as_deref()),
        prev_hash.as_deref(),
        &conditions,
        new_observed_gen,
    ) || pd.status.as_ref().and_then(|s| s.error.as_deref()) != Some(error_msg);

    if !needs_patch {
        debug!("PangeaDashboard Failed status unchanged; skipping patch");
        return Ok(());
    }

    let patch = serde_json::json!({
        "status": {
            "phase": PangeaDashboardPhase::Failed,
            "error": error_msg,
            "conditions": conditions,
            "observedGeneration": new_observed_gen,
        }
    });
    crate::controller::status_patch::patch_status(pd, &state.client, patch)
        .await
        .map_err(Error::Kube)?;
    Ok(())
}

/// Diff-gate for the PangeaDashboard status PATCH.
///
/// Returns `true` only when at least one observable field would change.
/// Without it, every reconcile re-PATCHes the same tuple with a fresh
/// `lastTransitionTime` (`create_condition` stamps `Utc::now()`), which
/// refires the watch and creates the closed-loop reconcile the operator
/// hit on rio. Conditions are compared semantically (timestamp ignored)
/// via `conditions_observably_equal`.
fn dashboard_status_needs_patch(
    prev: Option<&crate::crd::PangeaDashboardStatus>,
    new_phase: PangeaDashboardPhase,
    new_config_map: Option<&str>,
    new_uid: Option<&str>,
    new_source_hash: Option<&str>,
    new_conditions: &[Condition],
    new_observed_gen: i64,
) -> bool {
    let prev_phase = prev.and_then(|s| s.phase);
    let prev_config_map = prev.and_then(|s| s.config_map_name.as_deref());
    let prev_uid = prev.and_then(|s| s.dashboard_uid.as_deref());
    let prev_hash = prev.and_then(|s| s.source_hash.as_deref());
    let prev_observed_gen = prev.map(|s| s.observed_generation).unwrap_or(0);
    let prev_conditions: &[Condition] = prev.map(|s| s.conditions.as_slice()).unwrap_or(&[]);
    let conditions_match =
        crate::controller::status::conditions_observably_equal(prev_conditions, new_conditions);

    prev.is_none()
        || !conditions_match
        || prev_phase != Some(new_phase)
        || prev_config_map != new_config_map
        || prev_uid != new_uid
        || prev_hash != new_source_hash
        || prev_observed_gen != new_observed_gen
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crd::pangea_dashboard::{
        PangeaDashboard, PangeaDashboardPhase, PangeaDashboardStatus,
    };
    use kube::CustomResourceExt;

    fn cond(typ: &str, status: &str, reason: &str, msg: &str) -> Condition {
        Condition {
            r#type: typ.into(),
            status: status.into(),
            reason: reason.into(),
            message: msg.into(),
            last_transition_time: chrono::TimeZone::with_ymd_and_hms(
                &chrono::Utc,
                2025,
                1,
                1,
                0,
                0,
                0,
            )
            .unwrap(),
        }
    }

    fn ready_status(observed_gen: i64, hash: &str, uid: Option<&str>) -> PangeaDashboardStatus {
        PangeaDashboardStatus {
            phase: Some(PangeaDashboardPhase::Ready),
            config_map_name: Some("pangea-dashboard-payments".into()),
            grafana_dashboard_name: None,
            dashboard_uid: uid.map(|s| s.into()),
            conditions: vec![cond("Ready", "True", "Synced", "ok")],
            observed_generation: observed_gen,
            error: None,
            source_hash: Some(hash.into()),
        }
    }

    // ── sourceHash ──

    #[test]
    fn source_hash_is_stable_and_distinct() {
        let a = source_hash_hex("Pangea::Dashboards::Library::WorkloadOverview.build");
        let b = source_hash_hex("Pangea::Dashboards::Library::WorkloadOverview.build");
        let c = source_hash_hex("something else");
        assert_eq!(a, b, "same source must hash identically");
        assert_ne!(a, c, "different source must hash differently");
        assert_eq!(a.len(), 64, "blake3 hex is 64 chars");
        assert!(a.chars().all(|ch| ch.is_ascii_hexdigit()));
    }

    #[test]
    fn source_hash_known_vector() {
        // BLAKE3("") well-known vector — locks the encoding so a future
        // swap of the hash impl is caught by this test.
        assert_eq!(
            source_hash_hex(""),
            "af1349b9f5f9a1a6a0404dea36dcc9499bcb25c9adc112b7cc9a93cae41f3262"
        );
    }

    // ── dashboard UID extraction ──

    #[test]
    fn uid_extracted_from_top_level() {
        let json = r#"{"uid":"payments-abc","title":"Payments","panels":[]}"#;
        assert_eq!(extract_dashboard_uid(json), Some("payments-abc".into()));
    }

    #[test]
    fn uid_extracted_from_nested_dashboard_wrapper() {
        let json = r#"{"dashboard":{"uid":"nested-xyz","title":"X"},"folderId":0}"#;
        assert_eq!(extract_dashboard_uid(json), Some("nested-xyz".into()));
    }

    #[test]
    fn uid_none_when_absent_or_invalid() {
        assert_eq!(extract_dashboard_uid(r#"{"title":"no uid"}"#), None);
        assert_eq!(extract_dashboard_uid("not json"), None);
    }

    // ── ConfigMap shape ──

    fn sample_pd() -> PangeaDashboard {
        let mut pd = PangeaDashboard::new(
            "payments",
            crate::crd::PangeaDashboardSpec {
                source: DashboardSource::Inline {
                    ruby: "x".into(),
                },
                grafana_instance_selector: Default::default(),
                folder: Some("rio".into()),
                overwrite: true,
                extend_modules: vec!["Pangea::Grafana".into()],
                message: None,
                suspend: false,
            },
        );
        pd.metadata.uid = Some("uid-1234".into());
        pd.metadata.namespace = Some("observability".into());
        pd
    }

    #[test]
    fn config_map_name_is_prefixed() {
        assert_eq!(config_map_name("payments"), "pangea-dashboard-payments");
    }

    #[test]
    fn config_map_has_sidecar_label_and_folder_annotation() {
        let pd = sample_pd();
        let cm = build_dashboard_config_map(
            &pd,
            "observability",
            "pangea-dashboard-payments",
            "payments",
            "rio",
            r#"{"uid":"u","title":"Payments"}"#,
        );

        let labels = cm.metadata.labels.expect("labels present");
        assert_eq!(labels.get("grafana_dashboard").map(String::as_str), Some("1"));
        assert_eq!(
            labels.get("pleme.io/pangea-dashboard").map(String::as_str),
            Some("payments")
        );

        let annotations = cm.metadata.annotations.expect("annotations present");
        assert_eq!(
            annotations.get("grafana_folder").map(String::as_str),
            Some("rio")
        );
    }

    #[test]
    fn config_map_data_keyed_by_dashboard_json_filename() {
        let pd = sample_pd();
        let json = r#"{"uid":"u","title":"Payments"}"#;
        let cm = build_dashboard_config_map(
            &pd,
            "observability",
            "pangea-dashboard-payments",
            "payments",
            "rio",
            json,
        );
        let data = cm.data.expect("data present");
        assert_eq!(data.get("payments.json").map(String::as_str), Some(json));
    }

    #[test]
    fn config_map_owner_reference_points_at_the_dashboard() {
        let pd = sample_pd();
        let cm = build_dashboard_config_map(
            &pd,
            "observability",
            "pangea-dashboard-payments",
            "payments",
            "rio",
            "{}",
        );
        let owners = cm.metadata.owner_references.expect("owner refs present");
        assert_eq!(owners.len(), 1);
        let o = &owners[0];
        assert_eq!(o.kind, "PangeaDashboard");
        assert_eq!(o.api_version, "pangea.pleme.io/v1alpha1");
        assert_eq!(o.name, "payments");
        assert_eq!(o.uid, "uid-1234");
        assert_eq!(o.controller, Some(true));
        assert_eq!(o.block_owner_deletion, Some(true));
    }

    #[test]
    fn config_map_default_folder_is_general() {
        // When spec.folder is None the reconcile passes "General"; verify
        // the CM carries that as the sidecar folder annotation.
        let pd = sample_pd();
        let cm = build_dashboard_config_map(
            &pd,
            "observability",
            "pangea-dashboard-payments",
            "payments",
            "General",
            "{}",
        );
        let annotations = cm.metadata.annotations.expect("annotations present");
        assert_eq!(
            annotations.get("grafana_folder").map(String::as_str),
            Some("General")
        );
    }

    // ── status diff-gate ──

    #[test]
    fn first_reconcile_must_patch() {
        let conds = vec![cond("Ready", "True", "Synced", "ok")];
        assert!(
            dashboard_status_needs_patch(
                None,
                PangeaDashboardPhase::Ready,
                Some("pangea-dashboard-payments"),
                Some("u"),
                Some("h1"),
                &conds,
                1,
            ),
            "missing prev status must always force a PATCH"
        );
    }

    #[test]
    fn steady_state_skips_patch() {
        let prev = ready_status(7, "h1", Some("u"));
        // Same phase, CM, uid, hash, gen — only fresh lastTransitionTime
        // would differ. Gate must skip.
        let conds = vec![cond("Ready", "True", "Synced", "ok")];
        assert!(
            !dashboard_status_needs_patch(
                Some(&prev),
                PangeaDashboardPhase::Ready,
                Some("pangea-dashboard-payments"),
                Some("u"),
                Some("h1"),
                &conds,
                7,
            ),
            "must NOT patch on timestamp-only churn"
        );
    }

    #[test]
    fn source_hash_change_must_patch() {
        let prev = ready_status(7, "h1", Some("u"));
        let conds = vec![cond("Ready", "True", "Synced", "ok")];
        assert!(
            dashboard_status_needs_patch(
                Some(&prev),
                PangeaDashboardPhase::Ready,
                Some("pangea-dashboard-payments"),
                Some("u"),
                Some("h2"),
                &conds,
                7,
            ),
            "a re-rendered dashboard (new hash) must force a PATCH"
        );
    }

    #[test]
    fn phase_flip_to_failed_must_patch() {
        let prev = ready_status(7, "h1", Some("u"));
        let conds = vec![cond("Ready", "False", "RenderFailed", "boom")];
        assert!(
            dashboard_status_needs_patch(
                Some(&prev),
                PangeaDashboardPhase::Failed,
                Some("pangea-dashboard-payments"),
                Some("u"),
                Some("h1"),
                &conds,
                7,
            ),
            "Ready→Failed transition must force a PATCH"
        );
    }

    #[test]
    fn uid_change_must_patch() {
        let prev = ready_status(7, "h1", Some("u1"));
        let conds = vec![cond("Ready", "True", "Synced", "ok")];
        assert!(
            dashboard_status_needs_patch(
                Some(&prev),
                PangeaDashboardPhase::Ready,
                Some("pangea-dashboard-payments"),
                Some("u2"),
                Some("h1"),
                &conds,
                7,
            ),
            "dashboard UID change must force a PATCH"
        );
    }

    #[test]
    fn observed_gen_advance_must_patch() {
        let prev = ready_status(7, "h1", Some("u"));
        let conds = vec![cond("Ready", "True", "Synced", "ok")];
        assert!(
            dashboard_status_needs_patch(
                Some(&prev),
                PangeaDashboardPhase::Ready,
                Some("pangea-dashboard-payments"),
                Some("u"),
                Some("h1"),
                &conds,
                8,
            ),
            "observed generation bump must force a PATCH"
        );
    }

    // ── existing CRD-shape regression coverage (kept) ──

    #[test]
    fn phase_serde_round_trip() {
        for p in [
            PangeaDashboardPhase::Pending,
            PangeaDashboardPhase::Synthesizing,
            PangeaDashboardPhase::Ready,
            PangeaDashboardPhase::Failed,
        ] {
            let s = serde_json::to_string(&p).unwrap();
            let back: PangeaDashboardPhase = serde_json::from_str(&s).unwrap();
            assert_eq!(format!("{:?}", p), format!("{:?}", back));
        }
    }

    #[test]
    fn status_default_round_trips() {
        let s = PangeaDashboardStatus::default();
        let j = serde_json::to_string(&s).unwrap();
        let back: PangeaDashboardStatus = serde_json::from_str(&j).unwrap();
        assert_eq!(format!("{:?}", s), format!("{:?}", back));
    }

    #[test]
    fn crd_yaml_renders_cleanly() {
        let yaml = serde_yaml::to_string(&PangeaDashboard::crd()).expect("CRD serializes");
        assert!(yaml.contains("pangeadashboards.pangea.pleme.io"));
        assert!(yaml.contains("- pangea"));
    }

    #[test]
    fn crd_namespaced_scope_locked() {
        let yaml = serde_yaml::to_string(&PangeaDashboard::crd()).expect("CRD serializes");
        assert!(yaml.contains("scope: Namespaced"));
    }

    #[test]
    fn crd_short_name_present() {
        let yaml = serde_yaml::to_string(&PangeaDashboard::crd()).expect("CRD serializes");
        assert!(yaml.contains("shortNames"));
    }

    #[test]
    fn status_default_phase_is_none() {
        let s = PangeaDashboardStatus::default();
        assert!(s.phase.is_none());
    }
}
