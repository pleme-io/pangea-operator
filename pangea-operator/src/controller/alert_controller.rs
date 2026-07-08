//! PangeaAlert controller — synthesizes alert-rule CRs from Ruby DSL.
//!
//! Sibling of [`super::dashboard_controller`]. Reconciliation loop:
//! 1. Read PangeaAlert spec (inline Ruby or ConfigMap ref).
//! 2. Render the Ruby through the embedded magnus backend → an
//!    alert-rule manifest (a `VMRule` or `PrometheusRule`, per
//!    `spec.backend`).
//! 3. Diff-gate on `sourceHash`: if unchanged AND the rule CR already
//!    exists, skip (no churn).
//! 4. Upsert the rule CR, owned by the PangeaAlert (ownerReference → GC
//!    on CR delete) and stamped with a `dest=<destination>` routing
//!    label.
//! 5. Patch status (phase, renderedResource ref, sourceHash,
//!    observedGeneration, Ready condition) through a
//!    `*_status_needs_patch` diff-gate per the operator's
//!    status-write-loop discipline.
//!
//! Delivery target: rio runs the victoria-metrics-k8s-stack, whose
//! vmalert reads `operator.victoriametrics.com/v1beta1 VMRule` CRs
//! (the default). `spec.backend: prometheusrule` instead emits a
//! `monitoring.coreos.com/v1 PrometheusRule` for prometheus-operator
//! stacks — the two are spec-interchangeable. The `AlertDelivery` enum
//! below is the typed seam; both backends flow through the same
//! dynamic-CR upsert path.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference;
use kube::{
    api::{Api, ApiResource, DynamicObject, Patch, PatchParams, PostParams},
    runtime::controller::Action,
    ResourceExt,
};
use tracing::{debug, error, info, instrument, warn};

use crate::crd::{AlertBackend, AlertSource, Condition, PangeaAlert, PangeaAlertPhase};
use crate::error::Error;

use super::{create_condition, ControllerState, DEFAULT_REQUEUE_INTERVAL, SHORT_REQUEUE_INTERVAL};

/// How a rendered alert is delivered. Typed seam mirroring
/// `DashboardDelivery`: both `spec.backend` arms (VMRule /
/// PrometheusRule) share the one dynamic rule-CR upsert path, so this
/// is a single variant today. A future non-CR target (e.g. a Datadog
/// monitor Terraform resource) would slot in here.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AlertDelivery {
    /// Upsert the rendered `VMRule` / `PrometheusRule` as a namespaced
    /// dynamic CR the alerting stack reconciles.
    RuleCr,
}

impl AlertDelivery {
    fn for_alert(_pa: &PangeaAlert) -> Self {
        AlertDelivery::RuleCr
    }
}

/// Controller for PangeaAlert resources.
pub struct AlertController {
    state: ControllerState,
}

impl AlertController {
    pub fn new(state: ControllerState) -> Self {
        Self { state }
    }

    /// Start the PangeaAlert controller.
    pub async fn run(self) -> crate::error::Result<()> {
        let client = self.state.client.clone();
        let state = Arc::new(self.state);

        info!("Starting PangeaAlert controller");

        crate::controller::generation_filter::filtered_controller::<PangeaAlert>(client)
            .run(
                move |pa, ctx| {
                    let state = Arc::clone(&ctx);
                    async move { reconcile(pa, state).await }
                },
                error_policy,
                state,
            )
            .for_each(|result| async move {
                match result {
                    Ok((obj, action)) => {
                        debug!(name = %obj.name, ?action, "PangeaAlert reconciliation completed");
                    }
                    Err(e) => {
                        error!(error = %e, "PangeaAlert reconciliation failed");
                    }
                }
            })
            .await;

        Ok(())
    }
}

fn error_policy(_pa: Arc<PangeaAlert>, error: &Error, ctx: Arc<ControllerState>) -> Action {
    crate::controller::error_policy::run_error_policy(
        &ctx.metrics,
        crate::crd::ControllerKind::Alert,
        error,
        Duration::from_secs(30),
    )
}

#[instrument(skip(state), fields(name = %pa.name_any()))]
async fn reconcile(pa: Arc<PangeaAlert>, state: Arc<ControllerState>) -> Result<Action, Error> {
    state
        .metrics
        .record_reconcile(crate::crd::ControllerKind::Alert, "ok");
    let name = pa.name_any();
    let namespace = pa.namespace().unwrap_or_else(|| "default".to_string());

    info!(%namespace, "Reconciling PangeaAlert");

    // Cluster-wide kill-switch — honor `OperatorPolicy/default`.
    if let Some(action) = crate::controller::policy_pipeline::run_for_controller(
        &state,
        crate::crd::ControllerKind::Alert,
    )
    .into_skip_action()
    {
        return Ok(action);
    }

    // Per-CR suspend gate (the cluster-scoped policy is checked above).
    if pa.spec.suspend {
        info!(%name, %namespace, "PangeaAlert suspended; skipping reconcile");
        return Ok(Action::requeue(Duration::from_secs(30)));
    }

    // Extract Ruby source.
    let ruby_source = match &pa.spec.source {
        AlertSource::Inline { ruby } => ruby.clone(),
        AlertSource::ConfigMapRef { name: cm_name, key } => {
            let cm_api: Api<k8s_openapi::api::core::v1::ConfigMap> =
                Api::namespaced(state.client.clone(), &namespace);
            match cm_api.get(cm_name).await {
                Ok(cm) => cm.data.and_then(|d| d.get(key).cloned()).unwrap_or_default(),
                Err(e) => {
                    warn!(%name, error = %e, "Failed to read source ConfigMap");
                    return Ok(Action::requeue(Duration::from_secs(30)));
                }
            }
        }
    };

    let source_hash = source_hash_hex(&ruby_source);
    let backend = pa.spec.backend;
    let rule_name = alert_rule_name(&name);
    let ar = rule_api_resource(backend);

    // Diff-gate: if sourceHash matches the last successful render AND
    // the rendered rule CR is still present, the alert is already
    // converged — skip render + write + status churn. A missing rule
    // (an out-of-band delete) forces a re-render even on a hash match.
    let prev_hash = pa.status.as_ref().and_then(|s| s.source_hash.as_deref());
    if prev_hash == Some(source_hash.as_str()) {
        let rule_api: Api<DynamicObject> =
            Api::namespaced_with(state.client.clone(), &namespace, &ar);
        if rule_api
            .get_opt(&rule_name)
            .await
            .map_err(Error::Kube)?
            .is_some()
        {
            debug!(%name, "sourceHash unchanged + rule CR present; skipping render");
            return Ok(Action::requeue(DEFAULT_REQUEUE_INTERVAL));
        }
    }

    // (a) Render the inline Ruby → alert-rule manifest JSON string,
    // rendered per spec.backend by the embedded magnus backend.
    let manifest_json = match state
        .compiler_backend
        .render_alert(
            ruby_source.clone(),
            pa.spec.extend_modules.clone(),
            backend.render_backend(),
        )
        .await
    {
        Ok(j) => j,
        Err(e) => {
            let msg = format!("alert render failed: {e}");
            warn!(%name, error = %msg, "PangeaAlert render failed");
            patch_failed(&pa, &state, &msg).await?;
            return Ok(Action::requeue(SHORT_REQUEUE_INTERVAL));
        }
    };

    // (b) Extract the rule spec from the rendered manifest.
    let spec_value = match extract_rule_spec(&manifest_json) {
        Ok(v) => v,
        Err(msg) => {
            warn!(%name, error = %msg, "PangeaAlert manifest invalid");
            patch_failed(&pa, &state, &msg).await?;
            return Ok(Action::requeue(SHORT_REQUEUE_INTERVAL));
        }
    };

    // (c) Upsert the rule CR, owned by the PangeaAlert + dest-labelled.
    match AlertDelivery::for_alert(&pa) {
        AlertDelivery::RuleCr => {
            if let Err(e) =
                upsert_rule_cr(&pa, &namespace, &rule_name, &ar, spec_value, &state).await
            {
                let msg = format!("rule CR upsert failed: {e}");
                warn!(%name, error = %msg, "PangeaAlert delivery failed");
                patch_failed(&pa, &state, &msg).await?;
                return Ok(Action::requeue(SHORT_REQUEUE_INTERVAL));
            }
        }
    }

    // (d) Patch status (Ready) through the diff-gate.
    let new_observed_gen = pa.metadata.generation.unwrap_or(0);
    let rule_kind = backend.kind();
    let conditions = vec![create_condition(
        "Ready",
        true,
        "Synced",
        "Alert rendered and delivered as a rule CR",
    )];

    let needs_patch = alert_status_needs_patch(
        pa.status.as_ref(),
        PangeaAlertPhase::Ready,
        Some(rule_name.as_str()),
        Some(rule_kind),
        Some(source_hash.as_str()),
        &conditions,
        new_observed_gen,
    );

    if needs_patch {
        let patch = serde_json::json!({
            "status": {
                "phase": PangeaAlertPhase::Ready,
                "renderedResourceName": rule_name,
                "renderedResourceKind": rule_kind,
                "sourceHash": source_hash,
                "conditions": conditions,
                "observedGeneration": new_observed_gen,
                // Clear any stale error from a prior Failed cycle.
                "error": serde_json::Value::Null,
            }
        });
        crate::controller::status_patch::patch_status(pa.as_ref(), &state.client, patch)
            .await
            .map_err(Error::Kube)?;
        info!(
            %name,
            rule = %rule_name,
            kind = %rule_kind,
            "PangeaAlert Ready"
        );
    } else {
        debug!(
            %name,
            "PangeaAlert status unchanged; skipping patch (avoids self-trigger watch loop)"
        );
    }

    Ok(Action::requeue(DEFAULT_REQUEUE_INTERVAL))
}

// ---------------------------------------------------------------------------
// Rule-CR delivery
// ---------------------------------------------------------------------------

/// The name of the rule CR a PangeaAlert renders into.
fn alert_rule_name(alert_name: &str) -> String {
    format!("pangea-alert-{alert_name}")
}

/// Typed `ApiResource` for the rule CR GVK selected by `spec.backend`.
/// Built with explicit `plural` rather than the kube pluralizer so the
/// dynamic Api hits the right resource path (`vmrules` /
/// `prometheusrules`).
fn rule_api_resource(backend: AlertBackend) -> ApiResource {
    ApiResource {
        group: backend.group().to_string(),
        version: backend.version().to_string(),
        api_version: backend.api_version(),
        kind: backend.kind().to_string(),
        plural: backend.plural().to_string(),
    }
}

/// Owner reference back to the PangeaAlert so deleting the CR
/// garbage-collects its rendered rule CR.
fn owner_reference(pa: &PangeaAlert) -> OwnerReference {
    OwnerReference {
        api_version: "pangea.pleme.io/v1alpha1".to_string(),
        kind: "PangeaAlert".to_string(),
        name: pa.name_any(),
        uid: pa.metadata.uid.clone().unwrap_or_default(),
        controller: Some(true),
        block_owner_deletion: Some(true),
    }
}

/// Pull the `.spec` object out of the rendered rule manifest. The Ruby
/// `Render::{Victoria,Prometheus}` emits `{apiVersion, kind, metadata,
/// spec}`; the controller owns apiVersion/kind/metadata (from
/// `spec.backend` + its own naming/labels/ownerRefs), so only `.spec`
/// (the alert groups) is carried through.
fn extract_rule_spec(manifest_json: &str) -> Result<serde_json::Value, String> {
    let manifest: serde_json::Value = serde_json::from_str(manifest_json)
        .map_err(|e| format!("alert manifest parse failed: {e}"))?;
    manifest
        .get("spec")
        .cloned()
        .ok_or_else(|| "rendered alert manifest missing `.spec`".to_string())
}

/// Build the rule CR `DynamicObject`: GVK from `spec.backend`,
/// controller-owned name/namespace/labels/ownerRefs, and the rendered
/// `.spec`.
///
/// Shape:
///   - `metadata.labels.dest = <destination>` — the routing selector.
///   - `metadata.labels."pleme.io/pangea-alert" = <name>` — provenance.
///   - `metadata.ownerReferences = [<PangeaAlert>]` — GC on delete.
///   - `spec = <rendered groups>` — the alert payload.
fn build_rule_object(
    pa: &PangeaAlert,
    namespace: &str,
    rule_name: &str,
    ar: &ApiResource,
    spec_value: serde_json::Value,
) -> DynamicObject {
    let mut labels = BTreeMap::new();
    labels.insert("dest".to_string(), pa.spec.routing.destination.to_string());
    labels.insert(
        "pleme.io/pangea-alert".to_string(),
        pa.name_any(),
    );

    let mut obj = DynamicObject::new(rule_name, ar);
    obj.metadata.namespace = Some(namespace.to_string());
    obj.metadata.labels = Some(labels);
    obj.metadata.owner_references = Some(vec![owner_reference(pa)]);
    obj.data = serde_json::json!({ "spec": spec_value });
    obj
}

/// Upsert the rule CR: create it if absent, else converge with a merge
/// patch. Idempotent — the CR is fully owned by this controller
/// (ownerReference + the `pleme.io/pangea-alert` label). Matches the
/// operator's create-then-merge idiom (`Patch::Merge`, not SSA), same
/// as the dashboard ConfigMap path.
async fn upsert_rule_cr(
    pa: &PangeaAlert,
    namespace: &str,
    rule_name: &str,
    ar: &ApiResource,
    spec_value: serde_json::Value,
    state: &ControllerState,
) -> Result<(), Error> {
    let obj = build_rule_object(pa, namespace, rule_name, ar, spec_value);
    let rule_api: Api<DynamicObject> =
        Api::namespaced_with(state.client.clone(), namespace, ar);

    match rule_api.create(&PostParams::default(), &obj).await {
        Ok(_) => {
            info!(rule = %rule_name, kind = %ar.kind, "Alert rule CR created");
            Ok(())
        }
        // Already exists — converge its content with a merge patch.
        Err(kube::Error::Api(ae)) if ae.code == 409 => {
            let pp = PatchParams::apply("pangea-operator");
            rule_api
                .patch(rule_name, &pp, &Patch::Merge(&obj))
                .await
                .map_err(Error::Kube)?;
            debug!(rule = %rule_name, "Alert rule CR converged");
            Ok(())
        }
        Err(e) => Err(Error::Kube(e)),
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Hex-encoded BLAKE3 of the Ruby source — the change-detection key in
/// `status.sourceHash`. Same idiom as the dashboard controller (the
/// hash's job is change detection, not a cryptographic claim).
fn source_hash_hex(source: &str) -> String {
    blake3::hash(source.as_bytes()).to_hex().to_string()
}

/// Patch the CR to `Failed` with a typed error + a `Ready=False`
/// condition, through the diff-gate.
async fn patch_failed(
    pa: &PangeaAlert,
    state: &ControllerState,
    error_msg: &str,
) -> Result<(), Error> {
    let new_observed_gen = pa.metadata.generation.unwrap_or(0);
    let conditions = vec![create_condition("Ready", false, "RenderFailed", error_msg)];

    let prev_hash = pa.status.as_ref().and_then(|s| s.source_hash.clone());
    let needs_patch = alert_status_needs_patch(
        pa.status.as_ref(),
        PangeaAlertPhase::Failed,
        pa.status.as_ref().and_then(|s| s.rendered_resource_name.as_deref()),
        pa.status.as_ref().and_then(|s| s.rendered_resource_kind.as_deref()),
        prev_hash.as_deref(),
        &conditions,
        new_observed_gen,
    ) || pa.status.as_ref().and_then(|s| s.error.as_deref()) != Some(error_msg);

    if !needs_patch {
        debug!("PangeaAlert Failed status unchanged; skipping patch");
        return Ok(());
    }

    let patch = serde_json::json!({
        "status": {
            "phase": PangeaAlertPhase::Failed,
            "error": error_msg,
            "conditions": conditions,
            "observedGeneration": new_observed_gen,
        }
    });
    crate::controller::status_patch::patch_status(pa, &state.client, patch)
        .await
        .map_err(Error::Kube)?;
    Ok(())
}

/// Diff-gate for the PangeaAlert status PATCH. Returns `true` only when
/// at least one observable field would change (conditions compared
/// semantically, timestamp ignored) — the status-write-loop discipline.
fn alert_status_needs_patch(
    prev: Option<&crate::crd::PangeaAlertStatus>,
    new_phase: PangeaAlertPhase,
    new_rule_name: Option<&str>,
    new_rule_kind: Option<&str>,
    new_source_hash: Option<&str>,
    new_conditions: &[Condition],
    new_observed_gen: i64,
) -> bool {
    let prev_phase = prev.and_then(|s| s.phase);
    let prev_rule_name = prev.and_then(|s| s.rendered_resource_name.as_deref());
    let prev_rule_kind = prev.and_then(|s| s.rendered_resource_kind.as_deref());
    let prev_hash = prev.and_then(|s| s.source_hash.as_deref());
    let prev_observed_gen = prev.map(|s| s.observed_generation).unwrap_or(0);
    let prev_conditions: &[Condition] = prev.map(|s| s.conditions.as_slice()).unwrap_or(&[]);
    let conditions_match =
        crate::controller::status::conditions_observably_equal(prev_conditions, new_conditions);

    prev.is_none()
        || !conditions_match
        || prev_phase != Some(new_phase)
        || prev_rule_name != new_rule_name
        || prev_rule_kind != new_rule_kind
        || prev_hash != new_source_hash
        || prev_observed_gen != new_observed_gen
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crd::pangea_alert::{
        AlertDestination, AlertRouting, PangeaAlert, PangeaAlertPhase, PangeaAlertSpec,
        PangeaAlertStatus,
    };
    use crate::crd::AlertSource;

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

    fn ready_status(observed_gen: i64, hash: &str) -> PangeaAlertStatus {
        PangeaAlertStatus {
            phase: Some(PangeaAlertPhase::Ready),
            rendered_resource_name: Some("pangea-alert-secure-vpc".into()),
            rendered_resource_kind: Some("VMRule".into()),
            conditions: vec![cond("Ready", "True", "Synced", "ok")],
            observed_generation: observed_gen,
            error: None,
            source_hash: Some(hash.into()),
        }
    }

    fn sample_pa(backend: AlertBackend, dest: AlertDestination) -> PangeaAlert {
        let mut pa = PangeaAlert::new(
            "secure-vpc",
            PangeaAlertSpec {
                source: AlertSource::Inline { ruby: "x".into() },
                backend,
                routing: AlertRouting { destination: dest },
                extend_modules: vec!["Pangea::Alerts".into()],
                suspend: false,
            },
        );
        pa.metadata.uid = Some("uid-9999".into());
        pa.metadata.namespace = Some("monitoring".into());
        pa
    }

    // ── sourceHash ──

    #[test]
    fn source_hash_is_stable_and_distinct() {
        let a = source_hash_hex("alerts { group 'g' }");
        let b = source_hash_hex("alerts { group 'g' }");
        let c = source_hash_hex("something else");
        assert_eq!(a, b);
        assert_ne!(a, c);
        assert_eq!(a.len(), 64);
    }

    // ── naming + GVK ──

    #[test]
    fn rule_name_is_prefixed() {
        assert_eq!(alert_rule_name("secure-vpc"), "pangea-alert-secure-vpc");
    }

    #[test]
    fn api_resource_matches_backend_gvk() {
        let vm = rule_api_resource(AlertBackend::VmRule);
        assert_eq!(vm.kind, "VMRule");
        assert_eq!(vm.plural, "vmrules");
        assert_eq!(vm.api_version, "operator.victoriametrics.com/v1beta1");

        let prom = rule_api_resource(AlertBackend::PrometheusRule);
        assert_eq!(prom.kind, "PrometheusRule");
        assert_eq!(prom.plural, "prometheusrules");
        assert_eq!(prom.api_version, "monitoring.coreos.com/v1");
    }

    // ── manifest spec extraction ──

    #[test]
    fn extracts_spec_from_manifest() {
        let manifest = r#"{"apiVersion":"operator.victoriametrics.com/v1beta1","kind":"VMRule","metadata":{"name":"x"},"spec":{"groups":[{"name":"g","rules":[]}]}}"#;
        let spec = extract_rule_spec(manifest).expect("spec present");
        assert!(spec.get("groups").is_some());
    }

    #[test]
    fn missing_spec_is_a_typed_error() {
        let manifest = r#"{"apiVersion":"x","kind":"VMRule","metadata":{"name":"x"}}"#;
        assert!(extract_rule_spec(manifest).is_err());
    }

    #[test]
    fn bad_json_is_a_typed_error() {
        assert!(extract_rule_spec("not json").is_err());
    }

    // ── rule object shape ──

    #[test]
    fn rule_object_carries_dest_label_and_owner_ref() {
        let pa = sample_pa(AlertBackend::VmRule, AlertDestination::Akeyless);
        let ar = rule_api_resource(AlertBackend::VmRule);
        let obj = build_rule_object(
            &pa,
            "monitoring",
            "pangea-alert-secure-vpc",
            &ar,
            serde_json::json!({"groups": []}),
        );

        let labels = obj.metadata.labels.expect("labels present");
        assert_eq!(labels.get("dest").map(String::as_str), Some("akeyless"));
        assert_eq!(
            labels.get("pleme.io/pangea-alert").map(String::as_str),
            Some("secure-vpc")
        );

        let owners = obj.metadata.owner_references.expect("owner refs present");
        assert_eq!(owners.len(), 1);
        assert_eq!(owners[0].kind, "PangeaAlert");
        assert_eq!(owners[0].api_version, "pangea.pleme.io/v1alpha1");
        assert_eq!(owners[0].name, "secure-vpc");
        assert_eq!(owners[0].uid, "uid-9999");
        assert_eq!(owners[0].controller, Some(true));

        assert_eq!(obj.metadata.namespace.as_deref(), Some("monitoring"));
        assert_eq!(obj.data.get("spec").and_then(|s| s.get("groups")).is_some(), true);
    }

    #[test]
    fn rule_object_types_reflect_backend() {
        let pa = sample_pa(AlertBackend::PrometheusRule, AlertDestination::TwoF);
        let ar = rule_api_resource(AlertBackend::PrometheusRule);
        let obj = build_rule_object(
            &pa,
            "monitoring",
            "pangea-alert-secure-vpc",
            &ar,
            serde_json::json!({"groups": []}),
        );
        let types = obj.types.expect("types present");
        assert_eq!(types.kind, "PrometheusRule");
        assert_eq!(types.api_version, "monitoring.coreos.com/v1");
        let labels = obj.metadata.labels.expect("labels present");
        assert_eq!(labels.get("dest").map(String::as_str), Some("2f"));
    }

    // ── status diff-gate ──

    #[test]
    fn first_reconcile_must_patch() {
        let conds = vec![cond("Ready", "True", "Synced", "ok")];
        assert!(alert_status_needs_patch(
            None,
            PangeaAlertPhase::Ready,
            Some("pangea-alert-secure-vpc"),
            Some("VMRule"),
            Some("h1"),
            &conds,
            1,
        ));
    }

    #[test]
    fn steady_state_skips_patch() {
        let prev = ready_status(7, "h1");
        let conds = vec![cond("Ready", "True", "Synced", "ok")];
        assert!(!alert_status_needs_patch(
            Some(&prev),
            PangeaAlertPhase::Ready,
            Some("pangea-alert-secure-vpc"),
            Some("VMRule"),
            Some("h1"),
            &conds,
            7,
        ));
    }

    #[test]
    fn source_hash_change_must_patch() {
        let prev = ready_status(7, "h1");
        let conds = vec![cond("Ready", "True", "Synced", "ok")];
        assert!(alert_status_needs_patch(
            Some(&prev),
            PangeaAlertPhase::Ready,
            Some("pangea-alert-secure-vpc"),
            Some("VMRule"),
            Some("h2"),
            &conds,
            7,
        ));
    }

    #[test]
    fn backend_flip_changes_rule_kind_must_patch() {
        let prev = ready_status(7, "h1");
        let conds = vec![cond("Ready", "True", "Synced", "ok")];
        assert!(alert_status_needs_patch(
            Some(&prev),
            PangeaAlertPhase::Ready,
            Some("pangea-alert-secure-vpc"),
            Some("PrometheusRule"),
            Some("h1"),
            &conds,
            7,
        ));
    }

    #[test]
    fn phase_flip_to_failed_must_patch() {
        let prev = ready_status(7, "h1");
        let conds = vec![cond("Ready", "False", "RenderFailed", "boom")];
        assert!(alert_status_needs_patch(
            Some(&prev),
            PangeaAlertPhase::Failed,
            Some("pangea-alert-secure-vpc"),
            Some("VMRule"),
            Some("h1"),
            &conds,
            7,
        ));
    }
}
