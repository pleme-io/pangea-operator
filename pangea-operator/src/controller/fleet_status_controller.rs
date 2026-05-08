//! Fleet aggregation controller.
//!
//! Owns the cluster-scoped `PangeaFleetStatus/default` singleton. Lists
//! every pangea CRD class once per reconcile, tallies per-phase
//! histograms + healthy/failed counts, and patches the singleton's
//! status. Refreshes every 30s.
//!
//! Bypasses `OperatorPolicy/default.spec.globalSuspend` by design —
//! fleet status is read-only observability and is *especially* useful
//! while the operator is paused. An operator-policy pause means
//! "stop reconciling user resources"; it does not mean "stop showing
//! me what's there." Watch the aggregator's run loop in
//! `controller::reconciler` — it never calls `policy_pipeline`.
//!
//! At startup the controller self-creates the default CR if it doesn't
//! exist (same pattern OperatorPolicy uses). Other-named instances are
//! ignored — only `default` is honored.

use crate::crd::pangea_fleet_status::{
    is_singleton_name, ArchitectureGemTracking, ComplianceBindingTracking, PangeaFleetStatus,
    PangeaFleetStatusSpec, PangeaFleetStatusStatus, PangeaNamespaceTracking,
    PhaseHistogramTracking, TemplateTracking, WorkspaceCatalogTracking,
    PANGEA_FLEET_STATUS_SINGLETON,
};
use crate::crd::{
    AmiTest, AmiTestPhase, ArchitectureGem, BindingComplianceState, ComplianceBinding,
    ComplianceSchedule, ComplianceSchedulePhase, ImagePipeline, ImagePipelinePhase,
    InfrastructureFlow, InfrastructureTemplate, PackerBuild, PackerBuildPhase, PangeaDashboard,
    PangeaDashboardPhase, PangeaNamespace, Phase as TemplatePhase, SynthesizerFormat,
    SynthesizerFormatPhase, WorkspaceCatalog,
};
use chrono::Utc;
use futures::StreamExt;
use kube::{
    api::{Api, ListParams, Patch, PatchParams, PostParams},
    runtime::{
        controller::{Action, Controller},
        watcher::Config,
    },
    Client,
};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, error, info, warn};

/// Refresh cadence. 30s strikes a balance: fast enough to be
/// actionable (operators see new resources within half a minute);
/// slow enough that 13 list-all calls × 30s is well under a single
/// kube-apiserver QPS budget.
const REFRESH_INTERVAL: Duration = Duration::from_secs(30);

/// Wire fleet aggregation into the operator's runtime. Self-creates
/// the singleton on startup, then runs the controller indefinitely.
pub fn run(client: Client) -> impl std::future::Future<Output = ()> {
    let api: Api<PangeaFleetStatus> = Api::all(client.clone());
    let context = Arc::new(Context {
        client: client.clone(),
        last_patched: tokio::sync::Mutex::new(None),
    });

    // Best-effort bootstrap: create the singleton if missing. If the
    // create races with another operator instance (singleton-replica
    // failover scenario), we ignore the 409.
    let bootstrap_client = client.clone();
    tokio::spawn(async move {
        if let Err(e) = ensure_singleton(&bootstrap_client).await {
            warn!(error = %e, "Failed to ensure PangeaFleetStatus/default exists; controller will retry on first reconcile");
        }
    });

    Controller::new(api, Config::default())
        .run(reconcile, error_policy, context)
        .for_each(|res| async move {
            match res {
                Ok((obj, _)) => {
                    debug!("PangeaFleetStatus reconciled: {:?}", obj.name);
                }
                Err(e) => error!("PangeaFleetStatus reconcile error: {:?}", e),
            }
        })
}

struct Context {
    client: Client,
    /// Cache of the most-recently-PATCHed status. Compared against
    /// each new aggregation to gate the PATCH. We keep our own cache
    /// rather than reading `fs.status` because kube-rs's cached
    /// `Arc<PangeaFleetStatus>` lags the apiserver — when our PATCH
    /// triggers a watch event, the reconcile fires *before* the
    /// cache has the just-patched value, so `fs.status` is the
    /// previous-previous value and our gate would always trigger
    /// a write. The Mutex<Option<...>> shape is single-writer
    /// (this controller) and rare-mutate (only on real changes).
    last_patched: tokio::sync::Mutex<Option<PangeaFleetStatusStatus>>,
}

#[derive(Debug, thiserror::Error)]
enum FleetError {
    #[error("kube API error: {0}")]
    Kube(#[from] kube::Error),
    #[error("missing metadata.name on PangeaFleetStatus")]
    MissingName,
}

/// Reconciler — refreshes the singleton's status with the current
/// fleet aggregation. Bypasses globalSuspend.
async fn reconcile(
    fs: Arc<PangeaFleetStatus>,
    ctx: Arc<Context>,
) -> Result<Action, FleetError> {
    let name = fs.metadata.name.clone().ok_or(FleetError::MissingName)?;

    if !is_singleton_name(&name) {
        warn!(
            name = %name,
            singleton = PANGEA_FLEET_STATUS_SINGLETON,
            "Ignoring non-default PangeaFleetStatus — only `default` is honored"
        );
        return Ok(Action::await_change());
    }

    debug!("Refreshing PangeaFleetStatus/default");
    let new_status = aggregate_fleet_status(&ctx.client).await?;

    // Diff-gate the PATCH — same antipattern this file's siblings hit
    // during rio firefighting 2026-05-07. `aggregate_fleet_status`
    // returns a status with `last_updated_at = Utc::now()` and a fresh
    // `last_transition_time` on the `Updated` condition; without the
    // gate, every reconcile re-PATCHes the same fleet aggregation
    // with new timestamps, refiring this controller's watch and
    // creating a self-trigger loop on top of the 30s requeue floor.
    //
    // Compare against `ctx.last_patched`, NOT `fs.status`: kube-rs's
    // cached `Arc<PangeaFleetStatus>` lags the apiserver, so when
    // our PATCH triggers a watch event the reconcile fires before
    // the cache observes the just-patched value — `fs.status` would
    // be the previous-previous status and the gate would never fire.
    // Tracking last-patched explicitly in Context is single-writer
    // (this controller) so no race.
    let mut last = ctx.last_patched.lock().await;
    if !fleet_status_changed(last.as_ref(), &new_status) {
        debug!(
            "PangeaFleetStatus aggregation unchanged; skipping patch \
             (avoids self-trigger watch loop on top of 30s refresh)"
        );
        return Ok(Action::requeue(REFRESH_INTERVAL));
    }

    patch_status(&ctx.client, &new_status).await?;
    *last = Some(new_status);
    Ok(Action::requeue(REFRESH_INTERVAL))
}

/// Returns `true` iff the proposed fleet aggregation differs from
/// `prev` on any *substantive* field — i.e., any per-CRD-class
/// tracker. The `last_updated_at` timestamp and the `Updated`
/// condition's `last_transition_time` are intentionally excluded:
/// `aggregate_fleet_status` always restamps both, so a naive
/// equality check would never gate. Trackers all derive `PartialEq`,
/// so this is a straight field-by-field compare.
fn fleet_status_changed(
    prev: Option<&PangeaFleetStatusStatus>,
    new: &PangeaFleetStatusStatus,
) -> bool {
    let Some(p) = prev else { return true };
    p.workspace_catalogs != new.workspace_catalogs
        || p.infrastructure_templates != new.infrastructure_templates
        || p.architecture_gems != new.architecture_gems
        || p.pangea_namespaces != new.pangea_namespaces
        || p.image_pipelines != new.image_pipelines
        || p.packer_builds != new.packer_builds
        || p.ami_tests != new.ami_tests
        || p.compliance_schedules != new.compliance_schedules
        || p.compliance_bindings != new.compliance_bindings
        || p.infrastructure_flows != new.infrastructure_flows
        || p.pangea_dashboards != new.pangea_dashboards
        || p.synthesizer_formats != new.synthesizer_formats
}

/// List every tracked pangea CRD class and return a populated
/// `PangeaFleetStatusStatus`. Each list call is bounded by the
/// kube-apiserver — listing thousands of CRs across 12 classes runs
/// in a few hundred ms on a healthy cluster.
async fn aggregate_fleet_status(client: &Client) -> Result<PangeaFleetStatusStatus, FleetError> {
    let lp = ListParams::default();

    // ── WorkspaceCatalog ──
    let wcat_api: Api<WorkspaceCatalog> = Api::all(client.clone());
    let wcats = wcat_api.list(&lp).await?;
    let workspace_catalogs = aggregate_workspace_catalogs(&wcats.items);

    // ── InfrastructureTemplate ──
    let it_api: Api<InfrastructureTemplate> = Api::all(client.clone());
    let templates = it_api.list(&lp).await?;
    let infrastructure_templates = aggregate_templates(&templates.items);

    // ── ArchitectureGem ──
    let agem_api: Api<ArchitectureGem> = Api::all(client.clone());
    let gems = agem_api.list(&lp).await?;
    let architecture_gems = aggregate_architecture_gems(&gems.items);

    // ── PangeaNamespace ──
    let pns_api: Api<PangeaNamespace> = Api::all(client.clone());
    let namespaces = pns_api.list(&lp).await?;
    let pangea_namespaces = aggregate_pangea_namespaces(&namespaces.items);

    // ── ImagePipeline ──
    let ip_api: Api<ImagePipeline> = Api::all(client.clone());
    let pipelines = ip_api.list(&lp).await?;
    let image_pipelines = histogram_from(
        &pipelines.items,
        |p| p.status.as_ref().and_then(|s| s.phase.as_ref().map(|x| format!("{:?}", x))),
    );

    // ── PackerBuild ──
    let pb_api: Api<PackerBuild> = Api::all(client.clone());
    let builds = pb_api.list(&lp).await?;
    let packer_builds = histogram_from(
        &builds.items,
        |b: &PackerBuild| {
            b.status.as_ref().and_then(|s| s.phase.as_ref().map(|x| format!("{:?}", x)))
        },
    );

    // ── AmiTest ──
    let at_api: Api<AmiTest> = Api::all(client.clone());
    let tests = at_api.list(&lp).await?;
    let ami_tests = histogram_from(
        &tests.items,
        |t: &AmiTest| {
            t.status.as_ref().and_then(|s| s.phase.as_ref().map(|x| format!("{:?}", x)))
        },
    );

    // ── ComplianceSchedule ──
    let cs_api: Api<ComplianceSchedule> = Api::all(client.clone());
    let schedules = cs_api.list(&lp).await?;
    let compliance_schedules = histogram_from(
        &schedules.items,
        |s: &ComplianceSchedule| {
            s.status.as_ref().and_then(|st| st.phase.as_ref().map(|x| format!("{:?}", x)))
        },
    );

    // ── ComplianceBinding ──
    let cb_api: Api<ComplianceBinding> = Api::all(client.clone());
    let bindings = cb_api.list(&lp).await?;
    let compliance_bindings = aggregate_compliance_bindings(&bindings.items);

    // ── InfrastructureFlow ──
    let if_api: Api<InfrastructureFlow> = Api::all(client.clone());
    let flows = if_api.list(&lp).await?;
    let infrastructure_flows = histogram_from(
        &flows.items,
        |f: &InfrastructureFlow| {
            f.status.as_ref().and_then(|s| s.phase.as_ref().map(|x| format!("{:?}", x)))
        },
    );

    // ── PangeaDashboard ──
    let pd_api: Api<PangeaDashboard> = Api::all(client.clone());
    let dashboards = pd_api.list(&lp).await?;
    let pangea_dashboards = histogram_from(
        &dashboards.items,
        |d: &PangeaDashboard| {
            d.status.as_ref().and_then(|s| s.phase.as_ref().map(|x| format!("{:?}", x)))
        },
    );

    // ── SynthesizerFormat ──
    let sf_api: Api<SynthesizerFormat> = Api::all(client.clone());
    let formats = sf_api.list(&lp).await?;
    let synthesizer_formats = histogram_from(
        &formats.items,
        |f: &SynthesizerFormat| {
            f.status.as_ref().and_then(|s| s.phase.as_ref().map(|x| format!("{:?}", x)))
        },
    );

    // Phase enum unused warnings are fine — they're imported above for
    // reference and may be used in future tests.
    let _ = (
        std::marker::PhantomData::<TemplatePhase>,
        std::marker::PhantomData::<ImagePipelinePhase>,
        std::marker::PhantomData::<PackerBuildPhase>,
        std::marker::PhantomData::<AmiTestPhase>,
        std::marker::PhantomData::<ComplianceSchedulePhase>,
        std::marker::PhantomData::<PangeaDashboardPhase>,
        std::marker::PhantomData::<SynthesizerFormatPhase>,
        std::marker::PhantomData::<BindingComplianceState>,
    );

    Ok(PangeaFleetStatusStatus {
        workspace_catalogs,
        infrastructure_templates,
        architecture_gems,
        pangea_namespaces,
        image_pipelines,
        packer_builds,
        ami_tests,
        compliance_schedules,
        compliance_bindings,
        infrastructure_flows,
        pangea_dashboards,
        synthesizer_formats,
        last_updated_at: Some(Utc::now()),
        conditions: vec![crate::crd::Condition {
            r#type: "Updated".into(),
            status: "True".into(),
            last_transition_time: Utc::now(),
            reason: "FleetAggregated".into(),
            message: "Fleet status refreshed successfully.".into(),
        }],
    })
}

// ─── Pure aggregation helpers (testable without a kube client) ───────

/// Tally WorkspaceCatalogs into the typed shape. Pre-formats a
/// "verified/total" summary string for the printer column.
pub fn aggregate_workspace_catalogs(items: &[WorkspaceCatalog]) -> WorkspaceCatalogTracking {
    let total = items.len() as u32;
    let verified = items
        .iter()
        .filter(|c| c.status.as_ref().map(|s| s.verified).unwrap_or(false))
        .count() as u32;
    WorkspaceCatalogTracking {
        total,
        verified,
        summary: format!("{}/{}", verified, total),
    }
}

/// Tally InfrastructureTemplates into the typed shape with full
/// per-phase histogram + ready/failed/auto-suspended counts.
pub fn aggregate_templates(items: &[InfrastructureTemplate]) -> TemplateTracking {
    let total = items.len() as u32;
    let mut by_phase: BTreeMap<String, u32> = BTreeMap::new();
    let mut ready = 0u32;
    let mut failed = 0u32;
    let mut auto_suspended = 0u32;
    for t in items {
        let phase_str = t
            .status
            .as_ref()
            .and_then(|s| s.phase.as_ref().map(|p| format!("{:?}", p)))
            .unwrap_or_else(|| "Unknown".into());
        *by_phase.entry(phase_str.clone()).or_insert(0) += 1;
        match phase_str.as_str() {
            "Ready" => ready += 1,
            "Failed" => failed += 1,
            _ => {}
        }
        if t.status.as_ref().map(|s| s.auto_suspended).unwrap_or(false) {
            auto_suspended += 1;
        }
    }
    TemplateTracking {
        total,
        ready,
        failed,
        auto_suspended,
        by_phase,
    }
}

/// Tally ArchitectureGems with a "loaded/total" summary string.
pub fn aggregate_architecture_gems(items: &[ArchitectureGem]) -> ArchitectureGemTracking {
    let total = items.len() as u32;
    let mut loaded = 0u32;
    let mut failed = 0u32;
    for g in items {
        if let Some(phase_str) = g
            .status
            .as_ref()
            .and_then(|s| s.phase.as_ref().map(|p| format!("{:?}", p)))
        {
            match phase_str.as_str() {
                "Loaded" => loaded += 1,
                "Failed" => failed += 1,
                _ => {}
            }
        }
    }
    ArchitectureGemTracking {
        total,
        loaded,
        failed,
        summary: format!("{}/{}", loaded, total),
    }
}

/// Tally PangeaNamespaces. `ready` counts namespaces whose status
/// reports `backend_ready=true`.
pub fn aggregate_pangea_namespaces(items: &[PangeaNamespace]) -> PangeaNamespaceTracking {
    let total = items.len() as u32;
    let ready = items
        .iter()
        .filter(|n| n.status.as_ref().map(|s| s.backend_ready).unwrap_or(false))
        .count() as u32;
    PangeaNamespaceTracking { total, ready }
}

/// Tally ComplianceBindings. `gated` counts bindings currently
/// gating their targets due to NonCompliant compliance state.
pub fn aggregate_compliance_bindings(items: &[ComplianceBinding]) -> ComplianceBindingTracking {
    let total = items.len() as u32;
    let gated = items
        .iter()
        .filter(|b| {
            matches!(
                b.status
                    .as_ref()
                    .and_then(|s| s.compliance_state),
                Some(BindingComplianceState::NonCompliant)
            )
        })
        .count() as u32;
    ComplianceBindingTracking { total, gated }
}

/// Generic per-phase histogram builder. Pass a closure that pulls
/// the phase string out of an item's status (returning None when
/// the status hasn't been observed yet).
pub fn histogram_from<T>(
    items: &[T],
    phase_of: impl Fn(&T) -> Option<String>,
) -> PhaseHistogramTracking {
    let total = items.len() as u32;
    let mut by_phase: BTreeMap<String, u32> = BTreeMap::new();
    for item in items {
        let phase = phase_of(item).unwrap_or_else(|| "Unknown".into());
        *by_phase.entry(phase).or_insert(0) += 1;
    }
    PhaseHistogramTracking { total, by_phase }
}

// ─── Status patch ────────────────────────────────────────────────────

async fn patch_status(
    client: &Client,
    status: &PangeaFleetStatusStatus,
) -> Result<(), FleetError> {
    let api: Api<PangeaFleetStatus> = Api::all(client.clone());
    let patch = serde_json::json!({ "status": status });
    api.patch_status(
        PANGEA_FLEET_STATUS_SINGLETON,
        &PatchParams::apply("pangea-operator"),
        &Patch::Merge(&patch),
    )
    .await?;
    info!(
        templates = status.infrastructure_templates.total,
        wcats = status.workspace_catalogs.total,
        gems = status.architecture_gems.total,
        "PangeaFleetStatus refreshed"
    );
    Ok(())
}

/// Self-create the singleton CR if it doesn't exist. Idempotent —
/// returning Ok on 409 conflict (another instance won the create
/// race).
async fn ensure_singleton(client: &Client) -> Result<(), kube::Error> {
    let api: Api<PangeaFleetStatus> = Api::all(client.clone());
    if api
        .get_opt(PANGEA_FLEET_STATUS_SINGLETON)
        .await?
        .is_some()
    {
        return Ok(());
    }
    let cr = PangeaFleetStatus::new(PANGEA_FLEET_STATUS_SINGLETON, PangeaFleetStatusSpec {});
    match api.create(&PostParams::default(), &cr).await {
        Ok(_) => {
            info!(
                singleton = PANGEA_FLEET_STATUS_SINGLETON,
                "Created PangeaFleetStatus singleton"
            );
            Ok(())
        }
        Err(kube::Error::Api(ref e)) if e.code == 409 => {
            // Lost the create race with another operator replica;
            // that's fine, the singleton exists now.
            Ok(())
        }
        Err(e) => Err(e),
    }
}

fn error_policy(
    _obj: Arc<PangeaFleetStatus>,
    error: &FleetError,
    _ctx: Arc<Context>,
) -> Action {
    warn!(error = %error, "PangeaFleetStatus error policy triggered");
    Action::requeue(REFRESH_INTERVAL)
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── fleet_status_changed (diff-gate) tests ─────────────────
    //
    // The gate must compare every per-CRD-class tracker but
    // INTENTIONALLY ignore `last_updated_at` and the `Updated`
    // condition's `last_transition_time` — both are restamped on
    // every aggregation pass. Without that exclusion, the gate
    // would never fire and the self-trigger watch loop returns.

    fn empty_status() -> PangeaFleetStatusStatus {
        PangeaFleetStatusStatus {
            workspace_catalogs: Default::default(),
            infrastructure_templates: Default::default(),
            architecture_gems: Default::default(),
            pangea_namespaces: Default::default(),
            image_pipelines: Default::default(),
            packer_builds: Default::default(),
            ami_tests: Default::default(),
            compliance_schedules: Default::default(),
            compliance_bindings: Default::default(),
            infrastructure_flows: Default::default(),
            pangea_dashboards: Default::default(),
            synthesizer_formats: Default::default(),
            last_updated_at: Some(chrono::Utc::now()),
            conditions: vec![],
        }
    }

    #[test]
    fn fleet_status_first_reconcile_must_patch() {
        let new = empty_status();
        assert!(
            fleet_status_changed(None, &new),
            "missing prev (first reconcile) must always force a PATCH"
        );
    }

    #[test]
    fn fleet_status_only_timestamp_change_skips_patch() {
        use chrono::TimeZone;
        // The rio loop case: every aggregation pass restamps
        // last_updated_at + the Updated condition's transition time,
        // but the per-CRD trackers are byte-equal. Gate must skip.
        let mut prev = empty_status();
        let mut new = empty_status();
        // Different timestamps — but the gate must ignore them.
        prev.last_updated_at =
            Some(chrono::Utc.with_ymd_and_hms(2026, 5, 7, 0, 0, 0).unwrap());
        new.last_updated_at =
            Some(chrono::Utc.with_ymd_and_hms(2026, 5, 7, 0, 0, 30).unwrap());
        assert!(
            !fleet_status_changed(Some(&prev), &new),
            "timestamp-only churn must NOT trigger a PATCH (the loop bug)"
        );
    }

    #[test]
    fn fleet_status_template_count_change_must_patch() {
        let prev = empty_status();
        let mut new = empty_status();
        new.infrastructure_templates.total = 1;
        assert!(
            fleet_status_changed(Some(&prev), &new),
            "template count change must force a PATCH"
        );
    }

    #[test]
    fn fleet_status_workspace_catalog_change_must_patch() {
        let prev = empty_status();
        let mut new = empty_status();
        new.workspace_catalogs.total = 2;
        new.workspace_catalogs.verified = 1;
        new.workspace_catalogs.summary = "1/2".into();
        assert!(
            fleet_status_changed(Some(&prev), &new),
            "workspace catalog tracker change must force a PATCH"
        );
    }

    // ─── Aggregation helpers ───
    //
    // All pure: feed lists of CRs (built via JSON for compactness),
    // assert the aggregation. No kube::Client required.

    fn fake_template(phase: &str, auto_suspended: bool) -> InfrastructureTemplate {
        let payload = serde_json::json!({
            "apiVersion": "pangea.pleme.io/v1alpha1",
            "kind": "InfrastructureTemplate",
            "metadata": { "name": "x", "namespace": "n" },
            "spec": {
                "source": { "raw": "" },
                "pangeaNamespace": "default"
            },
            "status": {
                "phase": phase,
                "autoSuspended": auto_suspended,
                "observedGeneration": 0,
                "cycleCount": 0,
                "failureCount": 0,
                "consecutiveCompileFailures": 0,
                "consecutiveDriftCycles": 0,
                "stuckResources": [],
                "driftDetails": [],
                "conditions": []
            }
        });
        serde_json::from_value(payload).expect("fake template parses")
    }

    #[test]
    fn aggregate_templates_counts_phase_histogram_and_ready_failed() {
        let items = vec![
            fake_template("Ready", false),
            fake_template("Ready", false),
            fake_template("Failed", false),
            fake_template("Compiling", false),
            fake_template("Failed", true),
        ];
        let agg = aggregate_templates(&items);
        assert_eq!(agg.total, 5);
        assert_eq!(agg.ready, 2, "two Ready templates");
        assert_eq!(agg.failed, 2, "two Failed templates");
        assert_eq!(agg.auto_suspended, 1, "one auto-suspended");
        assert_eq!(agg.by_phase.get("Ready"), Some(&2));
        assert_eq!(agg.by_phase.get("Failed"), Some(&2));
        assert_eq!(agg.by_phase.get("Compiling"), Some(&1));
    }

    #[test]
    fn aggregate_templates_handles_empty_list() {
        let agg = aggregate_templates(&[]);
        assert_eq!(agg.total, 0);
        assert_eq!(agg.ready, 0);
        assert_eq!(agg.failed, 0);
        assert!(agg.by_phase.is_empty());
    }

    #[test]
    fn aggregate_templates_treats_missing_status_as_unknown_phase() {
        // Defensive: a template that hasn't been reconciled yet has
        // no status. The aggregator must classify it under
        // "Unknown" rather than crashing or skipping.
        let payload = serde_json::json!({
            "apiVersion": "pangea.pleme.io/v1alpha1",
            "kind": "InfrastructureTemplate",
            "metadata": { "name": "fresh", "namespace": "n" },
            "spec": {
                "source": { "raw": "" },
                "pangeaNamespace": "default"
            }
        });
        let t: InfrastructureTemplate = serde_json::from_value(payload).unwrap();
        let agg = aggregate_templates(&[t]);
        assert_eq!(agg.total, 1);
        assert_eq!(agg.by_phase.get("Unknown"), Some(&1));
    }

    fn fake_workspace_catalog(verified: bool) -> WorkspaceCatalog {
        let payload = serde_json::json!({
            "apiVersion": "pangea.pleme.io/v1alpha1",
            "kind": "WorkspaceCatalog",
            "metadata": { "name": "x" },
            "spec": {
                "source": {
                    "gitRepository": {
                        "url": "https://example.invalid/r",
                        "ref": "main",
                        "path": "workspaces/x"
                    }
                },
                "requiredGems": []
            },
            "status": {
                "verified": verified,
                "templateCount": 0,
                "conditions": []
            }
        });
        serde_json::from_value(payload).expect("fake wcat parses")
    }

    #[test]
    fn aggregate_workspace_catalogs_summary_string_matches_total_and_verified() {
        let items = vec![
            fake_workspace_catalog(true),
            fake_workspace_catalog(false),
            fake_workspace_catalog(true),
        ];
        let agg = aggregate_workspace_catalogs(&items);
        assert_eq!(agg.total, 3);
        assert_eq!(agg.verified, 2);
        // Pre-formatted summary string drives the printer column.
        assert_eq!(agg.summary, "2/3");
    }

    #[test]
    fn aggregate_workspace_catalogs_zero_total_yields_zero_over_zero() {
        let agg = aggregate_workspace_catalogs(&[]);
        assert_eq!(agg.total, 0);
        assert_eq!(agg.verified, 0);
        assert_eq!(agg.summary, "0/0");
    }

    fn fake_compliance_binding(state: Option<&str>) -> ComplianceBinding {
        let mut payload = serde_json::json!({
            "apiVersion": "pangea.pleme.io/v1alpha1",
            "kind": "ComplianceBinding",
            "metadata": { "name": "b", "namespace": "n" },
            "spec": {
                "complianceRef": { "name": "cs1" },
                "targets": [],
                "enforcement": "gate",
                "reactions": []
            }
        });
        if let Some(st) = state {
            payload["status"] = serde_json::json!({
                "complianceState": st,
                "targets": [],
                "targetCount": 0,
                "conditions": []
            });
        }
        serde_json::from_value(payload).expect("fake binding parses")
    }

    #[test]
    fn aggregate_compliance_bindings_counts_only_noncompliant_as_gated() {
        let items = vec![
            fake_compliance_binding(Some("compliant")),
            fake_compliance_binding(Some("nonCompliant")),
            fake_compliance_binding(Some("nonCompliant")),
            fake_compliance_binding(Some("unknown")),
            fake_compliance_binding(None), // no status yet
        ];
        let agg = aggregate_compliance_bindings(&items);
        assert_eq!(agg.total, 5);
        assert_eq!(agg.gated, 2);
    }

    #[test]
    fn histogram_from_works_for_arbitrary_typed_lists() {
        // Generic shape — exercises the polymorphism, doesn't matter
        // what T actually is.
        struct Item { phase: Option<&'static str> }
        let items = vec![
            Item { phase: Some("A") },
            Item { phase: Some("A") },
            Item { phase: Some("B") },
            Item { phase: None },
        ];
        let agg = histogram_from(&items, |i| i.phase.map(String::from));
        assert_eq!(agg.total, 4);
        assert_eq!(agg.by_phase.get("A"), Some(&2));
        assert_eq!(agg.by_phase.get("B"), Some(&1));
        assert_eq!(agg.by_phase.get("Unknown"), Some(&1));
    }
}
