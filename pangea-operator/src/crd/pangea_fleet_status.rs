//! Cluster-scoped fleet aggregation singleton.
//!
//! `PangeaFleetStatus/default` is the operator's running rollup of how
//! many resources of each pangea CRD class exist on the cluster, what
//! phases they're in, and how many are healthy. Authored by the
//! `fleet_status_controller`, never by users.
//!
//! Why a separate CR rather than extending `OperatorPolicy.status`:
//! the policy CR is user-authored (`globalSuspend`, `controllerSuspend`,
//! `workspaceSuspend`); polluting its status with operator-driven
//! aggregations would mix two concerns at the printer-column tier.
//! Keeping fleet observability in its own singleton makes
//! `kubectl get pangeafleetstatus default` the canonical at-a-glance
//! command, leaves OperatorPolicy lean, and means the aggregator
//! controller can refresh the status independent of any policy
//! reconciliation.
//!
//! The CR has an empty (operator-driven) spec — there is nothing for
//! a user to configure. The status is what observers read.

use chrono::{DateTime, Utc};
use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::Condition;

/// Cluster-scoped operator-authored fleet aggregation singleton.
///
/// The operator self-creates `PangeaFleetStatus/default` at startup
/// and refreshes its status periodically (every 30s) and on watch
/// events from any tracked pangea CRD. Bypasses the OperatorPolicy
/// kill-switch — fleet status is read-only observability and is
/// especially useful while the operator is paused.
#[derive(CustomResource, Debug, Clone, Default, Serialize, Deserialize, JsonSchema, PartialEq)]
#[kube(
    group = "pangea.pleme.io",
    version = "v1alpha1",
    kind = "PangeaFleetStatus",
    plural = "pangeafleetstatuses",
    shortname = "pfs",
    category = "pangea",
    status = "PangeaFleetStatusStatus",
    printcolumn = r#"{"name":"WCats","type":"string","jsonPath":".status.workspaceCatalogs.summary"}"#,
    printcolumn = r#"{"name":"Templates","type":"integer","jsonPath":".status.infrastructureTemplates.total"}"#,
    printcolumn = r#"{"name":"Ready","type":"integer","jsonPath":".status.infrastructureTemplates.ready"}"#,
    printcolumn = r#"{"name":"Failed","type":"integer","jsonPath":".status.infrastructureTemplates.failed"}"#,
    printcolumn = r#"{"name":"Gems","type":"string","jsonPath":".status.architectureGems.summary"}"#,
    printcolumn = r#"{"name":"Namespaces","type":"integer","jsonPath":".status.pangeaNamespaces.total"}"#,
    printcolumn = r#"{"name":"Updated","type":"date","jsonPath":".status.lastUpdatedAt"}"#,
    printcolumn = r#"{"name":"Age","type":"date","jsonPath":".metadata.creationTimestamp"}"#
)]
#[serde(rename_all = "camelCase")]
pub struct PangeaFleetStatusSpec {
    // Empty — operator-driven, not user-authored. The struct exists
    // because kube-rs CustomResource requires a Spec type; users
    // should leave this object alone.
}

/// Status fields populated by `fleet_status_controller`.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct PangeaFleetStatusStatus {
    /// WorkspaceCatalog rollup.
    #[serde(default)]
    pub workspace_catalogs: WorkspaceCatalogTracking,

    /// InfrastructureTemplate rollup with per-phase histogram.
    #[serde(default)]
    pub infrastructure_templates: TemplateTracking,

    /// ArchitectureGem rollup.
    #[serde(default)]
    pub architecture_gems: ArchitectureGemTracking,

    /// PangeaNamespace rollup.
    #[serde(default)]
    pub pangea_namespaces: PangeaNamespaceTracking,

    /// ImagePipeline rollup with per-phase histogram.
    #[serde(default)]
    pub image_pipelines: PhaseHistogramTracking,

    /// PackerBuild rollup with per-phase histogram.
    #[serde(default)]
    pub packer_builds: PhaseHistogramTracking,

    /// AmiTest rollup with per-phase histogram.
    #[serde(default)]
    pub ami_tests: PhaseHistogramTracking,

    /// ComplianceSchedule rollup with per-phase histogram.
    #[serde(default)]
    pub compliance_schedules: PhaseHistogramTracking,

    /// ComplianceBinding rollup. `gated` counts bindings whose
    /// status.complianceState is currently NonCompliant.
    #[serde(default)]
    pub compliance_bindings: ComplianceBindingTracking,

    /// InfrastructureFlow rollup with per-phase histogram.
    #[serde(default)]
    pub infrastructure_flows: PhaseHistogramTracking,

    /// PangeaDashboard rollup with per-phase histogram.
    #[serde(default)]
    pub pangea_dashboards: PhaseHistogramTracking,

    /// SynthesizerFormat rollup with per-phase histogram.
    #[serde(default)]
    pub synthesizer_formats: PhaseHistogramTracking,

    /// When the controller last refreshed this status.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_updated_at: Option<DateTime<Utc>>,

    /// Standard conditions. The fleet-status controller emits
    /// `Updated=True` after a successful refresh.
    #[serde(default)]
    pub conditions: Vec<Condition>,
}

/// WorkspaceCatalog aggregation. `summary` is a printer-column-friendly
/// "verified/total" string (e.g. "1/1").
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct WorkspaceCatalogTracking {
    /// Total catalogs in the cluster.
    #[serde(default)]
    pub total: u32,
    /// Catalogs with `status.verified == true`.
    #[serde(default)]
    pub verified: u32,
    /// Pre-formatted "verified/total" string for printer columns.
    /// Computed by the controller, not by users.
    #[serde(default)]
    pub summary: String,
}

/// InfrastructureTemplate aggregation with per-phase histogram +
/// healthy/auto-suspended counts. Carries `ready` and `failed` as
/// dedicated fields so printer columns can surface them directly.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct TemplateTracking {
    /// Total InfrastructureTemplates in the cluster.
    #[serde(default)]
    pub total: u32,
    /// Templates whose `.status.phase` is `Ready`.
    #[serde(default)]
    pub ready: u32,
    /// Templates whose `.status.phase` is `Failed`.
    #[serde(default)]
    pub failed: u32,
    /// Templates whose `.status.autoSuspended` is true.
    #[serde(default)]
    pub auto_suspended: u32,
    /// Per-phase histogram. Keys are phase names (Pending, Compiling,
    /// Initializing, Planning, Applying, Verifying, Verified, Ready,
    /// Drifted, Failed, Destroying). Counts include the dedicated
    /// fields above — this is the full distribution.
    #[serde(default)]
    pub by_phase: std::collections::BTreeMap<String, u32>,
}

/// ArchitectureGem aggregation.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ArchitectureGemTracking {
    /// Total gems in the cluster.
    #[serde(default)]
    pub total: u32,
    /// Gems whose `.status.phase == Loaded`.
    #[serde(default)]
    pub loaded: u32,
    /// Gems whose `.status.phase == Failed`.
    #[serde(default)]
    pub failed: u32,
    /// Pre-formatted "loaded/total" string for printer columns.
    #[serde(default)]
    pub summary: String,
}

/// PangeaNamespace aggregation.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct PangeaNamespaceTracking {
    /// Total namespaces in the cluster.
    #[serde(default)]
    pub total: u32,
    /// Namespaces whose `.status.backendReady == true`.
    #[serde(default)]
    pub ready: u32,
}

/// Generic phase-histogram tracking shape used by the secondary CRDs
/// (image_pipelines / packer_builds / ami_tests / compliance_schedules
/// / infrastructure_flows / pangea_dashboards / synthesizer_formats).
/// Each of those CRDs has a status.phase enum but doesn't need
/// dedicated ready/failed printer columns at the fleet level —
/// `kubectl describe` surfaces the full histogram on demand.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct PhaseHistogramTracking {
    /// Total CRs of this kind.
    #[serde(default)]
    pub total: u32,
    /// Per-phase histogram. Empty when total is zero.
    #[serde(default)]
    pub by_phase: std::collections::BTreeMap<String, u32>,
}

/// ComplianceBinding aggregation. `gated` counts bindings whose
/// `.status.complianceState == NonCompliant` and are therefore
/// actively gating their targets.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ComplianceBindingTracking {
    /// Total bindings in the cluster.
    #[serde(default)]
    pub total: u32,
    /// Bindings currently gating targets due to non-compliance.
    #[serde(default)]
    pub gated: u32,
}

/// Canonical singleton name. The operator only honors a CR named
/// exactly `default`; other names are rejected at reconcile.
pub const PANGEA_FLEET_STATUS_SINGLETON: &str = "default";

/// Returns true iff the given object name is the honored singleton.
pub fn is_singleton_name(name: &str) -> bool {
    name == PANGEA_FLEET_STATUS_SINGLETON
}

#[cfg(test)]
mod tests {
    use super::*;
    use kube::CustomResourceExt;

    #[test]
    fn singleton_name_is_default() {
        assert_eq!(PANGEA_FLEET_STATUS_SINGLETON, "default");
        assert!(is_singleton_name("default"));
        assert!(!is_singleton_name("other"));
        assert!(!is_singleton_name(""));
    }

    #[test]
    fn crd_renders_cluster_scoped() {
        let yaml = serde_yaml::to_string(&PangeaFleetStatus::crd()).expect("CRD yaml");
        assert!(yaml.contains("pangeafleetstatuses.pangea.pleme.io"));
        assert!(yaml.contains("scope: Cluster"));
        assert!(yaml.contains("- pangea"));
    }

    #[test]
    fn status_default_round_trips_through_serde() {
        // Empty status → JSON → status. The shape is operator-
        // authored, but we want defensive coverage in case a
        // sidecar tool reads the CR with a partial JSON view.
        let s = PangeaFleetStatusStatus::default();
        let j = serde_json::to_string(&s).expect("serialize");
        let back: PangeaFleetStatusStatus = serde_json::from_str(&j).expect("deserialize");
        // Compare via debug repr — `Condition` (in conditions vec) doesn't
        // impl PartialEq fleet-wide, so we can't use `assert_eq!`.
        assert_eq!(format!("{:?}", s), format!("{:?}", back));
    }

    #[test]
    fn template_tracking_phase_histogram_is_btreemap() {
        // BTreeMap preserves a stable key order in JSON output —
        // important so two reconciles with the same input produce
        // the same patch body and the content-equality guard
        // doesn't false-trigger.
        let mut t = TemplateTracking::default();
        t.by_phase.insert("Ready".into(), 2);
        t.by_phase.insert("Failed".into(), 2);
        let j1 = serde_json::to_string(&t).unwrap();
        let j2 = serde_json::to_string(&t).unwrap();
        assert_eq!(j1, j2);
    }
}
