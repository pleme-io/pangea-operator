//! PangeaDashboard CRD — synthesizes Grafana dashboards from Ruby DSL.
//!
//! The controller compiles inline Ruby (via the compiler sidecar) into Grafana
//! dashboard JSON, then creates a ConfigMap + GrafanaDashboard CRD that the
//! Grafana Operator reconciles into the target Grafana instance.

use std::collections::BTreeMap;

use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use strum::Display;

use super::Condition;

/// PangeaDashboard synthesizes a Grafana dashboard from Pangea Ruby DSL.
#[derive(CustomResource, Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[kube(
    group = "pangea.pleme.io",
    version = "v1alpha1",
    kind = "PangeaDashboard",
    namespaced,
    status = "PangeaDashboardStatus",
    shortname = "pd",
    category = "pangea",
    printcolumn = r#"{"name":"Folder","type":"string","jsonPath":".spec.folder"}"#,
    printcolumn = r#"{"name":"Phase","type":"string","jsonPath":".status.phase"}"#,
    printcolumn = r#"{"name":"Dashboard","type":"string","jsonPath":".status.dashboardUid"}"#,
    printcolumn = r#"{"name":"Age","type":"date","jsonPath":".metadata.creationTimestamp"}"#
)]
#[serde(rename_all = "camelCase")]
pub struct PangeaDashboardSpec {
    /// Dashboard source — inline Ruby DSL or reference to a ConfigMap.
    pub source: DashboardSource,

    /// Label selector for the target Grafana instance.
    /// Must match the Grafana CR's labels (e.g., `dashboards: grafana`).
    #[serde(default)]
    pub grafana_instance_selector: BTreeMap<String, String>,

    /// Grafana folder to place the dashboard in.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub folder: Option<String>,

    /// Whether to overwrite an existing dashboard with the same UID.
    #[serde(default = "default_true")]
    pub overwrite: bool,

    /// Ruby modules to extend the synthesizer with before evaluation.
    /// Defaults to `["Pangea::Grafana"]` for dashboard builder access.
    #[serde(
        default = "default_extend_modules",
        skip_serializing_if = "Vec::is_empty"
    )]
    pub extend_modules: Vec<String>,

    /// Commit message for Grafana's built-in version tracking.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,

    /// When true, the dashboard controller skips synthesis + Grafana
    /// publish entirely. Compose with the cluster-scoped
    /// `OperatorPolicy/default` for fleet-wide pause.
    #[serde(default)]
    pub suspend: bool,
}

fn default_true() -> bool {
    true
}

fn default_extend_modules() -> Vec<String> {
    vec!["Pangea::Grafana".to_string()]
}

/// Where a dashboard's definition comes from.
///
/// Ordered weakest-containment first. `Inline { ruby }` hands arbitrary
/// Ruby to an in-process CRuby interpreter that shares the operator's
/// address space and credentials — anyone who can write a CR gets code
/// execution. `Inline { tlisp }` is a restricted evaluator with no
/// filesystem, network or subprocess reach. `Architecture` carries **no
/// language at all**: a name and a parameter object, resolved against
/// the catalogue baked into the image.
///
/// That last one is the destination for essentially every dashboard.
/// Surveyed 2026-08-11, 17 of the 18 `PangeaDashboard` CRs the fleet
/// emits carry no logic — their "Ruby" is a fixed six-line wrapper
/// around an architecture name, a params blob and a folder. For those,
/// code injection stops being *contained* and becomes *unrepresentable*,
/// because the schema has no field that can hold code.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum DashboardSource {
    /// Inline Ruby code that returns a Grafana dashboard JSON string.
    Inline {
        /// Ruby source code. Must return a JSON string (e.g., via DashboardBuilder.build).
        ruby: String,
    },
    /// Inline tatara-lisp: a `(deflava-dashboard …)` form.
    InlineTlisp {
        /// tatara-lisp source. Evaluated by the lava backend.
        tlisp: String,
    },
    /// A named architecture from the image's catalogue, plus its
    /// parameters. Carries no evaluable source.
    Architecture {
        /// Catalogue entry name, e.g. `workload-overview`.
        name: String,
        /// Parameters bound into the architecture.
        #[serde(default)]
        #[schemars(schema_with = "super::opaque_json_schema")]
        params: Option<serde_json::Value>,
    },
    /// Reference to a ConfigMap containing the source.
    ConfigMapRef {
        /// ConfigMap name.
        name: String,
        /// Key within the ConfigMap.
        key: String,
    },
}

/// Which evaluator a source needs.
///
/// The controller cannot otherwise tell a backend what it is holding —
/// `render_dashboard` receives a bare string — so routing would have to
/// sniff the text. This makes it typed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DashboardLanguage {
    Ruby,
    Tlisp,
}

impl DashboardSource {
    /// The language of this source, for backend dispatch.
    ///
    /// A `ConfigMapRef` is Ruby for backward compatibility: existing
    /// ConfigMaps hold Ruby, and silently reinterpreting them as
    /// tatara-lisp would fail at parse rather than doing nothing.
    #[must_use]
    pub fn language(&self) -> DashboardLanguage {
        match self {
            Self::Inline { .. } | Self::ConfigMapRef { .. } => DashboardLanguage::Ruby,
            Self::InlineTlisp { .. } | Self::Architecture { .. } => DashboardLanguage::Tlisp,
        }
    }
}

// ---------------------------------------------------------------------------
// Status
// ---------------------------------------------------------------------------

/// Lifecycle phase of a PangeaDashboard.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema, Display, Default,
)]
pub enum PangeaDashboardPhase {
    /// Awaiting synthesis.
    #[default]
    Pending,
    /// Ruby DSL is being compiled by the sidecar.
    Synthesizing,
    /// ConfigMap and GrafanaDashboard CRD created successfully.
    Ready,
    /// Compilation or resource creation failed.
    Failed,
}

/// Status of a PangeaDashboard.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct PangeaDashboardStatus {
    /// Current lifecycle phase.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub phase: Option<PangeaDashboardPhase>,

    /// Name of the generated ConfigMap containing dashboard JSON.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub config_map_name: Option<String>,

    /// Name of the generated GrafanaDashboard CRD.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub grafana_dashboard_name: Option<String>,

    /// UID of the dashboard in Grafana (extracted from synthesized JSON).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dashboard_uid: Option<String>,

    /// Kubernetes-style conditions.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub conditions: Vec<Condition>,

    /// Last observed generation.
    #[serde(default)]
    pub observed_generation: i64,

    /// Error message if phase is Failed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,

    /// SHA-256 hash of the last successfully compiled Ruby source.
    /// Used for change detection to avoid unnecessary recompilations.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_hash: Option<String>,
}
