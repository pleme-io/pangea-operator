//! PangeaAlert CRD — synthesizes alert-rule CRs from a Ruby DSL.
//!
//! Sibling of [`crate::crd::pangea_dashboard::PangeaDashboard`]: same
//! declare-once, operator-reconciled shape, different output. The
//! controller compiles inline Ruby (via the embedded magnus backend)
//! that builds a `Pangea::Alerts` AST and renders it into a
//! VictoriaMetrics `VMRule` (or Prometheus-operator `PrometheusRule`)
//! manifest, then upserts that rule CR — owner-referenced to the
//! PangeaAlert and stamped with a `dest=<destination>` routing label —
//! into the cluster.
//!
//! Where PangeaDashboard delivers a sidecar-labelled ConfigMap the
//! vm-stack Grafana sidecar loads, PangeaAlert delivers a rule CR the
//! vmalert / prometheus-operator stack loads. Both stay in YAML; both
//! diff-gate on `sourceHash`; both mirror the Pending → Synthesizing →
//! Ready/Failed FSM.

use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use strum::Display;

use super::Condition;

/// PangeaAlert synthesizes an alert-rule CR from Pangea Ruby DSL.
#[derive(CustomResource, Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[kube(
    group = "pangea.pleme.io",
    version = "v1alpha1",
    kind = "PangeaAlert",
    namespaced,
    status = "PangeaAlertStatus",
    shortname = "pa",
    category = "pangea",
    printcolumn = r#"{"name":"Backend","type":"string","jsonPath":".spec.backend"}"#,
    printcolumn = r#"{"name":"Dest","type":"string","jsonPath":".spec.routing.destination"}"#,
    printcolumn = r#"{"name":"Phase","type":"string","jsonPath":".status.phase"}"#,
    printcolumn = r#"{"name":"Rule","type":"string","jsonPath":".status.renderedResourceName"}"#,
    printcolumn = r#"{"name":"Age","type":"date","jsonPath":".metadata.creationTimestamp"}"#
)]
#[serde(rename_all = "camelCase")]
pub struct PangeaAlertSpec {
    /// Alert source — inline Ruby DSL or reference to a ConfigMap.
    /// The inline Ruby builds a `Pangea::Alerts` AST (its last
    /// expression); the operator renders it per `spec.backend`.
    pub source: AlertSource,

    /// Which alert-rule CRD the AST renders into. `vmrule` (default)
    /// emits a VictoriaMetrics `operator.victoriametrics.com/v1beta1
    /// VMRule`; `prometheusrule` emits a
    /// `monitoring.coreos.com/v1 PrometheusRule`. The two are spec-
    /// interchangeable (vmalert reads both); the field is the typed
    /// seam that selects the target CRD.
    #[serde(default)]
    pub backend: AlertBackend,

    /// Routing metadata stamped onto the rendered rule CR. The
    /// `destination` becomes a `dest=<destination>` label so alert
    /// pipelines can select rules by owner without parsing the expr.
    #[serde(default)]
    pub routing: AlertRouting,

    /// Ruby modules to `require` before evaluating the inline DSL.
    /// Defaults to `["Pangea::Alerts"]` for alert-builder access.
    #[serde(
        default = "default_extend_modules",
        skip_serializing_if = "Vec::is_empty"
    )]
    pub extend_modules: Vec<String>,

    /// When true, the alert controller skips synthesis + rule publish
    /// entirely. Compose with the cluster-scoped
    /// `OperatorPolicy/default` for fleet-wide pause.
    #[serde(default)]
    pub suspend: bool,
}

fn default_extend_modules() -> Vec<String> {
    vec!["Pangea::Alerts".to_string()]
}

/// Source of the alert Ruby DSL. Mirrors
/// [`crate::crd::pangea_dashboard::DashboardSource`].
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum AlertSource {
    /// Inline Ruby code that builds a `Pangea::Alerts` AST as its last
    /// expression (or a pre-rendered rule-manifest Hash/JSON string).
    Inline {
        /// Ruby source. Its last expression must evaluate to a
        /// `Pangea::Alerts::Types::Alerts` AST (built via the
        /// `Pangea::Alerts::DSL::AlertsBuilder`), which the operator
        /// renders per `spec.backend`.
        ruby: String,
    },
    /// Reference to a ConfigMap containing the Ruby source.
    ConfigMapRef {
        /// ConfigMap name.
        name: String,
        /// Key within the ConfigMap.
        key: String,
    },
}

/// The alert-rule CRD an `AlertSource` renders into.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema, Display, Default,
)]
pub enum AlertBackend {
    /// `operator.victoriametrics.com/v1beta1 VMRule` — the rio default
    /// (rio runs the victoria-metrics-k8s-stack; vmalert reads VMRule).
    #[default]
    #[serde(rename = "vmrule")]
    #[strum(serialize = "vmrule")]
    VmRule,
    /// `monitoring.coreos.com/v1 PrometheusRule` — the prometheus-
    /// operator shape (spec-interchangeable with VMRule).
    #[serde(rename = "prometheusrule")]
    #[strum(serialize = "prometheusrule")]
    PrometheusRule,
}

impl AlertBackend {
    /// API group of the rendered rule CR.
    pub fn group(&self) -> &'static str {
        match self {
            AlertBackend::VmRule => "operator.victoriametrics.com",
            AlertBackend::PrometheusRule => "monitoring.coreos.com",
        }
    }

    /// API version (bare, no group) of the rendered rule CR.
    pub fn version(&self) -> &'static str {
        match self {
            AlertBackend::VmRule => "v1beta1",
            AlertBackend::PrometheusRule => "v1",
        }
    }

    /// Full `group/version` apiVersion of the rendered rule CR.
    pub fn api_version(&self) -> String {
        format!("{}/{}", self.group(), self.version())
    }

    /// Kind of the rendered rule CR.
    pub fn kind(&self) -> &'static str {
        match self {
            AlertBackend::VmRule => "VMRule",
            AlertBackend::PrometheusRule => "PrometheusRule",
        }
    }

    /// Resource plural of the rendered rule CR (for a dynamic Api).
    pub fn plural(&self) -> &'static str {
        match self {
            AlertBackend::VmRule => "vmrules",
            AlertBackend::PrometheusRule => "prometheusrules",
        }
    }

    /// The `Pangea::Alerts::Render` module the embedded backend picks
    /// to render the AST — the operator-side half of "render per
    /// spec.backend". `:victoria` for VMRule, `:prometheus` for
    /// PrometheusRule.
    pub fn render_backend(&self) -> crate::ruby::AlertRenderBackend {
        match self {
            AlertBackend::VmRule => crate::ruby::AlertRenderBackend::Victoria,
            AlertBackend::PrometheusRule => crate::ruby::AlertRenderBackend::Prometheus,
        }
    }
}

/// Routing metadata for the rendered rule CR.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct AlertRouting {
    /// Where this alert's notifications belong. Stamped onto the rule
    /// CR as a `dest=<destination>` label.
    #[serde(default)]
    pub destination: AlertDestination,
}

/// Alert routing destination. Stamped as the `dest=` label value.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema, Display, Default,
)]
pub enum AlertDestination {
    /// The operator's personal channel (default).
    #[default]
    #[serde(rename = "luis")]
    #[strum(serialize = "luis")]
    Luis,
    /// The akeyless on-call channel.
    #[serde(rename = "akeyless")]
    #[strum(serialize = "akeyless")]
    Akeyless,
    /// The 2f (second-factor / follow-the-sun) channel.
    #[serde(rename = "2f")]
    #[strum(serialize = "2f")]
    TwoF,
}

// ---------------------------------------------------------------------------
// Status
// ---------------------------------------------------------------------------

/// Lifecycle phase of a PangeaAlert. Mirrors
/// [`crate::crd::pangea_dashboard::PangeaDashboardPhase`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema, Display, Default)]
pub enum PangeaAlertPhase {
    /// Awaiting synthesis.
    #[default]
    Pending,
    /// Ruby DSL is being compiled + rendered.
    Synthesizing,
    /// Rule CR created/converged successfully.
    Ready,
    /// Compilation or rule creation failed.
    Failed,
}

/// Status of a PangeaAlert. Mirrors
/// [`crate::crd::pangea_dashboard::PangeaDashboardStatus`].
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct PangeaAlertStatus {
    /// Current lifecycle phase.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub phase: Option<PangeaAlertPhase>,

    /// Name of the generated rule CR (VMRule / PrometheusRule).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rendered_resource_name: Option<String>,

    /// Kind of the generated rule CR (`VMRule` / `PrometheusRule`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rendered_resource_kind: Option<String>,

    /// Kubernetes-style conditions.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub conditions: Vec<Condition>,

    /// Last observed generation.
    #[serde(default)]
    pub observed_generation: i64,

    /// Error message if phase is Failed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,

    /// BLAKE3 hash of the last successfully rendered Ruby source.
    /// Used for change detection to avoid unnecessary re-renders.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_hash: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backend_serde_is_lowercase() {
        assert_eq!(
            serde_json::to_string(&AlertBackend::VmRule).unwrap(),
            "\"vmrule\""
        );
        assert_eq!(
            serde_json::to_string(&AlertBackend::PrometheusRule).unwrap(),
            "\"prometheusrule\""
        );
    }

    #[test]
    fn backend_default_is_vmrule() {
        assert_eq!(AlertBackend::default(), AlertBackend::VmRule);
    }

    #[test]
    fn backend_gvk_matches_the_render_targets() {
        let vm = AlertBackend::VmRule;
        assert_eq!(vm.api_version(), "operator.victoriametrics.com/v1beta1");
        assert_eq!(vm.kind(), "VMRule");
        assert_eq!(vm.plural(), "vmrules");

        let prom = AlertBackend::PrometheusRule;
        assert_eq!(prom.api_version(), "monitoring.coreos.com/v1");
        assert_eq!(prom.kind(), "PrometheusRule");
        assert_eq!(prom.plural(), "prometheusrules");
    }

    #[test]
    fn destination_serde_includes_the_2f_rename() {
        assert_eq!(serde_json::to_string(&AlertDestination::Luis).unwrap(), "\"luis\"");
        assert_eq!(
            serde_json::to_string(&AlertDestination::Akeyless).unwrap(),
            "\"akeyless\""
        );
        assert_eq!(serde_json::to_string(&AlertDestination::TwoF).unwrap(), "\"2f\"");
        assert_eq!(AlertDestination::default(), AlertDestination::Luis);
    }

    #[test]
    fn destination_display_is_the_dest_label_value() {
        assert_eq!(AlertDestination::Luis.to_string(), "luis");
        assert_eq!(AlertDestination::Akeyless.to_string(), "akeyless");
        assert_eq!(AlertDestination::TwoF.to_string(), "2f");
    }

    #[test]
    fn phase_serde_round_trip() {
        for p in [
            PangeaAlertPhase::Pending,
            PangeaAlertPhase::Synthesizing,
            PangeaAlertPhase::Ready,
            PangeaAlertPhase::Failed,
        ] {
            let s = serde_json::to_string(&p).unwrap();
            let back: PangeaAlertPhase = serde_json::from_str(&s).unwrap();
            assert_eq!(format!("{:?}", p), format!("{:?}", back));
        }
    }

    #[test]
    fn status_default_round_trips() {
        let s = PangeaAlertStatus::default();
        let j = serde_json::to_string(&s).unwrap();
        let back: PangeaAlertStatus = serde_json::from_str(&j).unwrap();
        assert_eq!(format!("{:?}", s), format!("{:?}", back));
    }

    #[test]
    fn status_default_phase_is_none() {
        assert!(PangeaAlertStatus::default().phase.is_none());
    }
}
