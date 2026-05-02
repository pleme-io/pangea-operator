//! OperatorPolicy CRD — cluster-scoped, fleet-wide kill-switch.
//!
//! Composes with each CR's per-resource `spec.suspend`:
//!   * If `OperatorPolicy/default.spec.globalSuspend == true`, every
//!     controller skips reconciliation regardless of per-CR state.
//!   * If `OperatorPolicy/default.spec.controllerSuspend.<kind> == true`,
//!     only that controller skips. Other controllers proceed.
//!   * If a single CR has `spec.suspend == true`, only that CR is
//!     skipped.
//!
//! Convention: only the singleton named `default` is honored. Other
//! `OperatorPolicy` resources are ignored with a Warning event. This
//! keeps the surface unambiguous: there is exactly one fleet-wide
//! switch, and it's always at `OperatorPolicy/default`.
//!
//! Default behavior when no `OperatorPolicy/default` exists: allow.
//! That is, controllers run normally — the CRD is opt-in and
//! backwards-compatible with deployments that pre-date this primitive.

use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Cluster-scoped fleet-wide kill-switch for the operator's controllers.
#[derive(CustomResource, Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
#[kube(
    group = "pangea.pleme.io",
    version = "v1alpha1",
    kind = "OperatorPolicy",
    plural = "operatorpolicies",
    shortname = "oppol",
    status = "OperatorPolicyStatus",
    printcolumn = r#"{"name":"GlobalSuspend","type":"boolean","jsonPath":".spec.globalSuspend"}"#,
    printcolumn = r#"{"name":"Reason","type":"string","jsonPath":".spec.globalSuspendReason"}"#,
    printcolumn = r#"{"name":"Skipped","type":"integer","jsonPath":".status.reconcilesSkipped"}"#,
    printcolumn = r#"{"name":"Age","type":"date","jsonPath":".metadata.creationTimestamp"}"#
)]
#[serde(rename_all = "camelCase")]
pub struct OperatorPolicySpec {
    /// Master kill-switch. When true, every operator controller skips
    /// reconciliation entirely. Status counter `reconcilesSkipped`
    /// increments on every skipped reconcile so you can verify the
    /// pause is taking effect.
    #[serde(default)]
    pub global_suspend: bool,

    /// Operator-readable reason for the suspend, surfaced in:
    ///   * structured logs on every skipped reconcile
    ///   * Kubernetes Events with reason=`OperatorSuspended`
    ///   * `kubectl describe operatorpolicy default` output
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub global_suspend_reason: Option<String>,

    /// Per-controller suspend. Use to pause a specific controller while
    /// debugging without freezing the whole operator. `globalSuspend`
    /// dominates this field — if it's true, per-controller flags are
    /// not consulted.
    #[serde(default)]
    pub controller_suspend: ControllerSuspend,
}

/// Per-controller suspend flags. Each field corresponds 1:1 with a
/// concrete reconciler in the operator.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ControllerSuspend {
    /// Pause `template_controller` (InfrastructureTemplate).
    #[serde(default)]
    pub template: bool,

    /// Pause `namespace_controller` (PangeaNamespace).
    #[serde(default)]
    pub namespace: bool,

    /// Pause `workspace_catalog_controller`.
    #[serde(default)]
    pub workspace_catalog: bool,

    /// Pause `architecture_gem_controller`.
    #[serde(default)]
    pub architecture_gem: bool,

    /// Pause `compliance_binding_controller`.
    #[serde(default)]
    pub compliance_binding: bool,

    /// Pause `compliance_schedule_controller`.
    #[serde(default)]
    pub compliance_schedule: bool,

    /// Pause `image_pipeline_controller`.
    #[serde(default)]
    pub image_pipeline: bool,

    /// Pause `flow_controller` (InfrastructureFlow).
    #[serde(default)]
    pub flow: bool,

    /// Pause `dashboard_controller` (PangeaDashboard).
    #[serde(default)]
    pub dashboard: bool,

    /// Pause `ami_test_controller`.
    #[serde(default)]
    pub ami_test: bool,

    /// Pause `packer_build_controller`.
    #[serde(default)]
    pub packer_build: bool,

    /// Pause `synthesizer_format_controller`.
    #[serde(default)]
    pub synthesizer_format: bool,
}

/// Typed identifier for each operator controller. Used by the policy
/// gate to decide whether to skip reconciliation for a given
/// reconciler invocation.
///
/// Adding a new controller? Add a variant here AND a field in
/// `ControllerSuspend` with the same camelCase name. The match in
/// `ControllerSuspend::is_set` enforces exhaustiveness at compile time.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ControllerKind {
    Template,
    Namespace,
    WorkspaceCatalog,
    ArchitectureGem,
    ComplianceBinding,
    ComplianceSchedule,
    ImagePipeline,
    Flow,
    Dashboard,
    AmiTest,
    PackerBuild,
    SynthesizerFormat,
}

impl ControllerKind {
    /// Stable string key used in logs, events, and metrics labels.
    pub const fn name(&self) -> &'static str {
        match self {
            ControllerKind::Template => "template",
            ControllerKind::Namespace => "namespace",
            ControllerKind::WorkspaceCatalog => "workspaceCatalog",
            ControllerKind::ArchitectureGem => "architectureGem",
            ControllerKind::ComplianceBinding => "complianceBinding",
            ControllerKind::ComplianceSchedule => "complianceSchedule",
            ControllerKind::ImagePipeline => "imagePipeline",
            ControllerKind::Flow => "flow",
            ControllerKind::Dashboard => "dashboard",
            ControllerKind::AmiTest => "amiTest",
            ControllerKind::PackerBuild => "packerBuild",
            ControllerKind::SynthesizerFormat => "synthesizerFormat",
        }
    }
}

impl ControllerSuspend {
    /// Compile-time-exhaustive lookup: returns true iff the controller
    /// is suspended at the per-controller layer. Adding a new
    /// `ControllerKind` variant forces this match to be updated.
    pub const fn is_set(&self, kind: ControllerKind) -> bool {
        match kind {
            ControllerKind::Template => self.template,
            ControllerKind::Namespace => self.namespace,
            ControllerKind::WorkspaceCatalog => self.workspace_catalog,
            ControllerKind::ArchitectureGem => self.architecture_gem,
            ControllerKind::ComplianceBinding => self.compliance_binding,
            ControllerKind::ComplianceSchedule => self.compliance_schedule,
            ControllerKind::ImagePipeline => self.image_pipeline,
            ControllerKind::Flow => self.flow,
            ControllerKind::Dashboard => self.dashboard,
            ControllerKind::AmiTest => self.ami_test,
            ControllerKind::PackerBuild => self.packer_build,
            ControllerKind::SynthesizerFormat => self.synthesizer_format,
        }
    }
}

/// Status of an OperatorPolicy. The reconciler mirrors `spec` into
/// `status.effective` after every spec change so consumers can observe
/// the policy's resolved view without re-reading spec.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct OperatorPolicyStatus {
    /// Last `metadata.generation` the controller has reconciled.
    #[serde(default)]
    pub observed_generation: i64,

    /// Wall-clock timestamp of the last spec change observed by the
    /// controller. Empty until the policy is first reconciled.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_changed_at: Option<chrono::DateTime<chrono::Utc>>,

    /// Effective view of the spec. Mirrors spec verbatim today; reserved
    /// for future computed fields (e.g., merged-from-multiple-sources).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub effective: Option<OperatorPolicySpec>,

    /// Rolling counter incremented every time a controller skips a
    /// reconcile due to this policy. Resets to zero on operator restart.
    /// Use to confirm the kill-switch is taking effect:
    ///   `kubectl get operatorpolicy default -o jsonpath='{.status.reconcilesSkipped}'`
    #[serde(default)]
    pub reconciles_skipped: u64,
}

/// The canonical name of the singleton OperatorPolicy. Other names are
/// ignored with a warning Event.
pub const OPERATOR_POLICY_SINGLETON: &str = "default";

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_spec_is_fully_permissive() {
        let spec = OperatorPolicySpec {
            global_suspend: false,
            global_suspend_reason: None,
            controller_suspend: ControllerSuspend::default(),
        };
        // No suspends = every controller proceeds.
        for kind in [
            ControllerKind::Template,
            ControllerKind::Namespace,
            ControllerKind::WorkspaceCatalog,
            ControllerKind::ArchitectureGem,
            ControllerKind::ComplianceBinding,
            ControllerKind::ComplianceSchedule,
            ControllerKind::ImagePipeline,
            ControllerKind::Flow,
            ControllerKind::Dashboard,
            ControllerKind::AmiTest,
            ControllerKind::PackerBuild,
            ControllerKind::SynthesizerFormat,
        ] {
            assert!(!spec.global_suspend);
            assert!(!spec.controller_suspend.is_set(kind));
        }
    }

    #[test]
    fn per_controller_suspend_is_independent() {
        let mut suspend = ControllerSuspend::default();
        suspend.template = true;
        suspend.dashboard = true;

        // Suspended ones return true.
        assert!(suspend.is_set(ControllerKind::Template));
        assert!(suspend.is_set(ControllerKind::Dashboard));

        // Others remain false.
        assert!(!suspend.is_set(ControllerKind::Namespace));
        assert!(!suspend.is_set(ControllerKind::Flow));
        assert!(!suspend.is_set(ControllerKind::ImagePipeline));
    }

    #[test]
    fn controller_kind_name_stable() {
        // Stable keys for log/metric labels — guard against silent rename.
        assert_eq!(ControllerKind::Template.name(), "template");
        assert_eq!(ControllerKind::WorkspaceCatalog.name(), "workspaceCatalog");
        assert_eq!(ControllerKind::ArchitectureGem.name(), "architectureGem");
        assert_eq!(ControllerKind::ComplianceBinding.name(), "complianceBinding");
        assert_eq!(ControllerKind::ImagePipeline.name(), "imagePipeline");
    }

    #[test]
    fn spec_serde_roundtrip() {
        let spec = OperatorPolicySpec {
            global_suspend: true,
            global_suspend_reason: Some("rewrite-in-progress".to_string()),
            controller_suspend: ControllerSuspend {
                template: true,
                ..Default::default()
            },
        };
        let json = serde_json::to_string(&spec).unwrap();
        let back: OperatorPolicySpec = serde_json::from_str(&json).unwrap();
        assert_eq!(spec, back);
    }

    #[test]
    fn spec_serde_uses_camelcase() {
        let spec = OperatorPolicySpec {
            global_suspend: true,
            global_suspend_reason: Some("test".to_string()),
            controller_suspend: ControllerSuspend {
                workspace_catalog: true,
                ..Default::default()
            },
        };
        let json = serde_json::to_value(&spec).unwrap();
        // Ensure k8s-idiomatic camelCase, not Rust's snake_case.
        assert!(json.get("globalSuspend").is_some());
        assert!(json.get("globalSuspendReason").is_some());
        assert!(json.get("controllerSuspend").is_some());
        assert!(json
            .get("controllerSuspend")
            .unwrap()
            .get("workspaceCatalog")
            .is_some());
    }

    #[test]
    fn singleton_name_is_default() {
        // Hard-coded contract: only "default" is reconciled.
        assert_eq!(OPERATOR_POLICY_SINGLETON, "default");
    }
}
