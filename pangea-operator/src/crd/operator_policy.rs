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
use std::collections::BTreeMap;

/// Cluster-scoped fleet-wide kill-switch for the operator's controllers.
#[derive(CustomResource, Debug, Clone, Default, Serialize, Deserialize, JsonSchema, PartialEq)]
#[kube(
    group = "pangea.pleme.io",
    version = "v1alpha1",
    kind = "OperatorPolicy",
    plural = "operatorpolicies",
    shortname = "oppol",
    category = "pangea",
    status = "OperatorPolicyStatus",
    printcolumn = r#"{"name":"GlobalSuspend","type":"boolean","jsonPath":".spec.globalSuspend"}"#,
    printcolumn = r#"{"name":"Reason","type":"string","jsonPath":".spec.globalSuspendReason"}"#,
    printcolumn = r#"{"name":"Skipped","type":"integer","jsonPath":".status.reconcilesSkipped"}"#,
    printcolumn = r#"{"name":"WorkspaceOverrides","type":"integer","jsonPath":".status.workspaceOverrides"}"#,
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

    /// Per-workspace tri-state suspend. Specific overrides general:
    /// the resolution ladder for a single template is
    ///
    ///   1. `workspaceSuspend.templates["<ns>/<name>"].state`
    ///   2. `workspaceSuspend.catalogs["<owner-catalog>"].state`
    ///      (cascade — catalog Active carves the entire catalog out
    ///      of a global pause, catalog Paused freezes everything
    ///      under it)
    ///   3. `controllerSuspend.template`
    ///   4. `globalSuspend`
    ///
    /// stopping at the first non-`Inherit` decision. `Active` at any
    /// of layers 1–2 means "specifically configured to reconcile
    /// during a global pause" — a surgical carve-out. `Paused` at
    /// any of layers 1–2 freezes that workspace regardless of more
    /// general state.
    #[serde(default)]
    pub workspace_suspend: WorkspaceSuspend,
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

/// Per-workspace tri-state pause. Each entry's `state` decides what
/// happens for a single workspace; missing entries default to
/// `Inherit` — fall through to controller/global layers.
///
/// Two key spaces:
///
///   * `templates` — keyed `"<namespace>/<name>"` of an
///     `InfrastructureTemplate`. Most-specific layer.
///   * `catalogs` — keyed by `WorkspaceCatalog` name. Cascades to
///     every template owned by that catalog unless the template
///     overrides.
///
/// Both maps default to empty (no per-workspace override = inherit
/// the layer above). Backwards-compatible: deployments without
/// these fields deserialize unchanged.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct WorkspaceSuspend {
    /// Per-template overrides keyed `"<namespace>/<name>"`.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub templates: BTreeMap<String, WorkspaceSuspendEntry>,

    /// Per-catalog overrides keyed by `WorkspaceCatalog` name.
    /// Cascades to children unless a child template overrides.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub catalogs: BTreeMap<String, WorkspaceSuspendEntry>,
}

/// A single per-workspace decision: what state, optionally why.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct WorkspaceSuspendEntry {
    /// Tri-state decision. Defaults to `Inherit`, which is
    /// equivalent to omitting the entry entirely.
    #[serde(default)]
    pub state: WorkspaceState,

    /// Optional human-readable note, surfaced in skip logs and the
    /// `policy_skipped_total{reason=...}` metric label. Optional
    /// throughout the surface — pausing without a reason is
    /// allowed, dashboards display "(no reason given)".
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

/// Tri-state per-workspace pause directive.
///
///   * `Inherit` (default) — fall through to the next layer.
///   * `Paused` — explicit pause regardless of more-general state.
///   * `Active` — explicit run even when `globalSuspend=true`. The
///     "surgical carve-out" — use sparingly, audit reasons in git.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum WorkspaceState {
    /// Default. Effective state is whatever the next layer decides.
    #[default]
    Inherit,
    /// Explicit pause. Wins over global, controller, and catalog.
    Paused,
    /// Explicit run during a global/controller/catalog pause. Used
    /// for the one or two workspaces that must keep reconciling
    /// while everything else is frozen.
    Active,
}

/// Resolved per-workspace decision after running the precedence
/// ladder. Returned by `resolve_template_state` /
/// `resolve_catalog_state`. Controllers map this to a kube
/// `Action`: `Paused` → `requeue`, others → continue reconciling.
#[derive(Debug, Clone, PartialEq)]
pub enum SuspensionDecision {
    /// No pause applies — controller proceeds normally.
    NotSuspended,
    /// Workspace is paused. Optional reason flows from the
    /// matching entry's `reason` field, or from the layer that
    /// fired (e.g. `globalSuspendReason` when the global gate
    /// dominated).
    Paused {
        reason: Option<String>,
        source: SuspensionSource,
    },
    /// Workspace has an explicit `Active` carve-out. Controller
    /// proceeds, but observability surfaces this as an override so
    /// dashboards can show "what's still running under a global
    /// pause."
    Active {
        reason: Option<String>,
        source: SuspensionSource,
    },
}

impl SuspensionDecision {
    /// True iff the decision skips the reconcile body.
    pub const fn is_paused(&self) -> bool {
        matches!(self, SuspensionDecision::Paused { .. })
    }

    /// True iff the decision is an explicit Active carve-out.
    pub const fn is_active_override(&self) -> bool {
        matches!(self, SuspensionDecision::Active { .. })
    }

    /// Returns the resolution layer that produced this decision,
    /// for metric labels + structured logs. Returns `None` for
    /// `NotSuspended`.
    pub const fn source(&self) -> Option<SuspensionSource> {
        match self {
            SuspensionDecision::NotSuspended => None,
            SuspensionDecision::Paused { source, .. } => Some(*source),
            SuspensionDecision::Active { source, .. } => Some(*source),
        }
    }

    /// Returns the optional reason associated with the decision.
    pub fn reason(&self) -> Option<&str> {
        match self {
            SuspensionDecision::NotSuspended => None,
            SuspensionDecision::Paused { reason, .. } => reason.as_deref(),
            SuspensionDecision::Active { reason, .. } => reason.as_deref(),
        }
    }
}

/// The layer of the precedence ladder that produced a
/// `SuspensionDecision`. Used for `policy_skipped_total{source=...}`
/// label values + structured log fields.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SuspensionSource {
    /// `OperatorPolicy.spec.globalSuspend` (binary, dominates when
    /// nothing more specific overrides).
    Global,
    /// `OperatorPolicy.spec.controllerSuspend.<kind>`.
    Controller,
    /// `OperatorPolicy.spec.workspaceSuspend.catalogs["<name>"]`
    /// — cascades to children.
    Catalog,
    /// `OperatorPolicy.spec.workspaceSuspend.templates["<ns>/<name>"]`
    /// — most specific.
    Workspace,
}

impl SuspensionSource {
    /// Stable string used in metric label values + log fields.
    pub const fn name(&self) -> &'static str {
        match self {
            SuspensionSource::Global => "global",
            SuspensionSource::Controller => "controller",
            SuspensionSource::Catalog => "catalog",
            SuspensionSource::Workspace => "workspace",
        }
    }
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

    /// Number of `Active` workspace carve-outs currently in effect
    /// (templates + catalogs combined). Surfaced as a printer column
    /// so `kubectl get oppol` shows at a glance how many surgical
    /// exemptions exist while a global pause is in place. Counted
    /// directly from `spec.workspaceSuspend`; not a runtime gauge.
    #[serde(default)]
    pub workspace_overrides: u64,
}

impl WorkspaceSuspend {
    /// Count entries with `state == Active` across both maps. Used to
    /// populate `OperatorPolicyStatus.workspace_overrides` and the
    /// matching printer column.
    pub fn count_active_overrides(&self) -> u64 {
        let templates = self
            .templates
            .values()
            .filter(|e| e.state == WorkspaceState::Active)
            .count();
        let catalogs = self
            .catalogs
            .values()
            .filter(|e| e.state == WorkspaceState::Active)
            .count();
        (templates + catalogs) as u64
    }

    /// Compose the canonical key for a per-template entry from a
    /// (namespace, name) pair. Centralized so callers and tests can't
    /// drift in formatting.
    pub fn template_key(namespace: &str, name: &str) -> String {
        format!("{}/{}", namespace, name)
    }

    /// Lookup helper: returns the entry for a template if present.
    pub fn template_entry(
        &self,
        namespace: &str,
        name: &str,
    ) -> Option<&WorkspaceSuspendEntry> {
        self.templates.get(&Self::template_key(namespace, name))
    }

    /// Lookup helper: returns the entry for a catalog if present.
    pub fn catalog_entry(&self, name: &str) -> Option<&WorkspaceSuspendEntry> {
        self.catalogs.get(name)
    }
}

/// The canonical name of the singleton OperatorPolicy. Other names are
/// ignored with a warning Event.
pub const OPERATOR_POLICY_SINGLETON: &str = "default";

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_spec_is_fully_permissive() {
        let spec = OperatorPolicySpec::default();
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
        // Workspace maps default to empty.
        assert!(spec.workspace_suspend.templates.is_empty());
        assert!(spec.workspace_suspend.catalogs.is_empty());
        assert_eq!(spec.workspace_suspend.count_active_overrides(), 0);
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
            ..Default::default()
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
            ..Default::default()
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

    // ── WorkspaceSuspend tri-state ─────────────────────────────────

    #[test]
    fn workspace_state_default_is_inherit() {
        // The default of the enum must be `Inherit` so an entry
        // declared without an explicit state is a no-op (i.e. omitting
        // the entry == declaring `state: inherit`).
        assert_eq!(WorkspaceState::default(), WorkspaceState::Inherit);
    }

    #[test]
    fn workspace_state_serializes_lowercase() {
        // YAML idiom: `state: paused`, `state: active`, `state: inherit`.
        // This is what users will type in OperatorPolicy YAML.
        assert_eq!(
            serde_json::to_value(WorkspaceState::Paused).unwrap(),
            serde_json::Value::String("paused".into())
        );
        assert_eq!(
            serde_json::to_value(WorkspaceState::Active).unwrap(),
            serde_json::Value::String("active".into())
        );
        assert_eq!(
            serde_json::to_value(WorkspaceState::Inherit).unwrap(),
            serde_json::Value::String("inherit".into())
        );
    }

    #[test]
    fn workspace_suspend_template_key_format() {
        // Centralized key formatter — guards against drift between
        // callers and the cache lookup.
        assert_eq!(
            WorkspaceSuspend::template_key("pangea-system", "cloudflare-pleme"),
            "pangea-system/cloudflare-pleme"
        );
    }

    #[test]
    fn workspace_suspend_lookup_helpers() {
        let mut ws = WorkspaceSuspend::default();
        ws.templates.insert(
            "ns/foo".into(),
            WorkspaceSuspendEntry {
                state: WorkspaceState::Paused,
                reason: Some("debugging".into()),
            },
        );
        ws.catalogs.insert(
            "opensource".into(),
            WorkspaceSuspendEntry {
                state: WorkspaceState::Active,
                reason: None,
            },
        );

        assert!(ws.template_entry("ns", "foo").is_some());
        assert!(ws.template_entry("ns", "bar").is_none());
        assert_eq!(
            ws.catalog_entry("opensource").map(|e| e.state),
            Some(WorkspaceState::Active)
        );
        assert!(ws.catalog_entry("missing").is_none());
    }

    #[test]
    fn count_active_overrides_combines_templates_and_catalogs() {
        let mut ws = WorkspaceSuspend::default();
        ws.templates.insert(
            "ns/a".into(),
            WorkspaceSuspendEntry {
                state: WorkspaceState::Active,
                reason: None,
            },
        );
        ws.templates.insert(
            "ns/b".into(),
            WorkspaceSuspendEntry {
                state: WorkspaceState::Paused,
                reason: None,
            },
        );
        ws.catalogs.insert(
            "opensource".into(),
            WorkspaceSuspendEntry {
                state: WorkspaceState::Active,
                reason: None,
            },
        );
        ws.catalogs.insert(
            "core".into(),
            WorkspaceSuspendEntry {
                state: WorkspaceState::Inherit,
                reason: None,
            },
        );
        // Paused entries don't count; only Active.
        assert_eq!(ws.count_active_overrides(), 2);
    }

    #[test]
    fn workspace_suspend_serde_roundtrip() {
        let spec = OperatorPolicySpec {
            global_suspend: true,
            global_suspend_reason: Some("rewrite".into()),
            workspace_suspend: WorkspaceSuspend {
                templates: BTreeMap::from([(
                    "pangea-system/cloudflare-pleme".into(),
                    WorkspaceSuspendEntry {
                        state: WorkspaceState::Active,
                        reason: Some("monitoring github org".into()),
                    },
                )]),
                catalogs: BTreeMap::from([(
                    "opensource".into(),
                    WorkspaceSuspendEntry {
                        state: WorkspaceState::Paused,
                        reason: None,
                    },
                )]),
            },
            ..Default::default()
        };
        let yaml = serde_yaml::to_string(&spec).unwrap();
        // YAML must look like what users type:
        assert!(yaml.contains("workspaceSuspend:"));
        assert!(yaml.contains("state: active"));
        assert!(yaml.contains("state: paused"));
        // Round-trip preserves equality.
        let back: OperatorPolicySpec = serde_yaml::from_str(&yaml).unwrap();
        assert_eq!(spec, back);
    }

    #[test]
    fn empty_workspace_suspend_omitted_in_serialized_output() {
        // When no per-workspace overrides are set, the maps should be
        // skipped in serialization (skip_serializing_if). Old
        // OperatorPolicy/default deployments stay byte-stable.
        let spec = OperatorPolicySpec {
            global_suspend: true,
            global_suspend_reason: Some("rewrite".into()),
            ..Default::default()
        };
        let json = serde_json::to_value(&spec).unwrap();
        // workspaceSuspend is present but its inner maps are empty +
        // omitted via skip_serializing_if.
        let ws = json.get("workspaceSuspend").expect("ws field present");
        assert!(
            ws.get("templates").is_none(),
            "empty templates map must be omitted"
        );
        assert!(
            ws.get("catalogs").is_none(),
            "empty catalogs map must be omitted"
        );
    }

    #[test]
    fn suspension_decision_helpers() {
        let p = SuspensionDecision::Paused {
            reason: Some("debugging".into()),
            source: SuspensionSource::Workspace,
        };
        assert!(p.is_paused());
        assert!(!p.is_active_override());
        assert_eq!(p.source(), Some(SuspensionSource::Workspace));
        assert_eq!(p.reason(), Some("debugging"));

        let a = SuspensionDecision::Active {
            reason: None,
            source: SuspensionSource::Catalog,
        };
        assert!(!a.is_paused());
        assert!(a.is_active_override());
        assert_eq!(a.source(), Some(SuspensionSource::Catalog));
        assert_eq!(a.reason(), None);

        let n = SuspensionDecision::NotSuspended;
        assert!(!n.is_paused());
        assert!(!n.is_active_override());
        assert_eq!(n.source(), None);
        assert_eq!(n.reason(), None);
    }

    #[test]
    fn suspension_source_names_are_stable() {
        // Stable strings used in metric labels — guard against rename.
        assert_eq!(SuspensionSource::Global.name(), "global");
        assert_eq!(SuspensionSource::Controller.name(), "controller");
        assert_eq!(SuspensionSource::Catalog.name(), "catalog");
        assert_eq!(SuspensionSource::Workspace.name(), "workspace");
    }
}
