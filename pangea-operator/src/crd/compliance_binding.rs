//! ComplianceBinding CRD definition.
//!
//! Binds a ComplianceSchedule to infrastructure targets (InfrastructureTemplate,
//! ImagePipeline, InfrastructureFlow). When compliance state changes, the
//! binding drives reactions: gating deployments, triggering rollbacks, emitting
//! alerts, or updating sekiban admission gates.
//!
//! Example:
//! ```yaml
//! apiVersion: pangea.pleme.io/v1alpha1
//! kind: ComplianceBinding
//! metadata:
//!   name: prod-must-be-compliant
//! spec:
//!   complianceRef:
//!     name: prod-nist-continuous
//!   targets:
//!     - kind: ImagePipeline
//!       name: akeyless-dev-ami-rollout
//!     - kind: InfrastructureTemplate
//!       name: akeyless-dev-cluster
//!   enforcement: gate
//!   reactions:
//!     - event: nonCompliant
//!       action: suspendTarget
//!     - event: compliant
//!       action: resumeTarget
//!     - event: nonCompliant
//!       action: webhook
//!       webhookUrl: https://hooks.slack.com/...
//!   sekibanIntegration:
//!     signatureGateRef: infrastructure-prod-gate
//!     certificationRef: prod-certification
//! ```

use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use strum::Display;

use super::Condition;

/// ComplianceBinding connects a ComplianceSchedule to infrastructure targets
/// and defines enforcement behavior when compliance state changes.
#[derive(CustomResource, Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[kube(
    group = "pangea.pleme.io",
    version = "v1alpha1",
    kind = "ComplianceBinding",
    namespaced,
    status = "ComplianceBindingStatus",
    shortname = "cb",
    category = "pangea",
    printcolumn = r#"{"name":"Enforcement","type":"string","jsonPath":".spec.enforcement"}"#,
    printcolumn = r#"{"name":"Compliance","type":"string","jsonPath":".status.complianceState"}"#,
    printcolumn = r#"{"name":"Targets","type":"integer","jsonPath":".status.targetCount"}"#,
    printcolumn = r#"{"name":"Age","type":"date","jsonPath":".metadata.creationTimestamp"}"#
)]
#[serde(rename_all = "camelCase")]
pub struct ComplianceBindingSpec {
    /// Reference to the ComplianceSchedule that provides compliance state.
    pub compliance_ref: ComplianceRef,

    /// Infrastructure targets bound to this compliance schedule.
    pub targets: Vec<BindingTarget>,

    /// Enforcement level when compliance fails.
    #[serde(default)]
    pub enforcement: EnforcementLevel,

    /// Automated reactions to compliance state changes.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub reactions: Vec<Reaction>,

    /// Sekiban admission webhook integration.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sekiban_integration: Option<SekibanIntegration>,

    /// Minimum consecutive compliant runs before allowing gated operations.
    /// Prevents a single passing run from unblocking after a series of failures.
    #[serde(default = "default_min_consecutive")]
    pub min_consecutive_compliant: u32,

    /// Suspend this binding (targets are ungated).
    #[serde(default)]
    pub suspend: bool,
}

fn default_min_consecutive() -> u32 {
    1
}

/// Reference to a ComplianceSchedule.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ComplianceRef {
    /// Name of the ComplianceSchedule.
    pub name: String,

    /// Namespace (defaults to same as binding).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
}

/// An infrastructure target bound to compliance.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct BindingTarget {
    /// Kind of the target resource.
    pub kind: TargetKind,

    /// Name of the target resource.
    pub name: String,

    /// Namespace (defaults to same as binding).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
}

/// Supported target resource kinds.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, Display)]
pub enum TargetKind {
    InfrastructureTemplate,
    InfrastructureFlow,
    ImagePipeline,
    PackerBuild,
}

/// Enforcement level applied to targets when compliance fails.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, Display, Default)]
#[serde(rename_all = "camelCase")]
pub enum EnforcementLevel {
    /// Log non-compliance but take no action.
    Audit,

    /// Set `spec.suspend = true` on non-compliant targets.
    /// Targets resume when compliance is restored.
    #[default]
    Gate,

    /// Trigger target rollback (if supported by the target kind).
    Rollback,

    /// Only enforce via sekiban admission webhook (no direct target mutation).
    AdmissionOnly,
}

// ---------------------------------------------------------------------------
// Reactions
// ---------------------------------------------------------------------------

/// Automated reaction to a compliance state change.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Reaction {
    /// Event that triggers this reaction.
    pub event: ComplianceEvent,

    /// Action to take.
    pub action: ReactionAction,

    /// Webhook URL for webhook actions.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub webhook_url: Option<String>,

    /// Custom message template for webhook/alert actions.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message_template: Option<String>,
}

/// Compliance events that can trigger reactions.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, Display)]
#[serde(rename_all = "camelCase")]
pub enum ComplianceEvent {
    /// Compliance state changed to non-compliant.
    NonCompliant,
    /// Compliance state restored to compliant.
    Compliant,
    /// A specific control failed.
    ControlFailed,
    /// Compliance run errored (couldn't execute).
    Error,
    /// Compliance hash changed (infrastructure drift).
    HashChanged,
}

/// Actions that can be taken in response to compliance events.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, Display)]
#[serde(rename_all = "camelCase")]
pub enum ReactionAction {
    /// Set `spec.suspend = true` on targets.
    SuspendTarget,
    /// Set `spec.suspend = false` on targets.
    ResumeTarget,
    /// POST event to a webhook URL.
    Webhook,
    /// Emit a K8s Event on the target resource.
    Event,
    /// Trigger a reconciliation of the target.
    Reconcile,
}

// ---------------------------------------------------------------------------
// Sekiban integration
// ---------------------------------------------------------------------------

/// Integration with sekiban admission gating.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SekibanIntegration {
    /// SignatureGate to update with compliance hash.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub signature_gate_ref: Option<String>,

    /// Certification to update with compliance attestation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub certification_ref: Option<String>,

    /// Update deployment annotations with certification hash
    /// for sekiban admission webhook enforcement.
    #[serde(default)]
    pub annotate_deployments: bool,

    /// Annotation key for the certification hash.
    #[serde(default = "default_annotation_key")]
    pub annotation_key: String,
}

fn default_annotation_key() -> String {
    "sekiban.pleme.io/certification-hash".to_string()
}

// ---------------------------------------------------------------------------
// Status
// ---------------------------------------------------------------------------

/// Status of a ComplianceBinding.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ComplianceBindingStatus {
    /// Current compliance state as observed by this binding.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub compliance_state: Option<BindingComplianceState>,

    /// Number of bound targets.
    #[serde(default)]
    pub target_count: u32,

    /// Per-target enforcement status.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub targets: Vec<TargetStatus>,

    /// Compliance hash from the referenced ComplianceSchedule.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub compliance_hash: Option<String>,

    /// Kubernetes-style conditions.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub conditions: Vec<Condition>,

    /// Last observed generation.
    #[serde(default)]
    pub observed_generation: i64,
}

/// Compliance state as observed by the binding.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema, Display)]
#[serde(rename_all = "camelCase")]
pub enum BindingComplianceState {
    /// ComplianceSchedule reports compliant.
    Compliant,
    /// ComplianceSchedule reports non-compliant.
    NonCompliant,
    /// ComplianceSchedule hasn't run yet or errored.
    Unknown,
    /// Binding is suspended.
    Suspended,
}

impl Default for BindingComplianceState {
    fn default() -> Self {
        BindingComplianceState::Unknown
    }
}

/// Enforcement status for a single target.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct TargetStatus {
    /// Target kind.
    pub kind: TargetKind,

    /// Target name.
    pub name: String,

    /// Whether the target is currently gated (suspended).
    pub gated: bool,

    /// Last action taken on this target.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_action: Option<String>,
}
