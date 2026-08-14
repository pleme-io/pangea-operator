//! InfrastructureFlow CRD definition.
//!
//! Defines a directed acyclic graph (DAG) of InfrastructureTemplate steps
//! with dependencies, output passing, and parallel execution control.
//! Replaces fleet.yaml with a Kubernetes-native orchestration primitive.

use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use strum::{Display, EnumString};

use super::TemplateSource;

/// Resolve a step's destroy protection against its flow's, MONOTONE UPWARD.
///
/// `step_destroy_protection(flow, step)` is `flow || step` — a step may RAISE
/// protection and can never lower it.
///
/// It replaces `step.destroy_protection.unwrap_or(flow.destroy_protection)`,
/// which let `Some(false)` on one step in a list quietly unprotect it inside a
/// protected flow. That is a second, quieter route to a destroy than the flag
/// everyone reads: the flow spec says protected, and the reader has to notice
/// one overriding step buried in a `steps:` array to know otherwise.
///
/// Lowering protection is still reachable — at the flow level, where it is
/// visible, and still only alongside a DestroyAuthorization.
#[must_use]
pub const fn step_destroy_protection(flow: bool, step: Option<bool>) -> bool {
    match step {
        Some(step_value) => flow || step_value,
        None => flow,
    }
}

/// Flow-level destroy protection is ON unless a flow says otherwise.
///
/// A free function because `#[serde(default)]` on a bool yields `false`, and
/// that silent `false` is exactly the defect this replaces.
const fn default_flow_destroy_protection() -> bool {
    true
}

/// InfrastructureFlow orchestrates multiple InfrastructureTemplates as a DAG.
#[derive(CustomResource, Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[kube(
    group = "pangea.pleme.io",
    version = "v1alpha1",
    kind = "InfrastructureFlow",
    namespaced,
    status = "InfrastructureFlowStatus",
    shortname = "flow",
    category = "pangea",
    printcolumn = r#"{"name":"Phase","type":"string","jsonPath":".status.phase"}"#,
    printcolumn = r#"{"name":"Steps","type":"integer","jsonPath":".status.totalSteps"}"#,
    printcolumn = r#"{"name":"Ready","type":"integer","jsonPath":".status.readySteps"}"#,
    printcolumn = r#"{"name":"Age","type":"date","jsonPath":".metadata.creationTimestamp"}"#
)]
#[serde(rename_all = "camelCase")]
pub struct InfrastructureFlowSpec {
    /// PangeaNamespace for all templates in this flow.
    pub pangea_namespace: String,

    /// Ordered steps in the flow. Dependencies form a DAG.
    pub steps: Vec<FlowStep>,

    /// How to order destruction. Default: reverse dependency order.
    #[serde(default)]
    pub destroy_order: DestroyOrder,

    /// Maximum number of templates to reconcile in parallel. Default: 1.
    #[serde(default = "default_parallelism")]
    pub parallelism: u32,

    /// Suspend the entire flow.
    #[serde(default)]
    pub suspend: bool,

    /// Prevent destruction of all templates in this flow. **Defaults to `true`.**
    ///
    /// Same reasoning and same default as
    /// [`InfrastructureTemplateSpec::destroy_protection`]: it was
    /// `#[serde(default)]` on a bool, i.e. `false`, so a flow that never
    /// mentioned the field was destroyable BY SAYING NOTHING — and a flow
    /// carries N templates, so the blast radius of that silence was larger
    /// here than on a single template.
    #[serde(default = "default_flow_destroy_protection")]
    pub destroy_protection: bool,
}

fn default_parallelism() -> u32 {
    1
}

/// A single step in an InfrastructureFlow.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct FlowStep {
    /// Unique name for this step within the flow.
    pub name: String,

    /// Template to deploy. Either reference an existing template or provide inline source.
    pub template_ref: FlowTemplateRef,

    /// Steps that must complete before this one starts.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub depends_on: Vec<String>,

    /// Variables for this step. Values may contain {{ steps.X.outputs.Y }} references.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub variables: Option<BTreeMap<String, serde_json::Value>>,

    /// Override auto-approve for this step.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auto_approve: Option<bool>,

    /// Override destroy protection for this step. **Can only RAISE it.**
    ///
    /// `Some(true)` protects a step inside an unprotected flow. `Some(false)`
    /// does NOT unprotect a step inside a protected flow — see
    /// [`InfrastructureFlowSpec::step_destroy_protection`], which takes the
    /// stricter of the two.
    ///
    /// A per-step override that could lower the flow's protection is a second,
    /// quieter place to arrive at a destroy: the flow says protected, one step
    /// buried in a list says otherwise, and the reader of the flow spec sees
    /// only the protection. Monotone-upward removes that shape entirely.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub destroy_protection: Option<bool>,

    /// Override refresh interval for drift detection.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub refresh_interval: Option<String>,

    /// Retry policy for this step.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry: Option<FlowRetryPolicy>,
}

/// Retry policy for a flow step.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct FlowRetryPolicy {
    /// Maximum retry attempts. Default: 3.
    #[serde(default = "default_max_retries")]
    pub max_retries: u32,

    /// Backoff strategy: exponential (default), linear, or constant.
    #[serde(default)]
    pub backoff: BackoffStrategy,

    /// Initial delay between retries. Default: "30s".
    #[serde(default = "default_initial_delay")]
    pub initial_delay: String,

    /// Maximum delay between retries. Default: "10m".
    #[serde(default = "default_max_delay")]
    pub max_delay: String,
}

fn default_max_retries() -> u32 {
    3
}

fn default_initial_delay() -> String {
    "30s".into()
}

fn default_max_delay() -> String {
    "10m".into()
}

/// Backoff strategy for retries.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema, Display, EnumString,
)]
#[serde(rename_all = "camelCase")]
pub enum BackoffStrategy {
    Exponential,
    Linear,
    Constant,
}

impl Default for BackoffStrategy {
    fn default() -> Self {
        BackoffStrategy::Exponential
    }
}

/// Reference to a template: either an existing one or an inline source.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct FlowTemplateRef {
    /// Name of an existing InfrastructureTemplate to manage.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,

    /// Inline template source. The flow controller creates an
    /// InfrastructureTemplate from this source, named `{flow}-{step}`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<TemplateSource>,
}

/// Destroy ordering strategy.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema, Display, EnumString,
)]
#[serde(rename_all = "camelCase")]
pub enum DestroyOrder {
    /// Destroy in reverse dependency order (default).
    Reverse,
    /// Destroy all in parallel (no ordering).
    Parallel,
    /// Do not destroy managed templates on flow deletion.
    None,
}

impl Default for DestroyOrder {
    fn default() -> Self {
        DestroyOrder::Reverse
    }
}

/// Status of an InfrastructureFlow.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct InfrastructureFlowStatus {
    /// Current phase of the flow.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub phase: Option<FlowPhase>,

    /// Total number of steps.
    #[serde(default)]
    pub total_steps: u32,

    /// Number of steps in Ready phase.
    #[serde(default)]
    pub ready_steps: u32,

    /// Per-step status.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub steps: BTreeMap<String, FlowStepStatus>,

    /// Kubernetes-style conditions.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub conditions: Vec<super::Condition>,

    /// Last observed generation.
    #[serde(default)]
    pub observed_generation: i64,

    /// Last error message.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
}

/// Phase of an InfrastructureFlow.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    JsonSchema,
    Display,
    EnumString,
    Default,
)]
pub enum FlowPhase {
    /// Waiting to start.
    #[default]
    Pending,
    /// Templates are being deployed.
    Progressing,
    /// All templates are Ready.
    Ready,
    /// One or more templates failed.
    Failed,
    /// Flow is being destroyed.
    Destroying,
}

/// Status of a single step within a flow.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct FlowStepStatus {
    /// Phase of the template for this step.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub phase: Option<String>,

    /// Outputs from the template (available after Ready).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub outputs: Option<BTreeMap<String, serde_json::Value>>,

    /// Name of the managed InfrastructureTemplate.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub template_name: Option<String>,

    /// Whether dependencies are satisfied.
    #[serde(default)]
    pub dependencies_ready: bool,

    /// Last error for this step.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,

    /// Full terraform state snapshot (from `tofu show -json`).
    /// Enables {{ steps.X.state.resource_type.resource_name.attribute }} references.
    /// Stored as an opaque JSON object.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(schema_with = "super::opaque_json_schema")]
    pub state: Option<serde_json::Value>,

    /// Whether workspace has been pre-initialized (warm-up).
    #[serde(default)]
    pub warmed_up: bool,
}

// The local `opaque_json_schema` copy that used to live here is DELETED, not
// fixed. It was byte-identical to `super::opaque_json_schema` and carried the
// same defect — `type: object` with no `x-kubernetes-preserve-unknown-fields`,
// which prunes every field inside it. Two copies meant fixing the shared one
// would have left this CRD still silently emptying its opaque field, which is
// precisely the reason the fleet's rule is to solve a thing once in one place.
// The `#[schemars(schema_with = ...)]` above now points at the shared helper.

impl InfrastructureFlow {
    /// Get the template name for a flow step.
    pub fn template_name_for_step(&self, step_name: &str) -> String {
        let flow_name = self.metadata.name.as_deref().unwrap_or("unknown");
        format!("{}-{}", flow_name, step_name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // M0 coverage floor (theory/PANGEA-OPERATOR.md §XVII row 6): pin the
    // Default variant before swapping the hand-written `impl Default` for
    // std `#[derive(Default)]` + `#[default]`.
    #[test]
    fn flow_phase_default_is_pending() {
        assert_eq!(FlowPhase::default(), FlowPhase::Pending);
    }
}

#[cfg(test)]
mod step_protection_tests {
    use super::step_destroy_protection;

    #[test]
    fn a_step_can_raise_protection() {
        assert!(step_destroy_protection(false, Some(true)));
    }

    #[test]
    fn a_step_cannot_lower_protection() {
        // The defect this replaces: `unwrap_or` returned false here, so one
        // step in a list could unprotect itself inside a protected flow.
        assert!(
            step_destroy_protection(true, Some(false)),
            "a step must not be able to unprotect itself inside a protected flow"
        );
    }

    #[test]
    fn absent_inherits_the_flow() {
        assert!(step_destroy_protection(true, None));
        assert!(!step_destroy_protection(false, None));
    }
}
