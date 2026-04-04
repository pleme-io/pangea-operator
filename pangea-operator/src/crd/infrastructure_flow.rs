//! InfrastructureFlow CRD definition.
//!
//! Defines a directed acyclic graph (DAG) of InfrastructureTemplate steps
//! with dependencies, output passing, and parallel execution control.
//! Replaces fleet.yaml with a Kubernetes-native orchestration primitive.

use chrono::{DateTime, Utc};
use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use strum::{Display, EnumString};

use super::TemplateSource;

/// InfrastructureFlow orchestrates multiple InfrastructureTemplates as a DAG.
#[derive(CustomResource, Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[kube(
    group = "pangea.pleme.io",
    version = "v1alpha1",
    kind = "InfrastructureFlow",
    namespaced,
    status = "InfrastructureFlowStatus",
    shortname = "flow",
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

    /// Prevent destruction of all templates in this flow.
    #[serde(default)]
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

    /// Override destroy protection for this step.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub destroy_protection: Option<bool>,

    /// Override refresh interval for drift detection.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub refresh_interval: Option<String>,
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
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema, Display, EnumString)]
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
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema, Display, EnumString)]
pub enum FlowPhase {
    /// Waiting to start.
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

impl Default for FlowPhase {
    fn default() -> Self {
        FlowPhase::Pending
    }
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
}

impl InfrastructureFlow {
    /// Get the template name for a flow step.
    pub fn template_name_for_step(&self, step_name: &str) -> String {
        let flow_name = self.metadata.name.as_deref().unwrap_or("unknown");
        format!("{}-{}", flow_name, step_name)
    }
}
