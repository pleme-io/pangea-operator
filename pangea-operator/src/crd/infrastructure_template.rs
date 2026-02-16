//! InfrastructureTemplate CRD definition.
//!
//! Represents a Pangea infrastructure template to be deployed and managed
//! by the operator. Supports inline templates, ConfigMap references, and
//! Git repository sources.

use chrono::{DateTime, Utc};
use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use strum::{Display, EnumString};

/// InfrastructureTemplate represents a Pangea infrastructure template
/// to be compiled, planned, and applied by the operator.
#[derive(CustomResource, Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[kube(
    group = "pangea.pleme.io",
    version = "v1alpha1",
    kind = "InfrastructureTemplate",
    namespaced,
    status = "InfrastructureTemplateStatus",
    shortname = "infra",
    shortname = "it",
    printcolumn = r#"{"name":"Phase","type":"string","jsonPath":".status.phase"}"#,
    printcolumn = r#"{"name":"Namespace","type":"string","jsonPath":".spec.pangeaNamespace"}"#,
    printcolumn = r#"{"name":"Resources","type":"integer","jsonPath":".status.resources.total"}"#,
    printcolumn = r#"{"name":"Age","type":"date","jsonPath":".metadata.creationTimestamp"}"#
)]
#[serde(rename_all = "camelCase")]
pub struct InfrastructureTemplateSpec {
    /// Source of the infrastructure template.
    pub source: TemplateSource,

    /// Pangea namespace for state isolation.
    /// This determines the PostgreSQL schema used for state storage.
    #[serde(rename = "pangeaNamespace")]
    pub pangea_namespace: String,

    /// Optional specific template name to deploy if the source file
    /// contains multiple templates.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub template_name: Option<String>,

    /// Variables to pass to the template during compilation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub variables: Option<BTreeMap<String, serde_json::Value>>,

    /// Whether to automatically apply changes without manual approval.
    #[serde(default)]
    pub auto_approve: bool,

    /// Interval for drift detection checks.
    /// Defaults to "5m" (5 minutes).
    #[serde(default = "default_refresh_interval")]
    pub refresh_interval: String,

    /// Suspend reconciliation for this template.
    #[serde(default)]
    pub suspend: bool,

    /// Retry policy for failed operations.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry_policy: Option<RetryPolicy>,

    /// Provider credentials configuration.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_credentials: Option<ProviderCredentials>,

    /// InSpec compliance profiles to run after apply.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub compliance_profiles: Vec<String>,
}

fn default_refresh_interval() -> String {
    "5m".to_string()
}

/// Source of the infrastructure template.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct TemplateSource {
    /// Inline Ruby DSL template content.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inline: Option<String>,

    /// Reference to a ConfigMap containing the template.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub config_map_ref: Option<ConfigMapRef>,

    /// Reference to a Git repository containing the template.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub git_repository: Option<GitRepositoryRef>,
}

/// Reference to a ConfigMap key.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ConfigMapRef {
    /// Name of the ConfigMap.
    pub name: String,

    /// Key within the ConfigMap containing the template.
    pub key: String,

    /// Namespace of the ConfigMap (defaults to same namespace as the InfrastructureTemplate).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
}

/// Reference to a Git repository.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct GitRepositoryRef {
    /// Git repository URL.
    pub url: String,

    /// Git reference (branch, tag, or commit SHA).
    #[serde(default = "default_git_ref")]
    pub r#ref: String,

    /// Path to the template file within the repository.
    pub path: String,

    /// Reference to a Secret containing Git credentials.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub secret_ref: Option<SecretRef>,
}

fn default_git_ref() -> String {
    "main".to_string()
}

/// Reference to a Kubernetes Secret.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SecretRef {
    /// Name of the Secret.
    pub name: String,

    /// Namespace of the Secret (defaults to same namespace as the resource).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
}

/// Retry policy for failed operations.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct RetryPolicy {
    /// Maximum number of retry attempts.
    #[serde(default = "default_max_retries")]
    pub max_retries: u32,

    /// Base delay between retries in seconds.
    #[serde(default = "default_backoff_seconds")]
    pub backoff_seconds: u32,
}

fn default_max_retries() -> u32 {
    3
}

fn default_backoff_seconds() -> u32 {
    30
}

/// Provider credentials configuration.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ProviderCredentials {
    /// AWS credentials configuration.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub aws: Option<AwsCredentials>,

    /// Cloudflare credentials configuration.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cloudflare: Option<CloudflareCredentials>,
}

/// AWS credentials configuration.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct AwsCredentials {
    /// Secret containing AWS credentials.
    pub secret_ref: SecretRef,

    /// Region to use for AWS operations.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub region: Option<String>,

    /// Optional role ARN to assume.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub role_arn: Option<String>,
}

/// Cloudflare credentials configuration.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct CloudflareCredentials {
    /// Secret containing Cloudflare API token.
    pub secret_ref: SecretRef,
}

/// Status of an InfrastructureTemplate.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct InfrastructureTemplateStatus {
    /// Current phase of the template lifecycle.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub phase: Option<Phase>,

    /// Conditions representing the current state.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub conditions: Vec<Condition>,

    /// Last successfully applied revision (content hash or git commit).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_applied_revision: Option<String>,

    /// Timestamp of the last successful plan.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_planned_at: Option<DateTime<Utc>>,

    /// Timestamp of the last successful apply.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_applied_at: Option<DateTime<Utc>>,

    /// Timestamp of the last drift check.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_drift_check_at: Option<DateTime<Utc>>,

    /// Summary of managed resources.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resources: Option<ResourceSummary>,

    /// Outputs from the last successful apply.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub outputs: Option<BTreeMap<String, serde_json::Value>>,

    /// Human-readable summary of the last plan.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub plan_summary: Option<String>,

    /// PostgreSQL state key path.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub state_key: Option<String>,

    /// Last observed generation of the spec.
    #[serde(default)]
    pub observed_generation: i64,

    /// Number of consecutive failures.
    #[serde(default)]
    pub failure_count: u32,

    /// Last error message if in Failed state.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,

    /// Compliance check results.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub compliance: Option<ComplianceStatus>,
}

/// Lifecycle phase of an InfrastructureTemplate.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema, Display, EnumString)]
pub enum Phase {
    /// Initial state, waiting to be processed.
    Pending,
    /// Compiling Ruby DSL to Terraform JSON.
    Compiling,
    /// Running `tofu init`.
    Initializing,
    /// Running `tofu plan`.
    Planning,
    /// Running `tofu apply`.
    Applying,
    /// Successfully applied, no pending changes.
    Ready,
    /// Drift detected, changes pending approval.
    Drifted,
    /// Operation failed.
    Failed,
    /// Running `tofu destroy`.
    Destroying,
}

impl Default for Phase {
    fn default() -> Self {
        Phase::Pending
    }
}

/// Kubernetes-style condition.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Condition {
    /// Type of condition.
    pub r#type: String,

    /// Status of the condition (True, False, Unknown).
    pub status: String,

    /// Last time the condition transitioned.
    pub last_transition_time: DateTime<Utc>,

    /// Machine-readable reason for the condition.
    pub reason: String,

    /// Human-readable message.
    pub message: String,
}

/// Summary of managed resources.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ResourceSummary {
    /// Total number of managed resources.
    #[serde(default)]
    pub total: u32,

    /// Resources to be added in the pending plan.
    #[serde(default)]
    pub added: u32,

    /// Resources to be changed in the pending plan.
    #[serde(default)]
    pub changed: u32,

    /// Resources to be destroyed in the pending plan.
    #[serde(default)]
    pub destroyed: u32,
}

/// Compliance check status.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ComplianceStatus {
    /// Overall compliance status.
    pub status: String,

    /// Compliance score (0-100).
    pub score: f64,

    /// Number of passed controls.
    pub passed_controls: u32,

    /// Number of failed controls.
    pub failed_controls: u32,

    /// Number of skipped controls.
    pub skipped_controls: u32,

    /// Last compliance check timestamp.
    pub last_check_at: DateTime<Utc>,

    /// Per-profile results.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub profiles: Vec<ProfileResult>,
}

/// Result for a single compliance profile.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ProfileResult {
    /// Profile name.
    pub profile: String,

    /// Profile score (0-100).
    pub score: f64,

    /// Profile status (compliant, non-compliant).
    pub status: String,

    /// IDs of failed controls.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub failed_control_ids: Vec<String>,
}

impl InfrastructureTemplate {
    /// Check if this template needs a new reconciliation.
    pub fn needs_reconciliation(&self) -> bool {
        let Some(status) = &self.status else {
            return true;
        };

        // Check if spec has changed
        if status.observed_generation != self.metadata.generation.unwrap_or(0) {
            return true;
        }

        // Check if suspended
        if self.spec.suspend {
            return false;
        }

        // Check phase
        matches!(
            status.phase,
            Some(Phase::Pending) | Some(Phase::Failed) | Some(Phase::Drifted)
        )
    }

    /// Get the effective retry count.
    pub fn retry_count(&self) -> u32 {
        self.status
            .as_ref()
            .map(|s| s.failure_count)
            .unwrap_or(0)
    }

    /// Check if retries are exhausted.
    pub fn retries_exhausted(&self) -> bool {
        let max_retries = self
            .spec
            .retry_policy
            .as_ref()
            .map(|p| p.max_retries)
            .unwrap_or(default_max_retries());

        self.retry_count() >= max_retries
    }
}
