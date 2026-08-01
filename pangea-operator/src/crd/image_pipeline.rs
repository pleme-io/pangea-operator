//! ImagePipeline CRD definition.
//!
//! Top-level orchestrator that chains the full AMI lifecycle:
//! build (PackerBuild) → test (AmiTest) → deploy (patch InfrastructureTemplate)
//! → verify (plan assertions + health checks) → complete (or rollback).
//!
//! Each phase's output causally feeds the next phase's input. The pipeline
//! creates child CRs with owner references and watches their status to drive
//! phase transitions.

use chrono::{DateTime, Utc};
use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use strum::Display;

use super::ami_test::{FailurePolicy, TestSuite};
use super::{Condition, ProviderCredentials, SecretRef, TemplateObjectRef, TemplateSource};

/// ImagePipeline orchestrates the full AMI lifecycle:
/// build → test → deploy → verify → complete (or rollback).
#[derive(CustomResource, Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[kube(
    group = "pangea.pleme.io",
    version = "v1alpha1",
    kind = "ImagePipeline",
    namespaced,
    status = "ImagePipelineStatus",
    shortname = "ip",
    category = "pangea",
    printcolumn = r#"{"name":"Phase","type":"string","jsonPath":".status.phase"}"#,
    printcolumn = r#"{"name":"AMI","type":"string","jsonPath":".status.build.amiId"}"#,
    printcolumn = r#"{"name":"Plan","type":"string","jsonPath":".status.deploy.planSummary"}"#,
    printcolumn = r#"{"name":"Age","type":"date","jsonPath":".metadata.creationTimestamp"}"#
)]
#[serde(rename_all = "camelCase")]
pub struct ImagePipelineSpec {
    /// Phase 1: Build configuration.
    pub build: PipelineBuildSpec,

    /// Phase 2: Test configuration. Omit to skip testing.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub test: Option<PipelineTestSpec>,

    /// Phase 3+4: Deploy and verify configuration.
    pub deploy: PipelineDeploySpec,

    /// Suspend reconciliation.
    #[serde(default)]
    pub suspend: bool,
}

// ---------------------------------------------------------------------------
// Build spec
// ---------------------------------------------------------------------------

/// Configuration for the build phase.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct PipelineBuildSpec {
    /// Source of the Packer template (Ruby DSL or pre-compiled JSON).
    pub source: TemplateSource,

    /// Variables injected into `packer build`.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub variables: BTreeMap<String, serde_json::Value>,

    /// Provider credentials for the build.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_credentials: Option<ProviderCredentials>,

    /// Build timeout. Defaults to "45m".
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout: Option<String>,
}

// ---------------------------------------------------------------------------
// Test spec
// ---------------------------------------------------------------------------

/// Configuration for the test phase.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct PipelineTestSpec {
    /// Test suites to run (DAG-ordered).
    pub suites: Vec<TestSuite>,

    /// Failure policy for the test phase.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failure_policy: Option<FailurePolicy>,

    /// Provider credentials for test infrastructure.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_credentials: Option<ProviderCredentials>,
}

// ---------------------------------------------------------------------------
// Deploy spec
// ---------------------------------------------------------------------------

/// Configuration for the deploy and verify phases.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct PipelineDeploySpec {
    /// Reference to the InfrastructureTemplate to update with the new AMI ID.
    pub target_template_ref: TemplateObjectRef,

    /// Name of the variable in the template that holds the AMI ID.
    pub ami_variable: String,

    /// Approval mode for the deploy phase.
    #[serde(default)]
    pub approval: ApprovalMode,

    /// Pre-apply assertions evaluated against the `tofu plan` output.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub plan_assertions: Vec<PlanAssertion>,

    /// Post-apply health checks to verify the deployment.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub health_checks: Vec<HealthCheck>,

    /// Rollback configuration. If omitted, no automatic rollback.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rollback: Option<RollbackConfig>,
}

/// Approval configuration for the deploy phase.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ApprovalMode {
    // NOTE: the `///` line below is exactly the original text —
    // deliberately NOT expanded — because schemars bakes every `///`
    // doc comment into the generated CRD's `description:`, and
    // `imagepipelines.pangea.pleme.io` is NOT in
    // `scripts/crd-drift-baseline.txt` (unlike `infrastructuretemplates`),
    // so a changed description here is live, un-baselined CRD-schema
    // drift that fails `.github/workflows/crd-drift.yml` outright. See
    // `ApprovalMode::kind` and `ApprovalModeKind` (below) for the full
    // rationale instead — those aren't part of any CRD schema, so their
    // doc comments are free to be as long as they need to be.
    /// Mode: "auto", "manual", or "webhook".
    #[serde(default = "default_approval_mode")]
    pub mode: String,

    /// Webhook URL (required when mode is "webhook").
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub webhook_url: Option<String>,
}

fn default_approval_mode() -> String {
    "manual".to_string()
}

impl Default for ApprovalMode {
    fn default() -> Self {
        Self {
            mode: "manual".to_string(),
            webhook_url: None,
        }
    }
}

/// Closed vocabulary for [`ApprovalMode::mode`]. Not the wire type
/// (see that field's doc) — [`ApprovalMode::kind`] is the one place a
/// caller converts the wire string into a value the compiler can
/// exhaustively match on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApprovalModeKind {
    /// Apply immediately once a plan exists — no human/webhook gate.
    Auto,
    /// Wait for `status.deploy.approvedBy` to be set by a human.
    Manual,
    /// Declared in the vocabulary (`mode: "webhook"` is a documented,
    /// schema-valid value) but not wired to any dispatch yet — no
    /// code path reads `webhook_url`. Kept as its own variant (never
    /// folded into Manual, and never treated as an unrecognized
    /// value) so the controller can fail loudly with a message that
    /// says exactly that, instead of quietly behaving like Manual and
    /// hanging in AwaitingApproval forever with no operator signal.
    Webhook,
}

impl ApprovalMode {
    /// Parse `self.mode` into its typed vocabulary member.
    ///
    /// `Err(_)` carries an operator-facing message naming the actual
    /// unrecognized value. The controller surfaces this as the
    /// ImagePipeline's Failed-phase condition (see
    /// `image_pipeline_controller::handle_planning`) instead of the
    /// old behavior of silently sitting in AwaitingApproval forever.
    pub fn kind(&self) -> Result<ApprovalModeKind, String> {
        match self.mode.as_str() {
            "auto" => Ok(ApprovalModeKind::Auto),
            "manual" => Ok(ApprovalModeKind::Manual),
            "webhook" => Ok(ApprovalModeKind::Webhook),
            other => Err(format!(
                "unrecognized approval.mode {other:?}; valid values are \"auto\", \"manual\", \"webhook\""
            )),
        }
    }
}

// ---------------------------------------------------------------------------
// Plan assertions
// ---------------------------------------------------------------------------

/// Pre-apply assertion evaluated against the infrastructure plan.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct PlanAssertion {
    /// Human-readable name for this assertion.
    pub name: String,

    /// The assertion rule.
    pub rule: PlanAssertionRule,
}

/// Rules for plan assertions. Only one field should be set per assertion.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct PlanAssertionRule {
    /// Maximum number of resources that may be destroyed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_destroyed: Option<u32>,

    /// Maximum number of resources that may be added.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_added: Option<u32>,

    /// Only these resource types may be changed.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub allowed_resource_types: Vec<String>,

    /// These resource types must NOT be changed.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub forbidden_resource_types: Vec<String>,

    /// Maximum total resources changed (added + changed + destroyed).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_total_changed: Option<u32>,
}

// ---------------------------------------------------------------------------
// Health checks
// ---------------------------------------------------------------------------

/// Post-deploy health check.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct HealthCheck {
    /// Human-readable name for this check.
    pub name: String,

    /// Type of health check.
    pub check_type: HealthCheckType,

    /// Number of retries before failing. Defaults to 10.
    #[serde(default = "default_retries")]
    pub retries: u32,

    /// Interval between retries. Defaults to "10s".
    #[serde(default = "default_check_interval")]
    pub interval: String,
}

fn default_retries() -> u32 {
    10
}

fn default_check_interval() -> String {
    "10s".to_string()
}

/// Type of health check. Set one of http, tcp, or kubernetes.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct HealthCheckType {
    /// HTTP GET probe.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub http: Option<HttpHealthCheck>,

    /// TCP connection probe.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tcp: Option<TcpHealthCheck>,

    /// Kubernetes resource check via a Job.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kubernetes: Option<KubernetesHealthCheck>,
}

/// HTTP health check configuration.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct HttpHealthCheck {
    /// URL to probe.
    pub endpoint: String,
    /// Expected HTTP status code.
    pub expected_status: u16,
}

/// TCP health check configuration.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct TcpHealthCheck {
    /// Host:port to connect to.
    pub endpoint: String,
}

/// Kubernetes health check configuration.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct KubernetesHealthCheck {
    /// Secret containing kubeconfig for the target cluster.
    pub kubeconfig_secret_ref: SecretRef,
    /// kubectl/script command to execute.
    pub check: String,
}

// ---------------------------------------------------------------------------
// Rollback
// ---------------------------------------------------------------------------

/// Rollback configuration.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct RollbackConfig {
    /// Enable automatic rollback.
    #[serde(default = "default_true")]
    pub enabled: bool,

    /// Conditions that trigger automatic rollback.
    #[serde(default)]
    pub auto_rollback_on: Vec<RollbackTrigger>,

    /// Keep the failed AMI for debugging instead of deregistering it.
    #[serde(default = "default_true")]
    pub retain_failed_ami: bool,
}

fn default_true() -> bool {
    true
}

/// Conditions that trigger automatic rollback.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum RollbackTrigger {
    /// Rollback when a health check fails.
    HealthCheckFailure,
    /// Rollback when a plan assertion fails.
    PlanAssertionFailure,
    /// Rollback when `tofu apply` fails.
    ApplyFailure,
}

// ---------------------------------------------------------------------------
// Phase and Status
// ---------------------------------------------------------------------------

/// Lifecycle phase of an ImagePipeline.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema, Display, Default,
)]
pub enum ImagePipelinePhase {
    /// Initial state.
    #[default]
    Pending,
    /// Building AMI (child PackerBuild in progress).
    Building,
    /// Testing AMI (child AmiTest in progress).
    Testing,
    /// Running `tofu plan` on the target template.
    Planning,
    /// Waiting for manual or webhook approval.
    AwaitingApproval,
    /// Running `tofu apply` on the target template.
    Applying,
    /// Running post-deploy health checks.
    Verifying,
    /// Pipeline completed successfully.
    Completed,
    /// Pipeline failed.
    Failed,
    /// Rolling back to the previous AMI.
    RollingBack,
}

/// Status of an ImagePipeline.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ImagePipelineStatus {
    /// Current phase of the pipeline.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub phase: Option<ImagePipelinePhase>,

    /// Build phase results.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub build: Option<PipelineBuildStatus>,

    /// Test phase results.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub test: Option<PipelineTestStatus>,

    /// Deploy phase results.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deploy: Option<PipelineDeployStatus>,

    /// Verification phase results.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub verification: Option<PipelineVerificationStatus>,

    /// Kubernetes-style conditions.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub conditions: Vec<Condition>,

    /// Last observed generation of the spec.
    #[serde(default)]
    pub observed_generation: i64,

    /// Timestamp when pipeline started.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub started_at: Option<DateTime<Utc>>,

    /// Timestamp when pipeline completed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub completed_at: Option<DateTime<Utc>>,
}

/// Build phase status within the pipeline.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct PipelineBuildStatus {
    /// Name of the child PackerBuild CR.
    pub packer_build_ref: String,

    /// AMI ID produced by the build.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ami_id: Option<String>,

    /// AWS region of the built AMI.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ami_region: Option<String>,
}

/// Test phase status within the pipeline.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct PipelineTestStatus {
    /// Name of the child AmiTest CR.
    pub ami_test_ref: String,

    /// Whether all test suites passed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub passed: Option<bool>,

    /// Human-readable summary (e.g., "3/3 passed").
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub summary: Option<String>,
}

/// Deploy phase status within the pipeline.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct PipelineDeployStatus {
    /// Plan summary from `tofu plan` (e.g., "+0 ~2 -0").
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub plan_summary: Option<String>,

    /// Hash of the pending plan.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub plan_hash: Option<String>,

    /// Who approved the plan (for manual approval mode).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub approved_by: Option<String>,

    /// When the plan was approved.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub approved_at: Option<DateTime<Utc>>,

    /// AMI ID before the pipeline started (for rollback).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub previous_ami_id: Option<String>,

    /// Comparison of expected vs actual changes.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub expected_vs_actual: Vec<ChangeVerification>,
}

/// Comparison of a single resource change: expected vs actual.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ChangeVerification {
    /// Resource address (e.g., "aws_launch_template.control_plane").
    pub resource: String,

    /// What the plan predicted.
    pub expected: String,

    /// What actually happened.
    pub actual: String,

    /// Whether expected matched actual.
    pub matched: bool,
}

/// Verification phase status.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct PipelineVerificationStatus {
    /// Per-check results.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub health_checks: Vec<HealthCheckResult>,

    /// Whether all health checks passed.
    #[serde(default)]
    pub all_passed: bool,
}

/// Result of a single health check.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct HealthCheckResult {
    /// Check name (matches HealthCheck.name).
    pub name: String,

    /// Whether the check passed.
    pub passed: bool,

    /// Number of attempts made.
    pub attempts: u32,

    /// Error message if failed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mode(mode: &str) -> ApprovalMode {
        ApprovalMode {
            mode: mode.to_string(),
            webhook_url: None,
        }
    }

    #[test]
    fn kind_recognizes_auto() {
        assert_eq!(mode("auto").kind(), Ok(ApprovalModeKind::Auto));
    }

    #[test]
    fn kind_recognizes_manual() {
        assert_eq!(mode("manual").kind(), Ok(ApprovalModeKind::Manual));
    }

    #[test]
    fn kind_recognizes_webhook_as_its_own_variant_not_manual() {
        // FAILURE SCENARIO this regresses: before this fix the
        // controller only ever asked `is_auto()`, so "webhook" — a
        // real, documented, schema-valid value — fell into the same
        // `else` branch as "manual" with no distinction and no error.
        assert_eq!(mode("webhook").kind(), Ok(ApprovalModeKind::Webhook));
        assert_ne!(mode("webhook").kind(), mode("manual").kind());
    }

    #[test]
    fn kind_rejects_a_typo_instead_of_silently_becoming_manual() {
        // FAILURE SCENARIO this regresses: `mode: "Auto"` (a plausible
        // capitalization typo) or any other unrecognized string. Before
        // this fix, `ApprovalMode::is_auto()` returning `false` was the
        // ONLY signal the controller read, so a typo — exactly like
        // "webhook" above — silently behaved like "manual" and the
        // pipeline hung in AwaitingApproval forever with zero error.
        let err = mode("Auto").kind().unwrap_err();
        assert!(
            err.contains("Auto"),
            "error should name the bad value: {err}"
        );
        assert!(err.contains("auto") && err.contains("manual") && err.contains("webhook"));
    }

    #[test]
    fn kind_is_never_ok_for_an_empty_string() {
        assert!(mode("").kind().is_err());
    }

    #[test]
    fn default_mode_is_manual_and_parses() {
        assert_eq!(ApprovalMode::default().kind(), Ok(ApprovalModeKind::Manual));
    }

    // M0 coverage floor (theory/PANGEA-OPERATOR.md §XVII row 6): pin the
    // Default variant before swapping the hand-written `impl Default` for
    // std `#[derive(Default)]` + `#[default]`.
    #[test]
    fn image_pipeline_phase_default_is_pending() {
        assert_eq!(ImagePipelinePhase::default(), ImagePipelinePhase::Pending);
    }
}
