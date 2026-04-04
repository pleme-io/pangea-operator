//! ComplianceSchedule CRD definition.
//!
//! Drives continuous compliance testing against live infrastructure managed by
//! pangea-operator. Supports two test runners:
//!
//! - **kensa** — Rust-native compliance engine with 8 Tier-1 resource types,
//!   NIST 800-53 / OSCAL mapping, and BLAKE3 result hashing.
//! - **InSpec** — Ruby-based profiles (e.g., inspec-akeyless) executed via Jobs.
//!
//! Results flow into:
//! - **tameshi** HeartbeatChain (cryptographic audit trail)
//! - **Vector/Shinryu** pipeline (structured observability events)
//! - **Prometheus** metrics (control pass rates, drift counters)
//! - **sekiban** SignatureGate/Certification updates (admission gating)
//!
//! Example:
//! ```yaml
//! apiVersion: pangea.pleme.io/v1alpha1
//! kind: ComplianceSchedule
//! metadata:
//!   name: prod-nist-continuous
//! spec:
//!   schedule: "0 */6 * * *"
//!   suites:
//!     - name: akeyless-secrets
//!       runner: inspec
//!       profile: inspec-akeyless
//!     - name: nist-moderate
//!       runner: kensa
//!       profile: nist-800-53-moderate
//!     - name: custom-checks
//!       runner: script
//!       image: ghcr.io/pleme-io/custom-compliance:v1
//!   reporting:
//!     vector:
//!       endpoint: vector.observe.svc:6514
//!       topic: infrastructure-compliance
//!     prometheus:
//!       enabled: true
//!     s3:
//!       bucket: compliance-reports
//!   attestation:
//!     enabled: true
//!     heartbeatChain: s3://attestations/heartbeat
//! ```

use chrono::{DateTime, Utc};
use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use strum::Display;

use super::{Condition, ProviderCredentials, SecretRef, TemplateObjectRef};

/// ComplianceSchedule drives continuous compliance testing against live
/// infrastructure and feeds results into the observability + gating pipeline.
#[derive(CustomResource, Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[kube(
    group = "pangea.pleme.io",
    version = "v1alpha1",
    kind = "ComplianceSchedule",
    namespaced,
    status = "ComplianceScheduleStatus",
    shortname = "cs",
    printcolumn = r#"{"name":"Phase","type":"string","jsonPath":".status.phase"}"#,
    printcolumn = r#"{"name":"Schedule","type":"string","jsonPath":".spec.schedule"}"#,
    printcolumn = r#"{"name":"Last Run","type":"date","jsonPath":".status.lastRunAt"}"#,
    printcolumn = r#"{"name":"Controls","type":"string","jsonPath":".status.controlSummary"}"#,
    printcolumn = r#"{"name":"Age","type":"date","jsonPath":".metadata.creationTimestamp"}"#
)]
#[serde(rename_all = "camelCase")]
pub struct ComplianceScheduleSpec {
    /// Cron schedule for compliance runs (e.g., "0 */6 * * *").
    /// If omitted, runs only on infrastructure change events.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schedule: Option<String>,

    /// Trigger a run when referenced InfrastructureTemplates change.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub on_change: Vec<TemplateObjectRef>,

    /// Compliance test suites to execute.
    pub suites: Vec<ComplianceSuite>,

    /// Provider credentials for test execution (AWS, etc.).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_credentials: Option<ProviderCredentials>,

    /// Reporting/observability destinations.
    #[serde(default)]
    pub reporting: ReportingConfig,

    /// Tameshi attestation configuration.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub attestation: Option<AttestationConfig>,

    /// Container image for the test runner Jobs.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub runner_image: Option<String>,

    /// Maximum concurrent suite executions.
    #[serde(default = "default_parallelism")]
    pub parallelism: u32,

    /// Suspend scheduling (manual runs still allowed).
    #[serde(default)]
    pub suspend: bool,
}

fn default_parallelism() -> u32 {
    2
}

// ---------------------------------------------------------------------------
// Compliance suites
// ---------------------------------------------------------------------------

/// A single compliance suite to execute.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ComplianceSuite {
    /// Unique name within this schedule.
    pub name: String,

    /// Which runner executes this suite.
    pub runner: ComplianceRunner,

    /// Runner-specific profile name or path.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub profile: Option<String>,

    /// Additional runner configuration.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(schema_with = "super::opaque_json_schema")]
    pub config: Option<serde_json::Value>,

    /// Timeout for this suite. Defaults to "15m".
    #[serde(default = "default_suite_timeout")]
    pub timeout: String,

    /// NIST 800-53 control family mappings for this suite.
    /// Used for compliance reporting and OSCAL export.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub control_families: Vec<String>,

    /// Specific controls to skip (waiver).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub waivers: Vec<String>,
}

fn default_suite_timeout() -> String {
    "15m".to_string()
}

/// Compliance test runner type.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, Display)]
#[serde(rename_all = "camelCase")]
pub enum ComplianceRunner {
    /// kensa — Rust-native compliance engine.
    /// Uses Tier-1 resources + NIST/OSCAL mapping.
    Kensa,

    /// InSpec — Ruby-based compliance profiles.
    /// Executes `inspec exec <profile>` with JSON reporter.
    InSpec,

    /// Arbitrary script in a K8s Job.
    /// Must output JSON results to stdout.
    Script,
}

// ---------------------------------------------------------------------------
// Reporting
// ---------------------------------------------------------------------------

/// Observability and reporting configuration.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ReportingConfig {
    /// Emit structured events to Vector/Shinryu.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub vector: Option<VectorConfig>,

    /// Expose Prometheus metrics.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prometheus: Option<PrometheusConfig>,

    /// Store reports in S3.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub s3: Option<S3ReportConfig>,

    /// Report retention in days.
    #[serde(default = "default_retention_days")]
    pub retention_days: u32,
}

fn default_retention_days() -> u32 {
    90
}

/// Vector/Shinryu reporting configuration.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct VectorConfig {
    /// Vector endpoint (e.g., "vector.observe.svc:6514").
    pub endpoint: String,

    /// Topic/source name for events.
    #[serde(default = "default_vector_topic")]
    pub topic: String,

    /// Encoding format: "json" (default) or "logfmt".
    #[serde(default = "default_encoding")]
    pub encoding: String,
}

fn default_vector_topic() -> String {
    "infrastructure-compliance".to_string()
}

fn default_encoding() -> String {
    "json".to_string()
}

/// Prometheus metrics configuration.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct PrometheusConfig {
    /// Enable Prometheus metrics.
    #[serde(default = "default_true")]
    pub enabled: bool,

    /// Metric name prefix (e.g., "pangea_compliance_").
    #[serde(default = "default_metric_prefix")]
    pub prefix: String,
}

fn default_true() -> bool {
    true
}

fn default_metric_prefix() -> String {
    "pangea_compliance_".to_string()
}

/// S3 report storage configuration.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct S3ReportConfig {
    /// S3 bucket name.
    pub bucket: String,

    /// S3 key prefix.
    #[serde(default = "default_s3_prefix")]
    pub prefix: String,

    /// AWS region.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub region: Option<String>,

    /// Credentials for S3 access.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub credentials_secret_ref: Option<SecretRef>,
}

fn default_s3_prefix() -> String {
    "compliance-reports/".to_string()
}

// ---------------------------------------------------------------------------
// Attestation
// ---------------------------------------------------------------------------

/// Tameshi attestation integration.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct AttestationConfig {
    /// Enable BLAKE3 attestation of compliance results.
    #[serde(default = "default_true")]
    pub enabled: bool,

    /// S3 URI for HeartbeatChain persistence.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub heartbeat_chain_uri: Option<String>,

    /// Layer types to collect and hash for CertificationArtifact.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub layer_types: Vec<String>,

    /// Reference to a sekiban SignatureGate to update with compliance hash.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub signature_gate_ref: Option<String>,

    /// Reference to a sekiban Certification to update.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub certification_ref: Option<String>,
}

// ---------------------------------------------------------------------------
// Phase and Status
// ---------------------------------------------------------------------------

/// Lifecycle phase of a ComplianceSchedule.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema, Display)]
pub enum ComplianceSchedulePhase {
    /// Waiting for first scheduled run.
    Idle,
    /// Compliance suites are executing.
    Running,
    /// All suites passed on last run.
    Compliant,
    /// One or more suites failed on last run.
    NonCompliant,
    /// Error during execution.
    Error,
}

impl Default for ComplianceSchedulePhase {
    fn default() -> Self {
        ComplianceSchedulePhase::Idle
    }
}

/// Status of a ComplianceSchedule.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ComplianceScheduleStatus {
    /// Current phase.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub phase: Option<ComplianceSchedulePhase>,

    /// Timestamp of last completed run.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_run_at: Option<DateTime<Utc>>,

    /// Timestamp of next scheduled run.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_run_at: Option<DateTime<Utc>>,

    /// Per-suite results from last run.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub suites: Vec<ComplianceSuiteResult>,

    /// Human-readable control summary (e.g., "118/120 controls passed").
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub control_summary: Option<String>,

    /// Aggregate compliance hash (BLAKE3 of all suite results).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub compliance_hash: Option<String>,

    /// Number of consecutive compliant runs.
    #[serde(default)]
    pub consecutive_compliant: u32,

    /// Number of consecutive non-compliant runs.
    #[serde(default)]
    pub consecutive_non_compliant: u32,

    /// Total runs completed.
    #[serde(default)]
    pub total_runs: u64,

    /// Kubernetes-style conditions.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub conditions: Vec<Condition>,

    /// Last observed generation.
    #[serde(default)]
    pub observed_generation: i64,
}

/// Result of a single compliance suite execution.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ComplianceSuiteResult {
    /// Suite name.
    pub name: String,

    /// Runner used.
    pub runner: ComplianceRunner,

    /// Whether the suite passed.
    pub passed: bool,

    /// Number of controls assessed.
    #[serde(default)]
    pub controls_assessed: u32,

    /// Number of controls passed.
    #[serde(default)]
    pub controls_passed: u32,

    /// Number of controls failed.
    #[serde(default)]
    pub controls_failed: u32,

    /// Number of controls skipped (waived).
    #[serde(default)]
    pub controls_skipped: u32,

    /// Failed control IDs (e.g., ["AC-3", "AU-2"]).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub failed_controls: Vec<String>,

    /// BLAKE3 hash of this suite's results.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result_hash: Option<String>,

    /// K8s Job name that executed this suite.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub job_name: Option<String>,

    /// Execution duration.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub duration: Option<String>,

    /// Timestamp of completion.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub completed_at: Option<DateTime<Utc>>,
}
