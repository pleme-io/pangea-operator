//! AmiTest CRD definition.
//!
//! Validates an AMI through tiered test suites before it enters the deployment
//! chain. Supports boot validation (Packer test), multi-node cluster formation
//! tests, InSpec compliance scans, and arbitrary script execution via K8s Jobs.
//! Suites form a DAG — each suite can declare dependencies on other suites.

use chrono::{DateTime, Utc};
use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use strum::Display;

use super::{Condition, ProviderCredentials};

/// AmiTest validates an AMI through a DAG of test suites.
#[derive(CustomResource, Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[kube(
    group = "pangea.pleme.io",
    version = "v1alpha1",
    kind = "AmiTest",
    namespaced,
    status = "AmiTestStatus",
    shortname = "at",
    category = "pangea",
    printcolumn = r#"{"name":"Phase","type":"string","jsonPath":".status.phase"}"#,
    printcolumn = r#"{"name":"AMI","type":"string","jsonPath":".status.amiId"}"#,
    printcolumn = r#"{"name":"Suites","type":"string","jsonPath":".status.suiteSummary"}"#,
    printcolumn = r#"{"name":"Age","type":"date","jsonPath":".metadata.creationTimestamp"}"#
)]
#[serde(rename_all = "camelCase")]
pub struct AmiTestSpec {
    /// Source of the AMI to test — reference a PackerBuild CR or specify inline.
    pub ami_source: AmiSource,

    /// Test suites forming a DAG. Each suite can declare dependencies.
    pub suites: Vec<TestSuite>,

    /// Provider credentials (AWS) for test infrastructure.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_credentials: Option<ProviderCredentials>,

    /// What to do when a suite fails.
    #[serde(default)]
    pub failure_policy: FailurePolicy,

    /// Container image to use for test Jobs (must contain ami-forge).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub test_runner_image: Option<String>,

    /// Suspend reconciliation.
    #[serde(default)]
    pub suspend: bool,
}

/// Source of the AMI to test.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum AmiSource {
    /// Reference to a PackerBuild CR in the same namespace.
    PackerBuildRef {
        /// Name of the PackerBuild CR.
        name: String,
    },

    /// Inline AMI specification.
    Inline {
        /// AMI ID (e.g., "ami-0abc123def456").
        ami_id: String,
        /// AWS region of the AMI.
        region: String,
    },
}

/// A single test suite within the DAG.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct TestSuite {
    /// Unique name for this suite within the AmiTest.
    pub name: String,

    /// Type of test to run.
    pub suite_type: TestSuiteType,

    /// Suite names that must complete successfully before this suite runs.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub depends_on: Vec<String>,

    /// Timeout for this suite. Defaults to "15m".
    #[serde(default = "default_suite_timeout")]
    pub timeout: String,

    /// Type-specific configuration (JSON value interpreted per suite_type).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(schema_with = "super::opaque_json_schema")]
    pub config: Option<serde_json::Value>,
}

fn default_suite_timeout() -> String {
    "15m".to_string()
}

/// Type of test suite.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, Display)]
#[serde(rename_all = "camelCase")]
pub enum TestSuiteType {
    /// Boot an EC2 instance from the AMI, run a validation script, teardown.
    /// Config: `{ "test_template": "...", "variables": {} }`
    PackerTest,

    /// Multi-node K3s + WireGuard cluster formation test (ami-forge cluster-test).
    /// Config: `{ "node_count": 3, "validate_fluxcd": true, "validate_vpn": true }`
    ClusterTest,

    /// InSpec compliance profile execution.
    /// Config: `{ "profile": "inspec-akeyless", "controls": [] }`
    InSpec,

    /// Arbitrary script execution in a K8s Job.
    /// Config: `{ "image": "...", "command": [...], "args": [...] }`
    Script,
}

/// Policy for handling test failures.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct FailurePolicy {
    /// Deregister the AMI on test failure.
    #[serde(default)]
    pub deregister_ami: bool,

    /// Delete orphaned EBS snapshots on AMI deregistration.
    #[serde(default)]
    pub cleanup_snapshots: bool,
}

impl Default for FailurePolicy {
    fn default() -> Self {
        Self {
            deregister_ami: false,
            cleanup_snapshots: false,
        }
    }
}

/// Lifecycle phase of an AmiTest.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema, Display)]
pub enum AmiTestPhase {
    /// Initial state, waiting to be processed.
    Pending,
    /// Resolving AMI ID from PackerBuild reference.
    Resolving,
    /// Test suites are executing.
    Running,
    /// All suites passed.
    Passed,
    /// One or more suites failed.
    Failed,
    /// Cleaning up (deregistering AMI, deleting snapshots).
    Cleaning,
}

impl Default for AmiTestPhase {
    fn default() -> Self {
        AmiTestPhase::Pending
    }
}

/// Phase of a single test suite.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema, Display)]
pub enum SuitePhase {
    /// Waiting for dependencies.
    Pending,
    /// Suite is executing.
    Running,
    /// Suite completed successfully.
    Succeeded,
    /// Suite failed.
    Failed,
    /// Suite was skipped (dependency failed).
    Skipped,
}

impl Default for SuitePhase {
    fn default() -> Self {
        SuitePhase::Pending
    }
}

/// Status of an AmiTest.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct AmiTestStatus {
    /// Current phase of the test lifecycle.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub phase: Option<AmiTestPhase>,

    /// Resolved AMI ID being tested.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ami_id: Option<String>,

    /// AWS region of the AMI.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ami_region: Option<String>,

    /// Per-suite results.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub suites: Vec<SuiteResult>,

    /// Human-readable summary (e.g., "3/3 passed").
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub suite_summary: Option<String>,

    /// Kubernetes-style conditions.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub conditions: Vec<Condition>,

    /// Last observed generation of the spec.
    #[serde(default)]
    pub observed_generation: i64,

    /// Timestamp when testing started.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub started_at: Option<DateTime<Utc>>,

    /// Timestamp when testing completed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub completed_at: Option<DateTime<Utc>>,
}

/// Result of a single test suite execution.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SuiteResult {
    /// Suite name (matches TestSuite.name).
    pub name: String,

    /// Current phase of this suite.
    pub phase: SuitePhase,

    /// Name of the K8s Job executing this suite.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub job_name: Option<String>,

    /// Execution duration.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub duration: Option<String>,

    /// Human-readable result message.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,

    /// Timestamp when the suite started.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub started_at: Option<DateTime<Utc>>,

    /// Timestamp when the suite completed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub completed_at: Option<DateTime<Utc>>,
}
