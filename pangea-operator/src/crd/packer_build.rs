//! PackerBuild CRD definition.
//!
//! Represents a Packer build execution managed by the operator.
//! The Ruby DSL source is compiled to Packer JSON via the compiler sidecar,
//! then executed with `packer build`. The resulting AMI ID is extracted from
//! the packer-manifest.json and stored in status.

use chrono::{DateTime, Utc};
use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use strum::Display;

use super::{Condition, ProviderCredentials, SecretRef, TemplateSource};

/// PackerBuild represents a single Packer build execution.
/// The operator compiles the Ruby DSL source to Packer JSON, validates it,
/// runs `packer build`, and extracts the AMI ID from the manifest.
#[derive(CustomResource, Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[kube(
    group = "pangea.pleme.io",
    version = "v1alpha1",
    kind = "PackerBuild",
    namespaced,
    status = "PackerBuildStatus",
    shortname = "pb",
    printcolumn = r#"{"name":"Phase","type":"string","jsonPath":".status.phase"}"#,
    printcolumn = r#"{"name":"AMI","type":"string","jsonPath":".status.amiId"}"#,
    printcolumn = r#"{"name":"Duration","type":"string","jsonPath":".status.buildDuration"}"#,
    printcolumn = r#"{"name":"Age","type":"date","jsonPath":".metadata.creationTimestamp"}"#
)]
#[serde(rename_all = "camelCase")]
pub struct PackerBuildSpec {
    /// Source of the Packer template (Ruby DSL or pre-compiled JSON).
    pub source: TemplateSource,

    /// Variables injected into `packer build` via `-var key=value`.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub variables: BTreeMap<String, serde_json::Value>,

    /// References to variable files injected via `-var-file`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub var_files: Vec<VarFileSource>,

    /// Provider credentials (AWS, etc.) mounted as environment variables.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_credentials: Option<ProviderCredentials>,

    /// Build timeout. Defaults to "45m".
    #[serde(default = "default_build_timeout")]
    pub timeout: String,

    /// Packer on-error behavior: "cleanup" (default) or "abort".
    #[serde(default = "default_on_error")]
    pub on_error: String,

    /// Path within workspace for the packer-manifest.json output.
    /// Defaults to "packer-manifest.json".
    #[serde(default = "default_manifest_path")]
    pub manifest_path: String,

    /// If true, the AMI will not be deregistered when the CR is deleted.
    #[serde(default)]
    pub retain_ami: bool,

    /// Suspend reconciliation.
    #[serde(default)]
    pub suspend: bool,
}

fn default_build_timeout() -> String {
    "45m".to_string()
}

fn default_on_error() -> String {
    "cleanup".to_string()
}

fn default_manifest_path() -> String {
    "packer-manifest.json".to_string()
}

/// Source for a variable file (-var-file).
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct VarFileSource {
    /// Inline JSON content for the var file.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(schema_with = "super::opaque_json_schema")]
    pub inline: Option<serde_json::Value>,

    /// Reference to a ConfigMap key containing the var file.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub config_map_ref: Option<super::ConfigMapRef>,

    /// Reference to a Secret key containing the var file.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub secret_ref: Option<SecretRef>,
}

/// Lifecycle phase of a PackerBuild.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema, Display)]
pub enum PackerBuildPhase {
    /// Initial state, waiting to be processed.
    Pending,
    /// Compiling Ruby DSL to Packer JSON via sidecar.
    Compiling,
    /// Running `packer validate`.
    Validating,
    /// Running `packer build`.
    Building,
    /// Parsing packer-manifest.json to extract AMI ID.
    Extracting,
    /// Build completed successfully, AMI ID available.
    Ready,
    /// Build failed.
    Failed,
}

impl Default for PackerBuildPhase {
    fn default() -> Self {
        PackerBuildPhase::Pending
    }
}

/// Status of a PackerBuild.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct PackerBuildStatus {
    /// Current phase of the build lifecycle.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub phase: Option<PackerBuildPhase>,

    /// AMI ID extracted from the packer manifest.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ami_id: Option<String>,

    /// AWS region of the built AMI.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ami_region: Option<String>,

    /// Duration of the packer build phase.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub build_duration: Option<String>,

    /// Raw packer-manifest.json content for debugging.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub packer_manifest: Option<String>,

    /// Kubernetes-style conditions.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub conditions: Vec<Condition>,

    /// Last observed generation of the spec.
    #[serde(default)]
    pub observed_generation: i64,

    /// Last error message if in Failed state.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error_message: Option<String>,

    /// Number of consecutive retries.
    #[serde(default)]
    pub retry_count: i32,

    /// Timestamp of build start.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub started_at: Option<DateTime<Utc>>,

    /// Timestamp of build completion.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub completed_at: Option<DateTime<Utc>>,
}
