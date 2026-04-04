//! SynthesizerFormat CRD definition.
//!
//! Cluster-scoped resource that declares a new synthesizer output format.
//! The compiler sidecar materializes a TypedArraySynthesizer from this
//! definition on the fly — no code changes needed to support a new tool's
//! JSON schema.
//!
//! Example:
//! ```yaml
//! apiVersion: pangea.pleme.io/v1alpha1
//! kind: SynthesizerFormat
//! metadata:
//!   name: packer
//! spec:
//!   description: "HashiCorp Packer machine image builder"
//!   arraySections:
//!     - name: builders
//!       typeField: type
//!       keyTransform: snake-to-kebab
//!     - name: provisioners
//!       typeField: type
//!       keyTransform: snake-to-kebab
//!     - name: post-processors
//!       typeField: type
//!       keyTransform: snake-to-kebab
//!   mapSections:
//!     - name: variables
//!   keyTransform: snake-to-kebab       # default for all sections
//!   compileEndpoint: /compile-packer   # optional convenience alias
//! ```

use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use strum::Display;

use super::Condition;

/// SynthesizerFormat declares a synthesizer output format that the compiler
/// sidecar can materialize on the fly.
#[derive(CustomResource, Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[kube(
    group = "pangea.pleme.io",
    version = "v1alpha1",
    kind = "SynthesizerFormat",
    status = "SynthesizerFormatStatus",
    shortname = "sf",
    printcolumn = r#"{"name":"Sections","type":"string","jsonPath":".status.sectionSummary"}"#,
    printcolumn = r#"{"name":"Phase","type":"string","jsonPath":".status.phase"}"#,
    printcolumn = r#"{"name":"Age","type":"date","jsonPath":".metadata.creationTimestamp"}"#
)]
#[serde(rename_all = "camelCase")]
pub struct SynthesizerFormatSpec {
    /// Human-readable description of what this format produces.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,

    /// Sections that are arrays of typed objects.
    /// Each entry in the array gets a type field derived from the DSL method's
    /// first argument (e.g., `synth.builder(:amazon_ebs, ...)` → `{"type": "amazon-ebs"}`).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub array_sections: Vec<ArraySectionSpec>,

    /// Sections that are key-value maps.
    /// Entries are stored by name (e.g., `synth.variable(:ami_id, ...)` → `{"ami_id": {...}}`).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub map_sections: Vec<MapSectionSpec>,

    /// Default key transform applied to all hash keys in the output.
    /// Individual sections can override this.
    #[serde(default)]
    pub key_transform: KeyTransform,

    /// Optional convenience endpoint path to register on the compiler sidecar.
    /// E.g., "/compile-packer" creates a dedicated endpoint that doesn't
    /// require the `format` field in the request body.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub compile_endpoint: Option<String>,

    /// Ruby modules to extend the synthesizer with before evaluation.
    /// These must be available in the compiler sidecar's load path.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub extend_modules: Vec<String>,

    /// Static preamble Ruby code injected before the user source.
    /// Useful for requiring libraries or defining helper methods.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub preamble: Option<String>,
}

/// Specification for an array section.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ArraySectionSpec {
    /// Section name in the output JSON (e.g., "builders", "provisioners").
    pub name: String,

    /// Name of the field that holds the type identifier within each entry.
    /// Defaults to "type".
    #[serde(default = "default_type_field")]
    pub type_field: String,

    /// Whether to include an optional "name" field from the DSL's second
    /// positional argument. Defaults to true.
    #[serde(default = "default_true")]
    pub allow_name: bool,

    /// Key transform override for this section. If unset, uses the format default.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key_transform: Option<KeyTransform>,

    /// If set, the DSL method name is this value instead of the auto-singularized
    /// section name. E.g., section "post-processors" auto-singularizes to
    /// "post-processor"; override to use "post_processor" instead.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub method_alias: Option<String>,
}

fn default_type_field() -> String {
    "type".to_string()
}

fn default_true() -> bool {
    true
}

/// Specification for a map section.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct MapSectionSpec {
    /// Section name in the output JSON (e.g., "variables", "env").
    pub name: String,

    /// Key transform override for this section.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key_transform: Option<KeyTransform>,

    /// DSL method alias override.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub method_alias: Option<String>,
}

/// Key transformation strategy for hash keys in the synthesized output.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema, Display, Default)]
#[serde(rename_all = "kebab-case")]
pub enum KeyTransform {
    /// No transformation — keys pass through as-is.
    #[default]
    None,

    /// Convert Ruby snake_case to kebab-case (e.g., `ami_name` → `ami-name`).
    /// This is the Packer convention.
    SnakeToKebab,

    /// Convert Ruby snake_case to camelCase (e.g., `instance_type` → `instanceType`).
    /// This is the AWS/CloudFormation convention.
    SnakeToCamel,

    /// Convert to SCREAMING_SNAKE_CASE (e.g., `api_key` → `API_KEY`).
    /// Common for environment variable maps.
    SnakeToScreaming,
}

// ---------------------------------------------------------------------------
// Status
// ---------------------------------------------------------------------------

/// Lifecycle phase of a SynthesizerFormat.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema, Display)]
pub enum SynthesizerFormatPhase {
    /// Validated and ready for use.
    Ready,
    /// Validation failed (e.g., duplicate section names, invalid config).
    Invalid,
}

impl Default for SynthesizerFormatPhase {
    fn default() -> Self {
        SynthesizerFormatPhase::Ready
    }
}

/// Status of a SynthesizerFormat.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SynthesizerFormatStatus {
    /// Current phase.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub phase: Option<SynthesizerFormatPhase>,

    /// Human-readable summary of sections (e.g., "3 array, 1 map").
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub section_summary: Option<String>,

    /// Generated DSL method names for documentation.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub dsl_methods: Vec<String>,

    /// Kubernetes-style conditions.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub conditions: Vec<Condition>,

    /// Last observed generation.
    #[serde(default)]
    pub observed_generation: i64,

    /// Validation error if phase is Invalid.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

// ---------------------------------------------------------------------------
// Conversion to sidecar request format
// ---------------------------------------------------------------------------

impl SynthesizerFormatSpec {
    /// Serialize this format spec into the JSON structure the compiler sidecar
    /// expects in the `format_definition` field of a compile request.
    pub fn to_sidecar_definition(&self) -> serde_json::Value {
        let array_sections: Vec<serde_json::Value> = self
            .array_sections
            .iter()
            .map(|s| {
                let mut obj = serde_json::json!({
                    "name": s.name,
                    "type_field": s.type_field,
                    "allow_name": s.allow_name,
                });
                if let Some(ref kt) = s.key_transform {
                    obj["key_transform"] = serde_json::json!(kt);
                }
                if let Some(ref alias) = s.method_alias {
                    obj["method_alias"] = serde_json::json!(alias);
                }
                obj
            })
            .collect();

        let map_sections: Vec<serde_json::Value> = self
            .map_sections
            .iter()
            .map(|s| {
                let mut obj = serde_json::json!({ "name": s.name });
                if let Some(ref kt) = s.key_transform {
                    obj["key_transform"] = serde_json::json!(kt);
                }
                if let Some(ref alias) = s.method_alias {
                    obj["method_alias"] = serde_json::json!(alias);
                }
                obj
            })
            .collect();

        serde_json::json!({
            "array_sections": array_sections,
            "map_sections": map_sections,
            "key_transform": self.key_transform,
            "extend_modules": self.extend_modules,
            "preamble": self.preamble,
        })
    }
}
