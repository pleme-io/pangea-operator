//! Typed Rust mirror of the pangea-operator Helm chart's values surface.
//!
//! Mirrors `helmworks/charts/pangea-operator/values.yaml` so the chart's
//! `values.schema.json` can be generated mechanically from `schemars`
//! rather than hand-authored. Run:
//!
//! ```text
//! cargo run --bin pangea-operator -- --generate-values-schema
//! ```
//!
//! to emit the schema; pipe into `helmworks/charts/pangea-operator/values.schema.json`.
//!
//! Scope: the structurally-typed slice of values where consumer typos
//! cause silent runtime failures. Fields that are pass-through to
//! upstream charts (image, resources, podSecurityContext, …) are
//! captured as `serde_json::Value` because their shapes are owned
//! by upstream charts (Bitnami / pleme-lib) and tracking those would
//! couple our schema to upstream churn.
//!
//! The hand-authored schema in helmworks adds a JSON Schema `allOf`
//! conditional that schemars can't express directly:
//!
//! ```text
//! when useEmbeddedRuby=true → require gemAuth.tokenSecret.name (non-empty)
//! ```
//!
//! The generator threads that conditional onto the schemars output as
//! a post-process step (see `generate_values_schema_json`).

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Top-level pangea-operator chart values.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, Default)]
#[serde(rename_all = "camelCase")]
pub struct ChartValues {
    /// tracing-subscriber EnvFilter directive, threaded into RUST_LOG by
    /// the deployment template (Q4 / chart 0.8.5).
    #[serde(default = "default_log_level")]
    pub log_level: String,

    /// When true, operator runs the magnus+CRuby compiler in-process
    /// (no sidecar). Requires `gem_auth.token_secret.name` to be set
    /// to a non-empty Secret name.
    #[serde(default)]
    pub use_embedded_ruby: bool,

    /// Authentication for fetching operator-runtime gems
    /// (only consulted when use_embedded_ruby=true).
    #[serde(default)]
    pub gem_auth: GemAuth,

    /// OperatorPolicy/default singleton CR config.
    #[serde(default)]
    pub operator_policy: OperatorPolicyValues,

    /// Observability toggles (chart-side; the operator's own metrics
    /// are always emitted).
    #[serde(default)]
    pub monitoring: ToggleSection,

    /// PrometheusRule CR rendering toggle.
    #[serde(default)]
    pub prometheus_rules: ToggleSection,

    /// Pass-through: every other top-level field. Untyped here because
    /// most of the deeper surface (image, resources, sidecars,
    /// podSecurityContext, etc.) is shared with upstream charts and
    /// shouldn't drift independently.
    #[serde(flatten, default)]
    pub other: BTreeMap<String, serde_json::Value>,
}

fn default_log_level() -> String {
    "pangea_operator=info".to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, Default)]
#[serde(rename_all = "camelCase")]
pub struct GemAuth {
    pub token_secret: Option<TokenSecretRef>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct TokenSecretRef {
    /// Secret name. Required-non-empty when use_embedded_ruby=true
    /// (enforced via the generator's post-process step).
    pub name: String,
    /// Key within the Secret. Defaults to "token".
    #[serde(default = "default_token_key")]
    pub key: String,
}

fn default_token_key() -> String {
    "token".to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ToggleSection {
    #[serde(default = "default_true")]
    pub enabled: bool,
}

impl Default for ToggleSection {
    fn default() -> Self {
        Self { enabled: true }
    }
}

fn default_true() -> bool {
    true
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, Default)]
#[serde(rename_all = "camelCase")]
pub struct OperatorPolicyValues {
    /// Whether to render the OperatorPolicy/default CR.
    #[serde(default = "default_true")]
    pub create: bool,

    /// Master kill-switch.
    #[serde(default)]
    pub global_suspend: bool,

    /// Operator-readable reason surfaced in logs + Events when
    /// global_suspend=true.
    #[serde(default)]
    pub global_suspend_reason: String,

    /// Per-controller suspend.
    #[serde(default)]
    pub controller_suspend: ControllerSuspendValues,

    /// Per-workspace tri-state suspend.
    #[serde(default)]
    pub workspace_suspend: WorkspaceSuspendValues,
}

/// Per-controller suspend flags. Each maps to a ControllerKind variant.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, Default)]
#[serde(rename_all = "camelCase")]
pub struct ControllerSuspendValues {
    #[serde(default)] pub template: bool,
    #[serde(default)] pub namespace: bool,
    #[serde(default)] pub workspace_catalog: bool,
    #[serde(default)] pub architecture_gem: bool,
    #[serde(default)] pub compliance_binding: bool,
    #[serde(default)] pub compliance_schedule: bool,
    #[serde(default)] pub image_pipeline: bool,
    #[serde(default)] pub flow: bool,
    #[serde(default)] pub dashboard: bool,
    #[serde(default)] pub ami_test: bool,
    #[serde(default)] pub packer_build: bool,
    #[serde(default)] pub synthesizer_format: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, Default)]
#[serde(rename_all = "camelCase")]
pub struct WorkspaceSuspendValues {
    /// Per-template overrides keyed `"<namespace>/<name>"`.
    #[serde(default)]
    pub templates: BTreeMap<String, WorkspaceSuspendEntry>,

    /// Per-catalog overrides keyed by WorkspaceCatalog name.
    #[serde(default)]
    pub catalogs: BTreeMap<String, WorkspaceSuspendEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, Default)]
#[serde(rename_all = "camelCase")]
pub struct WorkspaceSuspendEntry {
    /// Tri-state pause directive.
    #[serde(default)]
    pub state: WorkspaceState,

    /// Optional human-readable reason.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

/// Tri-state per-workspace pause directive (mirror of
/// crd::WorkspaceState).
#[derive(Debug, Clone, Copy, Serialize, Deserialize, JsonSchema, PartialEq, Eq, Default)]
#[serde(rename_all = "lowercase")]
pub enum WorkspaceState {
    #[default]
    Inherit,
    Paused,
    Active,
}

/// Generate the chart's values.schema.json. Uses schemars for the
/// type+structure and layers the `useEmbeddedRuby → gemAuth` conditional
/// on top (schemars 0.8 doesn't express JSON Schema's allOf-if-then).
pub fn generate_values_schema_json() -> String {
    let schema = schemars::schema_for!(ChartValues);
    let mut value: serde_json::Value =
        serde_json::to_value(&schema).expect("schemars schema serializes");

    // Layer the conditional rule. Helm's underlying JSON Schema
    // validator supports allOf+if/then; schemars doesn't emit it
    // natively, so we splice the rule in here.
    let conditional = serde_json::json!({
        "if": {
            "properties": { "useEmbeddedRuby": { "const": true } },
            "required": ["useEmbeddedRuby"]
        },
        "then": {
            "required": ["gemAuth"],
            "properties": {
                "gemAuth": {
                    "required": ["tokenSecret"],
                    "properties": {
                        "tokenSecret": {
                            "required": ["name"],
                            "properties": {
                                "name": {
                                    "type": "string",
                                    "minLength": 1
                                }
                            }
                        }
                    }
                }
            }
        }
    });
    if let Some(obj) = value.as_object_mut() {
        let all_of = obj.entry("allOf").or_insert_with(|| serde_json::json!([]));
        if let Some(arr) = all_of.as_array_mut() {
            arr.push(conditional);
        }
    }

    serde_json::to_string_pretty(&value).expect("schema serializes to JSON")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schema_renders_without_panic() {
        let s = generate_values_schema_json();
        assert!(s.contains("\"$schema\""));
        assert!(s.contains("\"ChartValues\"") || s.contains("\"title\""));
    }

    #[test]
    fn schema_includes_workspace_state_enum() {
        let s = generate_values_schema_json();
        // The WorkspaceState enum must surface as a 3-value oneOf/enum
        // so consumer typos like state=paused-urgently fail at lint.
        assert!(s.contains("\"inherit\""));
        assert!(s.contains("\"paused\""));
        assert!(s.contains("\"active\""));
    }

    #[test]
    fn schema_carries_use_embedded_ruby_conditional() {
        let s = generate_values_schema_json();
        assert!(s.contains("\"allOf\""));
        assert!(s.contains("\"useEmbeddedRuby\""));
        assert!(s.contains("\"minLength\""));
    }

    #[test]
    fn workspace_state_default_is_inherit() {
        assert_eq!(WorkspaceState::default(), WorkspaceState::Inherit);
    }

    #[test]
    fn round_trip_workspace_suspend_entry() {
        let e = WorkspaceSuspendEntry {
            state: WorkspaceState::Paused,
            reason: Some("debug".into()),
        };
        let j = serde_json::to_string(&e).unwrap();
        let back: WorkspaceSuspendEntry = serde_json::from_str(&j).unwrap();
        assert_eq!(e.reason, back.reason);
        assert_eq!(e.state, back.state);
    }
}
