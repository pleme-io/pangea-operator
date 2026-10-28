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

    #[serde(default)]
    #[schemars(
        description = "Compiler backend: lava (default when unset and useEmbeddedRuby is false), embedded, process or http. Empty derives it from useEmbeddedRuby/rubyIsolation."
    )]
    pub compiler_backend: CompilerBackendChoice,

    #[serde(default)]
    #[schemars(
        description = "Ruby templates compiled by the lava backend's contained interpreter (PANGEA_LAVA_RUBY). On by default; enabled: false refuses them with a typed error."
    )]
    pub lava_ruby: LavaRubyValues,

    #[serde(default)]
    #[schemars(
        description = "Leaf work queue, leaf workers and their autoscaling (pangea-operator docs/LEAVES.md §17). queue: in_process renders no worker, no HPA and no stream."
    )]
    pub leaf: LeafValues,

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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct LavaRubyValues {
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default)]
    #[schemars(
        description = "PANGEA_LAVA_RUBY_LOAD_PATH: colon-separated gem lib dirs. Empty uses every <gem>/lib under /usr/share/lava/ruby, the closure the operator image bakes in."
    )]
    pub load_path: String,
}

impl Default for LavaRubyValues {
    fn default() -> Self {
        Self {
            enabled: true,
            load_path: String::new(),
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct LeafValues {
    #[serde(default)]
    pub queue: LeafQueue,
    #[serde(default)]
    pub nats: LeafNats,
    #[serde(default)]
    #[schemars(
        description = "One durable JetStream consumer per credential, subject pangea.leaf.<name>.>, MaxAckPending = cap."
    )]
    pub credentials: Vec<LeafCredential>,
    #[serde(default)]
    pub jetstream: LeafJetStream,
    #[serde(default)]
    pub workers: LeafWorkers,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub enum LeafQueue {
    #[default]
    #[serde(rename = "in_process")]
    InProcess,
    #[serde(rename = "jetstream")]
    Jetstream,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct LeafNats {
    #[serde(default)]
    #[schemars(
        description = "nats:// URL of the JetStream broker. Empty with queue: jetstream refuses that entry alone: no worker, HPA or stream renders and the leader stays in process."
    )]
    pub url: String,
    #[serde(default)]
    pub auth: LeafNatsAuth,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct LeafNatsAuth {
    #[serde(default)]
    #[schemars(
        description = "Secret holding the broker user and password. Empty connects without auth."
    )]
    pub secret_name: String,
    #[serde(default = "default_nats_user_key")]
    pub user_key: String,
    #[serde(default = "default_nats_password_key")]
    pub password_key: String,
}

impl Default for LeafNatsAuth {
    fn default() -> Self {
        Self {
            secret_name: String::new(),
            user_key: default_nats_user_key(),
            password_key: default_nats_password_key(),
        }
    }
}

fn default_nats_user_key() -> String {
    "user".to_string()
}

fn default_nats_password_key() -> String {
    "password".to_string()
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct LeafCredential {
    #[schemars(regex(pattern = r"^[a-z0-9]([a-z0-9-]*[a-z0-9])?$"))]
    pub name: String,
    #[schemars(range(min = 1))]
    pub cap: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct LeafJetStream {
    #[serde(default = "default_true")]
    #[schemars(
        description = "Declare the stream and consumers with the chart's init Job (an OCI nats-box image). false where the node runs no OCI image (engenho native)."
    )]
    pub declare: bool,
    #[serde(default = "default_leaf_stream")]
    pub stream: String,
    #[serde(default = "default_duplicate_window")]
    pub duplicate_window: String,
    #[serde(default = "default_ack_wait")]
    pub ack_wait: String,
    #[serde(default = "default_max_deliver")]
    #[schemars(range(min = 1))]
    pub max_deliver: u32,
}

impl Default for LeafJetStream {
    fn default() -> Self {
        Self {
            declare: true,
            stream: default_leaf_stream(),
            duplicate_window: default_duplicate_window(),
            ack_wait: default_ack_wait(),
            max_deliver: default_max_deliver(),
        }
    }
}

fn default_leaf_stream() -> String {
    "PANGEA_LEAF".to_string()
}

fn default_duplicate_window() -> String {
    "10m".to_string()
}

fn default_ack_wait() -> String {
    "60s".to_string()
}

fn default_max_deliver() -> u32 {
    5
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct LeafWorkers {
    #[serde(default)]
    #[schemars(description = "Fixed replicas when autoscaling.enabled is false.")]
    pub replicas: u32,
    #[serde(default = "default_worker_memory")]
    #[schemars(
        regex(pattern = r"^(derived|[1-9][0-9]*(Mi|Gi))$"),
        description = "derived: request = baseMi + concurrency * leafPeakMi + providers * providerMi, limit = 1.5 * request. A quantity sets both."
    )]
    pub memory: String,
    #[serde(default)]
    pub derivation: MemoryDerivation,
    #[serde(default)]
    pub autoscaling: LeafAutoscaling,
}

impl Default for LeafWorkers {
    fn default() -> Self {
        Self {
            replicas: 0,
            memory: default_worker_memory(),
            derivation: MemoryDerivation::default(),
            autoscaling: LeafAutoscaling::default(),
        }
    }
}

fn default_worker_memory() -> String {
    "derived".to_string()
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct MemoryDerivation {
    #[serde(default = "default_base_mi")]
    pub base_mi: u32,
    #[serde(default = "default_leaf_peak_mi")]
    #[schemars(
        description = "Largest status.leaf.peakRssBytes in MiB; the fat-plan peak (55.6 MiB) until calibration."
    )]
    pub leaf_peak_mi: u32,
    #[serde(default = "default_concurrency")]
    #[schemars(range(min = 1))]
    pub concurrency: u32,
    #[serde(default = "default_providers")]
    pub providers: u32,
    #[serde(default = "default_provider_mi")]
    pub provider_mi: u32,
}

impl Default for MemoryDerivation {
    fn default() -> Self {
        Self {
            base_mi: default_base_mi(),
            leaf_peak_mi: default_leaf_peak_mi(),
            concurrency: default_concurrency(),
            providers: default_providers(),
            provider_mi: default_provider_mi(),
        }
    }
}

fn default_base_mi() -> u32 {
    24
}

fn default_leaf_peak_mi() -> u32 {
    56
}

fn default_concurrency() -> u32 {
    4
}

fn default_providers() -> u32 {
    1
}

fn default_provider_mi() -> u32 {
    33
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct LeafAutoscaling {
    #[serde(default = "default_true")]
    #[schemars(description = "false renders no HPA and fixed workers.replicas.")]
    pub enabled: bool,
    #[serde(default)]
    pub min: u32,
    #[serde(default = "default_autoscaling_max")]
    #[schemars(range(min = 1))]
    pub max: u32,
    #[serde(default = "default_autoscaling_target")]
    #[schemars(
        range(min = 1),
        description = "T: weighted pending leaf work per replica (pangea_leaf_pending_work)."
    )]
    pub target: u32,
    #[serde(default = "default_horizon_secs")]
    #[schemars(
        range(min = 1),
        description = "H: each credential's pending work is capped at its rate times H."
    )]
    pub horizon_secs: u32,
    #[serde(default = "default_horizon_secs")]
    pub scale_down_stabilization_secs: u32,
}

impl Default for LeafAutoscaling {
    fn default() -> Self {
        Self {
            enabled: true,
            min: 0,
            max: default_autoscaling_max(),
            target: default_autoscaling_target(),
            horizon_secs: default_horizon_secs(),
            scale_down_stabilization_secs: default_horizon_secs(),
        }
    }
}

fn default_autoscaling_max() -> u32 {
    4
}

fn default_autoscaling_target() -> u32 {
    600
}

fn default_horizon_secs() -> u32 {
    300
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema, Default)]
pub enum CompilerBackendChoice {
    #[default]
    #[serde(rename = "")]
    Derived,
    #[serde(rename = "lava")]
    Lava,
    #[serde(rename = "embedded")]
    Embedded,
    #[serde(rename = "process")]
    Process,
    #[serde(rename = "http")]
    Http,
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
    #[serde(default)]
    pub template: bool,
    #[serde(default)]
    pub namespace: bool,
    #[serde(default)]
    pub workspace_catalog: bool,
    #[serde(default)]
    pub architecture_gem: bool,
    #[serde(default)]
    pub compliance_binding: bool,
    #[serde(default)]
    pub compliance_schedule: bool,
    #[serde(default)]
    pub image_pipeline: bool,
    #[serde(default)]
    pub flow: bool,
    #[serde(default)]
    pub dashboard: bool,
    #[serde(default)]
    pub ami_test: bool,
    #[serde(default)]
    pub packer_build: bool,
    #[serde(default)]
    pub synthesizer_format: bool,
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
    let mut value = helm_forge::values_schema::schema_value::<ChartValues>();

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

    helm_forge::values_schema::to_json_string(&value)
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
    fn schema_types_the_compiler_backend_and_lava_ruby() {
        let s = generate_values_schema_json();
        assert!(s.contains("\"compilerBackend\""));
        assert!(s.contains("\"lavaRuby\""));
        for v in ["\"lava\"", "\"embedded\"", "\"process\"", "\"http\""] {
            assert!(s.contains(v), "{v}");
        }
        let v: ChartValues = serde_json::from_str("{}").unwrap();
        assert_eq!(v.compiler_backend, CompilerBackendChoice::Derived);
        assert!(v.lava_ruby.enabled);
        assert!(v.lava_ruby.load_path.is_empty());
        assert!(s.contains("\"loadPath\""));
        assert!(serde_json::from_str::<ChartValues>(r#"{"lavaRuby":{"loadPath":7}}"#).is_err());
        assert!(serde_json::from_str::<ChartValues>(r#"{"compilerBackend":"cobol"}"#).is_err());
    }

    #[test]
    fn leaf_values_default_to_the_in_process_queue_and_the_section_17_scaling() {
        let v: ChartValues = serde_json::from_str("{}").unwrap();
        assert_eq!(v.leaf.queue, LeafQueue::InProcess);
        assert!(v.leaf.nats.url.is_empty());
        assert!(v.leaf.credentials.is_empty());
        assert!(v.leaf.jetstream.declare);
        assert_eq!(v.leaf.jetstream.stream, "PANGEA_LEAF");
        assert_eq!(v.leaf.jetstream.duplicate_window, "10m");
        assert_eq!(v.leaf.jetstream.ack_wait, "60s");
        assert_eq!(v.leaf.jetstream.max_deliver, 5);
        let w = &v.leaf.workers;
        assert_eq!(w.replicas, 0);
        assert_eq!(w.memory, "derived");
        assert_eq!(
            (
                w.derivation.base_mi,
                w.derivation.leaf_peak_mi,
                w.derivation.concurrency,
                w.derivation.providers,
                w.derivation.provider_mi
            ),
            (24, 56, 4, 1, 33)
        );
        let a = &w.autoscaling;
        assert!(a.enabled);
        assert_eq!(
            (
                a.min,
                a.max,
                a.target,
                a.horizon_secs,
                a.scale_down_stabilization_secs
            ),
            (0, 4, 600, 300, 300)
        );
    }

    #[test]
    fn leaf_values_refuse_an_unknown_queue_and_a_negative_bound() {
        assert!(serde_json::from_str::<ChartValues>(r#"{"leaf":{"queue":"kafka"}}"#).is_err());
        assert!(serde_json::from_str::<ChartValues>(
            r#"{"leaf":{"workers":{"autoscaling":{"max":-1}}}}"#
        )
        .is_err());
        assert!(
            serde_json::from_str::<ChartValues>(r#"{"leaf":{"credentials":[{"name":"gh"}]}}"#)
                .is_err()
        );
        let v: ChartValues = serde_json::from_str(
            r#"{"leaf":{"queue":"jetstream","nats":{"url":"nats://n:4222"},"credentials":[{"name":"github-pleme-io","cap":4}]}}"#,
        )
        .unwrap();
        assert_eq!(v.leaf.queue, LeafQueue::Jetstream);
        assert_eq!(v.leaf.credentials[0].cap, 4);
    }

    #[test]
    fn schema_types_the_leaf_queue_workers_and_autoscaling() {
        let s = generate_values_schema_json();
        for k in [
            "\"leaf\"",
            "\"in_process\"",
            "\"jetstream\"",
            "\"autoscaling\"",
            "\"horizonSecs\"",
            "\"scaleDownStabilizationSecs\"",
            "\"duplicateWindow\"",
            "\"maxDeliver\"",
            "\"leafPeakMi\"",
        ] {
            assert!(s.contains(k), "{k}");
        }
        assert!(s.contains(r#"^(derived|[1-9][0-9]*(Mi|Gi))$"#));
        assert!(s.contains(r#"^[a-z0-9]([a-z0-9-]*[a-z0-9])?$"#));
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
