//! ArchitectureGem CRD — declares an architecture gem (Ruby Pangea
//! library like `pangea-architectures`, `pangea-aws`, `pangea-akeyless`,
//! `pangea-cloudflare`) that the operator must load + smoke-test at
//! startup before any InfrastructureTemplate that references one of
//! its classes can advance past `Verified`.
//!
//! See `pleme-io/theory/PANGEA-WORKSPACE-RECONCILIATION.md` for the
//! full design. This is the M1 deliverable: typed registry + smoke
//! gate that turns a `LoadError: cannot load such file --
//! pangea/architectures/cloudflare_tunnel` from a silent retry-loop
//! into a fail-fast typed condition on the ArchitectureGem CR.
//!
//! Cluster-scoped because gems are fleet-wide concerns (an operator
//! either has the gem loaded or it doesn't; no per-namespace shape).

use chrono::{DateTime, Utc};
use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use strum::{Display, EnumString};

use super::infrastructure_template::GitRepositoryRef;

/// ArchitectureGem declares a Pangea architecture gem to be loaded +
/// smoke-tested by the operator's compiler sidecar.
#[derive(CustomResource, Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[kube(
    group = "pangea.pleme.io",
    version = "v1alpha1",
    kind = "ArchitectureGem",
    status = "ArchitectureGemStatus",
    shortname = "archgem",
    shortname = "agem",
    printcolumn = r#"{"name":"Phase","type":"string","jsonPath":".status.phase"}"#,
    printcolumn = r#"{"name":"Loaded","type":"integer","jsonPath":".status.loadedClassCount"}"#,
    printcolumn = r#"{"name":"Smoke","type":"string","jsonPath":".status.smokeStatus"}"#,
    printcolumn = r#"{"name":"Age","type":"date","jsonPath":".metadata.creationTimestamp"}"#
)]
#[serde(rename_all = "camelCase")]
pub struct ArchitectureGemSpec {
    /// Ruby gem name as it appears in Bundler / require statements.
    /// Example: `pangea-architectures`.
    pub gem_name: String,

    /// Required gem version (semver constraint or exact). Operator
    /// refuses to load if the compiler sidecar reports a different
    /// version.
    pub version: String,

    /// Where the gem source lives. M1 supports gitRepository only;
    /// future: published-gem (RubyGems.org) and OCI-artifact modes.
    pub source: GemSource,

    /// Classes the operator EXPECTS this gem to expose. Operator
    /// refuses to advance past `Verifying` if any expected class is
    /// missing from the compiler's loaded namespace.
    ///
    /// Format: fully-qualified Ruby class names, e.g.
    /// `Pangea::Architectures::CloudflareTunnel`.
    pub expected_classes: Vec<String>,

    /// Smoke-test fixtures. Each fixture exercises one class with a
    /// known-good input; the operator runs them against the compiler
    /// sidecar at reconcile time. ALL fixtures must pass for the gem
    /// to advance to `Loaded`.
    #[serde(default)]
    pub fixtures: Vec<SmokeFixture>,

    /// Gem-level policy defaults — root of the four-level cascade
    /// (gem → workspace → template → resource). Lower levels can
    /// override with safety precedence
    /// (`refuse > requireApproval > autoApply`).
    #[serde(default)]
    pub policy: GemPolicy,

    /// How often to re-verify the gem (e.g. catch sidecar restart
    /// where the gem was unloaded). Defaults to `5m`.
    #[serde(default = "default_refresh_interval")]
    pub refresh_interval: String,

    /// Suspend reconciliation. Operator skips this gem entirely;
    /// existing classes stay in the registry (last-known-good).
    #[serde(default)]
    pub suspend: bool,
}

/// Where to fetch a gem's source from + which evaluator runs it.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct GemSource {
    /// Which evaluator runs this gem. `Ruby` is the default for
    /// backward compatibility — every existing ArchitectureGem CR
    /// continues to validate + reconcile unchanged.
    ///
    /// `Lisp` selects the terreno evaluator (theory/TERRENO.md M2);
    /// `Wasm` selects a typed wasmtime component. Both are
    /// reserved at the schema layer today; the operator's
    /// prepare_gem returns a typed `NotImplemented` condition until
    /// terreno-eval lands.
    #[serde(default)]
    pub kind: SourceKind,

    /// Git repository — clone the gem at the given ref. Operator
    /// (embedded mode) clones into the gem cache + prepends to
    /// $LOAD_PATH (Ruby) or registers with the lisp/wasm evaluator
    /// per `kind`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub git_repository: Option<GitRepositoryRef>,
    // TODO M7: oci_artifact (gem published as OCI), rubygems (registry).
}

/// Which evaluator interprets the gem's source files.
///
/// See `theory/TERRENO.md` for the design intent: both Pangea Ruby
/// (Track A) and terreno (Track B) register `ArchitectureGem` CRs;
/// the operator dispatches to the correct backend via this field.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub enum SourceKind {
    /// Pangea Ruby DSL (Track A). The default — every pre-M2
    /// ArchitectureGem CR has this implicitly.
    #[default]
    Ruby,
    /// terreno tatara-lisp (Track B). Reserved at the schema layer;
    /// terreno-eval crate (M2) wires the actual dispatch.
    Lisp,
    /// Typed WIT-shaped wasm component. Reserved for M2+ — wasmtime
    /// integration via the embedded operator.
    Wasm,
}

/// Smoke-test fixture for one architecture class. The compiler runs
/// the architecture's `.build(...)` method against the inputs and
/// confirms the result compiles to valid Terraform JSON.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SmokeFixture {
    /// Fully-qualified class name being exercised.
    /// E.g. `Pangea::Architectures::CloudflareTunnel`.
    pub class_name: String,

    /// Path to the fixture input file, relative to the gem's
    /// repository root. Format is YAML; the YAML is deserialized as
    /// the architecture's `.build(synth, args)` second argument.
    /// Example: `spec/fixtures/cloudflare_tunnel.yaml`.
    pub fixture_path: String,

    /// Optional human-readable description shown in error messages
    /// when the fixture fails.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

/// Policy fields — the typed shape used at all four cascade levels
/// (gem, workspace, template, resource). Innermost wins with safety
/// precedence; see `theory/PANGEA-WORKSPACE-RECONCILIATION.md`.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct GemPolicy {
    /// Default drift reaction for resources from this gem.
    /// `autoApply` (silent), `requireApproval` (stall),
    /// `refuse` (block all changes), `alert` (notify but don't apply).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub drift_reaction: Option<DriftReaction>,

    /// Default destroy protection for resources from this gem.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub destroy_protection: Option<bool>,

    /// Settling policy — what to do if the gem keeps drifting.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub settling_policy: Option<SettlingPolicy>,

    /// Approval routing for `requireApproval` reactions.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub approval_routing: Option<ApprovalRouting>,

    /// Reactive policy — declarative responses to "things didn't go
    /// to a good state". Cascade root for downstream workspaces +
    /// templates. See `ReactivePolicy` for the shape.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reactive: Option<ReactivePolicy>,
}

/// What the operator does when it detects drift on a managed resource.
#[derive(Debug, Clone, Display, EnumString, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
#[strum(serialize_all = "camelCase")]
pub enum DriftReaction {
    /// Apply the corrective change automatically.
    AutoApply,
    /// Stall in `Drifted` until a human approves via the configured
    /// routing channel.
    RequireApproval,
    /// Block all changes; operator marks the template `Failed` until
    /// a higher authority overrides via `breakGlass`.
    Refuse,
    /// Notify via approval routing but do not apply or stall.
    Alert,
}

/// Settling policy — backstop for resources that keep drifting.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SettlingPolicy {
    /// After this many consecutive drift cycles, take
    /// `onExhaustion` action.
    #[serde(default = "default_max_drift_cycles")]
    pub max_consecutive_drift_cycles: u32,

    /// What to do when `maxConsecutiveDriftCycles` is hit.
    #[serde(default = "default_on_exhaustion")]
    pub on_exhaustion: SettlingExhaustionAction,
}

/// Action when settling policy is exhausted.
#[derive(Debug, Clone, Display, EnumString, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
#[strum(serialize_all = "camelCase")]
pub enum SettlingExhaustionAction {
    /// Mark template `Failed`; emit Warning event; surface as
    /// `Settled=False` condition.
    Fail,
    /// Mark template `Drifted` indefinitely; keep alerting.
    Alert,
    /// Continue retrying with increasing backoff (eventually-consistent
    /// shape).
    Retry,
}

/// Reactive policy — declarative responses to "things didn't go to a
/// good state". Three escalation paths:
///
/// * `failureEscalation` — fires when consecutive Failed reconciles
///   exceed `maxConsecutiveFailures`. Default: 5 → Alert.
/// * `phaseTimeout` — fires when the template stays in a non-terminal
///   phase (Compiling / Planning / Applying) past the per-phase
///   threshold. Defaults: 5m / 10m / 30m → Alert.
/// * `verifiedBlocked` — fires when the `Verified` gate stays False
///   past `timeout` (parent gem not Loaded, fixture failing, etc.).
///   Default: 10m → Alert.
///
/// Each escalation has an `onExhaustion` action: `Alert` (event +
/// `Healthy=False` + routing-formatted log line, reconcile continues),
/// `Suspend` (set `status.autoSuspended=true`, halt reconcile until
/// operator-human resumes), or `Page` (urgent routing notify, no
/// other state change).
///
/// ReactivePolicy lives at every cascade level (gem → workspace →
/// template → resource). Innermost-set wins per field; defaults
/// apply when nothing is set.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ReactivePolicy {
    /// What to do when consecutive Failed reconciles exceed threshold.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failure_escalation: Option<FailureEscalation>,

    /// What to do when stuck in a non-terminal phase too long.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub phase_timeout: Option<PhaseTimeoutPolicy>,

    /// What to do when `Verified=False` persists past timeout.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub verified_blocked: Option<VerifiedBlockedPolicy>,
}

/// Escalate after N consecutive failed reconciles.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct FailureEscalation {
    /// Threshold. Default 5.
    #[serde(default = "default_max_consecutive_failures")]
    pub max_consecutive_failures: u32,

    /// What to do when threshold is reached.
    #[serde(default)]
    pub on_exhaustion: ReactiveAction,

    /// Where to route the notification.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub routing: Option<ApprovalRouting>,
}

/// Escalate when stuck in a non-terminal phase past per-phase
/// threshold.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct PhaseTimeoutPolicy {
    /// How long can `Compiling` last before escalating. Format same
    /// as refreshInterval (`5m`, `30s`, `1h`). Default `5m`.
    #[serde(default = "default_compiling_timeout")]
    pub compiling: String,

    /// `Planning` threshold. Default `10m`.
    #[serde(default = "default_planning_timeout")]
    pub planning: String,

    /// `Applying` threshold. Default `30m`.
    #[serde(default = "default_applying_timeout")]
    pub applying: String,

    /// What to do when the threshold is exceeded.
    #[serde(default)]
    pub on_timeout: ReactiveAction,

    /// Where to route the notification.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub routing: Option<ApprovalRouting>,
}

/// Escalate when the `Verified` condition stays False for too long
/// (gem not loaded, fixture failing, expected class missing).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct VerifiedBlockedPolicy {
    /// How long can `Verified=False` persist. Default `10m`.
    #[serde(default = "default_verified_blocked_timeout")]
    pub timeout: String,

    /// What to do when the threshold is exceeded.
    #[serde(default)]
    pub on_blocked: ReactiveAction,

    /// Where to route the notification.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub routing: Option<ApprovalRouting>,
}

/// What to do when a reactive policy is triggered.
#[derive(Debug, Clone, Copy, Default, Display, EnumString, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
#[strum(serialize_all = "camelCase")]
pub enum ReactiveAction {
    /// Emit a Warning event + flip `Healthy=False` condition + log a
    /// routing-formatted line. Reconcile loop continues unchanged.
    /// **Default.** The lightest-touch escalation — observers see
    /// the unhealthy signal, but the operator keeps trying.
    #[default]
    Alert,
    /// Set `status.autoSuspended=true` and halt the reconcile loop
    /// for this template. Operator-human must explicitly clear by
    /// patching status.autoSuspended=false (or deleting + recreating
    /// the CR). Use when the template is hot-spinning in a bad state
    /// and you want it stopped — like a circuit breaker.
    Suspend,
    /// Highest-urgency routing notify (e.g. Slack ping with @here,
    /// ntfy with priority=high, GitHub issue with `urgent` label).
    /// No operator-side state change beyond `Healthy=False`. Use
    /// when humans must respond but the operator should keep trying.
    Page,
}

fn default_max_consecutive_failures() -> u32 {
    5
}

fn default_compiling_timeout() -> String {
    "5m".to_string()
}

fn default_planning_timeout() -> String {
    "10m".to_string()
}

fn default_applying_timeout() -> String {
    "30m".to_string()
}

fn default_verified_blocked_timeout() -> String {
    "10m".to_string()
}

/// Where approval requests get routed when a resource enters
/// `requireApproval` state.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ApprovalRouting {
    /// ntfy topic name (homelab convention — see pleme-io
    /// alertmanager-ntfy module).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ntfy_topic: Option<String>,

    /// Slack channel (SaaS convention — Datadog/Splunk-paired
    /// clusters).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub slack_channel: Option<String>,

    /// GitHub issue template name. Operator opens an issue in the
    /// workspace's source repo.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub github_issue_template: Option<String>,
}

/// Status reflecting the loader's view of this gem.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ArchitectureGemStatus {
    /// Lifecycle phase. See `Phase` enum.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub phase: Option<Phase>,

    /// Smoke test outcome.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub smoke_status: Option<SmokeStatus>,

    /// Classes actually present in the compiler's loaded namespace
    /// for this gem. Source of truth for admission webhook + template
    /// `Verified` check.
    #[serde(default)]
    pub loaded_classes: Vec<String>,

    /// `loadedClasses.len()` — exposed as a printer column for `kubectl
    /// get archgem` ergonomics.
    #[serde(default)]
    pub loaded_class_count: u32,

    /// Classes from `spec.expectedClasses` that are MISSING from the
    /// compiler's namespace. Empty when the gem is fully loaded.
    #[serde(default)]
    pub missing_classes: Vec<String>,

    /// Per-fixture smoke-test results.
    #[serde(default)]
    pub fixture_results: Vec<FixtureResult>,

    /// Last reconcile timestamp.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_reconcile_time: Option<DateTime<Utc>>,

    /// Compiler-reported gem version (compared against `spec.version`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed_version: Option<String>,

    /// Standard kube conditions: `Loaded`, `SmokeTested`, `Ready`.
    #[serde(default)]
    pub conditions: Vec<Condition>,
}

/// Lifecycle phase for an ArchitectureGem.
#[derive(Debug, Clone, Display, EnumString, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "PascalCase")]
#[strum(serialize_all = "PascalCase")]
pub enum Phase {
    /// Awaiting first reconcile.
    Pending,
    /// Operator is querying the compiler sidecar to discover which
    /// classes are actually loaded.
    Loading,
    /// Operator is running smoke-test fixtures.
    SmokeTesting,
    /// All expected classes loaded + every fixture passed. Templates
    /// referencing this gem may now advance past `Verified`.
    Loaded,
    /// One or more expected classes missing OR a fixture failed.
    /// Status fields name the specific failures.
    Failed,
    /// Reconciliation suspended via `spec.suspend`.
    Suspended,
}

/// Smoke test outcome rollup.
#[derive(Debug, Clone, Display, EnumString, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "PascalCase")]
#[strum(serialize_all = "PascalCase")]
pub enum SmokeStatus {
    /// At least one fixture not yet run.
    NotRun,
    /// All fixtures passed.
    Passed,
    /// At least one fixture failed.
    Failed,
}

/// Per-fixture smoke-test result.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct FixtureResult {
    /// Class the fixture exercised.
    pub class_name: String,

    /// Whether the fixture passed.
    pub passed: bool,

    /// When the fixture last ran.
    pub last_run: DateTime<Utc>,

    /// Error message if the fixture failed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,

    /// Hash of the fixture input — operator detects fixture drift
    /// between releases and re-runs accordingly.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input_hash: Option<String>,
}

/// Standard kube condition shape.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Condition {
    /// Condition type — `Loaded`, `SmokeTested`, `Ready`.
    #[serde(rename = "type")]
    pub condition_type: String,
    /// `True`, `False`, `Unknown`.
    pub status: String,
    /// Machine-readable reason.
    pub reason: String,
    /// Human-readable message.
    pub message: String,
    /// When the condition last transitioned.
    pub last_transition_time: DateTime<Utc>,
}

fn default_refresh_interval() -> String {
    "5m".to_string()
}

fn default_max_drift_cycles() -> u32 {
    4
}

fn default_on_exhaustion() -> SettlingExhaustionAction {
    SettlingExhaustionAction::Fail
}

#[cfg(test)]
mod tests {
    use super::*;
    use kube::CustomResourceExt;

    #[test]
    fn architecture_gem_crd_generates() {
        let crd = ArchitectureGem::crd();
        let yaml = serde_yaml::to_string(&crd).expect("crd serializes");
        assert!(yaml.contains("ArchitectureGem"));
        assert!(yaml.contains("pangea.pleme.io"));
        assert!(yaml.contains("v1alpha1"));
    }

    #[test]
    fn drift_reaction_safety_precedence_orderable() {
        // refuse > requireApproval > autoApply > alert
        // We don't enforce ordering as a Rust trait — that's resolver
        // responsibility — but ensure the variants exist for the
        // resolver to match on.
        assert_ne!(DriftReaction::AutoApply, DriftReaction::Refuse);
        assert_ne!(DriftReaction::RequireApproval, DriftReaction::Alert);
    }

    #[test]
    fn smoke_status_default_not_run() {
        let s = SmokeStatus::NotRun;
        assert_eq!(s.to_string(), "NotRun");
    }

    #[test]
    fn phase_display_round_trip() {
        for phase in [
            Phase::Pending,
            Phase::Loading,
            Phase::SmokeTesting,
            Phase::Loaded,
            Phase::Failed,
            Phase::Suspended,
        ] {
            let s = phase.to_string();
            let parsed: Phase = s.parse().expect("phase round-trips");
            assert_eq!(parsed, phase);
        }
    }

    #[test]
    fn gem_policy_default_all_none() {
        let p = GemPolicy::default();
        assert!(p.drift_reaction.is_none());
        assert!(p.destroy_protection.is_none());
        assert!(p.settling_policy.is_none());
        assert!(p.approval_routing.is_none());
    }

    #[test]
    fn cloudflare_tunnel_fixture_shape() {
        let fixture = SmokeFixture {
            class_name: "Pangea::Architectures::CloudflareTunnel".to_string(),
            fixture_path: "spec/fixtures/cloudflare_tunnel.yaml".to_string(),
            description: Some("rio-style tunnel with ingress + DNS".to_string()),
        };
        let json = serde_json::to_string(&fixture).expect("serializes");
        assert!(json.contains("CloudflareTunnel"));
        assert!(json.contains("cloudflare_tunnel.yaml"));
    }
}

// Used by the smoke-test fixture used in M3 integration tests. Kept
// alongside the CRD so future fixture-format changes are colocated.
pub mod fixture_format {
    use serde::{Deserialize, Serialize};
    use std::collections::BTreeMap;

    /// Fixture YAML schema. Each fixture is a map of typed inputs
    /// the architecture's `.build` method consumes.
    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct Fixture {
        pub class_name: String,
        pub inputs: BTreeMap<String, serde_json::Value>,
        /// Expected substring(s) in the rendered Terraform JSON
        /// output. Operator confirms each substring is present.
        #[serde(default)]
        pub expect_resource_types: Vec<String>,
    }
}
