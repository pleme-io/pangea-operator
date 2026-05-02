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
    printcolumn = r#"{"name":"Cycle","type":"integer","jsonPath":".status.cycleCount"}"#,
    printcolumn = r#"{"name":"Matched","type":"integer","jsonPath":".status.lastCycle.summary.matched"}"#,
    printcolumn = r#"{"name":"Updated","type":"integer","jsonPath":".status.lastCycle.summary.updated"}"#,
    printcolumn = r#"{"name":"Drifted","type":"integer","jsonPath":".status.lastCycle.summary.driftedUncorrected"}"#,
    printcolumn = r#"{"name":"Healthy","type":"string","jsonPath":".status.conditions[?(@.type=='Healthy')].status"}"#,
    printcolumn = r#"{"name":"Suspended","type":"boolean","jsonPath":".status.autoSuspended"}"#,
    printcolumn = r#"{"name":"Protected","type":"boolean","jsonPath":".spec.destroyProtection"}"#,
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

    /// Prevent destruction of the managed infrastructure.
    ///
    /// When enabled, the operator will refuse to run `tofu destroy` even if
    /// the CR is deleted. Plan and apply continue to work normally for drift
    /// correction. This is critical for self-managed bootstrap infrastructure
    /// — the cluster, database, and network that the operator itself runs on.
    ///
    /// To actually destroy protected infrastructure, first set this to false,
    /// then delete the CR.
    #[serde(default)]
    pub destroy_protection: bool,

    /// Cross-template variable references. Resolved before compilation by
    /// fetching the referenced template's outputs.
    ///
    /// Example:
    /// ```yaml
    /// variableRefs:
    ///   vpc_id:
    ///     templateRef: { name: vpc-template }
    ///     outputKey: vpc_id
    /// ```
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub variable_refs: Option<BTreeMap<String, VariableRef>>,

    /// Retry policy for failed operations.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry_policy: Option<RetryPolicy>,

    /// Provider credentials configuration.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_credentials: Option<ProviderCredentials>,

    /// InSpec compliance profiles to run after apply.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub compliance_profiles: Vec<String>,

    /// Per-resource policy rules controlling what the operator may do
    /// without human approval. Evaluated top-to-bottom against each
    /// resource change in a plan; the FIRST matching rule's `decision`
    /// applies. Changes that match no rule fall back to
    /// `defaultDecision` (or to `autoApprove` if `defaultDecision` is
    /// unset).
    ///
    /// Aggregation across all changes:
    ///   - any `refuse`          → operator marks plan Failed, won't apply
    ///   - else any `requireApproval` → operator waits for `approvedPlanHash`
    ///   - else                  → operator applies immediately
    ///
    /// Empty list = behave exactly as before (`autoApprove` controls
    /// everything). Use this to express things like "auto-apply
    /// low-risk DNS creates, require approval for any
    /// `cloudflare_dns_record` delete, refuse any `cloudflare_zone`
    /// destroy".
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub policies: Vec<PolicyRule>,

    /// Decision applied to changes that match no rule in `policies`.
    /// If unset, defaults to `autoApply` — the operator aggressively
    /// settles drift on every change at every risk level. Set this to
    /// `refuse` to make the policy list strictly opt-in (only changes
    /// explicitly allowed by a rule may be applied), or to
    /// `requireApproval` to gate everything not explicitly auto-applied.
    ///
    /// `spec.autoApprove` is no longer consulted by this engine; it
    /// remains in the schema for legacy compatibility but does not
    /// override `defaultDecision`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_decision: Option<PolicyDecision>,

    /// Bounds on how long the operator may keep cycling through
    /// drift→apply→drift loops before declaring the template stuck.
    /// State settling is the operator's primary success metric — when
    /// it can't reach a settled state after the configured number of
    /// cycles, this is escalated loudly via a `Settled=False`
    /// condition + Warning event.
    ///
    /// Defaults: 5 cycles, then `fail` (transition to Failed, surface
    /// the address list of resources that keep re-drifting).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub settling_policy: Option<SettlingPolicy>,

    /// Reactive policy — declarative responses to "things didn't go
    /// to a good state". Innermost level of the cascade
    /// (gem → workspace → template). When unset at every cascade
    /// level, the operator applies sensible defaults: 5 consecutive
    /// failures → Alert, phase timeouts of 5m / 10m / 30m for
    /// Compiling / Planning / Applying → Alert, Verified=False for
    /// 10m → Alert. See the `ReactivePolicy` type in the shared
    /// `architecture_gem` module.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reactive_policy: Option<crate::crd::architecture_gem::ReactivePolicy>,

    /// Automatic-import policy — when set with `autoOnConflict:
    /// true`, the operator runs `tofu import` for EVERY plan
    /// `create`-action whose resource-type has a natural-ID rule
    /// (declared in `naturalIds`, fallback to operator-bundled
    /// defaults), substituting against the planned attribute values
    /// from `tofu show -json plan`. Resources that already exist in
    /// the cloud provider get adopted into state instead of failing
    /// the apply with "already exists".
    ///
    /// This is the typed answer to "fully consume existing
    /// infrastructure into pangea-operator." `importHints`
    /// (per-address) takes precedence; `importPolicy.naturalIds`
    /// (per-resource-type) is the fallback; operator-bundled
    /// defaults (in `controller/import.rs::bundled_natural_ids`)
    /// fill the rest. Three layers — author-explicit, then
    /// per-template type-rules, then sensible cluster-wide defaults.
    ///
    /// Default: unset (no auto-import; existing `importHints`
    /// behaviour preserved).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub import_policy: Option<ImportPolicy>,

    /// Hints used by the operator to import out-of-band cloud
    /// resources into tofu state instead of creating duplicates.
    ///
    /// Keys are tofu resource addresses (e.g.
    /// `cloudflare_dns_record.foo`), values are import IDs (the
    /// provider-specific shape, e.g. `<zone_id>/<record_id>` for
    /// cloudflare DNS, role name for `aws_iam_role`, etc.). Values
    /// support `{{ .varName }}` substitution from `spec.variables` —
    /// e.g. `"{{ .cloudflare_zone_id }}/foo"` resolves
    /// `varName` against `spec.variables` (string-coerced) before
    /// passing to `tofu import`.
    ///
    /// Operator behavior: before each `tofu apply`, for every plan
    /// action with `action: create` whose resource address matches a
    /// key here, the operator runs `tofu import <addr>
    /// <substituted-id>`. After import the resource is in state, the
    /// apply becomes a no-op or update, and the cycle receipt
    /// records the resource as `Outcome::Imported` instead of
    /// `Outcome::Created`. This is the typed answer to "auto-import
    /// any state".
    ///
    /// Empty map (default) = no imports — every `create` action goes
    /// straight to the apply and creates a fresh resource as before.
    /// Hint values that fail substitution are skipped with a Warning
    /// event; hint imports that fail (e.g. ID doesn't match) leave
    /// the resource for the apply to handle.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub import_hints: BTreeMap<String, String>,
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

/// Cross-template variable reference. Fetches an output from another template.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct VariableRef {
    /// Reference to the source template.
    pub template_ref: TemplateObjectRef,

    /// Key in the source template's status.outputs to read.
    pub output_key: String,
}

/// Reference to another InfrastructureTemplate.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct TemplateObjectRef {
    /// Name of the InfrastructureTemplate.
    pub name: String,

    /// Namespace (defaults to same namespace as the referencing template).
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

    /// GitHub credentials configuration. Used by templates that
    /// declare github_* providers (e.g. pangea-github's
    /// `cloudflare_zero_trust_*` adjacent shapes — repos, runner
    /// groups, branch protection). Same shape as the cloudflare
    /// credentials block: a secretRef to a Secret containing a PAT.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub github: Option<GitHubCredentials>,
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

/// GitHub credentials configuration. The referenced Secret holds a
/// fine-grained or classic PAT exposed to the tofu provider as
/// `GITHUB_TOKEN` (or whatever envFrom shape the github_* providers
/// expect).
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct GitHubCredentials {
    /// Secret containing the GitHub PAT.
    pub secret_ref: SecretRef,
}

/// Automatic-import policy. Drives the operator's pre-apply
/// `tofu import` sweep for `create`-actions whose target resources
/// already exist in the cloud provider.
///
/// Three resolution layers, innermost wins:
///  1. `spec.importHints` — per-address override (highest priority)
///  2. `naturalIds` (this map) — per-resource-type templates with
///     `{{ .planned.<attr> }}` substitution
///  3. operator-bundled defaults (in `controller/import.rs`) for
///     common providers (`github_repository`, `aws_iam_role`, …)
///
/// Substitution syntax (same as `importHints`): `{{ .planned.foo }}`
/// reads `change.after.foo` from the parsed `tofu show -json plan`
/// for the resource. `{{ var }}` (no `planned.` prefix) reads from
/// `spec.variables` as in `importHints`.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ImportPolicy {
    /// Master switch. When `false` (default), no auto-import fires
    /// regardless of `naturalIds` or bundled defaults — you stay on
    /// today's per-address `importHints`-only behaviour. When
    /// `true`, every `create`-action is a candidate for auto-import.
    #[serde(default)]
    pub auto_on_conflict: bool,

    /// Per-resource-type natural-ID extraction templates. Keys are
    /// terraform resource types (`github_repository`,
    /// `aws_iam_role`, etc.); values are substitution templates
    /// like `{{ .planned.name }}` or
    /// `{{ .planned.zone_id }}/{{ .planned.id }}`.
    ///
    /// Empty map = fall through to operator-bundled defaults.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub natural_ids: BTreeMap<String, String>,
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

    /// Per-resource drift / change detail from the last plan.
    /// Populated whenever a plan reports `has_changes`. Lets external
    /// observers see WHICH resources changed and HOW without parsing
    /// raw tofu output. Capped to 50 entries (full list available via
    /// the operator's GraphQL API for large plans).
    ///
    /// Always serialized (no skip-if-empty) so an explicit empty array
    /// clears the field via JSON Merge Patch — otherwise stale drift
    /// would survive a clean settle.
    #[serde(default)]
    pub drift_details: Vec<DriftDetail>,

    /// Hash of the pending plan awaiting approval.
    /// Set by the operator after planning. Users approve by copying this
    /// value to `approvedPlanHash` via kubectl patch or GraphQL mutation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pending_plan_hash: Option<String>,

    /// Hash of the approved plan. Set by the user to approve a pending plan.
    /// When this matches `pendingPlanHash`, the operator proceeds to apply.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub approved_plan_hash: Option<String>,

    /// Compliance check results.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub compliance: Option<ComplianceStatus>,

    /// Aggregate result of evaluating `spec.policies` against the last
    /// plan's drift details. Drives the plan→apply gate. Absent when
    /// the template uses legacy `autoApprove`-only mode (no `policies`
    /// and no `defaultDecision`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub policy_evaluation: Option<PolicyEvaluation>,

    /// State-settling counter. Counts consecutive drift cycles where
    /// applying a plan does NOT result in a clean drift check. Reset
    /// to zero when a Ready→Ready transition sees no drift. Drives
    /// `SettlingPolicy` escalation.
    #[serde(default)]
    pub consecutive_drift_cycles: u32,

    /// Resource addresses that keep showing up in successive drift
    /// cycles — the "stuck" set. Computed as the intersection of
    /// drift-detail addresses across the last N cycles. Capped at 20
    /// for status hygiene; full set available via the operator's
    /// GraphQL API. Empty when `consecutiveDriftCycles == 0`.
    ///
    /// Always serialized (no skip-if-empty) so explicit clearing on a
    /// settle propagates via JSON Merge Patch.
    #[serde(default)]
    pub stuck_resources: Vec<String>,

    /// Monotonic cycle counter — bumped once per completed
    /// reconcile (every plan→apply pair, or every plan-only when no
    /// changes). Lets observers detect the operator's heartbeat
    /// without polling timestamps.
    #[serde(default)]
    pub cycle_count: u64,

    /// Receipt for the most recent reconcile cycle: per-resource
    /// outcomes (Matched / Updated / Created / Destroyed / Imported /
    /// Drifted / Failed) plus aggregate counts. The shape the user
    /// reads to answer "what did the operator just do, and to what?"
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_cycle: Option<ReconcileCycle>,

    /// When the current `phase` was entered. Reset on every phase
    /// transition. Used by ReactivePolicy.phaseTimeout to detect
    /// templates stuck in a non-terminal phase.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub phase_entered_at: Option<DateTime<Utc>>,

    /// First time the `Verified` condition flipped to False. Used by
    /// ReactivePolicy.verifiedBlocked to detect templates whose gate
    /// has been blocked too long. Cleared when Verified flips back
    /// to True.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub verified_blocked_since: Option<DateTime<Utc>>,

    /// Set to true by a ReactivePolicy `Suspend` action. The operator
    /// halts reconcile for this template until cleared. Cleared
    /// manually via `kubectl patch ... --subresource status -p
    /// '{"status":{"autoSuspended":false}}'` (or by deleting the CR
    /// and recreating it). See `lastEscalationReason` for why the
    /// auto-suspend triggered.
    #[serde(default)]
    pub auto_suspended: bool,

    /// When the operator last emitted an escalation event for this
    /// template. Debounce signal — escalation actions don't re-fire
    /// every reconcile while the bad state persists; they fire once
    /// per entry into the bad state and then stay quiet until the
    /// state clears + re-enters.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_escalated_at: Option<DateTime<Utc>>,

    /// Machine-readable reason for the last escalation. One of
    /// `FailureEscalation`, `PhaseTimeout:<phase>`,
    /// `VerifiedBlocked`. Empty when no escalation has fired.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_escalation_reason: Option<String>,
}

/// Receipt for one reconcile cycle. Surfaces the answer to "what did
/// the operator do this cycle?" as typed data instead of free-text logs.
///
/// Generated at the end of every plan→apply pair (or every plan-only
/// when no changes were planned). The aggregate counts answer "how
/// much converged?"; the `outcomes` list answers "to what?".
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ReconcileCycle {
    /// Monotonic cycle number (mirrors `status.cycleCount` at the
    /// moment this cycle was emitted; redundant but lets observers
    /// snapshot the receipt with its cycle number atomically).
    pub cycle: u64,

    /// When the cycle started (immediately after Verified gate).
    pub started_at: DateTime<Utc>,

    /// When the cycle completed (after apply, or after plan-with-no-changes).
    pub completed_at: DateTime<Utc>,

    /// Source revision the cycle reconciled against — git commit SHA
    /// for git-sourced templates, content hash for inline / configmap.
    /// Mirrors `status.lastAppliedRevision` for the cycle that just
    /// landed; lets observers correlate cycles to source changes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_revision: Option<String>,

    /// Human-readable plan summary, mirroring tofu's `Plan: +X ~Y -Z`
    /// shape. Same string as top-level `status.planSummary` at the
    /// moment this cycle ran.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub plan_summary: Option<String>,

    /// Aggregate outcome counts for the cycle. `matched` = total
    /// managed resources minus everything that planned a change.
    /// Other counts are per-Outcome variant.
    #[serde(default)]
    pub summary: CycleSummary,

    /// Per-resource outcomes for resources the cycle TOUCHED — i.e.
    /// resources that the plan reported a change on (Created /
    /// Updated / Destroyed) or that the apply failed on (Failed) or
    /// that policy gated (Drifted).
    ///
    /// Resources that had no planned change are NOT individually
    /// listed here (they're rolled up into `summary.matched`); listing
    /// every untouched resource per cycle would balloon status
    /// payloads unbounded. Capped at 100 entries.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub outcomes: Vec<ResourceOutcome>,
}

/// Aggregate counts for one cycle. Sum of all variants equals total
/// managed resource count.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct CycleSummary {
    /// Resources whose declared state matched actual state — no plan
    /// action (`no-op`).
    #[serde(default)]
    pub matched: u32,
    /// Resources updated to declared state by this cycle's apply.
    #[serde(default)]
    pub updated: u32,
    /// Resources newly created by this cycle's apply.
    #[serde(default)]
    pub created: u32,
    /// Resources destroyed by this cycle's apply.
    #[serde(default)]
    pub destroyed: u32,
    /// Resources adopted into state by `tofu import` (reserved — set
    /// to 0 until the import path lands in a follow-up).
    #[serde(default)]
    pub imported: u32,
    /// Resources where drift was detected but NOT corrected (policy =
    /// requireApproval / refuse, or apply was skipped).
    #[serde(default)]
    pub drifted_uncorrected: u32,
    /// Resources where the apply errored.
    #[serde(default)]
    pub failed: u32,
}

/// Per-resource cycle outcome. The typed answer to "what happened to
/// THIS resource this cycle?".
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ResourceOutcome {
    /// Terraform resource address (e.g. `cloudflare_dns_record.foo`).
    pub address: String,

    /// What happened.
    pub outcome: Outcome,

    /// Original tofu action category (`create`, `update`, `delete`,
    /// `replace`, `noop`) when known. Lets observers see the raw
    /// terraform shape behind the typed Outcome.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub action: Option<String>,

    /// Optional context — for `Drifted` outcomes, the policy decision
    /// that gated the change; for `Failed`, the tofu error excerpt.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

/// Typed outcome for one resource within one reconcile cycle.
///
/// The vocabulary is the user-facing answer to "did the operator
/// match the declared state, update it, import it, or fail?". Maps
/// from tofu's lower-level action vocabulary (`create`/`update`/
/// `delete`/`replace`/`no-op`) plus the apply outcome plus policy
/// gating.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema, Display, EnumString)]
#[serde(rename_all = "PascalCase")]
#[strum(serialize_all = "PascalCase")]
pub enum Outcome {
    /// Declared state already matched actual state — apply was a no-op
    /// for this resource (terraform `no-op` action).
    Matched,
    /// Apply ran an `update` on this resource (or a replace, which is
    /// net "updated to declared state").
    Updated,
    /// Apply ran a `create` on this resource.
    Created,
    /// Apply ran a `destroy` on this resource.
    Destroyed,
    /// Apply ran a `tofu import` to adopt an out-of-band resource into
    /// state. Reserved variant — emitted by the import path landing
    /// in a follow-up.
    Imported,
    /// Plan reported drift, but the operator did NOT correct it
    /// (policy = requireApproval / refuse, or apply was skipped due
    /// to a settling escalation).
    Drifted,
    /// Apply errored on this specific resource.
    Failed,
}

/// Lifecycle phase of an InfrastructureTemplate.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema, Display, EnumString)]
pub enum Phase {
    /// Initial state, waiting to be processed.
    Pending,
    /// M2 — checking that every ArchitectureGem this template
    /// references has phase `Loaded` (every expected class loaded +
    /// every fixture passed). Cannot advance until verified. See
    /// theory/PANGEA-WORKSPACE-RECONCILIATION.md "Reconciliation
    /// state machine".
    Verifying,
    /// M2 — typed gate: every required gem is loaded + smoke-tested.
    /// Past this point gem-loading-failure modes are eliminated.
    /// Compiler errors at this stage are environmental
    /// (Cloudflare API outage, AWS rate limit), never substrate-level
    /// "the operator forgot to package the gem."
    Verified,
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

/// Per-resource drift / change detail from a plan.
///
/// One entry per resource the plan would touch. Action is the
/// terraform action category; risk is a heuristic so observers can
/// quickly triage (a `delete` on a destroy-protected resource is
/// `high`, a no-op refresh is `none`, a single-attribute update is
/// `low`). `attributes` lists the field names that differ — values
/// are intentionally elided so secrets don't leak into the K8s API.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct DriftDetail {
    /// Terraform resource address (e.g. `cloudflare_dns_record.foo`).
    pub address: String,

    /// Action category: create | update | delete | replace | noop.
    pub action: String,

    /// Risk heuristic: none | low | medium | high.
    pub risk: String,

    /// Attribute names that differ between current and desired state.
    /// Empty for create / delete (no per-attr diff applies).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub attributes: Vec<String>,

    /// Resolved policy decision for this specific change. Set when
    /// `spec.policies` is non-empty or `spec.defaultDecision` is
    /// non-null. Values: `autoApply` | `requireApproval` | `refuse`.
    /// Absent means policy evaluation didn't run (legacy
    /// `autoApprove`-only mode).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub policy_decision: Option<String>,

    /// Name of the `PolicyRule` that matched this change, or
    /// `<default>` if no rule matched and the default decision was
    /// applied. Absent means policy evaluation didn't run.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub matched_policy: Option<String>,
}

/// One policy rule. Match clauses use AND semantics (all set fields must
/// match the change); within a clause, list entries use OR semantics
/// (any list entry that matches counts). Empty / omitted clauses are
/// wildcards.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct PolicyRule {
    /// Human-readable label. Surfaced in `status.driftDetails[].matchedPolicy`
    /// and in the per-rule planSummary, so a quick `kubectl describe`
    /// shows which rule triggered which decision.
    pub name: String,

    /// Match criteria. All set fields must match (AND); within each
    /// list, any entry counts (OR).
    #[serde(rename = "match")]
    pub match_: PolicyMatch,

    /// What the controller may do for changes this rule matches.
    pub decision: PolicyDecision,
}

/// Match criteria for a `PolicyRule`. All set fields are AND'd.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct PolicyMatch {
    /// Glob patterns against the terraform resource type
    /// (e.g. `cloudflare_dns_record`, `cloudflare_*`, `aws_iam_*`).
    /// Only `*` (zero-or-more chars) is supported — keeps matching
    /// trivial and predictable.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub resource_types: Vec<String>,

    /// Regular expressions matched against the full resource address
    /// (e.g. `^cloudflare_dns_record\\.rio-.*$`). Invalid regexes are
    /// rejected at evaluation time and logged — they never silently
    /// match nothing.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub address_patterns: Vec<String>,

    /// Restrict to specific actions. Empty = any action.
    /// Valid values: `create`, `update`, `delete`, `replace`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub actions: Vec<String>,

    /// Restrict to specific risk levels. Empty = any risk.
    /// Valid values: `none`, `low`, `medium`, `high`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub risk_levels: Vec<String>,

    /// Glob patterns against the changed-attribute names. Matches if
    /// ANY of the change's attributes matches ANY of these patterns.
    /// Useful for "require approval if `ttl` or any `secret*` field
    /// changes".
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub attributes: Vec<String>,
}

/// Decision a `PolicyRule` (or the default fallback) carries.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum PolicyDecision {
    /// Apply immediately without human approval.
    AutoApply,
    /// Set `pendingPlanHash` and wait for matching `approvedPlanHash`.
    RequireApproval,
    /// Mark the template Failed; never apply this plan. Strongest gate.
    Refuse,
}

impl PolicyDecision {
    /// Lowercase string for status surfacing.
    pub fn as_str(&self) -> &'static str {
        match self {
            PolicyDecision::AutoApply => "autoApply",
            PolicyDecision::RequireApproval => "requireApproval",
            PolicyDecision::Refuse => "refuse",
        }
    }
}

/// Aggregate policy result for an entire plan. Surfaced in
/// `status.policyEvaluation` so observers see the worst-case decision
/// without re-walking every drift entry.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct PolicyEvaluation {
    /// Worst decision across all changes. Drives the
    /// plan→apply transition.
    pub aggregate: String,

    /// Number of changes that resolved to `autoApply`.
    #[serde(default)]
    pub auto_apply_count: u32,

    /// Number of changes that resolved to `requireApproval`.
    #[serde(default)]
    pub require_approval_count: u32,

    /// Number of changes that resolved to `refuse`. Non-zero means
    /// `aggregate == refuse` and the plan is blocked.
    #[serde(default)]
    pub refuse_count: u32,

    /// Sample of refused resource addresses (capped at 10) to give
    /// quick triage signal in `kubectl describe`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub refused_addresses: Vec<String>,
}

/// State-settling escalation policy.
///
/// State settling means: after applying a plan, the next drift-check
/// reports no changes. Each Ready→Drifted→Ready cycle increments
/// `status.consecutiveDriftCycles`; a clean drift check (Ready → Ready
/// with no changes) resets it to zero. Once the counter exceeds
/// `maxConsecutiveDriftCycles`, the operator takes the
/// `onExhaustion` action and emits a Warning event listing the
/// resources that keep re-drifting.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SettlingPolicy {
    /// Maximum allowed consecutive drift cycles before escalation.
    /// A "cycle" is one Ready→Drifted→(plan→apply)→Ready transition
    /// where the post-apply drift check still reports changes.
    /// Defaults to 5.
    #[serde(default = "default_max_drift_cycles")]
    pub max_consecutive_drift_cycles: u32,

    /// What to do when `maxConsecutiveDriftCycles` is exceeded.
    /// Defaults to `fail` — the loudest signal: phase Failed, error
    /// message naming the stuck resources, Warning event, condition
    /// `Settled=False reason=StuckInDriftLoop`. The point is to make
    /// it impossible to ignore a system that can't reach steady state.
    #[serde(default)]
    pub on_exhaustion: SettlingExhaustionAction,
}

impl Default for SettlingPolicy {
    fn default() -> Self {
        Self {
            max_consecutive_drift_cycles: default_max_drift_cycles(),
            on_exhaustion: SettlingExhaustionAction::default(),
        }
    }
}

fn default_max_drift_cycles() -> u32 {
    5
}

/// What to do when state-settling fails.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum SettlingExhaustionAction {
    /// Transition to phase=Failed with a loud error message naming
    /// the stuck resource addresses. Stops further reconciliation
    /// until human intervention. **Default.**
    Fail,
    /// Stay in the current loop but flip `Settled=False` condition
    /// and emit a Warning event each cycle. Keeps trying — useful
    /// for transient flakiness in a provider where you'd rather page
    /// than stop.
    Alert,
    /// Just track the counter, surface it in status, but keep
    /// retrying silently. Use only when you genuinely don't want the
    /// operator to escalate (e.g. a known-flaky third-party API).
    Continue,
}

impl Default for SettlingExhaustionAction {
    fn default() -> Self {
        SettlingExhaustionAction::Fail
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use kube::CustomResourceExt;

    #[test]
    fn crd_still_generates_with_cycle_fields() {
        let crd = InfrastructureTemplate::crd();
        let yaml = serde_yaml::to_string(&crd).expect("crd serializes");
        assert!(yaml.contains("InfrastructureTemplate"));
        assert!(yaml.contains("cycleCount"));
        assert!(yaml.contains("lastCycle"));
    }

    #[test]
    fn outcome_display_round_trips() {
        for o in [
            Outcome::Matched,
            Outcome::Updated,
            Outcome::Created,
            Outcome::Destroyed,
            Outcome::Imported,
            Outcome::Drifted,
            Outcome::Failed,
        ] {
            let s = o.to_string();
            let parsed: Outcome = s.parse().expect("outcome round-trips");
            assert_eq!(parsed, o);
        }
    }

    #[test]
    fn cycle_summary_default_zero() {
        let s = CycleSummary::default();
        assert_eq!(s.matched, 0);
        assert_eq!(s.updated, 0);
        assert_eq!(s.created, 0);
        assert_eq!(s.destroyed, 0);
        assert_eq!(s.imported, 0);
        assert_eq!(s.drifted_uncorrected, 0);
        assert_eq!(s.failed, 0);
    }

    #[test]
    fn reconcile_cycle_serializes_with_camel_case() {
        let cycle = ReconcileCycle {
            cycle: 7,
            started_at: chrono::Utc::now(),
            completed_at: chrono::Utc::now(),
            source_revision: Some("abc1234".into()),
            plan_summary: Some("+0 ~1 -0".into()),
            summary: CycleSummary {
                matched: 19,
                updated: 1,
                ..Default::default()
            },
            outcomes: vec![ResourceOutcome {
                address: "cloudflare_dns_record.foo".into(),
                outcome: Outcome::Updated,
                action: Some("update".into()),
                message: None,
            }],
        };
        let json = serde_json::to_string(&cycle).expect("serializes");
        assert!(json.contains("cycle"));
        assert!(json.contains("startedAt"));
        assert!(json.contains("completedAt"));
        assert!(json.contains("planSummary"));
        assert!(json.contains("driftedUncorrected"));
        assert!(json.contains("\"outcome\":\"Updated\""));
    }

    #[test]
    fn template_status_default_omits_last_cycle() {
        let s = InfrastructureTemplateStatus::default();
        let json = serde_json::to_string(&s).expect("serializes");
        // `lastCycle` is skip_serializing_if=Option::is_none — must
        // not appear in default-status payload (no extra etcd churn
        // for templates that haven't reconciled yet).
        assert!(!json.contains("lastCycle"));
        assert!(json.contains("cycleCount"));
    }
}
