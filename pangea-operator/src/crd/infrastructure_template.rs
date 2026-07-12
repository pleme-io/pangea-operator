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
    category = "pangea",
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

    /// IaC backend to use for plan/apply on this CR. Accepts `tofu`
    /// (default — OpenTofu subprocess) or `magma` (in-process Rust
    /// library via the `executor_magma` feature). When unset, falls
    /// back to the operator-wide default from the `PANGEA_EXECUTOR`
    /// env var, then to `tofu`.
    ///
    /// Both backends coexist indefinitely; this field is the per-CR
    /// opt-in for magma during the burn-in period. Per
    /// `theory/MAGMA-OPERATOR-BACKEND.md` §VI.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub executor: Option<String>,

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

    /// Typed, cascading conflict-resolution policy — the operator's
    /// answer to "apply hit a provider conflict (the resource already
    /// exists / is already protected) that the pre-apply import sweep
    /// didn't catch." Rather than failing the cycle, the operator
    /// classifies each conflict and, for `import`-resolution conflicts,
    /// adopts the out-of-band resource via `tofu import` then re-applies
    /// (up to `maxRounds`).
    ///
    /// Two layers, general + specific:
    ///   - GENERAL (default): unset → a bundled policy
    ///     (`alreadyExists` + `alreadyProtected` → `import`, everything
    ///     else → `fail`, 3 rounds) that fires automatically whenever
    ///     `importPolicy.autoOnConflict` is true. Existing templates
    ///     self-heal conflicts with NO spec change.
    ///   - SPECIFIC: set `rules` to override per resource-type + kind
    ///     (first match wins), `defaultResolution` for the unmatched
    ///     fallback, and `enabled` to force on/off independent of
    ///     `autoOnConflict`.
    ///
    /// Default: unset → bundled policy gated on `autoOnConflict`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub conflict_policy: Option<ConflictResolutionPolicy>,

    /// Publish selected `status.outputs` to user-defined K8s Secrets
    /// after every successful apply. Each binding picks a single
    /// output address and writes it to a named Secret/key in any
    /// namespace the operator has reach to.
    ///
    /// X2 of the Crossplane-absorb plan — analog of Crossplane's
    /// `writeConnectionSecretToRef`, but per-binding (not all
    /// outputs to one secret) and with a typed `sensitive` flag.
    /// Idempotent via server-side apply: re-publishes only when the
    /// value actually changes.
    ///
    /// Empty (default) = no secret publication; consumers read
    /// `status.outputs` JSON directly.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub output_bindings: Vec<OutputBinding>,

    /// Secrets resolved into ephemeral, pod-local files under the
    /// template's tofu workspace — written once per Initializing-phase
    /// reconcile, immediately before `tofu init`, by
    /// `controller::template::secret_files::write_secret_files`. A
    /// rendered template references each by
    /// `${file("${path.module}/<name>")}` HCL interpolation.
    ///
    /// Generalizes the shape `handle_compiling`'s git-auth resolution
    /// already proves for git credentials (resolve a Secret → write to
    /// a file in the workspace → reference from rendered HCL) to ANY
    /// named secret a workspace's Ruby needs as a file on disk — not
    /// just git auth. First consumer: a raw (non-cloud-managed)
    /// `kubernetes { token = ... }` provider block, which has no Ruby
    /// `provider :xyz do` surface to inline an `ENV.fetch` into and
    /// isn't provider-block-shaped credential data (region, api_token,
    /// …) either.
    ///
    /// Deliberately OUTSIDE the `ProviderCredentials`/`ProviderKind`
    /// exhaustiveness chain (see that type's own doc comment) — this
    /// is not a provider credential the operator renders into
    /// `providers.tf.json`, it's an arbitrary named file the rendered
    /// HCL reads via `file(...)`. Never written to git; lives only in
    /// the pod-local emptyDir workspace (★★ MAGMA-NATIVE's one
    /// sanctioned filesystem reach — no new persistence surface, no
    /// new secret-at-rest concern beyond what `_git_user`/`_git_pass`
    /// already establish).
    ///
    /// Skipped entirely on the magma path (`load_config_routed` reads
    /// providers from the resolved config, never from workspace files)
    /// — same gating as `write_provider_config`. Empty (default) = no
    /// secret files written; existing templates pay nothing for this.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub secret_files: Vec<SecretFileRef>,
}

/// One named secret resolved into a workspace file. See
/// `InfrastructureTemplateSpec::secret_files`.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SecretFileRef {
    /// Filename written under the workspace directory, referenced from
    /// rendered HCL as `${file("${path.module}/<name>")}`. Must be a
    /// bare filename — no path separators, no `..`, no leading `.`
    /// (validated at write time by
    /// `secret_files::write_secret_files`, not at the schema layer, to
    /// keep the CRD a plain string field rather than a regex-pattern
    /// one).
    pub name: String,

    /// Secret to read the value from.
    pub secret_ref: SecretRef,

    /// Key within the Secret's `.data` map.
    pub key: String,
}

/// One binding of a tofu output to a K8s Secret key. Authored by
/// the user on `InfrastructureTemplate.spec.outputBindings`; consumed
/// by `controller/template/output_bindings.rs::apply_output_bindings`
/// after every successful apply.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct OutputBinding {
    /// Tofu output address — looked up against `status.outputs`
    /// after apply. Examples:
    ///   - `cloudflare_pages_domain.varanda.id`
    ///   - `aws_iam_role.deployer.arn`
    /// Outputs the tofu run did not produce are skipped with a
    /// log line; they don't fail the reconcile (apply already
    /// succeeded by the time bindings publish).
    pub output: String,

    /// Where to write the value.
    pub secret_ref: OutputSecretRef,
}

/// Target Secret + key for one output binding.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct OutputSecretRef {
    /// Secret name. Created if missing; updated via server-side
    /// apply if existing.
    pub name: String,

    /// Secret namespace. Required — operators don't infer from the
    /// template's namespace because templates often produce outputs
    /// consumed in another namespace. The operator must have RBAC
    /// to write Secrets in this namespace (chart 0.8.12+ grants
    /// secrets create/update/patch cluster-wide).
    pub namespace: String,

    /// Key within the Secret's `.data` map.
    pub key: String,

    /// When true, the operator stamps a
    /// `pangea.pleme.io/sensitive=true` label on the Secret.
    /// Doesn't change K8s storage behavior — etcd encryption is
    /// orthogonal — but surfaces author intent for downstream
    /// tooling (mounted-as-env policies, audit dashboards, ESO
    /// hand-off).
    #[serde(default)]
    pub sensitive: bool,
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

    /// Plan-time fallback value used when the upstream template's real output
    /// isn't available yet (Terragrunt `mock_outputs` parity, P2). With a mock,
    /// this template can PLAN before its upstream has applied; the operator still
    /// gates APPLY until the real output lands (a mocked value is never applied).
    /// `null`/absent ⇒ the upstream must be Ready before this template proceeds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    // A mocked upstream output is arbitrary JSON (usually a scalar ID/ARN).
    // Without an explicit schema, schemars emits a typeless schema for
    // serde_json::Value, which Kubernetes rejects as non-structural
    // ("mockOutput.type: Required value: must not be empty") — this broke the
    // chart 0.8.26 CRD apply and blocked every operator upgrade.
    #[schemars(schema_with = "super::any_json_schema")]
    pub mock_output: Option<serde_json::Value>,
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
///
/// **Exhaustiveness contract:** every field on this struct must be
/// matched on by `ProviderCredentials::iter_secret_refs` (and any
/// other call site that needs to walk all providers). Adding a new
/// provider field WITHOUT updating the iter method's match arm is a
/// compile-time error — this is the typed-substrate guarantee that
/// supersedes the silent "added GitHubCredentials but never wired
/// the env-var injection" bug shipped in 92f2f74.
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

/// Typed identifier for each provider known to the operator.
///
/// Adding a variant here forces:
///   * a matching field on `ProviderCredentials` (or compile fails)
///   * a matching arm in `ProviderCredentials::iter_secret_refs` (or
///     compile fails)
///   * a matching arm in any other consumer that exhaustively walks
///     this enum
///
/// The compile-time chain is the entire point — silent "added
/// provider X, forgot to wire credential injection" bugs become
/// impossible.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ProviderKind {
    Aws,
    Cloudflare,
    GitHub,
}

impl ProviderKind {
    /// Stable string key used in logs, metrics labels, and structured
    /// errors. Same value as the camelCase serde field name on
    /// `ProviderCredentials` for k8s-API symmetry.
    pub const fn name(&self) -> &'static str {
        match self {
            ProviderKind::Aws => "aws",
            ProviderKind::Cloudflare => "cloudflare",
            ProviderKind::GitHub => "github",
        }
    }

    /// Does the operator emit an entry for this kind into
    /// `providers.tf.json`?
    ///
    /// Two authority models for provider configuration:
    ///
    /// - **Operator-side** (`true`): the operator pulls the secret +
    ///   renders the provider block into its own `providers.tf.json`
    ///   alongside the workspace's compiled output. Used when the
    ///   provider's natural Ruby DSL surface doesn't carry credentials
    ///   inline (AWS region, Cloudflare API token).
    /// - **Ruby-side** (`false`): the workspace's Ruby `provider :foo
    ///   do { token … }` block already inlines the credential — usually
    ///   via `ENV.fetch` against an env var the operator injects per
    ///   `iter_secret_refs`. The operator must NOT emit a parallel
    ///   provider block here; doing so produces conflicting / empty
    ///   provider definitions.
    ///
    /// **Compile-time exhaustiveness:** adding a new `ProviderKind`
    /// variant forces this match to be extended. "I added a provider
    /// type, forgot the operator-emit decision" is impossible — this
    /// is the typed contract that supersedes the silent
    /// `{"provider": {}}` wedge that pleme-io-opensource hit when
    /// `GitHubCredentials` was the only declared provider and the
    /// operator's emitter unconditionally wrote an empty `providers`
    /// map.
    pub const fn operator_emits_provider_block(&self) -> bool {
        match self {
            ProviderKind::Aws => true,
            ProviderKind::Cloudflare => true,
            // GitHub: the Ruby `provider :github do { token gh_token }`
            // block in workspace renderers (e.g. github_org_workspace.rb)
            // inlines the token via ENV. Operator emits only the env-var
            // injection, no providers.tf.json block.
            ProviderKind::GitHub => false,
        }
    }
}

impl ProviderCredentials {
    /// Walk every populated provider's `(kind, secret_ref)`.
    ///
    /// **Exhaustive at compile time.** Adding a new field to the
    /// struct without adding a new line here is a Rust unused-field
    /// warning AND a missing case in any consumer that maps over
    /// `ProviderKind` — both surface in CI before the operator ships.
    ///
    /// The implementation deliberately uses an explicit destructuring
    /// pattern (rather than per-field accessor calls) so missing a
    /// new field is a `non_exhaustive_omitted_patterns`-flavored
    /// compile message, not a runtime no-op.
    pub fn iter_secret_refs(&self) -> Vec<(ProviderKind, &SecretRef)> {
        // Destructure the entire struct so the compiler enforces that
        // every field is named here. Adding a field will force this
        // line to fail to compile until the new field is added below.
        let ProviderCredentials {
            aws,
            cloudflare,
            github,
        } = self;

        let mut out = Vec::new();
        if let Some(c) = aws {
            out.push((ProviderKind::Aws, &c.secret_ref));
        }
        if let Some(c) = cloudflare {
            out.push((ProviderKind::Cloudflare, &c.secret_ref));
        }
        if let Some(c) = github {
            out.push((ProviderKind::GitHub, &c.secret_ref));
        }
        out
    }
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

/// Typed classification of a provider conflict surfaced by a failed
/// `tofu apply`. The operator's conflict classifier
/// (`controller::conflict::classify_apply_conflicts`) maps OpenTofu's
/// diagnostic text onto these kinds; the [`ConflictResolutionPolicy`]
/// then decides what to do per kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum ConflictKind {
    /// The resource already exists in the provider (HTTP 409/422
    /// "already exists" / "name already taken"). The canonical case:
    /// an out-of-band resource the pre-apply import sweep didn't adopt.
    AlreadyExists,
    /// The resource (or a sub-resource it manages) is already in the
    /// target protected/locked state (e.g. a branch is "already
    /// protected").
    AlreadyProtected,
    /// Any other apply failure. Never auto-resolved by the bundled
    /// default — surfaced as a real error unless a rule says otherwise.
    Other,
}

/// What the operator does about a classified conflict.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum ConflictResolution {
    /// Adopt the existing resource into state via `tofu import`, then
    /// re-apply. Converges the posture without recreating.
    Import,
    /// Leave the resource as-is; don't import, don't fail the round on
    /// its account. Use for resources whose conflict is expected/benign.
    Skip,
    /// Surface the conflict as a real apply failure (current behaviour).
    Fail,
}

/// One typed conflict-resolution rule — the *specific* layer. Evaluated
/// top-to-bottom; the first rule whose `resourceTypes` (empty = any) and
/// `kinds` (empty = any) match the conflict wins.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ConflictRule {
    /// Tofu resource types this rule matches (e.g. `github_repository`).
    /// Empty = match any resource type.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub resource_types: Vec<String>,

    /// Conflict kinds this rule matches. Empty = match any kind.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub kinds: Vec<ConflictKind>,

    /// What to do when this rule matches.
    pub resolution: ConflictResolution,
}

/// Typed, configurable conflict-resolution policy.
///
/// Two layers, general + specific (see [`ConflictResolutionPolicy::bundled_default`]):
///   - GENERAL: the bundled default adopts `alreadyExists` /
///     `alreadyProtected` via import and fails everything else; it fires
///     automatically when `importPolicy.autoOnConflict` is true.
///   - SPECIFIC: author `rules` to override per (resourceType, kind),
///     `defaultResolution` for the unmatched fallback, and `enabled` to
///     force the whole mechanism on/off independent of `autoOnConflict`.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ConflictResolutionPolicy {
    /// Master switch. `Some(true)`/`Some(false)` forces the post-apply
    /// conflict catch on/off. `None` (default) inherits from
    /// `importPolicy.autoOnConflict` — so templates already opting into
    /// auto-import get the convergence guarantee with no extra config.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub enabled: Option<bool>,

    /// Specific per-(resourceType, kind) rules, first match wins.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub rules: Vec<ConflictRule>,

    /// Resolution for conflicts no `rules` entry matched. Defaults to
    /// `fail` (never silently paper over an unrecognized conflict).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_resolution: Option<ConflictResolution>,

    /// Max import→re-apply rounds before giving up (clamped to 1..=10).
    /// Default 3.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_rounds: Option<u32>,
}

impl ConflictResolutionPolicy {
    /// The bundled general-case policy: adopt `alreadyExists` +
    /// `alreadyProtected` via import, fail everything else, 3 rounds.
    /// `enabled: None` so it inherits `importPolicy.autoOnConflict`.
    pub fn bundled_default() -> Self {
        Self {
            enabled: None,
            rules: vec![ConflictRule {
                resource_types: Vec::new(),
                kinds: vec![ConflictKind::AlreadyExists, ConflictKind::AlreadyProtected],
                resolution: ConflictResolution::Import,
            }],
            default_resolution: Some(ConflictResolution::Fail),
            max_rounds: Some(3),
        }
    }

    /// Resolve the action for a `(resource_type, kind)` conflict: first
    /// matching rule wins, else `default_resolution`, else `Fail`.
    pub fn resolution_for(&self, resource_type: &str, kind: ConflictKind) -> ConflictResolution {
        for rule in &self.rules {
            let type_ok = rule.resource_types.is_empty()
                || rule.resource_types.iter().any(|t| t == resource_type);
            let kind_ok = rule.kinds.is_empty() || rule.kinds.contains(&kind);
            if type_ok && kind_ok {
                return rule.resolution;
            }
        }
        self.default_resolution.unwrap_or(ConflictResolution::Fail)
    }

    /// Round budget, clamped to a sane 1..=10.
    pub fn rounds(&self) -> u32 {
        self.max_rounds.unwrap_or(3).clamp(1, 10)
    }
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

    /// Git commit SHA the most recent successful compile consumed
    /// (gitRepository sources only). The freshness model's anchor:
    /// `Ready` is only settled against THIS revision; handle_ready's
    /// gate compares it to `observedHeadRevision` and bounces to
    /// Compiling on mismatch. Before this field existed, staleness
    /// was unrepresentable in the wrong direction — no status field
    /// could express "the compile is behind the remote".
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub compiled_revision: Option<String>,

    /// When `compiledRevision` was produced. Only restamped when the
    /// revision actually changes (diff-gated; no etcd churn for
    /// same-rev recompiles).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub compiled_at: Option<DateTime<Utc>>,

    /// The remote HEAD the freshness gate last observed via
    /// `git ls-remote` (1 RTT, no clone). Tier-honest: a C2
    /// external-world observation, renewed per check — never a
    /// compile-time proof.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed_head_revision: Option<String>,

    /// When the freshness gate last successfully observed the remote
    /// HEAD. An old timestamp on a Ready template means the remote
    /// has been unreachable (the gate proceeds on Unknown but says so
    /// in the Settled condition message).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_freshness_check_at: Option<DateTime<Utc>>,

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

    /// Number of consecutive Compiling-phase failures observed before
    /// any successful compile. Resets to zero on the first successful
    /// compile. Drives the same `SettlingPolicy.maxConsecutiveDrift
    /// Cycles` escalation as drift cycles — so a template that can't
    /// compile (missing gem, syntax error, broken DSL) eventually
    /// transitions to `phase=Failed` instead of looping in
    /// `phase=Compiling, cycleCount=0` forever.
    ///
    /// Why a separate counter: pre-2026-05, compile failures didn't
    /// increment `consecutive_drift_cycles` (because no cycle ever
    /// completes — cycle_count stays at 0), so settling policy never
    /// fired. Templates like `pleme-io-opensource` sat in Compiling
    /// for hours with `cannot load such file -- pangea-github`. This
    /// counter closes that gap by escalating compile failures
    /// independently of the drift-cycle path.
    #[serde(default)]
    pub consecutive_compile_failures: u32,

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

    /// Echo of the IaC executor the operator selected at runtime for
    /// this template's reconcile path. `"magma"` or `"tofu"`.
    ///
    /// Before this field existed the only way to answer "what's the
    /// operator actually running for this CR?" was to grep its startup
    /// logs for `Wiring Postgres pool for magma state backend` etc.
    /// The operator's own declaration belongs in CR status, queryable
    /// via `kubectl get itr X -o jsonpath='{.status.executor}'`.
    ///
    /// Updated on every cycle (idempotent — `record_reconcile_cycle`'s
    /// content-equality guard suppresses no-op patches), so a flip
    /// caused by feature-flag or backend-availability change shows up
    /// on the next reconcile.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub executor: Option<String>,

    /// Echo of the state backend the operator wired for this cycle.
    /// `"pg/<db_name>"` for Postgres-backed state (the magma path's
    /// canonical case), `"local"` for filesystem, `None` for stateless
    /// executors. Today this is inferable only from main.rs startup
    /// log lines — the CR doesn't say where its state lives. Adding
    /// it here closes that loop.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub backend: Option<String>,
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

    /// Echo of the IaC executor that ran THIS cycle. Preserves
    /// "magma planned this" even after a future flip of
    /// `status.executor`. Mirrors `status.executor` at cycle-record
    /// time; `None` for cycles written by operator versions before
    /// the field landed (backward-compatible deserialize).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub executor: Option<String>,

    /// Distribution of plan action verbs across ALL planned changes
    /// (including no-op). The compact answer to "what would this cycle
    /// actually do?" — replaces having to `kubectl exec` into the pod
    /// + parse `magma-bundle.json plan.changes` by hand.
    ///
    /// Different from `summary` above: `summary` is post-decision
    /// (matched / updated / created / …) and rolls up untouched
    /// resources into `matched`. This is pre-decision (the action
    /// verb the plan emitted for each resource, no-op included), so
    /// it preserves the distinction between "1054 resources, none
    /// changed" (`noOp: 1054`) and "1054 resources, none managed yet"
    /// (`create: 1054`).
    ///
    /// `None` for tofu cycles + for cycles where the bundle wasn't
    /// available (cleanup races, executor wrote no bundle).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub action_distribution: Option<ActionDistribution>,

    /// Reference to the `magma-bundle.json` artifact this cycle
    /// produced. Today the bundle lives in the operator pod's emptyDir
    /// at `/var/pangea/workspaces/<ns>/<name>/magma-bundle.json` and
    /// is only readable via `kubectl exec`. Carrying its `bundleId` +
    /// `sha256` here lets observers verify the bundle they fetched
    /// matches the cycle they read about. A follow-up slice publishes
    /// the bundle as a ConfigMap and extends this struct with
    /// `name`/`namespace` fields.
    ///
    /// `None` for tofu cycles + cycles whose bundle couldn't be
    /// read (parse error, missing file).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bundle_ref: Option<BundleRef>,

    /// Severity rollup across this cycle's resource changes —
    /// `{cosmetic, functional, breaking}`. The user-facing answer to
    /// "how scary is this plan?". Magma populates from its native
    /// drift-classifier severities; tofu populates from the pure
    /// `action_to_severity` mapping (Cosmetic for no-op, Functional
    /// for create/update, Breaking for delete/replace). The sum
    /// equals the total change count.
    ///
    /// `None` when this cycle had no per-resource changes to
    /// classify (empty plan).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub severity_rollup: Option<SeverityRollup>,

    /// Lifecycle FSM phase recorded in the magma bundle —
    /// `"planning"`, `"applying"`, `"verifying"`, `"stable"`,
    /// `"failed"`. Honestly absent (`None`) for tofu cycles: tofu has
    /// no lifecycle FSM, so the operator never fabricates one.
    /// Magma's lifecycle states surface here so observers can answer
    /// "where did this cycle stop?" without exec-reading the bundle.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lifecycle_phase: Option<String>,
}

/// Per-cycle severity rollup — counts of resource changes per
/// severity bucket. Sum equals the total change count for the cycle.
///
/// Mirrors `executor::cycle_artifact::SeverityRollup` at the CRD type
/// layer (so the schemars derive flows through to the CRD YAML
/// without depending on the executor module). The two types
/// round-trip via `SeverityRollup::from` conversions wired in
/// `build_reconcile_cycle`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SeverityRollup {
    /// Cosmetic changes — comment-level, no real effect (most no-ops
    /// fall here when surfaced from magma; tofu surfaces them via the
    /// pure action→severity mapping for no-op).
    #[serde(default)]
    pub cosmetic: u32,
    /// Functional changes — resource updated, created, or
    /// semantically-meaningful attributes change. The default bucket
    /// for create/update.
    #[serde(default)]
    pub functional: u32,
    /// Breaking changes — destroy, replace, or anything that loses
    /// data / interrupts service. The bucket operators pay attention
    /// to. Maps to magma's `critical` severity at the bundle boundary.
    #[serde(default)]
    pub breaking: u32,
}

/// Distribution of plan action verbs across the changes a cycle's
/// plan emitted. One bucket per terraform action; `other` catches
/// future tofu vocab additions (read, forget, …) so a new verb never
/// silently drops out of the rollup.
///
/// The sum of all buckets equals the total `plan.changes` entry count
/// — i.e. equals the managed-resource count for the workspace. For a
/// fully-converged workspace `noOp` equals that total + everything
/// else is zero.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ActionDistribution {
    /// Resources the plan classified as no-op — declared state already
    /// matches actual state, no apply action.
    #[serde(default)]
    pub no_op: u32,
    /// Resources the plan would create on apply.
    #[serde(default)]
    pub create: u32,
    /// Resources the plan would update in-place on apply.
    #[serde(default)]
    pub update: u32,
    /// Resources the plan would destroy on apply.
    #[serde(default)]
    pub delete: u32,
    /// Resources the plan would destroy + recreate (terraform's
    /// `replace` action).
    #[serde(default)]
    pub replace: u32,
    /// Catch-all for action verbs not bucketed above (terraform's
    /// `read`, `forget`, future additions). Keeps the rollup faithful
    /// even when the upstream vocab grows.
    #[serde(default)]
    pub other: u32,
}

/// Reference to the magma-bundle.json artifact a cycle produced.
///
/// Carries the bundle's identity (`bundle_id` from the bundle itself)
/// + size. A follow-up slice publishes bundles as ConfigMaps and
/// extends this struct with `configMapName` + `configMapNamespace`
/// so observers can fetch the bundle without `kubectl exec`-ing the
/// operator pod.
///
/// The `bundle_id` is already a BLAKE3 over the bundle's canonical
/// representation (produced by magma_bundle when the bundle is
/// minted), so it doubles as the artifact's content fingerprint —
/// no separate file digest is needed at this layer.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct BundleRef {
    /// Bundle kind discriminator from `magma-bundle.json.kind` — today
    /// always `"terraform"`; future executors (pulumi, opentofu,
    /// kubernetes) get their own discriminator.
    pub kind: String,
    /// Stable bundle identifier — the `bundle_id` field from
    /// `magma-bundle.json` (BLAKE3 over the bundle's canonical
    /// representation). Lets observers correlate cycles to bundles
    /// across compaction/cleanup AND verify the bundle they fetched
    /// matches the cycle they read about (the bundle_id is the
    /// content hash).
    pub bundle_id: String,
    /// Bundle file size in bytes — UX hint ("is this a 1KB null
    /// bundle or a 3MB serious one?") and capacity-planning input for
    /// the future ConfigMap-publication slice (etcd value-size budget).
    pub size_bytes: u64,
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
    /// HEAD observed, compile of it cannot succeed. Unlike `Failed`
    /// this phase self-heals: its handler retries Compiling on
    /// backoff and exits the moment a new commit compiles — a broken
    /// commit on the tracked ref parks here LOUDLY (Events + ladder
    /// escalation already fired) instead of wedging in Failed until a
    /// human resets. Entered via `handle_compile_failure` when the
    /// consecutive-compile-failure threshold trips.
    CompileBlocked,
    /// Running `tofu destroy`.
    Destroying,
}

impl Default for Phase {
    fn default() -> Self {
        Phase::Pending
    }
}

impl Phase {
    /// Every lifecycle phase, in state-machine order. The fleet
    /// `pangea_templates_by_phase` gauge resets all of these to 0 each
    /// aggregation tick before applying live counts, so a phase that
    /// just emptied reads 0 — never a stale, never-decremented series.
    pub const ALL: [Phase; 11] = [
        Phase::Pending,
        Phase::Verifying,
        Phase::Verified,
        Phase::Compiling,
        Phase::Initializing,
        Phase::Planning,
        Phase::Applying,
        Phase::Ready,
        Phase::Drifted,
        Phase::Failed,
        Phase::Destroying,
    ];
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

/// Implements the shared `ConditionLike` accessor trait so this CRD's
/// `Condition` type works with `crate::controller::status` helpers
/// (`merge_condition_transitions`, `conditions_observably_equal`).
/// Without this impl, every consumer (template, flow, compliance,
/// synthesizer, fleet_status — anything using `crate::crd::Condition`)
/// would have to hand-roll its own condition comparison.
impl crate::controller::status::ConditionLike for Condition {
    type Time = DateTime<Utc>;
    fn condition_type(&self) -> &str { &self.r#type }
    fn status(&self) -> &str { &self.status }
    fn reason(&self) -> &str { &self.reason }
    fn message(&self) -> &str { &self.message }
    fn last_transition_time(&self) -> &DateTime<Utc> { &self.last_transition_time }
    fn set_last_transition_time(&mut self, t: DateTime<Utc>) { self.last_transition_time = t; }
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
    /// Valid values: `create`, `update`, `delete`, `replace`
    /// (see [`DriftAction`]). Matched case-sensitively against
    /// `DriftDetail.action` — `executor::policy::actions_match` warns
    /// and treats the whole clause as no-match if either side falls
    /// outside this vocabulary, rather than silently comparing two
    /// strings that were never going to be equal.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub actions: Vec<String>,

    /// Restrict to specific risk levels. Empty = any risk.
    /// Valid values: `none`, `low`, `medium`, `high`
    /// (see [`RiskLevel`]). Matched case-sensitively against
    /// `DriftDetail.risk` — `executor::policy::risk_levels_match` warns
    /// and treats the whole clause as no-match if either side falls
    /// outside this vocabulary, rather than silently comparing two
    /// strings that were never going to be equal.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub risk_levels: Vec<String>,

    /// Glob patterns against the changed-attribute names. Matches if
    /// ANY of the change's attributes matches ANY of these patterns.
    /// Useful for "require approval if `ttl` or any `secret*` field
    /// changes".
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub attributes: Vec<String>,
}

/// Closed vocabulary for `DriftDetail.action` / `PolicyMatch.actions`.
///
/// Deliberately NOT the wire type of either field (both stay plain
/// `String` — see their doc comments) so that a CRD instance with an
/// out-of-vocabulary value still decodes instead of failing the whole
/// watch stream; `parse_wire` is the one place every producer (the
/// tofu-plan `risk_level()` classifier, the magma-path
/// `plan_action_to_terraform_str()` mapper) and the policy matcher
/// agree on what "in vocabulary" means, so a divergence between them
/// is a loud `executor::policy` warning instead of a silently-dead
/// policy rule.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DriftAction {
    Create,
    Update,
    Delete,
    Replace,
}

impl DriftAction {
    /// Parse the lowercase wire string this vocabulary uses. Returns
    /// `None` for anything else — including `"noop"` (never surfaced
    /// in a `DriftDetail`) and any case/spelling mismatch.
    pub fn parse_wire(s: &str) -> Option<Self> {
        match s {
            "create" => Some(Self::Create),
            "update" => Some(Self::Update),
            "delete" => Some(Self::Delete),
            "replace" => Some(Self::Replace),
            _ => None,
        }
    }
}

/// Closed vocabulary for `DriftDetail.risk` / `PolicyMatch.riskLevels`.
/// See [`DriftAction`] for why this mirrors, rather than replaces,
/// the `String` wire type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RiskLevel {
    None,
    Low,
    Medium,
    High,
}

impl RiskLevel {
    /// Parse the lowercase wire string this vocabulary uses.
    pub fn parse_wire(s: &str) -> Option<Self> {
        match s {
            "none" => Some(Self::None),
            "low" => Some(Self::Low),
            "medium" => Some(Self::Medium),
            "high" => Some(Self::High),
            _ => None,
        }
    }
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
    fn provider_kind_name_stable() {
        // Stable keys for log/metric labels; matches camelCase serde
        // field names on `ProviderCredentials`.
        assert_eq!(ProviderKind::Aws.name(), "aws");
        assert_eq!(ProviderKind::Cloudflare.name(), "cloudflare");
        assert_eq!(ProviderKind::GitHub.name(), "github");
    }

    fn empty_secret_ref(name: &str) -> SecretRef {
        SecretRef {
            name: name.to_string(),
            namespace: None,
        }
    }

    #[test]
    fn iter_secret_refs_empty_when_all_none() {
        let creds = ProviderCredentials {
            aws: None,
            cloudflare: None,
            github: None,
        };
        assert!(creds.iter_secret_refs().is_empty());
    }

    #[test]
    fn iter_secret_refs_yields_only_populated_providers() {
        let creds = ProviderCredentials {
            aws: None,
            cloudflare: Some(CloudflareCredentials {
                secret_ref: empty_secret_ref("cf"),
            }),
            github: Some(GitHubCredentials {
                secret_ref: empty_secret_ref("gh"),
            }),
        };
        let refs = creds.iter_secret_refs();
        assert_eq!(refs.len(), 2);
        let kinds: Vec<ProviderKind> = refs.iter().map(|(k, _)| *k).collect();
        assert!(kinds.contains(&ProviderKind::Cloudflare));
        assert!(kinds.contains(&ProviderKind::GitHub));
        assert!(!kinds.contains(&ProviderKind::Aws));
    }

    #[test]
    fn iter_secret_refs_yields_all_when_all_populated() {
        let creds = ProviderCredentials {
            aws: Some(AwsCredentials {
                secret_ref: empty_secret_ref("aws"),
                region: None,
                role_arn: None,
            }),
            cloudflare: Some(CloudflareCredentials {
                secret_ref: empty_secret_ref("cf"),
            }),
            github: Some(GitHubCredentials {
                secret_ref: empty_secret_ref("gh"),
            }),
        };
        let refs = creds.iter_secret_refs();
        assert_eq!(refs.len(), 3);

        // The exhaustiveness contract: count must equal the number
        // of fields on ProviderCredentials. If a future commit adds
        // a fourth provider field but forgets the iter_secret_refs
        // case, this test still expects 3 — but the destructuring
        // pattern in iter_secret_refs would have broken at compile
        // time first. This test is the runtime backstop.
        let aws_ref = refs
            .iter()
            .find(|(k, _)| *k == ProviderKind::Aws)
            .map(|(_, sref)| sref.name.as_str());
        assert_eq!(aws_ref, Some("aws"));
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
            ..Default::default()
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
