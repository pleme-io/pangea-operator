//! Typed, tier-resolved operator configuration — the shikumi
//! progressive-discovery surface for `pangea-operator`.
//!
//! # Why this exists
//!
//! Before this module the operator read its configuration through a dozen
//! scattered `std::env::var(...)` sites (leader identity in `leader.rs`,
//! server addrs + compiler + Postgres wiring in `main.rs`, executor binaries
//! in `executor/mod.rs`, the executor gate in `executor/backend_select.rs`,
//! reconcile/budget knobs in `controller/`, observability in
//! `observability/`, …). There was no single typed schema, no provenance,
//! no `config-show`. This closed the ★★ CONFIGURATION MANAGEMENT
//! `skip-shikumi` gap: [`OperatorConfig`] is one `#[serde(deny_unknown_fields)]`
//! struct mirroring that entire env surface, implementing
//! [`shikumi::TieredConfig`], resolved once at startup through the sealed
//! progressive fold with per-leaf [`shikumi::Provenance`].
//!
//! # The tiers, mapped honestly
//!
//! * **`discovered()`** — the k8s downward-API / pod-environment reads ARE
//!   the discovered tier: `POD_NAMESPACE` and `POD_NAME`/`HOSTNAME` (the
//!   leader identity), wired declaratively through [`IdentityLayer`] (a local
//!   [`shikumi::DiscoveryLayer`]). Every other section is `bare()` here.
//! * **`prescribed_default()`** — the operator's sane defaults: the health /
//!   metrics / graphql / grpc addrs, the lease name + durations, the compiler
//!   backend, the Postgres coordinates, and — per the ★★ MAGMA-NATIVE
//!   directive — `executor.backend = "magma"` + `executor.forbid_tofu = true`
//!   as the shipped magma-native default. Built ON TOP of `discovered()` so a
//!   detected identity shows through (last-changer attribution credits it to
//!   `Discovered`).
//! * **env overlay** — every bespoke `PANGEA_*` / `PG*` / `*_ADDR` env var,
//!   read with byte-identical parse + fallback logic to the legacy sites and
//!   assembled into one [`shikumi::ProgressiveLayer::env`] overlay at the
//!   `Custom` tier. Present env wins; absent falls to the prescribed default.
//!
//! # Migration status (tier-honest)
//!
//! This is an ADDITIVE, behavior-preserving surface on a LIVE operator.
//! Startup reads that are cleanly centralizable in `main.rs` are migrated to
//! consume the typed config (server addrs, [`crate::executor::ExecutorConfig`],
//! compiler, non-secret Postgres coordinates). The load-bearing reads that
//! are threaded through shared state or decide magma-vs-tofu — leader
//! election (`leader.rs`), the executor gate (`PANGEA_EXECUTOR` /
//! `PANGEA_FORBID_TOFU`), reconcile/budget, observability, routing, gem cache
//! — are MIRRORED in the schema (so provenance is complete + `config-show`
//! is accurate) but keep their existing live read path unchanged
//! (`pending-shikumi`); their equivalence is pinned by the tests below.
//!
//! Secrets (`PGPASSWORD`, `PANGEA_API_TOKEN`, `PANGEA_GEM_AUTH_TOKEN`) are
//! deliberately NOT part of this serialized surface — they must never land in
//! a provenance dump / `config-show`. They stay direct env reads.

use std::path::{Path, PathBuf};

use figment::providers::Serialized;
use figment::value::Dict;
use figment::Figment;
use serde::{Deserialize, Serialize};
use shikumi::{
    DiscoveryLayer, ProgressiveLayer, ProgressiveResolution, ProvenanceMap, TieredConfig,
};

/// The whole operator configuration, one typed struct mirroring the env
/// surface. Resolved once at startup via [`Self::resolve`].
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperatorConfig {
    /// Pod identity (discovered tier — `POD_NAMESPACE`, `POD_NAME`/`HOSTNAME`).
    pub identity: IdentityConfig,
    /// Leader-election lease knobs (`LEADER_*`).
    pub leader: LeaderElectionConfig,
    /// HTTP/gRPC listen addresses (`*_ADDR`).
    pub servers: ServersConfig,
    /// Executor selection + binaries + timeouts (`PANGEA_EXECUTOR`,
    /// `PANGEA_FORBID_TOFU`, `TOFU_BINARY`, `PANGEA_TIMEOUT`, …).
    pub executor: ExecutorSurface,
    /// Pangea DSL compiler backend (`COMPILER_ENDPOINT`,
    /// `PANGEA_COMPILER_BACKEND`, `PANGEA_RUBY_WORKERS`).
    pub compiler: CompilerConfig,
    /// Postgres state-backend coordinates (`PGHOST`/`PGPORT`/`PGUSER`/
    /// `PGDATABASE`). Password is a secret, deliberately excluded.
    pub database: DatabaseConfig,
    /// Reconcile concurrency + admission budget (`PANGEA_RECONCILE_WORKERS`,
    /// `PANGEA_BUDGET_*`).
    pub reconcile: ReconcileConfig,
    /// Tracing / OTLP (`LOG_FORMAT`, `OTEL_*`).
    pub observability: ObservabilityConfig,
    /// Escalation routing (`PANGEA_NTFY_BASE_URL`).
    pub routing: RoutingConfig,
    /// GraphQL API surface (`PANGEA_ENABLE_PLAYGROUND`). Token is a secret,
    /// deliberately excluded.
    pub api: ApiConfig,
    /// Embedded-Ruby gem cache (`PANGEA_GEM_CACHE_DIR`). Auth token is a
    /// secret, deliberately excluded.
    pub gem: GemConfig,
    #[serde(default)]
    pub network: crate::network::NetworkConfig,
    /// Event-driven reconcile triggers (`PANGEA_HONOR_RECONCILE_REQUESTS`).
    /// Additive: every knob defaults to the behavior the operator had before
    /// it existed, so an older config file reads unchanged.
    #[serde(default)]
    pub reactive: ReactiveConfig,
    /// Leaf-by-default admission (`PANGEA_LEAF_ADMISSION`). Additive: a
    /// config file without the section reads as the enforced default.
    #[serde(default)]
    pub leaf: LeafConfig,
    #[serde(default)]
    pub workers: WorkersConfig,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkersConfig {
    #[serde(default)]
    pub autoscaling: AutoscalingConfig,
    #[serde(default)]
    pub weights: pangea_leaf::work::Weights,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AutoscalingConfig {
    #[serde(default = "default_on")]
    pub enabled: bool,
    #[serde(default)]
    pub min: u32,
    #[serde(default = "default_autoscaling_max")]
    pub max: u32,
    #[serde(default = "default_autoscaling_target")]
    pub target: u64,
    #[serde(default = "default_autoscaling_horizon_secs")]
    pub horizon_secs: u64,
}

fn default_autoscaling_max() -> u32 {
    4
}

fn default_autoscaling_target() -> u64 {
    600
}

fn default_autoscaling_horizon_secs() -> u64 {
    300
}

impl Default for AutoscalingConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            min: 0,
            max: default_autoscaling_max(),
            target: default_autoscaling_target(),
            horizon_secs: default_autoscaling_horizon_secs(),
        }
    }
}

/// Pod identity — the discovered tier.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IdentityConfig {
    /// The operator's own namespace (`POD_NAMESPACE`; fallback
    /// `pangea-system`).
    pub namespace: String,
    /// This pod's unique holder identity (`POD_NAME` → `HOSTNAME` → pid).
    pub pod_name: String,
}

/// Leader-election lease configuration.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LeaderElectionConfig {
    /// Whether leader election runs (`LEADER_ELECTION`; default true).
    pub election_enabled: bool,
    /// `Lease` object name (`LEADER_LEASE_NAME`).
    pub lease_name: String,
    /// Lease validity window in seconds (`LEADER_LEASE_DURATION_SECS`).
    pub lease_duration_secs: u64,
    /// Holder renew interval in seconds (`LEADER_RENEW_SECS`).
    pub renew_secs: u64,
    /// Non-holder retry interval in seconds (`LEADER_RETRY_SECS`).
    pub retry_secs: u64,
}

/// HTTP / gRPC listen addresses. Stored as strings; parsed to
/// [`std::net::SocketAddr`] at consumption (same panic-on-invalid semantics
/// the legacy `main.rs` had).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServersConfig {
    /// Health + `/readyz` + back-compat `/metrics` (`HEALTH_ADDR`).
    pub health_addr: String,
    /// Dedicated metrics scrape endpoint (`METRICS_ADDR`).
    pub metrics_addr: String,
    /// GraphQL server (`GRAPHQL_ADDR`).
    pub graphql_addr: String,
    /// gRPC server (`GRPC_ADDR`).
    pub grpc_addr: String,
}

/// Executor selection, binaries, timeouts.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutorSurface {
    /// Operator-wide default executor (`PANGEA_EXECUTOR`). `None` ⇒ resolve to
    /// magma; `prescribed_default()` names it `magma` explicitly. Consumed via
    /// `ExecutorBackend::resolve` (`pending-shikumi`: live gate unchanged).
    #[serde(default)]
    pub backend: Option<String>,
    /// Ban tofu (`PANGEA_FORBID_TOFU`). The prescribed default is `true`, and
    /// the live gate (`forbid_tofu_posture`) agrees: unset forbids, and only an
    /// explicit false allows tofu.
    pub forbid_tofu: bool,
    /// OpenTofu binary path (`TOFU_BINARY`).
    pub tofu_binary: String,
    /// Packer binary path (`PACKER_BINARY`).
    pub packer_binary: String,
    /// Workspace base dir (`PANGEA_WORKSPACE_BASE`) as seen by
    /// [`crate::executor::ExecutorConfig`] (default `/tmp/pangea-workspaces`).
    pub workspace_base: String,
    /// Tofu command timeout in seconds (`PANGEA_TIMEOUT`).
    pub timeout_secs: u64,
    /// Packer command timeout in seconds (`PACKER_TIMEOUT`).
    pub packer_timeout_secs: u64,
    /// Optional Ruby binary for compilation (`RUBY_BINARY`).
    #[serde(default)]
    pub ruby_binary: Option<String>,
    /// Verbose executor output (`PANGEA_VERBOSE`).
    pub verbose: bool,
}

/// The compiler backend `prescribed_default()` names, chosen by what this
/// binary can actually construct.
///
/// `embedded` needs the magnus interpreter linked in; `lava` needs nothing
/// beyond the crate itself and is available in every build. Naming a backend
/// the binary lacks is a startup panic by design, so the prescription is
/// tied to the feature rather than to a hope about how the image was built.
///
/// ── ★ LAVA UNCONDITIONALLY, 2026-08-25 ───────────────────────────────
/// This was cfg-selected: `embedded` when the interpreter was linked in,
/// `lava` otherwise. The reasoning was sound — never prescribe a backend
/// the binary cannot construct — but it made the SHIPPED default
/// `embedded`, because `embedded_ruby` is a default feature.
///
/// lava is present in EVERY build, so prescribing it unconditionally
/// cannot name an unconstructible backend; the protection the cfg gave
/// is preserved by lava's universality rather than by the branch.
///
/// What the branch cost while it stood: the default path reached `git`
/// 14 times to fetch gems, and embedded unrestricted CRuby — no `$SAFE`,
/// no taint, no timeout, no restricted binding — in this operator's
/// address space, where `Kernel#system` and backticks are reachable from
/// CR content on the fleet control plane. `embedded` remains available by
/// name for workspaces that still author Ruby.
pub const DEFAULT_COMPILER_BACKEND: &str = "lava";

/// Pangea DSL compiler backend selection.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CompilerConfig {
    /// `lava` (tatara-lisp, in-process, no Ruby, no subprocess — THE
    /// DEFAULT), `embedded` (magnus CRuby + GemCache), or `http` (sunset
    /// sidecar, explicit opt-in only).
    /// (`PANGEA_COMPILER_BACKEND`).
    pub backend: String,
    /// HTTP sidecar endpoint for the legacy backend (`COMPILER_ENDPOINT`).
    pub endpoint: String,
    /// Embedded Ruby owner-thread pool size (`PANGEA_RUBY_WORKERS`).
    pub ruby_workers: usize,
    pub lava_ruby: bool,
}

/// Postgres state-backend coordinates (non-secret).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DatabaseConfig {
    /// Host (`PGHOST`).
    pub host: String,
    /// Port (`PGPORT`).
    pub port: u16,
    /// User (`PGUSER`).
    pub user: String,
    /// Database name (`PGDATABASE`).
    pub database: String,
    /// Max connections in the magma workload pool
    /// (`PANGEA_DB_POOL_MAX`).
    ///
    /// This bounds concurrent magma state work, so it wants to be at least
    /// `reconcile.workers` or workers queue on each other for a connection
    /// rather than on real work. It deliberately does NOT bound `/readyz`:
    /// the readiness probe owns a separate one-connection pool so a busy
    /// operator cannot starve its own health check (main.rs).
    pub pool_max_connections: u32,
}

/// Reconcile concurrency + live-dispatch admission budget.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReconcileConfig {
    /// Concurrent reconcile workers, clamped `[1, 32]`
    /// (`PANGEA_RECONCILE_WORKERS`).
    pub workers: usize,
    /// Per-workspace admission budget (`PANGEA_BUDGET_PER_WORKSPACE`).
    pub budget_per_workspace: usize,
    /// Global admission budget (`PANGEA_BUDGET_GLOBAL`).
    pub budget_global: usize,
    /// How long shutdown waits for in-flight expensive phases (Compiling /
    /// Planning / Applying) to finish before controllers are aborted
    /// (`PANGEA_DRAIN_MAX_WAIT_SECS`).
    ///
    /// Lives here rather than in its own section because it bounds exactly
    /// the work `budget_per_workspace`/`budget_global` admit — the drain
    /// waits on that same permit count.
    ///
    /// The real ceiling is the pod's `terminationGracePeriodSeconds`: past
    /// that the kubelet sends SIGKILL and the post-drain work (pool close,
    /// final status write) is lost. The chart renders both from one value so
    /// they cannot drift; the default here is sized to be safe under the
    /// Kubernetes default grace period of 30s with reserve to spare, so an
    /// operator deployed without the chart is still correct, just impatient.
    pub drain_max_wait_secs: u64,
}

/// Tracing / OpenTelemetry.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObservabilityConfig {
    /// `pretty` (default) or `json` (`LOG_FORMAT`).
    pub log_format: String,
    /// OTLP gRPC collector endpoint (`OTEL_EXPORTER_OTLP_ENDPOINT`).
    #[serde(default)]
    pub otel_endpoint: Option<String>,
    /// OTLP service.name resource attribute (`OTEL_SERVICE_NAME`).
    pub otel_service_name: String,
    #[serde(default)]
    pub leaf_series: pangea_leaf::series::SeriesMode,
    #[serde(default = "default_leaf_series_max")]
    pub leaf_series_max: usize,
}

fn default_leaf_series_max() -> usize {
    pangea_leaf::series::DEFAULT_SERIES_MAX
}

/// Escalation routing (ntfy).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RoutingConfig {
    /// ntfy base URL (`PANGEA_NTFY_BASE_URL`).
    pub ntfy_base_url: String,
}

/// GraphQL API surface (non-secret).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ApiConfig {
    /// Whether the GraphQL playground is served (`PANGEA_ENABLE_PLAYGROUND`;
    /// default true).
    pub enable_playground: bool,
}

/// Event-driven reconcile triggers.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReactiveConfig {
    /// Treat a change of the `pangea.pleme.io/reconcile-requested` annotation
    /// as a reconcile trigger, as a spec change is (`PANGEA_HONOR_RECONCILE_REQUESTS`;
    /// default false). Off, the watch stream drops metadata-only edits, so the
    /// GraphQL `requestReconcile` mutation and `kubectl annotate` are inert
    /// and a settled template waits for its `refreshInterval` tick.
    #[serde(default)]
    pub honor_reconcile_requests: bool,
    /// A pass whose own heartbeat lands again withdraws the watchdog's
    /// `heartbeatLost` stall and restores its Planning/Applying pass
    /// (`PANGEA_RESUME_STALLED_ON_HEARTBEAT`; default true). Off, the stall
    /// stands until a new pass begins.
    #[serde(default = "default_on")]
    pub resume_stalled_on_heartbeat: bool,
    /// The watchdog's re-dispatch of a stalled template first aborts the
    /// in-process reconcile still holding it, releasing its budget slot,
    /// heartbeat task and kube-runtime slot, so the nudge starts a fresh pass
    /// (`PANGEA_PREEMPT_STALLED_PASS`; default true). A pass whose own beat
    /// landed within the heartbeat deadline is never preempted. Off, the nudge
    /// waits behind the in-flight reconcile.
    #[serde(default = "default_on")]
    pub preempt_stalled_pass: bool,
    /// The first reconcile of a template in a new incarnation adopts a pass a
    /// previous incarnation left Planning, Applying or orphaned, recording it
    /// Pending under this incarnation as the reconcile restarts it
    /// (`PANGEA_ADOPT_ORPHANED_PASS`; default true). Off, the watchdog names it
    /// `orphanedPhase` after the orphan grace and re-dispatches.
    #[serde(default = "default_on")]
    pub adopt_orphaned_pass: bool,
}

fn default_on() -> bool {
    true
}

impl Default for ReactiveConfig {
    fn default() -> Self {
        Self {
            honor_reconcile_requests: false,
            resume_stalled_on_heartbeat: true,
            preempt_stalled_pass: true,
            adopt_orphaned_pass: true,
        }
    }
}

/// Leaf-by-default knobs. Each one configures a behavior off; none deletes it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LeafConfig {
    /// `enforce` (default) refuses a never-reconciled fat template unless it
    /// declares `pangea.pleme.io/skip-leaf`; `off` admits it. A template that
    /// already reconciled is never refused (`PANGEA_LEAF_ADMISSION`).
    #[serde(default)]
    pub admission: pangea_leaf::admission::AdmissionMode,
    /// How far (percent) the planning-phase p95 may rise above its floor before
    /// the ratchet reports a regression (`PANGEA_LEAF_RATCHET_TOLERANCE_PERCENT`;
    /// default 10).
    #[serde(default = "default_ratchet_tolerance_percent")]
    pub ratchet_tolerance_percent: u32,
    /// A template whose planning-phase p95 exceeds this many seconds is fat even
    /// with one leaf key set (`PANGEA_LEAF_CYCLE_BUDGET_SECS`; default unset).
    #[serde(default)]
    pub cycle_budget_secs: Option<u64>,
    /// When a catalog names its fat parent (`pangea.pleme.io/migrate-from`),
    /// move the parent's state into the declared leaves in one Postgres
    /// transaction, gated by the bijection (`PANGEA_LEAF_MIGRATE`; default
    /// true). `false` runs nothing and leaves the fat row untouched.
    #[serde(default = "default_leaf_migrate")]
    pub migrate: bool,
    #[serde(default = "default_leaf_adopt")]
    pub adopt: bool,
    #[serde(default)]
    pub priority: pangea_leaf::admit::PriorityMode,
    #[serde(default = "default_per_credential_cap")]
    pub per_credential_cap: usize,
    #[serde(default)]
    pub drift_share: pangea_leaf::admit::Share,
    #[serde(default)]
    pub queue: LeafQueueKind,
    #[serde(default)]
    pub nats: LeafNatsConfig,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LeafQueueKind {
    #[default]
    InProcess,
    Jetstream,
}

impl LeafQueueKind {
    #[must_use]
    pub fn parse(raw: &str) -> Option<Self> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "in_process" | "inprocess" => Some(Self::InProcess),
            "jetstream" => Some(Self::Jetstream),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LeafNatsConfig {
    #[serde(default)]
    pub url: Option<String>,
    #[serde(default = "default_ack_wait_secs")]
    pub ack_wait_secs: u64,
    #[serde(default = "default_max_deliver")]
    pub max_deliver: u64,
    #[serde(default = "default_duplicate_window_secs")]
    pub duplicate_window_secs: u64,
}

fn default_ack_wait_secs() -> u64 {
    60
}

fn default_max_deliver() -> u64 {
    5
}

fn default_duplicate_window_secs() -> u64 {
    600
}

impl Default for LeafNatsConfig {
    fn default() -> Self {
        Self {
            url: None,
            ack_wait_secs: default_ack_wait_secs(),
            max_deliver: default_max_deliver(),
            duplicate_window_secs: default_duplicate_window_secs(),
        }
    }
}

fn default_per_credential_cap() -> usize {
    8
}

fn default_leaf_adopt() -> bool {
    true
}

fn default_leaf_migrate() -> bool {
    true
}

fn default_ratchet_tolerance_percent() -> u32 {
    pangea_leaf::ratchet::DEFAULT_TOLERANCE_PERCENT
}

impl Default for LeafConfig {
    fn default() -> Self {
        Self {
            admission: pangea_leaf::admission::AdmissionMode::default(),
            ratchet_tolerance_percent: default_ratchet_tolerance_percent(),
            cycle_budget_secs: None,
            migrate: default_leaf_migrate(),
            adopt: default_leaf_adopt(),
            priority: pangea_leaf::admit::PriorityMode::default(),
            per_credential_cap: default_per_credential_cap(),
            drift_share: pangea_leaf::admit::Share::default(),
            queue: LeafQueueKind::default(),
            nats: LeafNatsConfig::default(),
        }
    }
}

/// Embedded-Ruby gem cache (non-secret).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GemConfig {
    /// Per-CR git-clone cache dir (`PANGEA_GEM_CACHE_DIR`;
    /// default `/var/pangea/gems`).
    pub cache_dir: String,
}

// ── env helpers — read one bespoke env var with legacy-exact semantics ──

/// A present, non-empty env var (the legacy `.ok().filter(|s| !s.is_empty())`
/// shape used for identity + lease name).
fn env_nonempty(key: &str) -> Option<String> {
    std::env::var(key).ok().filter(|s| !s.is_empty())
}

/// A present env var, empty or not (the legacy `.ok()` shape used for addrs +
/// binaries + `PANGEA_EXECUTOR`).
fn env_present(key: &str) -> Option<String> {
    std::env::var(key).ok()
}

/// Parse a positive integer env var, keeping only values `> 0` — the legacy
/// `env_secs` shape (`parse().ok().filter(|n| *n > 0)`).
fn env_pos_u64(key: &str) -> Option<u64> {
    std::env::var(key)
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .filter(|n| *n > 0)
}

/// Parse an integer env var (`parse().ok()`), any value.
fn env_parse<T: std::str::FromStr>(key: &str) -> Option<T> {
    std::env::var(key).ok().and_then(|s| s.parse::<T>().ok())
}

/// Detect the pod identity exactly as `leader::LeaderConfig::from_env` does:
/// `POD_NAMESPACE` (non-empty) else `pangea-system`; `POD_NAME` else
/// `HOSTNAME` else `pangea-operator-<pid>`.
fn detect_identity() -> IdentityConfig {
    let namespace = env_nonempty("POD_NAMESPACE").unwrap_or_else(|| "pangea-system".to_string());
    let pod_name = env_nonempty("POD_NAME")
        .or_else(|| env_nonempty("HOSTNAME"))
        .unwrap_or_else(|| {
            // Avoid `format!` per ★★ TYPED EMISSION: assemble by push.
            let mut s = String::from("pangea-operator-");
            s.push_str(&std::process::id().to_string());
            s
        });
    IdentityConfig {
        namespace,
        pod_name,
    }
}

/// The `LEADER_ELECTION` truthiness — on by default; `false|0|off|no`
/// disables. Byte-identical to `main::leader_election_enabled`.
fn detect_leader_election(raw: &str) -> bool {
    let v = raw.trim().to_ascii_lowercase();
    !(v == "false" || v == "0" || v == "off" || v == "no")
}

/// The `PANGEA_VERBOSE` truthiness (`1` or `true`), matching
/// `ExecutorConfig::from_env`.
fn detect_verbose(raw: &str) -> bool {
    raw == "1" || raw.to_lowercase() == "true"
}

// ── discovery layer — the discovered tier, wired declaratively ──

/// The pod-identity discovery layer. Contributes `{identity: {...}}` from the
/// downward-API env; every other section stays at `bare()`.
struct IdentityLayer;

impl DiscoveryLayer for IdentityLayer {
    fn name(&self) -> &'static str {
        "pangea-operator.identity"
    }

    fn discover(&self) -> Dict {
        #[derive(Serialize)]
        struct Wrap {
            identity: IdentityConfig,
        }
        to_dict(&Wrap {
            identity: detect_identity(),
        })
    }
}

/// Serialize any `T: Serialize` into a figment [`Dict`] — the exact mechanism
/// shikumi's own tier fold uses (`Serialized::defaults` → `extract::<Dict>`).
fn to_dict<T: Serialize>(value: &T) -> Dict {
    Figment::new()
        .merge(Serialized::defaults(value))
        .extract::<Dict>()
        .unwrap_or_default()
}

// ── env overlay — the Custom-tier operator override, byte-identical reads ──

// Each section is a struct-of-`Option` with `skip_serializing_if` so a present
// env var contributes its leaf and an absent one is omitted (falls to the
// prescribed default). Field names MUST match the config structs above so the
// dict keys merge into the right leaves.

#[derive(Serialize, Default)]
struct EnvOverlay {
    leader: LeaderOverlay,
    servers: ServersOverlay,
    executor: ExecutorOverlay,
    compiler: CompilerOverlay,
    database: DatabaseOverlay,
    reconcile: ReconcileOverlay,
    observability: ObservabilityOverlay,
    routing: RoutingOverlay,
    api: ApiOverlay,
    gem: GemOverlay,
    reactive: ReactiveOverlay,
    leaf: LeafOverlay,
    workers: WorkersOverlay,
}

#[derive(Serialize, Default)]
struct WorkersOverlay {
    autoscaling: AutoscalingOverlay,
}

#[derive(Serialize, Default)]
struct AutoscalingOverlay {
    #[serde(skip_serializing_if = "Option::is_none")]
    enabled: Option<bool>,
}

#[derive(Serialize, Default)]
struct LeaderOverlay {
    #[serde(skip_serializing_if = "Option::is_none")]
    election_enabled: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    lease_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    lease_duration_secs: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    renew_secs: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    retry_secs: Option<u64>,
}

#[derive(Serialize, Default)]
struct ServersOverlay {
    #[serde(skip_serializing_if = "Option::is_none")]
    health_addr: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    metrics_addr: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    graphql_addr: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    grpc_addr: Option<String>,
}

#[derive(Serialize, Default)]
struct ExecutorOverlay {
    #[serde(skip_serializing_if = "Option::is_none")]
    backend: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    forbid_tofu: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tofu_binary: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    packer_binary: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    workspace_base: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    timeout_secs: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    packer_timeout_secs: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    ruby_binary: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    verbose: Option<bool>,
}

#[derive(Serialize, Default)]
struct CompilerOverlay {
    #[serde(skip_serializing_if = "Option::is_none")]
    backend: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    endpoint: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    ruby_workers: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    lava_ruby: Option<bool>,
}

#[derive(Serialize, Default)]
struct DatabaseOverlay {
    #[serde(skip_serializing_if = "Option::is_none")]
    host: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    port: Option<u16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    user: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    database: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pool_max_connections: Option<u32>,
}

#[derive(Serialize, Default)]
struct ReconcileOverlay {
    #[serde(skip_serializing_if = "Option::is_none")]
    workers: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    budget_per_workspace: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    budget_global: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    drain_max_wait_secs: Option<u64>,
}

#[derive(Serialize, Default)]
struct ObservabilityOverlay {
    #[serde(skip_serializing_if = "Option::is_none")]
    log_format: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    otel_endpoint: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    otel_service_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    leaf_series: Option<pangea_leaf::series::SeriesMode>,
    #[serde(skip_serializing_if = "Option::is_none")]
    leaf_series_max: Option<usize>,
}

#[derive(Serialize, Default)]
struct RoutingOverlay {
    #[serde(skip_serializing_if = "Option::is_none")]
    ntfy_base_url: Option<String>,
}

#[derive(Serialize, Default)]
struct ApiOverlay {
    #[serde(skip_serializing_if = "Option::is_none")]
    enable_playground: Option<bool>,
}

#[derive(Serialize, Default)]
struct ReactiveOverlay {
    #[serde(skip_serializing_if = "Option::is_none")]
    honor_reconcile_requests: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    resume_stalled_on_heartbeat: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    preempt_stalled_pass: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    adopt_orphaned_pass: Option<bool>,
}

#[derive(Serialize, Default)]
struct LeafOverlay {
    #[serde(skip_serializing_if = "Option::is_none")]
    admission: Option<pangea_leaf::admission::AdmissionMode>,
    #[serde(skip_serializing_if = "Option::is_none")]
    ratchet_tolerance_percent: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    cycle_budget_secs: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    migrate: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    adopt: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    priority: Option<pangea_leaf::admit::PriorityMode>,
    #[serde(skip_serializing_if = "Option::is_none")]
    per_credential_cap: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    drift_share: Option<pangea_leaf::admit::Share>,
    #[serde(skip_serializing_if = "Option::is_none")]
    queue: Option<LeafQueueKind>,
    #[serde(skip_serializing_if = "NatsOverlay::is_empty")]
    nats: NatsOverlay,
}

#[derive(Serialize, Default)]
struct NatsOverlay {
    #[serde(skip_serializing_if = "Option::is_none")]
    url: Option<String>,
}

impl NatsOverlay {
    fn is_empty(&self) -> bool {
        self.url.is_none()
    }
}

#[derive(Serialize, Default)]
struct GemOverlay {
    #[serde(skip_serializing_if = "Option::is_none")]
    cache_dir: Option<String>,
}

impl EnvOverlay {
    /// Read every bespoke env var with byte-identical parse + validity
    /// filtering to the legacy sites. Absent (or present-but-invalid, where
    /// the legacy code fell back) ⇒ `None` ⇒ the leaf falls to the prescribed
    /// default. `POD_*` identity vars are NOT here — they are the discovered
    /// tier ([`IdentityLayer`]).
    fn from_process_env() -> Self {
        Self {
            leader: LeaderOverlay {
                election_enabled: env_present("LEADER_ELECTION")
                    .map(|v| detect_leader_election(&v)),
                lease_name: env_nonempty("LEADER_LEASE_NAME"),
                lease_duration_secs: env_pos_u64("LEADER_LEASE_DURATION_SECS"),
                renew_secs: env_pos_u64("LEADER_RENEW_SECS"),
                retry_secs: env_pos_u64("LEADER_RETRY_SECS"),
            },
            servers: ServersOverlay {
                health_addr: env_present("HEALTH_ADDR"),
                metrics_addr: env_present("METRICS_ADDR"),
                graphql_addr: env_present("GRAPHQL_ADDR"),
                grpc_addr: env_present("GRPC_ADDR"),
            },
            executor: ExecutorOverlay {
                backend: env_present("PANGEA_EXECUTOR"),
                forbid_tofu: env_present("PANGEA_FORBID_TOFU").map(|v| {
                    crate::executor::backend_select::forbid_tofu_posture(Some(v.as_str()))
                }),
                tofu_binary: env_present("TOFU_BINARY"),
                packer_binary: env_present("PACKER_BINARY"),
                workspace_base: env_present("PANGEA_WORKSPACE_BASE"),
                timeout_secs: env_parse::<u64>("PANGEA_TIMEOUT"),
                packer_timeout_secs: env_parse::<u64>("PACKER_TIMEOUT"),
                ruby_binary: env_present("RUBY_BINARY"),
                verbose: env_present("PANGEA_VERBOSE").map(|v| detect_verbose(&v)),
            },
            compiler: CompilerOverlay {
                backend: env_present("PANGEA_COMPILER_BACKEND"),
                endpoint: env_present("COMPILER_ENDPOINT"),
                ruby_workers: env_parse::<usize>("PANGEA_RUBY_WORKERS"),
                lava_ruby: env_present("PANGEA_LAVA_RUBY")
                    .map(|v| crate::ruby::parse_lava_ruby(Some(v.as_str()))),
            },
            database: DatabaseOverlay {
                host: env_present("PGHOST"),
                port: env_parse::<u16>("PGPORT"),
                user: env_present("PGUSER"),
                database: env_present("PGDATABASE"),
                pool_max_connections: env_parse::<u32>("PANGEA_DB_POOL_MAX"),
            },
            reconcile: ReconcileOverlay {
                // The clamp matches `reconcile_workers_from_env`.
                workers: env_parse::<usize>("PANGEA_RECONCILE_WORKERS").map(|n| n.clamp(1, 32)),
                budget_per_workspace: env_parse::<usize>("PANGEA_BUDGET_PER_WORKSPACE"),
                budget_global: env_parse::<usize>("PANGEA_BUDGET_GLOBAL"),
                drain_max_wait_secs: env_parse::<u64>("PANGEA_DRAIN_MAX_WAIT_SECS"),
            },
            observability: ObservabilityOverlay {
                log_format: env_present("LOG_FORMAT"),
                otel_endpoint: env_present("OTEL_EXPORTER_OTLP_ENDPOINT"),
                otel_service_name: env_present("OTEL_SERVICE_NAME"),
                leaf_series: env_present("PANGEA_LEAF_SERIES")
                    .and_then(|v| pangea_leaf::series::SeriesMode::parse(&v)),
                leaf_series_max: env_parse::<usize>("PANGEA_LEAF_SERIES_MAX"),
            },
            routing: RoutingOverlay {
                ntfy_base_url: env_present("PANGEA_NTFY_BASE_URL"),
            },
            api: ApiOverlay {
                enable_playground: env_present("PANGEA_ENABLE_PLAYGROUND")
                    .map(|v| v == "true" || v == "1"),
            },
            reactive: ReactiveOverlay {
                honor_reconcile_requests: env_present("PANGEA_HONOR_RECONCILE_REQUESTS")
                    .map(|v| v == "true" || v == "1"),
                resume_stalled_on_heartbeat: env_present("PANGEA_RESUME_STALLED_ON_HEARTBEAT")
                    .map(|v| v == "true" || v == "1"),
                preempt_stalled_pass: env_present("PANGEA_PREEMPT_STALLED_PASS")
                    .map(|v| v == "true" || v == "1"),
                adopt_orphaned_pass: env_present("PANGEA_ADOPT_ORPHANED_PASS")
                    .map(|v| v == "true" || v == "1"),
            },
            gem: GemOverlay {
                cache_dir: env_present("PANGEA_GEM_CACHE_DIR"),
            },
            workers: WorkersOverlay {
                autoscaling: AutoscalingOverlay {
                    enabled: env_present("PANGEA_WORKERS_AUTOSCALING")
                        .map(|v| crate::executor::backend_select::parse_truthy(Some(v.as_str()))),
                },
            },
            leaf: LeafOverlay {
                admission: env_present("PANGEA_LEAF_ADMISSION")
                    .and_then(|v| pangea_leaf::admission::AdmissionMode::parse(&v)),
                ratchet_tolerance_percent: env_parse::<u32>(
                    "PANGEA_LEAF_RATCHET_TOLERANCE_PERCENT",
                ),
                cycle_budget_secs: env_parse::<u64>("PANGEA_LEAF_CYCLE_BUDGET_SECS"),
                migrate: env_parse::<bool>("PANGEA_LEAF_MIGRATE"),
                adopt: env_parse::<bool>("PANGEA_LEAF_ADOPT"),
                priority: env_present("PANGEA_LEAF_PRIORITY")
                    .and_then(|v| pangea_leaf::admit::PriorityMode::parse(&v)),
                per_credential_cap: env_parse::<usize>("PANGEA_LEAF_PER_CREDENTIAL_CAP"),
                drift_share: env_present("PANGEA_LEAF_DRIFT_SHARE")
                    .and_then(|v| pangea_leaf::admit::Share::parse(&v)),
                queue: env_present("PANGEA_LEAF_QUEUE").and_then(|v| LeafQueueKind::parse(&v)),
                nats: NatsOverlay {
                    url: env_nonempty("PANGEA_LEAF_NATS_URL"),
                },
            },
        }
    }
}

pub const DEFAULT_CONFIG_FILE: &str = "/etc/pangea/config.yaml";
pub const CONFIG_FILE_ENV: &str = "PANGEA_CONFIG_FILE";

#[must_use]
pub fn config_file_path() -> PathBuf {
    PathBuf::from(env_nonempty(CONFIG_FILE_ENV).unwrap_or_else(|| DEFAULT_CONFIG_FILE.to_string()))
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "lowercase")]
pub enum FileTier {
    Absent { path: PathBuf },
    Loaded { path: PathBuf },
    Refused { path: PathBuf, reason: String },
}

pub struct ResolvedOperatorConfig {
    pub resolution: ProgressiveResolution<OperatorConfig>,
    pub file: FileTier,
}

// ── TieredConfig — the sealed progressive fold ──

impl TieredConfig for OperatorConfig {
    fn bare() -> Self {
        Self {
            identity: IdentityConfig {
                namespace: String::new(),
                pod_name: String::new(),
            },
            leader: LeaderElectionConfig {
                election_enabled: false,
                lease_name: String::new(),
                lease_duration_secs: 0,
                renew_secs: 0,
                retry_secs: 0,
            },
            servers: ServersConfig {
                health_addr: String::new(),
                metrics_addr: String::new(),
                graphql_addr: String::new(),
                grpc_addr: String::new(),
            },
            executor: ExecutorSurface {
                backend: None,
                forbid_tofu: false,
                tofu_binary: String::new(),
                packer_binary: String::new(),
                workspace_base: String::new(),
                timeout_secs: 0,
                packer_timeout_secs: 0,
                ruby_binary: None,
                verbose: false,
            },
            compiler: CompilerConfig {
                backend: String::new(),
                endpoint: String::new(),
                ruby_workers: 0,
                lava_ruby: false,
            },
            database: DatabaseConfig {
                host: String::new(),
                port: 0,
                user: String::new(),
                database: String::new(),
                pool_max_connections: 0,
            },
            reconcile: ReconcileConfig {
                workers: 0,
                budget_per_workspace: 0,
                budget_global: 0,
                drain_max_wait_secs: 0,
            },
            observability: ObservabilityConfig {
                log_format: String::new(),
                otel_endpoint: None,
                otel_service_name: String::new(),
                leaf_series: pangea_leaf::series::SeriesMode::default(),
                leaf_series_max: default_leaf_series_max(),
            },
            routing: RoutingConfig {
                ntfy_base_url: String::new(),
            },
            api: ApiConfig {
                enable_playground: false,
            },
            gem: GemConfig {
                cache_dir: String::new(),
            },
            network: crate::network::NetworkConfig::default(),
            reactive: ReactiveConfig {
                honor_reconcile_requests: false,
                resume_stalled_on_heartbeat: false,
                preempt_stalled_pass: false,
                adopt_orphaned_pass: false,
            },
            leaf: LeafConfig::default(),
            workers: WorkersConfig::default(),
        }
    }

    /// `bare()` + the pod-identity discovery layer. Every non-identity section
    /// stays at `bare()`; the prescribed defaults land in
    /// [`Self::prescribed_default`].
    fn discovered() -> Self {
        Self::discovered_from_layers(&[&IdentityLayer])
    }

    /// The prescribed defaults, built ON TOP of [`Self::discovered`] so a
    /// detected identity shows through (credited to `Discovered` by the
    /// last-changer fold). Values mirror the legacy per-site defaults exactly,
    /// except the magma-native `executor.backend`/`forbid_tofu` (per the
    /// ★★ MAGMA-NATIVE directive).
    fn prescribed_default() -> Self {
        let mut c = Self::discovered();
        c.leader = LeaderElectionConfig {
            election_enabled: true,
            lease_name: "pangea-operator-leader".to_string(),
            lease_duration_secs: 15,
            renew_secs: 5,
            retry_secs: 3,
        };
        c.servers = ServersConfig {
            health_addr: "0.0.0.0:8080".to_string(),
            metrics_addr: "0.0.0.0:9090".to_string(),
            graphql_addr: "0.0.0.0:8081".to_string(),
            grpc_addr: "0.0.0.0:50051".to_string(),
        };
        c.executor = ExecutorSurface {
            backend: Some("magma".to_string()),
            forbid_tofu: true,
            tofu_binary: "tofu".to_string(),
            packer_binary: "packer".to_string(),
            workspace_base: "/tmp/pangea-workspaces".to_string(),
            timeout_secs: 600,
            packer_timeout_secs: 2700,
            ruby_binary: None,
            verbose: false,
        };
        c.compiler = CompilerConfig {
            // ── ★ THE DEFAULT FOLLOWS THE FEATURE, AND MUST ──────────────
            // main.rs PANICS on `embedded` when embedded_ruby is not
            // compiled in — deliberately, rather than falling back to the
            // sunset HTTP sidecar. A fixed "embedded" here therefore made a
            // Ruby-free build crash on startup for every deployment that had
            // not already migrated, which is precisely why the Ruby-free
            // configuration could not become the default even though it
            // compiled clean and passed the suite.
            //
            // Selecting by cfg removes that coupling: a build that HAS the
            // interpreter still prescribes it, and a build that does not
            // prescribes lava, which is present in both. The illegal
            // combination — prescribing a backend this binary cannot
            // construct — now has no way to be written.
            backend: DEFAULT_COMPILER_BACKEND.to_string(),
            endpoint: "http://localhost:8082".to_string(),
            ruby_workers: 1,
            lava_ruby: crate::ruby::DEFAULT_LAVA_RUBY,
        };
        c.database = DatabaseConfig {
            host: "pangea-database-rw.pangea-system.svc.cluster.local".to_string(),
            port: 5432,
            user: "postgres".to_string(),
            database: "pangea_state".to_string(),
            // 5 was the hardcoded value before this became configurable.
            // Kept as the default so the change is inert until tuned.
            pool_max_connections: 5,
        };
        c.reconcile = ReconcileConfig {
            workers: 16,
            budget_per_workspace: 4,
            budget_global: 16,
            // Safe under the Kubernetes default terminationGracePeriodSeconds
            // of 30s, leaving 10s for the pool close and final status write.
            // The chart raises this and the grace period together.
            drain_max_wait_secs: 20,
        };
        c.observability = ObservabilityConfig {
            log_format: "pretty".to_string(),
            otel_endpoint: None,
            otel_service_name: "pangea-operator".to_string(),
            leaf_series: pangea_leaf::series::SeriesMode::default(),
            leaf_series_max: default_leaf_series_max(),
        };
        c.routing = RoutingConfig {
            ntfy_base_url: "https://ntfy.sh".to_string(),
        };
        c.api = ApiConfig {
            enable_playground: true,
        };
        c.gem = GemConfig {
            cache_dir: "/var/pangea/gems".to_string(),
        };
        c.reactive = ReactiveConfig::default();
        c
    }
}

impl OperatorConfig {
    /// Resolve the full config through the sealed progressive fold, overlaying
    /// the operator's bespoke env vars at the `Custom` tier. The one call site
    /// `main.rs` reaches for at startup.
    #[must_use]
    pub fn resolve() -> ProgressiveResolution<Self> {
        Self::resolve_reporting().resolution
    }

    #[must_use]
    pub fn resolve_reporting() -> ResolvedOperatorConfig {
        Self::resolve_from(
            &config_file_path(),
            to_dict(&EnvOverlay::from_process_env()),
        )
    }

    #[must_use]
    pub fn resolve_from(file: &Path, env: Dict) -> ResolvedOperatorConfig {
        let (file_layer, file_tier) = Self::file_overlay(file);
        let mut overlays: Vec<ProgressiveLayer> = file_layer.into_iter().collect();
        overlays.push(ProgressiveLayer::env("pangea-operator", env));
        ResolvedOperatorConfig {
            resolution: Self::resolve_progressive_with(&overlays),
            file: file_tier,
        }
    }

    fn file_overlay(path: &Path) -> (Option<ProgressiveLayer>, FileTier) {
        let shown = path.to_path_buf();
        if !path.exists() {
            return (None, FileTier::Absent { path: shown });
        }
        let layer = match ProgressiveLayer::try_from_file(path) {
            Ok(layer) => layer,
            Err(e) => {
                return (
                    None,
                    FileTier::Refused {
                        path: shown,
                        reason: e.to_string(),
                    },
                )
            }
        };
        let checked = Figment::new()
            .merge(Serialized::defaults(Self::prescribed_default()))
            .merge(Serialized::defaults(layer.dict()))
            .extract::<Self>();
        match checked {
            Ok(_) => (Some(layer), FileTier::Loaded { path: shown }),
            Err(e) => (
                None,
                FileTier::Refused {
                    path: shown,
                    reason: e.to_string(),
                },
            ),
        }
    }

    /// Emit the resolved provenance at startup — a real operability win: the
    /// answer to "why is the executor magma / the namespace X?" is a log line,
    /// not code archaeology. One `info` summary + per-leaf `debug` lines,
    /// through the typed [`shikumi::Provenance`] `Display` (no `format!`).
    pub fn log_provenance(provenance: &ProvenanceMap) {
        let tiers: Vec<&'static str> = provenance
            .contributing_tiers()
            .iter()
            .map(|t| t.as_str())
            .collect();
        tracing::info!(
            leaves = provenance.len(),
            contributing_tiers = ?tiers,
            "operator config resolved (progressive tiers)"
        );
        for (path, prov) in provenance.entries() {
            let field = path.join(".");
            tracing::debug!(field = %field, provenance = %prov, "config leaf provenance");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::executor::ExecutorConfig;
    use std::sync::Mutex;

    // env is process-global; serialize the env-mutating tests in this module.
    // The env vars touched here are NOT touched by other test files
    // (reconciler.rs uses PANGEA_RECONCILE_WORKERS; gem_cache.rs uses
    // PANGEA_GEM_AUTH_TOKEN) — the parity test reads a disjoint set — but the
    // guard keeps this module's own env tests from racing each other.
    static ENV_GUARD: Mutex<()> = Mutex::new(());

    /// The MIGRATED env vars whose reads `main.rs` now routes through the typed
    /// config. Cleared before each env test so a leaked value can't skew it.
    const MIGRATED_VARS: &[&str] = &[
        "HEALTH_ADDR",
        "METRICS_ADDR",
        "GRAPHQL_ADDR",
        "GRPC_ADDR",
        "TOFU_BINARY",
        "PACKER_BINARY",
        "PANGEA_WORKSPACE_BASE",
        "PANGEA_TIMEOUT",
        "PACKER_TIMEOUT",
        "RUBY_BINARY",
        "PANGEA_VERBOSE",
        "COMPILER_ENDPOINT",
        "PANGEA_COMPILER_BACKEND",
        "PANGEA_RUBY_WORKERS",
        "PANGEA_LAVA_RUBY",
        "PGHOST",
        "PGPORT",
        "PGUSER",
        "PGDATABASE",
    ];

    fn clear_migrated() {
        for k in MIGRATED_VARS {
            std::env::remove_var(k);
        }
    }

    #[test]
    fn prescribed_default_matches_legacy_per_site_defaults() {
        // No env dependence — pins the Default tier against the values the
        // scattered `from_env` sites hard-coded.
        let p = OperatorConfig::prescribed_default();
        assert_eq!(p.servers.health_addr, "0.0.0.0:8080");
        assert_eq!(p.servers.metrics_addr, "0.0.0.0:9090");
        assert_eq!(p.servers.graphql_addr, "0.0.0.0:8081");
        assert_eq!(p.servers.grpc_addr, "0.0.0.0:50051");
        assert!(p.leader.election_enabled);
        assert_eq!(p.leader.lease_name, "pangea-operator-leader");
        assert_eq!(p.leader.lease_duration_secs, 15);
        assert_eq!(p.leader.renew_secs, 5);
        assert_eq!(p.leader.retry_secs, 3);
        assert_eq!(p.executor.tofu_binary, "tofu");
        assert_eq!(p.executor.packer_binary, "packer");
        assert_eq!(p.executor.workspace_base, "/tmp/pangea-workspaces");
        assert_eq!(p.executor.timeout_secs, 600);
        assert_eq!(p.executor.packer_timeout_secs, 2700);
        assert_eq!(p.executor.ruby_binary, None);
        assert!(!p.executor.verbose);
        // Feature-dependent by construction — see DEFAULT_COMPILER_BACKEND.
        // Asserting the literal "embedded" here is what would fail a
        // Ruby-free build, and the value it would be asserting is one that
        // build cannot honour.
        assert_eq!(p.compiler.backend, DEFAULT_COMPILER_BACKEND);
        assert_eq!(p.compiler.backend, "lava");
        assert!(p.compiler.lava_ruby);
        assert_eq!(p.compiler.endpoint, "http://localhost:8082");
        assert_eq!(p.compiler.ruby_workers, 1);
        assert_eq!(
            p.database.host,
            "pangea-database-rw.pangea-system.svc.cluster.local"
        );
        assert_eq!(p.database.port, 5432);
        assert_eq!(p.database.user, "postgres");
        assert_eq!(p.database.database, "pangea_state");
        assert_eq!(p.reconcile.workers, 16);
        assert_eq!(p.reconcile.budget_per_workspace, 4);
        assert_eq!(p.reconcile.budget_global, 16);
        assert_eq!(p.observability.log_format, "pretty");
        assert_eq!(p.observability.otel_service_name, "pangea-operator");
        assert_eq!(p.routing.ntfy_base_url, "https://ntfy.sh");
        assert!(p.api.enable_playground);
        assert_eq!(p.gem.cache_dir, "/var/pangea/gems");
        assert!(
            !p.reactive.honor_reconcile_requests,
            "the prescribed default preserves the pre-reactive behavior"
        );
        assert!(p.reactive.resume_stalled_on_heartbeat);
        assert!(p.reactive.preempt_stalled_pass);
        assert!(p.reactive.adopt_orphaned_pass);
        assert_eq!(
            p.leaf.admission,
            pangea_leaf::admission::AdmissionMode::Enforce
        );
        assert_eq!(p.leaf.ratchet_tolerance_percent, 10);
        assert_eq!(p.leaf.cycle_budget_secs, None);
    }

    #[test]
    fn magma_native_default_is_prescribed() {
        // Per ★★ MAGMA-NATIVE: the shipped default names magma + forbids tofu.
        let p = OperatorConfig::prescribed_default();
        assert_eq!(p.executor.backend.as_deref(), Some("magma"));
        assert!(p.executor.forbid_tofu);
    }

    #[test]
    fn the_prescribed_compiler_backend_shells_out_for_nothing() {
        // The shipped default must be the backend that spawns no processes and
        // links no interpreter. This is pinned as a TEST because the executor's
        // equivalent default drifted in the docs for months while the code was
        // right — a default nobody exercises deliberately is exactly the kind
        // that rots unnoticed.
        let p = OperatorConfig::prescribed_default();
        assert_eq!(p.compiler.backend, "lava");
        assert_eq!(DEFAULT_COMPILER_BACKEND, "lava");
    }

    #[test]
    fn bare_is_zero_opinion_floor() {
        let b = OperatorConfig::bare();
        assert_eq!(b.servers.health_addr, "");
        assert_eq!(b.executor.timeout_secs, 0);
        assert_eq!(b.executor.backend, None);
        assert!(!b.executor.forbid_tofu);
        assert!(!b.leader.election_enabled);
        assert_eq!(b.database.port, 0);
        assert!(!b.reactive.resume_stalled_on_heartbeat);
        assert!(!b.reactive.preempt_stalled_pass);
        assert!(!b.reactive.adopt_orphaned_pass);
    }

    #[test]
    fn discovered_resolves_pod_identity_from_env() {
        let _g = ENV_GUARD.lock().unwrap();
        std::env::set_var("POD_NAMESPACE", "rio-system");
        std::env::set_var("POD_NAME", "pangea-operator-abc123");
        let d = OperatorConfig::discovered();
        assert_eq!(d.identity.namespace, "rio-system");
        assert_eq!(d.identity.pod_name, "pangea-operator-abc123");
        // Non-identity sections stay bare in the discovered tier.
        assert_eq!(d.servers.health_addr, "");
        std::env::remove_var("POD_NAMESPACE");
        std::env::remove_var("POD_NAME");
    }

    #[test]
    fn discovered_falls_back_when_pod_env_absent() {
        let _g = ENV_GUARD.lock().unwrap();
        std::env::remove_var("POD_NAMESPACE");
        std::env::remove_var("POD_NAME");
        std::env::remove_var("HOSTNAME");
        let d = OperatorConfig::discovered();
        assert_eq!(d.identity.namespace, "pangea-system");
        assert!(d.identity.pod_name.starts_with("pangea-operator-"));
    }

    #[test]
    fn provenance_is_complete_and_credits_the_right_tier() {
        let _g = ENV_GUARD.lock().unwrap();
        clear_migrated();
        std::env::remove_var("POD_NAMESPACE");
        std::env::remove_var("POD_NAME");
        std::env::remove_var("HOSTNAME");
        std::env::set_var("HEALTH_ADDR", "0.0.0.0:19080");
        let resolved = OperatorConfig::resolve();
        let prov = resolved.provenance();

        // Every non-null leaf carries a provenance entry (33 = 36 total leaves
        // minus the 3 `None` optionals whose null may not seed a leaf).
        assert!(
            prov.len() >= 33,
            "provenance incomplete: {} leaves",
            prov.len()
        );

        // The env overlay (Custom tier) won HEALTH_ADDR.
        let health = prov.provenance_of(&["servers", "health_addr"]).unwrap();
        assert_eq!(health.tier(), shikumi::ConfigTierKind::Custom);
        assert_eq!(resolved.value().servers.health_addr, "0.0.0.0:19080");

        // The magma-native executor default came from the Default tier.
        let backend = prov.provenance_of(&["executor", "backend"]).unwrap();
        assert_eq!(backend.tier(), shikumi::ConfigTierKind::Default);
        let forbid = prov.provenance_of(&["executor", "forbid_tofu"]).unwrap();
        assert_eq!(forbid.tier(), shikumi::ConfigTierKind::Default);

        // The pod namespace fallback is credited to the Discovered tier.
        let ns = prov.provenance_of(&["identity", "namespace"]).unwrap();
        assert_eq!(ns.tier(), shikumi::ConfigTierKind::Discovered);
        assert_eq!(resolved.value().identity.namespace, "pangea-system");

        std::env::remove_var("HEALTH_ADDR");
    }

    #[test]
    fn boot_parity_executor_config_all_defaults() {
        // With no migrated env set, the typed ExecutorConfig equals the legacy
        // ExecutorConfig::from_env().
        let _g = ENV_GUARD.lock().unwrap();
        clear_migrated();
        let via_typed = ExecutorConfig::from_operator_config(OperatorConfig::resolve().value());
        let via_legacy = ExecutorConfig::from_env();
        assert_eq!(via_typed, via_legacy);
    }

    #[test]
    fn boot_parity_executor_config_with_env_overrides() {
        let _g = ENV_GUARD.lock().unwrap();
        clear_migrated();
        std::env::set_var("TOFU_BINARY", "/opt/tofu");
        std::env::set_var("PANGEA_WORKSPACE_BASE", "/data/ws");
        std::env::set_var("PANGEA_TIMEOUT", "1234");
        std::env::set_var("PANGEA_VERBOSE", "true");
        std::env::set_var("RUBY_BINARY", "/usr/bin/ruby");

        let via_typed = ExecutorConfig::from_operator_config(OperatorConfig::resolve().value());
        let via_legacy = ExecutorConfig::from_env();
        assert_eq!(via_typed, via_legacy);
        // And spot-check the effective values.
        assert_eq!(via_typed.workspace_base.to_string_lossy(), "/data/ws");
        assert_eq!(via_typed.timeout_secs, 1234);
        assert!(via_typed.verbose);

        clear_migrated();
    }

    #[test]
    fn boot_parity_invalid_number_falls_back_to_default() {
        // A present-but-unparseable PANGEA_TIMEOUT falls back to 600 in BOTH
        // the legacy read and the typed overlay (the overlay omits the leaf).
        let _g = ENV_GUARD.lock().unwrap();
        clear_migrated();
        std::env::set_var("PANGEA_TIMEOUT", "not-a-number");
        let via_typed = ExecutorConfig::from_operator_config(OperatorConfig::resolve().value());
        let via_legacy = ExecutorConfig::from_env();
        assert_eq!(via_typed.timeout_secs, 600);
        assert_eq!(via_typed, via_legacy);
        clear_migrated();
    }

    #[test]
    fn boot_parity_server_addrs_and_db_and_compiler() {
        let _g = ENV_GUARD.lock().unwrap();
        clear_migrated();
        std::env::set_var("GRAPHQL_ADDR", "0.0.0.0:18081");
        std::env::set_var("PGHOST", "db.internal");
        std::env::set_var("PGPORT", "6543");
        std::env::set_var("PANGEA_COMPILER_BACKEND", "http");
        std::env::set_var("PANGEA_RUBY_WORKERS", "8");

        let cfg = OperatorConfig::resolve().into_value();

        // Server addr: env wins, else prescribed default (matches
        // `env::var("X").unwrap_or(default)`).
        assert_eq!(cfg.servers.graphql_addr, "0.0.0.0:18081");
        assert_eq!(cfg.servers.health_addr, "0.0.0.0:8080");
        // DB: env wins, non-secret coordinates.
        assert_eq!(cfg.database.host, "db.internal");
        assert_eq!(cfg.database.port, 6543);
        assert_eq!(cfg.database.user, "postgres");
        // Compiler.
        assert_eq!(cfg.compiler.backend, "http");
        assert_eq!(cfg.compiler.ruby_workers, 8);
        assert_eq!(cfg.compiler.endpoint, "http://localhost:8082");
        assert!(cfg.compiler.lava_ruby);

        std::env::set_var("PANGEA_LAVA_RUBY", "off");
        assert!(!OperatorConfig::resolve().into_value().compiler.lava_ruby);
        std::env::set_var("PANGEA_LAVA_RUBY", "on");
        assert!(OperatorConfig::resolve().into_value().compiler.lava_ruby);

        clear_migrated();
    }

    fn scratch_file(tag: &str, body: &str) -> PathBuf {
        let mut p = std::env::temp_dir();
        p.push(
            [
                "pangea-operator-config-",
                tag,
                "-",
                &std::process::id().to_string(),
                ".yaml",
            ]
            .concat(),
        );
        std::fs::write(&p, body).unwrap();
        p
    }

    fn env_dict(pairs: &[(&str, &str, &str)]) -> Dict {
        let mut root = Dict::new();
        for (section, key, value) in pairs {
            let mut leaf = Dict::new();
            leaf.insert((*key).to_string(), figment::value::Value::from(*value));
            root.insert((*section).to_string(), figment::value::Value::from(leaf));
        }
        root
    }

    #[test]
    fn absent_file_is_reported_and_changes_nothing() {
        let _g = ENV_GUARD.lock().unwrap();
        let path = std::env::temp_dir().join("pangea-operator-config-does-not-exist.yaml");
        let r = OperatorConfig::resolve_from(&path, Dict::new());
        assert_eq!(r.file, FileTier::Absent { path: path.clone() });
        assert_eq!(
            r.resolution.into_value(),
            OperatorConfig::resolve_progressive().into_value()
        );
    }

    #[test]
    fn file_tier_beats_default_and_env_beats_file() {
        let _g = ENV_GUARD.lock().unwrap();
        let path = scratch_file(
            "precedence",
            "servers:\n  health_addr: 0.0.0.0:1111\n  grpc_addr: 0.0.0.0:2222\nrouting:\n  ntfy_base_url: https://ntfy.pleme.internal\n",
        );
        let r = OperatorConfig::resolve_from(
            &path,
            env_dict(&[("servers", "grpc_addr", "0.0.0.0:3333")]),
        );
        assert_eq!(r.file, FileTier::Loaded { path: path.clone() });
        let prov = r.resolution.provenance().clone();
        let v = r.resolution.into_value();
        assert_eq!(v.servers.health_addr, "0.0.0.0:1111");
        assert_eq!(v.servers.grpc_addr, "0.0.0.0:3333");
        assert_eq!(v.routing.ntfy_base_url, "https://ntfy.pleme.internal");
        assert_eq!(v.servers.metrics_addr, "0.0.0.0:9090");
        let health = prov.provenance_of(&["servers", "health_addr"]).unwrap();
        assert_eq!(health.tier(), shikumi::ConfigTierKind::Custom);
        assert_eq!(health, &shikumi::Provenance::file(path.clone()));
        let grpc = prov.provenance_of(&["servers", "grpc_addr"]).unwrap();
        assert_eq!(grpc, &shikumi::Provenance::env("pangea-operator"));
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn malformed_file_is_refused_and_env_still_applies() {
        let _g = ENV_GUARD.lock().unwrap();
        for (tag, body) in [
            (
                "unknown-key",
                "servers:\n  health_addr: x\nnot_a_section: 1\n",
            ),
            ("bad-type", "database:\n  port: not-a-port\n"),
            ("not-yaml", "servers: [unclosed\n"),
        ] {
            let path = scratch_file(tag, body);
            let r = OperatorConfig::resolve_from(
                &path,
                env_dict(&[("servers", "graphql_addr", "0.0.0.0:4444")]),
            );
            assert!(
                matches!(r.file, FileTier::Refused { .. }),
                "{tag}: {:?}",
                r.file
            );
            let v = r.resolution.into_value();
            assert_eq!(v.servers.graphql_addr, "0.0.0.0:4444", "{tag}");
            assert_eq!(v.servers.health_addr, "0.0.0.0:8080", "{tag}");
            std::fs::remove_file(path).unwrap();
        }
    }

    #[test]
    fn reactive_section_is_read_from_the_file_tier() {
        let _g = ENV_GUARD.lock().unwrap();
        let path = scratch_file("reactive", "reactive:\n  honor_reconcile_requests: true\n");
        let r = OperatorConfig::resolve_from(&path, Dict::new());
        assert_eq!(r.file, FileTier::Loaded { path: path.clone() });
        assert!(
            r.resolution.into_value().reactive.honor_reconcile_requests,
            "the chart's config.file.data reaches the operator through the file tier"
        );
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn the_stall_switches_are_on_without_a_file_and_off_through_the_file_tier() {
        let _g = ENV_GUARD.lock().unwrap();
        let partial = scratch_file(
            "reactive-partial",
            "reactive:\n  honor_reconcile_requests: true\n",
        );
        let on = OperatorConfig::resolve_from(&partial, Dict::new())
            .resolution
            .into_value()
            .reactive;
        assert!(
            on.resume_stalled_on_heartbeat && on.preempt_stalled_pass && on.adopt_orphaned_pass
        );
        std::fs::remove_file(partial).unwrap();
        let path = scratch_file(
            "reactive-off",
            "reactive:\n  resume_stalled_on_heartbeat: false\n  preempt_stalled_pass: false\n  adopt_orphaned_pass: false\n",
        );
        let off = OperatorConfig::resolve_from(&path, Dict::new())
            .resolution
            .into_value()
            .reactive;
        assert!(!off.resume_stalled_on_heartbeat);
        assert!(!off.preempt_stalled_pass);
        assert!(!off.adopt_orphaned_pass);
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn leaf_admission_is_read_from_the_file_tier() {
        let _g = ENV_GUARD.lock().unwrap();
        let path = scratch_file("leaf", "leaf:\n  admission: off\n");
        let r = OperatorConfig::resolve_from(&path, Dict::new());
        assert_eq!(r.file, FileTier::Loaded { path: path.clone() });
        assert_eq!(
            r.resolution.into_value().leaf.admission,
            pangea_leaf::admission::AdmissionMode::Off
        );
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn leaf_admission_generate_is_read_from_the_file_tier() {
        let _g = ENV_GUARD.lock().unwrap();
        let path = scratch_file("leaf-generate", "leaf:\n  admission: generate\n");
        let r = OperatorConfig::resolve_from(&path, Dict::new());
        assert_eq!(r.file, FileTier::Loaded { path: path.clone() });
        assert_eq!(
            r.resolution.into_value().leaf.admission,
            pangea_leaf::admission::AdmissionMode::Generate
        );
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn leaf_ratchet_and_budget_are_read_from_the_file_tier() {
        let _g = ENV_GUARD.lock().unwrap();
        let path = scratch_file(
            "leaf-ratchet",
            "leaf:\n  ratchet_tolerance_percent: 0\n  cycle_budget_secs: 600\n",
        );
        let r = OperatorConfig::resolve_from(&path, Dict::new());
        let leaf = r.resolution.into_value().leaf;
        assert_eq!(leaf.ratchet_tolerance_percent, 0);
        assert_eq!(leaf.cycle_budget_secs, Some(600));
        assert_eq!(
            leaf.admission,
            pangea_leaf::admission::AdmissionMode::Enforce
        );
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn leaf_priority_caps_and_drift_share_are_read_from_the_file_tier() {
        let _g = ENV_GUARD.lock().unwrap();
        let defaults = OperatorConfig::prescribed_default().leaf;
        assert_eq!(defaults.priority, pangea_leaf::admit::PriorityMode::On);
        assert_eq!(defaults.per_credential_cap, 8);
        assert_eq!(defaults.drift_share, pangea_leaf::admit::Share::HALF);
        let path = scratch_file(
            "leaf-priority",
            "leaf:\n  priority: off\n  per_credential_cap: 0\n  drift_share: 0\n",
        );
        let r = OperatorConfig::resolve_from(&path, Dict::new());
        assert_eq!(r.file, FileTier::Loaded { path: path.clone() });
        let leaf = r.resolution.into_value().leaf;
        assert_eq!(leaf.priority, pangea_leaf::admit::PriorityMode::Off);
        assert_eq!(leaf.per_credential_cap, 0);
        assert_eq!(leaf.drift_share, pangea_leaf::admit::Share::NONE);
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn leaf_queue_and_nats_are_read_from_the_file_tier() {
        let _g = ENV_GUARD.lock().unwrap();
        let p = OperatorConfig::prescribed_default().leaf;
        assert_eq!(p.queue, LeafQueueKind::InProcess);
        assert_eq!(p.nats.url, None);
        let path = scratch_file(
            "leaf-queue",
            "leaf:\n  queue: jetstream\n  nats:\n    url: nats://nats:4222\n    ack_wait_secs: 30\n",
        );
        let r = OperatorConfig::resolve_from(&path, Dict::new());
        assert_eq!(r.file, FileTier::Loaded { path: path.clone() });
        let leaf = r.resolution.into_value().leaf;
        assert_eq!(leaf.queue, LeafQueueKind::Jetstream);
        assert_eq!(leaf.nats.url.as_deref(), Some("nats://nats:4222"));
        assert_eq!(leaf.nats.ack_wait_secs, 30);
        assert_eq!(leaf.nats.max_deliver, 5);
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn worker_autoscaling_and_weights_are_read_from_the_file_tier() {
        let _g = ENV_GUARD.lock().unwrap();
        let p = OperatorConfig::prescribed_default().workers;
        assert!(p.autoscaling.enabled);
        assert_eq!((p.autoscaling.min, p.autoscaling.max), (0, 4));
        assert_eq!(p.autoscaling.target, 600);
        assert_eq!(p.autoscaling.horizon_secs, 300);
        assert_eq!(p.weights.create, 3);
        let path = scratch_file(
            "workers",
            "workers:\n  autoscaling:\n    enabled: false\n  weights:\n    create: 4\n",
        );
        let r = OperatorConfig::resolve_from(&path, Dict::new());
        assert_eq!(r.file, FileTier::Loaded { path: path.clone() });
        let w = r.resolution.into_value().workers;
        assert!(!w.autoscaling.enabled);
        assert_eq!(w.weights.create, 4);
        assert_eq!(w.weights.replace, 5);
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn leaf_series_and_its_cap_are_read_from_the_file_tier() {
        let _g = ENV_GUARD.lock().unwrap();
        let p = OperatorConfig::prescribed_default().observability;
        assert_eq!(p.leaf_series, pangea_leaf::series::SeriesMode::Unhealthy);
        assert_eq!(p.leaf_series_max, 256);
        let path = scratch_file(
            "leaf-series",
            "observability:\n  leaf_series: off\n  leaf_series_max: 10\n",
        );
        let r = OperatorConfig::resolve_from(&path, Dict::new());
        assert_eq!(r.file, FileTier::Loaded { path: path.clone() });
        let o = r.resolution.into_value().observability;
        assert_eq!(o.leaf_series, pangea_leaf::series::SeriesMode::Off);
        assert_eq!(o.leaf_series_max, 10);
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn a_drift_share_outside_the_unit_interval_refuses_the_file() {
        let _g = ENV_GUARD.lock().unwrap();
        let path = scratch_file("leaf-share", "leaf:\n  drift_share: 1.5\n");
        let r = OperatorConfig::resolve_from(&path, Dict::new());
        assert!(matches!(r.file, FileTier::Refused { .. }));
        assert_eq!(
            r.resolution.into_value().leaf.drift_share,
            pangea_leaf::admit::Share::HALF
        );
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn leaf_admission_env_overrides_the_default() {
        let _g = ENV_GUARD.lock().unwrap();
        std::env::set_var("PANGEA_LEAF_ADMISSION", "off");
        let v = OperatorConfig::resolve().into_value();
        std::env::remove_var("PANGEA_LEAF_ADMISSION");
        assert_eq!(v.leaf.admission, pangea_leaf::admission::AdmissionMode::Off);
    }

    #[test]
    fn network_section_is_read_from_the_file_tier() {
        let _g = ENV_GUARD.lock().unwrap();
        let path = scratch_file(
            "network",
            concat!(
                "network:\n",
                "  fleet:\n    defaults: { posture: tailnet, port: 443 }\n",
                "  sites:\n    house:\n      defaults: { posture: lan }\n",
                "  nodes:\n    plo:\n      site: house\n      services:\n        ha: { port: 8123 }\n",
                "  services:\n",
                "    ha: { node: plo, names: [ha.pleme.internal], upstream: 'http://127.0.0.1:8123' }\n",
                "    grafana: { node: rio }\n",
                "  devices:\n    house:\n      plug: { kind: plug, mac: 'aa:bb:cc:dd:ee:ff', posture: lan }\n",
            ),
        );
        let r = OperatorConfig::resolve_from(&path, Dict::new());
        assert_eq!(r.file, FileTier::Loaded { path: path.clone() });
        let net = r.resolution.into_value().network.resolve();
        let ha = net.services["ha"].clone().unwrap();
        assert_eq!(ha.posture, crate::network::Posture::Lan);
        assert_eq!(ha.port, 8123);
        assert_eq!(ha.site.as_deref(), Some("house"));
        let grafana = net.services["grafana"].clone().unwrap();
        assert_eq!(grafana.posture, crate::network::Posture::Tailnet);
        assert_eq!(grafana.port, 443);
        assert!(net.devices[&("house".to_string(), "plug".to_string())].is_ok());
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn config_file_path_defaults_to_the_chart_mount() {
        let _g = ENV_GUARD.lock().unwrap();
        std::env::remove_var(CONFIG_FILE_ENV);
        assert_eq!(config_file_path(), PathBuf::from("/etc/pangea/config.yaml"));
        std::env::set_var(CONFIG_FILE_ENV, "/tmp/x.yaml");
        assert_eq!(config_file_path(), PathBuf::from("/tmp/x.yaml"));
        std::env::remove_var(CONFIG_FILE_ENV);
    }

    #[test]
    fn roundtrips_through_yaml() {
        // deny_unknown_fields + full-field serialize must roundtrip so a
        // `config-show`/file overlay is lossless.
        let p = OperatorConfig::prescribed_default();
        let y = serde_yaml::to_string(&p).unwrap();
        let back: OperatorConfig = serde_yaml::from_str(&y).unwrap();
        assert_eq!(p, back);
    }
}
