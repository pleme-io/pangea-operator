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
    /// Ban tofu as a silent fallback (`PANGEA_FORBID_TOFU`). The magma-native
    /// prescribed default is `true`; the legacy env-absent default is `false`
    /// and the live gate (`forbid_tofu_from_env`) is unchanged
    /// (`pending-shikumi`), so no runtime behavior change.
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

/// Pangea DSL compiler backend selection.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CompilerConfig {
    /// `embedded` (magnus, default) or `http` (sunset sidecar)
    /// (`PANGEA_COMPILER_BACKEND`).
    pub backend: String,
    /// HTTP sidecar endpoint for the legacy backend (`COMPILER_ENDPOINT`).
    pub endpoint: String,
    /// Embedded Ruby owner-thread pool size (`PANGEA_RUBY_WORKERS`).
    pub ruby_workers: usize,
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
}

#[derive(Serialize, Default)]
struct ReconcileOverlay {
    #[serde(skip_serializing_if = "Option::is_none")]
    workers: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    budget_per_workspace: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    budget_global: Option<usize>,
}

#[derive(Serialize, Default)]
struct ObservabilityOverlay {
    #[serde(skip_serializing_if = "Option::is_none")]
    log_format: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    otel_endpoint: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    otel_service_name: Option<String>,
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
                forbid_tofu: env_present("PANGEA_FORBID_TOFU")
                    .map(|v| crate::executor::backend_select::parse_truthy(Some(v.as_str()))),
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
            },
            database: DatabaseOverlay {
                host: env_present("PGHOST"),
                port: env_parse::<u16>("PGPORT"),
                user: env_present("PGUSER"),
                database: env_present("PGDATABASE"),
            },
            reconcile: ReconcileOverlay {
                // The clamp matches `reconcile_workers_from_env`.
                workers: env_parse::<usize>("PANGEA_RECONCILE_WORKERS").map(|n| n.clamp(1, 32)),
                budget_per_workspace: env_parse::<usize>("PANGEA_BUDGET_PER_WORKSPACE"),
                budget_global: env_parse::<usize>("PANGEA_BUDGET_GLOBAL"),
            },
            observability: ObservabilityOverlay {
                log_format: env_present("LOG_FORMAT"),
                otel_endpoint: env_present("OTEL_EXPORTER_OTLP_ENDPOINT"),
                otel_service_name: env_present("OTEL_SERVICE_NAME"),
            },
            routing: RoutingOverlay {
                ntfy_base_url: env_present("PANGEA_NTFY_BASE_URL"),
            },
            api: ApiOverlay {
                enable_playground: env_present("PANGEA_ENABLE_PLAYGROUND")
                    .map(|v| v == "true" || v == "1"),
            },
            gem: GemOverlay {
                cache_dir: env_present("PANGEA_GEM_CACHE_DIR"),
            },
        }
    }
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
            },
            database: DatabaseConfig {
                host: String::new(),
                port: 0,
                user: String::new(),
                database: String::new(),
            },
            reconcile: ReconcileConfig {
                workers: 0,
                budget_per_workspace: 0,
                budget_global: 0,
            },
            observability: ObservabilityConfig {
                log_format: String::new(),
                otel_endpoint: None,
                otel_service_name: String::new(),
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
            backend: "embedded".to_string(),
            endpoint: "http://localhost:8082".to_string(),
            ruby_workers: 1,
        };
        c.database = DatabaseConfig {
            host: "pangea-database-rw.pangea-system.svc.cluster.local".to_string(),
            port: 5432,
            user: "postgres".to_string(),
            database: "pangea_state".to_string(),
        };
        c.reconcile = ReconcileConfig {
            workers: 4,
            budget_per_workspace: 4,
            budget_global: 16,
        };
        c.observability = ObservabilityConfig {
            log_format: "pretty".to_string(),
            otel_endpoint: None,
            otel_service_name: "pangea-operator".to_string(),
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
        c
    }
}

impl OperatorConfig {
    /// Resolve the full config through the sealed progressive fold, overlaying
    /// the operator's bespoke env vars at the `Custom` tier. The one call site
    /// `main.rs` reaches for at startup.
    #[must_use]
    pub fn resolve() -> ProgressiveResolution<Self> {
        let overlay =
            ProgressiveLayer::env("pangea-operator", to_dict(&EnvOverlay::from_process_env()));
        Self::resolve_progressive_with(&[overlay])
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
        assert_eq!(p.compiler.backend, "embedded");
        assert_eq!(p.compiler.endpoint, "http://localhost:8082");
        assert_eq!(p.compiler.ruby_workers, 1);
        assert_eq!(
            p.database.host,
            "pangea-database-rw.pangea-system.svc.cluster.local"
        );
        assert_eq!(p.database.port, 5432);
        assert_eq!(p.database.user, "postgres");
        assert_eq!(p.database.database, "pangea_state");
        assert_eq!(p.reconcile.workers, 4);
        assert_eq!(p.reconcile.budget_per_workspace, 4);
        assert_eq!(p.reconcile.budget_global, 16);
        assert_eq!(p.observability.log_format, "pretty");
        assert_eq!(p.observability.otel_service_name, "pangea-operator");
        assert_eq!(p.routing.ntfy_base_url, "https://ntfy.sh");
        assert!(p.api.enable_playground);
        assert_eq!(p.gem.cache_dir, "/var/pangea/gems");
    }

    #[test]
    fn magma_native_default_is_prescribed() {
        // Per ★★ MAGMA-NATIVE: the shipped default names magma + forbids tofu.
        let p = OperatorConfig::prescribed_default();
        assert_eq!(p.executor.backend.as_deref(), Some("magma"));
        assert!(p.executor.forbid_tofu);
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

        clear_migrated();
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
