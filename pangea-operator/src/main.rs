//! Pangea Operator - Kubernetes operator for Pangea infrastructure management.

use pangea_operator::drain::DrainOutcome;
use pangea_operator::{
    controller::{
        AmiTestController, ComplianceBindingController, ComplianceScheduleController,
        ControllerState, DashboardController, FlowController, ImagePipelineController,
        NamespaceController, OperatorPolicyController, PackerBuildController,
        SynthesizerFormatController, TemplateController,
    },
    crd::generate_crds,
    error::Result,
    executor::ExecutorConfig,
    leader::{LeaderConfig, LeaderElector, Leadership},
    observability::{init_tracing, run_health_server, Metrics, ShuttingDown},
};

#[cfg(feature = "graphql")]
use pangea_operator::run_graphql_server;

use kube::Client;
use std::{env, net::SocketAddr, sync::Arc};
use tracing::{error, info, warn};
use tsunagu::ShutdownController;

/// Server listen addresses, projected from the typed
/// [`pangea_operator::config::OperatorConfig`] `servers` section. The parse
/// (+ panic-on-invalid) semantics match the legacy `env::var(...).parse()`.
struct Config {
    /// Health server address.
    health_addr: SocketAddr,

    /// Metrics server address (can be same as health).
    metrics_addr: SocketAddr,

    /// GraphQL server address.
    #[cfg(feature = "graphql")]
    graphql_addr: SocketAddr,

    /// gRPC server address.
    #[cfg(feature = "grpc")]
    grpc_addr: SocketAddr,
}

impl Config {
    /// Project the server addresses out of the resolved typed config.
    fn from_operator(cfg: &pangea_operator::config::OperatorConfig) -> Self {
        Self {
            health_addr: cfg
                .servers
                .health_addr
                .parse()
                .expect("Invalid HEALTH_ADDR"),
            metrics_addr: cfg
                .servers
                .metrics_addr
                .parse()
                .expect("Invalid METRICS_ADDR"),
            #[cfg(feature = "graphql")]
            graphql_addr: cfg
                .servers
                .graphql_addr
                .parse()
                .expect("Invalid GRAPHQL_ADDR"),
            #[cfg(feature = "grpc")]
            grpc_addr: cfg.servers.grpc_addr.parse().expect("Invalid GRPC_ADDR"),
        }
    }
}

/// Leader election is ON by default; `LEADER_ELECTION=false|0|off|no` disables
/// it (single-instance deployments, local runs, tests).
fn leader_election_enabled() -> bool {
    match std::env::var("LEADER_ELECTION") {
        Ok(v) => {
            let v = v.trim().to_ascii_lowercase();
            !(v == "false" || v == "0" || v == "off" || v == "no")
        }
        Err(_) => true,
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // Install a single process-wide rustls CryptoProvider FIRST. The
    // executor_magma deps (tonic → rustls) feature-unify BOTH aws-lc-rs
    // and ring, so rustls 0.23 can no longer auto-select and panics at
    // first TLS use ("Could not automatically determine the
    // process-level CryptoProvider from Rustls crate features"). Pick
    // aws-lc-rs explicitly. Idempotent — ignore the already-installed Err.
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

    // Handle CRD generation (a CLI arg, not env config — checked before
    // tracing so the output stays clean for redirection).
    if env::args().any(|arg| arg == "--generate-crds") {
        print!("{}", generate_crds());
        return Ok(());
    }

    // Handle values.schema.json generation. Use:
    //
    //   cargo run --bin pangea-operator -- --generate-values-schema \
    //     > helmworks/charts/pangea-operator/values.schema.json
    //
    // The schema is sourced from `chart_values::ChartValues` (typed
    // mirror of values.yaml) via schemars + a hand-spliced
    // useEmbeddedRuby/gemAuth conditional.
    if env::args().any(|arg| arg == "--generate-values-schema") {
        print!(
            "{}",
            pangea_operator::chart_values::generate_values_schema_json()
        );
        return Ok(());
    }

    // Initialize tracing
    init_tracing()?;

    // One-shot schema-migration mode (theory/MAGMA-POSTGRES-LIFECYCLE.md
    // §1, M1). Checked right after tracing so failures log normally
    // rather than needing clean-stdout redirection like the two
    // generator flags above.
    if env::args().any(|arg| arg == "--migrate") {
        return run_migrate().await;
    }

    info!(
        version = env!("CARGO_PKG_VERSION"),
        "Starting Pangea Operator"
    );

    // Resolve the whole operator configuration through the shikumi progressive
    // fold (bare → discovered[pod identity] → prescribed_default → env
    // overlay) and log which tier set each value. This is the typed successor
    // to the scattered `std::env::var` reads; see src/config.rs. Provenance
    // logging is a real operability win — "why is the executor magma / the
    // namespace X?" becomes a boot log line.
    let resolved = pangea_operator::config::OperatorConfig::resolve();
    pangea_operator::config::OperatorConfig::log_provenance(resolved.provenance());
    let op_cfg = resolved.into_value();

    let config = Config::from_operator(&op_cfg);

    // Create Kubernetes client
    let client = Client::try_default()
        .await
        .map_err(|e| pangea_operator::Error::Kube(e))?;

    info!("Connected to Kubernetes cluster");

    // Initialize metrics
    let metrics = Arc::new(Metrics::new());

    // Load executor configuration from the typed config (was
    // ExecutorConfig::from_env() — byte-identical, pinned by config parity
    // tests).
    let executor_config = ExecutorConfig::from_operator_config(&op_cfg);
    info!(
        tofu_binary = ?executor_config.tofu_binary,
        workspace_base = ?executor_config.workspace_base,
        timeout_secs = executor_config.timeout_secs,
        "Executor configuration loaded"
    );

    // Construct the compiler backend. Selection:
    //   PANGEA_COMPILER_BACKEND=http      (default)  → sidecar over HTTP
    //   PANGEA_COMPILER_BACKEND=embedded             → magnus + GemCache
    //                                                  (only when feature
    //                                                  `embedded_ruby` is
    //                                                  compiled in)
    // See theory/PANGEA-WORKSPACE-RECONCILIATION.md § M8.2.
    let compiler_endpoint = op_cfg.compiler.endpoint.clone();
    // Sidecar sunset (2026-06-02): default to in-process magnus. The HTTP
    // sidecar is legacy; embedded is the strategy. An explicit
    // PANGEA_COMPILER_BACKEND=http remains only as a migration escape hatch.
    let backend_kind = op_cfg.compiler.backend.clone();
    let compiler_backend: std::sync::Arc<dyn pangea_operator::ruby::CompilerBackend> =
        match backend_kind.as_str() {
            #[cfg(feature = "embedded_ruby")]
            "embedded" => {
                // S2: pool of N ruby owner threads. PANGEA_RUBY_WORKERS
                // env knob defaults to 1 (= today's behavior). S3
                // surfaces this in helm values + flips the rio
                // default to 4 once the metrics from S1 confirm
                // the pool is healthy.
                let n_workers: usize = op_cfg.compiler.ruby_workers;
                info!(
                    workers = n_workers,
                    "Compiler backend: embedded magnus + gem cache"
                );
                let cache = pangea_operator::ruby::GemCache::from_env();
                let pool = pangea_operator::ruby::RubyPool::spawn(n_workers, vec![])
                    .await
                    .expect("spawn ruby pool");
                let pool = std::sync::Arc::new(pool);
                // Attach metrics so every dispatcher call emits
                // pangea_compile_queue_depth + pangea_compile_request_seconds
                // — see observability/metrics.rs S1 docstring.
                let backend =
                    pangea_operator::ruby::EmbeddedCompilerBackend::with_cache(pool, cache)
                        .with_metrics(metrics.clone());
                std::sync::Arc::new(backend)
            }
            // Fail loud: embedded was requested but the feature isn't compiled
            // in. Do NOT silently fall back to the sunset HTTP sidecar — that
            // silent fallback is exactly how the operator shipped HTTP-only
            // despite PANGEA_COMPILER_BACKEND=embedded (the build-spec dropped
            // the non-default embedded_ruby feature). embedded_ruby is now a
            // default feature; if it's somehow absent, that's a build bug.
            #[cfg(not(feature = "embedded_ruby"))]
            "embedded" => panic!(
                "PANGEA_COMPILER_BACKEND=embedded but the embedded_ruby feature is not \
                 compiled in. The HTTP compiler sidecar is sunset — rebuild with the \
                 embedded_ruby feature (now default). Refusing to fall back to HTTP."
            ),
            _ => {
                info!(endpoint = %compiler_endpoint, "Compiler backend: HTTP sidecar (LEGACY/sunset — embedded is the strategy)");
                pangea_operator::controller::architecture_gem_controller::http_backend(
                    compiler_endpoint.clone(),
                )
            }
        };

    // Create controller state
    let state = ControllerState::new(
        client.clone(),
        metrics.clone(),
        executor_config,
        compiler_backend.clone(),
    )
    .await?;

    // Wire the shared Postgres pool to the `pangea_state` DB so magma's
    // state backend (`ControllerState::state_backend`) goes live. Without
    // this, `state_backend` stays `None` and `executor_for` always falls
    // back to tofu regardless of a CR's `spec.executor`. Connection params
    // come from the standard libpq env names (PGHOST/PGPORT/PGUSER/
    // PGDATABASE/PGPASSWORD); the deploy sets these. PGPASSWORD is the
    // gate: with no password we never attempt a connection and magma
    // simply falls back to tofu — identical to today's behavior. A
    // connect failure logs + continues with the un-pooled `state` (never
    // crashes the operator). See `ControllerState::with_db_pool` +
    // docs/design/0005-autonomic-convergence-on-magma.md.
    // Dedicated readiness-probe pool, deliberately NOT the workload pool.
    //
    // /readyz ran on a clone of the magma pool, which caps at 5 connections.
    // A plan/apply wave holds those 5, so the probe's schema check queued
    // behind real work and blew its own 2s bound — measured on camelot-eks
    // 2026-08-05, "readiness probe: schema-presence check timed out after 2s"
    // interleaved with "magma: state saved" from the same process. The
    // operator reported itself unready precisely BECAUSE it was busy doing
    // its job, which is the same false-signal asymmetry the helmrelease
    // already corrected for liveness (10s x 6) without reaching the shared
    // pool underneath readiness.
    //
    // One connection is enough: the probe is two `LIMIT 0` schema reads.
    // Isolation is the point, not throughput.
    let mut probe_pool: Option<Arc<tokio::sync::RwLock<sqlx::PgPool>>> = None;

    let state = match env::var("PGPASSWORD").ok().filter(|p| !p.is_empty()) {
        None => {
            info!("no PGPASSWORD; magma state backend not wired, magma falls back to tofu");
            state
        }
        Some(pg_password) => {
            // Non-secret coordinates come from the typed config; PGPASSWORD
            // stays a direct env read (a secret, deliberately absent from the
            // serialized config surface).
            let pg_host = op_cfg.database.host.clone();
            let pg_port: u16 = op_cfg.database.port;
            let pg_user = op_cfg.database.user.clone();
            let pg_database = op_cfg.database.database.clone();

            let connect_options = sqlx::postgres::PgConnectOptions::new()
                .host(&pg_host)
                .port(pg_port)
                .username(&pg_user)
                .password(&pg_password)
                .database(&pg_database);

            info!(
                pg_host = %pg_host,
                pg_port,
                pg_user = %pg_user,
                pg_database = %pg_database,
                "Wiring Postgres pool for magma state backend"
            );

            let probe_options = connect_options.clone();

            match sqlx::postgres::PgPoolOptions::new()
                .max_connections(op_cfg.database.pool_max_connections.max(1))
                .connect_with(connect_options)
                .await
            {
                Ok(pool) => {
                    info!("Connected to pangea_state; magma state backend wired");
                    // Best-effort, exactly like the table ensures below: if
                    // the probe pool cannot be built we fall back to the
                    // shared pool at the health-server wiring, which is the
                    // pre-existing behaviour rather than a regression.
                    match sqlx::postgres::PgPoolOptions::new()
                        .max_connections(1)
                        .acquire_timeout(std::time::Duration::from_secs(2))
                        .connect_with(probe_options)
                        .await
                    {
                        Ok(p) => {
                            probe_pool = Some(Arc::new(tokio::sync::RwLock::new(p)));
                            info!("Readiness probe pool wired (isolated from the magma pool)");
                        }
                        Err(e) => warn!(
                            error = %e,
                            "failed to wire the dedicated readiness pool; \
                             /readyz falls back to sharing the magma pool"
                        ),
                    }
                    let state = state.with_db_pool(pool);
                    // Ensure the durable artifact table exists before any
                    // magma plan/apply persists rendered config / plan /
                    // bundle (zero-disk path). Best-effort: a failure here
                    // logs + continues — the magma executor's per-op typed
                    // errors surface the gap loudly rather than silently
                    // writing to disk. Per ★★ MAGMA-NATIVE EXECUTION.
                    if let Some(store) = state.artifact_store.as_ref() {
                        if let Err(e) = store.ensure_table().await {
                            warn!(error = %e, "failed to ensure pangea_meta.artifacts table");
                        }
                    }
                    // Same best-effort pattern for the advisory-lock
                    // tracking table: `pg_try_advisory_lock` itself needs
                    // no table (it's a built-in Postgres session
                    // primitive), so a failure here only loses the
                    // `pangea_meta.state_locks` observability rows, never
                    // the actual mutual-exclusion guarantee `handle_applying`
                    // / `handle_destroying` depend on.
                    if let Some(lock_mgr) = state.state_lock.as_ref() {
                        if let Err(e) = lock_mgr.ensure_lock_table().await {
                            warn!(error = %e, "failed to ensure pangea_meta.state_locks table");
                        }
                    }
                    state
                }
                Err(e) => {
                    warn!(
                        error = %e,
                        "failed to connect to pangea_state; magma state backend not wired, \
                         magma falls back to tofu"
                    );
                    state
                }
            }
        }
    };

    // Shared shutdown-in-progress flag (theory/MAGMA-POSTGRES-LIFECYCLE.md
    // §4, M2, Gap D) — flipped true the moment a drain signal is
    // received, before any controller is aborted, so /readyz on every
    // health server this process runs reflects "draining" immediately
    // rather than waiting on a DB probe (or a DB-less deploy's
    // unconditional 200) to notice.
    let shutting_down: ShuttingDown = Arc::new(std::sync::atomic::AtomicBool::new(false));

    // Spawn health server (8080) — /healthz + /readyz; also serves
    // /metrics for back-compat with scrape configs that hit the
    // health port. /readyz probes the magma state-backend pool (when
    // wired) with a bounded schema-presence check, so the pod is
    // pulled from the Service endpoints while Postgres — or its
    // schema — is unavailable.
    let health_metrics = metrics.clone();
    // Prefer the isolated probe pool; fall back to the shared magma pool so a
    // probe-pool failure degrades to the previous behaviour rather than
    // leaving /readyz with no DB to check at all.
    let health_ready_pool = probe_pool.clone().or_else(|| state.db_pool.clone());
    let health_addr = config.health_addr;
    let health_shutting_down = shutting_down.clone();
    tokio::spawn(async move {
        if let Err(e) = run_health_server(
            health_addr,
            health_metrics,
            health_ready_pool,
            health_shutting_down,
        )
        .await
        {
            error!(error = %e, "Health server error");
        }
    });

    // Spawn dedicated metrics server (9090) so the container/service
    // declared "metrics" port actually has something behind it. Same
    // handler shape as health_addr; ServiceMonitor scrapes hit this.
    // No DB readiness probe here — a scrape endpoint's readiness is
    // liveness, and adding a DB probe to it adds no signal — but it
    // still shares the shutdown flag for consistency.
    let metrics_only = metrics.clone();
    let metrics_addr = config.metrics_addr;
    let metrics_shutting_down = shutting_down.clone();
    tokio::spawn(async move {
        if let Err(e) =
            run_health_server(metrics_addr, metrics_only, None, metrics_shutting_down).await
        {
            error!(error = %e, "Metrics server error");
        }
    });

    info!(%config.health_addr, %config.metrics_addr, "Health + metrics servers started");

    // Install the SIGTERM/SIGINT drain handler EARLY — before leader election —
    // so a follower pod still waiting for the lease exits promptly when
    // Kubernetes scales it down or rolls it.
    let shutdown = ShutdownController::install();

    // ── Leader election ────────────────────────────────────────────────────
    // Exactly one pod reconciles at a time (magma/tofu hold per-workspace state
    // locks). The health/readiness servers above are ALREADY live and do NOT
    // depend on the lease, so under a RollingUpdate a broken new image never
    // becomes Ready while the old leader keeps the lease and keeps reconciling
    // — a bad rollout is a no-op, not an outage. Only when the new pod is Ready
    // does k8s terminate the old one, releasing the lease for a clean handoff.
    // Set LEADER_ELECTION=false to disable (single-instance / local / tests).
    let leader_handle = if leader_election_enabled() {
        let cfg = LeaderConfig::from_env();
        let elector = LeaderElector::new(client.clone(), cfg);
        info!(
            identity = %elector.identity(),
            lease = %elector.lease_name(),
            "Leader election enabled — acquiring lease before starting controllers"
        );
        // Acquire leadership, but bail cleanly if we are drained while still a
        // follower waiting for the lease.
        let mut acquire_drain = shutdown.token();
        let outcome = tokio::select! {
            _ = acquire_drain.wait_ref() => None,
            o = elector.acquire() => Some(o),
        };
        match outcome {
            None => {
                info!("Shutdown received while awaiting leadership — exiting without starting controllers");
                return Ok(());
            }
            // RBAC/Lease API unavailable — run as a singleton (pre-leader-
            // election behavior). No renew task; nothing to step down from.
            Some(Leadership::Unavailable) => None,
            Some(Leadership::Acquired) => {
                info!("Acquired leadership — starting controllers");
                // Keep the lease renewed in the background; the task returns if
                // leadership is ever lost, which we treat as a shutdown trigger.
                let renew = elector.clone();
                Some(tokio::spawn(async move {
                    renew.keep_renewed().await;
                }))
            }
        }
    } else {
        info!(
            "Leader election disabled (LEADER_ELECTION=false) — starting controllers immediately"
        );
        None
    };

    // Spawn controllers
    let template_state = state.clone();
    let template_controller = tokio::spawn(async move {
        let controller = TemplateController::new(template_state);
        if let Err(e) = controller.run().await {
            error!(error = %e, "Template controller error");
        }
    });

    let namespace_state = state.clone();
    let namespace_controller = tokio::spawn(async move {
        let controller = NamespaceController::new(namespace_state);
        if let Err(e) = controller.run().await {
            error!(error = %e, "Namespace controller error");
        }
    });

    let flow_state = state.clone();
    let flow_controller = tokio::spawn(async move {
        let controller = FlowController::new(flow_state);
        if let Err(e) = controller.run().await {
            error!(error = %e, "Flow controller error");
        }
    });

    let packer_build_state = state.clone();
    let packer_build_controller = tokio::spawn(async move {
        let controller = PackerBuildController::new(packer_build_state);
        if let Err(e) = controller.run().await {
            error!(error = %e, "PackerBuild controller error");
        }
    });

    let ami_test_state = state.clone();
    let ami_test_controller = tokio::spawn(async move {
        let controller = AmiTestController::new(ami_test_state);
        if let Err(e) = controller.run().await {
            error!(error = %e, "AmiTest controller error");
        }
    });

    let image_pipeline_state = state.clone();
    let image_pipeline_controller = tokio::spawn(async move {
        let controller = ImagePipelineController::new(image_pipeline_state);
        if let Err(e) = controller.run().await {
            error!(error = %e, "ImagePipeline controller error");
        }
    });

    let synth_format_state = state.clone();
    let synth_format_controller = tokio::spawn(async move {
        let controller = SynthesizerFormatController::new(synth_format_state);
        if let Err(e) = controller.run().await {
            error!(error = %e, "SynthesizerFormat controller error");
        }
    });

    let compliance_schedule_state = state.clone();
    let compliance_schedule_controller = tokio::spawn(async move {
        let controller = ComplianceScheduleController::new(compliance_schedule_state);
        if let Err(e) = controller.run().await {
            error!(error = %e, "ComplianceSchedule controller error");
        }
    });

    let compliance_binding_state = state.clone();
    let compliance_binding_controller = tokio::spawn(async move {
        let controller = ComplianceBindingController::new(compliance_binding_state);
        if let Err(e) = controller.run().await {
            error!(error = %e, "ComplianceBinding controller error");
        }
    });

    // PangeaDashboard reconciler — renders a dashboard CR's inline Pangea
    // Ruby through the shared (embedded) compiler backend into Grafana
    // JSON, then upserts a sidecar-labelled ConfigMap the vm-stack Grafana
    // dashboard sidecar loads. See docs/DASHBOARD-AS-CODE.md.
    let dashboard_state = state.clone();
    let dashboard_controller = tokio::spawn(async move {
        let controller = DashboardController::new(dashboard_state);
        if let Err(e) = controller.run().await {
            error!(error = %e, "PangeaDashboard controller error");
        }
    });

    // M1 — ArchitectureGem reconciler. Reuses the shared compiler
    // backend already in state — same dispatch as every other
    // controller (template, packer, synthesizer-format).
    // See theory/PANGEA-WORKSPACE-RECONCILIATION.md § M8.2.
    let arch_gem_client = state.client.clone();
    let arch_gem_backend = state.compiler_backend.clone();
    let arch_gem_policy = state.operator_policy.clone();
    let arch_gem_metrics = state.metrics.clone();
    let architecture_gem_controller = tokio::spawn(async move {
        pangea_operator::controller::architecture_gem_controller::run(
            arch_gem_client,
            arch_gem_backend,
            arch_gem_policy,
            arch_gem_metrics,
        )
        .await;
        error!("ArchitectureGem controller exited");
    });

    // M1+ — WorkspaceCatalog reconciler. Watches WorkspaceCatalog CRs;
    // populates status.{templateCount, verified, conditions} based on
    // (a) the readiness of every required ArchitectureGem and (b) the
    // count of InfrastructureTemplate CRs labeled with this catalog.
    // The cascade root for workspace-level policy.
    let wsc_client = state.client.clone();
    let wsc_policy = state.operator_policy.clone();
    let wsc_metrics = state.metrics.clone();
    let workspace_catalog_controller = tokio::spawn(async move {
        pangea_operator::controller::workspace_catalog_controller::run(
            wsc_client,
            wsc_policy,
            wsc_metrics,
        )
        .await;
        error!("WorkspaceCatalog controller exited");
    });

    // ReconciliationLoop (roda) reconciler — the loop-granularity axis
    // (theory/RECONCILIATION-TOPOLOGY.md §II). Watches ReconciliationLoop CRs;
    // resolves each loop's label selector against the fleet's
    // InfrastructureTemplates and reports membership + ticks at the loop's
    // cadence. First increment: membership + observation + cadence; cadence-
    // ownership + the malha one-per-resource axis are the next increment.
    let roda_client = state.client.clone();
    let roda_metrics = state.metrics.clone();
    let reconciliation_loop_controller = tokio::spawn(async move {
        pangea_operator::controller::reconciliation_loop_controller::run(roda_client, roda_metrics)
            .await;
        error!("ReconciliationLoop controller exited");
    });

    // OperatorPolicy controller — propagates `OperatorPolicy/default`
    // spec into the in-memory cache that every other controller reads
    // via `policy_gate`, and mirrors spec → status. Started before any
    // other controller's first reconcile would benefit from a faster
    // policy lookup, but order doesn't actually matter — the cache
    // initializes to permissive so worst case is a few extra reconciles
    // before the policy propagates.
    let operator_policy_state = state.clone();
    let operator_policy_controller = tokio::spawn(async move {
        let controller = OperatorPolicyController::new(operator_policy_state);
        if let Err(e) = controller.run().await {
            error!(error = %e, "OperatorPolicy controller error");
        }
    });

    // PangeaFleetStatus controller — refreshes the cluster-scoped
    // singleton with per-CRD-class aggregations every 30s. Bypasses
    // OperatorPolicy.globalSuspend so fleet visibility stays live
    // even while user-resource reconciles are paused. Self-creates
    // the `default` CR on startup.
    let fleet_status_client = state.client.clone();
    let fleet_status_metrics = state.metrics.clone();
    let fleet_status_controller = tokio::spawn(async move {
        pangea_operator::controller::fleet_status_controller::run(
            fleet_status_client,
            fleet_status_metrics,
        )
        .await;
        error!("PangeaFleetStatus controller exited");
    });

    info!("Controllers started");

    // Start GraphQL server
    #[cfg(feature = "graphql")]
    {
        let graphql_client = client.clone();
        let graphql_addr = config.graphql_addr;
        tokio::spawn(async move {
            if let Err(e) = run_graphql_server(graphql_addr, graphql_client).await {
                error!(error = %e, "GraphQL server error");
            }
        });
        info!(%config.graphql_addr, "GraphQL server started");
    }

    // Wait for either a SIGTERM/SIGINT drain OR loss of leadership (another pod
    // took the lease). Either way, stop the controllers and exit; Kubernetes
    // restarts us as a follower if it was a leadership loss.
    match leader_handle {
        Some(handle) => {
            let mut drain = shutdown.token();
            tokio::select! {
                _ = drain.wait_ref() => info!("Shutdown signal received, stopping operator"),
                _ = handle => warn!(
                    "Lost leadership lease — stopping controllers so a single reconciler is preserved"
                ),
            }
        }
        None => {
            shutdown.token().wait().await;
            info!("Shutdown signal received, stopping operator");
        }
    }

    // Gap D (theory/MAGMA-POSTGRES-LIFECYCLE.md §4, M2): flip /readyz to
    // failing on every health server FIRST, before touching a single
    // controller — the Service should pull this pod's endpoint before
    // any in-flight or new work routes to it, not after.
    shutting_down.store(true, std::sync::atomic::Ordering::Relaxed);
    info!("Readiness flipped to failing; draining before controller shutdown");

    // Give in-flight magma applies a bounded window to reach their next
    // checkpoint boundary (state + bundle committed atomically to
    // Postgres — see MAGMA-OPERATOR-BACKEND.md's own atomicity
    // invariant) before their task is cancelled. Postgres itself rolls
    // back an uncommitted transaction on connection loss either way (no
    // half-applied state can persist), but a hard `.abort()` mid-cycle
    // still discards otherwise-complete work that a grace window lets
    // finish and commit instead of being wastefully retried after restart.
    //
    // This used to be an unconditional `sleep(5s)`, which was wrong in both
    // directions: three orders of magnitude too short for a real cycle (a
    // measured ~11m plan + ~11m apply on pleme-io-opensource), and pure
    // latency for the far more common idle shutdown. It waits on the
    // admission budget now — see `drain` for the incident that motivated it.
    let drain_budget = std::time::Duration::from_secs(op_cfg.reconcile.drain_max_wait_secs);
    let in_flight_budgets = state.workspace_budgets.clone();
    match pangea_operator::drain::await_in_flight(
        move || in_flight_budgets.total_in_flight(),
        drain_budget,
        std::time::Duration::from_secs(1),
    )
    .await
    {
        DrainOutcome::Idle => info!("No expensive phases in flight; draining immediately"),
        DrainOutcome::Drained { waited } => info!(
            waited_secs = waited.as_secs(),
            "In-flight reconcile work committed before shutdown"
        ),
        // The case the old code hit on every single eviction, silently.
        DrainOutcome::Deadline {
            still_in_flight,
            waited,
        } => warn!(
            still_in_flight,
            waited_secs = waited.as_secs(),
            drain_budget_secs = op_cfg.reconcile.drain_max_wait_secs,
            "Drain deadline reached with work still running; aborting will discard these cycles \
             and they will restart from the top. Raise podDisruptionBudget/terminationGracePeriod \
             or shard the workspace if this recurs."
        ),
    }

    // Abort controllers (they run forever)
    template_controller.abort();
    namespace_controller.abort();
    flow_controller.abort();
    packer_build_controller.abort();
    ami_test_controller.abort();
    image_pipeline_controller.abort();
    synth_format_controller.abort();
    compliance_schedule_controller.abort();
    compliance_binding_controller.abort();
    dashboard_controller.abort();
    architecture_gem_controller.abort();
    reconciliation_loop_controller.abort();
    workspace_catalog_controller.abort();
    operator_policy_controller.abort();
    fleet_status_controller.abort();

    // Explicit pool.close() — confirmed absent before this change; the
    // pool previously just died with the process. Closing signals every
    // idle connection to terminate and refuses new acquisitions, so the
    // CNPG primary sees a clean disconnect rather than a dropped-socket
    // ambiguity on the next connection-count/idle-timeout check.
    if let Some(pool) = state.db_pool.as_ref() {
        pool.read().await.close().await;
        info!("magma Postgres pool closed");
    }

    info!("Pangea Operator stopped");
    Ok(())
}

/// One-shot schema-migration mode (`--migrate`). Connects the PG pool
/// using the same `PGHOST`/`PGUSER`/`PGDATABASE`/`PGPASSWORD` coordinates
/// the normal startup path reads, runs the same idempotent
/// [`pangea_operator::backend::ArtifactStore::ensure_table`] /
/// [`pangea_operator::backend::StateLock::ensure_lock_table`] calls, then
/// exits.
///
/// theory/MAGMA-POSTGRES-LIFECYCLE.md §1 (M1) — meant to be invoked as a
/// shinka `DatabaseMigration`'s migrator command, so the schema-ready
/// signal becomes an explicit, observable step a `shinka-wait` init
/// container can poll, rather than the best-effort ensure buried inside
/// the main process's happy path (which logs a warning and continues on
/// failure — the right posture for a long-running process that must
/// keep converging, wrong for a discrete migration step whose entire
/// job is to surface a schema problem loudly before the main container
/// ever starts). Every failure path here returns `Err` (non-zero exit),
/// the inverse of the main path's converge-don't-crash posture.
async fn run_migrate() -> Result<()> {
    info!(
        version = env!("CARGO_PKG_VERSION"),
        "Running pangea-operator --migrate"
    );

    let pg_password = env::var("PGPASSWORD")
        .ok()
        .filter(|p| !p.is_empty())
        .ok_or_else(|| {
            pangea_operator::error::Error::Config("--migrate requires PGPASSWORD to be set".into())
        })?;

    let resolved = pangea_operator::config::OperatorConfig::resolve();
    let op_cfg = resolved.into_value();

    let connect_options = sqlx::postgres::PgConnectOptions::new()
        .host(&op_cfg.database.host)
        .port(op_cfg.database.port)
        .username(&op_cfg.database.user)
        .password(&pg_password)
        .database(&op_cfg.database.database);

    info!(
        pg_host = %op_cfg.database.host,
        pg_port = op_cfg.database.port,
        pg_user = %op_cfg.database.user,
        pg_database = %op_cfg.database.database,
        "Connecting for --migrate"
    );

    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect_with(connect_options)
        .await?;
    let pool = Arc::new(pool);

    pangea_operator::backend::ArtifactStore::new(pool.clone())
        .ensure_table()
        .await?;
    pangea_operator::backend::StateLock::new(pool)
        .ensure_lock_table()
        .await?;

    info!("--migrate: schema ensured successfully");
    Ok(())
}
