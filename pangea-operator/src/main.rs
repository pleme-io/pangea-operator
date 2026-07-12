//! Pangea Operator - Kubernetes operator for Pangea infrastructure management.

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
    leader::{Leadership, LeaderConfig, LeaderElector},
    observability::{init_tracing, run_health_server, Metrics},
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
        print!("{}", pangea_operator::chart_values::generate_values_schema_json());
        return Ok(());
    }

    // Initialize tracing
    init_tracing()?;

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
                info!(workers = n_workers, "Compiler backend: embedded magnus + gem cache");
                let cache = pangea_operator::ruby::GemCache::from_env();
                let pool = pangea_operator::ruby::RubyPool::spawn(n_workers, vec![])
                    .await
                    .expect("spawn ruby pool");
                let pool = std::sync::Arc::new(pool);
                // Attach metrics so every dispatcher call emits
                // pangea_compile_queue_depth + pangea_compile_request_seconds
                // — see observability/metrics.rs S1 docstring.
                let backend = pangea_operator::ruby::EmbeddedCompilerBackend::with_cache(
                    pool,
                    cache,
                )
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

            match sqlx::postgres::PgPoolOptions::new()
                .max_connections(5)
                .connect_with(connect_options)
                .await
            {
                Ok(pool) => {
                    info!("Connected to pangea_state; magma state backend wired");
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

    // Spawn health server (8080) — /healthz + /readyz; also serves
    // /metrics for back-compat with scrape configs that hit the
    // health port. /readyz probes the magma state-backend pool (when
    // wired) with a bounded SELECT 1, so the pod is pulled from the
    // Service endpoints while Postgres is unreachable.
    let health_metrics = metrics.clone();
    let health_ready_pool = state.db_pool.clone();
    let health_addr = config.health_addr;
    tokio::spawn(async move {
        if let Err(e) = run_health_server(health_addr, health_metrics, health_ready_pool).await {
            error!(error = %e, "Health server error");
        }
    });

    // Spawn dedicated metrics server (9090) so the container/service
    // declared "metrics" port actually has something behind it. Same
    // handler shape as health_addr; ServiceMonitor scrapes hit this.
    // No readiness probe here — a scrape endpoint's readiness is
    // liveness, and adding a DB probe to it adds no signal.
    let metrics_only = metrics.clone();
    let metrics_addr = config.metrics_addr;
    tokio::spawn(async move {
        if let Err(e) = run_health_server(metrics_addr, metrics_only, None).await {
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
        info!("Leader election disabled (LEADER_ELECTION=false) — starting controllers immediately");
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
            wsc_client, wsc_policy, wsc_metrics,
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

    info!("Pangea Operator stopped");
    Ok(())
}

