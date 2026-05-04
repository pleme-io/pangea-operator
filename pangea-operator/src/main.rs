//! Pangea Operator - Kubernetes operator for Pangea infrastructure management.

use pangea_operator::{
    controller::{
        AmiTestController, ComplianceBindingController, ComplianceScheduleController,
        ControllerState, FlowController, ImagePipelineController, NamespaceController,
        OperatorPolicyController, PackerBuildController, SynthesizerFormatController,
        TemplateController,
    },
    crd::generate_crds,
    error::Result,
    executor::ExecutorConfig,
    observability::{init_tracing, run_health_server, Metrics},
};

#[cfg(feature = "graphql")]
use pangea_operator::run_graphql_server;

use kube::Client;
use std::{env, net::SocketAddr, sync::Arc};
use tracing::{error, info};
use tsunagu::ShutdownController;

/// Application configuration from environment variables.
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

    /// Generate CRDs and exit.
    generate_crds: bool,
}

impl Config {
    /// Load configuration from environment.
    fn from_env() -> Self {
        let health_addr = env::var("HEALTH_ADDR")
            .unwrap_or_else(|_| "0.0.0.0:8080".to_string())
            .parse()
            .expect("Invalid HEALTH_ADDR");

        let metrics_addr = env::var("METRICS_ADDR")
            .unwrap_or_else(|_| "0.0.0.0:9090".to_string())
            .parse()
            .expect("Invalid METRICS_ADDR");

        #[cfg(feature = "graphql")]
        let graphql_addr = env::var("GRAPHQL_ADDR")
            .unwrap_or_else(|_| "0.0.0.0:8081".to_string())
            .parse()
            .expect("Invalid GRAPHQL_ADDR");

        #[cfg(feature = "grpc")]
        let grpc_addr = env::var("GRPC_ADDR")
            .unwrap_or_else(|_| "0.0.0.0:50051".to_string())
            .parse()
            .expect("Invalid GRPC_ADDR");

        let generate_crds = env::args().any(|arg| arg == "--generate-crds");

        Self {
            health_addr,
            metrics_addr,
            #[cfg(feature = "graphql")]
            graphql_addr,
            #[cfg(feature = "grpc")]
            grpc_addr,
            generate_crds,
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let config = Config::from_env();

    // Handle CRD generation
    if config.generate_crds {
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

    // Create Kubernetes client
    let client = Client::try_default()
        .await
        .map_err(|e| pangea_operator::Error::Kube(e))?;

    info!("Connected to Kubernetes cluster");

    // Initialize metrics
    let metrics = Arc::new(Metrics::new());

    // Load executor configuration from environment
    let executor_config = ExecutorConfig::from_env();
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
    let compiler_endpoint = std::env::var("COMPILER_ENDPOINT")
        .unwrap_or_else(|_| "http://localhost:8082".to_string());
    let backend_kind = std::env::var("PANGEA_COMPILER_BACKEND")
        .unwrap_or_else(|_| "http".to_string());
    let compiler_backend: std::sync::Arc<dyn pangea_operator::ruby::CompilerBackend> =
        match backend_kind.as_str() {
            #[cfg(feature = "embedded_ruby")]
            "embedded" => {
                info!("Compiler backend: embedded magnus + gem cache");
                let cache = pangea_operator::ruby::GemCache::from_env();
                let owner = pangea_operator::ruby::RubyOwner::spawn(vec![])
                    .await
                    .expect("spawn ruby owner thread");
                // Attach metrics so every dispatcher call emits
                // pangea_compile_queue_depth + pangea_compile_request_seconds
                // — see observability/metrics.rs S1 docstring.
                let backend = pangea_operator::ruby::EmbeddedCompilerBackend::with_cache(
                    owner.tx_handle(),
                    cache,
                )
                .with_metrics(metrics.clone());
                // Leak the owner so it lives the lifetime of the
                // process — Drop on shutdown is best-effort.
                std::mem::forget(owner);
                std::sync::Arc::new(backend)
            }
            _ => {
                info!(endpoint = %compiler_endpoint, "Compiler backend: HTTP sidecar");
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

    // Spawn health server (8080) — /healthz + /readyz; also serves
    // /metrics for back-compat with scrape configs that hit the
    // health port.
    let health_metrics = metrics.clone();
    let health_addr = config.health_addr;
    tokio::spawn(async move {
        if let Err(e) = run_health_server(health_addr, health_metrics).await {
            error!(error = %e, "Health server error");
        }
    });

    // Spawn dedicated metrics server (9090) so the container/service
    // declared "metrics" port actually has something behind it. Same
    // handler shape as health_addr; ServiceMonitor scrapes hit this.
    let metrics_only = metrics.clone();
    let metrics_addr = config.metrics_addr;
    tokio::spawn(async move {
        if let Err(e) = run_health_server(metrics_addr, metrics_only).await {
            error!(error = %e, "Metrics server error");
        }
    });

    info!(%config.health_addr, %config.metrics_addr, "Health + metrics servers started");

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
    let fleet_status_controller = tokio::spawn(async move {
        pangea_operator::controller::fleet_status_controller::run(fleet_status_client).await;
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

    // Install SIGTERM/SIGINT handler and wait for drain.
    let shutdown = ShutdownController::install();
    shutdown.token().wait().await;

    info!("Shutdown signal received, stopping operator");

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
    architecture_gem_controller.abort();
    workspace_catalog_controller.abort();
    operator_policy_controller.abort();
    fleet_status_controller.abort();

    info!("Pangea Operator stopped");
    Ok(())
}

