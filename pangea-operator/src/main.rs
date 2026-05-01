//! Pangea Operator - Kubernetes operator for Pangea infrastructure management.

use pangea_operator::{
    controller::{
        AmiTestController, ComplianceBindingController, ComplianceScheduleController,
        ControllerState, FlowController, ImagePipelineController, NamespaceController,
        PackerBuildController, SynthesizerFormatController, TemplateController,
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

    // Create controller state
    let state = ControllerState::new(client.clone(), metrics.clone(), executor_config).await?;

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

    // M1 — ArchitectureGem reconciler. Talks to the compiler sidecar
    // at COMPILER_ENDPOINT (default http://localhost:8082). See
    // theory/PANGEA-WORKSPACE-RECONCILIATION.md.
    let arch_gem_client = state.client.clone();
    let arch_gem_compiler_url = std::env::var("COMPILER_ENDPOINT")
        .unwrap_or_else(|_| "http://localhost:8082".to_string());
    let architecture_gem_controller = tokio::spawn(async move {
        pangea_operator::controller::architecture_gem_controller::run(
            arch_gem_client,
            arch_gem_compiler_url,
        )
        .await;
        error!("ArchitectureGem controller exited");
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

    info!("Pangea Operator stopped");
    Ok(())
}

