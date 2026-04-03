//! Pangea Operator - Kubernetes operator for Pangea infrastructure management.

use pangea_operator::{
    controller::{ControllerState, NamespaceController, TemplateController},
    crd::generate_crds,
    error::Result,
    executor::ExecutorConfig,
    observability::{init_tracing, run_health_server, Metrics},
};

#[cfg(feature = "graphql")]
use pangea_operator::run_graphql_server;

use kube::Client;
use std::{env, net::SocketAddr, sync::Arc};
use tokio::signal;
use tracing::{error, info};

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

    // Spawn health/metrics server
    let health_metrics = metrics.clone();
    let health_addr = config.health_addr;
    tokio::spawn(async move {
        if let Err(e) = run_health_server(health_addr, health_metrics).await {
            error!(error = %e, "Health server error");
        }
    });

    info!(%config.health_addr, "Health server started");

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

    // Wait for shutdown signal
    shutdown_signal().await;

    info!("Shutdown signal received, stopping operator");

    // Abort controllers (they run forever)
    template_controller.abort();
    namespace_controller.abort();

    info!("Pangea Operator stopped");
    Ok(())
}

/// Wait for shutdown signal (SIGTERM or SIGINT).
async fn shutdown_signal() {
    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("Failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("Failed to install SIGTERM handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
}
