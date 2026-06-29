//! pangea-operator-mcp stdio entrypoint. stdout owns the MCP JSON-RPC protocol,
//! so all tracing goes to stderr. The client is the in-cluster/default kube
//! client — run it as a sidecar in the operator chart (HTTP transport, exposed
//! via the cluster tunnel) or locally against a reachable kubeconfig context.

use std::sync::Arc;

use anyhow::Result;
use pangea_operator_mcp::{KubeStore, PangeaOperatorMcp};
use rmcp::{transport::stdio, ServiceExt};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("pangea_operator_mcp=info")),
        )
        .with_writer(std::io::stderr)
        .init();

    tracing::info!("pangea-operator-mcp starting (stdio transport)");

    let store = KubeStore::from_env()
        .await
        .map_err(|e| anyhow::anyhow!("failed to initialise kube client: {e}"))?;
    let server = PangeaOperatorMcp::new(Arc::new(store));

    let service = server.serve(stdio()).await.map_err(|e| anyhow::anyhow!("serve: {e}"))?;
    service.waiting().await.map_err(|e| anyhow::anyhow!("waiting: {e}"))?;

    tracing::info!("pangea-operator-mcp exiting");
    Ok(())
}
