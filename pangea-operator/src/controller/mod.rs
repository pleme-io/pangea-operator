//! Kubernetes controller components for the Pangea Operator.
//!
//! This module contains the reconciliation logic for all custom resources:
//! InfrastructureTemplate, PangeaNamespace, InfrastructureFlow, PackerBuild,
//! AmiTest, and ImagePipeline.

mod flow_controller;
pub mod flow_scheduler;
mod reconciler;
mod template_controller;
mod namespace_controller;
mod packer_build_controller;
mod ami_test_controller;
mod image_pipeline_controller;

pub use reconciler::*;
pub use flow_controller::FlowController;
pub use template_controller::TemplateController;
pub use namespace_controller::NamespaceController;
pub use packer_build_controller::PackerBuildController;
pub use ami_test_controller::AmiTestController;
pub use image_pipeline_controller::ImagePipelineController;

use crate::error::Result;
use crate::executor::{ExecutorConfig, PackerExecutor, TofuExecutor, WorkspaceManager};
use kube::Client;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;

/// Shared state for all controllers.
#[derive(Clone)]
pub struct ControllerState {
    /// Kubernetes client.
    pub client: Client,

    /// Metrics registry.
    pub metrics: Arc<crate::observability::Metrics>,

    /// PostgreSQL connection pool (if configured).
    pub db_pool: Option<Arc<RwLock<sqlx::PgPool>>>,

    /// OpenTofu executor for running infrastructure commands.
    pub executor: Arc<TofuExecutor>,

    /// Packer executor for running AMI build commands.
    pub packer_executor: Arc<PackerExecutor>,

    /// Workspace manager for isolated template directories.
    pub workspace_manager: Arc<WorkspaceManager>,
}

impl ControllerState {
    /// Create new controller state with executor configuration.
    pub async fn new(
        client: Client,
        metrics: Arc<crate::observability::Metrics>,
        executor_config: ExecutorConfig,
    ) -> Result<Self> {
        let executor = Arc::new(TofuExecutor::new(
            executor_config.tofu_binary.clone(),
            Duration::from_secs(executor_config.timeout_secs),
            executor_config.verbose,
        ));

        let packer_executor = Arc::new(PackerExecutor::new(
            executor_config.packer_binary.clone(),
            Duration::from_secs(executor_config.packer_timeout_secs),
            executor_config.verbose,
        ));

        let workspace_manager = Arc::new(WorkspaceManager::new(
            executor_config.workspace_base.clone(),
        ));

        // Ensure workspace base directory exists
        workspace_manager.init().await?;

        Ok(Self {
            client,
            metrics,
            db_pool: None,
            executor,
            packer_executor,
            workspace_manager,
        })
    }

    /// Set the database pool.
    pub fn with_db_pool(mut self, pool: sqlx::PgPool) -> Self {
        self.db_pool = Some(Arc::new(RwLock::new(pool)));
        self
    }
}
