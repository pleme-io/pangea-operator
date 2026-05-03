//! Kubernetes controller components for the Pangea Operator.
//!
//! This module contains the reconciliation logic for all custom resources:
//! InfrastructureTemplate, PangeaNamespace, InfrastructureFlow, PackerBuild,
//! AmiTest, and ImagePipeline.

pub mod architecture_gem_controller;
pub mod import;
pub mod operator_policy_cache;
pub mod operator_policy_controller;
pub mod policy_gate;
pub mod policy_pipeline;
pub mod post_reconcile_pipeline;
pub mod reactive;
pub mod routing;
pub mod status;
pub mod workspace_catalog_controller;
mod flow_controller;
pub mod flow_scheduler;
pub mod policy_cascade;
mod reconciler;
pub mod settling;
mod template_controller;
mod namespace_controller;
mod packer_build_controller;
mod ami_test_controller;
mod image_pipeline_controller;
mod synthesizer_format_controller;
mod compliance_schedule_controller;
mod compliance_binding_controller;
mod dashboard_controller;

pub use reconciler::*;
pub use flow_controller::FlowController;
pub use template_controller::TemplateController;
pub use namespace_controller::NamespaceController;
pub use packer_build_controller::PackerBuildController;
pub use ami_test_controller::AmiTestController;
pub use image_pipeline_controller::ImagePipelineController;
pub use synthesizer_format_controller::SynthesizerFormatController;
pub use compliance_schedule_controller::ComplianceScheduleController;
pub use compliance_binding_controller::ComplianceBindingController;
pub use dashboard_controller::DashboardController;
pub use operator_policy_controller::OperatorPolicyController;

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

    /// Compiler backend — HTTP sidecar today, embedded magnus when
    /// `embedded_ruby` is feature-on + `PANGEA_COMPILER_BACKEND=embedded`.
    /// See `theory/PANGEA-WORKSPACE-RECONCILIATION.md` § M8.2.
    pub compiler_backend: Arc<dyn crate::ruby::CompilerBackend>,

    /// Routing-delivery client — pings ntfy / Slack / GitHub when a
    /// reactive policy fires. Configured via PANGEA_NTFY_BASE_URL
    /// (defaults to https://ntfy.sh).
    pub routing_client: Arc<routing::RoutingClient>,

    /// In-memory snapshot of `OperatorPolicy/default`. Read by every
    /// reconciler via `policy_gate::check_operator_policy` to honor
    /// the fleet-wide kill-switch + per-controller suspends. Updated
    /// by the `operator_policy_watcher` task.
    pub operator_policy: Arc<operator_policy_cache::OperatorPolicyCache>,
}

impl ControllerState {
    /// Create new controller state with executor configuration.
    pub async fn new(
        client: Client,
        metrics: Arc<crate::observability::Metrics>,
        executor_config: ExecutorConfig,
        compiler_backend: Arc<dyn crate::ruby::CompilerBackend>,
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
            compiler_backend,
            routing_client: Arc::new(routing::RoutingClient::from_env()),
            operator_policy: Arc::new(operator_policy_cache::OperatorPolicyCache::new_permissive()),
        })
    }

    /// Set the database pool.
    pub fn with_db_pool(mut self, pool: sqlx::PgPool) -> Self {
        self.db_pool = Some(Arc::new(RwLock::new(pool)));
        self
    }
}
