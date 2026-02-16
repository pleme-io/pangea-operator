//! Kubernetes controller components for the Pangea Operator.
//!
//! This module contains the reconciliation logic for InfrastructureTemplate
//! and PangeaNamespace custom resources.

mod reconciler;
mod template_controller;
mod namespace_controller;

pub use reconciler::*;
pub use template_controller::TemplateController;
pub use namespace_controller::NamespaceController;

use crate::error::Result;
use kube::Client;
use std::sync::Arc;
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
}

impl ControllerState {
    /// Create new controller state.
    pub async fn new(client: Client, metrics: Arc<crate::observability::Metrics>) -> Result<Self> {
        Ok(Self {
            client,
            metrics,
            db_pool: None,
        })
    }

    /// Set the database pool.
    pub fn with_db_pool(mut self, pool: sqlx::PgPool) -> Self {
        self.db_pool = Some(Arc::new(RwLock::new(pool)));
        self
    }
}
