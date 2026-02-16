//! Controller for PangeaNamespace resources.

use crate::crd::{BackendType, PangeaNamespace, PangeaNamespaceStatus};
use crate::error::{Error, Result};
use crate::observability::Metrics;

use futures::StreamExt;
use kube::{
    api::{Api, Patch, PatchParams},
    runtime::{
        controller::{Action, Controller},
        watcher::Config,
    },
    Client, ResourceExt,
};
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, error, info, instrument, warn};

use super::{ControllerState, DEFAULT_REQUEUE_INTERVAL, ERROR_REQUEUE_INTERVAL};

/// Controller for PangeaNamespace resources.
pub struct NamespaceController {
    state: ControllerState,
}

impl NamespaceController {
    /// Create a new namespace controller.
    pub fn new(state: ControllerState) -> Self {
        Self { state }
    }

    /// Run the controller.
    pub async fn run(self) -> Result<()> {
        let client = self.state.client.clone();
        let api: Api<PangeaNamespace> = Api::all(client.clone());
        let state = Arc::new(self.state);

        info!("Starting PangeaNamespace controller");

        Controller::new(api, Config::default())
            .run(
                move |namespace, ctx| {
                    let state = Arc::clone(&ctx);
                    async move { reconcile_namespace(namespace, state).await }
                },
                error_policy,
                state,
            )
            .for_each(|result| async move {
                match result {
                    Ok((obj, action)) => {
                        debug!(name = %obj.name, ?action, "Namespace reconciliation completed");
                    }
                    Err(e) => {
                        error!(error = %e, "Namespace reconciliation failed");
                    }
                }
            })
            .await;

        Ok(())
    }
}

/// Reconcile a PangeaNamespace resource.
#[instrument(skip(state), fields(name = %namespace.name_any()))]
async fn reconcile_namespace(
    namespace: Arc<PangeaNamespace>,
    state: Arc<ControllerState>,
) -> std::result::Result<Action, Error> {
    let name = namespace.name_any();

    info!("Reconciling PangeaNamespace");
    state.metrics.namespace_reconciliations_total.inc();

    // Validate backend configuration
    validate_backend(&namespace)?;

    // Verify backend connectivity
    let backend_ready = match namespace.spec.backend.r#type {
        BackendType::Pg => verify_postgres_backend(&namespace, &state).await,
        BackendType::S3 => verify_s3_backend(&namespace).await,
        BackendType::Local => Ok(true),
    };

    // Update status
    let (ready, error_msg) = match backend_ready {
        Ok(true) => (true, None),
        Ok(false) => (false, Some("Backend verification pending".to_string())),
        Err(e) => (false, Some(e.to_string())),
    };

    update_status(&namespace, ready, error_msg, &state).await?;

    if ready {
        // Ensure PostgreSQL schema exists
        if namespace.uses_postgres() {
            ensure_schema(&namespace, &state).await?;
        }
        Ok(Action::requeue(DEFAULT_REQUEUE_INTERVAL))
    } else {
        Ok(Action::requeue(ERROR_REQUEUE_INTERVAL))
    }
}

/// Validate backend configuration.
fn validate_backend(namespace: &PangeaNamespace) -> Result<()> {
    match namespace.spec.backend.r#type {
        BackendType::Pg => {
            if namespace.spec.backend.pg.is_none() {
                return Err(Error::Config(
                    "PostgreSQL backend type requires 'pg' configuration".into(),
                ));
            }
        }
        BackendType::S3 => {
            if namespace.spec.backend.s3.is_none() {
                return Err(Error::Config(
                    "S3 backend type requires 's3' configuration".into(),
                ));
            }
        }
        BackendType::Local => {
            // No configuration required for local
        }
    }
    Ok(())
}

/// Verify PostgreSQL backend connectivity.
async fn verify_postgres_backend(
    namespace: &PangeaNamespace,
    state: &ControllerState,
) -> Result<bool> {
    let pg = namespace
        .spec
        .backend
        .pg
        .as_ref()
        .ok_or_else(|| Error::Config("Missing PostgreSQL configuration".into()))?;

    // TODO: Resolve credentials from Secret
    // TODO: Test connection to PostgreSQL

    debug!(
        host = %pg.host,
        port = %pg.port,
        database = %pg.database,
        "PostgreSQL backend configured"
    );

    // For now, assume ready if configuration exists
    Ok(true)
}

/// Verify S3 backend configuration.
async fn verify_s3_backend(namespace: &PangeaNamespace) -> Result<bool> {
    let s3 = namespace
        .spec
        .backend
        .s3
        .as_ref()
        .ok_or_else(|| Error::Config("Missing S3 configuration".into()))?;

    debug!(
        bucket = %s3.bucket,
        region = %s3.region,
        "S3 backend configured"
    );

    // S3 backend is legacy, mark as ready if configured
    Ok(true)
}

/// Ensure PostgreSQL schema exists for this namespace.
async fn ensure_schema(namespace: &PangeaNamespace, state: &ControllerState) -> Result<()> {
    let schema_name = namespace.schema_name();

    debug!(schema_name = %schema_name, "Ensuring PostgreSQL schema exists");

    // TODO: Connect to PostgreSQL and create schema if not exists
    // CREATE SCHEMA IF NOT EXISTS {schema_name};

    Ok(())
}

/// Update namespace status.
async fn update_status(
    namespace: &PangeaNamespace,
    backend_ready: bool,
    error: Option<String>,
    state: &ControllerState,
) -> Result<()> {
    let name = namespace.name_any();
    let api: Api<PangeaNamespace> = Api::all(state.client.clone());

    let status = PangeaNamespaceStatus {
        backend_ready,
        error,
        schema_name: if namespace.uses_postgres() {
            Some(namespace.schema_name())
        } else {
            None
        },
        last_verified_at: Some(chrono::Utc::now()),
        ..namespace.status.clone().unwrap_or_default()
    };

    let patch = serde_json::json!({ "status": status });

    api.patch_status(
        &name,
        &PatchParams::apply("pangea-operator"),
        &Patch::Merge(&patch),
    )
    .await?;

    info!(backend_ready, "Updated namespace status");
    Ok(())
}

/// Error policy for the namespace controller.
fn error_policy(
    _obj: Arc<PangeaNamespace>,
    error: &Error,
    _ctx: Arc<ControllerState>,
) -> Action {
    error!(%error, "Namespace reconciliation error");
    Action::requeue(ERROR_REQUEUE_INTERVAL)
}
