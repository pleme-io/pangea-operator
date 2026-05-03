//! Controller for PangeaNamespace resources.

use crate::crd::{BackendType, PangeaNamespace, PangeaNamespaceStatus};
use crate::error::{Error, Result};

use futures::StreamExt;
use kube::{
    api::{Api, Patch, PatchParams},
    runtime::{
        controller::{Action, Controller},
        watcher::Config,
    }, ResourceExt,
};
use std::sync::Arc;
use tracing::{debug, error, info, instrument};

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

    // Cluster-wide kill-switch — honor `OperatorPolicy/default`.
    if let Some(action) = crate::controller::policy_pipeline::run_for_controller(
        &state,
        crate::crd::ControllerKind::Namespace,
    )
    .into_skip_action()
    {
        return Ok(action);
    }

    // Per-CR suspend gate (the cluster-scoped policy is checked above).
    if namespace.spec.suspend {
        info!(name = %name, "PangeaNamespace suspended; skipping reconcile");
        return Ok(Action::requeue(DEFAULT_REQUEUE_INTERVAL));
    }

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
    _state: &ControllerState,
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
async fn ensure_schema(namespace: &PangeaNamespace, _state: &ControllerState) -> Result<()> {
    let schema_name = namespace.schema_name();

    debug!(schema_name = %schema_name, "Ensuring PostgreSQL schema exists");

    // TODO: Connect to PostgreSQL and create schema if not exists
    // CREATE SCHEMA IF NOT EXISTS {schema_name};

    Ok(())
}

/// Update namespace status.
///
/// Skips the patch if no meaningful field has changed AND
/// `last_verified_at` was bumped within the last minute. Without
/// this guard the reconcile loop is self-driving — every patch_status
/// triggers a watch event → another reconcile → another timestamp
/// bump → ~1 reconcile/sec forever, which API-floods the operator
/// and starves the template/flow controllers.
async fn update_status(
    namespace: &PangeaNamespace,
    backend_ready: bool,
    error: Option<String>,
    state: &ControllerState,
) -> Result<()> {
    let name = namespace.name_any();
    let api: Api<PangeaNamespace> = Api::all(state.client.clone());

    let now = chrono::Utc::now();
    let schema_name = if namespace.uses_postgres() {
        Some(namespace.schema_name())
    } else {
        None
    };

    let prev = namespace.status.as_ref();
    let prev_ready = prev.map(|s| s.backend_ready).unwrap_or(false);
    let prev_error = prev.and_then(|s| s.error.clone());
    let prev_schema = prev.and_then(|s| s.schema_name.clone());
    let last_verified_age = prev
        .and_then(|s| s.last_verified_at)
        .map(|t| now.signed_duration_since(t));

    let meaningful_change = prev_ready != backend_ready
        || prev_error != error
        || prev_schema != schema_name;
    let stale = last_verified_age
        .map(|d| d >= chrono::Duration::seconds(60))
        .unwrap_or(true);

    if !meaningful_change && !stale {
        debug!(backend_ready, "namespace status unchanged, skipping patch");
        return Ok(());
    }

    let status = PangeaNamespaceStatus {
        backend_ready,
        error,
        schema_name,
        last_verified_at: Some(now),
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
    ctx: Arc<ControllerState>,
) -> Action {
    crate::controller::error_policy::run_error_policy(
        &ctx.metrics,
        crate::crd::ControllerKind::Namespace,
        error,
        ERROR_REQUEUE_INTERVAL,
    )
}

#[cfg(test)]
mod tests {
    use crate::crd::{PangeaNamespace, PangeaNamespaceStatus};
    use kube::CustomResourceExt;

    #[test]
    fn status_default_round_trips() {
        let s = PangeaNamespaceStatus::default();
        let j = serde_json::to_string(&s).unwrap();
        let back: PangeaNamespaceStatus = serde_json::from_str(&j).unwrap();
        assert_eq!(format!("{:?}", s), format!("{:?}", back));
    }

    #[test]
    fn status_default_template_count_is_zero() {
        let s = PangeaNamespaceStatus::default();
        assert_eq!(s.template_count, 0);
    }

    #[test]
    fn crd_yaml_renders_cleanly() {
        let yaml = serde_yaml::to_string(&PangeaNamespace::crd()).expect("CRD serializes");
        assert!(yaml.contains("pangeanamespaces.pangea.pleme.io"));
        assert!(yaml.contains("- pangea"));
    }
}

#[cfg(test)]
mod deep_tests {
    use super::validate_backend;
    use crate::crd::{BackendConfig, BackendType, PangeaNamespace, PangeaNamespaceSpec};
    use kube::api::ObjectMeta;

    fn ns(backend: BackendConfig) -> PangeaNamespace {
        PangeaNamespace {
            metadata: ObjectMeta {
                name: Some("test-ns".into()),
                ..Default::default()
            },
            spec: PangeaNamespaceSpec {
                description: None,
                backend,
                default_tags: Default::default(),
                default_providers: None,
                default_compliance_profiles: vec![],
                suspend: false,
            },
            status: None,
        }
    }

    #[test]
    fn pg_backend_requires_pg_config() {
        let n = ns(BackendConfig {
            r#type: BackendType::Pg,
            pg: None,
            s3: None,
        });
        let r = validate_backend(&n);
        assert!(r.is_err());
        assert!(format!("{:?}", r.unwrap_err()).contains("PostgreSQL"));
    }

    #[test]
    fn s3_backend_requires_s3_config() {
        let n = ns(BackendConfig {
            r#type: BackendType::S3,
            pg: None,
            s3: None,
        });
        let r = validate_backend(&n);
        assert!(r.is_err());
        assert!(format!("{:?}", r.unwrap_err()).contains("S3"));
    }

    #[test]
    fn local_backend_requires_no_extra_config() {
        let n = ns(BackendConfig {
            r#type: BackendType::Local,
            pg: None,
            s3: None,
        });
        assert!(validate_backend(&n).is_ok());
    }
}
