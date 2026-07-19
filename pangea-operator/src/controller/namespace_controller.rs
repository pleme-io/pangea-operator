//! Controller for PangeaNamespace resources.

use crate::backend::{BackendManager, Credentials};
use crate::crd::{BackendType, PangeaNamespace, PangeaNamespaceStatus, PostgresSecretRef};
use crate::error::{Error, Result};

use futures::StreamExt;
use k8s_openapi::api::core::v1::Secret;
use k8s_openapi::ByteString;
use kube::{
    api::{Api, Patch, PatchParams},
    runtime::controller::Action,
    ResourceExt,
};
use std::collections::BTreeMap;
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
        let state = Arc::new(self.state);

        info!("Starting PangeaNamespace controller");

        crate::controller::generation_filter::filtered_controller::<PangeaNamespace>(client)
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
    // Per-controller reconcile counter — completes the denominator
    // for the chart 0.8.14 PangeaControllerReconcileRateHigh alert
    // (the standalone `namespace_reconciliations_total` counter
    // above predates the labeled metric and stays for back-compat).
    state
        .metrics
        .record_reconcile(crate::crd::ControllerKind::Namespace, "ok");

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

    // Verify backend connectivity. For Postgres this resolves the
    // credentials Secret and opens a real connection (which
    // `PostgresBackend::connect` liveness-probes with `SELECT 1`) —
    // the connected `BackendManager` is handed back and reused below
    // for schema provisioning, so one reconcile tick pays for one
    // connection pool, not two.
    let mut pg_manager: Option<BackendManager> = None;
    let backend_ready: Result<bool> = match namespace.spec.backend.r#type {
        BackendType::Pg => match verify_postgres_backend(&namespace, &state).await {
            Ok(manager) => {
                pg_manager = Some(manager);
                Ok(true)
            }
            Err(e) => Err(e),
        },
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
        // Ensure PostgreSQL schema exists — reuses the connection
        // established by `verify_postgres_backend` above. `pg_manager`
        // is `Some` exactly when the backend is Postgres AND the
        // connectivity check succeeded, so this is unrepresentable
        // without a live, verified connection.
        if let Some(manager) = pg_manager.as_ref() {
            ensure_schema(&namespace, manager).await?;
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
///
/// Resolves `spec.backend.pg.secretRef` from the referenced Kubernetes
/// Secret and drives a real `BackendManager::from_namespace` connect.
/// `PostgresBackend::connect` opens a pool AND executes a `SELECT 1`
/// liveness probe while establishing it, so a bad host, expired
/// credentials, or an unreachable database surface here as a typed
/// `Error::Database` / `Error::Config` / `Error::SecretNotFound` —
/// never the previous "pg config is syntactically present ⇒ ready"
/// false positive. On success, returns the connected manager for reuse
/// by [`ensure_schema`].
async fn verify_postgres_backend(
    namespace: &PangeaNamespace,
    state: &ControllerState,
) -> Result<BackendManager> {
    let pg = namespace
        .spec
        .backend
        .pg
        .as_ref()
        .ok_or_else(|| Error::Config("Missing PostgreSQL configuration".into()))?;

    debug!(
        host = %pg.host,
        port = %pg.port,
        database = %pg.database,
        "Verifying PostgreSQL backend connectivity"
    );

    let credentials = resolve_pg_credentials(&pg.secret_ref, state).await?;

    BackendManager::from_namespace(namespace, credentials).await
}

/// Resolve PostgreSQL credentials from the Kubernetes Secret referenced
/// by `secretRef`. Mirrors the Secret-fetch pattern in
/// `controller::template::provider_creds` (get-or-`SecretNotFound`,
/// decode `data` values as UTF-8), specialized to the
/// username/password/CA-cert keys `PostgresSecretRef` declares.
async fn resolve_pg_credentials(
    secret_ref: &PostgresSecretRef,
    state: &ControllerState,
) -> Result<Credentials> {
    let ns = resolve_secret_namespace(
        secret_ref.namespace.as_deref(),
        std::env::var("POD_NAMESPACE").ok().as_deref(),
    );

    let secret_api: Api<Secret> = Api::namespaced(state.client.clone(), &ns);
    let secret = secret_api
        .get(&secret_ref.name)
        .await
        .map_err(|_| Error::SecretNotFound {
            namespace: ns.clone(),
            name: secret_ref.name.clone(),
        })?;

    let data = secret.data.as_ref().ok_or_else(|| {
        Error::Config(format!(
            "PostgreSQL credentials secret {}/{} has no data",
            ns, secret_ref.name
        ))
    })?;

    credentials_from_secret_data(secret_ref, &ns, data)
}

/// Pure extraction of [`Credentials`] from a Secret's decoded `data`
/// map — split out from [`resolve_pg_credentials`] so the key-lookup
/// logic (default vs. custom `username_key`/`password_key`/
/// `ca_cert_key`, and the typed errors when a declared key is missing)
/// is unit-testable without a live Kubernetes API.
fn credentials_from_secret_data(
    secret_ref: &PostgresSecretRef,
    secret_namespace: &str,
    data: &BTreeMap<String, ByteString>,
) -> Result<Credentials> {
    let username = data
        .get(secret_ref.username_key.as_str())
        .map(|v| String::from_utf8_lossy(&v.0).to_string())
        .ok_or_else(|| {
            Error::Config(format!(
                "key '{}' not found in PostgreSQL secret {}/{}",
                secret_ref.username_key, secret_namespace, secret_ref.name
            ))
        })?;

    let password = data
        .get(secret_ref.password_key.as_str())
        .map(|v| String::from_utf8_lossy(&v.0).to_string())
        .ok_or_else(|| {
            Error::Config(format!(
                "key '{}' not found in PostgreSQL secret {}/{}",
                secret_ref.password_key, secret_namespace, secret_ref.name
            ))
        })?;

    let mut credentials = Credentials::new(username, password);

    // A declared `ca_cert_key` that isn't actually present in the
    // Secret fails loudly (a misconfigured/rotated Secret must not
    // silently drop custom-CA verification and connect as if it
    // weren't configured — no silent security downgrades).
    if let Some(ca_cert_key) = secret_ref.ca_cert_key.as_deref() {
        let ca_cert = data
            .get(ca_cert_key)
            .map(|v| String::from_utf8_lossy(&v.0).to_string())
            .ok_or_else(|| {
                Error::Config(format!(
                    "key '{}' not found in PostgreSQL secret {}/{}",
                    ca_cert_key, secret_namespace, secret_ref.name
                ))
            })?;
        credentials = credentials.with_ca_cert(ca_cert);
    }

    Ok(credentials)
}

/// Resolve the namespace to look up a `PostgresSecretRef` in.
///
/// `PangeaNamespace` is cluster-scoped (unlike `InfrastructureTemplate`,
/// it has no owning namespace of its own to default a bare secret
/// reference to), so an explicit `secretRef.namespace` wins, falling
/// back to `POD_NAMESPACE` and finally the fleet-wide `"pangea-system"`
/// convention already used by `leader::LeaderConfig::from_env` /
/// `config::detect_identity`. Takes the env value as a parameter (rather
/// than reading `std::env::var` itself) so this resolution logic is
/// unit-testable without mutating global process state.
fn resolve_secret_namespace(explicit: Option<&str>, pod_namespace_env: Option<&str>) -> String {
    explicit
        .filter(|s| !s.is_empty())
        .or_else(|| pod_namespace_env.filter(|s| !s.is_empty()))
        .unwrap_or("pangea-system")
        .to_string()
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
///
/// Issues a real `CREATE SCHEMA IF NOT EXISTS` via the already-connected
/// `BackendManager`'s `SchemaManager` — the shipped, previously-unused
/// `backend::SchemaManager::ensure_schema` (which validates the schema
/// name against SQL-injection before executing).
async fn ensure_schema(namespace: &PangeaNamespace, manager: &BackendManager) -> Result<()> {
    let schema_name = namespace.schema_name();

    debug!(schema_name = %schema_name, "Ensuring PostgreSQL schema exists");

    manager.schema_manager().ensure_schema(&schema_name).await
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

    let meaningful_change =
        prev_ready != backend_ready || prev_error != error || prev_schema != schema_name;
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
fn error_policy(_obj: Arc<PangeaNamespace>, error: &Error, ctx: Arc<ControllerState>) -> Action {
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
                default_variables: None,
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

/// Tests for the Secret → `Credentials` resolution logic that
/// [`verify_postgres_backend`]/[`ensure_schema`] depend on. Before this
/// change, `verify_postgres_backend` never called any of this code —
/// it unconditionally returned `Ok(true)` whenever `spec.backend.pg`
/// was merely present. These tests exercise the real credential
/// extraction now load-bearing for `PangeaNamespace.status.backendReady`.
#[cfg(test)]
mod credential_resolution_tests {
    use super::{credentials_from_secret_data, resolve_secret_namespace};
    use crate::crd::PostgresSecretRef;
    use k8s_openapi::ByteString;
    use std::collections::BTreeMap;

    fn secret_ref() -> PostgresSecretRef {
        PostgresSecretRef {
            name: "db-secret".to_string(),
            namespace: None,
            username_key: "username".to_string(),
            password_key: "password".to_string(),
            ca_cert_key: None,
        }
    }

    fn data(pairs: &[(&str, &str)]) -> BTreeMap<String, ByteString> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), ByteString(v.as_bytes().to_vec())))
            .collect()
    }

    #[test]
    fn resolves_username_and_password_with_default_keys() {
        let d = data(&[("username", "pangea"), ("password", "s3cr3t")]);
        let creds = credentials_from_secret_data(&secret_ref(), "pangea-system", &d)
            .expect("username+password present");
        assert_eq!(creds.username, "pangea");
        assert_eq!(creds.password, "s3cr3t");
        assert!(creds.ca_cert.is_none());
    }

    #[test]
    fn resolves_with_custom_key_names() {
        let sref = PostgresSecretRef {
            username_key: "db_user".to_string(),
            password_key: "db_pwd".to_string(),
            ..secret_ref()
        };
        let d = data(&[("db_user", "custom-user"), ("db_pwd", "custom-pass")]);
        let creds =
            credentials_from_secret_data(&sref, "pangea-system", &d).expect("custom keys present");
        assert_eq!(creds.username, "custom-user");
        assert_eq!(creds.password, "custom-pass");
    }

    /// The load-bearing regression case: a `PostgresSecretRef` whose
    /// Secret exists but is missing the username key must NOT silently
    /// resolve to empty/absent credentials — it must be a typed,
    /// diagnosable error (this is exactly the class of bug that let
    /// `verify_postgres_backend` previously report `backendReady=true`
    /// with no real credentials at all).
    #[test]
    fn missing_username_key_is_a_typed_error() {
        let d = data(&[("password", "s3cr3t")]);
        let err = credentials_from_secret_data(&secret_ref(), "pangea-system", &d)
            .expect_err("missing username must error, not silently succeed");
        let msg = format!("{err}");
        assert!(
            msg.contains("username"),
            "error should name the missing key: {msg}"
        );
        assert!(
            msg.contains("db-secret"),
            "error should name the secret: {msg}"
        );
    }

    #[test]
    fn missing_password_key_is_a_typed_error() {
        let d = data(&[("username", "pangea")]);
        let err = credentials_from_secret_data(&secret_ref(), "pangea-system", &d)
            .expect_err("missing password must error, not silently succeed");
        assert!(format!("{err}").contains("password"));
    }

    #[test]
    fn resolves_ca_cert_when_key_present() {
        let sref = PostgresSecretRef {
            ca_cert_key: Some("ca.crt".to_string()),
            ..secret_ref()
        };
        let d = data(&[
            ("username", "pangea"),
            ("password", "s3cr3t"),
            (
                "ca.crt",
                "-----BEGIN CERTIFICATE-----\n...\n-----END CERTIFICATE-----",
            ),
        ]);
        let creds =
            credentials_from_secret_data(&sref, "pangea-system", &d).expect("ca cert present");
        assert!(creds.ca_cert.unwrap().contains("BEGIN CERTIFICATE"));
    }

    /// A declared `caCertKey` that isn't actually in the Secret must
    /// fail loudly rather than silently connecting without custom-CA
    /// verification — a misconfigured Secret must not downgrade TLS
    /// trust silently.
    #[test]
    fn declared_but_missing_ca_cert_key_is_a_typed_error() {
        let sref = PostgresSecretRef {
            ca_cert_key: Some("ca.crt".to_string()),
            ..secret_ref()
        };
        let d = data(&[("username", "pangea"), ("password", "s3cr3t")]);
        let err = credentials_from_secret_data(&sref, "pangea-system", &d)
            .expect_err("declared but absent ca_cert_key must error");
        assert!(format!("{err}").contains("ca.crt"));
    }

    #[test]
    fn no_ca_cert_key_declared_yields_no_ca_cert_and_no_error() {
        let d = data(&[("username", "pangea"), ("password", "s3cr3t")]);
        let creds = credentials_from_secret_data(&secret_ref(), "pangea-system", &d).unwrap();
        assert!(creds.ca_cert.is_none());
    }

    #[test]
    fn explicit_namespace_wins() {
        assert_eq!(
            resolve_secret_namespace(Some("db-ns"), Some("pod-ns")),
            "db-ns"
        );
    }

    #[test]
    fn falls_back_to_pod_namespace_env_when_no_explicit_namespace() {
        assert_eq!(resolve_secret_namespace(None, Some("pod-ns")), "pod-ns");
    }

    #[test]
    fn falls_back_to_pangea_system_when_nothing_set() {
        assert_eq!(resolve_secret_namespace(None, None), "pangea-system");
    }

    #[test]
    fn empty_explicit_namespace_falls_through_like_none() {
        assert_eq!(resolve_secret_namespace(Some(""), Some("pod-ns")), "pod-ns");
    }
}
