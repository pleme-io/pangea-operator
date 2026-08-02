//! Controller for PangeaNamespace resources.

use crate::backend::{BackendManager, Credentials};
use crate::crd::{
    BackendType, PangeaNamespace, PangeaNamespaceStatus, PostgresBackendConfig, PostgresSecretRef,
};
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
#[instrument(skip_all, fields(name = %namespace.name_any()))]
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

    // Prove the backend, THEN publish what was proved. The ordering is
    // the fix — see [`establish_backend`].
    let env = ControllerBackendEnv { state: &state };
    let readiness = establish_backend(&namespace, &env).await;

    update_status(&namespace, &readiness, &state).await?;

    if readiness.is_ready() {
        Ok(Action::requeue(DEFAULT_REQUEUE_INTERVAL))
    } else {
        Ok(Action::requeue(ERROR_REQUEUE_INTERVAL))
    }
}

/// What a reconcile actually PROVED about a namespace's backend.
///
/// The schema name lives inside [`NamespaceReadiness::Ready`] rather than
/// beside it, so the value that publishes a schema name is the same value
/// that carries the proof the schema was created. There is no way to hand
/// [`update_status`] a ready verdict and a schema name that came from
/// different places.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NamespaceReadiness {
    /// The backend answered, and its state schema exists.
    Ready {
        /// `Some` for Postgres, `None` for S3/local (which have no schema).
        schema_name: Option<String>,
    },
    /// Something the readiness claim depends on did not hold. Carries the
    /// reason so the CR says *why*, never just "false".
    NotReady { reason: String },
}

impl NamespaceReadiness {
    fn is_ready(&self) -> bool {
        matches!(self, NamespaceReadiness::Ready { .. })
    }
}

/// Verify the backend, provision its schema, and only then report ready.
///
/// # The bug this replaces
///
/// `reconcile_namespace` used to call `update_status(..., ready = true, …)`
/// and *then* call `ensure_schema`. When `ensure_schema` failed — which it
/// did on every reconcile for any namespace whose derived schema name the
/// DDL layer refused — the CR had already published `backendReady: true`
/// and `status.schemaName` for a schema that was never created. The `?` on
/// the schema call then returned the error to the controller runtime, so
/// nothing ever walked the status back: the false claim was written first
/// and the failure went to a log line.
///
/// That field is not decorative. `fleet_status_controller::aggregate_pangea_namespaces`
/// counts `backendReady` to answer "how many namespaces have a working
/// backend", so the fleet reported healthy for a backend that did not
/// exist — the readiness signal was inverted precisely for the namespaces
/// that were broken.
///
/// # Why the fix is an ordering, not a check
///
/// A readiness field is a claim about work already done, so the only
/// robust shape is to do the work first and let the claim be a report of
/// it. Adding a second "did the schema work?" check after the fact would
/// leave the same window open, just narrower.
///
/// Honest tier: this is **CI-gate-caught**, not unrepresentable. "The
/// schema exists in Postgres" is a fact held by the database, not a value
/// this process can own, so no type can make the bad pairing impossible —
/// [`NamespaceReadiness`] only makes it a data dependency (a ready verdict
/// has to be *constructed* past the provisioning step) and
/// `namespace_readiness_tests` pins both directions.
async fn establish_backend<E: NamespaceBackendEnv + ?Sized>(
    namespace: &PangeaNamespace,
    env: &E,
) -> NamespaceReadiness {
    // Verify connectivity. For Postgres this drives a REAL connection
    // (liveness-probed with `SELECT 1`) over whichever route actually
    // executes for this namespace — see [`verify_postgres_backend_with`].
    // The connected `BackendManager` is handed back and reused for schema
    // provisioning, so one reconcile tick pays for one connection.
    let verified = match namespace.spec.backend.r#type {
        BackendType::Pg => match verify_postgres_backend_with(namespace, env).await {
            Ok(v) => {
                info!(route = ?v.route, "PostgreSQL backend verified live");
                Some(v)
            }
            Err(e) => {
                return NamespaceReadiness::NotReady {
                    reason: e.to_string(),
                }
            }
        },
        BackendType::S3 => match verify_s3_backend(namespace).await {
            Ok(true) => None,
            Ok(false) => {
                return NamespaceReadiness::NotReady {
                    reason: "Backend verification pending".to_string(),
                }
            }
            Err(e) => {
                return NamespaceReadiness::NotReady {
                    reason: e.to_string(),
                }
            }
        },
        BackendType::Local => None,
    };

    // Provision the state schema BEFORE claiming ready. `verified` is
    // `Some` exactly when the backend is Postgres and the connectivity
    // check succeeded, so this cannot run without a live connection.
    let Some(v) = verified else {
        // S3 / local: nothing to provision and no schema to name.
        return NamespaceReadiness::Ready { schema_name: None };
    };

    let schema_name = namespace.schema_name();
    match env.ensure_schema(namespace, &v.manager).await {
        Ok(()) => NamespaceReadiness::Ready {
            schema_name: Some(schema_name),
        },
        Err(e) => {
            // Loud, and it names the schema — an operator reading the CR
            // has to be able to tell WHICH schema could not be created
            // without going to the operator's logs.
            error!(
                schema_name = %schema_name,
                error = %e,
                "backend is reachable but its state schema could not be created; \
                 reporting NOT ready (the state store is not usable yet)"
            );
            NamespaceReadiness::NotReady {
                reason: format!("schema {schema_name} could not be created: {e}"),
            }
        }
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

/// The identity of a Postgres database — *what* is addressed, not *how*
/// the connection is made.
///
/// Deliberately excludes username, password, SSL mode and pool sizing:
/// those are properties of a particular connection, not of the database
/// two connections are or are not sharing. Two routes that agree on
/// `(host, port, database)` are reading and writing the same rows, which
/// is exactly the question a readiness probe has to answer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PgCoordinates {
    pub host: String,
    pub port: u16,
    pub database: String,
}

impl PgCoordinates {
    /// Coordinates a `PangeaNamespace` DECLARES in `spec.backend.pg`.
    fn from_spec(pg: &PostgresBackendConfig) -> Self {
        Self {
            host: pg.host.clone(),
            port: pg.port,
            database: pg.database.clone(),
        }
    }

    /// Coordinates the operator's own live pool is ACTUALLY connected to
    /// — read back off the pool's own `PgConnectOptions`, so this can
    /// never drift from the connection magma really uses.
    fn from_pool(pool: &sqlx::PgPool) -> Self {
        let options = pool.connect_options();
        Self {
            host: options.get_host().to_string(),
            port: options.get_port(),
            database: options.get_database().unwrap_or_default().to_string(),
        }
    }
}

/// Which route proved (or must prove) a namespace's Postgres backend live.
///
/// The two routes are NOT interchangeable and the distinction is the
/// whole point of this type: `OperatorPool` is the route the **magma
/// executor** uses, `SecretRef` is the route the legacy **tofu**
/// executor uses. A probe that checks one while the executor uses the
/// other reports a verdict about a connection nothing exercises.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProbeRoute {
    /// The operator's process-wide Postgres pool — built at startup from
    /// `PGHOST`/`PGPORT`/`PGUSER`/`PGDATABASE`/`PGPASSWORD` (see
    /// `main.rs`) and the sole source of `ControllerState`'s
    /// `state_backend` / `artifact_store` / `state_lock`. Every magma
    /// plan, apply, state row and artifact row goes through it.
    OperatorPool,
    /// A connection opened from `spec.backend.pg.secretRef` — the
    /// coordinates the legacy tofu executor renders into
    /// `backend.tf.json` (`BackendConfigGenerator::generate_backend_config`).
    SecretRef,
}

/// A backend proven live, plus the connection that proved it (reused for
/// schema provisioning so one reconcile tick pays for one connection).
pub struct VerifiedBackend {
    pub route: ProbeRoute,
    pub manager: BackendManager,
}

/// Pick the route whose failure would actually mean "this namespace's
/// state storage does not work".
///
/// The rule is simply *follow the executor*: when the operator holds a
/// live pool to the same database the namespace declares, that pool IS
/// the namespace's backend and probing anything else is probing a
/// connection nothing uses. Only when there is no such pool — a DB-less
/// deployment, or a namespace pointing at a different database entirely
/// — does the `secretRef` route describe something real (the tofu path).
fn choose_probe_route(
    declared: &PgCoordinates,
    operator_pool: Option<&PgCoordinates>,
) -> ProbeRoute {
    match operator_pool {
        Some(pool) if pool == declared => ProbeRoute::OperatorPool,
        _ => ProbeRoute::SecretRef,
    }
}

/// The side effects a Postgres readiness probe needs, behind one
/// injectable seam so both verdict directions — reachable ⇒ ready and
/// unreachable ⇒ NOT ready — are testable without a live database or a
/// live apiserver.
#[async_trait::async_trait]
trait NamespaceBackendEnv: Send + Sync {
    /// Coordinates of the operator's own pool, or `None` when no pool is
    /// wired (`PGPASSWORD` unset / the startup connect failed).
    async fn operator_pool_coordinates(&self) -> Option<PgCoordinates>;

    /// Liveness-probe the operator's own pool and hand back a manager
    /// over it. Errors when the database is unreachable.
    async fn connect_operator_pool(&self) -> Result<BackendManager>;

    /// Resolve `spec.backend.pg.secretRef` and open a connection with
    /// those credentials. Errors when the Secret is missing, malformed,
    /// or the database is unreachable.
    async fn connect_via_secret_ref(&self, namespace: &PangeaNamespace) -> Result<BackendManager>;

    /// Create this namespace's state schema if it does not exist.
    ///
    /// On the seam rather than called directly by [`establish_backend`]
    /// so the FAILING direction is testable. The ordering bug this file
    /// fixes was invisible to the old suite for exactly that reason:
    /// provisioning could only be exercised against a live Postgres, so
    /// no test ever observed what the CR published when it failed.
    async fn ensure_schema(
        &self,
        namespace: &PangeaNamespace,
        manager: &BackendManager,
    ) -> Result<()>;
}

/// Verify PostgreSQL backend connectivity over the route that actually
/// executes for this namespace.
///
/// # The bug this replaces
///
/// The previous probe ALWAYS resolved `spec.backend.pg.secretRef` from a
/// Kubernetes Secret, looked up in `secretRef.namespace` → `POD_NAMESPACE`
/// → the hardcoded `"pangea-system"` fallback. On the magma path that is
/// the wrong question twice over:
///
///   * the operator does not connect with those credentials at all — it
///     connects with the `PG*` env vars wired into the pod (`main.rs`),
///     which is why 22k+ artifact-row updates could be landing in the
///     very database the probe was reporting unreachable; and
///   * with `POD_NAMESPACE` unset the lookup silently addressed
///     `pangea-system`, a namespace holding no Secrets, so the probe
///     returned `SecretNotFound` **forever** — a permanent false
///     negative that `fleet_status_controller::aggregate_pangea_namespaces`
///     then counted as "no namespace has a working backend".
///
/// A health signal that can only ever say one thing is a vacuous guard.
/// The fix is not to make the probe optimistic (that trades a false
/// negative for a strictly worse false positive) but to point it at the
/// executor's own connection, so both verdicts stay reachable and each
/// one means something.
async fn verify_postgres_backend_with<E: NamespaceBackendEnv + ?Sized>(
    namespace: &PangeaNamespace,
    env: &E,
) -> Result<VerifiedBackend> {
    let pg = namespace
        .spec
        .backend
        .pg
        .as_ref()
        .ok_or_else(|| Error::Config("Missing PostgreSQL configuration".into()))?;

    let declared = PgCoordinates::from_spec(pg);
    let pool_coords = env.operator_pool_coordinates().await;
    let route = choose_probe_route(&declared, pool_coords.as_ref());

    debug!(
        host = %declared.host,
        port = declared.port,
        database = %declared.database,
        ?route,
        "Verifying PostgreSQL backend connectivity"
    );

    match route {
        ProbeRoute::OperatorPool => {
            let manager = env.connect_operator_pool().await?;
            Ok(VerifiedBackend { route, manager })
        }
        ProbeRoute::SecretRef => {
            if let Some(pool) = pool_coords.as_ref() {
                // The operator HAS a pool, but to a different database
                // than this namespace declares. On the magma path the
                // executor will use the pool regardless, so the declared
                // coordinates describe storage nothing writes to. Say so
                // loudly rather than silently probing the unused route.
                warn!(
                    declared_host = %declared.host,
                    declared_port = declared.port,
                    declared_database = %declared.database,
                    pool_host = %pool.host,
                    pool_port = pool.port,
                    pool_database = %pool.database,
                    "PangeaNamespace declares a database the operator holds no pool to; \
                     falling back to the secretRef route (the tofu path). On the magma \
                     path the executor uses the operator pool, so these coordinates are \
                     not where state will land."
                );
            }
            let manager = env.connect_via_secret_ref(namespace).await?;
            Ok(VerifiedBackend { route, manager })
        }
    }
}

/// The live `ControllerState`-backed implementation of the probe seam.
struct ControllerBackendEnv<'a> {
    state: &'a ControllerState,
}

#[async_trait::async_trait]
impl NamespaceBackendEnv for ControllerBackendEnv<'_> {
    async fn operator_pool_coordinates(&self) -> Option<PgCoordinates> {
        let pool = self.state.db_pool.as_ref()?;
        let pool = pool.read().await;
        Some(PgCoordinates::from_pool(&pool))
    }

    async fn connect_operator_pool(&self) -> Result<BackendManager> {
        let pool = self.state.db_pool.as_ref().ok_or_else(|| {
            Error::StateBackend("operator Postgres pool is not wired".to_string())
        })?;
        let pool = pool.read().await.clone();

        // The liveness half. `main.rs` proves the pool CONNECTED once at
        // startup; this proves it is reachable NOW, which is what a
        // per-tick readiness field claims. An unreachable database (CNPG
        // switchover, network partition, credentials rotated out from
        // under a stale pool) fails here as a typed `Error::Database` —
        // so `backendReady=false` stays reachable and the probe is not
        // a guard that can only say one thing.
        sqlx::query("SELECT 1")
            .execute(&pool)
            .await
            .map_err(Error::Database)?;

        Ok(BackendManager::from_pool(pool))
    }

    async fn connect_via_secret_ref(&self, namespace: &PangeaNamespace) -> Result<BackendManager> {
        let pg = namespace
            .spec
            .backend
            .pg
            .as_ref()
            .ok_or_else(|| Error::Config("Missing PostgreSQL configuration".into()))?;
        let credentials = resolve_pg_credentials(&pg.secret_ref, self.state).await?;
        // `PostgresBackend::connect` opens a pool AND executes a
        // `SELECT 1` liveness probe while establishing it.
        BackendManager::from_namespace(namespace, credentials).await
    }

    async fn ensure_schema(
        &self,
        namespace: &PangeaNamespace,
        manager: &BackendManager,
    ) -> Result<()> {
        ensure_schema(namespace, manager).await
    }
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
    readiness: &NamespaceReadiness,
    state: &ControllerState,
) -> Result<()> {
    let name = namespace.name_any();
    let api: Api<PangeaNamespace> = Api::all(state.client.clone());

    let now = chrono::Utc::now();

    // Both fields are read off ONE value, so `backendReady: true` and a
    // published `schemaName` cannot come from different conclusions. On
    // the not-ready branch `schema_name` is `None`, which — being
    // `skip_serializing_if = "Option::is_none"` under a MERGE patch —
    // omits the key rather than nulling it: a name published by an
    // earlier successful reconcile is left alone, not silently erased,
    // and simply stops being re-asserted while the namespace is broken.
    let (backend_ready, schema_name, error) = match readiness {
        NamespaceReadiness::Ready { schema_name } => (true, schema_name.clone(), None),
        NamespaceReadiness::NotReady { reason } => (false, None, Some(reason.clone())),
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

/// Tests for the backend-readiness probe — the route choice and, more
/// importantly, that the probe can reach BOTH verdicts.
///
/// The bug these pin: `status.backendReady` was a permanent false
/// negative on camelot-eks. The probe always resolved
/// `spec.backend.pg.secretRef` out of `pangea-system` (the hardcoded
/// last-resort namespace, reached because `POD_NAMESPACE` is unset in
/// the deployment), a namespace holding no Secrets — while the magma
/// executor read and wrote the same Postgres continuously through the
/// operator's own `PG*`-env-built pool. Every namespace reported
/// `backendReady=false`, and `fleet_status_controller` counted zero
/// working backends fleet-wide.
///
/// A probe that can only ever return one answer proves nothing, so the
/// suite deliberately exercises the *unreachable* direction as hard as
/// the reachable one — including the case where a healthy but unrelated
/// operator pool must NOT be allowed to manufacture a ready verdict.
#[cfg(test)]
mod backend_probe_tests {
    use super::{
        choose_probe_route, verify_postgres_backend_with, NamespaceBackendEnv, PgCoordinates,
        ProbeRoute,
    };
    use crate::backend::BackendManager;
    use crate::crd::{
        BackendConfig, BackendType, PangeaNamespace, PangeaNamespaceSpec, PostgresBackendConfig,
        PostgresSecretRef,
    };
    use crate::error::{Error, Result};
    use kube::api::ObjectMeta;

    // ── fixtures ─────────────────────────────────────────────────────

    fn coords(host: &str, port: u16, database: &str) -> PgCoordinates {
        PgCoordinates {
            host: host.to_string(),
            port,
            database: database.to_string(),
        }
    }

    /// The live camelot-eks shape, verbatim: a cluster-scoped
    /// `PangeaNamespace` whose `secretRef` carries NO namespace (so the
    /// old probe fell through to `pangea-system`), pointing at the CNPG
    /// service the operator's own pool also targets.
    fn camelot_namespace() -> PangeaNamespace {
        pg_namespace(
            "pangea-database-rw.camelot.svc.cluster.local",
            5432,
            "pangea_state",
            "pangea-database-superuser",
        )
    }

    fn pg_namespace(host: &str, port: u16, database: &str, secret: &str) -> PangeaNamespace {
        PangeaNamespace {
            metadata: ObjectMeta {
                name: Some("camelot".into()),
                ..Default::default()
            },
            spec: PangeaNamespaceSpec {
                description: None,
                backend: BackendConfig {
                    r#type: BackendType::Pg,
                    pg: Some(PostgresBackendConfig {
                        host: host.to_string(),
                        port,
                        database: database.to_string(),
                        schema_prefix: "pangea_".to_string(),
                        ssl_mode: "disable".to_string(),
                        secret_ref: PostgresSecretRef {
                            name: secret.to_string(),
                            // Deliberately absent — this is what made the
                            // old probe address `pangea-system`.
                            namespace: None,
                            username_key: "username".to_string(),
                            password_key: "password".to_string(),
                            ca_cert_key: None,
                        },
                        pool: None,
                    }),
                    s3: None,
                },
                default_tags: Default::default(),
                default_providers: None,
                default_variables: None,
                default_compliance_profiles: vec![],
                suspend: false,
            },
            status: None,
        }
    }

    /// A `BackendManager` over a LAZY pool — constructed without dialing
    /// anything, so a mocked "reachable" route can hand back the same
    /// type the real route does.
    fn lazy_manager() -> BackendManager {
        let pool = sqlx::PgPool::connect_lazy("postgres://u:p@localhost:5432/pangea_state")
            .expect("a well-formed URL parses without connecting");
        BackendManager::from_pool(pool)
    }

    /// Injectable stand-in for the two real routes. Each route is
    /// independently switchable between reachable and unreachable, which
    /// is what makes the both-directions assertions below non-vacuous.
    pub(super) struct MockEnv {
        pool_coordinates: Option<PgCoordinates>,
        pool_reachable: std::result::Result<(), Error>,
        secret_ref_reachable: std::result::Result<(), Error>,
        /// `Some(reason)` makes `CREATE SCHEMA` fail. Independent of
        /// reachability on purpose: the ordering bug lives exactly in the
        /// combination "backend answers, schema creation fails", which is
        /// unreachable if the two are welded together.
        schema_failure: Option<String>,
    }

    impl MockEnv {
        pub(super) fn new() -> Self {
            Self {
                pool_coordinates: None,
                pool_reachable: Ok(()),
                secret_ref_reachable: Ok(()),
                schema_failure: None,
            }
        }

        pub(super) fn with_pool_at(mut self, c: PgCoordinates) -> Self {
            self.pool_coordinates = Some(c);
            self
        }

        pub(super) fn pool_unreachable(mut self, why: &str) -> Self {
            self.pool_reachable = Err(Error::StateBackend(why.to_string()));
            self
        }

        fn secret_missing(mut self, namespace: &str, name: &str) -> Self {
            self.secret_ref_reachable = Err(Error::SecretNotFound {
                namespace: namespace.to_string(),
                name: name.to_string(),
            });
            self
        }

        pub(super) fn schema_creation_fails(mut self, why: &str) -> Self {
            self.schema_failure = Some(why.to_string());
            self
        }
    }

    // `Error` is not `Clone`, so re-materialize rather than clone.
    fn replay(outcome: &std::result::Result<(), Error>) -> Result<BackendManager> {
        match outcome {
            Ok(()) => Ok(lazy_manager()),
            Err(e) => Err(Error::StateBackend(e.to_string())),
        }
    }

    #[async_trait::async_trait]
    impl NamespaceBackendEnv for MockEnv {
        async fn operator_pool_coordinates(&self) -> Option<PgCoordinates> {
            self.pool_coordinates.clone()
        }

        async fn connect_operator_pool(&self) -> Result<BackendManager> {
            replay(&self.pool_reachable)
        }

        async fn connect_via_secret_ref(
            &self,
            _namespace: &PangeaNamespace,
        ) -> Result<BackendManager> {
            replay(&self.secret_ref_reachable)
        }

        async fn ensure_schema(
            &self,
            _namespace: &PangeaNamespace,
            _manager: &BackendManager,
        ) -> Result<()> {
            match &self.schema_failure {
                None => Ok(()),
                Some(why) => Err(Error::Config(why.clone())),
            }
        }
    }

    // ── route choice (pure) ──────────────────────────────────────────

    #[test]
    fn matching_coordinates_route_to_the_operator_pool() {
        let declared = coords(
            "pangea-database-rw.camelot.svc.cluster.local",
            5432,
            "pangea_state",
        );
        let pool = declared.clone();
        assert_eq!(
            choose_probe_route(&declared, Some(&pool)),
            ProbeRoute::OperatorPool
        );
    }

    #[test]
    fn no_operator_pool_routes_to_the_secret_ref() {
        let declared = coords("db", 5432, "pangea_state");
        assert_eq!(choose_probe_route(&declared, None), ProbeRoute::SecretRef);
    }

    #[test]
    fn each_coordinate_alone_is_enough_to_diverge() {
        let declared = coords("db", 5432, "pangea_state");
        for pool in [
            coords("other-db", 5432, "pangea_state"),
            coords("db", 5433, "pangea_state"),
            coords("db", 5432, "some_other_database"),
        ] {
            assert_eq!(
                choose_probe_route(&declared, Some(&pool)),
                ProbeRoute::SecretRef,
                "a pool to a different database must not be treated as this namespace's backend: \
                 {pool:?}"
            );
        }
    }

    // `#[tokio::test]`, not `#[test]`: `PgPool::connect_lazy` installs an
    // idle reaper and panics outside a Tokio context.
    #[tokio::test]
    async fn pool_coordinates_are_read_off_the_pool_itself() {
        // The coordinates must come from the connection, never from a
        // parallel copy of the config that could drift from it.
        let pool = sqlx::PgPool::connect_lazy("postgres://u:p@some-host:6432/some_db").unwrap();
        assert_eq!(
            PgCoordinates::from_pool(&pool),
            coords("some-host", 6432, "some_db")
        );
    }

    // ── both directions ──────────────────────────────────────────────

    #[tokio::test]
    async fn reachable_operator_pool_reports_ready() {
        let ns = camelot_namespace();
        let env = MockEnv::new().with_pool_at(coords(
            "pangea-database-rw.camelot.svc.cluster.local",
            5432,
            "pangea_state",
        ));

        let verified = verify_postgres_backend_with(&ns, &env)
            .await
            .expect("a live operator pool to the declared database is a working backend");
        assert_eq!(verified.route, ProbeRoute::OperatorPool);
    }

    #[tokio::test]
    async fn unreachable_operator_pool_reports_not_ready() {
        // The other direction. Same namespace, same route — only the
        // database's reachability changes, and the verdict must follow
        // it. Without this the probe would be a guard that can only say
        // "ready", which is strictly worse than the bug being fixed.
        let ns = camelot_namespace();
        let env = MockEnv::new()
            .with_pool_at(coords(
                "pangea-database-rw.camelot.svc.cluster.local",
                5432,
                "pangea_state",
            ))
            .pool_unreachable("connection refused");

        let err = verify_postgres_backend_with(&ns, &env)
            .await
            .err()
            .expect("an unreachable database must NOT report ready");
        assert!(
            err.to_string().contains("connection refused"),
            "the failure must name what went wrong: {err}"
        );
    }

    #[tokio::test]
    async fn secret_ref_route_reports_both_directions_too() {
        let ns = camelot_namespace();

        // No operator pool (a DB-less / tofu deployment): the secretRef
        // route is the real one, and a working one reports ready.
        let ok = verify_postgres_backend_with(&ns, &MockEnv::new())
            .await
            .expect("a working secretRef connection is a working backend");
        assert_eq!(ok.route, ProbeRoute::SecretRef);

        // ...and a broken one does not.
        let env = MockEnv::new().secret_missing("pangea-system", "pangea-database-superuser");
        assert!(
            verify_postgres_backend_with(&ns, &env).await.is_err(),
            "a missing credentials Secret on the route that IS in use must report not-ready"
        );
    }

    // ── the regression + the false-positive guard ────────────────────

    #[tokio::test]
    async fn live_camelot_shape_no_longer_consults_the_empty_pangea_system_namespace() {
        // Exactly what was observed on camelot-eks: the Secret named by
        // the CR does not exist in `pangea-system` (nothing does), yet
        // the operator's pool is connected to that very database and
        // magma writes to it every cycle. The probe must follow the
        // executor and report ready.
        let ns = camelot_namespace();
        let env = MockEnv::new()
            .with_pool_at(coords(
                "pangea-database-rw.camelot.svc.cluster.local",
                5432,
                "pangea_state",
            ))
            .secret_missing("pangea-system", "pangea-database-superuser");

        let verified = verify_postgres_backend_with(&ns, &env)
            .await
            .expect("the live, working executor connection decides readiness");
        assert_eq!(verified.route, ProbeRoute::OperatorPool);
    }

    #[tokio::test]
    async fn a_healthy_unrelated_pool_cannot_manufacture_a_ready_verdict() {
        // The anti-false-positive case. The operator holds a perfectly
        // healthy pool — to a DIFFERENT database. That proves nothing
        // about this namespace's declared backend, so the probe must
        // fall to the secretRef route and report its real failure.
        let ns = pg_namespace("elsewhere.svc", 5432, "other_state", "some-secret");
        let env = MockEnv::new()
            .with_pool_at(coords(
                "pangea-database-rw.camelot.svc.cluster.local",
                5432,
                "pangea_state",
            ))
            .secret_missing("pangea-system", "some-secret");

        assert!(
            verify_postgres_backend_with(&ns, &env).await.is_err(),
            "a healthy pool to an unrelated database must not be borrowed as proof"
        );
    }

    #[tokio::test]
    async fn a_namespace_without_pg_config_is_a_typed_error_not_a_ready_verdict() {
        let mut ns = camelot_namespace();
        ns.spec.backend.pg = None;
        assert!(verify_postgres_backend_with(&ns, &MockEnv::new())
            .await
            .is_err());
    }

    /// Fixture reuse for the readiness suite below, which needs the same
    /// live camelot shape but drives the whole verify→provision→publish
    /// sequence rather than just the probe.
    pub(super) fn camelot_namespace_fixture() -> PangeaNamespace {
        camelot_namespace()
    }

    pub(super) fn coords_fixture(host: &str, port: u16, database: &str) -> PgCoordinates {
        coords(host, port, database)
    }
}

/// [`establish_backend`] — readiness is published only after the thing it
/// claims is true.
///
/// The suite the old code did not have. `backendReady` was set BEFORE
/// `ensure_schema` ran, so the one state that mattered — backend
/// reachable, schema creation failed — had no test and no way to be
/// reached without a live Postgres. Every assertion here is about that
/// combination or about not over-correcting it into the opposite lie.
#[cfg(test)]
mod namespace_readiness_tests {
    use super::backend_probe_tests::{camelot_namespace_fixture, coords_fixture, MockEnv};
    use super::{establish_backend, NamespaceReadiness};
    use crate::crd::BackendType;

    fn operator_pool_env() -> MockEnv {
        MockEnv::new().with_pool_at(coords_fixture(
            "pangea-database-rw.camelot.svc.cluster.local",
            5432,
            "pangea_state",
        ))
    }

    // ── the regression ───────────────────────────────────────────────

    #[tokio::test]
    async fn a_failed_schema_creation_is_never_published_as_ready() {
        // THE BUG, pinned. The backend is perfectly reachable — the probe
        // succeeds, exactly as it does on camelot-eks — and only the
        // CREATE SCHEMA fails. The old ordering published
        // `backendReady: true` here and then returned the error, so the
        // fleet counted this namespace as having a working backend.
        let ns = camelot_namespace_fixture();
        let env = operator_pool_env().schema_creation_fails("Invalid schema name: pangea_x-y");

        let readiness = establish_backend(&ns, &env).await;

        assert!(
            !matches!(readiness, NamespaceReadiness::Ready { .. }),
            "a schema that was never created must not be reported ready: {readiness:?}"
        );
    }

    #[tokio::test]
    async fn the_failure_reason_names_the_schema_and_the_cause() {
        // "Not ready" with no reason is only marginally better than the
        // false positive — an operator still has to go read logs. The
        // brief says keep the error path informative, so the reason must
        // carry BOTH which schema and why.
        let ns = camelot_namespace_fixture();
        let env = operator_pool_env().schema_creation_fails("Invalid schema name: pangea_camelot");

        match establish_backend(&ns, &env).await {
            NamespaceReadiness::NotReady { reason } => {
                assert!(
                    reason.contains("pangea_camelot"),
                    "the reason must name the schema that failed: {reason}"
                );
                assert!(
                    reason.contains("Invalid schema name"),
                    "the reason must carry the underlying cause: {reason}"
                );
            }
            other => panic!("expected NotReady, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn a_not_ready_verdict_carries_no_schema_name_to_publish() {
        // The other half of the false claim. `status.schemaName` named a
        // schema that did not exist; the type now makes the name
        // unreachable from a failed verdict, so `update_status` has
        // nothing to publish.
        let ns = camelot_namespace_fixture();
        let env = operator_pool_env().schema_creation_fails("nope");

        let readiness = establish_backend(&ns, &env).await;
        // `Ready { schema_name }` is the ONLY variant with the field —
        // there is no accessor to misuse on the failing branch.
        assert_eq!(
            readiness,
            NamespaceReadiness::NotReady {
                reason: "schema pangea_camelot could not be created: Configuration error: nope"
                    .to_string()
            }
        );
    }

    // ── the other direction, so the gate is not vacuous ──────────────

    #[tokio::test]
    async fn a_reachable_backend_with_a_created_schema_is_ready_and_names_it() {
        // Without this the fix could be "always report not ready", which
        // is a worse signal than the bug. Readiness must stay reachable.
        let ns = camelot_namespace_fixture();

        assert_eq!(
            establish_backend(&ns, &operator_pool_env()).await,
            NamespaceReadiness::Ready {
                schema_name: Some("pangea_camelot".to_string())
            }
        );
    }

    #[tokio::test]
    async fn an_unreachable_backend_never_reaches_the_schema_step() {
        // Ordering in the other direction: a dead database must fail on
        // connectivity and report THAT, not a schema error, and must not
        // be rescued by a mock whose schema step happens to succeed.
        let ns = camelot_namespace_fixture();
        let env = MockEnv::new()
            .with_pool_at(coords_fixture(
                "pangea-database-rw.camelot.svc.cluster.local",
                5432,
                "pangea_state",
            ))
            .pool_unreachable("connection refused");

        match establish_backend(&ns, &env).await {
            NamespaceReadiness::NotReady { reason } => assert!(
                reason.contains("connection refused"),
                "the connectivity failure must be the reported reason: {reason}"
            ),
            other => panic!("expected NotReady, got {other:?}"),
        }
    }

    // ── backends with no schema to provision ─────────────────────────

    #[tokio::test]
    async fn s3_and_local_backends_are_ready_without_a_schema_name() {
        // Neither has a Postgres schema, so neither may publish one —
        // and neither may be dragged into not-ready by a schema step
        // that does not apply to it.
        for backend in [BackendType::S3, BackendType::Local] {
            let mut ns = camelot_namespace_fixture();
            ns.spec.backend.r#type = backend;
            ns.spec.backend.s3 = Some(crate::crd::S3BackendConfig {
                bucket: "b".to_string(),
                region: "us-east-2".to_string(),
                key_prefix: None,
                dynamodb_table: None,
                secret_ref: None,
            });

            assert_eq!(
                establish_backend(&ns, &MockEnv::new().schema_creation_fails("unreachable")).await,
                NamespaceReadiness::Ready { schema_name: None },
                "{backend:?} has no schema step to fail"
            );
        }
    }
}
