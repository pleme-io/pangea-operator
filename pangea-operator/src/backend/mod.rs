//! PostgreSQL state backend for OpenTofu.
//!
//! This module provides PostgreSQL-based state storage for OpenTofu,
//! using the `pg` backend type with schema isolation per namespace.

mod artifact_store;
mod config;
mod lock;
mod postgres;
mod retry;
mod schema;
mod state;
pub mod state_backend;
mod tofu_pg_state_backend;

pub use artifact_store::ArtifactStore;
pub use config::{
    AwsCredentialsConfig, BackendConfigGenerator, CloudflareCredentialsConfig,
    PorkbunCredentialsConfig,
};
pub use lock::{LockGuard, StateLock};
pub use postgres::PostgresBackend;
pub use retry::{is_connection_level, RetryPolicy, RetryingStateBackend};
pub use schema::SchemaManager;
// `StateEntry`/`TerraformState` are the types the PUBLIC `StateBackend`
// trait's methods return (`state_backend.rs`) — any external mock
// implementation of that trait (see its own doc comment: "lets future
// tests inject `InMemoryStateBackend`... without testcontainers") needs
// to name them, which a private `mod state` alone does not allow
// (E0603 on the path, regardless of the structs' own `pub` modifier).
// Re-exported here so the trait's public interface is actually usable
// from outside this module, not just internally.
pub use state::{StateEntry, StateStore, TerraformState};
pub use state_backend::{InMemoryStateBackend, PostgresStateBackend, StateBackend};
pub use tofu_pg_state_backend::TofuPgStateBackend;

use crate::crd::PangeaNamespace;
use crate::error::Result;
use sqlx::PgPool;
use std::sync::Arc;

/// Backend manager that coordinates all PostgreSQL operations.
#[derive(Clone)]
pub struct BackendManager {
    pool: Arc<PgPool>,
    schema_manager: SchemaManager,
    state_store: StateStore,
    lock_manager: StateLock,
    artifact_store: ArtifactStore,
}

impl BackendManager {
    /// Create a new backend manager from a PangeaNamespace configuration.
    pub async fn from_namespace(
        namespace: &PangeaNamespace,
        credentials: Credentials,
    ) -> Result<Self> {
        let pg_config = namespace.spec.backend.pg.as_ref().ok_or_else(|| {
            crate::error::Error::Config("Missing PostgreSQL configuration".into())
        })?;

        let pool = PostgresBackend::connect(pg_config, credentials).await?;
        let pool = Arc::new(pool);

        Ok(Self {
            pool: pool.clone(),
            schema_manager: SchemaManager::new(pool.clone()),
            state_store: StateStore::new(pool.clone()),
            lock_manager: StateLock::new(pool.clone()),
            artifact_store: ArtifactStore::new(pool.clone()),
        })
    }

    /// Create from an existing pool.
    pub fn from_pool(pool: PgPool) -> Self {
        let pool = Arc::new(pool);
        Self {
            pool: pool.clone(),
            schema_manager: SchemaManager::new(pool.clone()),
            state_store: StateStore::new(pool.clone()),
            lock_manager: StateLock::new(pool.clone()),
            artifact_store: ArtifactStore::new(pool.clone()),
        }
    }

    /// Get the schema manager.
    pub fn schema_manager(&self) -> &SchemaManager {
        &self.schema_manager
    }

    /// Get the state store.
    pub fn state_store(&self) -> &StateStore {
        &self.state_store
    }

    /// Get the lock manager.
    pub fn lock_manager(&self) -> &StateLock {
        &self.lock_manager
    }

    /// Get the artifact store (durable reconcile artifacts on the magma
    /// path: rendered config / plan / bundle, all in Postgres).
    pub fn artifact_store(&self) -> &ArtifactStore {
        &self.artifact_store
    }

    /// Get the connection pool.
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }
}

/// Database credentials.
#[derive(Clone)]
pub struct Credentials {
    pub username: String,
    pub password: String,
    pub ca_cert: Option<String>,
}

impl std::fmt::Debug for Credentials {
    /// Redacts `password` (and elides `ca_cert` contents) so an
    /// accidental `{:?}`/`.expect_err()` on a `Result<Credentials, _>`
    /// — e.g. in a log line or test assertion — can never leak the
    /// database password.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Credentials")
            .field("username", &self.username)
            .field("password", &"<redacted>")
            .field("ca_cert", &self.ca_cert.as_ref().map(|_| "<present>"))
            .finish()
    }
}

impl Credentials {
    /// Create new credentials.
    pub fn new(username: impl Into<String>, password: impl Into<String>) -> Self {
        Self {
            username: username.into(),
            password: password.into(),
            ca_cert: None,
        }
    }

    /// Add a CA certificate.
    pub fn with_ca_cert(mut self, ca_cert: impl Into<String>) -> Self {
        self.ca_cert = Some(ca_cert.into());
        self
    }
}
