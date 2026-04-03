//! PostgreSQL state backend for OpenTofu.
//!
//! This module provides PostgreSQL-based state storage for OpenTofu,
//! using the `pg` backend type with schema isolation per namespace.

mod postgres;
mod schema;
mod state;
mod lock;
mod config;

pub use postgres::PostgresBackend;
pub use schema::SchemaManager;
pub use state::StateStore;
pub use lock::{StateLock, LockGuard};
pub use config::{BackendConfigGenerator, AwsCredentialsConfig, CloudflareCredentialsConfig};

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
}

impl BackendManager {
    /// Create a new backend manager from a PangeaNamespace configuration.
    pub async fn from_namespace(
        namespace: &PangeaNamespace,
        credentials: Credentials,
    ) -> Result<Self> {
        let pg_config = namespace
            .spec
            .backend
            .pg
            .as_ref()
            .ok_or_else(|| crate::error::Error::Config("Missing PostgreSQL configuration".into()))?;

        let pool = PostgresBackend::connect(pg_config, credentials).await?;
        let pool = Arc::new(pool);

        Ok(Self {
            pool: pool.clone(),
            schema_manager: SchemaManager::new(pool.clone()),
            state_store: StateStore::new(pool.clone()),
            lock_manager: StateLock::new(pool.clone()),
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
