//! PostgreSQL advisory locks for state locking.
//!
//! Uses PostgreSQL's advisory lock mechanism to prevent concurrent
//! modifications to the same template's state.

use crate::error::{Error, Result};
use sqlx::PgPool;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use tracing::{debug, info, warn};

/// Manages state locks using PostgreSQL advisory locks.
#[derive(Clone)]
pub struct StateLock {
    pool: Arc<PgPool>,
}

/// Lock information for tracking.
#[derive(Debug, Clone)]
pub struct LockInfo {
    pub lock_id: i64,
    pub schema_name: String,
    pub template_name: String,
    pub holder: String,
    pub acquired_at: chrono::DateTime<chrono::Utc>,
}

/// RAII guard for automatic lock release.
pub struct LockGuard {
    pool: Arc<PgPool>,
    lock_id: i64,
    released: bool,
}

impl StateLock {
    /// Create a new state lock manager.
    pub fn new(pool: Arc<PgPool>) -> Self {
        Self { pool }
    }

    /// Try to acquire a lock for a template.
    ///
    /// Returns a LockGuard if successful, or an error if the lock is held.
    pub async fn try_acquire(
        &self,
        schema_name: &str,
        template_name: &str,
        holder: &str,
    ) -> Result<LockGuard> {
        let lock_id = compute_lock_id(schema_name, template_name);

        debug!(lock_id, schema_name, template_name, holder, "Trying to acquire lock");

        // Try to acquire session-level advisory lock (non-blocking)
        let row: (bool,) = sqlx::query_as("SELECT pg_try_advisory_lock($1)")
            .bind(lock_id)
            .fetch_one(self.pool.as_ref())
            .await
            .map_err(|e| Error::Database(e))?;

        if row.0 {
            info!(lock_id, schema_name, template_name, "Lock acquired");

            // Record lock in tracking table (best effort)
            let _ = self.record_lock(lock_id, schema_name, template_name, holder).await;

            Ok(LockGuard {
                pool: self.pool.clone(),
                lock_id,
                released: false,
            })
        } else {
            warn!(lock_id, schema_name, template_name, "Lock held by another process");
            Err(Error::LockFailed(format!(
                "State lock for {}/{} is held by another process",
                schema_name, template_name
            )))
        }
    }

    /// Wait to acquire a lock with timeout.
    pub async fn acquire_with_timeout(
        &self,
        schema_name: &str,
        template_name: &str,
        holder: &str,
        timeout_secs: u64,
    ) -> Result<LockGuard> {
        let lock_id = compute_lock_id(schema_name, template_name);
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(timeout_secs);

        debug!(lock_id, schema_name, template_name, timeout_secs, "Waiting for lock");

        while std::time::Instant::now() < deadline {
            match self.try_acquire(schema_name, template_name, holder).await {
                Ok(guard) => return Ok(guard),
                Err(Error::LockFailed(_)) => {
                    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                }
                Err(e) => return Err(e),
            }
        }

        Err(Error::Timeout(timeout_secs))
    }

    /// Force release a lock (for cleanup/recovery).
    pub async fn force_release(&self, schema_name: &str, template_name: &str) -> Result<bool> {
        let lock_id = compute_lock_id(schema_name, template_name);

        warn!(lock_id, schema_name, template_name, "Force releasing lock");

        let row: (bool,) = sqlx::query_as("SELECT pg_advisory_unlock($1)")
            .bind(lock_id)
            .fetch_one(self.pool.as_ref())
            .await
            .map_err(|e| Error::Database(e))?;

        if row.0 {
            // Remove from tracking table
            let _ = self.remove_lock_record(lock_id).await;
            info!(lock_id, "Lock force released");
        }

        Ok(row.0)
    }

    /// Check if a lock is held.
    pub async fn is_locked(&self, schema_name: &str, template_name: &str) -> Result<bool> {
        let lock_id = compute_lock_id(schema_name, template_name);

        // Check if we can acquire and immediately release
        let row: (bool,) = sqlx::query_as("SELECT pg_try_advisory_lock($1)")
            .bind(lock_id)
            .fetch_one(self.pool.as_ref())
            .await
            .map_err(|e| Error::Database(e))?;

        if row.0 {
            // We got it, release immediately
            let _: (bool,) = sqlx::query_as("SELECT pg_advisory_unlock($1)")
                .bind(lock_id)
                .fetch_one(self.pool.as_ref())
                .await
                .map_err(|e| Error::Database(e))?;
            Ok(false) // Not locked
        } else {
            Ok(true) // Locked by someone else
        }
    }

    /// Record lock acquisition in tracking table.
    async fn record_lock(
        &self,
        lock_id: i64,
        schema_name: &str,
        template_name: &str,
        holder: &str,
    ) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO pangea_meta.state_locks (lock_id, schema_name, template_name, holder, acquired_at)
            VALUES ($1, $2, $3, $4, NOW())
            ON CONFLICT (lock_id) DO UPDATE SET
                holder = EXCLUDED.holder,
                acquired_at = NOW()
            "#,
        )
        .bind(lock_id)
        .bind(schema_name)
        .bind(template_name)
        .bind(holder)
        .execute(self.pool.as_ref())
        .await
        .map_err(|e| Error::Database(e))?;

        Ok(())
    }

    /// Remove lock from tracking table.
    async fn remove_lock_record(&self, lock_id: i64) -> Result<()> {
        sqlx::query("DELETE FROM pangea_meta.state_locks WHERE lock_id = $1")
            .bind(lock_id)
            .execute(self.pool.as_ref())
            .await
            .map_err(|e| Error::Database(e))?;

        Ok(())
    }

    /// Ensure the lock tracking table exists.
    pub async fn ensure_lock_table(&self) -> Result<()> {
        // Create meta schema if not exists
        sqlx::query("CREATE SCHEMA IF NOT EXISTS pangea_meta")
            .execute(self.pool.as_ref())
            .await
            .map_err(|e| Error::Database(e))?;

        // Create lock tracking table
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS pangea_meta.state_locks (
                lock_id BIGINT PRIMARY KEY,
                schema_name VARCHAR(63) NOT NULL,
                template_name VARCHAR(253) NOT NULL,
                holder VARCHAR(255) NOT NULL,
                acquired_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            "#,
        )
        .execute(self.pool.as_ref())
        .await
        .map_err(|e| Error::Database(e))?;

        Ok(())
    }
}

impl LockGuard {
    /// Release the lock early.
    pub async fn release(mut self) -> Result<()> {
        self.do_release().await
    }

    async fn do_release(&mut self) -> Result<()> {
        if self.released {
            return Ok(());
        }

        let row: (bool,) = sqlx::query_as("SELECT pg_advisory_unlock($1)")
            .bind(self.lock_id)
            .fetch_one(self.pool.as_ref())
            .await
            .map_err(|e| Error::Database(e))?;

        self.released = true;

        if row.0 {
            debug!(lock_id = self.lock_id, "Lock released");
        } else {
            warn!(lock_id = self.lock_id, "Lock was not held (already released?)");
        }

        Ok(())
    }
}

impl Drop for LockGuard {
    fn drop(&mut self) {
        if !self.released {
            // Best effort release - can't await in drop
            let pool = self.pool.clone();
            let lock_id = self.lock_id;
            tokio::spawn(async move {
                let _ = sqlx::query("SELECT pg_advisory_unlock($1)")
                    .bind(lock_id)
                    .execute(pool.as_ref())
                    .await;
            });
        }
    }
}

/// Compute a stable lock ID from schema and template names.
fn compute_lock_id(schema_name: &str, template_name: &str) -> i64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    schema_name.hash(&mut hasher);
    template_name.hash(&mut hasher);
    hasher.finish() as i64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compute_lock_id() {
        let id1 = compute_lock_id("pangea_prod", "web_server");
        let id2 = compute_lock_id("pangea_prod", "web_server");
        let id3 = compute_lock_id("pangea_staging", "web_server");

        assert_eq!(id1, id2); // Same inputs = same ID
        assert_ne!(id1, id3); // Different inputs = different ID
    }
}
