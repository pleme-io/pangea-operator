//! State storage operations for OpenTofu state.
//!
//! Provides read/write access to OpenTofu state stored in PostgreSQL,
//! following the pg backend table format.

use crate::error::{Error, Result};
use chrono::{DateTime, Utc};
use sqlx::PgPool;
use std::sync::Arc;
use tracing::{debug, info};

/// Manages OpenTofu state in PostgreSQL.
#[derive(Clone)]
pub struct StateStore {
    pool: Arc<PgPool>,
}

/// Represents a stored state entry.
#[derive(Debug, Clone)]
pub struct StateEntry {
    pub id: i64,
    pub name: String,
    pub data: Option<Vec<u8>>,
    pub created_at: DateTime<Utc>,
}

/// Parsed Terraform/OpenTofu state.
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct TerraformState {
    pub version: u32,
    pub terraform_version: String,
    pub serial: u64,
    pub lineage: String,
    #[serde(default)]
    pub outputs: serde_json::Value,
    #[serde(default)]
    pub resources: Vec<serde_json::Value>,
}

impl StateStore {
    /// Create a new state store.
    pub fn new(pool: Arc<PgPool>) -> Self {
        Self { pool }
    }

    /// Get the current state for a template.
    pub async fn get_state(
        &self,
        schema_name: &str,
        template_name: &str,
        state_name: &str,
    ) -> Result<Option<StateEntry>> {
        let table_name = format!("{}.{}_states", schema_name, template_name);

        let query = format!(
            r#"
            SELECT id, name, data, created_at
            FROM {}
            WHERE name = $1
            ORDER BY id DESC
            LIMIT 1
            "#,
            table_name
        );

        let row: Option<(i64, String, Option<Vec<u8>>, DateTime<Utc>)> = sqlx::query_as(&query)
            .bind(state_name)
            .fetch_optional(self.pool.as_ref())
            .await
            .map_err(|e| Error::Database(e))?;

        Ok(row.map(|(id, name, data, created_at)| StateEntry {
            id,
            name,
            data,
            created_at,
        }))
    }

    /// Get and parse the Terraform state.
    pub async fn get_parsed_state(
        &self,
        schema_name: &str,
        template_name: &str,
        state_name: &str,
    ) -> Result<Option<TerraformState>> {
        let entry = self.get_state(schema_name, template_name, state_name).await?;

        match entry {
            Some(e) if e.data.is_some() => {
                let data = e.data.unwrap();
                let state: TerraformState = serde_json::from_slice(&data)
                    .map_err(|e| Error::Serialization(e))?;
                Ok(Some(state))
            }
            _ => Ok(None),
        }
    }

    /// Save state for a template.
    pub async fn save_state(
        &self,
        schema_name: &str,
        template_name: &str,
        state_name: &str,
        data: &[u8],
    ) -> Result<i64> {
        let table_name = format!("{}.{}_states", schema_name, template_name);

        debug!(
            table_name,
            state_name,
            data_size = data.len(),
            "Saving state"
        );

        // Use upsert to handle both insert and update
        let query = format!(
            r#"
            INSERT INTO {} (name, data)
            VALUES ($1, $2)
            ON CONFLICT (name) DO UPDATE SET
                data = EXCLUDED.data,
                created_at = NOW()
            RETURNING id
            "#,
            table_name
        );

        let row: (i64,) = sqlx::query_as(&query)
            .bind(state_name)
            .bind(data)
            .fetch_one(self.pool.as_ref())
            .await
            .map_err(|e| Error::Database(e))?;

        info!(
            table_name,
            state_name,
            id = row.0,
            "State saved"
        );

        Ok(row.0)
    }

    /// Delete state for a template.
    pub async fn delete_state(
        &self,
        schema_name: &str,
        template_name: &str,
        state_name: &str,
    ) -> Result<bool> {
        let table_name = format!("{}.{}_states", schema_name, template_name);

        let query = format!("DELETE FROM {} WHERE name = $1", table_name);

        let result = sqlx::query(&query)
            .bind(state_name)
            .execute(self.pool.as_ref())
            .await
            .map_err(|e| Error::Database(e))?;

        Ok(result.rows_affected() > 0)
    }

    /// List all states for a template.
    pub async fn list_states(
        &self,
        schema_name: &str,
        template_name: &str,
    ) -> Result<Vec<StateEntry>> {
        let table_name = format!("{}.{}_states", schema_name, template_name);

        let query = format!(
            r#"
            SELECT id, name, data, created_at
            FROM {}
            ORDER BY created_at DESC
            "#,
            table_name
        );

        let rows: Vec<(i64, String, Option<Vec<u8>>, DateTime<Utc>)> = sqlx::query_as(&query)
            .fetch_all(self.pool.as_ref())
            .await
            .map_err(|e| Error::Database(e))?;

        Ok(rows
            .into_iter()
            .map(|(id, name, data, created_at)| StateEntry {
                id,
                name,
                data,
                created_at,
            })
            .collect())
    }

    /// Get resource count from state.
    pub async fn get_resource_count(
        &self,
        schema_name: &str,
        template_name: &str,
        state_name: &str,
    ) -> Result<u32> {
        match self.get_parsed_state(schema_name, template_name, state_name).await? {
            Some(state) => Ok(state.resources.len() as u32),
            None => Ok(0),
        }
    }

    /// Get state outputs.
    pub async fn get_outputs(
        &self,
        schema_name: &str,
        template_name: &str,
        state_name: &str,
    ) -> Result<Option<serde_json::Value>> {
        match self.get_parsed_state(schema_name, template_name, state_name).await? {
            Some(state) => Ok(Some(state.outputs)),
            None => Ok(None),
        }
    }
}
