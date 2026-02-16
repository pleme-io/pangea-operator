//! Pangea namespace types.

use serde::{Deserialize, Serialize};

/// Pangea namespace for state isolation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "server", derive(async_graphql::SimpleObject))]
pub struct PangeaNamespace {
    /// Namespace name.
    pub name: Option<String>,
    /// Backend type (pg, s3, etc.).
    pub backend_type: String,
    /// Database host for pg backend.
    pub database_host: Option<String>,
    /// Database name for pg backend.
    pub database_name: Option<String>,
    /// Whether the namespace is ready for use.
    pub is_ready: bool,
    /// Schema name for pg backend.
    pub schema_name: Option<String>,
    /// Number of templates in this namespace.
    pub template_count: i32,
}

impl Default for PangeaNamespace {
    fn default() -> Self {
        Self {
            name: None,
            backend_type: "pg".to_string(),
            database_host: None,
            database_name: None,
            is_ready: false,
            schema_name: None,
            template_count: 0,
        }
    }
}

impl PangeaNamespace {
    /// Returns the display name of the namespace.
    pub fn display_name(&self) -> &str {
        self.name.as_deref().unwrap_or("unnamed")
    }

    /// Returns true if the namespace uses PostgreSQL backend.
    pub fn is_pg_backend(&self) -> bool {
        self.backend_type == "pg"
    }
}
