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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pangea_namespace_default() {
        let ns = PangeaNamespace::default();
        assert!(ns.name.is_none());
        assert_eq!(ns.backend_type, "pg");
        assert!(ns.database_host.is_none());
        assert!(ns.database_name.is_none());
        assert!(!ns.is_ready);
        assert!(ns.schema_name.is_none());
        assert_eq!(ns.template_count, 0);
    }

    #[test]
    fn test_display_name_with_name() {
        let ns = PangeaNamespace {
            name: Some("production".to_string()),
            ..Default::default()
        };
        assert_eq!(ns.display_name(), "production");
    }

    #[test]
    fn test_display_name_without_name() {
        let ns = PangeaNamespace::default();
        assert_eq!(ns.display_name(), "unnamed");
    }

    #[test]
    fn test_is_pg_backend_true() {
        let ns = PangeaNamespace::default();
        assert!(ns.is_pg_backend());
    }

    #[test]
    fn test_is_pg_backend_false() {
        let ns = PangeaNamespace {
            backend_type: "s3".to_string(),
            ..Default::default()
        };
        assert!(!ns.is_pg_backend());
    }

    #[test]
    fn test_pangea_namespace_serde_roundtrip() {
        let ns = PangeaNamespace {
            name: Some("staging".to_string()),
            backend_type: "pg".to_string(),
            database_host: Some("db.example.com".to_string()),
            database_name: Some("pangea".to_string()),
            is_ready: true,
            schema_name: Some("pangea_staging".to_string()),
            template_count: 5,
        };
        let json = serde_json::to_string(&ns).unwrap();
        let back: PangeaNamespace = serde_json::from_str(&json).unwrap();
        assert_eq!(ns, back);
    }
}
