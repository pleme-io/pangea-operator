//! Shared types for Pangea infrastructure management platform.
//!
//! This crate provides common types used across:
//! - pangea-operator (Kubernetes operator with GraphQL API)
//! - pangea-cli (command-line client)
//! - pangea-web (Yew/WASM frontend)
//!
//! # Features
//!
//! - `server`: Enables async-graphql derives for server-side use
//! - `client`: Enables cynic derives for client-side use

mod namespace;
mod phase;
mod plan;
mod template;

pub use namespace::PangeaNamespace;
pub use phase::Phase;
pub use plan::PlanResult;
pub use template::{InfrastructureTemplate, ResourceCounts, TemplateSource};

/// DateTime scalar type for GraphQL.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct DateTime(pub String);

impl From<chrono::DateTime<chrono::Utc>> for DateTime {
    fn from(dt: chrono::DateTime<chrono::Utc>) -> Self {
        DateTime(dt.to_rfc3339())
    }
}

impl std::fmt::Display for DateTime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[cfg(feature = "server")]
mod server_impls {
    use super::DateTime;
    use async_graphql::{InputValueError, InputValueResult, Scalar, ScalarType, Value};

    #[Scalar]
    impl ScalarType for DateTime {
        fn parse(value: Value) -> InputValueResult<Self> {
            match value {
                Value::String(s) => Ok(DateTime(s)),
                _ => Err(InputValueError::expected_type(value)),
            }
        }

        fn to_value(&self) -> Value {
            Value::String(self.0.clone())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_datetime_from_chrono() {
        let dt = chrono::Utc::now();
        let pangea_dt = DateTime::from(dt.clone());
        assert_eq!(pangea_dt.0, dt.to_rfc3339());
    }

    #[test]
    fn test_datetime_display() {
        let dt = DateTime("2024-01-15T10:30:00Z".to_string());
        assert_eq!(format!("{}", dt), "2024-01-15T10:30:00Z");
    }

    #[test]
    fn test_datetime_roundtrip_serde() {
        let dt = DateTime("2024-06-01T00:00:00+00:00".to_string());
        let json = serde_json::to_string(&dt).unwrap();
        let back: DateTime = serde_json::from_str(&json).unwrap();
        assert_eq!(dt, back);
    }

    #[test]
    fn test_datetime_empty_string() {
        let dt = DateTime(String::new());
        assert_eq!(format!("{}", dt), "");
    }
}
