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

mod phase;
mod template;
mod namespace;
mod plan;

pub use phase::Phase;
pub use template::{InfrastructureTemplate, TemplateSource, ResourceCounts};
pub use namespace::PangeaNamespace;
pub use plan::PlanResult;

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
