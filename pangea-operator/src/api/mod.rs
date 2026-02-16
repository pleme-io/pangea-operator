//! API layer for the Pangea Operator.
//!
//! Provides GraphQL API for querying and mutating infrastructure templates.

#[cfg(feature = "graphql")]
pub mod graphql;

#[cfg(feature = "graphql")]
pub use graphql::{build_schema, graphql_router, run_graphql_server, PangeaSchema};
