//! GraphQL API implementation.
//!
//! Provides queries, mutations, and subscriptions for managing infrastructure templates.

mod schema;
mod types;
mod resolvers;

pub use schema::{build_schema, graphql_router, run_graphql_server, PangeaSchema};
pub use types::*;
