//! GraphQL API implementation.
//!
//! Provides queries, mutations, and subscriptions for managing infrastructure templates.

mod resolvers;
mod schema;
mod types;

pub use schema::{build_schema, graphql_router, run_graphql_server, PangeaSchema};
pub use types::*;
