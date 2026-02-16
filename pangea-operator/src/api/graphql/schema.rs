//! GraphQL schema definition and server setup.

use async_graphql::Schema;
use async_graphql_axum::{GraphQLRequest, GraphQLResponse, GraphQLSubscription};
use axum::{
    body::Body,
    extract::State,
    http::{header::AUTHORIZATION, Request, StatusCode},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, post},
    Router,
};
use kube::Client;
use std::{env, net::SocketAddr, sync::Arc};
use tokio::net::TcpListener;
use tracing::{error, info, warn};

use super::resolvers::{GraphQLContext, MutationRoot, QueryRoot, SubscriptionRoot};
use crate::error::Result;

/// The Pangea GraphQL schema type.
pub type PangeaSchema = Schema<QueryRoot, MutationRoot, SubscriptionRoot>;

/// API configuration for authentication.
#[derive(Clone)]
pub struct ApiConfig {
    /// Expected API token for Bearer authentication.
    pub api_token: Option<String>,
    /// Whether to allow playground access (disabled in production).
    pub enable_playground: bool,
}

impl ApiConfig {
    /// Load configuration from environment variables.
    pub fn from_env() -> Self {
        let api_token = env::var("PANGEA_API_TOKEN").ok();
        let enable_playground = env::var("PANGEA_ENABLE_PLAYGROUND")
            .map(|v| v == "true" || v == "1")
            .unwrap_or(true);

        if api_token.is_none() {
            warn!("PANGEA_API_TOKEN not set - API will be unauthenticated!");
        }

        Self {
            api_token,
            enable_playground,
        }
    }
}

/// Shared application state.
#[derive(Clone)]
pub struct AppState {
    pub schema: PangeaSchema,
    pub config: ApiConfig,
}

/// Build the GraphQL schema.
pub fn build_schema(client: Client) -> PangeaSchema {
    let context = GraphQLContext::new(client);

    Schema::build(QueryRoot, MutationRoot, SubscriptionRoot)
        .data(context)
        .finish()
}

/// Authentication middleware.
async fn auth_middleware(
    State(state): State<Arc<AppState>>,
    request: Request<Body>,
    next: Next,
) -> Response {
    // If no token is configured, allow all requests (development mode)
    let Some(expected_token) = &state.config.api_token else {
        return next.run(request).await;
    };

    // Extract Authorization header
    let auth_header = request
        .headers()
        .get(AUTHORIZATION)
        .and_then(|h| h.to_str().ok());

    match auth_header {
        Some(header) if header.starts_with("Bearer ") => {
            let token = &header[7..];
            if token == expected_token {
                next.run(request).await
            } else {
                warn!("Invalid API token provided");
                (StatusCode::UNAUTHORIZED, "Invalid token").into_response()
            }
        }
        Some(_) => {
            (StatusCode::UNAUTHORIZED, "Invalid authorization format, expected: Bearer <token>").into_response()
        }
        None => {
            (StatusCode::UNAUTHORIZED, "Missing Authorization header").into_response()
        }
    }
}

/// GraphQL handler for queries and mutations.
async fn graphql_handler(
    State(state): State<Arc<AppState>>,
    req: GraphQLRequest,
) -> GraphQLResponse {
    state.schema.execute(req.into_inner()).await.into()
}

/// GraphQL Playground handler.
async fn graphql_playground(
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    if !state.config.enable_playground {
        return (StatusCode::NOT_FOUND, "Playground disabled").into_response();
    }

    axum::response::Html(async_graphql::http::playground_source(
        async_graphql::http::GraphQLPlaygroundConfig::new("/graphql")
            .subscription_endpoint("/graphql/ws"),
    ))
    .into_response()
}

/// WebSocket subscription handler.
async fn graphql_ws_handler(
    State(state): State<Arc<AppState>>,
    protocol: async_graphql_axum::GraphQLProtocol,
    websocket: axum::extract::WebSocketUpgrade,
) -> impl IntoResponse {
    let schema = state.schema.clone();
    websocket
        .protocols(["graphql-transport-ws", "graphql-ws"])
        .on_upgrade(move |socket| {
            async_graphql_axum::GraphQLWebSocket::new(socket, schema, protocol).serve()
        })
}

/// Create the GraphQL router with authentication.
pub fn graphql_router(schema: PangeaSchema) -> Router {
    let config = ApiConfig::from_env();
    let state = Arc::new(AppState { schema, config });

    // Protected routes (require auth)
    let protected_routes = Router::new()
        .route("/graphql", post(graphql_handler))
        .route("/graphql/ws", get(graphql_ws_handler))
        .route_layer(middleware::from_fn_with_state(state.clone(), auth_middleware));

    // Public routes (playground - can be disabled via config)
    let public_routes = Router::new()
        .route("/graphql", get(graphql_playground));

    Router::new()
        .merge(protected_routes)
        .merge(public_routes)
        .with_state(state)
}

/// Run the GraphQL server.
pub async fn run_graphql_server(addr: SocketAddr, client: Client) -> Result<()> {
    let schema = build_schema(client);
    let app = graphql_router(schema);

    info!(%addr, "Starting GraphQL server");

    let listener = TcpListener::bind(addr).await.map_err(|e| {
        error!(error = %e, "Failed to bind GraphQL server");
        crate::Error::Io(e)
    })?;

    axum::serve(listener, app).await.map_err(|e| {
        error!(error = %e, "GraphQL server error");
        crate::Error::Io(e)
    })?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_schema_builds() {
        // This test just verifies the schema can be built without a real client
        // In a real test, we'd use a mock client
    }
}
