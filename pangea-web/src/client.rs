//! GraphQL client for WASM.

use cynic::{MutationBuilder, Operation, QueryBuilder};
use gloo_net::http::Request;
use serde::{de::DeserializeOwned, Serialize};

use crate::schema::*;

/// GraphQL response structure.
#[derive(serde::Deserialize)]
struct GraphQLResponse<T> {
    data: Option<T>,
    errors: Option<Vec<GraphQLError>>,
}

#[derive(serde::Deserialize)]
struct GraphQLError {
    message: String,
}

/// Pangea GraphQL client for WASM.
#[derive(Clone)]
pub struct PangeaClient {
    endpoint: String,
    token: Option<String>,
}

impl PangeaClient {
    /// Create a new client.
    pub fn new(endpoint: &str, token: Option<String>) -> Self {
        Self {
            endpoint: endpoint.to_string(),
            token,
        }
    }

    /// Execute a GraphQL operation.
    async fn execute<T, V>(&self, operation: &Operation<T, V>) -> Result<T, String>
    where
        T: DeserializeOwned + 'static,
        V: Serialize,
    {
        let body = serde_json::to_string(operation).map_err(|e| e.to_string())?;

        let mut request = Request::post(&self.endpoint)
            .header("Content-Type", "application/json");

        if let Some(ref token) = self.token {
            request = request.header("Authorization", &format!("Bearer {}", token));
        }

        let response = request
            .body(body)
            .map_err(|e| e.to_string())?
            .send()
            .await
            .map_err(|e| e.to_string())?;

        if !response.ok() {
            return Err(format!("HTTP error: {}", response.status()));
        }

        let result: GraphQLResponse<T> = response.json().await.map_err(|e| e.to_string())?;

        if let Some(errors) = result.errors {
            let messages: Vec<_> = errors.iter().map(|e| e.message.clone()).collect();
            return Err(format!("GraphQL errors: {}", messages.join(", ")));
        }

        result.data.ok_or_else(|| "No data in response".to_string())
    }

    /// List all templates.
    pub async fn list_templates(
        &self,
        namespace: Option<String>,
        phase: Option<Phase>,
    ) -> Result<Vec<InfrastructureTemplate>, String> {
        let operation = TemplatesQuery::build(TemplatesQueryVariables { namespace, phase });
        let data = self.execute(&operation).await?;
        Ok(data.templates)
    }

    /// Get a single template.
    pub async fn get_template(
        &self,
        namespace: &str,
        name: &str,
    ) -> Result<Option<InfrastructureTemplate>, String> {
        let operation = TemplateQuery::build(TemplateQueryVariables {
            namespace: namespace.to_string(),
            name: name.to_string(),
        });
        let data = self.execute(&operation).await?;
        Ok(data.template)
    }

    /// List all namespaces.
    pub async fn list_namespaces(&self) -> Result<Vec<PangeaNamespace>, String> {
        let operation = NamespacesQuery::build(());
        let data = self.execute(&operation).await?;
        Ok(data.namespaces)
    }

    /// Get plan for a template.
    pub async fn get_plan(
        &self,
        namespace: &str,
        name: &str,
    ) -> Result<Option<PlanResult>, String> {
        let operation = PlanQuery::build(PlanQueryVariables {
            namespace: namespace.to_string(),
            name: name.to_string(),
        });
        let data = self.execute(&operation).await?;
        Ok(data.plan)
    }

    /// Apply changes to a template.
    pub async fn apply(
        &self,
        namespace: &str,
        name: &str,
    ) -> Result<InfrastructureTemplate, String> {
        let operation = ApplyMutation::build(ApplyMutationVariables {
            input: TemplateInput {
                namespace: namespace.to_string(),
                name: name.to_string(),
            },
        });
        let data = self.execute(&operation).await?;
        Ok(data.apply)
    }

    /// Approve pending changes.
    pub async fn approve(
        &self,
        namespace: &str,
        name: &str,
    ) -> Result<InfrastructureTemplate, String> {
        let operation = ApproveMutation::build(ApproveMutationVariables {
            input: TemplateInput {
                namespace: namespace.to_string(),
                name: name.to_string(),
            },
        });
        let data = self.execute(&operation).await?;
        Ok(data.approve)
    }
}
