//! GraphQL client for communicating with the Pangea operator.

use anyhow::{Context, Result};
use cynic::{http::ReqwestExt, MutationBuilder, Operation, QueryBuilder};
use reqwest::{header::{HeaderMap, HeaderValue, AUTHORIZATION}, Client};

use crate::schema::*;

/// Pangea GraphQL client.
pub struct PangeaClient {
    client: Client,
    endpoint: String,
    token: Option<String>,
}

impl PangeaClient {
    /// Create a new client.
    pub fn new(endpoint: &str, token: Option<String>) -> Result<Self> {
        let client = Client::builder()
            .build()
            .context("Failed to create HTTP client")?;

        Ok(Self {
            client,
            endpoint: endpoint.to_string(),
            token,
        })
    }

    /// Execute a GraphQL query.
    async fn execute<T, V>(&self, operation: Operation<T, V>) -> Result<T>
    where
        T: serde::de::DeserializeOwned + 'static,
        V: serde::Serialize,
    {
        let mut request = self.client.post(&self.endpoint);

        // Add auth header if token is configured
        if let Some(ref token) = self.token {
            let mut headers = HeaderMap::new();
            let auth_value = HeaderValue::from_str(&format!("Bearer {}", token))
                .context("Invalid token format")?;
            headers.insert(AUTHORIZATION, auth_value);
            request = request.headers(headers);
        }

        let response = request
            .run_graphql(operation)
            .await
            .context("GraphQL request failed")?;

        if let Some(errors) = response.errors {
            let error_messages: Vec<_> = errors.iter().map(|e| e.message.clone()).collect();
            anyhow::bail!("GraphQL errors: {}", error_messages.join(", "));
        }

        response.data.context("No data in GraphQL response")
    }

    /// List all templates.
    pub async fn list_templates(
        &self,
        namespace: Option<&str>,
        phase: Option<Phase>,
    ) -> Result<Vec<InfrastructureTemplate>> {
        let operation = TemplatesQuery::build(TemplatesQueryVariables {
            namespace: namespace.map(|s| s.to_string()),
            phase,
        });

        let data = self.execute(operation).await?;
        Ok(data.templates)
    }

    /// Get a single template.
    pub async fn get_template(
        &self,
        namespace: &str,
        name: &str,
    ) -> Result<Option<InfrastructureTemplate>> {
        let operation = TemplateQuery::build(TemplateQueryVariables {
            namespace: namespace.to_string(),
            name: name.to_string(),
        });

        let data = self.execute(operation).await?;
        Ok(data.template)
    }

    /// List all namespaces.
    pub async fn list_namespaces(&self) -> Result<Vec<PangeaNamespace>> {
        let operation = NamespacesQuery::build(());
        let data = self.execute(operation).await?;
        Ok(data.namespaces)
    }

    /// Get a single namespace.
    pub async fn get_namespace(&self, name: &str) -> Result<Option<PangeaNamespace>> {
        let operation = NamespaceQuery::build(NamespaceQueryVariables {
            name: name.to_string(),
        });

        let data = self.execute(operation).await?;
        Ok(data.namespace)
    }

    /// Get plan for a template.
    pub async fn get_plan(&self, namespace: &str, name: &str) -> Result<Option<PlanResult>> {
        let operation = PlanQuery::build(PlanQueryVariables {
            namespace: namespace.to_string(),
            name: name.to_string(),
        });

        let data = self.execute(operation).await?;
        Ok(data.plan)
    }

    /// Trigger apply for a template.
    pub async fn apply(&self, namespace: &str, name: &str) -> Result<InfrastructureTemplate> {
        let operation = ApplyMutation::build(ApplyMutationVariables {
            input: TemplateInput {
                namespace: namespace.to_string(),
                name: name.to_string(),
            },
        });

        let data = self.execute(operation).await?;
        Ok(data.apply)
    }

    /// Approve pending changes for a template.
    pub async fn approve(&self, namespace: &str, name: &str) -> Result<InfrastructureTemplate> {
        let operation = ApproveMutation::build(ApproveMutationVariables {
            input: TemplateInput {
                namespace: namespace.to_string(),
                name: name.to_string(),
            },
        });

        let data = self.execute(operation).await?;
        Ok(data.approve)
    }

    /// Trigger destroy for a template.
    pub async fn destroy(&self, namespace: &str, name: &str) -> Result<InfrastructureTemplate> {
        let operation = DestroyMutation::build(DestroyMutationVariables {
            input: TemplateInput {
                namespace: namespace.to_string(),
                name: name.to_string(),
            },
        });

        let data = self.execute(operation).await?;
        Ok(data.destroy)
    }

    /// Force unlock state for a template.
    pub async fn unlock(&self, namespace: &str, name: &str) -> Result<bool> {
        let operation = UnlockMutation::build(UnlockMutationVariables {
            input: TemplateInput {
                namespace: namespace.to_string(),
                name: name.to_string(),
            },
        });

        let data = self.execute(operation).await?;
        Ok(data.unlock)
    }

    /// Suspend/resume reconciliation for a template.
    pub async fn suspend(
        &self,
        namespace: &str,
        name: &str,
        suspended: bool,
    ) -> Result<InfrastructureTemplate> {
        let operation = SuspendMutation::build(SuspendMutationVariables {
            input: TemplateInput {
                namespace: namespace.to_string(),
                name: name.to_string(),
            },
            suspended,
        });

        let data = self.execute(operation).await?;
        Ok(data.suspend)
    }

    /// Get the endpoint URL.
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }
}

/// Parse a template reference (namespace/name or just name).
pub fn parse_template_ref(reference: &str, default_namespace: Option<&str>) -> Result<(String, String)> {
    if let Some((ns, name)) = reference.split_once('/') {
        Ok((ns.to_string(), name.to_string()))
    } else if let Some(ns) = default_namespace {
        Ok((ns.to_string(), reference.to_string()))
    } else {
        anyhow::bail!(
            "Template reference must be in format 'namespace/name' or provide --namespace flag"
        )
    }
}
