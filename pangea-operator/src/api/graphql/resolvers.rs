//! GraphQL resolvers for queries, mutations, and subscriptions.

use async_graphql::{Context, Object, Result, Subscription};
use futures::{Stream, StreamExt};
use kube::{Api, Client};
use tokio::sync::broadcast;
use tokio_stream::wrappers::BroadcastStream;
use tracing::{error, info};

use crate::crd::{
    InfrastructureTemplate as CrdTemplate, PangeaNamespace as CrdNamespace,
};

use super::types::{InfrastructureTemplate, LogEntry, PangeaNamespace, Phase, PlanResult, TemplateInput};

/// Shared context for GraphQL operations.
#[derive(Clone)]
pub struct GraphQLContext {
    pub client: Client,
    pub log_sender: broadcast::Sender<LogEntry>,
}

impl GraphQLContext {
    pub fn new(client: Client) -> Self {
        let (log_sender, _) = broadcast::channel(1024);
        Self { client, log_sender }
    }
}

/// GraphQL queries.
pub struct QueryRoot;

#[Object]
impl QueryRoot {
    /// Get a single infrastructure template.
    async fn template(
        &self,
        ctx: &Context<'_>,
        namespace: String,
        name: String,
    ) -> Result<Option<InfrastructureTemplate>> {
        let context = ctx.data::<GraphQLContext>()?;
        let api: Api<CrdTemplate> = Api::namespaced(context.client.clone(), &namespace);

        match api.get_opt(&name).await {
            Ok(Some(template)) => Ok(Some(InfrastructureTemplate::from(template))),
            Ok(None) => Ok(None),
            Err(e) => {
                error!(error = %e, "Failed to get template");
                Err(async_graphql::Error::new(format!("Failed to get template: {}", e)))
            }
        }
    }

    /// List infrastructure templates.
    async fn templates(
        &self,
        ctx: &Context<'_>,
        namespace: Option<String>,
        phase: Option<Phase>,
    ) -> Result<Vec<InfrastructureTemplate>> {
        let context = ctx.data::<GraphQLContext>()?;

        let templates: Vec<CrdTemplate> = if let Some(ns) = namespace {
            let api: Api<CrdTemplate> = Api::namespaced(context.client.clone(), &ns);
            api.list(&Default::default()).await?.items
        } else {
            let api: Api<CrdTemplate> = Api::all(context.client.clone());
            api.list(&Default::default()).await?.items
        };

        let result: Vec<InfrastructureTemplate> = templates
            .into_iter()
            .filter(|t| {
                if let Some(ref filter_phase) = phase {
                    let current_phase = t
                        .status
                        .as_ref()
                        .and_then(|s| s.phase)
                        .map(Phase::from)
                        .unwrap_or(Phase::Pending);
                    &current_phase == filter_phase
                } else {
                    true
                }
            })
            .map(InfrastructureTemplate::from)
            .collect();

        Ok(result)
    }

    /// List Pangea namespaces.
    async fn namespaces(&self, ctx: &Context<'_>) -> Result<Vec<PangeaNamespace>> {
        let context = ctx.data::<GraphQLContext>()?;
        let api: Api<CrdNamespace> = Api::all(context.client.clone());

        let namespaces = api.list(&Default::default()).await?;

        Ok(namespaces
            .items
            .into_iter()
            .map(PangeaNamespace::from)
            .collect())
    }

    /// Get a Pangea namespace.
    async fn namespace(&self, ctx: &Context<'_>, name: String) -> Result<Option<PangeaNamespace>> {
        let context = ctx.data::<GraphQLContext>()?;
        let api: Api<CrdNamespace> = Api::all(context.client.clone());

        match api.get_opt(&name).await {
            Ok(Some(ns)) => Ok(Some(PangeaNamespace::from(ns))),
            Ok(None) => Ok(None),
            Err(e) => {
                error!(error = %e, "Failed to get namespace");
                Err(async_graphql::Error::new(format!(
                    "Failed to get namespace: {}",
                    e
                )))
            }
        }
    }

    /// Get the plan for a template (preview of pending changes).
    async fn plan(
        &self,
        ctx: &Context<'_>,
        namespace: String,
        name: String,
    ) -> Result<Option<PlanResult>> {
        let context = ctx.data::<GraphQLContext>()?;
        let api: Api<CrdTemplate> = Api::namespaced(context.client.clone(), &namespace);

        match api.get_opt(&name).await {
            Ok(Some(template)) => {
                let status = template.status.as_ref();
                let resources = status.and_then(|s| s.resources.as_ref());

                if let Some(res) = resources {
                    let has_changes = res.added > 0 || res.changed > 0 || res.destroyed > 0;
                    let summary = status
                        .and_then(|s| s.plan_summary.clone())
                        .unwrap_or_else(|| {
                            if has_changes {
                                format!(
                                    "+{} ~{} -{}",
                                    res.added, res.changed, res.destroyed
                                )
                            } else {
                                "No changes".to_string()
                            }
                        });

                    Ok(Some(PlanResult {
                        has_changes,
                        summary,
                        added: res.added as i32,
                        changed: res.changed as i32,
                        destroyed: res.destroyed as i32,
                    }))
                } else {
                    Ok(None)
                }
            }
            Ok(None) => Ok(None),
            Err(e) => {
                error!(error = %e, "Failed to get template plan");
                Err(async_graphql::Error::new(format!(
                    "Failed to get template plan: {}",
                    e
                )))
            }
        }
    }
}

/// GraphQL mutations.
pub struct MutationRoot;

#[Object]
impl MutationRoot {
    /// Trigger reconciliation for a template.
    async fn apply(&self, ctx: &Context<'_>, input: TemplateInput) -> Result<InfrastructureTemplate> {
        let context = ctx.data::<GraphQLContext>()?;
        let api: Api<CrdTemplate> = Api::namespaced(context.client.clone(), &input.namespace);

        // Existence check — fail-fast if the template doesn't exist
        // before issuing the annotation patch. The fetched object isn't
        // used; the patch is unconditional.
        let _ = api.get(&input.name).await?;

        let patch = serde_json::json!({
            "metadata": {
                "annotations": {
                    "pangea.pleme.io/reconcile-requested": chrono::Utc::now().to_rfc3339()
                }
            }
        });

        let patched = api
            .patch(
                &input.name,
                &kube::api::PatchParams::apply("pangea-graphql"),
                &kube::api::Patch::Merge(&patch),
            )
            .await?;

        info!(
            namespace = %input.namespace,
            name = %input.name,
            "Triggered reconciliation via GraphQL"
        );

        Ok(InfrastructureTemplate::from(patched))
    }

    /// Approve pending changes for a template.
    async fn approve(
        &self,
        ctx: &Context<'_>,
        input: TemplateInput,
    ) -> Result<InfrastructureTemplate> {
        let context = ctx.data::<GraphQLContext>()?;
        let api: Api<CrdTemplate> = Api::namespaced(context.client.clone(), &input.namespace);

        let patch = serde_json::json!({
            "metadata": {
                "annotations": {
                    "pangea.pleme.io/approved": chrono::Utc::now().to_rfc3339()
                }
            }
        });

        let patched = api
            .patch(
                &input.name,
                &kube::api::PatchParams::apply("pangea-graphql"),
                &kube::api::Patch::Merge(&patch),
            )
            .await?;

        info!(
            namespace = %input.namespace,
            name = %input.name,
            "Approved changes via GraphQL"
        );

        Ok(InfrastructureTemplate::from(patched))
    }

    /// Trigger destroy for a template.
    async fn destroy(
        &self,
        ctx: &Context<'_>,
        input: TemplateInput,
    ) -> Result<InfrastructureTemplate> {
        let context = ctx.data::<GraphQLContext>()?;
        let api: Api<CrdTemplate> = Api::namespaced(context.client.clone(), &input.namespace);

        let patch = serde_json::json!({
            "metadata": {
                "annotations": {
                    "pangea.pleme.io/destroy-requested": chrono::Utc::now().to_rfc3339()
                }
            }
        });

        let patched = api
            .patch(
                &input.name,
                &kube::api::PatchParams::apply("pangea-graphql"),
                &kube::api::Patch::Merge(&patch),
            )
            .await?;

        info!(
            namespace = %input.namespace,
            name = %input.name,
            "Triggered destroy via GraphQL"
        );

        Ok(InfrastructureTemplate::from(patched))
    }

    /// Force unlock state for a template.
    async fn unlock(&self, ctx: &Context<'_>, input: TemplateInput) -> Result<bool> {
        let context = ctx.data::<GraphQLContext>()?;
        let api: Api<CrdTemplate> = Api::namespaced(context.client.clone(), &input.namespace);

        let patch = serde_json::json!({
            "metadata": {
                "annotations": {
                    "pangea.pleme.io/force-unlock": chrono::Utc::now().to_rfc3339()
                }
            }
        });

        api.patch(
            &input.name,
            &kube::api::PatchParams::apply("pangea-graphql"),
            &kube::api::Patch::Merge(&patch),
        )
        .await?;

        info!(
            namespace = %input.namespace,
            name = %input.name,
            "Force unlocked state via GraphQL"
        );

        Ok(true)
    }

    /// Suspend reconciliation for a template.
    async fn suspend(
        &self,
        ctx: &Context<'_>,
        input: TemplateInput,
        suspended: bool,
    ) -> Result<InfrastructureTemplate> {
        let context = ctx.data::<GraphQLContext>()?;
        let api: Api<CrdTemplate> = Api::namespaced(context.client.clone(), &input.namespace);

        let patch = serde_json::json!({
            "spec": {
                "suspend": suspended
            }
        });

        let patched = api
            .patch(
                &input.name,
                &kube::api::PatchParams::apply("pangea-graphql"),
                &kube::api::Patch::Merge(&patch),
            )
            .await?;

        info!(
            namespace = %input.namespace,
            name = %input.name,
            suspended = suspended,
            "Updated suspend status via GraphQL"
        );

        Ok(InfrastructureTemplate::from(patched))
    }
}

/// GraphQL subscriptions.
pub struct SubscriptionRoot;

#[Subscription]
impl SubscriptionRoot {
    /// Subscribe to log entries for a template.
    async fn logs(
        &self,
        ctx: &Context<'_>,
        namespace: String,
        name: String,
    ) -> Result<impl Stream<Item = LogEntry>> {
        let context = ctx.data::<GraphQLContext>()?;
        let receiver = context.log_sender.subscribe();

        let stream = BroadcastStream::new(receiver).filter_map(move |result| {
            let ns = namespace.clone();
            let n = name.clone();
            async move {
                match result {
                    Ok(entry) if entry.namespace == ns && entry.name == n => Some(entry),
                    _ => None,
                }
            }
        });

        Ok(stream)
    }

    /// Subscribe to all template status changes in a namespace.
    async fn template_status(
        &self,
        _ctx: &Context<'_>,
        _namespace: String,
    ) -> Result<impl Stream<Item = InfrastructureTemplate>> {
        // TODO: Implement using kube watcher
        // For now, return an empty stream
        Ok(futures::stream::empty())
    }
}
