//! GraphQL schema types for cynic.

use serde::{Deserialize, Serialize};

/// Cynic schema module.
#[cynic::schema("pangea")]
mod schema {}

/// DateTime scalar type.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DateTime(pub String);

cynic::impl_scalar!(DateTime, schema::DateTime);

/// Phase enum for template lifecycle.
#[derive(cynic::Enum, Clone, Copy, Debug, PartialEq, Eq)]
#[cynic(graphql_type = "Phase", rename_all = "SCREAMING_SNAKE_CASE")]
pub enum Phase {
    Pending,
    Compiling,
    Initializing,
    Planning,
    Applying,
    Ready,
    Drifted,
    Failed,
    Destroying,
}

impl std::fmt::Display for Phase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Phase::Pending => write!(f, "Pending"),
            Phase::Compiling => write!(f, "Compiling"),
            Phase::Initializing => write!(f, "Initializing"),
            Phase::Planning => write!(f, "Planning"),
            Phase::Applying => write!(f, "Applying"),
            Phase::Ready => write!(f, "Ready"),
            Phase::Drifted => write!(f, "Drifted"),
            Phase::Failed => write!(f, "Failed"),
            Phase::Destroying => write!(f, "Destroying"),
        }
    }
}

impl Phase {
    /// Get CSS class for this phase.
    pub fn css_class(&self) -> &'static str {
        match self {
            Phase::Ready => "phase-ready",
            Phase::Failed => "phase-failed",
            Phase::Drifted => "phase-drifted",
            Phase::Pending | Phase::Compiling | Phase::Initializing => "phase-pending",
            Phase::Planning | Phase::Applying | Phase::Destroying => "phase-in-progress",
        }
    }
}

/// Resource counts for a template.
#[derive(cynic::QueryFragment, Debug, Clone, Serialize, PartialEq)]
#[cynic(graphql_type = "ResourceCounts")]
pub struct ResourceCounts {
    pub total: i32,
    pub added: i32,
    pub changed: i32,
    pub destroyed: i32,
}

/// Template source information.
#[derive(cynic::QueryFragment, Debug, Clone, Serialize, PartialEq)]
#[cynic(graphql_type = "TemplateSource")]
pub struct TemplateSource {
    pub source_type: String,
    pub reference: String,
}

/// Infrastructure template.
#[derive(cynic::QueryFragment, Debug, Clone, Serialize, PartialEq)]
#[cynic(graphql_type = "InfrastructureTemplate")]
pub struct InfrastructureTemplate {
    pub namespace: Option<String>,
    pub name: Option<String>,
    pub phase: Phase,
    pub pangea_namespace: String,
    pub source: TemplateSource,
    pub auto_approve: bool,
    pub suspended: bool,
    pub last_applied_at: Option<DateTime>,
    pub resource_counts: ResourceCounts,
    pub plan_summary: Option<String>,
    pub last_error: Option<String>,
    pub failure_count: i32,
}

/// Pangea namespace.
#[derive(cynic::QueryFragment, Debug, Clone, Serialize, PartialEq)]
#[cynic(graphql_type = "PangeaNamespace")]
pub struct PangeaNamespace {
    pub name: Option<String>,
    pub backend_type: String,
    pub database_host: Option<String>,
    pub database_name: Option<String>,
    pub is_ready: bool,
    pub schema_name: Option<String>,
    pub template_count: i32,
}

/// Plan result.
#[derive(cynic::QueryFragment, Debug, Clone, Serialize, PartialEq)]
#[cynic(graphql_type = "PlanResult")]
pub struct PlanResult {
    pub has_changes: bool,
    pub summary: String,
    pub added: i32,
    pub changed: i32,
    pub destroyed: i32,
}

// --- Query Fragments ---

#[derive(cynic::QueryVariables, Debug)]
pub struct TemplatesQueryVariables {
    pub namespace: Option<String>,
    pub phase: Option<Phase>,
}

#[derive(cynic::QueryFragment, Debug)]
#[cynic(graphql_type = "Query", variables = "TemplatesQueryVariables")]
pub struct TemplatesQuery {
    #[arguments(namespace: $namespace, phase: $phase)]
    pub templates: Vec<InfrastructureTemplate>,
}

#[derive(cynic::QueryVariables, Debug)]
pub struct TemplateQueryVariables {
    pub namespace: String,
    pub name: String,
}

#[derive(cynic::QueryFragment, Debug)]
#[cynic(graphql_type = "Query", variables = "TemplateQueryVariables")]
pub struct TemplateQuery {
    #[arguments(namespace: $namespace, name: $name)]
    pub template: Option<InfrastructureTemplate>,
}

#[derive(cynic::QueryFragment, Debug)]
#[cynic(graphql_type = "Query")]
pub struct NamespacesQuery {
    pub namespaces: Vec<PangeaNamespace>,
}

#[derive(cynic::QueryVariables, Debug)]
pub struct PlanQueryVariables {
    pub namespace: String,
    pub name: String,
}

#[derive(cynic::QueryFragment, Debug)]
#[cynic(graphql_type = "Query", variables = "PlanQueryVariables")]
pub struct PlanQuery {
    #[arguments(namespace: $namespace, name: $name)]
    pub plan: Option<PlanResult>,
}

// --- Mutation Fragments ---

#[derive(cynic::InputObject, Debug)]
#[cynic(graphql_type = "TemplateInput")]
pub struct TemplateInput {
    pub namespace: String,
    pub name: String,
}

#[derive(cynic::QueryVariables, Debug)]
pub struct ApplyMutationVariables {
    pub input: TemplateInput,
}

#[derive(cynic::QueryFragment, Debug)]
#[cynic(graphql_type = "Mutation", variables = "ApplyMutationVariables")]
pub struct ApplyMutation {
    #[arguments(input: $input)]
    pub apply: InfrastructureTemplate,
}

#[derive(cynic::QueryVariables, Debug)]
pub struct ApproveMutationVariables {
    pub input: TemplateInput,
}

#[derive(cynic::QueryFragment, Debug)]
#[cynic(graphql_type = "Mutation", variables = "ApproveMutationVariables")]
pub struct ApproveMutation {
    #[arguments(input: $input)]
    pub approve: InfrastructureTemplate,
}
