//! Infrastructure template types.

use serde::{Deserialize, Serialize};
use crate::{DateTime, Phase};

/// Resource counts for a template.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[cfg_attr(feature = "server", derive(async_graphql::SimpleObject))]
pub struct ResourceCounts {
    /// Total number of managed resources.
    pub total: i32,
    /// Resources to be added.
    pub added: i32,
    /// Resources to be changed.
    pub changed: i32,
    /// Resources to be destroyed.
    pub destroyed: i32,
}

/// Template source information.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "server", derive(async_graphql::SimpleObject))]
pub struct TemplateSource {
    /// Source type: inline, configMapRef, or gitRepository.
    pub source_type: String,
    /// Reference to the source (content hash, configMap name, or git URL).
    pub reference: String,
}

impl Default for TemplateSource {
    fn default() -> Self {
        Self {
            source_type: "inline".to_string(),
            reference: String::new(),
        }
    }
}

/// Infrastructure template representing a Pangea-managed resource set.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "server", derive(async_graphql::SimpleObject))]
pub struct InfrastructureTemplate {
    /// Kubernetes namespace.
    pub namespace: Option<String>,
    /// Template name.
    pub name: Option<String>,
    /// Current lifecycle phase.
    pub phase: Phase,
    /// Pangea namespace for state isolation.
    pub pangea_namespace: String,
    /// Source configuration.
    pub source: TemplateSource,
    /// Whether to auto-approve changes.
    pub auto_approve: bool,
    /// Whether reconciliation is suspended.
    pub suspended: bool,
    /// Last successful apply timestamp.
    pub last_applied_at: Option<DateTime>,
    /// Current resource counts.
    pub resource_counts: ResourceCounts,
    /// Summary of pending plan.
    pub plan_summary: Option<String>,
    /// Last error message.
    pub last_error: Option<String>,
    /// Number of consecutive failures.
    pub failure_count: i32,
}

impl Default for InfrastructureTemplate {
    fn default() -> Self {
        Self {
            namespace: None,
            name: None,
            phase: Phase::Pending,
            pangea_namespace: "default".to_string(),
            source: TemplateSource::default(),
            auto_approve: false,
            suspended: false,
            last_applied_at: None,
            resource_counts: ResourceCounts::default(),
            plan_summary: None,
            last_error: None,
            failure_count: 0,
        }
    }
}

impl InfrastructureTemplate {
    /// Returns the full name (namespace/name) of the template.
    pub fn full_name(&self) -> String {
        format!(
            "{}/{}",
            self.namespace.as_deref().unwrap_or("default"),
            self.name.as_deref().unwrap_or("unnamed")
        )
    }

    /// Returns true if the template has pending changes.
    pub fn has_pending_changes(&self) -> bool {
        let rc = &self.resource_counts;
        rc.added > 0 || rc.changed > 0 || rc.destroyed > 0
    }
}
