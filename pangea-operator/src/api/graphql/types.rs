//! GraphQL types for the Pangea API.
//!
//! These types mirror the CRD types but with async-graphql derives for GraphQL API.

use async_graphql::{Enum, InputObject, Object, SimpleObject};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::crd::{
    self, InfrastructureTemplate as CrdTemplate,
    PangeaNamespace as CrdNamespace, Phase as CrdPhase,
};

/// Lifecycle phase of an InfrastructureTemplate.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Enum, Serialize, Deserialize)]
pub enum Phase {
    Pending,
    /// M2 — Verifying ArchitectureGem registry.
    Verifying,
    /// M2 — typed gate passed; safe to compile.
    Verified,
    Compiling,
    Initializing,
    Planning,
    Applying,
    Ready,
    Drifted,
    Failed,
    /// Self-healing park: HEAD observed, compile of it cannot
    /// succeed; retried on backoff.
    CompileBlocked,
    Destroying,
}

impl From<CrdPhase> for Phase {
    fn from(phase: CrdPhase) -> Self {
        match phase {
            CrdPhase::Pending => Phase::Pending,
            CrdPhase::Verifying => Phase::Verifying,
            CrdPhase::Verified => Phase::Verified,
            CrdPhase::Compiling => Phase::Compiling,
            CrdPhase::Initializing => Phase::Initializing,
            CrdPhase::Planning => Phase::Planning,
            CrdPhase::Applying => Phase::Applying,
            CrdPhase::Ready => Phase::Ready,
            CrdPhase::Drifted => Phase::Drifted,
            CrdPhase::Failed => Phase::Failed,
            CrdPhase::CompileBlocked => Phase::CompileBlocked,
            CrdPhase::Destroying => Phase::Destroying,
        }
    }
}

/// Summary of managed resources.
#[derive(Debug, Clone, Default, SimpleObject, Serialize, Deserialize)]
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

impl From<&crd::ResourceSummary> for ResourceCounts {
    fn from(summary: &crd::ResourceSummary) -> Self {
        Self {
            total: summary.total as i32,
            added: summary.added as i32,
            changed: summary.changed as i32,
            destroyed: summary.destroyed as i32,
        }
    }
}

/// Condition representing current state.
#[derive(Debug, Clone, SimpleObject, Serialize, Deserialize)]
pub struct Condition {
    /// Type of condition.
    pub condition_type: String,
    /// Status (True, False, Unknown).
    pub status: String,
    /// Last transition time.
    pub last_transition_time: DateTime<Utc>,
    /// Machine-readable reason.
    pub reason: String,
    /// Human-readable message.
    pub message: String,
}

impl From<&crd::Condition> for Condition {
    fn from(c: &crd::Condition) -> Self {
        Self {
            condition_type: c.r#type.clone(),
            status: c.status.clone(),
            last_transition_time: c.last_transition_time,
            reason: c.reason.clone(),
            message: c.message.clone(),
        }
    }
}

/// Template source information.
#[derive(Debug, Clone, SimpleObject, Serialize, Deserialize)]
pub struct TemplateSource {
    /// Source type (inline, configMap, git).
    pub source_type: String,
    /// Source reference (content preview, configMap name, or git URL).
    pub reference: String,
}

impl From<&crd::TemplateSource> for TemplateSource {
    fn from(source: &crd::TemplateSource) -> Self {
        if let Some(inline) = &source.inline {
            let preview = if inline.len() > 100 {
                format!("{}...", &inline[..100])
            } else {
                inline.clone()
            };
            Self {
                source_type: "inline".to_string(),
                reference: preview,
            }
        } else if let Some(cm) = &source.config_map_ref {
            Self {
                source_type: "configMap".to_string(),
                reference: format!("{}/{}", cm.name, cm.key),
            }
        } else if let Some(git) = &source.git_repository {
            Self {
                source_type: "git".to_string(),
                reference: format!("{}@{}", git.url, git.r#ref),
            }
        } else {
            Self {
                source_type: "unknown".to_string(),
                reference: "".to_string(),
            }
        }
    }
}

/// Infrastructure template GraphQL type.
pub struct InfrastructureTemplate {
    inner: CrdTemplate,
}

impl From<CrdTemplate> for InfrastructureTemplate {
    fn from(template: CrdTemplate) -> Self {
        Self { inner: template }
    }
}

#[Object]
impl InfrastructureTemplate {
    /// Kubernetes namespace.
    async fn namespace(&self) -> Option<String> {
        self.inner.metadata.namespace.clone()
    }

    /// Template name.
    async fn name(&self) -> Option<String> {
        self.inner.metadata.name.clone()
    }

    /// Current phase.
    async fn phase(&self) -> Phase {
        self.inner
            .status
            .as_ref()
            .and_then(|s| s.phase)
            .map(Phase::from)
            .unwrap_or(Phase::Pending)
    }

    /// Pangea namespace for state isolation.
    async fn pangea_namespace(&self) -> &str {
        &self.inner.spec.pangea_namespace
    }

    /// Template source.
    async fn source(&self) -> TemplateSource {
        TemplateSource::from(&self.inner.spec.source)
    }

    /// Whether auto-approve is enabled.
    async fn auto_approve(&self) -> bool {
        self.inner.spec.auto_approve
    }

    /// Whether reconciliation is suspended.
    async fn suspended(&self) -> bool {
        self.inner.spec.suspend
    }

    /// Last applied timestamp.
    async fn last_applied_at(&self) -> Option<DateTime<Utc>> {
        self.inner.status.as_ref().and_then(|s| s.last_applied_at)
    }

    /// Last planned timestamp.
    async fn last_planned_at(&self) -> Option<DateTime<Utc>> {
        self.inner.status.as_ref().and_then(|s| s.last_planned_at)
    }

    /// Resource counts.
    async fn resource_counts(&self) -> ResourceCounts {
        self.inner
            .status
            .as_ref()
            .and_then(|s| s.resources.as_ref())
            .map(ResourceCounts::from)
            .unwrap_or_default()
    }

    /// Conditions.
    async fn conditions(&self) -> Vec<Condition> {
        self.inner
            .status
            .as_ref()
            .map(|s| s.conditions.iter().map(Condition::from).collect())
            .unwrap_or_default()
    }

    /// Plan summary.
    async fn plan_summary(&self) -> Option<String> {
        self.inner
            .status
            .as_ref()
            .and_then(|s| s.plan_summary.clone())
    }

    /// Last error message.
    async fn last_error(&self) -> Option<String> {
        self.inner.status.as_ref().and_then(|s| s.last_error.clone())
    }

    /// Failure count.
    async fn failure_count(&self) -> i32 {
        self.inner
            .status
            .as_ref()
            .map(|s| s.failure_count as i32)
            .unwrap_or(0)
    }
}

/// Pangea namespace GraphQL type.
pub struct PangeaNamespace {
    inner: CrdNamespace,
}

impl From<CrdNamespace> for PangeaNamespace {
    fn from(ns: CrdNamespace) -> Self {
        Self { inner: ns }
    }
}

#[Object]
impl PangeaNamespace {
    /// Namespace name.
    async fn name(&self) -> Option<String> {
        self.inner.metadata.name.clone()
    }

    /// Backend type (pg, s3, local).
    async fn backend_type(&self) -> String {
        format!("{:?}", self.inner.spec.backend.r#type).to_lowercase()
    }

    /// Database host.
    async fn database_host(&self) -> Option<String> {
        self.inner
            .spec
            .backend
            .pg
            .as_ref()
            .map(|pg| pg.host.clone())
    }

    /// Database name.
    async fn database_name(&self) -> Option<String> {
        self.inner
            .spec
            .backend
            .pg
            .as_ref()
            .map(|pg| pg.database.clone())
    }

    /// Is backend ready.
    async fn is_ready(&self) -> bool {
        self.inner
            .status
            .as_ref()
            .map(|s| s.backend_ready)
            .unwrap_or(false)
    }

    /// Schema name.
    async fn schema_name(&self) -> Option<String> {
        self.inner
            .status
            .as_ref()
            .and_then(|s| s.schema_name.clone())
    }

    /// Template count.
    async fn template_count(&self) -> i32 {
        self.inner
            .status
            .as_ref()
            .map(|s| s.template_count as i32)
            .unwrap_or(0)
    }
}

/// Result of a plan operation.
#[derive(Debug, Clone, SimpleObject, Serialize, Deserialize)]
pub struct PlanResult {
    /// Whether there are changes.
    pub has_changes: bool,
    /// Human-readable summary.
    pub summary: String,
    /// Resources to add.
    pub added: i32,
    /// Resources to change.
    pub changed: i32,
    /// Resources to destroy.
    pub destroyed: i32,
}

/// Log entry from reconciliation.
#[derive(Debug, Clone, SimpleObject, Serialize, Deserialize)]
pub struct LogEntry {
    /// Timestamp.
    pub timestamp: DateTime<Utc>,
    /// Log level.
    pub level: String,
    /// Message.
    pub message: String,
    /// Template namespace.
    pub namespace: String,
    /// Template name.
    pub name: String,
}

/// Input for triggering operations.
#[derive(Debug, Clone, InputObject)]
pub struct TemplateInput {
    /// Kubernetes namespace.
    pub namespace: String,
    /// Template name.
    pub name: String,
}
