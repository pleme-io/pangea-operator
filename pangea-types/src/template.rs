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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_resource_counts_default() {
        let rc = ResourceCounts::default();
        assert_eq!(rc.total, 0);
        assert_eq!(rc.added, 0);
        assert_eq!(rc.changed, 0);
        assert_eq!(rc.destroyed, 0);
    }

    #[test]
    fn test_template_source_default() {
        let ts = TemplateSource::default();
        assert_eq!(ts.source_type, "inline");
        assert!(ts.reference.is_empty());
    }

    #[test]
    fn test_infrastructure_template_default() {
        let tmpl = InfrastructureTemplate::default();
        assert_eq!(tmpl.phase, Phase::Pending);
        assert_eq!(tmpl.pangea_namespace, "default");
        assert!(!tmpl.auto_approve);
        assert!(!tmpl.suspended);
        assert!(tmpl.namespace.is_none());
        assert!(tmpl.name.is_none());
        assert!(tmpl.last_applied_at.is_none());
        assert!(tmpl.plan_summary.is_none());
        assert!(tmpl.last_error.is_none());
        assert_eq!(tmpl.failure_count, 0);
    }

    #[test]
    fn test_full_name_with_both() {
        let tmpl = InfrastructureTemplate {
            namespace: Some("prod".to_string()),
            name: Some("web-server".to_string()),
            ..Default::default()
        };
        assert_eq!(tmpl.full_name(), "prod/web-server");
    }

    #[test]
    fn test_full_name_defaults_when_none() {
        let tmpl = InfrastructureTemplate::default();
        assert_eq!(tmpl.full_name(), "default/unnamed");
    }

    #[test]
    fn test_full_name_partial_none() {
        let tmpl = InfrastructureTemplate {
            namespace: Some("staging".to_string()),
            name: None,
            ..Default::default()
        };
        assert_eq!(tmpl.full_name(), "staging/unnamed");

        let tmpl2 = InfrastructureTemplate {
            namespace: None,
            name: Some("db".to_string()),
            ..Default::default()
        };
        assert_eq!(tmpl2.full_name(), "default/db");
    }

    #[test]
    fn test_has_pending_changes_no_changes() {
        let tmpl = InfrastructureTemplate::default();
        assert!(!tmpl.has_pending_changes());
    }

    #[test]
    fn test_has_pending_changes_added() {
        let tmpl = InfrastructureTemplate {
            resource_counts: ResourceCounts { added: 1, ..Default::default() },
            ..Default::default()
        };
        assert!(tmpl.has_pending_changes());
    }

    #[test]
    fn test_has_pending_changes_changed() {
        let tmpl = InfrastructureTemplate {
            resource_counts: ResourceCounts { changed: 1, ..Default::default() },
            ..Default::default()
        };
        assert!(tmpl.has_pending_changes());
    }

    #[test]
    fn test_has_pending_changes_destroyed() {
        let tmpl = InfrastructureTemplate {
            resource_counts: ResourceCounts { destroyed: 1, ..Default::default() },
            ..Default::default()
        };
        assert!(tmpl.has_pending_changes());
    }

    #[test]
    fn test_infrastructure_template_serde_roundtrip() {
        let tmpl = InfrastructureTemplate {
            namespace: Some("test".to_string()),
            name: Some("my-template".to_string()),
            phase: Phase::Ready,
            pangea_namespace: "production".to_string(),
            source: TemplateSource {
                source_type: "gitRepository".to_string(),
                reference: "https://github.com/example/repo".to_string(),
            },
            auto_approve: true,
            suspended: false,
            last_applied_at: Some(DateTime("2024-01-01T00:00:00Z".to_string())),
            resource_counts: ResourceCounts { total: 10, added: 2, changed: 1, destroyed: 0 },
            plan_summary: Some("2 to add, 1 to change".to_string()),
            last_error: None,
            failure_count: 0,
        };
        let json = serde_json::to_string(&tmpl).unwrap();
        let back: InfrastructureTemplate = serde_json::from_str(&json).unwrap();
        assert_eq!(tmpl, back);
    }
}
