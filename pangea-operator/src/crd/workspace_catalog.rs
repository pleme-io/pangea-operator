//! WorkspaceCatalog CRD — declares an operator-watched workspace and
//! its workspace-level policy in the four-level cascade
//! (gem → workspace → template → resource).
//!
//! See `pleme-io/theory/PANGEA-WORKSPACE-RECONCILIATION.md` §
//! "Hierarchical policy cascade" for the design.
//!
//! Cluster-scoped because workspaces are typically a fleet-wide
//! concern (one workspace tree, many templates spread across
//! namespaces).

use chrono::{DateTime, Utc};
use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::architecture_gem::{ApprovalRouting, DriftReaction, SettlingPolicy};
use super::infrastructure_template::GitRepositoryRef;

#[derive(CustomResource, Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[kube(
    group = "pangea.pleme.io",
    version = "v1alpha1",
    kind = "WorkspaceCatalog",
    status = "WorkspaceCatalogStatus",
    shortname = "wsc",
    shortname = "wcat",
    printcolumn = r#"{"name":"Source","type":"string","jsonPath":".spec.source.gitRepository.url"}"#,
    printcolumn = r#"{"name":"Templates","type":"integer","jsonPath":".status.templateCount"}"#,
    printcolumn = r#"{"name":"Verified","type":"boolean","jsonPath":".status.verified"}"#,
    printcolumn = r#"{"name":"Age","type":"date","jsonPath":".metadata.creationTimestamp"}"#
)]
#[serde(rename_all = "camelCase")]
pub struct WorkspaceCatalogSpec {
    /// Where the workspace lives.
    pub source: WorkspaceSource,

    /// Architecture gems this workspace's templates need. Operator
    /// refuses to advance any template under this catalog past
    /// `Verified` until every listed gem has phase `Loaded` (every
    /// expected class loaded + every fixture passed).
    #[serde(default)]
    pub required_gems: Vec<String>,

    /// Workspace-level policy — applied to every template in the
    /// workspace unless overridden at template or resource level
    /// (cascade with safety precedence).
    #[serde(default)]
    pub policy: WorkspacePolicy,

    /// Suspend reconciliation of every template under this catalog.
    #[serde(default)]
    pub suspend: bool,
}

/// Where to read a workspace's source from.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct WorkspaceSource {
    /// Git repository (clone-and-watch). M3 supports gitRepository
    /// only.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub git_repository: Option<GitRepositoryRef>,

    /// Subdirectory within the repo containing the workspace.
    /// Example: `workspaces/cloudflare-pleme`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
}

/// Workspace-level policy — same shape as gem-level + a few
/// workspace-only fields.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct WorkspacePolicy {
    /// How often the operator runs `tofu plan` to detect drift.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reconcile_interval: Option<String>,

    /// Default drift reaction for every template under this
    /// workspace (overrides the gem default; can be overridden by
    /// a template-level policy).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub drift_reaction: Option<DriftReaction>,

    /// Settling policy — see `architecture_gem.rs`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub settling_policy: Option<SettlingPolicy>,

    /// Approval routing.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub approval_routing: Option<ApprovalRouting>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct WorkspaceCatalogStatus {
    /// Number of templates the operator has reconciled under this
    /// catalog.
    #[serde(default)]
    pub template_count: u32,

    /// Whether every required gem is `Loaded` AND the workspace
    /// itself is reachable.
    #[serde(default)]
    pub verified: bool,

    /// Last reconcile timestamp.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_reconcile_time: Option<DateTime<Utc>>,

    /// Conditions: `Reachable`, `GemsLoaded`, `Verified`.
    #[serde(default)]
    pub conditions: Vec<super::architecture_gem::Condition>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use kube::CustomResourceExt;

    #[test]
    fn workspace_catalog_crd_generates() {
        let crd = WorkspaceCatalog::crd();
        let yaml = serde_yaml::to_string(&crd).expect("crd serializes");
        assert!(yaml.contains("WorkspaceCatalog"));
        assert!(yaml.contains("pangea.pleme.io"));
    }

    #[test]
    fn workspace_policy_default_all_none() {
        let p = WorkspacePolicy::default();
        assert!(p.reconcile_interval.is_none());
        assert!(p.drift_reaction.is_none());
    }
}
