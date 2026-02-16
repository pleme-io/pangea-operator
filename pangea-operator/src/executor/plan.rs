//! OpenTofu plan parsing and analysis.

use crate::error::{Error, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Parsed OpenTofu plan.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Plan {
    /// Format version.
    pub format_version: String,

    /// Terraform/OpenTofu version.
    pub terraform_version: String,

    /// Planned values.
    #[serde(default)]
    pub planned_values: PlannedValues,

    /// Resource changes.
    #[serde(default)]
    pub resource_changes: Vec<ResourceChange>,

    /// Output changes.
    #[serde(default)]
    pub output_changes: HashMap<String, OutputChange>,

    /// Configuration.
    #[serde(default)]
    pub configuration: serde_json::Value,
}

/// Planned values in a plan.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PlannedValues {
    /// Root module.
    #[serde(default)]
    pub root_module: PlannedModule,
}

/// A planned module.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PlannedModule {
    /// Resources in this module.
    #[serde(default)]
    pub resources: Vec<PlannedResource>,

    /// Child modules.
    #[serde(default)]
    pub child_modules: Vec<PlannedModule>,
}

/// A planned resource.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlannedResource {
    /// Resource address.
    pub address: String,

    /// Resource type.
    #[serde(rename = "type")]
    pub resource_type: String,

    /// Resource name.
    pub name: String,

    /// Provider name.
    pub provider_name: String,

    /// Schema version.
    #[serde(default)]
    pub schema_version: u32,

    /// Resource values.
    #[serde(default)]
    pub values: serde_json::Value,

    /// Sensitive values.
    #[serde(default)]
    pub sensitive_values: serde_json::Value,
}

/// A resource change in a plan.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourceChange {
    /// Resource address.
    pub address: String,

    /// Resource type.
    #[serde(rename = "type")]
    pub resource_type: String,

    /// Resource name.
    pub name: String,

    /// Provider name.
    pub provider_name: String,

    /// Change details.
    pub change: Change,
}

/// Change details.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Change {
    /// Actions to perform.
    pub actions: Vec<String>,

    /// Before values.
    #[serde(default)]
    pub before: Option<serde_json::Value>,

    /// After values.
    #[serde(default)]
    pub after: Option<serde_json::Value>,

    /// After values (unknown).
    #[serde(default)]
    pub after_unknown: serde_json::Value,

    /// Before sensitive.
    #[serde(default)]
    pub before_sensitive: serde_json::Value,

    /// After sensitive.
    #[serde(default)]
    pub after_sensitive: serde_json::Value,
}

/// Output change.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OutputChange {
    /// Actions.
    pub actions: Vec<String>,

    /// Before value.
    #[serde(default)]
    pub before: Option<serde_json::Value>,

    /// After value.
    #[serde(default)]
    pub after: Option<serde_json::Value>,

    /// After unknown.
    #[serde(default)]
    pub after_unknown: bool,

    /// Before sensitive.
    #[serde(default)]
    pub before_sensitive: bool,

    /// After sensitive.
    #[serde(default)]
    pub after_sensitive: bool,
}

/// Type of change.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ChangeType {
    Create,
    Update,
    Delete,
    Replace,
    NoOp,
}

impl ChangeType {
    /// Parse from action strings.
    pub fn from_actions(actions: &[String]) -> Self {
        if actions.contains(&"create".to_string()) && actions.contains(&"delete".to_string()) {
            ChangeType::Replace
        } else if actions.contains(&"create".to_string()) {
            ChangeType::Create
        } else if actions.contains(&"delete".to_string()) {
            ChangeType::Delete
        } else if actions.contains(&"update".to_string()) {
            ChangeType::Update
        } else {
            ChangeType::NoOp
        }
    }
}

/// Summary of a plan.
#[derive(Debug, Clone, Default)]
pub struct PlanSummary {
    /// Number of resources to add.
    pub added: u32,

    /// Number of resources to change.
    pub changed: u32,

    /// Number of resources to destroy.
    pub destroyed: u32,

    /// Total resources.
    pub total: u32,

    /// Whether there are any changes.
    pub has_changes: bool,

    /// Resource changes by type.
    pub changes_by_type: HashMap<String, u32>,
}

impl Plan {
    /// Parse a plan from JSON.
    pub fn from_json(json: &str) -> Result<Self> {
        serde_json::from_str(json).map_err(|e| Error::Serialization(e))
    }

    /// Get a summary of the plan.
    pub fn summary(&self) -> PlanSummary {
        let mut summary = PlanSummary::default();

        for change in &self.resource_changes {
            let change_type = ChangeType::from_actions(&change.change.actions);

            match change_type {
                ChangeType::Create => summary.added += 1,
                ChangeType::Update => summary.changed += 1,
                ChangeType::Delete => summary.destroyed += 1,
                ChangeType::Replace => {
                    summary.added += 1;
                    summary.destroyed += 1;
                }
                ChangeType::NoOp => {}
            }

            *summary.changes_by_type.entry(change.resource_type.clone()).or_insert(0) += 1;
        }

        summary.total = self.resource_changes.len() as u32;
        summary.has_changes = summary.added > 0 || summary.changed > 0 || summary.destroyed > 0;

        summary
    }

    /// Get resources to be created.
    pub fn resources_to_create(&self) -> Vec<&ResourceChange> {
        self.resource_changes
            .iter()
            .filter(|r| {
                let ct = ChangeType::from_actions(&r.change.actions);
                matches!(ct, ChangeType::Create | ChangeType::Replace)
            })
            .collect()
    }

    /// Get resources to be destroyed.
    pub fn resources_to_destroy(&self) -> Vec<&ResourceChange> {
        self.resource_changes
            .iter()
            .filter(|r| {
                let ct = ChangeType::from_actions(&r.change.actions);
                matches!(ct, ChangeType::Delete | ChangeType::Replace)
            })
            .collect()
    }

    /// Get resources to be updated.
    pub fn resources_to_update(&self) -> Vec<&ResourceChange> {
        self.resource_changes
            .iter()
            .filter(|r| ChangeType::from_actions(&r.change.actions) == ChangeType::Update)
            .collect()
    }

    /// Format as human-readable summary.
    pub fn format_summary(&self) -> String {
        let summary = self.summary();

        if !summary.has_changes {
            return "No changes. Infrastructure is up-to-date.".to_string();
        }

        format!(
            "Plan: {} to add, {} to change, {} to destroy.",
            summary.added, summary.changed, summary.destroyed
        )
    }
}

impl PlanSummary {
    /// Format as human-readable string.
    pub fn format(&self) -> String {
        if !self.has_changes {
            return "No changes".to_string();
        }

        format!(
            "+{} ~{} -{}",
            self.added, self.changed, self.destroyed
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_change_type_from_actions() {
        assert_eq!(
            ChangeType::from_actions(&["create".to_string()]),
            ChangeType::Create
        );
        assert_eq!(
            ChangeType::from_actions(&["delete".to_string()]),
            ChangeType::Delete
        );
        assert_eq!(
            ChangeType::from_actions(&["update".to_string()]),
            ChangeType::Update
        );
        assert_eq!(
            ChangeType::from_actions(&["delete".to_string(), "create".to_string()]),
            ChangeType::Replace
        );
        assert_eq!(
            ChangeType::from_actions(&["no-op".to_string()]),
            ChangeType::NoOp
        );
    }

    #[test]
    fn test_plan_summary_format() {
        let mut summary = PlanSummary::default();
        assert_eq!(summary.format(), "No changes");

        summary.added = 2;
        summary.changed = 1;
        summary.destroyed = 3;
        summary.has_changes = true;

        assert_eq!(summary.format(), "+2 ~1 -3");
    }
}
