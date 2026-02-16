//! Plan result types.

use serde::{Deserialize, Serialize};

/// Result of a Terraform plan operation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "server", derive(async_graphql::SimpleObject))]
pub struct PlanResult {
    /// Whether the plan has any changes.
    pub has_changes: bool,
    /// Human-readable summary of changes.
    pub summary: String,
    /// Number of resources to add.
    pub added: i32,
    /// Number of resources to change.
    pub changed: i32,
    /// Number of resources to destroy.
    pub destroyed: i32,
}

impl Default for PlanResult {
    fn default() -> Self {
        Self {
            has_changes: false,
            summary: "No changes".to_string(),
            added: 0,
            changed: 0,
            destroyed: 0,
        }
    }
}

impl PlanResult {
    /// Creates a new PlanResult with no changes.
    pub fn no_changes() -> Self {
        Self::default()
    }

    /// Creates a new PlanResult with the given counts.
    pub fn with_changes(added: i32, changed: i32, destroyed: i32) -> Self {
        let has_changes = added > 0 || changed > 0 || destroyed > 0;
        let summary = if has_changes {
            format!(
                "Plan: {} to add, {} to change, {} to destroy",
                added, changed, destroyed
            )
        } else {
            "No changes".to_string()
        };

        Self {
            has_changes,
            summary,
            added,
            changed,
            destroyed,
        }
    }

    /// Returns the total number of affected resources.
    pub fn total_affected(&self) -> i32 {
        self.added + self.changed + self.destroyed
    }
}
