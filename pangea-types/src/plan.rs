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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_plan_result_default() {
        let pr = PlanResult::default();
        assert!(!pr.has_changes);
        assert_eq!(pr.summary, "No changes");
        assert_eq!(pr.added, 0);
        assert_eq!(pr.changed, 0);
        assert_eq!(pr.destroyed, 0);
    }

    #[test]
    fn test_no_changes() {
        let pr = PlanResult::no_changes();
        assert!(!pr.has_changes);
        assert_eq!(pr.total_affected(), 0);
    }

    #[test]
    fn test_with_changes_nonzero() {
        let pr = PlanResult::with_changes(3, 2, 1);
        assert!(pr.has_changes);
        assert_eq!(pr.added, 3);
        assert_eq!(pr.changed, 2);
        assert_eq!(pr.destroyed, 1);
        assert_eq!(pr.total_affected(), 6);
        assert!(pr.summary.contains("3 to add"));
        assert!(pr.summary.contains("2 to change"));
        assert!(pr.summary.contains("1 to destroy"));
    }

    #[test]
    fn test_with_changes_all_zero() {
        let pr = PlanResult::with_changes(0, 0, 0);
        assert!(!pr.has_changes);
        assert_eq!(pr.summary, "No changes");
        assert_eq!(pr.total_affected(), 0);
    }

    #[test]
    fn test_with_changes_only_added() {
        let pr = PlanResult::with_changes(5, 0, 0);
        assert!(pr.has_changes);
        assert_eq!(pr.total_affected(), 5);
    }

    #[test]
    fn test_with_changes_only_destroyed() {
        let pr = PlanResult::with_changes(0, 0, 10);
        assert!(pr.has_changes);
        assert_eq!(pr.total_affected(), 10);
    }

    #[test]
    fn test_plan_result_serde_roundtrip() {
        let pr = PlanResult::with_changes(1, 2, 3);
        let json = serde_json::to_string(&pr).unwrap();
        let back: PlanResult = serde_json::from_str(&json).unwrap();
        assert_eq!(pr, back);
    }
}
