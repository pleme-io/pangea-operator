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

            *summary
                .changes_by_type
                .entry(change.resource_type.clone())
                .or_insert(0) += 1;
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

    /// Render per-resource drift details for status surfacing.
    ///
    /// Capped at `limit` entries so the K8s status object stays small
    /// (default 50 — full list is available via the operator's
    /// GraphQL API for large plans). Skips no-op changes since they
    /// add noise without observability value.
    ///
    /// Attribute names are emitted but values are intentionally
    /// elided — keeps secrets out of the K8s API surface.
    pub fn drift_details(&self, limit: usize) -> Vec<DriftDetail> {
        self.resource_changes
            .iter()
            .filter_map(|rc| {
                let action = ChangeType::from_actions(&rc.change.actions);
                if matches!(action, ChangeType::NoOp) {
                    return None;
                }
                let attrs = changed_attributes(&rc.change);
                let risk = risk_level(action, &rc.resource_type, attrs.len());
                Some(DriftDetail {
                    address: rc.address.clone(),
                    action: action.as_str().to_string(),
                    risk: risk.to_string(),
                    attributes: attrs,
                })
            })
            .take(limit)
            .collect()
    }
}

/// Per-resource drift summary destined for the status object.
/// Mirrors `crd::infrastructure_template::DriftDetail`; kept here as
/// a plain struct so the executor module stays free of CRD imports.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DriftDetail {
    pub address: String,
    pub action: String,
    pub risk: String,
    #[serde(default)]
    pub attributes: Vec<String>,
}

/// Compute the symmetric set of attribute names that differ between
/// `before` and `after`. Used as a coarse-grained "what changed"
/// signal — values are deliberately not exposed.
fn changed_attributes(change: &Change) -> Vec<String> {
    use serde_json::Value;
    let before = change.before.as_ref().and_then(|v| v.as_object());
    let after = change.after.as_ref().and_then(|v| v.as_object());
    match (before, after) {
        (Some(b), Some(a)) => {
            let mut keys: Vec<String> = Vec::new();
            for (k, v_after) in a {
                let v_before = b.get(k).unwrap_or(&Value::Null);
                if v_before != v_after {
                    keys.push(k.clone());
                }
            }
            for k in b.keys() {
                if !a.contains_key(k) {
                    keys.push(k.clone());
                }
            }
            keys.sort();
            keys.dedup();
            keys
        }
        _ => Vec::new(),
    }
}

/// Heuristic risk classifier.
///
/// Conservative defaults: deletes / replaces of stateful resource
/// types (database, dns, network) are `high`; updates with many
/// attribute diffs are `medium`; single-attr updates are `low`;
/// pure creates are `low` (no risk to existing infra).
fn risk_level(action: ChangeType, resource_type: &str, attr_count: usize) -> &'static str {
    let stateful = matches!(
        resource_type,
        t if t.contains("dns_record")
            || t.contains("zone")
            || t.contains("database")
            || t.contains("rds")
            || t.contains("vpc")
            || t.contains("tunnel")
    );
    match action {
        ChangeType::Delete | ChangeType::Replace if stateful => "high",
        ChangeType::Delete | ChangeType::Replace => "medium",
        ChangeType::Update if attr_count > 3 => "medium",
        ChangeType::Update => "low",
        ChangeType::Create => "low",
        ChangeType::NoOp => "none",
    }
}

impl ChangeType {
    /// Lowercase action string for status surfacing.
    pub fn as_str(&self) -> &'static str {
        match self {
            ChangeType::Create => "create",
            ChangeType::Update => "update",
            ChangeType::Delete => "delete",
            ChangeType::Replace => "replace",
            ChangeType::NoOp => "noop",
        }
    }
}

impl PlanSummary {
    /// Format as human-readable string.
    pub fn format(&self) -> String {
        if !self.has_changes {
            return "No changes".to_string();
        }

        format!("+{} ~{} -{}", self.added, self.changed, self.destroyed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn minimal_plan_json() -> String {
        serde_json::json!({
            "format_version": "1.2",
            "terraform_version": "1.6.0",
            "resource_changes": [],
            "output_changes": {},
            "configuration": {}
        })
        .to_string()
    }

    fn plan_json_with_changes() -> String {
        serde_json::json!({
            "format_version": "1.2",
            "terraform_version": "1.6.0",
            "resource_changes": [
                {
                    "address": "aws_vpc.main",
                    "type": "aws_vpc",
                    "name": "main",
                    "provider_name": "registry.terraform.io/hashicorp/aws",
                    "change": { "actions": ["create"] }
                },
                {
                    "address": "aws_subnet.public",
                    "type": "aws_subnet",
                    "name": "public",
                    "provider_name": "registry.terraform.io/hashicorp/aws",
                    "change": { "actions": ["update"] }
                },
                {
                    "address": "aws_security_group.old",
                    "type": "aws_security_group",
                    "name": "old",
                    "provider_name": "registry.terraform.io/hashicorp/aws",
                    "change": { "actions": ["delete"] }
                },
                {
                    "address": "aws_instance.web",
                    "type": "aws_instance",
                    "name": "web",
                    "provider_name": "registry.terraform.io/hashicorp/aws",
                    "change": { "actions": ["delete", "create"] }
                },
                {
                    "address": "aws_s3_bucket.data",
                    "type": "aws_s3_bucket",
                    "name": "data",
                    "provider_name": "registry.terraform.io/hashicorp/aws",
                    "change": { "actions": ["no-op"] }
                }
            ],
            "output_changes": {},
            "configuration": {}
        })
        .to_string()
    }

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
    fn test_change_type_from_empty_actions() {
        assert_eq!(ChangeType::from_actions(&[]), ChangeType::NoOp);
    }

    #[test]
    fn test_change_type_replace_order_independent() {
        assert_eq!(
            ChangeType::from_actions(&["create".to_string(), "delete".to_string()]),
            ChangeType::Replace
        );
        assert_eq!(
            ChangeType::from_actions(&["delete".to_string(), "create".to_string()]),
            ChangeType::Replace
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

    #[test]
    fn test_plan_from_json_minimal() {
        let plan = Plan::from_json(&minimal_plan_json()).unwrap();
        assert_eq!(plan.format_version, "1.2");
        assert_eq!(plan.terraform_version, "1.6.0");
        assert!(plan.resource_changes.is_empty());
    }

    #[test]
    fn test_plan_from_json_invalid() {
        assert!(Plan::from_json("{invalid json}").is_err());
        assert!(Plan::from_json("").is_err());
    }

    #[test]
    fn test_plan_summary_with_changes() {
        let plan = Plan::from_json(&plan_json_with_changes()).unwrap();
        let summary = plan.summary();

        assert!(summary.has_changes);
        // create (1) + replace adds 1 = 2
        assert_eq!(summary.added, 2);
        assert_eq!(summary.changed, 1);
        // delete (1) + replace adds 1 = 2
        assert_eq!(summary.destroyed, 2);
        assert_eq!(summary.total, 5);
    }

    #[test]
    fn test_plan_summary_no_changes() {
        let plan = Plan::from_json(&minimal_plan_json()).unwrap();
        let summary = plan.summary();
        assert!(!summary.has_changes);
        assert_eq!(summary.added, 0);
        assert_eq!(summary.changed, 0);
        assert_eq!(summary.destroyed, 0);
    }

    #[test]
    fn test_plan_summary_changes_by_type() {
        let plan = Plan::from_json(&plan_json_with_changes()).unwrap();
        let summary = plan.summary();
        assert_eq!(*summary.changes_by_type.get("aws_vpc").unwrap(), 1);
        assert_eq!(*summary.changes_by_type.get("aws_subnet").unwrap(), 1);
        assert_eq!(
            *summary.changes_by_type.get("aws_security_group").unwrap(),
            1
        );
        assert_eq!(*summary.changes_by_type.get("aws_instance").unwrap(), 1);
        assert_eq!(*summary.changes_by_type.get("aws_s3_bucket").unwrap(), 1);
    }

    #[test]
    fn test_plan_resources_to_create() {
        let plan = Plan::from_json(&plan_json_with_changes()).unwrap();
        let creates = plan.resources_to_create();
        let names: Vec<&str> = creates.iter().map(|r| r.name.as_str()).collect();
        assert!(names.contains(&"main")); // create
        assert!(names.contains(&"web")); // replace includes create
        assert!(!names.contains(&"public")); // update only
        assert!(!names.contains(&"old")); // delete only
    }

    #[test]
    fn test_plan_resources_to_destroy() {
        let plan = Plan::from_json(&plan_json_with_changes()).unwrap();
        let destroys = plan.resources_to_destroy();
        let names: Vec<&str> = destroys.iter().map(|r| r.name.as_str()).collect();
        assert!(names.contains(&"old")); // delete
        assert!(names.contains(&"web")); // replace includes delete
        assert!(!names.contains(&"main")); // create only
    }

    #[test]
    fn test_plan_resources_to_update() {
        let plan = Plan::from_json(&plan_json_with_changes()).unwrap();
        let updates = plan.resources_to_update();
        assert_eq!(updates.len(), 1);
        assert_eq!(updates[0].name, "public");
    }

    #[test]
    fn test_plan_format_summary_with_changes() {
        let plan = Plan::from_json(&plan_json_with_changes()).unwrap();
        let formatted = plan.format_summary();
        assert!(formatted.contains("to add"));
        assert!(formatted.contains("to change"));
        assert!(formatted.contains("to destroy"));
    }

    #[test]
    fn test_plan_format_summary_no_changes() {
        let plan = Plan::from_json(&minimal_plan_json()).unwrap();
        let formatted = plan.format_summary();
        assert_eq!(formatted, "No changes. Infrastructure is up-to-date.");
    }

    #[test]
    fn test_plan_summary_default() {
        let summary = PlanSummary::default();
        assert_eq!(summary.added, 0);
        assert_eq!(summary.changed, 0);
        assert_eq!(summary.destroyed, 0);
        assert_eq!(summary.total, 0);
        assert!(!summary.has_changes);
        assert!(summary.changes_by_type.is_empty());
    }

    #[test]
    fn test_plan_from_json_with_output_changes() {
        let json = serde_json::json!({
            "format_version": "1.2",
            "terraform_version": "1.6.0",
            "resource_changes": [],
            "output_changes": {
                "vpc_id": {
                    "actions": ["create"],
                    "after": "vpc-12345"
                }
            },
            "configuration": {}
        })
        .to_string();
        let plan = Plan::from_json(&json).unwrap();
        assert_eq!(plan.output_changes.len(), 1);
        assert!(plan.output_changes.contains_key("vpc_id"));
    }

    #[test]
    fn test_drift_details_skips_noop_and_caps_to_limit() {
        let plan = Plan::from_json(&plan_json_with_changes()).unwrap();
        let details = plan.drift_details(50);

        // 5 changes total in fixture, 1 is no-op → 4 entries.
        assert_eq!(details.len(), 4);
        assert!(details.iter().all(|d| d.action != "noop"));

        let by_addr: std::collections::HashMap<&str, &DriftDetail> =
            details.iter().map(|d| (d.address.as_str(), d)).collect();
        assert_eq!(by_addr.get("aws_vpc.main").unwrap().action, "create");
        assert_eq!(by_addr.get("aws_subnet.public").unwrap().action, "update");
        assert_eq!(
            by_addr.get("aws_security_group.old").unwrap().action,
            "delete"
        );
        assert_eq!(by_addr.get("aws_instance.web").unwrap().action, "replace");
    }

    #[test]
    fn test_drift_details_risk_classification() {
        let plan = Plan::from_json(&plan_json_with_changes()).unwrap();
        let details = plan.drift_details(50);
        let by_addr: std::collections::HashMap<&str, &DriftDetail> =
            details.iter().map(|d| (d.address.as_str(), d)).collect();

        // VPC delete/replace → high (stateful keyword match)
        assert_eq!(by_addr.get("aws_vpc.main").unwrap().risk, "low"); // create-only
                                                                      // SG delete on a non-stateful resource → medium
        assert_eq!(
            by_addr.get("aws_security_group.old").unwrap().risk,
            "medium"
        );
    }

    #[test]
    fn test_drift_details_limit_caps_results() {
        let plan = Plan::from_json(&plan_json_with_changes()).unwrap();
        let details = plan.drift_details(2);
        assert_eq!(details.len(), 2);
    }

    #[test]
    fn test_changed_attributes_diffs_keys() {
        let json = serde_json::json!({
            "format_version": "1.2",
            "terraform_version": "1.6.0",
            "resource_changes": [{
                "address": "test.foo",
                "type": "test",
                "name": "foo",
                "provider_name": "registry/test",
                "change": {
                    "actions": ["update"],
                    "before": { "name": "old", "ttl": 60, "kept": "same" },
                    "after":  { "name": "new", "ttl": 120, "kept": "same" }
                }
            }]
        })
        .to_string();
        let plan = Plan::from_json(&json).unwrap();
        let details = plan.drift_details(50);
        assert_eq!(details.len(), 1);
        assert_eq!(
            details[0].attributes,
            vec!["name".to_string(), "ttl".to_string()]
        );
        assert!(!details[0].attributes.contains(&"kept".to_string()));
    }
}
