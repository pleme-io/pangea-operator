//! ReconciliationLoop CRD — the `roda` (Brazilian-Portuguese "wheel"): one
//! typed reconcile loop that drives a CRD-selected set of workspaces at its
//! own cadence + concurrency.
//!
//! See `pleme-io/theory/RECONCILIATION-TOPOLOGY.md` §II. The load-bearing
//! move is decoupling the two granularities a reconciler conflates:
//!   - **state granularity** — what unit of state converges (the workspace);
//!   - **loop granularity** — what unit drives the cadence (this CRD).
//!
//! Today an `InfrastructureTemplate` binds both: one template = one bag of
//! resources = one implicit cadence. A `ReconciliationLoop` makes the loop
//! axis explicit + configurable: one `roda` drives 1 workspace or 100 (the
//! selector decides), and the operator hosts many `roda`s concurrently — a
//! high-churn group on a fast wheel, a stable fleet on a slow wheel, a
//! critical resource on a dedicated 1:1 wheel. Membership is a label;
//! re-homing is a label change — all CRD, all GitOps.
//!
//! Cluster-scoped: a loop spans templates across namespaces (a fleet concern).
//!
//! **First increment (this CRD + controller):** the loop resolves its
//! selector against `InfrastructureTemplate`s, reports membership + ticks at
//! its cadence, and triggers a coordinated reconcile of its members on that
//! cadence. Full cadence-ownership (members reconcile ONLY on the loop's
//! schedule, suppressing their own interval) + the `malha` one-resource-per-
//! workspace axis (§III) are the next increment.

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::architecture_gem::{ReactivePolicy, SettlingPolicy};

/// Default base requeue cadence when `spec.cadence` is unset.
pub const DEFAULT_CADENCE: &str = "5m";
/// Default parallel reconciles within one loop.
pub const fn default_concurrency() -> u32 {
    4
}
/// Default shigoto worker budget bound for one loop.
pub const fn default_worker_budget() -> u32 {
    16
}

#[derive(CustomResource, Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[kube(
    group = "pangea.pleme.io",
    version = "v1alpha1",
    kind = "ReconciliationLoop",
    status = "ReconciliationLoopStatus",
    shortname = "roda",
    category = "pangea",
    printcolumn = r#"{"name":"Cadence","type":"string","jsonPath":".spec.cadence"}"#,
    printcolumn = r#"{"name":"Concurrency","type":"integer","jsonPath":".spec.concurrency"}"#,
    printcolumn = r#"{"name":"Members","type":"integer","jsonPath":".status.matchedCount"}"#,
    printcolumn = r#"{"name":"Suspended","type":"boolean","jsonPath":".spec.suspend"}"#,
    printcolumn = r#"{"name":"Age","type":"date","jsonPath":".metadata.creationTimestamp"}"#
)]
#[serde(rename_all = "camelCase")]
pub struct ReconciliationLoopSpec {
    /// Which workspaces (InfrastructureTemplates) this loop drives. A
    /// template is a member iff every label in `matchLabels` is present + equal
    /// on the template's `metadata.labels`. An empty selector matches nothing
    /// (never "everything" — a runaway loop is unrepresentable by default).
    pub selector: LoopSelector,

    /// Base requeue interval for this loop's tick (e.g. "5m", "30s"). The
    /// scheduling dual of the state axis: fine state, bounded loops.
    #[serde(default = "default_cadence")]
    pub cadence: String,

    /// Parallel reconciles within this one loop (the shigoto wave width).
    #[serde(default = "default_concurrency")]
    pub concurrency: u32,

    /// shigoto scheduler budget bound for this loop's Dag.
    #[serde(default = "default_worker_budget")]
    pub worker_budget: u32,

    /// Per-loop settling policy default (drift-cycle exhaustion bound).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub settling_policy: Option<SettlingPolicy>,

    /// Per-loop reactive/anomaly policy default (failure / timeout escalation).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub anomaly_policy: Option<ReactivePolicy>,

    /// Halt this loop's ticking until cleared.
    #[serde(default)]
    pub suspend: bool,
}

fn default_cadence() -> String {
    DEFAULT_CADENCE.to_string()
}

/// A label-based selector binding workspaces to this loop. matchLabels only
/// (the theory's surface); a richer expression form is a later addition.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct LoopSelector {
    /// All of these label key=value pairs must be present + equal on a
    /// template for it to be a member. Empty ⇒ matches nothing.
    #[serde(default)]
    pub match_labels: BTreeMap<String, String>,
}

impl LoopSelector {
    /// Does a candidate's labels satisfy this selector? Empty selector ⇒
    /// false (matches nothing — no accidental fleet-wide loop).
    #[must_use]
    pub fn matches(&self, labels: &BTreeMap<String, String>) -> bool {
        if self.match_labels.is_empty() {
            return false;
        }
        self.match_labels
            .iter()
            .all(|(k, v)| labels.get(k).is_some_and(|lv| lv == v))
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ReconciliationLoopStatus {
    /// Lifecycle phase of the loop itself.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub phase: Option<LoopPhase>,

    /// The `namespace/name` of every InfrastructureTemplate currently bound to
    /// this loop (the resolved membership — the observability half of `roda`).
    #[serde(default)]
    pub matched_workspaces: Vec<String>,

    /// Count of `matched_workspaces` (printer column).
    #[serde(default)]
    pub matched_count: u32,

    /// When this loop last completed a cadence tick.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_tick_at: Option<DateTime<Utc>>,

    /// Generation this status reflects (diff-gate against spec mutations).
    #[serde(default)]
    pub observed_generation: i64,

    /// Standard typed conditions (Ready / Healthy), timestamp-stable.
    #[serde(default)]
    pub conditions: Vec<super::Condition>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub enum LoopPhase {
    /// Selector resolved, members bound, ticking at cadence.
    Active,
    /// `spec.suspend = true` — not ticking.
    Suspended,
    /// Selector resolves to zero members (informative, not an error).
    Empty,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn labels(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn selector_matches_when_all_labels_present() {
        let sel = LoopSelector {
            match_labels: labels(&[("pleme.io/group", "cloudflare-edge")]),
        };
        assert!(sel.matches(&labels(&[
            ("pleme.io/group", "cloudflare-edge"),
            ("x", "y")
        ])));
        assert!(!sel.matches(&labels(&[("pleme.io/group", "other")])));
        assert!(!sel.matches(&labels(&[("x", "y")])));
    }

    #[test]
    fn empty_selector_matches_nothing() {
        // A runaway fleet-wide loop is unrepresentable by default.
        let sel = LoopSelector::default();
        assert!(!sel.matches(&labels(&[("anything", "at-all")])));
        assert!(!sel.matches(&BTreeMap::new()));
    }

    #[test]
    fn multi_label_selector_requires_all() {
        let sel = LoopSelector {
            match_labels: labels(&[("pleme.io/group", "edge"), ("pleme.io/tier", "critical")]),
        };
        assert!(sel.matches(&labels(&[
            ("pleme.io/group", "edge"),
            ("pleme.io/tier", "critical"),
            ("extra", "ok"),
        ])));
        // Missing one required label ⇒ not a member.
        assert!(!sel.matches(&labels(&[("pleme.io/group", "edge")])));
    }
}
