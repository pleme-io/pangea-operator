//! Reconcile-cycle receipt construction + persistence for
//! `InfrastructureTemplate`.
//!
//! Lifted from `template_controller.rs` during T3 (continuation of
//! R6/T1/T2). Each `record_reconcile_cycle` call builds a typed
//! receipt summarizing what changed in this cycle and patches it onto
//! `status.lastCycle` (with a content-equality guard to skip etcd
//! churn for Matched-only steady-state Ready→Ready flows).
//!
//! `CycleResult` is the typed input to `build_reconcile_cycle`; its
//! variants drive the per-resource outcome derivation (NoChanges →
//! Matched, AppliedSuccess → action-derived with import override,
//! AppliedFailure → Failed, PolicyGated → Drifted).

use chrono::{DateTime, Utc};
use kube::ResourceExt;
use std::collections::HashSet;
use tracing::{debug, info};

use crate::controller::ControllerState;
use crate::crd::{
    CycleSummary, DriftDetail, InfrastructureTemplate, Outcome, PolicyDecision, ReconcileCycle,
    ResourceOutcome,
};
use crate::error::Result;

/// What triggered the cycle emission — drives outcome derivation when
/// translating drift_details into per-resource ResourceOutcome entries.
#[derive(Debug, Clone)]
pub enum CycleResult {
    /// Plan reported no changes — every managed resource matched
    /// declared state. Apply did not run.
    NoChanges,
    /// Apply ran successfully on every change in `drift_details`.
    /// Each entry's terraform action becomes the per-resource outcome,
    /// with the additional twist that any address in
    /// `imported_addresses` overrides its action-derived outcome to
    /// `Outcome::Imported` — the cycle adopted that resource into
    /// state via `tofu import` before the apply ran.
    AppliedSuccess { imported_addresses: Vec<String> },
    /// Apply errored. Every change in `drift_details` becomes
    /// `Failed`; the apply error is attached as `message`.
    AppliedFailure(String),
    /// Policy gated this cycle (refuse / requireApproval). Apply did
    /// NOT run; every change is `Drifted` (uncorrected) with the
    /// policy decision as `message`.
    PolicyGated(PolicyDecision),
}

/// Build a typed receipt summarizing one reconcile cycle.
///
/// `drifts` is the set of resources the plan reported a change on
/// (already annotated with policy decisions when relevant).
/// `total` is the total managed-resource count; `total - drifts.len()`
/// becomes `summary.matched`. `next_cycle` is the cycle number for
/// THIS cycle (caller has already incremented `status.cycle_count`).
pub fn build_reconcile_cycle(
    next_cycle: u64,
    started_at: DateTime<Utc>,
    drifts: &[DriftDetail],
    total: u32,
    plan_summary: Option<String>,
    source_revision: Option<String>,
    result: CycleResult,
) -> ReconcileCycle {
    let mut summary = CycleSummary::default();

    let imported_set: HashSet<&str> = match &result {
        CycleResult::AppliedSuccess { imported_addresses } => {
            imported_addresses.iter().map(String::as_str).collect()
        }
        _ => HashSet::new(),
    };

    let outcomes: Vec<ResourceOutcome> = drifts
        .iter()
        .take(100)
        .map(|d| {
            let (outcome, message) = match result {
                CycleResult::AppliedFailure(ref err) => {
                    (Outcome::Failed, Some(truncate_for_status(err)))
                }
                CycleResult::PolicyGated(decision) => (
                    Outcome::Drifted,
                    Some(format!("policy decision: {}", decision.as_str())),
                ),
                CycleResult::NoChanges => {
                    // Defensive: a NoChanges cycle should have empty
                    // drifts. If we got here, treat as Matched.
                    (Outcome::Matched, None)
                }
                CycleResult::AppliedSuccess { .. } => {
                    if imported_set.contains(d.address.as_str()) {
                        // Import pre-pass adopted this resource — the
                        // user-facing outcome is "imported", not
                        // whatever action the original plan had.
                        (Outcome::Imported, Some("adopted via tofu import".to_string()))
                    } else {
                        (outcome_for_action(&d.action), None)
                    }
                }
            };
            match outcome {
                Outcome::Matched => summary.matched = summary.matched.saturating_add(1),
                Outcome::Updated => summary.updated = summary.updated.saturating_add(1),
                Outcome::Created => summary.created = summary.created.saturating_add(1),
                Outcome::Destroyed => summary.destroyed = summary.destroyed.saturating_add(1),
                Outcome::Imported => summary.imported = summary.imported.saturating_add(1),
                Outcome::Drifted => {
                    summary.drifted_uncorrected = summary.drifted_uncorrected.saturating_add(1)
                }
                Outcome::Failed => summary.failed = summary.failed.saturating_add(1),
            }
            ResourceOutcome {
                address: d.address.clone(),
                outcome,
                action: Some(d.action.clone()),
                message,
            }
        })
        .collect();

    // matched aggregate = (total - touched). For NoChanges cycles
    // drifts is empty so this equals `total`.
    let touched_count = drifts.len() as u32;
    let untouched = total.saturating_sub(touched_count);
    summary.matched = summary.matched.saturating_add(untouched);

    ReconcileCycle {
        cycle: next_cycle,
        started_at,
        completed_at: Utc::now(),
        source_revision,
        plan_summary,
        summary,
        outcomes,
    }
}

/// Map the terraform action vocabulary to the typed `Outcome` the
/// operator surfaces. The mapping is deliberately conservative:
/// replaces collapse to `Updated` (net effect = matches declared);
/// unknown actions land on `Updated` so we never silently lose a
/// signal.
pub fn outcome_for_action(action: &str) -> Outcome {
    match action {
        "no-op" | "noop" => Outcome::Matched,
        "create" => Outcome::Created,
        "update" => Outcome::Updated,
        "delete" => Outcome::Destroyed,
        "replace" => Outcome::Updated,
        "import" => Outcome::Imported,
        _ => Outcome::Updated,
    }
}

/// Trim a string to 256 characters with an ellipsis suffix when
/// truncated. Used to keep status fields under the etcd value-size
/// budget when stuffing terraform error output into a status patch.
pub fn truncate_for_status(s: &str) -> String {
    const MAX: usize = 256;
    if s.len() <= MAX {
        s.to_string()
    } else {
        let mut t = s[..MAX].to_string();
        t.push('…');
        t
    }
}

/// Patch `status.lastCycle` + bump `status.cycleCount`. Skips the
/// patch entirely if the receipt is content-equal to the prior one
/// (only the timestamps differ) — keeps reconcile-loop chatter off
/// etcd for steady-state Ready→Ready flows.
pub async fn record_reconcile_cycle(
    template: &InfrastructureTemplate,
    state: &ControllerState,
    drifts: &[DriftDetail],
    plan_summary: Option<String>,
    result: CycleResult,
) -> Result<()> {
    let name = template.name_any();

    let prior_status = template.status.clone().unwrap_or_default();
    let prior_cycle_count = prior_status.cycle_count;
    let next_cycle = prior_cycle_count.saturating_add(1);

    let total = prior_status
        .resources
        .as_ref()
        .map(|r| r.total)
        .unwrap_or(0);
    let started_at = prior_status
        .last_planned_at
        .unwrap_or_else(Utc::now);
    let source_revision = prior_status.last_applied_revision.clone();

    let new_cycle = build_reconcile_cycle(
        next_cycle,
        started_at,
        drifts,
        total,
        plan_summary,
        source_revision,
        result,
    );

    if let Some(prev) = prior_status.last_cycle.as_ref() {
        if cycle_content_equal(prev, &new_cycle) {
            debug!(
                template = %name,
                cycle = prior_cycle_count,
                "Reconcile cycle content unchanged; skipping status patch"
            );
            return Ok(());
        }
    }

    let mut new_status = prior_status;
    new_status.cycle_count = next_cycle;
    new_status.last_cycle = Some(new_cycle.clone());

    let patch = serde_json::json!({
        "status": {
            "cycleCount": new_status.cycle_count,
            "lastCycle": new_status.last_cycle,
        }
    });
    crate::controller::status_patch::patch_status(template, &state.client, patch)
    .await?;

    info!(
        template = %name,
        cycle = next_cycle,
        matched = new_cycle.summary.matched,
        updated = new_cycle.summary.updated,
        created = new_cycle.summary.created,
        destroyed = new_cycle.summary.destroyed,
        drifted_uncorrected = new_cycle.summary.drifted_uncorrected,
        failed = new_cycle.summary.failed,
        "ReconcileCycle recorded"
    );
    Ok(())
}

/// Two cycles are content-equal when summary, source_revision,
/// plan_summary, and outcomes match. Cycle number and timestamps are
/// deliberately ignored — they always differ between successive
/// reconciles, and skipping the patch when nothing else changed is
/// the whole point (no etcd churn for Matched-only steady state).
pub fn cycle_content_equal(a: &ReconcileCycle, b: &ReconcileCycle) -> bool {
    if a.summary.matched != b.summary.matched
        || a.summary.updated != b.summary.updated
        || a.summary.created != b.summary.created
        || a.summary.destroyed != b.summary.destroyed
        || a.summary.imported != b.summary.imported
        || a.summary.drifted_uncorrected != b.summary.drifted_uncorrected
        || a.summary.failed != b.summary.failed
    {
        return false;
    }
    if a.source_revision != b.source_revision || a.plan_summary != b.plan_summary {
        return false;
    }
    if a.outcomes.len() != b.outcomes.len() {
        return false;
    }
    for (ao, bo) in a.outcomes.iter().zip(b.outcomes.iter()) {
        if ao.address != bo.address
            || ao.outcome != bo.outcome
            || ao.action != bo.action
            || ao.message != bo.message
        {
            return false;
        }
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn outcome_for_action_terraform_vocab_round_trip() {
        // The five terraform actions plus the no-op variants. Drift
        // here would silently misclassify cycle outcomes — e.g. a
        // create shown as an update — and corrupt the audit trail.
        assert_eq!(outcome_for_action("create"), Outcome::Created);
        assert_eq!(outcome_for_action("update"), Outcome::Updated);
        assert_eq!(outcome_for_action("delete"), Outcome::Destroyed);
        assert_eq!(outcome_for_action("import"), Outcome::Imported);
        assert_eq!(outcome_for_action("no-op"), Outcome::Matched);
        assert_eq!(outcome_for_action("noop"), Outcome::Matched);
    }

    #[test]
    fn outcome_for_action_replace_collapses_to_update() {
        // A replace is destroy+create with the net effect "matches
        // declared state after the cycle" — same user-facing meaning
        // as Updated. The collapse is deliberate; drift here would
        // double-count the destroy in the cycle summary.
        assert_eq!(outcome_for_action("replace"), Outcome::Updated);
    }

    #[test]
    fn outcome_for_action_unknown_falls_back_to_updated() {
        // Unknown action → Updated rather than panicking. Future
        // terraform versions can introduce new verbs without crashing
        // the operator, but the mapping should never silently drop
        // the resource from the summary.
        assert_eq!(outcome_for_action("teleport"), Outcome::Updated);
        assert_eq!(outcome_for_action(""), Outcome::Updated);
    }

    #[test]
    fn truncate_for_status_passes_short_strings_through() {
        assert_eq!(truncate_for_status("short"), "short");
        let exactly_max = "a".repeat(256);
        assert_eq!(truncate_for_status(&exactly_max), exactly_max);
    }

    #[test]
    fn truncate_for_status_caps_at_256_with_ellipsis() {
        let too_long = "a".repeat(300);
        let out = truncate_for_status(&too_long);
        // 256 chars + ellipsis. We assert the cap, not the byte
        // length (which depends on '…' encoding).
        assert!(out.starts_with(&"a".repeat(256)));
        assert!(out.ends_with('…'));
    }

    #[test]
    fn cycle_content_equal_ignores_cycle_number_and_timestamps() {
        // Same content, different cycle/started_at/completed_at →
        // content_equal. This is the whole point: steady-state
        // reconciles must skip the etcd patch.
        let summary = CycleSummary { matched: 7, ..Default::default() };
        let mk = |cycle, ts| ReconcileCycle {
            cycle,
            started_at: ts,
            completed_at: ts,
            source_revision: Some("abc".into()),
            plan_summary: Some("no changes".into()),
            summary: summary.clone(),
            outcomes: vec![],
        };
        let a = mk(1, Utc::now());
        let b = mk(2, Utc::now() + chrono::Duration::seconds(1));
        assert!(cycle_content_equal(&a, &b));
    }

    #[test]
    fn cycle_content_equal_distinguishes_different_summaries() {
        let mk = |matched| {
            let mut summary = CycleSummary::default();
            summary.matched = matched;
            ReconcileCycle {
                cycle: 1,
                started_at: Utc::now(),
                completed_at: Utc::now(),
                source_revision: None,
                plan_summary: None,
                summary,
                outcomes: vec![],
            }
        };
        assert!(!cycle_content_equal(&mk(7), &mk(8)));
    }
}
