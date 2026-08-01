//! State-settling tracker.
//!
//! State settling is the operator's primary success metric: after we
//! apply a plan, the next drift check should report no changes. Each
//! Ready→Drifted→(plan→apply)→Ready cycle is one "drift cycle". A
//! clean drift check resets the counter; the counter exceeding
//! `SettlingPolicy::max_consecutive_drift_cycles` is the loud signal
//! that the system can't reach steady state.
//!
//! Two complementary signals — controllers should treat EITHER as a
//! stuck condition:
//!
//!   * **Cycle count** (`consecutive_drift_cycles >= threshold`):
//!     simple, count-based escalation. Catches "every 30s the same
//!     resource keeps re-drifting".
//!
//!   * **Drift fingerprint** (`current_fingerprint == previous`):
//!     content-based. Catches "we're cycling but the diff is
//!     identical every time" — the strongest "we are not making
//!     progress" signal, fires even before the count threshold.
//!
//! The `evaluate` function returns a structured outcome the controller
//! can branch on (escalate / continue / reset).

use crate::crd::{DriftDetail, SettlingExhaustionAction, SettlingPolicy};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

/// Outcome of one settling evaluation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SettlingOutcome {
    /// No drift on this check — counter should reset to 0.
    Settled,
    /// Drift this cycle, but we're under the escalation threshold and
    /// the fingerprint differs from last cycle (we ARE making progress).
    Progressing { cycles: u32 },
    /// Drift this cycle AND the diff is identical to last cycle —
    /// content-based "stuck" signal. Fires regardless of cycle count.
    StuckByFingerprint {
        cycles: u32,
        fingerprint: String,
        stuck_addresses: Vec<String>,
    },
    /// Drift this cycle AND we've exceeded the count threshold.
    StuckByCount {
        cycles: u32,
        stuck_addresses: Vec<String>,
    },
}

impl SettlingOutcome {
    /// Has this outcome reached a "stuck" terminal state?
    pub fn is_stuck(&self) -> bool {
        matches!(
            self,
            SettlingOutcome::StuckByFingerprint { .. } | SettlingOutcome::StuckByCount { .. }
        )
    }

    /// New cycle count to persist in status.
    pub fn cycle_count(&self) -> u32 {
        match self {
            SettlingOutcome::Settled => 0,
            SettlingOutcome::Progressing { cycles }
            | SettlingOutcome::StuckByFingerprint { cycles, .. }
            | SettlingOutcome::StuckByCount { cycles, .. } => *cycles,
        }
    }
}

/// Decide what the controller should do based on a settling outcome
/// and the configured `SettlingPolicy`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SettlingAction {
    /// All good, transition to Ready and reset counter.
    AcceptSettled,
    /// Drift exists, keep trying. (Continue normal Drifted→Plan loop.)
    KeepTrying,
    /// Loud Warning event + Settled=False condition, but keep trying.
    /// Used for `Alert` exhaustion mode.
    AlertButContinue,
    /// Transition to phase=Failed with a stuck-resource error message.
    /// Used for `Fail` exhaustion mode (default).
    EscalateToFailed,
}

/// Compute a stable fingerprint for a set of drift entries.
///
/// Used as the "is this the same drift as last cycle?" signal.
/// Hashes (address, action, attribute names) for each entry, sorted
/// by address so order doesn't matter. Risk level is intentionally
/// excluded — it's a derived heuristic, not part of the actual diff.
pub fn fingerprint(drifts: &[DriftDetail]) -> String {
    let mut entries: Vec<(String, String, Vec<String>)> = drifts
        .iter()
        .map(|d| {
            let mut attrs = d.attributes.clone();
            attrs.sort();
            (d.address.clone(), d.action.clone(), attrs)
        })
        .collect();
    entries.sort_by(|a, b| a.0.cmp(&b.0));

    let mut hasher = DefaultHasher::new();
    for (addr, action, attrs) in &entries {
        addr.hash(&mut hasher);
        action.hash(&mut hasher);
        for a in attrs {
            a.hash(&mut hasher);
        }
    }
    format!("{:016x}", hasher.finish())
}

/// Evaluate settling state.
///
/// `policy` is the spec policy (or default), `prior_cycles` is
/// `status.consecutive_drift_cycles`, `prior_fingerprint` is the most
/// recent drift fingerprint we recorded (if any), and `current_drifts`
/// is the drift list from this cycle's drift check (empty list means
/// no drift detected).
pub fn evaluate(
    policy: &SettlingPolicy,
    prior_cycles: u32,
    prior_fingerprint: Option<&str>,
    current_drifts: &[DriftDetail],
) -> SettlingOutcome {
    if current_drifts.is_empty() {
        return SettlingOutcome::Settled;
    }

    let cycles = prior_cycles.saturating_add(1);
    let current_fp = fingerprint(current_drifts);
    let stuck_addresses: Vec<String> = current_drifts
        .iter()
        .take(20)
        .map(|d| d.address.clone())
        .collect();

    if let Some(prev) = prior_fingerprint {
        if prev == current_fp {
            return SettlingOutcome::StuckByFingerprint {
                cycles,
                fingerprint: current_fp,
                stuck_addresses,
            };
        }
    }

    if cycles >= policy.max_consecutive_drift_cycles {
        return SettlingOutcome::StuckByCount {
            cycles,
            stuck_addresses,
        };
    }

    SettlingOutcome::Progressing { cycles }
}

/// Map an outcome + policy to a concrete controller action.
pub fn action_for(outcome: &SettlingOutcome, policy: &SettlingPolicy) -> SettlingAction {
    match outcome {
        SettlingOutcome::Settled => SettlingAction::AcceptSettled,
        SettlingOutcome::Progressing { .. } => SettlingAction::KeepTrying,
        SettlingOutcome::StuckByFingerprint { .. } | SettlingOutcome::StuckByCount { .. } => {
            match policy.on_exhaustion {
                SettlingExhaustionAction::Fail => SettlingAction::EscalateToFailed,
                SettlingExhaustionAction::Alert => SettlingAction::AlertButContinue,
                SettlingExhaustionAction::Continue => SettlingAction::KeepTrying,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn d(addr: &str, action: &str, attrs: &[&str]) -> DriftDetail {
        DriftDetail {
            address: addr.into(),
            action: action.into(),
            risk: "low".into(),
            attributes: attrs.iter().map(|s| s.to_string()).collect(),
            policy_decision: None,
            matched_policy: None,
        }
    }

    fn pol(max: u32, on: SettlingExhaustionAction) -> SettlingPolicy {
        SettlingPolicy {
            max_consecutive_drift_cycles: max,
            on_exhaustion: on,
        }
    }

    #[test]
    fn fingerprint_is_stable_across_order() {
        let a = vec![
            d("aws_vpc.x", "update", &["a", "b"]),
            d("aws_subnet.y", "create", &[]),
        ];
        let b = vec![
            d("aws_subnet.y", "create", &[]),
            d("aws_vpc.x", "update", &["b", "a"]),
        ];
        assert_eq!(fingerprint(&a), fingerprint(&b));
    }

    #[test]
    fn fingerprint_differs_on_attribute_change() {
        let a = vec![d("aws_vpc.x", "update", &["a"])];
        let b = vec![d("aws_vpc.x", "update", &["a", "b"])];
        assert_ne!(fingerprint(&a), fingerprint(&b));
    }

    #[test]
    fn empty_drifts_means_settled() {
        let p = pol(5, SettlingExhaustionAction::Fail);
        let outcome = evaluate(&p, 3, Some("abc"), &[]);
        assert_eq!(outcome, SettlingOutcome::Settled);
        assert_eq!(outcome.cycle_count(), 0);
    }

    #[test]
    fn progressing_increments_cycles() {
        let p = pol(5, SettlingExhaustionAction::Fail);
        let drifts = vec![d("a.b", "create", &[])];
        let outcome = evaluate(&p, 2, None, &drifts);
        assert_eq!(outcome, SettlingOutcome::Progressing { cycles: 3 });
    }

    #[test]
    fn identical_fingerprint_triggers_stuck_before_threshold() {
        let p = pol(5, SettlingExhaustionAction::Fail);
        let drifts = vec![d("a.b", "create", &[])];
        let fp = fingerprint(&drifts);
        let outcome = evaluate(&p, 1, Some(&fp), &drifts);
        match outcome {
            SettlingOutcome::StuckByFingerprint { cycles, .. } => assert_eq!(cycles, 2),
            _ => panic!("expected StuckByFingerprint, got {:?}", outcome),
        }
    }

    #[test]
    fn count_threshold_triggers_stuck_when_fingerprint_changes() {
        let p = pol(3, SettlingExhaustionAction::Fail);
        let drifts1 = vec![d("a.b", "create", &[])];
        let drifts2 = vec![d("a.c", "update", &["x"])];
        let fp1 = fingerprint(&drifts1);
        let outcome = evaluate(&p, 2, Some(&fp1), &drifts2);
        match outcome {
            SettlingOutcome::StuckByCount { cycles, .. } => assert_eq!(cycles, 3),
            _ => panic!("expected StuckByCount, got {:?}", outcome),
        }
    }

    #[test]
    fn action_dispatch_respects_exhaustion_mode() {
        let drifts = vec![d("a.b", "create", &[])];

        let stuck = SettlingOutcome::StuckByFingerprint {
            cycles: 2,
            fingerprint: fingerprint(&drifts),
            stuck_addresses: vec!["a.b".into()],
        };
        assert_eq!(
            action_for(&stuck, &pol(5, SettlingExhaustionAction::Fail)),
            SettlingAction::EscalateToFailed
        );
        assert_eq!(
            action_for(&stuck, &pol(5, SettlingExhaustionAction::Alert)),
            SettlingAction::AlertButContinue
        );
        assert_eq!(
            action_for(&stuck, &pol(5, SettlingExhaustionAction::Continue)),
            SettlingAction::KeepTrying
        );
        assert_eq!(
            action_for(
                &SettlingOutcome::Settled,
                &pol(5, SettlingExhaustionAction::Fail)
            ),
            SettlingAction::AcceptSettled
        );
    }
}
