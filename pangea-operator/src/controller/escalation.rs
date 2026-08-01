//! Escalation ladder — time-graded corrective actions for a template
//! that can't reach Ready.
//!
//! ## Why this exists
//!
//! The reconciliation motor is meant to seek state EFFECTIVELY. When
//! a template sits not-Ready for long enough, the controller needs
//! progressively deeper actions until it recovers OR refuses to
//! advance (pausing for human intervention). This module makes that
//! ladder a typed primitive: a pure function from
//! `(time_unready, history)` → recommended action. Each action is
//! idempotent and easily testable.
//!
//! Two classes of problem this addresses:
//!
//!   * **Known knowns** — a typed `Conflict` from the detection axis
//!     (`project_controller_detection_axis.md`) already named the bug
//!     class. The ladder picks an action proportional to how long
//!     the named condition has persisted.
//!   * **Unknown unknowns** — the controller saw an error it can't
//!     classify. The ladder still applies because the gate is TIME +
//!     RECURRENCE, not error shape. The deepest rung (`PauseAndAlert`)
//!     forces human attention rather than burning cycles on unknown
//!     conditions forever.
//!
//! ## The ladder
//!
//! | Rung | Default after | Action |
//! |---|---|---|
//! | 0 | 0s | `Retry` — normal reconcile; no extra action. |
//! | 1 | 5 min | `RefreshSource` — drop cached `_repo`, re-pull source. |
//! | 2 | 15 min | `ReloadGems` — purge `$LOADED_FEATURES` + re-broadcast all gem libs. |
//! | 3 | 30 min | `RecycleWorkers` — kill + respawn Ruby pool workers. |
//! | 4 | 60 min | `PauseAndAlert` — set `autoSuspended: true`, emit Event, stop wasting cycles. |
//!
//! Each rung's threshold is configurable per-template via
//! `spec.recoveryPolicy.rungs[]` (slice 4 schema), but the default
//! ladder is the production safe choice.
//!
//! ## Where this composes
//!
//! The ladder is the **fix axis** sibling to the detection axis. The
//! detection axis names anomalies; the escalation ladder takes action
//! against them. Both surface through the same `Conflict` shape so
//! consumers (status, events, metrics) don't branch on source.
//!
//! Compose with:
//!   * `settling.rs` — provides the stuck signals (`StuckByCount`,
//!     `StuckByFingerprint`) that bump the ladder's clock.
//!   * `error_policy.rs` — wraps reconciler errors; the ladder runs
//!     on top of error policy to choose corrective action.
//!   * detection axis (`pangea-ruby-eval::Conflict`) — typed anomalies
//!     flow into status; the ladder reads `status.lastReadyAt` to
//!     pick the rung.

use std::time::Duration;

/// One corrective action the controller can take. Each is
/// idempotent: running it twice is no harm.
///
/// Variants ordered shallowest → deepest. `Retry` is the no-op
/// default; `PauseAndAlert` is the terminal "stop burning cycles"
/// action.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum EscalationAction {
    /// Normal reconcile path — no extra action. The default for
    /// rung 0.
    Retry,
    /// Drop the workspace's cached `_repo` clone and re-pull from
    /// `spec.source.gitRepository`. Handles "the source moved on,
    /// our clone is stale".
    RefreshSource,
    /// Purge `$LOADED_FEATURES` entries under the gem-cache prefix
    /// and re-broadcast every gem lib to every pool worker. Handles
    /// "the in-process Ruby state is wedged; the file system is
    /// fine".
    ReloadGems,
    /// Kill + respawn every Ruby pool worker. Handles "the CRuby
    /// VM is in an irrecoverable state — load_path is corrupt,
    /// module constants are tangled, GC pressure is unsalvageable".
    /// Disruptive: in-flight compiles on this template are dropped.
    RecycleWorkers,
    /// Set `status.autoSuspended = true`, emit a high-severity Event
    /// (`reason: "EscalationPause"`), stop attempting reconcile until
    /// a human clears the suspend. Handles "we've tried everything
    /// we know; this needs human attention".
    PauseAndAlert,
}

impl EscalationAction {
    /// Stable label for tracing + metrics + event reasons. Pick once,
    /// never change.
    pub fn label(&self) -> &'static str {
        match self {
            Self::Retry => "retry",
            Self::RefreshSource => "refresh_source",
            Self::ReloadGems => "reload_gems",
            Self::RecycleWorkers => "recycle_workers",
            Self::PauseAndAlert => "pause_and_alert",
        }
    }

    /// Numeric depth — 0 = shallowest (`Retry`), 4 = deepest
    /// (`PauseAndAlert`). Useful for comparison + dashboard ordering.
    pub fn depth(&self) -> u8 {
        match self {
            Self::Retry => 0,
            Self::RefreshSource => 1,
            Self::ReloadGems => 2,
            Self::RecycleWorkers => 3,
            Self::PauseAndAlert => 4,
        }
    }
}

/// One step of the ladder: a threshold + the action to take when
/// time-unready exceeds the threshold.
///
/// Rungs are intentionally simple (threshold + action). The ladder
/// composes them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EscalationRung {
    /// Minimum time-since-last-Ready that gates this rung.
    pub min_duration_unready: Duration,
    /// The action to take when this rung is selected.
    pub action: EscalationAction,
}

/// Ordered list of rungs. `pick()` returns the deepest rung whose
/// threshold is satisfied by the input duration.
///
/// The rungs MUST be in ascending order of `min_duration_unready` for
/// `pick()` to behave correctly; `from_rungs` sorts defensively.
#[derive(Debug, Clone)]
pub struct EscalationLadder {
    rungs: Vec<EscalationRung>,
}

impl EscalationLadder {
    /// Production default for pangea-operator. 5-rung ladder; tuned
    /// against the bug classes we've seen 2026-Q2:
    ///
    /// * Compile-isolation churn (`Pangea::Resources::Cloudflare`
    ///   uninitialized) — RefreshSource at 5min if it's a stale clone;
    ///   ReloadGems at 15min if it's wedged CRuby state.
    /// * Pool worker accumulation — RecycleWorkers at 30min handles
    ///   the case where one VM has accumulated bad state.
    /// * Unknown-unknowns — PauseAndAlert at 60min stops the cycle
    ///   waste and forces inspection.
    pub fn pangea_default() -> Self {
        Self::from_rungs(vec![
            EscalationRung {
                min_duration_unready: Duration::from_secs(0),
                action: EscalationAction::Retry,
            },
            EscalationRung {
                min_duration_unready: Duration::from_secs(300),
                action: EscalationAction::RefreshSource,
            },
            EscalationRung {
                min_duration_unready: Duration::from_secs(900),
                action: EscalationAction::ReloadGems,
            },
            EscalationRung {
                min_duration_unready: Duration::from_secs(1800),
                action: EscalationAction::RecycleWorkers,
            },
            EscalationRung {
                min_duration_unready: Duration::from_secs(3600),
                action: EscalationAction::PauseAndAlert,
            },
        ])
    }

    /// Build a ladder from a custom rung list. Sorts ascending by
    /// `min_duration_unready` so `pick()` can short-circuit.
    pub fn from_rungs(mut rungs: Vec<EscalationRung>) -> Self {
        rungs.sort_by_key(|r| r.min_duration_unready);
        Self { rungs }
    }

    /// Pick the deepest rung whose threshold is satisfied. Returns
    /// `Retry` if the input duration is below every threshold (or
    /// the ladder is empty).
    pub fn pick(&self, duration_unready: Duration) -> EscalationAction {
        self.rungs
            .iter()
            .rev()
            .find(|r| duration_unready >= r.min_duration_unready)
            .map(|r| r.action)
            .unwrap_or(EscalationAction::Retry)
    }

    /// Inspect the configured rungs (for status surfacing /
    /// dashboards / debug).
    pub fn rungs(&self) -> &[EscalationRung] {
        &self.rungs
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_ladder_picks_retry_when_just_unready() {
        let l = EscalationLadder::pangea_default();
        assert_eq!(l.pick(Duration::from_secs(0)), EscalationAction::Retry);
        assert_eq!(l.pick(Duration::from_secs(60)), EscalationAction::Retry);
        assert_eq!(l.pick(Duration::from_secs(299)), EscalationAction::Retry);
    }

    #[test]
    fn default_ladder_escalates_at_each_threshold() {
        let l = EscalationLadder::pangea_default();
        assert_eq!(
            l.pick(Duration::from_secs(300)),
            EscalationAction::RefreshSource
        );
        assert_eq!(
            l.pick(Duration::from_secs(900)),
            EscalationAction::ReloadGems
        );
        assert_eq!(
            l.pick(Duration::from_secs(1800)),
            EscalationAction::RecycleWorkers
        );
        assert_eq!(
            l.pick(Duration::from_secs(3600)),
            EscalationAction::PauseAndAlert
        );
        assert_eq!(
            l.pick(Duration::from_secs(7200)),
            EscalationAction::PauseAndAlert
        );
    }

    #[test]
    fn default_ladder_picks_deepest_satisfied_rung_not_first() {
        // 31 minutes unready — past RecycleWorkers (30min) but not
        // PauseAndAlert (60min). Must NOT return RefreshSource (the
        // earliest qualifying rung) — must return RecycleWorkers
        // (the deepest qualifying).
        let l = EscalationLadder::pangea_default();
        assert_eq!(
            l.pick(Duration::from_secs(1860)),
            EscalationAction::RecycleWorkers,
        );
    }

    #[test]
    fn from_rungs_sorts_input_so_pick_works_with_unordered_config() {
        // Caller specifies thresholds out of order; ladder normalizes
        // so pick() can still rely on ascending iteration.
        let l = EscalationLadder::from_rungs(vec![
            EscalationRung {
                min_duration_unready: Duration::from_secs(1800),
                action: EscalationAction::RecycleWorkers,
            },
            EscalationRung {
                min_duration_unready: Duration::from_secs(0),
                action: EscalationAction::Retry,
            },
            EscalationRung {
                min_duration_unready: Duration::from_secs(900),
                action: EscalationAction::ReloadGems,
            },
            EscalationRung {
                min_duration_unready: Duration::from_secs(300),
                action: EscalationAction::RefreshSource,
            },
        ]);
        assert_eq!(
            l.pick(Duration::from_secs(950)),
            EscalationAction::ReloadGems
        );
        assert_eq!(l.pick(Duration::from_secs(0)), EscalationAction::Retry);
    }

    #[test]
    fn empty_ladder_falls_back_to_retry() {
        let l = EscalationLadder::from_rungs(vec![]);
        assert_eq!(l.pick(Duration::from_secs(86400)), EscalationAction::Retry);
    }

    #[test]
    fn action_label_is_stable_for_metric_use() {
        // Labels are emitted as Prometheus + tracing labels — they
        // MUST be stable. Locking the strings here catches accidental
        // renames that would break dashboards.
        assert_eq!(EscalationAction::Retry.label(), "retry");
        assert_eq!(EscalationAction::RefreshSource.label(), "refresh_source");
        assert_eq!(EscalationAction::ReloadGems.label(), "reload_gems");
        assert_eq!(EscalationAction::RecycleWorkers.label(), "recycle_workers");
        assert_eq!(EscalationAction::PauseAndAlert.label(), "pause_and_alert");
    }

    #[test]
    fn action_depth_orders_actions_for_comparison() {
        // depth() lets the caller assert "we escalated" without
        // pattern matching on every variant. Used in tests + future
        // dashboard ordering.
        assert!(EscalationAction::Retry.depth() < EscalationAction::RefreshSource.depth());
        assert!(EscalationAction::RefreshSource.depth() < EscalationAction::ReloadGems.depth());
        assert!(EscalationAction::ReloadGems.depth() < EscalationAction::RecycleWorkers.depth());
        assert!(EscalationAction::RecycleWorkers.depth() < EscalationAction::PauseAndAlert.depth());
    }
}
