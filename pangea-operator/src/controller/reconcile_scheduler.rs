//! Operator adapter onto the generic [`super::scheduling`] primitive.
//!
//! The fair-priority scheduling *algorithm* (class tiers, urgency,
//! fairness-deficit anti-starvation, dependency + backoff gating, bounded
//! dispatch) lives once in [`super::scheduling`], generic over the
//! [`Schedulable`] trait. This module is the operator's concrete unit —
//! [`ReconcileDemand`] — and its `Schedulable` impl. No algorithm is duplicated
//! here; adding a second consumer elsewhere in the fleet is "implement
//! `Schedulable`", nothing more.
//!
//! Admission control (the no-crash bound) is [`super::workspace_budget`]. The
//! two compose: [`schedule_next`] ranks the pending demands; the budget admits
//! the top `slots`.

pub use super::scheduling::{schedule, PriorityClass, Schedulable, UrgencyWeights};

/// A pending reconcile unit's typed scheduling demand (the operator's
/// [`Schedulable`] unit). One per InfrastructureTemplate awaiting expensive
/// work; the workspace is its fairness + budget scope.
#[derive(Debug, Clone)]
pub struct ReconcileDemand {
    /// Stable unique id (namespace/name) — deterministic tiebreak.
    pub unit: String,
    /// The workspace this unit belongs to (the budget + fairness scope).
    pub workspace: String,
    pub class: PriorityClass,
    /// Seconds since the last successful reconcile (most-stale-first).
    pub staleness_secs: u64,
    /// Source HEAD has moved past the compiled revision.
    pub behind_head: bool,
    /// Number of drifted resources (close the biggest gap first).
    pub drift_magnitude: u32,
    /// Anti-starvation term: rounds this unit's workspace has been passed over.
    pub fairness_deficit: u64,
    /// Not eligible until `now_secs >= backoff_until_secs` (exponential backoff
    /// for failing/wedged units). 0 = eligible now.
    pub backoff_until_secs: u64,
    /// All upstream dependencies (template-DAG / gem-readiness) are satisfied.
    pub deps_ready: bool,
}

impl Schedulable for ReconcileDemand {
    fn sched_id(&self) -> &str { &self.unit }
    fn sched_scope(&self) -> &str { &self.workspace }
    fn priority_class(&self) -> PriorityClass { self.class }
    fn staleness_secs(&self) -> u64 { self.staleness_secs }
    fn behind_target(&self) -> bool { self.behind_head }
    fn drift_magnitude(&self) -> u32 { self.drift_magnitude }
    fn fairness_deficit(&self) -> u64 { self.fairness_deficit }
    fn eligible(&self, now_secs: u64) -> bool {
        self.deps_ready && now_secs >= self.backoff_until_secs
    }
}

/// Rank the pending demands and return the next `slots` to dispatch. Thin
/// operator-named wrapper over [`super::scheduling::schedule`].
pub fn schedule_next<'a>(
    pending: &'a [ReconcileDemand],
    now_secs: u64,
    weights: &UrgencyWeights,
    slots: usize,
) -> Vec<&'a ReconcileDemand> {
    schedule(pending, now_secs, weights, slots)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn d(unit: &str, ws: &str, class: PriorityClass) -> ReconcileDemand {
        ReconcileDemand {
            unit: unit.into(),
            workspace: ws.into(),
            class,
            staleness_secs: 0,
            behind_head: false,
            drift_magnitude: 0,
            fairness_deficit: 0,
            backoff_until_secs: 0,
            deps_ready: true,
        }
    }

    /// The adapter flows correctly through the generic scheduler: class
    /// dominance + backoff gating hold for the operator's real unit type.
    #[test]
    fn reconcile_demand_schedules_via_the_generic_policy() {
        let crit = d("crit", "ws", PriorityClass::Critical);
        let mut loud = d("loud", "ws", PriorityClass::Normal);
        loud.staleness_secs = 999_999;
        loud.drift_magnitude = 999;
        let mut wedged = d("wedged", "ws", PriorityClass::Critical);
        wedged.backoff_until_secs = 1000;
        let pending = vec![loud, crit, wedged];
        let order: Vec<&str> = schedule_next(&pending, 500, &UrgencyWeights::default(), 10)
            .iter()
            .map(|x| x.unit.as_str())
            .collect();
        // wedged (in backoff at now=500) excluded; Critical `crit` preempts loud Normal.
        assert_eq!(order, vec!["crit", "loud"]);
    }

    /// A workspace passed over accrues deficit and is no longer starved.
    #[test]
    fn workspace_fairness_via_deficit() {
        let mut busy = d("busy", "ws-hot", PriorityClass::Normal);
        busy.drift_magnitude = 10;
        let mut starved = d("starved", "ws-cold", PriorityClass::Normal);
        starved.fairness_deficit = 100;
        let pending = vec![busy, starved];
        assert_eq!(schedule_next(&pending, 0, &UrgencyWeights::default(), 1)[0].unit, "starved");
    }
}
