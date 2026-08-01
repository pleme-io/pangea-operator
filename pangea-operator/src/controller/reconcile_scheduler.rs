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
    fn sched_id(&self) -> &str {
        &self.unit
    }
    fn sched_scope(&self) -> &str {
        &self.workspace
    }
    fn priority_class(&self) -> PriorityClass {
        self.class
    }
    fn staleness_secs(&self) -> u64 {
        self.staleness_secs
    }
    fn behind_target(&self) -> bool {
        self.behind_head
    }
    fn drift_magnitude(&self) -> u32 {
        self.drift_magnitude
    }
    fn fairness_deficit(&self) -> u64 {
        self.fairness_deficit
    }
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

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use super::scheduling::{pick_admitted, BudgetPermit, FairBudget};

/// The central reconcile dispatch queue (S3) — the stateful structure that makes
/// most-valuable-first ordering GOVERN live dispatch, not just a per-template
/// decision. Templates that need expensive work [`register`](ReconcileQueue::register)
/// a [`ReconcileDemand`]; the coordinator [`tick`](ReconcileQueue::tick)s, and
/// each tick:
///
/// 1. ranks every pending demand (most-valuable-first) and admits the top wave
///    within the [`FairBudget`] ([`pick_admitted`]) — bounded + per-scope-fair;
/// 2. **accrues fairness-deficit on every eligible demand that was passed over** —
///    this is what the single-tick [`pick_admitted`] cannot do alone, and is what
///    makes anti-starvation hold OVER TIME: a unit that keeps losing gets a rising
///    deficit until its urgency wins, so no unit waits forever.
///
/// The returned units carry RAII [`BudgetPermit`]s; the caller dispatches each and
/// drops the permit on completion (freeing the slot for the next tick). Holding
/// 100k demands costs one `HashMap`; a tick is O(n log n) rank + O(admitted)
/// acquire, and live in-flight never exceeds the budget. This is the engine S5
/// shards across replicas (one queue per workspace-shard).
pub struct ReconcileQueue {
    demands: Mutex<HashMap<String, ReconcileDemand>>,
    budget: Arc<FairBudget>,
    weights: UrgencyWeights,
}

impl ReconcileQueue {
    #[must_use]
    pub fn new(budget: Arc<FairBudget>, weights: UrgencyWeights) -> Self {
        Self {
            demands: Mutex::new(HashMap::new()),
            budget,
            weights,
        }
    }

    /// A template reports it needs expensive work (or refreshes its metadata).
    /// Re-registering keeps the existing fairness-deficit so a unit doesn't lose
    /// its accrued priority by re-announcing.
    pub fn register(&self, mut demand: ReconcileDemand) {
        let mut d = self.demands.lock().expect("reconcile-queue mutex poisoned");
        if let Some(prev) = d.get(&demand.unit) {
            demand.fairness_deficit = demand.fairness_deficit.max(prev.fairness_deficit);
        }
        d.insert(demand.unit.clone(), demand);
    }

    /// A unit finished its expensive work — remove it from the queue (its permit
    /// is dropped by the caller, freeing the slot).
    pub fn complete(&self, unit: &str) {
        self.demands
            .lock()
            .expect("reconcile-queue mutex poisoned")
            .remove(unit);
    }

    #[must_use]
    pub fn pending_len(&self) -> usize {
        self.demands
            .lock()
            .expect("reconcile-queue mutex poisoned")
            .len()
    }

    #[must_use]
    pub fn fairness_deficit_of(&self, unit: &str) -> u64 {
        self.demands
            .lock()
            .expect("reconcile-queue mutex poisoned")
            .get(unit)
            .map_or(0, |d| d.fairness_deficit)
    }

    /// Run one dispatch tick: select + admit the next most-valuable wave, and
    /// raise the fairness-deficit of every eligible demand that lost this round.
    /// Returns the admitted `(unit, permit)` pairs.
    pub fn tick(&self, now_secs: u64) -> Vec<(String, BudgetPermit)> {
        let mut d = self.demands.lock().expect("reconcile-queue mutex poisoned");
        let snapshot: Vec<ReconcileDemand> = d.values().cloned().collect();
        let admitted = pick_admitted(&snapshot, now_secs, &self.weights, &self.budget);
        let won: std::collections::HashSet<&str> =
            admitted.iter().map(|(u, _)| u.sched_id()).collect();
        // Anti-starvation OVER TIME: every eligible demand that was NOT admitted
        // this tick accrues deficit, so its urgency climbs until it wins.
        for demand in d.values_mut() {
            if demand.eligible(now_secs) && !won.contains(demand.unit.as_str()) {
                demand.fairness_deficit = demand.fairness_deficit.saturating_add(1);
            }
        }
        admitted
            .into_iter()
            .map(|(u, p)| (u.unit.clone(), p))
            .collect()
    }
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
        assert_eq!(
            schedule_next(&pending, 0, &UrgencyWeights::default(), 1)[0].unit,
            "starved"
        );
    }

    // ── ReconcileQueue: anti-starvation OVER TIME (the S3 contribution) ─────
    fn queue(per_scope: usize, global: usize) -> ReconcileQueue {
        ReconcileQueue::new(
            FairBudget::new(super::super::scheduling::BudgetConfig { per_scope, global }),
            UrgencyWeights::default(),
        )
    }

    /// THE over-time guarantee one-tick pick_admitted can't show: a permanently
    /// "loud" unit and a "quiet" unit share a scope with room for ONE. The loud
    /// one wins early ticks — but the quiet one's fairness-deficit climbs each
    /// time it's passed over, until its urgency overtakes and it wins. No unit
    /// waits forever.
    #[test]
    fn the_queue_never_starves_a_passed_over_unit() {
        let q = queue(1, 10); // one slot in the shared scope
        let mut loud = d("loud", "ws", PriorityClass::Normal);
        loud.drift_magnitude = 50; // always looks urgent
        let quiet = d("quiet", "ws", PriorityClass::Normal); // nothing urgent but presence
        q.register(loud);
        q.register(quiet);

        let mut quiet_ever_won = false;
        for t in 0..200u64 {
            let admitted = q.tick(t);
            if admitted.iter().any(|(u, _)| u == "quiet") {
                quiet_ever_won = true;
                break;
            }
            // permits drop here (end of loop body) → slot frees for next tick;
            // re-register the loud unit's freshness (it keeps demanding work).
            let mut loud_again = d("loud", "ws", PriorityClass::Normal);
            loud_again.drift_magnitude = 50;
            q.register(loud_again);
        }
        assert!(
            quiet_ever_won,
            "quiet's rising fairness-deficit must eventually win it a turn"
        );
        assert!(
            q.fairness_deficit_of("quiet") > 0 || q.pending_len() < 2,
            "quiet accrued deficit while waiting"
        );
    }

    /// A tick admits at most the budget, returns permits, and `complete` drains.
    #[test]
    fn queue_tick_is_bounded_and_completable() {
        let q = queue(2, 2);
        for i in 0..10 {
            q.register(d(
                &format!("u{i}"),
                &format!("ws{i}"),
                PriorityClass::Normal,
            ));
        }
        let admitted = q.tick(0);
        assert_eq!(admitted.len(), 2, "global cap bounds the wave");
        for (u, _p) in &admitted {
            q.complete(u);
        }
        drop(admitted); // permits released
        assert_eq!(q.pending_len(), 8, "completed units leave the queue");
    }
}
