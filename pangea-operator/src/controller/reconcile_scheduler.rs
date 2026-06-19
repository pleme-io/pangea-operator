//! Scalable reconcile scheduling policy (lifecycle S1+) — the *structured,
//! algorithmic* decision of **what to plan/apply next** at scale.
//!
//! # The problem at 100k
//!
//! With 100k reconcile units across many workspaces, the operator cannot work
//! on everything at once (it would exhaust RPC concurrency / memory / DB and
//! crash). Two layers solve this together:
//!
//! 1. **Admission control** ([`super::workspace_budget`]) — a hard bound on
//!    *in-flight* expensive work (global + per-workspace fair-share). This is
//!    the **no-crash** guarantee: only N units do expensive work at once, the
//!    rest wait. Backpressure, not collapse.
//! 2. **This module — the scheduling policy** — given far more eligible work
//!    than admission slots, *which* units get the slots, in what order, so the
//!    fleet **converges in the most valuable order without starving anyone**.
//!
//! # The ranking algorithm
//!
//! Every pending unit has a typed [`ReconcileDemand`]. Scheduling is a total
//! order over demands ([`ReconcileDemand::rank_key`]):
//!
//! 1. **Priority class** (hard tiers: Critical > High > Normal > Low) — a
//!    Critical unit always outranks any Normal one, regardless of everything
//!    else. Lets life-safety / SLA-bound infra preempt experiments.
//! 2. **Within a class, by `urgency`** — a weighted sum of:
//!    - **staleness** (time since last successful reconcile) — most-stale-first,
//!    - **behind-HEAD** (source moved past the compiled revision) — converge to
//!      the declared truth,
//!    - **drift magnitude** (how far from desired) — close the biggest gap per
//!      slot first (efficiency),
//!    - **fairness deficit** — how long this unit's workspace has been passed
//!      over. This is the anti-starvation term: a skipped unit's deficit grows
//!      each round, so its urgency rises until it *must* be scheduled. No unit
//!      can be starved forever.
//! 3. **Stable tiebreak** by unit id — so ranking is deterministic (no
//!    nondeterministic reordering of equal demands).
//!
//! And two **gates** decide *eligibility* before ranking even applies
//! ([`ReconcileDemand::eligible`]):
//! - **dependency readiness** — a unit whose upstreams aren't applied (the
//!   workspace template-DAG / gem-readiness) is not schedulable yet; it can't
//!   jump its dependencies.
//! - **backoff window** — a failing/wedged unit (CompileBlocked, repeated
//!   failures) is gated out until its exponential-backoff window elapses, so a
//!   handful of wedges can't burn the admission pool retrying — but it is never
//!   permanently excluded (after the window it re-enters ranking).
//!
//! Together: bounded admission (no crash) + priority/urgency/fairness ranking
//! (valuable + fair convergence) + dependency/backoff gating (no wasted work).
//! This is the shigoto-`BudgetTree`-plus-a-fair-priority-policy shape, authored
//! once here and intended as the fleet-wide pattern for any reconcile loop.
//!
//! # Tier honesty
//!
//! The proofs below are mechanical CI forcing-functions over the *policy*
//! (total order, class dominance, no-starvation-by-deficit, gating). They are
//! not a runtime guarantee of fleet liveness (that depends on admission
//! throughput vs arrival rate) — they guarantee the *decision function* is
//! total, deterministic, class-correct, and starvation-free by construction.

use std::cmp::Reverse;

/// Hard priority tiers. Lower discriminant = higher priority.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum PriorityClass {
    /// Life-safety / SLA-bound infra — preempts everything below.
    Critical = 0,
    High = 1,
    Normal = 2,
    /// Best-effort / experimental.
    Low = 3,
}

/// Tunable urgency weights. Defaults chosen so behind-HEAD and drift dominate
/// raw age, and the fairness deficit can eventually overcome any of them.
#[derive(Debug, Clone, Copy)]
pub struct UrgencyWeights {
    pub per_stale_sec: u64,
    pub behind_head: u64,
    pub per_drift: u64,
    pub per_deficit: u64,
}

impl Default for UrgencyWeights {
    fn default() -> Self {
        UrgencyWeights { per_stale_sec: 1, behind_head: 3600, per_drift: 120, per_deficit: 600 }
    }
}

/// A pending reconcile unit's typed scheduling demand.
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

impl ReconcileDemand {
    /// Eligible to be ranked at all: deps satisfied AND past its backoff window.
    /// A unit that fails this is simply not a candidate this tick — it can't
    /// jump its dependencies and a wedge can't burn slots inside its backoff.
    pub fn eligible(&self, now_secs: u64) -> bool {
        self.deps_ready && now_secs >= self.backoff_until_secs
    }

    /// Within-class urgency. Higher = scheduled sooner. The fairness deficit is
    /// additive and unbounded-growing, so a starved unit's urgency rises every
    /// round it's skipped until it outranks its peers — the no-starvation lever.
    pub fn urgency(&self, w: &UrgencyWeights) -> u64 {
        self.staleness_secs.saturating_mul(w.per_stale_sec)
            .saturating_add(if self.behind_head { w.behind_head } else { 0 })
            .saturating_add(u64::from(self.drift_magnitude).saturating_mul(w.per_drift))
            .saturating_add(self.fairness_deficit.saturating_mul(w.per_deficit))
    }

    /// Total scheduling order: class first (hard tier), then urgency
    /// (descending), then unit id (stable tiebreak). `BinaryHeap`/`sort` over
    /// this picks what to dispatch next.
    pub fn rank_key(&self, w: &UrgencyWeights) -> (PriorityClass, Reverse<u64>, &str) {
        (self.class, Reverse(self.urgency(w)), self.unit.as_str())
    }
}

/// Pick the next units to dispatch from `pending`, in scheduling order, up to
/// `slots` (the admission budget's free capacity). Pure + deterministic — the
/// caller pairs each returned unit with a budget permit. Ineligible units
/// (deps unmet / in backoff) are excluded entirely.
pub fn schedule_next<'a>(
    pending: &'a [ReconcileDemand],
    now_secs: u64,
    weights: &UrgencyWeights,
    slots: usize,
) -> Vec<&'a ReconcileDemand> {
    let mut eligible: Vec<&ReconcileDemand> = pending.iter().filter(|d| d.eligible(now_secs)).collect();
    // Sort by the total rank key; lower class first, then higher urgency, then id.
    eligible.sort_by(|a, b| a.rank_key(weights).cmp(&b.rank_key(weights)));
    eligible.into_iter().take(slots).collect()
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
    fn w() -> UrgencyWeights {
        UrgencyWeights::default()
    }

    /// The rank key is a total order (sorting is deterministic, ties broken by
    /// unit id) — same input always yields the same schedule.
    #[test]
    fn ranking_is_total_and_deterministic() {
        let mut a = d("z", "ws", PriorityClass::Normal);
        a.drift_magnitude = 5;
        let mut b = d("a", "ws", PriorityClass::Normal);
        b.drift_magnitude = 5; // identical urgency → tiebreak by id ("a" < "z")
        let pending = vec![a.clone(), b.clone()];
        let s1 = schedule_next(&pending, 0, &w(), 10);
        let s2 = schedule_next(&pending, 0, &w(), 10);
        assert_eq!(s1.iter().map(|x| &x.unit).collect::<Vec<_>>(),
                   s2.iter().map(|x| &x.unit).collect::<Vec<_>>(), "schedule must be deterministic");
        assert_eq!(s1[0].unit, "a", "equal urgency breaks to lower id");
    }

    /// Hard class dominance: a Critical unit with ZERO urgency still outranks a
    /// Normal unit screaming with drift + staleness.
    #[test]
    fn class_dominates_urgency() {
        let crit = d("crit", "ws", PriorityClass::Critical); // urgency 0
        let mut loud = d("loud", "ws", PriorityClass::Normal);
        loud.staleness_secs = 999_999;
        loud.drift_magnitude = 999;
        loud.behind_head = true;
        let pending = vec![loud, crit];
        let order = schedule_next(&pending, 0, &w(), 10);
        assert_eq!(order[0].unit, "crit", "Critical preempts a maximally-urgent Normal");
    }

    /// Anti-starvation: a unit repeatedly passed over accrues fairness deficit;
    /// its urgency rises until it outranks a steadily-busy rival. Proven by
    /// showing deficit growth flips the order.
    #[test]
    fn fairness_deficit_prevents_starvation() {
        let busy = {
            let mut x = d("busy", "ws-hot", PriorityClass::Normal);
            x.drift_magnitude = 10; // consistently high-urgency rival
            x
        };
        let starved_lo = d("starved", "ws-cold", PriorityClass::Normal); // deficit 0 → loses
        let pending0 = vec![busy.clone(), starved_lo.clone()];
        assert_eq!(schedule_next(&pending0, 0, &w(), 1)[0].unit, "busy", "with no deficit, busy wins");

        // after being skipped enough rounds, the starved unit's deficit dominates
        let mut starved_hi = starved_lo.clone();
        starved_hi.fairness_deficit = 100; // accrued from being passed over
        let pending1 = vec![busy, starved_hi];
        assert_eq!(schedule_next(&pending1, 0, &w(), 1)[0].unit, "starved",
                   "accrued deficit eventually overtakes a busy rival — no permanent starvation");
    }

    /// Gating: a unit in its backoff window or with unmet deps is NOT a
    /// candidate — a wedge can't burn admission slots, and nothing jumps its
    /// dependencies.
    #[test]
    fn backoff_and_deps_gate_eligibility() {
        let mut wedged = d("wedged", "ws", PriorityClass::Critical); // even Critical
        wedged.backoff_until_secs = 1000;
        let mut blocked = d("blocked", "ws", PriorityClass::Critical);
        blocked.deps_ready = false;
        let ready = d("ready", "ws", PriorityClass::Low); // low but eligible
        let pending = vec![wedged, blocked, ready];
        let order = schedule_next(&pending, 500, &w(), 10); // now=500 < backoff 1000
        let ids: Vec<&String> = order.iter().map(|x| &x.unit).collect();
        assert_eq!(ids, vec![&"ready".to_string()],
                   "wedged (in backoff) + blocked (deps) excluded; only the eligible Low runs");
        // once the backoff window passes, the wedged Critical re-enters + wins
        let mut wq = d("wedged", "ws", PriorityClass::Critical);
        wq.backoff_until_secs = 1000;
        let pending2 = vec![d("ready", "ws", PriorityClass::Low), wq];
        let order2 = schedule_next(&pending2, 1000, &w(), 10);
        assert_eq!(order2[0].unit, "wedged", "after the window, the wedged Critical is eligible again");
    }

    /// Efficiency: among same-class eligible units, bigger drift + staleness
    /// schedules first (close the biggest gap per slot).
    #[test]
    fn bigger_gap_schedules_first() {
        let mut small = d("small", "ws", PriorityClass::Normal);
        small.drift_magnitude = 1;
        let mut big = d("big", "ws", PriorityClass::Normal);
        big.drift_magnitude = 50;
        let pending = vec![small, big];
        let order = schedule_next(&pending, 0, &w(), 1);
        assert_eq!(order[0].unit, "big", "biggest drift first — converge the most per slot");
    }

    /// Backpressure: with more eligible work than slots, exactly `slots` are
    /// dispatched (the rest wait — no crash, bounded fan-out).
    #[test]
    fn respects_admission_slots() {
        let pending: Vec<ReconcileDemand> =
            (0..100_000).map(|i| d(&format!("u{i:06}"), "ws", PriorityClass::Normal)).collect();
        let order = schedule_next(&pending, 0, &w(), 8);
        assert_eq!(order.len(), 8, "100k pending, only `slots` dispatched — bounded, no crash");
    }
}
