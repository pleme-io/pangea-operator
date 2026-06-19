//! Generic fair-priority scheduling — the trait-based, fleet-reusable core.
//!
//! # Why this is a trait, not operator code
//!
//! "Bound in-flight work, never starve any tenant, and converge in the most
//! valuable order" is what *every* reconcile loop in the fleet needs — the
//! pangea-operator (templates × workspaces), breathe (bands × nodes), eclusa
//! (PRs × repos), Viggy (promessas × clusters). Re-inventing the ranking +
//! anti-starvation in each is exactly the duplication the Compounding Directive
//! forbids. So the *algorithm* lives here, generic over a [`Schedulable`]
//! contract; a consumer implements eight small accessors on its own unit type
//! and gets the whole policy — class tiers, urgency, fairness-deficit
//! anti-starvation, dependency + backoff gating, bounded backpressure — for
//! free, proven once.
//!
//! This is the substrate-extraction shape: the module depends only on `std`, so
//! it lifts into `shigoto` (next to `BudgetTree`) unchanged. The operator is
//! its first consumer ([`super::reconcile_scheduler::ReconcileDemand`]); the
//! budget admission half is [`super::workspace_budget`].
//!
//! # Tier honesty
//!
//! The proofs are mechanical CI forcing-functions over the *decision function*
//! (total order, class dominance, no-starvation-by-deficit, gating, bounded
//! dispatch). They are not a runtime liveness guarantee (that depends on
//! throughput vs arrival) — they guarantee the policy is total, deterministic,
//! class-correct, and starvation-free by construction.

use std::cmp::Reverse;

/// Hard priority tiers. Lower discriminant = higher priority.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum PriorityClass {
    /// Life-safety / SLA-bound — preempts everything below.
    Critical = 0,
    High = 1,
    Normal = 2,
    /// Best-effort / experimental.
    Low = 3,
}

/// Tunable urgency weights. Defaults: behind-target and drift dominate raw age,
/// and the fairness deficit can eventually overcome any of them.
#[derive(Debug, Clone, Copy)]
pub struct UrgencyWeights {
    pub per_stale_sec: u64,
    pub behind_target: u64,
    pub per_drift: u64,
    pub per_deficit: u64,
}

impl Default for UrgencyWeights {
    fn default() -> Self {
        UrgencyWeights { per_stale_sec: 1, behind_target: 3600, per_drift: 120, per_deficit: 600 }
    }
}

/// The contract a unit implements to be scheduled. Eight accessors describe the
/// unit; the `urgency`/`rank_key` default methods are the shared algorithm — a
/// consumer never re-implements ranking, only describes its unit.
pub trait Schedulable {
    /// Stable unique id — the deterministic tiebreak.
    fn sched_id(&self) -> &str;
    /// Fairness + budget bucket (workspace / tenant / namespace / repo).
    fn sched_scope(&self) -> &str;
    fn priority_class(&self) -> PriorityClass;
    /// Seconds since last successful reconcile (most-stale-first).
    fn staleness_secs(&self) -> u64;
    /// Behind the declared desired state (e.g. source moved past compiled).
    fn behind_target(&self) -> bool;
    /// How far from desired (drift count) — close the biggest gap first.
    fn drift_magnitude(&self) -> u32;
    /// Anti-starvation term: how many rounds this unit's scope was passed over.
    fn fairness_deficit(&self) -> u64;
    /// Eligible to be ranked: dependencies satisfied AND past any backoff
    /// window. An ineligible unit is simply not a candidate this tick.
    fn eligible(&self, now_secs: u64) -> bool;

    /// Within-class urgency (default algorithm). Higher = sooner. The fairness
    /// deficit is additive + unbounded-growing, so a starved unit's urgency
    /// rises every round skipped until it must be scheduled.
    fn urgency(&self, w: &UrgencyWeights) -> u64 {
        self.staleness_secs()
            .saturating_mul(w.per_stale_sec)
            .saturating_add(if self.behind_target() { w.behind_target } else { 0 })
            .saturating_add(u64::from(self.drift_magnitude()).saturating_mul(w.per_drift))
            .saturating_add(self.fairness_deficit().saturating_mul(w.per_deficit))
    }

    /// Total scheduling order: class (hard tier) → urgency (desc) → id (stable).
    fn rank_key(&self, w: &UrgencyWeights) -> (PriorityClass, Reverse<u64>, &str) {
        (self.priority_class(), Reverse(self.urgency(w)), self.sched_id())
    }
}

/// Pick the next units to dispatch from `pending`, in scheduling order, up to
/// `slots` (the admission budget's free capacity). Pure + deterministic.
/// Ineligible units (deps unmet / in backoff) are excluded entirely.
pub fn schedule<'a, T: Schedulable>(
    pending: &'a [T],
    now_secs: u64,
    weights: &UrgencyWeights,
    slots: usize,
) -> Vec<&'a T> {
    let mut eligible: Vec<&T> = pending.iter().filter(|d| d.eligible(now_secs)).collect();
    eligible.sort_by(|a, b| a.rank_key(weights).cmp(&b.rank_key(weights)));
    eligible.into_iter().take(slots).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A minimal mock unit — proves the trait + algorithm work for ANY
    /// consumer type, not just the operator's `ReconcileDemand`.
    struct Mock {
        id: String,
        scope: String,
        class: PriorityClass,
        stale: u64,
        behind: bool,
        drift: u32,
        deficit: u64,
        backoff_until: u64,
        deps_ready: bool,
    }
    impl Mock {
        fn new(id: &str, class: PriorityClass) -> Self {
            Mock {
                id: id.into(), scope: "s".into(), class, stale: 0, behind: false,
                drift: 0, deficit: 0, backoff_until: 0, deps_ready: true,
            }
        }
    }
    impl Schedulable for Mock {
        fn sched_id(&self) -> &str { &self.id }
        fn sched_scope(&self) -> &str { &self.scope }
        fn priority_class(&self) -> PriorityClass { self.class }
        fn staleness_secs(&self) -> u64 { self.stale }
        fn behind_target(&self) -> bool { self.behind }
        fn drift_magnitude(&self) -> u32 { self.drift }
        fn fairness_deficit(&self) -> u64 { self.deficit }
        fn eligible(&self, now: u64) -> bool { self.deps_ready && now >= self.backoff_until }
    }
    fn w() -> UrgencyWeights { UrgencyWeights::default() }

    #[test]
    fn deterministic_total_order() {
        let mut a = Mock::new("z", PriorityClass::Normal); a.drift = 5;
        let mut b = Mock::new("a", PriorityClass::Normal); b.drift = 5;
        let pending = vec![a, b];
        let s1: Vec<_> = schedule(&pending, 0, &w(), 9).iter().map(|x| x.id.clone()).collect();
        let s2: Vec<_> = schedule(&pending, 0, &w(), 9).iter().map(|x| x.id.clone()).collect();
        assert_eq!(s1, s2);
        assert_eq!(s1[0], "a", "equal urgency breaks to lower id");
    }

    #[test]
    fn class_dominates_urgency() {
        let crit = Mock::new("crit", PriorityClass::Critical);
        let mut loud = Mock::new("loud", PriorityClass::Normal);
        loud.stale = 999_999; loud.drift = 999; loud.behind = true;
        let pending = vec![loud, crit];
        assert_eq!(schedule(&pending, 0, &w(), 9)[0].id, "crit");
    }

    #[test]
    fn fairness_deficit_prevents_starvation() {
        let mut busy = Mock::new("busy", PriorityClass::Normal); busy.drift = 10;
        let starved0 = Mock::new("starved", PriorityClass::Normal);
        let p0 = vec![busy, starved0];
        assert_eq!(schedule(&p0, 0, &w(), 1)[0].id, "busy", "no deficit → busy wins");

        let mut busy2 = Mock::new("busy", PriorityClass::Normal); busy2.drift = 10;
        let mut starved1 = Mock::new("starved", PriorityClass::Normal); starved1.deficit = 100;
        let p1 = vec![busy2, starved1];
        assert_eq!(schedule(&p1, 0, &w(), 1)[0].id, "starved", "accrued deficit overtakes — no starvation");
    }

    #[test]
    fn backoff_and_deps_gate() {
        let mut wedged = Mock::new("wedged", PriorityClass::Critical); wedged.backoff_until = 1000;
        let mut blocked = Mock::new("blocked", PriorityClass::Critical); blocked.deps_ready = false;
        let ready = Mock::new("ready", PriorityClass::Low);
        let pending = vec![wedged, blocked, ready];
        let ids: Vec<_> = schedule(&pending, 500, &w(), 9).iter().map(|x| x.id.clone()).collect();
        assert_eq!(ids, vec!["ready".to_string()], "wedged+blocked excluded; only eligible Low runs");
    }

    #[test]
    fn bigger_gap_first() {
        let mut small = Mock::new("small", PriorityClass::Normal); small.drift = 1;
        let mut big = Mock::new("big", PriorityClass::Normal); big.drift = 50;
        let pending = vec![small, big];
        assert_eq!(schedule(&pending, 0, &w(), 1)[0].id, "big");
    }

    #[test]
    fn bounded_dispatch_at_100k() {
        let pending: Vec<Mock> = (0..100_000).map(|i| Mock::new(&format!("u{i:06}"), PriorityClass::Normal)).collect();
        assert_eq!(schedule(&pending, 0, &w(), 8).len(), 8, "100k pending → bounded dispatch");
    }
}
