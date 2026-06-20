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

/// The DISPATCH ENGINE — the single decision a central reconcile queue makes each
/// tick: of everything pending, which units actually run *now*, in what order,
/// without crashing. This composes the two bounds into one pass:
///
/// 1. **rank** ([`schedule`]) decides the ORDER — most-valuable-first
///    (priority × urgency × fairness-deficit), eligibility-gated.
/// 2. **budget** ([`FairBudget`]) decides the COUNT + per-scope FAIRNESS — it
///    admits in rank order until the global pool or a scope's slice is full, so
///    one workspace can never monopolize the pool (anti-starvation) and total
///    in-flight work is bounded (no-crash).
///
/// Returns the admitted units paired with their RAII [`BudgetPermit`] — the
/// caller dispatches each and drops the permit on completion (freeing the slot).
/// A unit that ranks high but whose scope is saturated is simply skipped this
/// tick and retried next (its fairness-deficit rises, so it wins soon) — never
/// dropped. This is the function the S2/S3 workspace dispatch queue calls; at
/// 100k pending it still returns in O(n log n) rank + O(admitted) acquire, and
/// the live in-flight count never exceeds the budget regardless of n.
#[must_use]
pub fn pick_admitted<'a, T: Schedulable>(
    pending: &'a [T],
    now_secs: u64,
    weights: &UrgencyWeights,
    budget: &std::sync::Arc<FairBudget>,
) -> Vec<(&'a T, BudgetPermit)> {
    let mut admitted = Vec::new();
    // Rank the whole eligible set (slots=len ⇒ full order), then admit greedily
    // in that order subject to the budget — the narrowest of global/per-scope caps
    // binds, exactly as the no-crash bound requires.
    for unit in schedule(pending, now_secs, weights, pending.len()) {
        match budget.try_acquire(unit.sched_scope()) {
            Some(permit) => admitted.push((unit, permit)),
            // Scope (or global pool) full — skip; the unit retries next tick with
            // a higher fairness-deficit. Keep scanning: a LATER unit in a
            // different, non-saturated scope can still be admitted (fairness).
            None => continue,
        }
    }
    admitted
}

// ──────────────────────────────────────────────────────────────────────────
// Admission control — the no-crash bound that pairs with the policy above.
// ──────────────────────────────────────────────────────────────────────────

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

/// Admission caps. `per_scope` bounds one scope's concurrent in-flight work
/// (the fair slice); `global` bounds the total (the pool). A scope == the
/// fairness bucket a [`Schedulable::sched_scope`] returns.
#[derive(Debug, Clone, Copy)]
pub struct BudgetConfig {
    pub per_scope: usize,
    pub global: usize,
}

impl Default for BudgetConfig {
    fn default() -> Self {
        BudgetConfig { per_scope: 2, global: 8 }
    }
}

#[derive(Default)]
struct Inflight {
    global: usize,
    per_scope: HashMap<String, usize>,
}

/// Scope-fair concurrency admission. The no-crash bound: only `global` units do
/// expensive work at once (the rest wait), and no scope exceeds `per_scope`, so
/// a wedged scope can never starve the others. Generic over the scope string —
/// works for workspaces, tenants, namespaces, repos. Lock held only for O(1)
/// arithmetic, never across `.await` (deadlock-free).
pub struct FairBudget {
    cfg: BudgetConfig,
    inflight: Mutex<Inflight>,
}

impl FairBudget {
    pub fn new(cfg: BudgetConfig) -> Arc<Self> {
        Arc::new(FairBudget { cfg, inflight: Mutex::new(Inflight::default()) })
    }

    /// Try to take a slot for `scope`. `Some(permit)` if both caps allow (the
    /// permit frees the slot on drop), else `None` — the caller requeues.
    /// Never blocks.
    pub fn try_acquire(self: &Arc<Self>, scope: &str) -> Option<BudgetPermit> {
        let mut g = self.inflight.lock().expect("budget mutex poisoned");
        let now = *g.per_scope.get(scope).unwrap_or(&0);
        if g.global >= self.cfg.global || now >= self.cfg.per_scope {
            return None;
        }
        g.global += 1;
        *g.per_scope.entry(scope.to_string()).or_insert(0) += 1;
        Some(BudgetPermit { budget: Arc::clone(self), scope: scope.to_string(), released: false })
    }

    pub fn in_flight(&self, scope: &str) -> usize {
        *self.inflight.lock().expect("budget mutex poisoned").per_scope.get(scope).unwrap_or(&0)
    }

    pub fn total_in_flight(&self) -> usize {
        self.inflight.lock().expect("budget mutex poisoned").global
    }

    fn release(&self, scope: &str) {
        let mut g = self.inflight.lock().expect("budget mutex poisoned");
        g.global = g.global.saturating_sub(1);
        if let Some(c) = g.per_scope.get_mut(scope) {
            *c = c.saturating_sub(1);
            if *c == 0 {
                g.per_scope.remove(scope);
            }
        }
    }
}

/// RAII admission slot — frees its slot on drop, exactly once (panic-safe).
pub struct BudgetPermit {
    budget: Arc<FairBudget>,
    scope: String,
    released: bool,
}

impl Drop for BudgetPermit {
    fn drop(&mut self) {
        if !self.released {
            self.released = true;
            self.budget.release(&self.scope);
        }
    }
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

    // ── admission (FairBudget) proofs ──────────────────────────────────────
    fn budget(per_scope: usize, global: usize) -> Arc<FairBudget> {
        FairBudget::new(BudgetConfig { per_scope, global })
    }

    #[test]
    fn per_scope_cap_holds() {
        let b = budget(2, 100);
        let _p1 = b.try_acquire("a").unwrap();
        let _p2 = b.try_acquire("a").unwrap();
        assert!(b.try_acquire("a").is_none(), "3rd acquire on scope a refused (cap 2)");
    }

    #[test]
    fn global_cap_holds() {
        let b = budget(10, 3);
        let _a = b.try_acquire("a").unwrap();
        let _c = b.try_acquire("b").unwrap();
        let _d = b.try_acquire("c").unwrap();
        assert!(b.try_acquire("d").is_none(), "4th refused (global cap 3)");
    }

    #[test]
    fn a_wedged_scope_does_not_starve_others() {
        let b = budget(2, 100);
        let _w1 = b.try_acquire("wedged").unwrap();
        let _w2 = b.try_acquire("wedged").unwrap();
        assert!(b.try_acquire("wedged").is_none(), "wedged at its cap");
        let _o1 = b.try_acquire("other").unwrap();
        let _o2 = b.try_acquire("other").unwrap();
        assert_eq!(b.in_flight("other"), 2, "other unaffected by wedged being capped");
    }

    #[test]
    fn raii_release_frees_the_slot() {
        let b = budget(1, 10);
        {
            let _p = b.try_acquire("a").unwrap();
            assert!(b.try_acquire("a").is_none());
        }
        assert_eq!(b.in_flight("a"), 0, "freed on drop");
        let _p2 = b.try_acquire("a").expect("re-acquire after release");
        assert_eq!(b.total_in_flight(), 1);
    }

    #[test]
    fn one_scope_cannot_hog_a_contended_pool() {
        let b = budget(2, 6);
        let mut held = Vec::new();
        for _ in 0..10 {
            if let Some(p) = b.try_acquire("hog") { held.push(p); }
        }
        assert_eq!(b.in_flight("hog"), 2, "capped at its share, not the whole pool");
        assert!(b.try_acquire("other").is_some(), "room remains for others");
    }

    // ── the composed dispatch engine: rank ⊕ budget ────────────────────────
    fn in_scope(id: &str, scope: &str, class: PriorityClass) -> Mock {
        let mut m = Mock::new(id, class);
        m.scope = scope.into();
        m
    }

    /// Most-valuable-first WITHIN the budget: same scope, per_scope=2 ⇒ exactly
    /// the top-2-by-rank are admitted, the 3rd skipped (scope full), none dropped.
    #[test]
    fn pick_admits_top_ranked_within_budget() {
        let b = budget(2, 10); // per_scope 2
        let crit = in_scope("crit", "ws", PriorityClass::Critical);
        let mut loud = in_scope("loud", "ws", PriorityClass::Normal);
        loud.drift = 999;
        let quiet = in_scope("quiet", "ws", PriorityClass::Low);
        let pending = vec![quiet, loud, crit];
        // BIND the result — the permits are RAII; dropping the Vec frees them.
        let admitted = pick_admitted(&pending, 0, &w(), &b);
        let got: Vec<&str> = admitted.iter().map(|(u, _)| u.sched_id()).collect();
        assert_eq!(got, vec!["crit", "loud"], "Critical then loud-Normal admitted; Low skipped (scope at cap)");
        assert_eq!(b.in_flight("ws"), 2, "exactly the per-scope cap in flight while the permits are held");
    }

    /// Anti-starvation across scopes: a saturated scope does NOT block admission
    /// of a lower-ranked unit in a different, free scope.
    #[test]
    fn pick_does_not_let_a_saturated_scope_block_other_scopes() {
        let b = budget(1, 10); // per_scope 1
        let mut hot1 = in_scope("hot1", "hot", PriorityClass::Critical);
        hot1.drift = 999;
        let hot2 = in_scope("hot2", "hot", PriorityClass::Critical); // same scope, loses the 1 slot
        let cold = in_scope("cold", "cold", PriorityClass::Low); // different scope, still admitted
        let pending = vec![hot1, hot2, cold];
        let got: Vec<&str> = pick_admitted(&pending, 0, &w(), &b).iter().map(|(u, _)| u.sched_id()).collect();
        assert!(got.contains(&"hot1") && got.contains(&"cold"), "hot1 + cold admitted: {got:?}");
        assert!(!got.contains(&"hot2"), "hot2 skipped — hot scope at cap (but cold not blocked)");
    }

    /// Bounded: the global pool cap is never exceeded no matter how much pends,
    /// and dropping the permits frees the pool (RAII).
    #[test]
    fn pick_is_globally_bounded_and_permits_release() {
        let b = budget(10, 3); // global 3
        let pending: Vec<Mock> = (0..1000)
            .map(|i| in_scope(&format!("u{i}"), &format!("ws{i}"), PriorityClass::Normal))
            .collect();
        {
            let admitted = pick_admitted(&pending, 0, &w(), &b);
            assert_eq!(admitted.len(), 3, "1000 pending → at most the global cap admitted");
            assert_eq!(b.total_in_flight(), 3);
        } // permits dropped here
        assert_eq!(b.total_in_flight(), 0, "RAII release frees the whole pool on completion");
    }
}
