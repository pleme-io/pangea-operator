//! Per-workspace concurrency budget (lifecycle S1).
//!
//! # The starvation this fixes
//!
//! Reconciles share a bounded worker pool. When a workspace wedges — e.g. a
//! batch of templates stuck retrying `Compiling` (the `pleme-io-opensource`
//! class) — its templates can occupy the pool doing expensive,
//! never-succeeding work and **starve every other workspace's reconciles**
//! (documented in `feedback_pangea_template_envfetch_compile_wedge`).
//!
//! The fix is the scaling seam (`super::workspace_lifecycle`): bound *per
//! workspace* how many templates do **expensive** work (compile / plan /
//! apply) at once. A wedged workspace can hold at most its slice; the rest of
//! the pool stays free for everyone else. Cheap phases (Pending advance, Ready
//! throttle, awaiting-approval) don't draw budget, so idle templates never
//! consume a slot.
//!
//! This is the in-process realization of shigoto's `BudgetTree` keyed by
//! workspace scope — non-blocking: a template whose workspace is at its cap
//! gets `None` and **requeues** (freeing the worker) rather than blocking it.
//!
//! # The four invariants (proven in `tests`)
//!
//! 1. **per-workspace cap** — a workspace never exceeds `per_workspace`
//!    concurrent permits.
//! 2. **global cap** — total live permits never exceed `global`.
//! 3. **cross-workspace isolation** — a workspace at *its* cap never blocks a
//!    *different* workspace from acquiring (until the global cap). This is the
//!    anti-starvation guarantee.
//! 4. **RAII release** — a dropped [`BudgetPermit`] frees its slot exactly once
//!    (panic-safe), so a failed/panicked reconcile can't leak budget.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

/// Budget caps. `per_workspace` bounds one workspace's concurrent expensive
/// reconciles; `global` bounds the total across all workspaces (the pool size).
#[derive(Debug, Clone, Copy)]
pub struct BudgetConfig {
    pub per_workspace: usize,
    pub global: usize,
}

impl Default for BudgetConfig {
    fn default() -> Self {
        // Conservative defaults; tuned via ReconciliationLoopSpec when wired.
        BudgetConfig { per_workspace: 2, global: 8 }
    }
}

#[derive(Default)]
struct Inflight {
    global: usize,
    per_ws: HashMap<String, usize>,
}

/// Per-workspace concurrency budget manager. Cheap, lock-guarded counters; the
/// lock is held only for the O(1) acquire/release arithmetic, never across an
/// `.await`, so it cannot deadlock the async runtime.
pub struct WorkspaceBudgets {
    cfg: BudgetConfig,
    inflight: Mutex<Inflight>,
}

impl WorkspaceBudgets {
    pub fn new(cfg: BudgetConfig) -> Arc<Self> {
        Arc::new(WorkspaceBudgets { cfg, inflight: Mutex::new(Inflight::default()) })
    }

    /// Try to take a slot for `workspace`'s expensive work. Returns a permit
    /// (released on drop) if both the per-workspace and global caps allow,
    /// else `None` — the caller requeues. Never blocks.
    pub fn try_acquire(self: &Arc<Self>, workspace: &str) -> Option<BudgetPermit> {
        let mut g = self.inflight.lock().expect("budget mutex poisoned");
        let ws_now = *g.per_ws.get(workspace).unwrap_or(&0);
        if g.global >= self.cfg.global || ws_now >= self.cfg.per_workspace {
            return None;
        }
        g.global += 1;
        *g.per_ws.entry(workspace.to_string()).or_insert(0) += 1;
        Some(BudgetPermit {
            budgets: Arc::clone(self),
            workspace: workspace.to_string(),
            released: false,
        })
    }

    /// Current in-flight count for a workspace (for metrics/diagnostics).
    pub fn in_flight(&self, workspace: &str) -> usize {
        *self.inflight.lock().expect("budget mutex poisoned").per_ws.get(workspace).unwrap_or(&0)
    }

    /// Current total in-flight across all workspaces.
    pub fn total_in_flight(&self) -> usize {
        self.inflight.lock().expect("budget mutex poisoned").global
    }

    fn release(&self, workspace: &str) {
        let mut g = self.inflight.lock().expect("budget mutex poisoned");
        g.global = g.global.saturating_sub(1);
        if let Some(c) = g.per_ws.get_mut(workspace) {
            *c = c.saturating_sub(1);
            if *c == 0 {
                g.per_ws.remove(workspace);
            }
        }
    }
}

/// RAII budget slot. Frees its slot on drop (exactly once, panic-safe).
pub struct BudgetPermit {
    budgets: Arc<WorkspaceBudgets>,
    workspace: String,
    released: bool,
}

impl Drop for BudgetPermit {
    fn drop(&mut self) {
        if !self.released {
            self.released = true;
            self.budgets.release(&self.workspace);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn budgets(per_ws: usize, global: usize) -> Arc<WorkspaceBudgets> {
        WorkspaceBudgets::new(BudgetConfig { per_workspace: per_ws, global })
    }

    /// Invariant 1: a workspace never exceeds its per-workspace cap.
    #[test]
    fn per_workspace_cap_holds() {
        let b = budgets(2, 100);
        let _p1 = b.try_acquire("ws-a").unwrap();
        let _p2 = b.try_acquire("ws-a").unwrap();
        assert!(b.try_acquire("ws-a").is_none(), "3rd acquire on ws-a must be refused (cap 2)");
        assert_eq!(b.in_flight("ws-a"), 2);
    }

    /// Invariant 2: total live permits never exceed the global cap.
    #[test]
    fn global_cap_holds() {
        let b = budgets(10, 3);
        let _a = b.try_acquire("ws-a").unwrap();
        let _b2 = b.try_acquire("ws-b").unwrap();
        let _c = b.try_acquire("ws-c").unwrap();
        assert!(b.try_acquire("ws-d").is_none(), "4th acquire must be refused (global cap 3)");
        assert_eq!(b.total_in_flight(), 3);
    }

    /// Invariant 3 (the anti-starvation guarantee): a workspace at its cap
    /// never blocks a different workspace from acquiring.
    #[test]
    fn a_wedged_workspace_does_not_starve_others() {
        let b = budgets(2, 100);
        // ws-wedged saturates its own cap with stuck work...
        let _w1 = b.try_acquire("ws-wedged").unwrap();
        let _w2 = b.try_acquire("ws-wedged").unwrap();
        assert!(b.try_acquire("ws-wedged").is_none(), "wedged ws is at its cap");
        // ...yet every other workspace still gets full service.
        let _o1 = b.try_acquire("ws-other").unwrap();
        let _o2 = b.try_acquire("ws-other").unwrap();
        assert_eq!(b.in_flight("ws-other"), 2, "ws-other is unaffected by ws-wedged being capped");
        // and a third, fourth workspace too
        assert!(b.try_acquire("ws-third").is_some());
        assert!(b.try_acquire("ws-fourth").is_some());
    }

    /// Invariant 4: dropping a permit frees the slot exactly once.
    #[test]
    fn raii_release_frees_the_slot() {
        let b = budgets(1, 10);
        {
            let _p = b.try_acquire("ws-a").unwrap();
            assert!(b.try_acquire("ws-a").is_none(), "at cap while permit held");
            assert_eq!(b.in_flight("ws-a"), 1);
        } // permit dropped here
        assert_eq!(b.in_flight("ws-a"), 0, "slot freed on drop");
        // bind the re-acquired permit (an unbound `try_acquire().is_some()` would
        // RAII-release immediately — which is itself correct behavior).
        let _p2 = b.try_acquire("ws-a").expect("can re-acquire after release");
        assert_eq!(b.total_in_flight(), 1);
    }

    /// Releasing never underflows (saturating), even under spurious double-free
    /// shapes — total stays consistent.
    #[test]
    fn release_is_consistent() {
        let b = budgets(3, 10);
        let p1 = b.try_acquire("ws-a").unwrap();
        let p2 = b.try_acquire("ws-a").unwrap();
        assert_eq!(b.in_flight("ws-a"), 2);
        drop(p1);
        assert_eq!(b.in_flight("ws-a"), 1);
        drop(p2);
        assert_eq!(b.in_flight("ws-a"), 0);
        assert_eq!(b.total_in_flight(), 0);
    }

    /// Fairness under global contention: one workspace cannot consume more than
    /// its per-workspace cap of a contended global pool, leaving room for others.
    #[test]
    fn one_workspace_cannot_hog_a_contended_pool() {
        let b = budgets(2, 6); // 3 workspaces' worth at cap 2
        let mut held = Vec::new();
        // ws-a tries to grab everything
        for _ in 0..10 {
            if let Some(p) = b.try_acquire("ws-a") { held.push(p); }
        }
        assert_eq!(b.in_flight("ws-a"), 2, "ws-a capped at its per-workspace share, not the whole pool");
        // 4 global slots remain for others
        assert!(b.try_acquire("ws-b").is_some());
        assert!(b.try_acquire("ws-c").is_some());
    }
}
