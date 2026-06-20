//! Cross-template run-all DAG — Terragrunt `run-all` parity (P3).
//!
//! Terragrunt's `run-all apply` walks the dependency graph and applies units in
//! topological order, never a unit before its `dependency` upstreams. This module
//! is the standalone-template equivalent: it assembles a DAG from the per-template
//! dependency edges (`template_dependency::dependency_edges`), and provides
//!
//! - `deps_satisfied` / `eligible` — the gate the [`ReconcileQueue`] already has
//!   (its `deps_ready` field): a template may reconcile only once ALL its upstream
//!   templates are `Ready`, so the run-all order falls out of the scheduler we
//!   already built — no separate orchestrator;
//! - `apply_order` (topological — upstreams first) and `destroy_order` (reverse),
//!   for whole-fleet plan/apply/destroy;
//! - cycle rejection — a dependency cycle is a typed error, never an infinite
//!   "waiting for upstream" loop.
//!
//! It lifts what `flow_scheduler.rs` does inside one `InfrastructureFlow` to the
//! top-level template/workspace layer. Pure `std`, fully unit-testable.

use std::collections::{BTreeMap, BTreeSet, VecDeque};

/// A template-dependency DAG: each template → the set of upstream templates it
/// depends on (an edge `t → u` means "t needs u's outputs/Ready-ness first").
#[derive(Debug, Default, Clone)]
pub struct TemplateDag {
    upstreams: BTreeMap<String, BTreeSet<String>>,
}

/// Why a DAG operation could not produce an order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DagError {
    /// A dependency cycle — the templates that remained unorderable (sorted).
    /// They can never all become Ready, so this is a typed authoring error, not
    /// a transient wait.
    Cycle(Vec<String>),
}

impl std::fmt::Display for DagError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DagError::Cycle(c) => write!(f, "dependency cycle among templates: {}", c.join(" → ")),
        }
    }
}

impl TemplateDag {
    #[must_use]
    pub fn new() -> Self {
        Self { upstreams: BTreeMap::new() }
    }

    /// Register a template and the upstreams it depends on. Idempotent; an
    /// upstream named here that is never itself `add`ed is still a node (a
    /// leaf with no dependencies of its own).
    pub fn add(&mut self, template: &str, upstreams: &[String]) {
        // Add the edges (borrow released at the end of this statement)…
        self.upstreams.entry(template.to_string()).or_default().extend(upstreams.iter().cloned());
        // …then ensure every named upstream exists as a node even if it has no
        // edges of its own (a separate borrow — avoids holding two at once).
        for u in upstreams {
            self.upstreams.entry(u.clone()).or_default();
        }
    }

    /// All template names in the DAG (deterministic order).
    #[must_use]
    pub fn nodes(&self) -> Vec<String> {
        self.upstreams.keys().cloned().collect()
    }

    /// True iff every upstream of `template` is in `ready`. A template with no
    /// upstreams is trivially satisfied (a root).
    #[must_use]
    pub fn deps_satisfied(&self, template: &str, ready: &BTreeSet<String>) -> bool {
        self.upstreams.get(template).is_none_or(|ups| ups.iter().all(|u| ready.contains(u)))
    }

    /// The templates the scheduler may dispatch NOW: every node whose upstreams
    /// are all `ready` and which is not itself already `ready`. Deterministic.
    #[must_use]
    pub fn eligible(&self, ready: &BTreeSet<String>) -> Vec<String> {
        self.upstreams
            .keys()
            .filter(|t| !ready.contains(*t) && self.deps_satisfied(t, ready))
            .cloned()
            .collect()
    }

    /// Topological apply order — every template after all its upstreams (Kahn's
    /// algorithm, ties broken by name for determinism). `Err(Cycle)` if the
    /// graph has a cycle.
    pub fn apply_order(&self) -> Result<Vec<String>, DagError> {
        // in-degree = number of upstreams each node still waits on.
        let mut indeg: BTreeMap<String, usize> =
            self.upstreams.iter().map(|(t, ups)| (t.clone(), ups.len())).collect();
        // dependents: u → templates that depend on u (to decrement on removal).
        let mut dependents: BTreeMap<String, Vec<String>> = BTreeMap::new();
        for (t, ups) in &self.upstreams {
            for u in ups {
                dependents.entry(u.clone()).or_default().push(t.clone());
            }
        }
        let mut ready: VecDeque<String> =
            indeg.iter().filter(|(_, d)| **d == 0).map(|(t, _)| t.clone()).collect();
        let mut order = Vec::with_capacity(indeg.len());
        while let Some(t) = ready.pop_front() {
            order.push(t.clone());
            if let Some(deps) = dependents.get(&t) {
                for d in deps {
                    if let Some(e) = indeg.get_mut(d) {
                        *e -= 1;
                        if *e == 0 {
                            // insert keeping name order (small N; deterministic).
                            let pos = ready.iter().position(|x| x > d).unwrap_or(ready.len());
                            ready.insert(pos, d.clone());
                        }
                    }
                }
            }
        }
        if order.len() == indeg.len() {
            Ok(order)
        } else {
            // the un-emitted nodes form (or feed) the cycle.
            let emitted: BTreeSet<&String> = order.iter().collect();
            let cyc: Vec<String> = indeg.keys().filter(|t| !emitted.contains(t)).cloned().collect();
            Err(DagError::Cycle(cyc))
        }
    }

    /// Reverse of [`apply_order`] — destroy dependents before their upstreams.
    pub fn destroy_order(&self) -> Result<Vec<String>, DagError> {
        self.apply_order().map(|mut o| {
            o.reverse();
            o
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dag(edges: &[(&str, &[&str])]) -> TemplateDag {
        let mut d = TemplateDag::new();
        for (t, ups) in edges {
            d.add(t, &ups.iter().map(|s| (*s).to_string()).collect::<Vec<_>>());
        }
        d
    }
    fn ready(names: &[&str]) -> BTreeSet<String> {
        names.iter().map(|s| (*s).to_string()).collect()
    }

    #[test]
    fn linear_chain_orders_upstreams_first() {
        // app depends on db depends on vpc
        let d = dag(&[("app", &["db"]), ("db", &["vpc"])]);
        assert_eq!(d.apply_order().unwrap(), vec!["vpc", "db", "app"]);
        assert_eq!(d.destroy_order().unwrap(), vec!["app", "db", "vpc"]);
    }

    #[test]
    fn deps_gate_matches_terragrunt_run_all() {
        let d = dag(&[("app", &["db"]), ("db", &["vpc"])]);
        // nothing ready: only vpc (no upstreams) is eligible.
        assert_eq!(d.eligible(&ready(&[])), vec!["vpc"]);
        // vpc ready: db unlocks; app still gated.
        assert_eq!(d.eligible(&ready(&["vpc"])), vec!["db"]);
        assert!(!d.deps_satisfied("app", &ready(&["vpc"])), "app waits on db");
        // vpc+db ready: app unlocks.
        assert_eq!(d.eligible(&ready(&["vpc", "db"])), vec!["app"]);
        assert!(d.deps_satisfied("app", &ready(&["vpc", "db"])));
    }

    #[test]
    fn diamond_orders_correctly() {
        // d depends on b and c; b and c each depend on a.
        let d = dag(&[("d", &["b", "c"]), ("b", &["a"]), ("c", &["a"])]);
        let order = d.apply_order().unwrap();
        let pos = |n: &str| order.iter().position(|x| x == n).unwrap();
        assert!(pos("a") < pos("b") && pos("a") < pos("c"), "a before b,c");
        assert!(pos("b") < pos("d") && pos("c") < pos("d"), "b,c before d");
        assert_eq!(order.len(), 4);
    }

    #[test]
    fn a_cycle_is_a_typed_error_not_an_infinite_wait() {
        let d = dag(&[("x", &["y"]), ("y", &["x"])]);
        match d.apply_order() {
            Err(DagError::Cycle(c)) => assert_eq!(c, vec!["x".to_string(), "y".to_string()]),
            other => panic!("expected a cycle error, got {other:?}"),
        }
        // a cyclic pair can never become eligible from empty-ready.
        assert!(d.eligible(&ready(&[])).is_empty(), "neither can start — correctly stuck, surfaced as a cycle");
    }

    #[test]
    fn roots_have_no_upstreams() {
        let d = dag(&[("solo", &[])]);
        assert!(d.deps_satisfied("solo", &ready(&[])));
        assert_eq!(d.apply_order().unwrap(), vec!["solo"]);
    }
}
