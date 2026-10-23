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
//! top-level template/workspace layer.
//!
//! # Substrate delegation
//!
//! The topology itself — nodes, edges, topological order, cycle detection — is
//! [`shigoto_dag::Dag`], not a hand-rolled Kahn's-algorithm walk. `Dag` operates
//! over typed [`shigoto_types::JobId`]s rather than bare template-name strings,
//! so this module keeps a thin `String ⇄ JobId` bridge (every template becomes a
//! `JobId` with a fixed `kind` and the template name as its `subject`) — that
//! bridge, plus the small `names: BTreeSet<String>` index needed because `Dag`
//! v0.1 doesn't expose "list every node" (only `predecessors`, `toposort`,
//! `waves`, `node_count`/`edge_count`), is genuinely new code; the topological
//! ordering and cycle detection are not.
//!
//! [`apply_order`](TemplateDag::apply_order) is built from
//! [`Dag::waves`](shigoto_dag::Dag::waves) rather than `Dag::toposort` directly:
//! waves are topologically ordered *and* deterministically tie-broken within a
//! wave (`shigoto_dag`'s own `stable_job_key`, which — since every template
//! shares one scope + kind here — reduces to sorting by template name). That
//! reproduces this module's original "ties broken by name" guarantee for free.
//!
//! On a cycle, `shigoto_dag::DagError::Cycle` names only ONE node on it (the one
//! `petgraph::algo::toposort` happened to reject). This module's original API
//! promised the FULL unresolvable set (every template that can never become
//! ready). [`unresolvable_nodes`](TemplateDag::unresolvable_nodes) recovers that
//! set via a small fixed-point scan built on [`Dag::predecessors`] — the same
//! primitive [`deps_satisfied`](TemplateDag::deps_satisfied) already uses, not a
//! second topological sort.

use std::collections::BTreeSet;

use shigoto_dag::{Dag, DagError as ShigotoDagError};
use shigoto_types::{JobId, JobKindId, JobScope, JobSubject};

/// The fixed work-kind every template node uses. `Dag` is generic over
/// `JobId { scope, kind, subject }`; this module only ever needs one flat
/// namespace (global scope, one kind), so `subject` alone carries the
/// template's identity.
fn template_kind() -> JobKindId {
    JobKindId::new("pangea-template")
}

fn node_id(template: &str) -> JobId {
    JobId {
        scope: JobScope::Global,
        kind: template_kind(),
        subject: JobSubject::Pinned(template.to_string()),
    }
}

fn node_name(id: &JobId) -> Option<String> {
    match &id.subject {
        JobSubject::Pinned(s) => Some(s.clone()),
        _ => None,
    }
}

/// A template-dependency DAG: each template → the set of upstream templates it
/// depends on (an edge `t → u` means "t needs u's outputs/Ready-ness first").
///
/// No `#[derive(Debug)]`: `shigoto_dag::Dag` doesn't implement `Debug` (v0.1
/// carries no such impl). `names` alone (via `nodes()`) is the useful debug
/// surface for this type anyway.
#[derive(Default)]
pub struct TemplateDag {
    dag: Dag,
    /// Every template name ever seen via [`add`](Self::add) — `Dag` v0.1 has no
    /// "list all nodes" accessor, so this is the thin index that makes
    /// [`nodes`](Self::nodes)/[`eligible`](Self::eligible) possible without a
    /// second graph structure.
    names: BTreeSet<String>,
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
        Self {
            dag: Dag::new(),
            names: BTreeSet::new(),
        }
    }

    /// Register a template and the upstreams it depends on. Idempotent; an
    /// upstream named here that is never itself `add`ed is still a node (a
    /// leaf with no dependencies of its own) — `Dag::add_edge` ensures both
    /// endpoints exist.
    pub fn add(&mut self, template: &str, upstreams: &[String]) {
        self.names.insert(template.to_string());
        let t_id = node_id(template);
        self.dag.ensure_node(t_id.clone());
        for u in upstreams {
            self.names.insert(u.clone());
            // Edge direction: upstream → downstream (u must reach a terminal
            // phase before t can start) — matches `Dag::predecessors(t)`
            // returning t's direct upstreams, which `deps_satisfied` relies on.
            self.dag.add_edge(node_id(u), t_id.clone());
        }
    }

    /// All template names in the DAG (deterministic order).
    #[must_use]
    pub fn nodes(&self) -> Vec<String> {
        self.names.iter().cloned().collect()
    }

    /// True iff every upstream of `template` is in `ready`. A template with no
    /// upstreams is trivially satisfied (a root).
    #[must_use]
    pub fn deps_satisfied(&self, template: &str, ready: &BTreeSet<String>) -> bool {
        self.dag
            .predecessors(&node_id(template))
            .iter()
            .all(|p| node_name(p).is_some_and(|n| ready.contains(&n)))
    }

    /// The templates the scheduler may dispatch NOW: every node whose upstreams
    /// are all `ready` and which is not itself already `ready`. Deterministic.
    #[must_use]
    pub fn eligible(&self, ready: &BTreeSet<String>) -> Vec<String> {
        self.names
            .iter()
            .filter(|t| !ready.contains(*t) && self.deps_satisfied(t, ready))
            .cloned()
            .collect()
    }

    /// Topological apply order — every template after all its upstreams, ties
    /// broken by name for determinism (via [`Dag::waves`]). `Err(Cycle)` if the
    /// graph has a cycle.
    pub fn apply_order(&self) -> Result<Vec<String>, DagError> {
        match self.dag.waves(None) {
            Ok(waves) => Ok(waves
                .into_iter()
                .flatten()
                .filter_map(|id| node_name(&id))
                .collect()),
            Err(ShigotoDagError::Cycle(_) | ShigotoDagError::DanglingEdge(_)) => {
                Err(DagError::Cycle(self.unresolvable_nodes()))
            }
        }
    }

    /// Reverse of [`apply_order`] — destroy dependents before their upstreams.
    pub fn destroy_order(&self) -> Result<Vec<String>, DagError> {
        self.apply_order().map(|mut o| {
            o.reverse();
            o
        })
    }

    /// On a cycle, recover the FULL set of templates that can never become
    /// ready (not just the single node `petgraph`'s toposort happened to
    /// reject). A straightforward fixed-point over [`deps_satisfied`]: grow
    /// the `ready` set with every node whose upstreams are already in it,
    /// until nothing more can be added; whatever's left is unorderable. This
    /// is O(V²) worst case — fine at template-node-count scale, and it is a
    /// diagnostic path only reached on error, not the hot apply path (which
    /// stays `Dag::waves`, O(V + E)).
    fn unresolvable_nodes(&self) -> Vec<String> {
        let mut ready: BTreeSet<String> = BTreeSet::new();
        loop {
            let mut grew = false;
            for name in &self.names {
                if !ready.contains(name) && self.deps_satisfied(name, &ready) {
                    ready.insert(name.clone());
                    grew = true;
                }
            }
            if !grew {
                break;
            }
        }
        self.names
            .iter()
            .filter(|n| !ready.contains(*n))
            .cloned()
            .collect()
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
        assert!(
            !d.deps_satisfied("app", &ready(&["vpc"])),
            "app waits on db"
        );
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
        assert!(
            d.eligible(&ready(&[])).is_empty(),
            "neither can start — correctly stuck, surfaced as a cycle"
        );
    }

    /// Regression: a cycle among a subset of a larger graph must surface
    /// EXACTLY the unresolvable subset (not the whole graph, and not just
    /// the one node `petgraph`'s toposort happened to name) — proves
    /// `unresolvable_nodes`'s fixed-point recovers the full set `Dag`'s
    /// single-node `DagError::Cycle` doesn't carry.
    #[test]
    fn cycle_among_a_subset_reports_exactly_the_stuck_templates() {
        // root has no deps (resolvable). x <-> y form a cycle, both
        // independently depending on the resolvable root — so the ONLY
        // reason x and y are stuck is each other, not root.
        let d = dag(&[("root", &[]), ("x", &["root", "y"]), ("y", &["root", "x"])]);
        match d.apply_order() {
            Err(DagError::Cycle(c)) => {
                assert_eq!(
                    c,
                    vec!["x".to_string(), "y".to_string()],
                    "root resolves; only x,y are stuck"
                );
            }
            other => panic!("expected a cycle error, got {other:?}"),
        }
        // root is NOT in a cycle even though the graph as a whole has one.
        assert_eq!(
            d.eligible(&ready(&[])),
            vec!["root"],
            "root has no deps — eligible despite the x/y cycle"
        );
    }

    #[test]
    fn roots_have_no_upstreams() {
        let d = dag(&[("solo", &[])]);
        assert!(d.deps_satisfied("solo", &ready(&[])));
        assert_eq!(d.apply_order().unwrap(), vec!["solo"]);
    }

    #[test]
    fn nodes_lists_every_registered_template_including_bare_upstreams() {
        // "vpc" is only ever named as an upstream, never `add`ed directly —
        // still must appear as a node (a leaf).
        let d = dag(&[("app", &["db"]), ("db", &["vpc"])]);
        assert_eq!(
            d.nodes(),
            vec!["app".to_string(), "db".to_string(), "vpc".to_string()]
        );
    }
}
