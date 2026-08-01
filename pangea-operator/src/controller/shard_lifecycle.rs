//! Typed state machine for a **shard** — which replica owns (reconciles) a
//! given workspace.
//!
//! # Why this scope exists
//!
//! The operator runs today as a leader-elected singleton. To scale
//! horizontally it must become **active-active, sharded by workspace** (the
//! reconcile/state/apply seam — see [`super::workspace_lifecycle`]): N replicas,
//! each owning a disjoint set of workspaces via a lease, all over the shared
//! DB-backed state (per-`PangeaNamespace` schema isolation means two replicas
//! reconciling different workspaces never contend). This module is the typed
//! core of that ownership lifecycle — the border the future shard-assignment
//! controller (lease acquisition + rebalance) consumes.
//!
//! It is the per-workspace-ownership peer of [`super::lifecycle`] (template)
//! and [`super::workspace_lifecycle`] (workspace): one typed
//! [`SHARD_TRANSITIONS`] table, a [`ShardPhase`] partition, and the four CI
//! forcing-functions (enumeration, no-trap, reachability, comfort) — so a shard
//! ownership that could strand a workspace (never owned, never released) is a
//! build failure.
//!
//! # The two good terminals
//!
//! `Owned` (Settled — a replica actively reconciles the workspace, the good
//! steady state) and `Released` (Terminal — the lease was cleanly handed off,
//! another replica re-claims from `Unassigned`). Every phase reaches one in
//! finitely many transitions, and every active phase can **safely reset to
//! `Unassigned`** on `LeaseLost` — the sharding analog of the drain-safe berth:
//! a lost/expired lease never wedges a workspace, it just re-enters the claim
//! cycle. This is what makes rebalancing + replica failure safe.
//!
//! # Tier honesty
//!
//! Controller reading phase off a lease/CR ⇒ transition legality is
//! parse-time-rejected; reachability/no-trap/comfort are mechanical CI
//! forcing-functions, not type-level. Never rounded up.

use std::fmt;

/// Ownership phases of a workspace shard.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShardPhase {
    /// No replica owns this workspace. Initial / after a release.
    Unassigned,
    /// A replica is attempting to acquire the lease.
    Claiming,
    /// A replica holds the lease and actively reconciles the workspace. The
    /// good steady state.
    Owned,
    /// The lease is being released (rebalance / scale-down): finish or park
    /// in-flight work, then release. Drain-safe handoff.
    Draining,
    /// Lease released — this owner is done; the workspace re-enters the claim
    /// cycle from `Unassigned`.
    Released,
}

impl fmt::Display for ShardPhase {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShardClass {
    /// Advances toward ownership / release.
    Forward,
    /// The good steady state — actively owned.
    Settled,
    /// Cleanly released (handed off).
    Terminal,
}

impl ShardClass {
    pub fn is_good_terminal(self) -> bool {
        matches!(self, ShardClass::Settled | ShardClass::Terminal)
    }
}

impl ShardPhase {
    /// Exhaustive classification.
    pub fn class(self) -> ShardClass {
        match self {
            ShardPhase::Unassigned | ShardPhase::Claiming | ShardPhase::Draining => {
                ShardClass::Forward
            }
            ShardPhase::Owned => ShardClass::Settled,
            ShardPhase::Released => ShardClass::Terminal,
        }
    }

    pub fn all() -> [ShardPhase; 5] {
        [
            ShardPhase::Unassigned,
            ShardPhase::Claiming,
            ShardPhase::Owned,
            ShardPhase::Draining,
            ShardPhase::Released,
        ]
    }

    pub fn legal_triggers(self) -> Vec<ShardTrigger> {
        SHARD_TRANSITIONS
            .iter()
            .filter(|t| t.from == self)
            .map(|t| t.trigger)
            .collect()
    }

    pub fn advance(self, trigger: ShardTrigger) -> Result<ShardPhase, ShardTransitionError> {
        SHARD_TRANSITIONS
            .iter()
            .find(|t| t.from == self && t.trigger == trigger)
            .map(|t| t.to)
            .ok_or(ShardTransitionError::Illegal {
                from: self,
                trigger,
                legal: self.legal_triggers(),
            })
    }

    pub fn edge_is_legal(self, to: ShardPhase) -> bool {
        self == to
            || SHARD_TRANSITIONS
                .iter()
                .any(|t| t.from == self && t.to == to)
    }
}

/// Exhaustiveness witness.
#[allow(dead_code)]
fn assert_shard_exhaustive(p: ShardPhase) {
    match p {
        ShardPhase::Unassigned
        | ShardPhase::Claiming
        | ShardPhase::Owned
        | ShardPhase::Draining
        | ShardPhase::Released => {}
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShardTrigger {
    /// This replica begins acquiring the lease.
    ClaimStarted,
    /// This replica won the lease.
    ClaimWon,
    /// Another replica won — abandon the attempt.
    ClaimLost,
    /// Rebalance / scale-down asks this owner to hand the workspace off.
    RebalanceRequested,
    /// Draining finished (in-flight parked) — release the lease.
    DrainComplete,
    /// The lease expired or was lost (replica failure, network partition) —
    /// safely reset to Unassigned to re-claim. The universal recovery edge.
    LeaseLost,
    /// A released workspace re-enters the claim cycle.
    Reassign,
}

impl fmt::Display for ShardTrigger {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShardEdgeKind {
    Forward,
    Remediation,
    Teardown,
}

#[derive(Debug, Clone, Copy)]
pub struct ShardTransition {
    pub from: ShardPhase,
    pub trigger: ShardTrigger,
    pub to: ShardPhase,
    pub kind: ShardEdgeKind,
}

const fn s(
    from: ShardPhase,
    trigger: ShardTrigger,
    to: ShardPhase,
    kind: ShardEdgeKind,
) -> ShardTransition {
    ShardTransition {
        from,
        trigger,
        to,
        kind,
    }
}

use ShardEdgeKind as K;
use ShardPhase as P;
use ShardTrigger as T;

/// Explicit shard transitions. The universal `LeaseLost → Unassigned` recovery
/// edge (every *active* phase can safely reset on a lost lease) is added
/// programmatically in [`SHARD_TRANSITIONS`].
pub static SHARD_EXPLICIT: &[ShardTransition] = &[
    s(P::Unassigned, T::ClaimStarted, P::Claiming, K::Forward),
    s(P::Claiming, T::ClaimWon, P::Owned, K::Forward),
    s(P::Claiming, T::ClaimLost, P::Unassigned, K::Remediation),
    s(P::Owned, T::RebalanceRequested, P::Draining, K::Teardown),
    s(P::Draining, T::DrainComplete, P::Released, K::Teardown),
    s(P::Released, T::Reassign, P::Unassigned, K::Forward),
];

fn shard_universal(from: ShardPhase) -> Vec<ShardTransition> {
    // Any active (lease-holding-or-acquiring) phase can lose the lease and must
    // safely reset to Unassigned — the sharding analog of drain-safe.
    match from {
        ShardPhase::Claiming | ShardPhase::Owned | ShardPhase::Draining => {
            vec![s(
                from,
                ShardTrigger::LeaseLost,
                ShardPhase::Unassigned,
                ShardEdgeKind::Remediation,
            )]
        }
        _ => Vec::new(),
    }
}

pub static SHARD_TRANSITIONS: std::sync::LazyLock<Vec<ShardTransition>> =
    std::sync::LazyLock::new(|| {
        let mut all = SHARD_EXPLICIT.to_vec();
        for p in ShardPhase::all() {
            all.extend(shard_universal(p));
        }
        all
    });

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ShardTransitionError {
    Illegal {
        from: ShardPhase,
        trigger: ShardTrigger,
        legal: Vec<ShardTrigger>,
    },
}

impl fmt::Display for ShardTransitionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ShardTransitionError::Illegal {
                from,
                trigger,
                legal,
            } => {
                let legal = if legal.is_empty() {
                    "<none — terminal>".to_string()
                } else {
                    legal
                        .iter()
                        .map(|t| t.to_string())
                        .collect::<Vec<_>>()
                        .join(", ")
                };
                write!(
                    f,
                    "illegal shard transition: no edge from `{from}` on `{trigger}`. \
                     Legal from `{from}`: [{legal}]. (Add the row to \
                     controller::shard_lifecycle::SHARD_EXPLICIT.)"
                )
            }
        }
    }
}

impl std::error::Error for ShardTransitionError {}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::{BTreeMap, BTreeSet, VecDeque};

    #[test]
    fn every_phase_enumerated() {
        let all = ShardPhase::all();
        let uniq: BTreeSet<_> = all.iter().map(|p| p.to_string()).collect();
        assert_eq!(uniq.len(), all.len());
        assert_eq!(all.len(), 5);
    }

    #[test]
    fn classes_partition() {
        let settled: Vec<_> = ShardPhase::all()
            .into_iter()
            .filter(|p| p.class() == ShardClass::Settled)
            .collect();
        let terminal: Vec<_> = ShardPhase::all()
            .into_iter()
            .filter(|p| p.class() == ShardClass::Terminal)
            .collect();
        assert_eq!(settled, vec![ShardPhase::Owned]);
        assert_eq!(terminal, vec![ShardPhase::Released]);
    }

    #[test]
    fn no_traps() {
        for p in ShardPhase::all() {
            if p.class().is_good_terminal() {
                continue;
            }
            // a phase-changing exit that isn't only the LeaseLost reset
            let exit = SHARD_TRANSITIONS
                .iter()
                .any(|t| t.from == p && t.trigger != ShardTrigger::LeaseLost && t.to != p);
            assert!(exit, "TRAP: shard phase `{p}` has no non-LeaseLost exit");
        }
    }

    #[test]
    fn every_phase_reaches_a_good_terminal() {
        let mut adj: BTreeMap<String, Vec<ShardPhase>> = BTreeMap::new();
        for tr in SHARD_TRANSITIONS.iter() {
            if tr.from != tr.to {
                adj.entry(tr.from.to_string()).or_default().push(tr.to);
            }
        }
        for start in ShardPhase::all() {
            let mut seen: BTreeSet<String> = BTreeSet::new();
            let mut q: VecDeque<ShardPhase> = VecDeque::new();
            q.push_back(start);
            seen.insert(start.to_string());
            let mut good = start.class().is_good_terminal();
            while let Some(cur) = q.pop_front() {
                if cur.class().is_good_terminal() {
                    good = true;
                    break;
                }
                if let Some(ns) = adj.get(&cur.to_string()) {
                    for &n in ns {
                        if seen.insert(n.to_string()) {
                            q.push_back(n);
                        }
                    }
                }
            }
            assert!(
                good,
                "NON-CONVERGENT shard phase `{start}` reaches no good terminal"
            );
        }
    }

    /// Comfort: every active phase can safely reset on a lost lease (the
    /// sharding drain-safe property — a replica failure never strands a
    /// workspace), and every non-terminal phase has a phase-changing exit.
    #[test]
    fn every_active_phase_is_safely_resettable() {
        for p in ShardPhase::all() {
            let edges: Vec<&ShardTransition> =
                SHARD_TRANSITIONS.iter().filter(|t| t.from == p).collect();
            if matches!(
                p,
                ShardPhase::Claiming | ShardPhase::Owned | ShardPhase::Draining
            ) {
                assert!(
                    edges.iter().any(|t| t.trigger == ShardTrigger::LeaseLost && t.to == ShardPhase::Unassigned),
                    "shard phase `{p}` cannot safely reset on LeaseLost — a replica failure would strand it"
                );
            }
            if !p.class().is_good_terminal() {
                assert!(
                    edges.iter().any(|t| t.to != p),
                    "shard phase `{p}` has no phase-changing exit"
                );
            }
        }
    }

    #[test]
    fn edge_legality_mirrors_table() {
        for tr in SHARD_TRANSITIONS.iter() {
            assert!(
                tr.from.edge_is_legal(tr.to),
                "table edge {} → {} illegal",
                tr.from,
                tr.to
            );
        }
        assert!(ShardPhase::Owned.edge_is_legal(ShardPhase::Owned));
        assert!(
            !ShardPhase::Unassigned.edge_is_legal(ShardPhase::Owned),
            "Unassigned → Owned must require Claiming"
        );
    }

    #[test]
    fn illegal_transition_has_great_error_stack() {
        let err = ShardPhase::Owned
            .advance(ShardTrigger::ClaimWon)
            .unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("from `Owned`"), "{msg}");
        assert!(msg.contains("`ClaimWon`"), "{msg}");
        assert!(msg.contains("Legal from `Owned`"), "{msg}");
    }

    #[test]
    fn happy_path() {
        assert_eq!(P::Unassigned.advance(T::ClaimStarted).unwrap(), P::Claiming);
        assert_eq!(P::Claiming.advance(T::ClaimWon).unwrap(), P::Owned);
        assert_eq!(
            P::Owned.advance(T::RebalanceRequested).unwrap(),
            P::Draining
        );
        assert_eq!(P::Draining.advance(T::DrainComplete).unwrap(), P::Released);
        assert_eq!(P::Released.advance(T::Reassign).unwrap(), P::Unassigned);
        // safe reset from anywhere active
        assert_eq!(P::Owned.advance(T::LeaseLost).unwrap(), P::Unassigned);
        assert_eq!(P::Claiming.advance(T::ClaimLost).unwrap(), P::Unassigned);
    }
}
