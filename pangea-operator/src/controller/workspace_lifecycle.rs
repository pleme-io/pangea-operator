//! Typed convergence state machine for a **workspace** — the reconcile / state
//! / apply seam.
//!
//! # Why this scope exists
//!
//! A `WorkspaceCatalog` today is a `verified: bool` + a policy cascade. But the
//! workspace is where four boundaries coincide — state isolation (one
//! `PangeaNamespace` schema), gem set (`requiredGems`), git source, and the
//! policy cascade — which makes it the natural unit of **scaling** and
//! **failure isolation**. This module promotes the workspace from a flag to a
//! first-class typed convergence controller: it owns
//!
//! - **gem-env readiness** for its templates (a per-workspace ruby env — the
//!   structural cure for cross-workspace `$LOAD_PATH` shadowing),
//! - the **per-workspace concurrency budget** under which its templates
//!   reconcile (so a wedged workspace cannot starve the others),
//! - the **template dependency DAG** ordering (independent templates run in
//!   parallel; cross-referencing templates serialize),
//! - **drain-safe handoff** when a shard lease moves between replicas.
//!
//! It is the per-workspace peer of [`super::lifecycle`] (per-template): same
//! eclusa/galho discipline — one typed [`WS_TRANSITIONS`] table, a
//! [`WorkspacePhase`] partition into convergence classes, and the four CI
//! forcing-functions (enumeration, no-trap, reachability, always-restable
//! comfort) that make a wedged or non-convergent workspace a build failure.
//!
//! # Tier honesty (`theory/UNREPRESENTABILITY.md` §II)
//!
//! Like the template FSM this is a controller reading phase off a CR, so
//! transition legality is **parse-time-rejected** (a `Result::Err`), not a
//! compile error; reachability / no-trap / comfort are **mechanical CI
//! forcing-functions**, not type-level proofs. Never rounded up.

use std::fmt;

/// Convergence phases of a workspace.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkspacePhase {
    /// Assigned to this replica but nothing loaded yet.
    Unloaded,
    /// Cloning + loading + smoke-testing the workspace's `requiredGems` into a
    /// per-workspace ruby env.
    LoadingGems,
    /// A required gem failed to load or smoke-test — templates must NOT compile
    /// against a half-loaded env (the gate the dead `Verifying`/`Verified`
    /// template stubs never enforced). Failure berth: self-heals by retrying.
    GemsFailed,
    /// Gems loaded + smoke-passed; templates may reconcile.
    Ready,
    /// Templates are reconciling (some not yet settled) under the per-workspace
    /// budget + dependency-DAG order. The active steady-work state.
    Converging,
    /// Every template is settled (Ready). The good steady state.
    Settled,
    /// One or more templates are stuck (Failed/CompileBlocked) past policy.
    /// Failure berth: recovers when the offending templates heal.
    Degraded,
    /// The shard lease is being released (rebalance / scale-down). Templates
    /// are parked at their comfortable berths; no new work dispatched.
    Draining,
    /// Lease released — this replica no longer owns the workspace. Terminal
    /// (another replica picks it up from `Unloaded`).
    Released,
}

impl fmt::Display for WorkspacePhase {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

/// Convergence role of a workspace phase (mirrors `lifecycle::PhaseClass`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WsClass {
    /// Advances toward serving templates.
    Forward,
    /// A loop berth re-converging on template drift.
    Recovery,
    /// The good steady state (all templates settled).
    Settled,
    /// Cleanly handed off (lease released).
    Terminal,
    /// A detour that must carry a recovery edge (GemsFailed, Degraded).
    Failure,
}

impl WsClass {
    pub fn is_good_terminal(self) -> bool {
        matches!(self, WsClass::Settled | WsClass::Terminal)
    }
}

impl WorkspacePhase {
    /// Exhaustive classification — a new phase fails to compile until classified.
    pub fn class(self) -> WsClass {
        match self {
            WorkspacePhase::Unloaded
            | WorkspacePhase::LoadingGems
            | WorkspacePhase::Ready
            | WorkspacePhase::Converging => WsClass::Forward,
            // Settled re-enters Converging on drift → its convergence role is
            // the good steady state; Converging is the working loop.
            WorkspacePhase::Settled => WsClass::Settled,
            WorkspacePhase::Released => WsClass::Terminal,
            WorkspacePhase::GemsFailed | WorkspacePhase::Degraded => WsClass::Failure,
            WorkspacePhase::Draining => WsClass::Forward, // forward → Released
        }
    }

    pub fn all() -> [WorkspacePhase; 9] {
        [
            WorkspacePhase::Unloaded,
            WorkspacePhase::LoadingGems,
            WorkspacePhase::GemsFailed,
            WorkspacePhase::Ready,
            WorkspacePhase::Converging,
            WorkspacePhase::Settled,
            WorkspacePhase::Degraded,
            WorkspacePhase::Draining,
            WorkspacePhase::Released,
        ]
    }

    pub fn legal_triggers(self) -> Vec<WsTrigger> {
        WS_TRANSITIONS
            .iter()
            .filter(|t| t.from == self)
            .map(|t| t.trigger)
            .collect()
    }

    pub fn advance(self, trigger: WsTrigger) -> Result<WorkspacePhase, WsTransitionError> {
        WS_TRANSITIONS
            .iter()
            .find(|t| t.from == self && t.trigger == trigger)
            .map(|t| t.to)
            .ok_or(WsTransitionError::Illegal {
                from: self,
                trigger,
                legal: self.legal_triggers(),
            })
    }

    /// Runtime guard (same role as `lifecycle::Phase::edge_is_legal`).
    pub fn edge_is_legal(self, to: WorkspacePhase) -> bool {
        self == to || WS_TRANSITIONS.iter().any(|t| t.from == self && t.to == to)
    }
}

/// Exhaustiveness witness — a new phase breaks this match.
#[allow(dead_code)]
fn assert_ws_exhaustive(p: WorkspacePhase) {
    match p {
        WorkspacePhase::Unloaded
        | WorkspacePhase::LoadingGems
        | WorkspacePhase::GemsFailed
        | WorkspacePhase::Ready
        | WorkspacePhase::Converging
        | WorkspacePhase::Settled
        | WorkspacePhase::Degraded
        | WorkspacePhase::Draining
        | WorkspacePhase::Released => {}
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WsTrigger {
    /// Start loading the workspace's requiredGems.
    GemLoadStarted,
    /// All requiredGems loaded + smoke-passed.
    GemsLoaded,
    /// A required gem failed to load/smoke-test.
    GemLoadFailed,
    /// Retry gem loading after backoff.
    GemRetry,
    /// The workspace's gem source moved (a requiredGem ref advanced) — reload
    /// the per-workspace env (the per-workspace gem-freshness edge).
    GemSourceChanged,
    /// Templates dispatched for reconcile (Ready → Converging).
    TemplatesDispatched,
    /// Every template reached a settled phase.
    AllTemplatesSettled,
    /// A settled workspace saw a template drift — re-converge.
    TemplateDrift,
    /// Templates stuck past policy (→ Degraded).
    TemplatesDegraded,
    /// The stuck templates healed (Degraded → Converging).
    DegradedRecovered,
    /// The shard lease is being released — drain.
    DrainRequested,
    /// Draining finished (in-flight parked) — release the lease.
    Drained,
}

impl fmt::Display for WsTrigger {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WsEdgeKind {
    Forward,
    Remediation,
    OperatorRework,
    Teardown,
}

#[derive(Debug, Clone, Copy)]
pub struct WsTransition {
    pub from: WorkspacePhase,
    pub trigger: WsTrigger,
    pub to: WorkspacePhase,
    pub kind: WsEdgeKind,
}

const fn w(
    from: WorkspacePhase,
    trigger: WsTrigger,
    to: WorkspacePhase,
    kind: WsEdgeKind,
) -> WsTransition {
    WsTransition {
        from,
        trigger,
        to,
        kind,
    }
}

use WorkspacePhase as P;
use WsEdgeKind as K;
use WsTrigger as T;

/// Explicit workspace transitions. The universal `DrainRequested → Draining`
/// edge (every non-terminal phase is drain-safe — the shard can hand off from
/// anywhere) is added programmatically in [`WS_TRANSITIONS`].
pub static WS_EXPLICIT: &[WsTransition] = &[
    w(P::Unloaded, T::GemLoadStarted, P::LoadingGems, K::Forward),
    w(P::LoadingGems, T::GemsLoaded, P::Ready, K::Forward),
    w(P::LoadingGems, T::GemLoadFailed, P::GemsFailed, K::Forward),
    w(P::GemsFailed, T::GemRetry, P::LoadingGems, K::Remediation),
    w(P::Ready, T::TemplatesDispatched, P::Converging, K::Forward),
    w(
        P::Converging,
        T::AllTemplatesSettled,
        P::Settled,
        K::Forward,
    ),
    w(P::Converging, T::TemplatesDegraded, P::Degraded, K::Forward),
    w(
        P::Degraded,
        T::DegradedRecovered,
        P::Converging,
        K::Remediation,
    ),
    w(P::Settled, T::TemplateDrift, P::Converging, K::Forward),
    // per-workspace gem freshness: a moved requiredGem reloads the env from any
    // serving phase (Ready/Converging/Settled/Degraded → LoadingGems).
    w(
        P::Ready,
        T::GemSourceChanged,
        P::LoadingGems,
        K::Remediation,
    ),
    w(
        P::Converging,
        T::GemSourceChanged,
        P::LoadingGems,
        K::Remediation,
    ),
    w(
        P::Settled,
        T::GemSourceChanged,
        P::LoadingGems,
        K::Remediation,
    ),
    w(
        P::Degraded,
        T::GemSourceChanged,
        P::LoadingGems,
        K::Remediation,
    ),
    // teardown
    w(P::Draining, T::Drained, P::Released, K::Teardown),
];

fn ws_universal(from: WorkspacePhase) -> Vec<WsTransition> {
    // Every phase except the terminal/draining ones can begin draining (a
    // lease handoff can be requested from any serving or failure berth).
    if from != WorkspacePhase::Draining && from != WorkspacePhase::Released {
        vec![w(
            from,
            WsTrigger::DrainRequested,
            WorkspacePhase::Draining,
            WsEdgeKind::Teardown,
        )]
    } else {
        Vec::new()
    }
}

pub static WS_TRANSITIONS: std::sync::LazyLock<Vec<WsTransition>> =
    std::sync::LazyLock::new(|| {
        let mut all = WS_EXPLICIT.to_vec();
        for p in WorkspacePhase::all() {
            all.extend(ws_universal(p));
        }
        all
    });

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WsTransitionError {
    Illegal {
        from: WorkspacePhase,
        trigger: WsTrigger,
        legal: Vec<WsTrigger>,
    },
}

impl fmt::Display for WsTransitionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            WsTransitionError::Illegal {
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
                    "illegal workspace transition: no edge from `{from}` on `{trigger}`. \
                     Legal from `{from}`: [{legal}]. (Add the row to \
                     controller::workspace_lifecycle::WS_EXPLICIT.)"
                )
            }
        }
    }
}

impl std::error::Error for WsTransitionError {}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::{BTreeMap, BTreeSet, VecDeque};

    #[test]
    fn every_phase_enumerated() {
        let all = WorkspacePhase::all();
        let uniq: BTreeSet<_> = all.iter().map(|p| p.to_string()).collect();
        assert_eq!(uniq.len(), all.len());
        assert_eq!(all.len(), 9);
    }

    #[test]
    fn classes_partition() {
        let settled: Vec<_> = WorkspacePhase::all()
            .into_iter()
            .filter(|p| p.class() == WsClass::Settled)
            .collect();
        let terminal: Vec<_> = WorkspacePhase::all()
            .into_iter()
            .filter(|p| p.class() == WsClass::Terminal)
            .collect();
        assert_eq!(settled, vec![WorkspacePhase::Settled]);
        assert_eq!(terminal, vec![WorkspacePhase::Released]);
    }

    #[test]
    fn no_traps() {
        for p in WorkspacePhase::all() {
            if p.class().is_good_terminal() {
                continue;
            }
            let exit = WS_TRANSITIONS
                .iter()
                .any(|t| t.from == p && t.trigger != WsTrigger::DrainRequested && t.to != p);
            assert!(exit, "TRAP: workspace phase `{p}` has no non-drain exit");
        }
    }

    #[test]
    fn every_phase_reaches_a_good_terminal() {
        let mut adj: BTreeMap<String, Vec<WorkspacePhase>> = BTreeMap::new();
        for tr in WS_TRANSITIONS.iter() {
            if tr.from != tr.to {
                adj.entry(tr.from.to_string()).or_default().push(tr.to);
            }
        }
        for start in WorkspacePhase::all() {
            let mut seen: BTreeSet<String> = BTreeSet::new();
            let mut q: VecDeque<WorkspacePhase> = VecDeque::new();
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
                "NON-CONVERGENT workspace phase `{start}` reaches no good terminal"
            );
        }
    }

    #[test]
    fn every_phase_is_a_comfortable_berth() {
        for p in WorkspacePhase::all() {
            let edges: Vec<&WsTransition> = WS_TRANSITIONS.iter().filter(|t| t.from == p).collect();
            // drain-safe from every non-terminal phase (shard handoff)
            if p != WorkspacePhase::Draining && p != WorkspacePhase::Released {
                assert!(
                    edges.iter().any(|t| t.to == WorkspacePhase::Draining),
                    "BERTH `{p}` not drainable — a shard handoff couldn't park here"
                );
            }
            if !p.class().is_good_terminal() {
                assert!(
                    edges.iter().any(|t| t.to != p),
                    "BERTH `{p}` has no phase-changing exit"
                );
            }
            if p.class() == WsClass::Failure {
                assert!(
                    edges.iter().any(|t| t.kind == WsEdgeKind::Remediation),
                    "FAILURE BERTH `{p}` has no remediation edge"
                );
            }
        }
    }

    #[test]
    fn edge_legality_mirrors_table() {
        for tr in WS_TRANSITIONS.iter() {
            assert!(
                tr.from.edge_is_legal(tr.to),
                "table edge {} → {} illegal",
                tr.from,
                tr.to
            );
        }
        assert!(WorkspacePhase::Settled.edge_is_legal(WorkspacePhase::Settled));
        assert!(!WorkspacePhase::Unloaded.edge_is_legal(WorkspacePhase::Settled));
    }

    #[test]
    fn illegal_transition_has_great_error_stack() {
        let err = WorkspacePhase::Settled
            .advance(WsTrigger::GemsLoaded)
            .unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("from `Settled`"), "{msg}");
        assert!(msg.contains("`GemsLoaded`"), "{msg}");
        assert!(msg.contains("Legal from `Settled`"), "{msg}");
    }

    #[test]
    fn happy_path() {
        assert_eq!(
            P::Unloaded.advance(T::GemLoadStarted).unwrap(),
            P::LoadingGems
        );
        assert_eq!(P::LoadingGems.advance(T::GemsLoaded).unwrap(), P::Ready);
        assert_eq!(
            P::Ready.advance(T::TemplatesDispatched).unwrap(),
            P::Converging
        );
        assert_eq!(
            P::Converging.advance(T::AllTemplatesSettled).unwrap(),
            P::Settled
        );
        assert_eq!(P::Settled.advance(T::TemplateDrift).unwrap(), P::Converging);
        // failure self-heals
        assert_eq!(P::GemsFailed.advance(T::GemRetry).unwrap(), P::LoadingGems);
        assert_eq!(
            P::Degraded.advance(T::DegradedRecovered).unwrap(),
            P::Converging
        );
        // drainable from anywhere
        assert_eq!(
            P::Converging.advance(T::DrainRequested).unwrap(),
            P::Draining
        );
        assert_eq!(P::Draining.advance(T::Drained).unwrap(), P::Released);
    }
}
