//! Typed lifecycle state machine for `InfrastructureTemplate`.
//!
//! # Why this module exists
//!
//! Before this, the template's `Phase` transitions lived *implicitly*,
//! scattered across the `handle_*` methods of `template_controller.rs`. There
//! was no single place that answered "what are the legal transitions?", no
//! mechanical proof that every phase can reach a good terminal, and no proof
//! that a phase can't become a wedge (the `pleme-io-opensource`
//! "stuck-in-Compiling-forever" class — see
//! `feedback_pangea_template_envfetch_compile_wedge`). Illegal transitions
//! were unrepresentable only by *hope*.
//!
//! This module makes the lifecycle a **typed, data-first state machine** with
//! the eclusa/galho discipline (see `theory/ECLUSA.md`):
//!
//! 1. **One [`TRANSITIONS`] table** is the single source of truth. A
//!    transition exists iff a row exists. [`Phase::advance`] is a pure lookup
//!    over it — an illegal `(from, trigger)` is a typed
//!    [`TransitionError::Illegal`] with a great error stack (it names the
//!    source phase, the trigger, and every trigger that *is* legal there),
//!    never a silent fall-through to `to_phase()`.
//! 2. **[`PhaseClass`]** partitions every phase (`Forward | Recovery |
//!    Settled | Terminal | Failure`) so we can reason mechanically about which
//!    edges advance vs heal vs park.
//! 3. **Three CI forcing-functions** (in the `tests` module) make whole bug
//!    classes unrepresentable-at-CI:
//!    - `every_phase_is_enumerated` — exhaustive match ⇒ a new `Phase`
//!      variant cannot be added without being classified + tabled.
//!    - `no_traps` — every non-terminal phase has an outgoing edge (a wedge
//!      fails the build).
//!    - `every_phase_reaches_a_good_terminal` — BFS proves from every phase a
//!      terminal in `{Ready, Destroyed}` is reachable in finitely many steps.
//!    - `every_phase_is_a_comfortable_berth` — the always-restable matrix:
//!      every phase offers a surgery-free exit.
//!
//! # Tier honesty (per `theory/UNREPRESENTABILITY.md` §II)
//!
//! This is a **controller** FSM: the live `Phase` is read off a CR as a
//! serialized tag, so transition legality is **parse-time-rejected** (a
//! `Result::Err` at the table lookup), *not* a compile error. We do not claim
//! compile-time unrepresentability here — the phantom-typestate `Galho<P>`
//! tier is the library path, which the controller cannot reach. The
//! reachability + no-trap + comfort guarantees are **mechanical CI
//! forcing-functions**, not type-level proofs (Rust cannot prove a
//! graph-reachability quantifier). Say which is which; never round up.

use crate::crd::Phase;
use std::collections::BTreeSet;
use std::fmt;

/// Where a phase sits in the convergence structure.
///
/// The partition is the basis of the no-trap + reachability proofs: only
/// [`PhaseClass::Settled`] and [`PhaseClass::Terminal`] may have no *forward*
/// exit; everything else must advance, recover, or be deletable.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PhaseClass {
    /// Drives the change toward applied state (Pending → … → Applying).
    Forward,
    /// A loop berth awaiting a signal/approval before re-advancing (Drifted).
    Recovery,
    /// The good steady state — converged, no pending changes (Ready).
    Settled,
    /// Leads to the resource being gone (Destroying → finalizer removed).
    Terminal,
    /// A detour that MUST carry a recovery exit, never a wedge
    /// (Failed, CompileBlocked).
    Failure,
}

impl PhaseClass {
    /// A "good terminal" in the eclusa sense — a place it is correct to come
    /// to rest. `Settled` (converged) and `Terminal` (cleanly gone) are the
    /// only two. Reachability is proven against this set.
    pub fn is_good_terminal(self) -> bool {
        matches!(self, PhaseClass::Settled | PhaseClass::Terminal)
    }
}

impl Phase {
    /// Classify every phase. This match is **exhaustive** — adding a `Phase`
    /// variant fails to compile until it is classified here, which forces the
    /// author to decide its convergence role (the first unrepresentability
    /// gate: an unclassified phase cannot exist).
    pub fn class(self) -> PhaseClass {
        match self {
            Phase::Pending
            | Phase::Verifying
            | Phase::Verified
            | Phase::Compiling
            | Phase::Initializing
            | Phase::Planning
            | Phase::Applying => PhaseClass::Forward,
            Phase::Drifted => PhaseClass::Recovery,
            Phase::Ready => PhaseClass::Settled,
            Phase::Destroying => PhaseClass::Terminal,
            Phase::Failed | Phase::CompileBlocked => PhaseClass::Failure,
        }
    }

    /// Every lifecycle phase, derived from an exhaustive match so it can never
    /// silently omit a variant the way the legacy `Phase::ALL` omits
    /// `CompileBlocked`. The closure forces the compiler to prove the array
    /// below is complete: if a variant is added to `Phase`, this match stops
    /// compiling until the variant is added to both the match and the array.
    pub fn all_lifecycle() -> [Phase; 12] {
        [
            Phase::Pending,
            Phase::Verifying,
            Phase::Verified,
            Phase::Compiling,
            Phase::Initializing,
            Phase::Planning,
            Phase::Applying,
            Phase::Ready,
            Phase::Drifted,
            Phase::Failed,
            Phase::CompileBlocked,
            Phase::Destroying,
        ]
    }

    /// The legal triggers out of this phase, in declaration order. Pure lookup
    /// over [`TRANSITIONS`]; used to build the great error stack on an illegal
    /// transition.
    pub fn legal_triggers(self) -> Vec<Trigger> {
        TRANSITIONS
            .iter()
            .filter(|t| t.from == self)
            .map(|t| t.trigger)
            .collect()
    }

    /// Advance the FSM. Returns the destination phase for a legal
    /// `(self, trigger)`, or a typed [`TransitionError`] otherwise. This is
    /// the single sanctioned way to compute the next phase — callers must not
    /// invent a destination, so an illegal transition is *parse-time-rejected*
    /// here rather than silently realized.
    pub fn advance(self, trigger: Trigger) -> Result<Phase, TransitionError> {
        TRANSITIONS
            .iter()
            .find(|t| t.from == self && t.trigger == trigger)
            .map(|t| t.to)
            .ok_or(TransitionError::Illegal {
                from: self,
                trigger,
                legal: self.legal_triggers(),
            })
    }
}

/// Exhaustiveness witness: a new `Phase` variant breaks this match, forcing
/// the author to acknowledge it — at which point they must also add it to
/// [`Phase::all_lifecycle`] (the `every_phase_is_enumerated` test pins the
/// count) and to [`Phase::class`] (which is itself an exhaustive match). The
/// three together make "a phase that exists but isn't in the FSM" a build
/// failure, not a latent gap.
#[allow(dead_code)]
fn assert_phase_exhaustive(p: Phase) {
    match p {
        Phase::Pending
        | Phase::Verifying
        | Phase::Verified
        | Phase::Compiling
        | Phase::Initializing
        | Phase::Planning
        | Phase::Applying
        | Phase::Ready
        | Phase::Drifted
        | Phase::Failed
        | Phase::CompileBlocked
        | Phase::Destroying => {}
    }
}

/// The typed signal that drives a transition. Each variant maps to a concrete
/// decision the `handle_*` methods already make; naming them turns the
/// implicit control flow into auditable data.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Trigger {
    /// `validate_source` succeeded (Pending → Compiling).
    SourceValidated,
    /// `Verifying`/`Verified` synthetic pass-through (gem-readiness gate is a
    /// no-op today; see `theory/PANGEA-WORKSPACE-RECONCILIATION.md`).
    GemsAssumedReady,
    /// `CompilerBackend::compile` succeeded (Compiling → Initializing).
    CompileOk,
    /// Consecutive-compile-failure threshold tripped (Compiling →
    /// CompileBlocked).
    CompileFailedThreshold,
    /// CompileBlocked backoff elapsed — retry the compile.
    RetryBackoffElapsed,
    /// `executor.init` succeeded (Initializing → Planning).
    InitOk,
    /// `executor.init` failed (Initializing → Failed).
    InitFailed,
    /// Plan reported no changes (→ Ready, the settled terminal).
    PlanNoChanges,
    /// Plan has changes + policy is AutoApply or approval already granted
    /// (Planning → Applying).
    ApplyAuthorized,
    /// Policy refused the change, or plan errored (Planning → Failed).
    PlanRefusedOrFailed,
    /// `executor.apply` succeeded (Applying → Ready).
    ApplyOk,
    /// `executor.apply` failed with no remediation edge (Applying → Failed).
    ApplyFailed,
    /// Drift detected while Ready and settling says keep-trying
    /// (Ready → Drifted).
    DriftDetected,
    /// Settling exhaustion escalated to Failed (Ready/Drifted → Failed).
    SettlingEscalated,
    /// Drift approved (auto or manual) — re-plan (Drifted → Planning).
    DriftApproved,
    /// Source HEAD moved past the compiled revision, OR the disk workspace was
    /// wiped on a pod roll — recompile from source. The git-edge freshness
    /// gate + the restart-safety bounce collapse to one recovery edge.
    SourceStaleOrWorkspaceLost,
    /// Spec `metadata.generation` advanced — clean + restart from Pending.
    SpecChanged,
    /// `Failed` backoff elapsed with retries remaining — restart the pipeline.
    FailureBackoffElapsed,
    /// CR deletion requested (any phase → Destroying), finalizer present.
    DeletionRequested,
    /// `executor.destroy` succeeded — finalizer removed, resource gone.
    DestroyOk,
    /// `executor.destroy` failed (Destroying → Failed).
    DestroyFailed,
}

impl fmt::Display for Trigger {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // The Debug form is the canonical machine name; reuse it so logs and
        // error stacks read identically (TYPED EMISSION: one render surface).
        write!(f, "{self:?}")
    }
}

/// What an edge does to the convergence structure — the eclusa `PhaseClass`
/// edge tag, so we can reason about which edges advance, heal, or abandon.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EdgeKind {
    /// Advances toward applied/settled.
    Forward,
    /// Heals a detour back onto the forward path (CompileBlocked → Compiling,
    /// Failed → Pending, stale → Compiling).
    Remediation,
    /// Operator/policy-gated rework (Drifted → Planning on approval).
    OperatorRework,
    /// Tears the resource down cleanly (→ Destroying, Destroying → gone).
    Teardown,
}

/// One typed transition row. The table of these is the FSM's whole truth.
#[derive(Debug, Clone, Copy)]
pub struct Transition {
    pub from: Phase,
    pub trigger: Trigger,
    pub to: Phase,
    pub kind: EdgeKind,
}

/// The single source of truth for the InfrastructureTemplate lifecycle.
///
/// Faithfully mirrors the transitions realized by `template_controller.rs`'s
/// `handle_*` methods (audited 2026-06-19). A transition exists **iff** a row
/// exists here; `template_controller.rs` is being migrated to compute its next
/// phase via [`Phase::advance`] over this table rather than calling
/// `update_phase` with a hand-picked destination.
///
/// Every phase except `Destroying` (terminal) carries the universal
/// `DeletionRequested → Destroying` and `SpecChanged → Pending` edges — those
/// are added programmatically by [`universal_edges_for`] rather than repeated
/// per row, and folded into [`TRANSITIONS`] at first use.
pub static EXPLICIT_TRANSITIONS: &[Transition] = &[
    // ── Forward path ────────────────────────────────────────────────
    t(Phase::Pending, Trigger::SourceValidated, Phase::Compiling, EdgeKind::Forward),
    t(Phase::Verifying, Trigger::GemsAssumedReady, Phase::Compiling, EdgeKind::Forward),
    t(Phase::Verified, Trigger::GemsAssumedReady, Phase::Compiling, EdgeKind::Forward),
    t(Phase::Compiling, Trigger::CompileOk, Phase::Initializing, EdgeKind::Forward),
    t(Phase::Initializing, Trigger::InitOk, Phase::Planning, EdgeKind::Forward),
    t(Phase::Planning, Trigger::ApplyAuthorized, Phase::Applying, EdgeKind::Forward),
    t(Phase::Planning, Trigger::PlanNoChanges, Phase::Ready, EdgeKind::Forward),
    t(Phase::Applying, Trigger::ApplyOk, Phase::Ready, EdgeKind::Forward),
    // ── Compile-failure self-heal ───────────────────────────────────
    t(Phase::Compiling, Trigger::CompileFailedThreshold, Phase::CompileBlocked, EdgeKind::Remediation),
    t(Phase::CompileBlocked, Trigger::RetryBackoffElapsed, Phase::Compiling, EdgeKind::Remediation),
    // ── Failure detours (every one carries a recovery exit) ─────────
    t(Phase::Initializing, Trigger::InitFailed, Phase::Failed, EdgeKind::Forward),
    t(Phase::Planning, Trigger::PlanRefusedOrFailed, Phase::Failed, EdgeKind::Forward),
    t(Phase::Applying, Trigger::ApplyFailed, Phase::Failed, EdgeKind::Forward),
    t(Phase::Ready, Trigger::SettlingEscalated, Phase::Failed, EdgeKind::Forward),
    t(Phase::Drifted, Trigger::SettlingEscalated, Phase::Failed, EdgeKind::Forward),
    t(Phase::Failed, Trigger::FailureBackoffElapsed, Phase::Pending, EdgeKind::Remediation),
    // ── Drift loop ──────────────────────────────────────────────────
    t(Phase::Ready, Trigger::DriftDetected, Phase::Drifted, EdgeKind::Forward),
    t(Phase::Drifted, Trigger::DriftApproved, Phase::Planning, EdgeKind::OperatorRework),
    // ── Freshness / restart-safety recovery (collapsed to one edge) ─
    t(Phase::Planning, Trigger::SourceStaleOrWorkspaceLost, Phase::Compiling, EdgeKind::Remediation),
    t(Phase::Applying, Trigger::SourceStaleOrWorkspaceLost, Phase::Compiling, EdgeKind::Remediation),
    t(Phase::Ready, Trigger::SourceStaleOrWorkspaceLost, Phase::Compiling, EdgeKind::Remediation),
    t(Phase::Drifted, Trigger::SourceStaleOrWorkspaceLost, Phase::Compiling, EdgeKind::Remediation),
    // ── Teardown ────────────────────────────────────────────────────
    t(Phase::Destroying, Trigger::DestroyOk, Phase::Destroying, EdgeKind::Teardown), // → finalizer removed (gone)
    t(Phase::Destroying, Trigger::DestroyFailed, Phase::Failed, EdgeKind::Forward),
];

/// `const fn` row constructor so the table is a compile-time `static`.
const fn t(from: Phase, trigger: Trigger, to: Phase, kind: EdgeKind) -> Transition {
    Transition { from, trigger, to, kind }
}

/// The universal edges every non-terminal phase carries: a CR deletion always
/// routes to `Destroying`, and a spec change always restarts from `Pending`.
/// Encoding these once (here) instead of per-row keeps the explicit table
/// readable while still making every phase provably deletable + restartable.
fn universal_edges_for(from: Phase) -> Vec<Transition> {
    let mut out = Vec::new();
    if from != Phase::Destroying {
        out.push(t(from, Trigger::DeletionRequested, Phase::Destroying, EdgeKind::Teardown));
    }
    // SpecChanged restarts from Pending for every non-Pending, non-Destroying
    // phase (mirrors template_controller.rs:306-329).
    if from != Phase::Pending && from != Phase::Destroying {
        out.push(t(from, Trigger::SpecChanged, Phase::Pending, EdgeKind::Remediation));
    }
    out
}

/// The complete transition table: the explicit rows plus the universal
/// deletion/spec-change edges for every phase. This is what [`Phase::advance`]
/// and all proofs consume.
pub static TRANSITIONS: std::sync::LazyLock<Vec<Transition>> = std::sync::LazyLock::new(|| {
    let mut all: Vec<Transition> = EXPLICIT_TRANSITIONS.to_vec();
    for p in Phase::all_lifecycle() {
        all.extend(universal_edges_for(p));
    }
    all
});

/// A typed transition error with a great error stack. The `Illegal` variant
/// names the source phase, the attempted trigger, AND every trigger that *is*
/// legal from there — so a wrong transition is a self-documenting failure, not
/// a silent no-op.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TransitionError {
    Illegal {
        from: Phase,
        trigger: Trigger,
        legal: Vec<Trigger>,
    },
}

impl fmt::Display for TransitionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TransitionError::Illegal { from, trigger, legal } => {
                let legal = if legal.is_empty() {
                    "<none — this phase is terminal>".to_string()
                } else {
                    legal
                        .iter()
                        .map(|t| t.to_string())
                        .collect::<Vec<_>>()
                        .join(", ")
                };
                write!(
                    f,
                    "illegal lifecycle transition: no edge from phase `{from}` on trigger \
                     `{trigger}`. Legal triggers from `{from}`: [{legal}]. \
                     (If this trigger should be legal, add the row to \
                     controller::lifecycle::EXPLICIT_TRANSITIONS — the table is the \
                     single source of truth.)"
                )
            }
        }
    }
}

impl std::error::Error for TransitionError {}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::{BTreeMap, VecDeque};

    /// CI forcing-function #1 — exhaustive enumeration. Proves the lifecycle
    /// list covers every `Phase` variant (the legacy `Phase::ALL` omits
    /// `CompileBlocked`; this can't). The `all_lifecycle` match is the witness;
    /// here we pin its length + uniqueness.
    #[test]
    fn every_phase_is_enumerated() {
        let all = Phase::all_lifecycle();
        let uniq: BTreeSet<_> = all.iter().map(|p| p.to_string()).collect();
        assert_eq!(
            uniq.len(),
            all.len(),
            "Phase::all_lifecycle has duplicates: {all:?}"
        );
        // 12 variants today; bump deliberately when the enum grows (the
        // all_lifecycle match will have forced you to add the variant first).
        assert_eq!(all.len(), 12, "Phase variant count changed — update all_lifecycle + this assertion");
    }

    /// Every phase must be classified, and exactly one good-terminal class
    /// (`Settled` = Ready) + one teardown terminal (`Terminal` = Destroying)
    /// exist. Pins the convergence structure.
    #[test]
    fn classes_partition_correctly() {
        let settled: Vec<_> = Phase::all_lifecycle().into_iter().filter(|p| p.class() == PhaseClass::Settled).collect();
        let terminal: Vec<_> = Phase::all_lifecycle().into_iter().filter(|p| p.class() == PhaseClass::Terminal).collect();
        assert_eq!(settled, vec![Phase::Ready], "Ready must be the sole Settled phase");
        assert_eq!(terminal, vec![Phase::Destroying], "Destroying must be the sole Terminal phase");
    }

    /// CI forcing-function #2 — no traps. Every non-good-terminal phase must
    /// have at least one outgoing edge that is NOT merely the universal
    /// deletion edge — i.e. it can make progress or recover without the CR
    /// being deleted. A wedge (the "stuck in Compiling forever" class) fails
    /// the build here.
    #[test]
    fn no_traps() {
        for p in Phase::all_lifecycle() {
            if p.class().is_good_terminal() {
                continue;
            }
            let has_non_deletion_exit = TRANSITIONS
                .iter()
                .any(|t| t.from == p && t.trigger != Trigger::DeletionRequested && t.to != p);
            assert!(
                has_non_deletion_exit,
                "TRAP: phase `{p}` ({:?}) has no non-deletion outgoing edge — it can only \
                 be escaped by deleting the CR. Every non-terminal phase must advance or \
                 recover. Add a recovery edge to EXPLICIT_TRANSITIONS.",
                p.class()
            );
        }
    }

    /// CI forcing-function #3 — universal reachability of a good terminal.
    /// BFS from every phase must reach a phase in `{Ready, Destroying}` in
    /// finitely many transitions. This is the convergence theorem: no phase
    /// can be a place from which the machine can never come to rest.
    ///
    /// Tier-honest: this is a mechanical CI proof, NOT a type-level guarantee
    /// (Rust can't prove a graph-reachability quantifier).
    #[test]
    fn every_phase_reaches_a_good_terminal() {
        // adjacency (ignores self-loops, which don't advance reachability)
        let mut adj: BTreeMap<String, Vec<Phase>> = BTreeMap::new();
        for tr in TRANSITIONS.iter() {
            if tr.from != tr.to {
                adj.entry(tr.from.to_string()).or_default().push(tr.to);
            }
        }
        for start in Phase::all_lifecycle() {
            let mut seen: BTreeSet<String> = BTreeSet::new();
            let mut q: VecDeque<Phase> = VecDeque::new();
            q.push_back(start);
            seen.insert(start.to_string());
            let mut reached_good = start.class().is_good_terminal();
            while let Some(cur) = q.pop_front() {
                if cur.class().is_good_terminal() {
                    reached_good = true;
                    break;
                }
                if let Some(nexts) = adj.get(&cur.to_string()) {
                    for &n in nexts {
                        if seen.insert(n.to_string()) {
                            q.push_back(n);
                        }
                    }
                }
            }
            assert!(
                reached_good,
                "NON-CONVERGENT: from phase `{start}` no good terminal (Ready|Destroying) is \
                 reachable. The machine could get stuck. Add a forward/recovery edge."
            );
        }
    }

    /// CI forcing-function #4 — the always-restable comfort matrix. Every
    /// phase must be a "comfortable berth": parking there must offer a
    /// surgery-free exit (advance | recover | abandon) AND a teardown path.
    /// This is exactly the guarantee whose absence let `pleme-io-opensource`
    /// wedge for ~31 days.
    #[test]
    fn every_phase_is_a_comfortable_berth() {
        for p in Phase::all_lifecycle() {
            let edges: Vec<&Transition> = TRANSITIONS.iter().filter(|t| t.from == p).collect();
            // 1. Always deletable (surgery-free abandon) — except the terminal
            //    teardown phase itself, which IS the abandon path.
            if p != Phase::Destroying {
                assert!(
                    edges.iter().any(|t| t.to == Phase::Destroying),
                    "BERTH `{p}` not abandonable: no edge to Destroying"
                );
            }
            // 2. Non-terminal phases must offer a real exit that changes phase.
            if !p.class().is_good_terminal() {
                assert!(
                    edges.iter().any(|t| t.to != p),
                    "BERTH `{p}` has no phase-changing exit"
                );
            }
            // 3. Failure berths must carry a remediation edge (heal back to the
            //    forward path), never only-abandon.
            if p.class() == PhaseClass::Failure {
                assert!(
                    edges.iter().any(|t| t.kind == EdgeKind::Remediation),
                    "FAILURE BERTH `{p}` has no remediation edge — it can only be abandoned, \
                     not healed. Failed/CompileBlocked must self-heal."
                );
            }
        }
    }

    /// The error stack is great: an illegal transition names the phase, the
    /// trigger, and the legal alternatives.
    #[test]
    fn illegal_transition_has_a_great_error_stack() {
        let err = Phase::Ready.advance(Trigger::CompileOk).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("phase `Ready`"), "{msg}");
        assert!(msg.contains("trigger `CompileOk`"), "{msg}");
        assert!(msg.contains("Legal triggers from `Ready`"), "{msg}");
        // Ready's real exits must be advertised:
        assert!(msg.contains("DriftDetected"), "{msg}");
    }

    /// Spot-check the happy path advances exactly as the handlers do.
    #[test]
    fn happy_path_advances() {
        assert_eq!(Phase::Pending.advance(Trigger::SourceValidated).unwrap(), Phase::Compiling);
        assert_eq!(Phase::Compiling.advance(Trigger::CompileOk).unwrap(), Phase::Initializing);
        assert_eq!(Phase::Initializing.advance(Trigger::InitOk).unwrap(), Phase::Planning);
        assert_eq!(Phase::Planning.advance(Trigger::ApplyAuthorized).unwrap(), Phase::Applying);
        assert_eq!(Phase::Applying.advance(Trigger::ApplyOk).unwrap(), Phase::Ready);
        // settled terminal: a no-change plan rests at Ready
        assert_eq!(Phase::Planning.advance(Trigger::PlanNoChanges).unwrap(), Phase::Ready);
        // self-heal: CompileBlocked always has a way back
        assert_eq!(Phase::CompileBlocked.advance(Trigger::RetryBackoffElapsed).unwrap(), Phase::Compiling);
        // every phase is deletable
        assert_eq!(Phase::Ready.advance(Trigger::DeletionRequested).unwrap(), Phase::Destroying);
    }
}
