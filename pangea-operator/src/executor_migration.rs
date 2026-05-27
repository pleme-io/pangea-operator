//! `executor_migration` — typed state machine for migrating one
//! workspace's IaC executor from **tofu** to **magma**, orchestrated at
//! the Kubernetes API layer.
//!
//! This is the *conductor* that sits on top of the executor *substrate*
//! (`executor::backend_select::ExecutorBackend` + `controller::
//! ControllerState::executor_for`). The substrate answers "which executor
//! runs this reconcile right now"; this FSM answers "how does a workspace
//! travel from tofu to magma safely, observably, and reversibly."
//!
//! # Design
//!
//! Pure by construction: [`step`] is `(&State, &Event, &Policy) ->
//! Transition` with no I/O. The controller feeds it events derived from
//! each reconcile (parity results, magma cycle outcomes, approvals) and
//! interprets the returned [`Effect`]s (set the authoritative executor,
//! run a shadow parity plan, emit a receipt, alert, …). Same shape as
//! `magma_fsm::LifecycleState` and the engenho `maquina::StateMachine`
//! pattern: an explicit transition table, total over `(state, event)`,
//! never panics. Unexpected pairs are identity transitions so a stray
//! event never takes a workload down.
//!
//! # The lifeline invariant
//!
//! Workspaces that carry the access path to a cluster we can only reach
//! over SSH (labelled `pangea.pleme.io/lifeline=rio-ssh`) set
//! [`MigrationPolicy::lifeline`]. For them the machine refuses to
//! auto-cut-over (cut-over is manual-approval only) and runs a
//! zero-divergence budget (a single parity divergence rolls straight back
//! to `Pinned`). "We can't lose SSH to rio" is encoded here as a typed
//! guard, not a hope.
//!
//! Normative spec: `theory/EXECUTOR-MIGRATION.md`.

use std::fmt;

// ── Phase ──────────────────────────────────────────────────────────

/// Where a workspace sits on its tofu→magma journey.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MigrationPhase {
    /// tofu is authoritative; no magma activity. Initial + safe-rest
    /// state. Lifeline workspaces live here until a human advances them.
    Pinned,
    /// tofu is authoritative AND magma runs a dry plan every cycle; the
    /// operator diffs the two typed plans into a parity receipt. No magma
    /// writes — zero blast radius.
    Shadow,
    /// Parity has held for `policy.parity_threshold` consecutive cycles;
    /// the workspace is eligible to cut over. Resting state only when
    /// cut-over is gated (lifeline or `auto_cutover=false`).
    Held,
    /// magma is authoritative; tofu is the one-flip fallback. State is
    /// shared (same PG rows, tofu-format), so no re-create storm.
    Cutover,
    /// magma has reconciled cleanly for `policy.verify_cycles` cycles.
    /// Terminal success — but still guarded (a later anomaly rolls back).
    Verified,
    /// Reverted to tofu after an anomaly or abort. Carries the reason.
    /// An operator `Acknowledge` returns it to `Pinned` for a retry.
    RolledBack,
}

impl MigrationPhase {
    /// Which executor is authoritative in this phase. The controller's
    /// `executor_for` honors this.
    pub fn authoritative(self) -> Authoritative {
        match self {
            MigrationPhase::Cutover | MigrationPhase::Verified => Authoritative::Magma,
            _ => Authoritative::Tofu,
        }
    }

    /// A terminal *resting* phase needs no further driving until an
    /// external event (approval, anomaly, abort) arrives.
    pub fn is_resting(self) -> bool {
        matches!(
            self,
            MigrationPhase::Pinned | MigrationPhase::Held | MigrationPhase::Verified | MigrationPhase::RolledBack
        )
    }
}

impl fmt::Display for MigrationPhase {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            MigrationPhase::Pinned => "Pinned",
            MigrationPhase::Shadow => "Shadow",
            MigrationPhase::Held => "Held",
            MigrationPhase::Cutover => "Cutover",
            MigrationPhase::Verified => "Verified",
            MigrationPhase::RolledBack => "RolledBack",
        };
        f.write_str(s)
    }
}

/// The executor a phase implies should run reconciles.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Authoritative {
    Tofu,
    Magma,
}

// ── Events ─────────────────────────────────────────────────────────

/// Outcome of a shadow parity check: does magma's typed plan match
/// tofu's plan for the same input + state?
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParityResult {
    /// `normalize(Plan_tofu) ≡ normalize(Plan_magma)`.
    Clean,
    /// The two executors disagree on the plan — a real backend divergence.
    Diverged,
}

/// Outcome of a magma-authoritative reconcile cycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CycleResult {
    /// Apply succeeded; drift (if any) was auto-corrected within policy.
    Clean,
    /// Something the live process must not absorb silently.
    Anomaly(AnomalyKind),
}

/// Why a magma cycle is an anomaly. Mirrors `magma_drift` decisions plus
/// hard failures.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AnomalyKind {
    /// `magma_apply::run_plan` returned a failure.
    ApplyFailed,
    /// `magma_drift` classified a change as `Refuse`.
    DriftRefused,
    /// `magma_drift` classified a change as `RequireApproval`.
    DriftRequiresApproval,
    /// The failed-cycle error budget was exhausted.
    ErrorBudgetBreach,
}

/// Why a migration was aborted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AbortReason {
    /// A human (or the fleet conductor) called it off.
    Manual,
    /// A budget/SLO breach upstream.
    BudgetBreach,
}

/// Reason recorded on a `RolledBack` phase.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RollbackReason {
    Anomaly(AnomalyKind),
    Aborted(AbortReason),
}

/// Events that drive the migration FSM. The controller derives these
/// from each reconcile + from the `ExecutorMigration` CRD / approvals.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MigrationEvent {
    /// Begin migrating this workspace (selected by the fleet conductor or
    /// a per-template `spec.executorMigration`). `Pinned → Shadow`.
    Enroll,
    /// A shadow parity check completed.
    Parity(ParityResult),
    /// Cut-over was approved (auto when `Held && auto_cutover && !lifeline`,
    /// otherwise a human/conductor decision).
    CutoverApproved,
    /// A magma-authoritative cycle completed.
    MagmaCycle(CycleResult),
    /// Abort the migration. Reverts to tofu if magma was authoritative.
    Abort(AbortReason),
    /// Operator acknowledged a `RolledBack` state; return to `Pinned`.
    Acknowledge,
}

// ── Policy (the guards) ────────────────────────────────────────────

/// Per-workspace migration policy. The transition guards read this.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MigrationPolicy {
    /// `K` — consecutive clean parity checks required to reach `Held`.
    pub parity_threshold: u32,
    /// `M` — consecutive clean magma cycles required to reach `Verified`.
    pub verify_cycles: u32,
    /// Cut over automatically once `Held` (ignored for lifeline workspaces,
    /// which are always manual).
    pub auto_cutover: bool,
    /// Parity divergences tolerated in `Shadow` before rolling back to
    /// `Pinned`. Lifeline workspaces force this to 0.
    pub divergence_budget: u32,
    /// SSH-lifeline workspace: manual-only cut-over, zero divergence
    /// tolerance.
    pub lifeline: bool,
}

impl MigrationPolicy {
    /// Conservative fleet default: 5 clean parity cycles to become
    /// eligible, manual cut-over, 3 clean magma cycles to verify, tolerate
    /// 1 transient divergence.
    pub fn conservative_default() -> Self {
        Self {
            parity_threshold: 5,
            verify_cycles: 3,
            auto_cutover: false,
            divergence_budget: 1,
            lifeline: false,
        }
    }

    /// Lifeline preset: the SSH-to-rio invariant. Longer parity soak,
    /// never auto-cut-over, zero divergence tolerance.
    pub fn lifeline() -> Self {
        Self {
            parity_threshold: 20,
            verify_cycles: 10,
            auto_cutover: false,
            divergence_budget: 0,
            lifeline: true,
        }
    }

    /// Effective divergence budget — always 0 for lifeline regardless of
    /// the configured value.
    fn effective_divergence_budget(&self) -> u32 {
        if self.lifeline {
            0
        } else {
            self.divergence_budget
        }
    }

    /// May this workspace cut over without a human? Never for lifeline.
    fn may_auto_cutover(&self) -> bool {
        self.auto_cutover && !self.lifeline
    }
}

// ── State ──────────────────────────────────────────────────────────

/// The full migration state for one workspace — mirrors
/// `status.executorMigration` on the InfrastructureTemplate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MigrationState {
    pub phase: MigrationPhase,
    /// Consecutive clean parity checks accrued in `Shadow`.
    pub parity_streak: u32,
    /// Parity divergences seen in the current `Shadow` episode.
    pub divergence_count: u32,
    /// Consecutive clean magma cycles accrued in `Cutover`.
    pub verified_cycles: u32,
    /// Set when `phase == RolledBack`.
    pub rolled_back_reason: Option<RollbackReason>,
}

impl Default for MigrationState {
    fn default() -> Self {
        Self {
            phase: MigrationPhase::Pinned,
            parity_streak: 0,
            divergence_count: 0,
            verified_cycles: 0,
            rolled_back_reason: None,
        }
    }
}

impl MigrationState {
    /// A freshly-pinned workspace.
    pub fn pinned() -> Self {
        Self::default()
    }

    fn rolled_back(reason: RollbackReason) -> Self {
        Self {
            phase: MigrationPhase::RolledBack,
            parity_streak: 0,
            divergence_count: 0,
            verified_cycles: 0,
            rolled_back_reason: Some(reason),
        }
    }
}

// ── Effects ────────────────────────────────────────────────────────

/// Side effects the controller performs after a transition. The FSM is
/// pure; effects are the only way it talks to the world.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Effect {
    /// Make this executor authoritative for the workspace's reconciles.
    SetAuthoritative(Authoritative),
    /// Run a dry magma plan this cycle and diff it against tofu's plan.
    RunShadowParity,
    /// Persist the parity receipt (BLAKE3-chained) into status/audit.
    EmitParityReceipt,
    /// Surface a cut-over approval request (lifeline / manual gate).
    RequestCutoverApproval,
    /// Record an anomaly for alerting + the audit chain.
    EmitAnomaly(AnomalyKind),
    /// Fire a routing alert (ntfy/Slack/GitHub) with this message.
    Alert(&'static str),
    /// Mark the migration verified-complete.
    RecordVerified,
}

// ── Transition ─────────────────────────────────────────────────────

/// Result of one [`step`]: the next state + the effects to perform.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Transition {
    pub next: MigrationState,
    pub effects: Vec<Effect>,
}

impl Transition {
    fn to(next: MigrationState, effects: Vec<Effect>) -> Self {
        Self { next, effects }
    }

    /// Identity: unchanged state, no effects (undefined / no-op pairs).
    fn identity(state: &MigrationState) -> Self {
        Self {
            next: *state,
            effects: Vec::new(),
        }
    }
}

// ── The transition function ────────────────────────────────────────

/// Advance the migration FSM. Pure + total: every `(state, event)` pair
/// returns a `Transition`; undefined pairs are identity (no-op) so a
/// stray event can never corrupt state or panic.
pub fn step(state: &MigrationState, event: &MigrationEvent, policy: &MigrationPolicy) -> Transition {
    use MigrationEvent::*;
    use MigrationPhase::*;

    // Abort is handled uniformly from any phase.
    if let Abort(reason) = event {
        return match state.phase {
            // tofu still authoritative — just stop, no rollback needed.
            Pinned | Shadow | Held => Transition::to(
                MigrationState::pinned(),
                vec![Effect::SetAuthoritative(Authoritative::Tofu)],
            ),
            // magma authoritative — revert.
            Cutover | Verified => Transition::to(
                MigrationState::rolled_back(RollbackReason::Aborted(*reason)),
                vec![
                    Effect::SetAuthoritative(Authoritative::Tofu),
                    Effect::Alert("executor migration aborted — reverted to tofu"),
                ],
            ),
            RolledBack => Transition::identity(state),
        };
    }

    match (state.phase, event) {
        // ── Pinned ──────────────────────────────────────────────
        (Pinned, Enroll) => {
            let next = MigrationState {
                phase: Shadow,
                ..MigrationState::pinned()
            };
            Transition::to(
                next,
                vec![Effect::SetAuthoritative(Authoritative::Tofu), Effect::RunShadowParity],
            )
        }

        // ── Shadow: accrue parity, watch divergences ────────────
        (Shadow, Parity(ParityResult::Clean)) => {
            let streak = state.parity_streak + 1;
            if streak >= policy.parity_threshold {
                // Eligible. Auto-cut-over only when policy allows and the
                // workspace is not a lifeline.
                if policy.may_auto_cutover() {
                    Transition::to(
                        MigrationState {
                            phase: Cutover,
                            verified_cycles: 0,
                            ..*state
                        },
                        vec![Effect::EmitParityReceipt, Effect::SetAuthoritative(Authoritative::Magma)],
                    )
                } else {
                    Transition::to(
                        MigrationState {
                            phase: Held,
                            parity_streak: streak,
                            ..*state
                        },
                        vec![Effect::EmitParityReceipt, Effect::RequestCutoverApproval],
                    )
                }
            } else {
                Transition::to(
                    MigrationState {
                        parity_streak: streak,
                        ..*state
                    },
                    vec![Effect::EmitParityReceipt],
                )
            }
        }
        (Shadow, Parity(ParityResult::Diverged)) => {
            let divergences = state.divergence_count + 1;
            if divergences > policy.effective_divergence_budget() {
                // Budget blown — stop shadowing, return to safe rest.
                Transition::to(
                    MigrationState::pinned(),
                    vec![
                        Effect::EmitAnomaly(AnomalyKind::ApplyFailed),
                        Effect::Alert("parity divergence budget exceeded — pinned to tofu"),
                        Effect::SetAuthoritative(Authoritative::Tofu),
                    ],
                )
            } else {
                Transition::to(
                    MigrationState {
                        parity_streak: 0,
                        divergence_count: divergences,
                        ..*state
                    },
                    vec![Effect::EmitParityReceipt],
                )
            }
        }

        // ── Held: eligible, awaiting cut-over decision ──────────
        (Held, CutoverApproved) => Transition::to(
            MigrationState {
                phase: Cutover,
                verified_cycles: 0,
                ..*state
            },
            vec![Effect::SetAuthoritative(Authoritative::Magma)],
        ),
        // A regression while held drops back to shadowing.
        (Held, Parity(ParityResult::Diverged)) => Transition::to(
            MigrationState {
                phase: Shadow,
                parity_streak: 0,
                divergence_count: state.divergence_count + 1,
                ..*state
            },
            vec![Effect::EmitAnomaly(AnomalyKind::ApplyFailed), Effect::RunShadowParity],
        ),
        (Held, Parity(ParityResult::Clean)) => Transition::identity(state),

        // ── Cutover: magma authoritative, accruing verification ─
        (Cutover, MagmaCycle(CycleResult::Clean)) => {
            let verified = state.verified_cycles + 1;
            if verified >= policy.verify_cycles {
                Transition::to(
                    MigrationState {
                        phase: Verified,
                        verified_cycles: verified,
                        ..*state
                    },
                    vec![Effect::RecordVerified],
                )
            } else {
                Transition::to(
                    MigrationState {
                        verified_cycles: verified,
                        ..*state
                    },
                    Vec::new(),
                )
            }
        }
        (Cutover, MagmaCycle(CycleResult::Anomaly(kind))) => Transition::to(
            MigrationState::rolled_back(RollbackReason::Anomaly(*kind)),
            vec![
                Effect::SetAuthoritative(Authoritative::Tofu),
                Effect::EmitAnomaly(*kind),
                Effect::Alert("magma cycle anomaly — rolled back to tofu"),
            ],
        ),

        // ── Verified: done, but still guarded ───────────────────
        (Verified, MagmaCycle(CycleResult::Anomaly(kind))) => Transition::to(
            MigrationState::rolled_back(RollbackReason::Anomaly(*kind)),
            vec![
                Effect::SetAuthoritative(Authoritative::Tofu),
                Effect::EmitAnomaly(*kind),
                Effect::Alert("post-verification anomaly — rolled back to tofu"),
            ],
        ),
        (Verified, MagmaCycle(CycleResult::Clean)) => Transition::identity(state),

        // ── RolledBack: await operator acknowledgement ──────────
        (RolledBack, Acknowledge) => Transition::to(
            MigrationState::pinned(),
            vec![Effect::SetAuthoritative(Authoritative::Tofu)],
        ),

        // ── Everything else is a no-op ──────────────────────────
        _ => Transition::identity(state),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn auto() -> MigrationPolicy {
        MigrationPolicy {
            parity_threshold: 3,
            verify_cycles: 2,
            auto_cutover: true,
            divergence_budget: 1,
            lifeline: false,
        }
    }

    fn manual() -> MigrationPolicy {
        MigrationPolicy {
            auto_cutover: false,
            ..auto()
        }
    }

    /// Drive a sequence of events and return the final state.
    fn run(start: MigrationState, events: &[MigrationEvent], policy: &MigrationPolicy) -> MigrationState {
        events.iter().fold(start, |s, e| step(&s, e, policy).next)
    }

    #[test]
    fn enroll_starts_shadowing_under_tofu() {
        let t = step(&MigrationState::pinned(), &MigrationEvent::Enroll, &manual());
        assert_eq!(t.next.phase, MigrationPhase::Shadow);
        assert_eq!(t.next.phase.authoritative(), Authoritative::Tofu);
        assert!(t.effects.contains(&Effect::RunShadowParity));
    }

    #[test]
    fn parity_streak_reaches_held_under_manual_policy() {
        let p = manual();
        let s = run(
            MigrationState { phase: MigrationPhase::Shadow, ..MigrationState::pinned() },
            &[
                MigrationEvent::Parity(ParityResult::Clean),
                MigrationEvent::Parity(ParityResult::Clean),
                MigrationEvent::Parity(ParityResult::Clean),
            ],
            &p,
        );
        assert_eq!(s.phase, MigrationPhase::Held);
        assert_eq!(s.parity_streak, 3);
        // Still tofu — Held has not cut over.
        assert_eq!(s.phase.authoritative(), Authoritative::Tofu);
    }

    #[test]
    fn auto_cutover_skips_held_and_goes_magma() {
        let p = auto();
        let start = MigrationState { phase: MigrationPhase::Shadow, ..MigrationState::pinned() };
        // 3rd clean parity meets threshold → straight to Cutover.
        let t = step(
            &MigrationState { parity_streak: 2, ..start },
            &MigrationEvent::Parity(ParityResult::Clean),
            &p,
        );
        assert_eq!(t.next.phase, MigrationPhase::Cutover);
        assert_eq!(t.next.phase.authoritative(), Authoritative::Magma);
        assert!(t.effects.contains(&Effect::SetAuthoritative(Authoritative::Magma)));
    }

    #[test]
    fn held_cutover_then_verify() {
        let p = manual();
        let s = run(
            MigrationState { phase: MigrationPhase::Held, parity_streak: 3, ..MigrationState::pinned() },
            &[
                MigrationEvent::CutoverApproved,
                MigrationEvent::MagmaCycle(CycleResult::Clean),
                MigrationEvent::MagmaCycle(CycleResult::Clean),
            ],
            &p,
        );
        assert_eq!(s.phase, MigrationPhase::Verified);
        assert_eq!(s.verified_cycles, 2);
    }

    #[test]
    fn anomaly_during_cutover_rolls_back_to_tofu() {
        let p = manual();
        let t = step(
            &MigrationState { phase: MigrationPhase::Cutover, ..MigrationState::pinned() },
            &MigrationEvent::MagmaCycle(CycleResult::Anomaly(AnomalyKind::ApplyFailed)),
            &p,
        );
        assert_eq!(t.next.phase, MigrationPhase::RolledBack);
        assert_eq!(t.next.phase.authoritative(), Authoritative::Tofu);
        assert_eq!(t.next.rolled_back_reason, Some(RollbackReason::Anomaly(AnomalyKind::ApplyFailed)));
        assert!(t.effects.contains(&Effect::SetAuthoritative(Authoritative::Tofu)));
    }

    #[test]
    fn verified_still_rolls_back_on_late_anomaly() {
        let p = manual();
        let t = step(
            &MigrationState { phase: MigrationPhase::Verified, verified_cycles: 2, ..MigrationState::pinned() },
            &MigrationEvent::MagmaCycle(CycleResult::Anomaly(AnomalyKind::DriftRefused)),
            &p,
        );
        assert_eq!(t.next.phase, MigrationPhase::RolledBack);
        assert_eq!(t.next.rolled_back_reason, Some(RollbackReason::Anomaly(AnomalyKind::DriftRefused)));
    }

    #[test]
    fn lifeline_never_auto_cuts_over_even_with_auto_policy() {
        // auto_cutover=true but lifeline=true → must still land in Held,
        // never Cutover, and request manual approval.
        let p = MigrationPolicy { lifeline: true, ..auto() };
        let start = MigrationState { phase: MigrationPhase::Shadow, parity_streak: 2, ..MigrationState::pinned() };
        let t = step(&start, &MigrationEvent::Parity(ParityResult::Clean), &p);
        assert_eq!(t.next.phase, MigrationPhase::Held, "lifeline must not auto-cut-over");
        assert_eq!(t.next.phase.authoritative(), Authoritative::Tofu);
        assert!(t.effects.contains(&Effect::RequestCutoverApproval));
    }

    #[test]
    fn lifeline_single_divergence_pins_immediately() {
        // Zero divergence budget for lifeline: the first divergence in
        // Shadow returns to Pinned (SSH path protected).
        let p = MigrationPolicy { lifeline: true, divergence_budget: 5, ..auto() };
        let start = MigrationState { phase: MigrationPhase::Shadow, parity_streak: 7, ..MigrationState::pinned() };
        let t = step(&start, &MigrationEvent::Parity(ParityResult::Diverged), &p);
        assert_eq!(t.next.phase, MigrationPhase::Pinned, "lifeline tolerates zero divergence");
        assert_eq!(t.next.phase.authoritative(), Authoritative::Tofu);
        assert!(t.effects.contains(&Effect::SetAuthoritative(Authoritative::Tofu)));
    }

    #[test]
    fn divergence_within_budget_resets_streak_but_stays_shadow() {
        let p = auto(); // budget 1
        let start = MigrationState { phase: MigrationPhase::Shadow, parity_streak: 2, divergence_count: 0, ..MigrationState::pinned() };
        let t = step(&start, &MigrationEvent::Parity(ParityResult::Diverged), &p);
        assert_eq!(t.next.phase, MigrationPhase::Shadow);
        assert_eq!(t.next.parity_streak, 0);
        assert_eq!(t.next.divergence_count, 1);
    }

    #[test]
    fn divergence_over_budget_pins() {
        let p = auto(); // budget 1
        let start = MigrationState { phase: MigrationPhase::Shadow, divergence_count: 1, ..MigrationState::pinned() };
        let t = step(&start, &MigrationEvent::Parity(ParityResult::Diverged), &p);
        assert_eq!(t.next.phase, MigrationPhase::Pinned);
    }

    #[test]
    fn abort_from_cutover_reverts_but_abort_from_shadow_just_pins() {
        let p = manual();
        // From Shadow: still tofu, so just pin (no rollback reason).
        let from_shadow = step(
            &MigrationState { phase: MigrationPhase::Shadow, ..MigrationState::pinned() },
            &MigrationEvent::Abort(AbortReason::Manual),
            &p,
        );
        assert_eq!(from_shadow.next.phase, MigrationPhase::Pinned);
        assert_eq!(from_shadow.next.rolled_back_reason, None);
        // From Cutover: magma was live, so revert + record.
        let from_cutover = step(
            &MigrationState { phase: MigrationPhase::Cutover, ..MigrationState::pinned() },
            &MigrationEvent::Abort(AbortReason::Manual),
            &p,
        );
        assert_eq!(from_cutover.next.phase, MigrationPhase::RolledBack);
        assert_eq!(from_cutover.next.rolled_back_reason, Some(RollbackReason::Aborted(AbortReason::Manual)));
    }

    #[test]
    fn rolledback_acknowledge_returns_to_pinned() {
        let p = manual();
        let t = step(
            &MigrationState::rolled_back(RollbackReason::Anomaly(AnomalyKind::ApplyFailed)),
            &MigrationEvent::Acknowledge,
            &p,
        );
        assert_eq!(t.next.phase, MigrationPhase::Pinned);
        assert_eq!(t.next.rolled_back_reason, None);
    }

    #[test]
    fn step_is_total_undefined_pairs_are_identity() {
        let p = manual();
        // CutoverApproved while Pinned is meaningless → no-op.
        let t = step(&MigrationState::pinned(), &MigrationEvent::CutoverApproved, &p);
        assert_eq!(t.next, MigrationState::pinned());
        assert!(t.effects.is_empty());
        // MagmaCycle while Shadow (magma isn't authoritative) → no-op.
        let s = MigrationState { phase: MigrationPhase::Shadow, parity_streak: 1, ..MigrationState::pinned() };
        let t2 = step(&s, &MigrationEvent::MagmaCycle(CycleResult::Clean), &p);
        assert_eq!(t2.next, s);
        assert!(t2.effects.is_empty());
    }

    #[test]
    fn full_happy_path_pinned_to_verified() {
        let p = manual();
        let mut s = MigrationState::pinned();
        s = step(&s, &MigrationEvent::Enroll, &p).next; // → Shadow
        for _ in 0..3 {
            s = step(&s, &MigrationEvent::Parity(ParityResult::Clean), &p).next;
        }
        assert_eq!(s.phase, MigrationPhase::Held);
        s = step(&s, &MigrationEvent::CutoverApproved, &p).next; // → Cutover
        assert_eq!(s.phase.authoritative(), Authoritative::Magma);
        for _ in 0..2 {
            s = step(&s, &MigrationEvent::MagmaCycle(CycleResult::Clean), &p).next;
        }
        assert_eq!(s.phase, MigrationPhase::Verified);
    }
}
