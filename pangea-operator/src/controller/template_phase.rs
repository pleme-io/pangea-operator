//! Per-phase typed dispatch for `InfrastructureTemplate` reconcile.
//!
//! S1's lighter touch (`dispatch_template_phase`) extracted the match
//! out of `reconcile_template`. D2 (this module) takes the next step:
//! each phase becomes a `ReconcilePhase` trait impl on a unit struct.
//! The dispatch is now type-driven instead of match-arm-driven, which
//! enables:
//!
//!   * Per-phase tests in their own modules. `PendingPhase::handle`
//!     can be unit-tested without spinning up the entire reconcile.
//!   * A stable `name()` accessor that consistent observability wraps
//!     can use (tracing spans, structured logs, metric labels).
//!   * Future per-phase metadata (e.g., default requeue interval,
//!     allowed-from set) attached to the trait impl alongside the
//!     handler.
//!
//! The actual phase logic still lives in `template_controller`'s
//! `handle_*` functions — this module is the thin trait shim. Moving
//! the logic into per-phase modules is a future step (file moves, no
//! semantic change). The trait + dispatch is the first half.

use crate::controller::ControllerState;
use crate::crd::{InfrastructureTemplate, Phase};
use crate::error::Result;

#[async_trait::async_trait]
pub trait ReconcilePhase: Send + Sync {
    /// Stable identifier — used in tracing spans, log fields,
    /// metric labels. MUST match the lowercase `Phase` variant
    /// (e.g. `"pending"`, `"compiling"`, …) so dashboards built on
    /// `reconciliation_duration_seconds{phase=...}` (the histogram
    /// keyed by these strings via `Metrics::record_phase_duration`)
    /// continue to slice cleanly.
    fn name(&self) -> &'static str;

    async fn handle(
        &self,
        template: &InfrastructureTemplate,
        state: &ControllerState,
    ) -> Result<crate::controller::reconciler::ReconcileAction>;
}

/// Map a `Phase` enum value to its trait-object handler. Returns
/// `None` for the synthetic Verifying/Verified pair which currently
/// no-ops forward to Compiling — that transition stays inline in
/// `dispatch_template_phase`.
pub fn for_phase(phase: Phase) -> Option<Box<dyn ReconcilePhase>> {
    Some(match phase {
        Phase::Pending => Box::new(PendingPhase),
        Phase::Compiling => Box::new(CompilingPhase),
        Phase::Initializing => Box::new(InitializingPhase),
        Phase::Planning => Box::new(PlanningPhase),
        Phase::Applying => Box::new(ApplyingPhase),
        Phase::Ready => Box::new(ReadyPhase),
        Phase::Drifted => Box::new(DriftedPhase),
        Phase::Failed => Box::new(FailedPhase),
        Phase::CompileBlocked => Box::new(CompileBlockedPhase),
        Phase::Destroying => Box::new(DestroyingPhase),
        Phase::Verifying | Phase::Verified => return None,
    })
}

// ── Phase trait impls — thin wrappers over template_controller's existing handlers ──

pub struct PendingPhase;
#[async_trait::async_trait]
impl ReconcilePhase for PendingPhase {
    fn name(&self) -> &'static str { "pending" }
    async fn handle(
        &self,
        template: &InfrastructureTemplate,
        state: &ControllerState,
    ) -> Result<crate::controller::reconciler::ReconcileAction> {
        super::template_controller::handle_pending_internal(template, state).await
    }
}

pub struct CompilingPhase;
#[async_trait::async_trait]
impl ReconcilePhase for CompilingPhase {
    fn name(&self) -> &'static str { "compiling" }
    async fn handle(
        &self,
        template: &InfrastructureTemplate,
        state: &ControllerState,
    ) -> Result<crate::controller::reconciler::ReconcileAction> {
        super::template_controller::handle_compiling_internal(template, state).await
    }
}

pub struct InitializingPhase;
#[async_trait::async_trait]
impl ReconcilePhase for InitializingPhase {
    fn name(&self) -> &'static str { "initializing" }
    async fn handle(
        &self,
        template: &InfrastructureTemplate,
        state: &ControllerState,
    ) -> Result<crate::controller::reconciler::ReconcileAction> {
        super::template_controller::handle_initializing_internal(template, state).await
    }
}

pub struct PlanningPhase;
#[async_trait::async_trait]
impl ReconcilePhase for PlanningPhase {
    fn name(&self) -> &'static str { "planning" }
    async fn handle(
        &self,
        template: &InfrastructureTemplate,
        state: &ControllerState,
    ) -> Result<crate::controller::reconciler::ReconcileAction> {
        super::template_controller::handle_planning_internal(template, state).await
    }
}

pub struct ApplyingPhase;
#[async_trait::async_trait]
impl ReconcilePhase for ApplyingPhase {
    fn name(&self) -> &'static str { "applying" }
    async fn handle(
        &self,
        template: &InfrastructureTemplate,
        state: &ControllerState,
    ) -> Result<crate::controller::reconciler::ReconcileAction> {
        super::template_controller::handle_applying_internal(template, state).await
    }
}

pub struct ReadyPhase;
#[async_trait::async_trait]
impl ReconcilePhase for ReadyPhase {
    fn name(&self) -> &'static str { "ready" }
    async fn handle(
        &self,
        template: &InfrastructureTemplate,
        state: &ControllerState,
    ) -> Result<crate::controller::reconciler::ReconcileAction> {
        super::template_controller::handle_ready_internal(template, state).await
    }
}

pub struct DriftedPhase;
#[async_trait::async_trait]
impl ReconcilePhase for DriftedPhase {
    fn name(&self) -> &'static str { "drifted" }
    async fn handle(
        &self,
        template: &InfrastructureTemplate,
        state: &ControllerState,
    ) -> Result<crate::controller::reconciler::ReconcileAction> {
        super::template_controller::handle_drifted_internal(template, state).await
    }
}

pub struct FailedPhase;
#[async_trait::async_trait]
impl ReconcilePhase for FailedPhase {
    fn name(&self) -> &'static str { "failed" }
    async fn handle(
        &self,
        template: &InfrastructureTemplate,
        state: &ControllerState,
    ) -> Result<crate::controller::reconciler::ReconcileAction> {
        super::template_controller::handle_failed_internal(template, state).await
    }
}

pub struct CompileBlockedPhase;
#[async_trait::async_trait]
impl ReconcilePhase for CompileBlockedPhase {
    fn name(&self) -> &'static str { "compileblocked" }
    async fn handle(
        &self,
        template: &InfrastructureTemplate,
        state: &ControllerState,
    ) -> Result<crate::controller::reconciler::ReconcileAction> {
        super::template_controller::handle_compile_blocked_internal(template, state).await
    }
}

pub struct DestroyingPhase;
#[async_trait::async_trait]
impl ReconcilePhase for DestroyingPhase {
    fn name(&self) -> &'static str { "destroying" }
    async fn handle(
        &self,
        template: &InfrastructureTemplate,
        state: &ControllerState,
    ) -> Result<crate::controller::reconciler::ReconcileAction> {
        super::template_controller::handle_destroying_internal(template, state).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn for_phase_covers_every_variant() {
        // for_phase returns None only for Verifying/Verified (intentional);
        // all other Phase variants must yield a trait object.
        assert!(for_phase(Phase::Pending).is_some());
        assert!(for_phase(Phase::Compiling).is_some());
        assert!(for_phase(Phase::Initializing).is_some());
        assert!(for_phase(Phase::Planning).is_some());
        assert!(for_phase(Phase::Applying).is_some());
        assert!(for_phase(Phase::Ready).is_some());
        assert!(for_phase(Phase::Drifted).is_some());
        assert!(for_phase(Phase::Failed).is_some());
        assert!(for_phase(Phase::CompileBlocked).is_some());
        assert!(for_phase(Phase::Destroying).is_some());
        // The Verifying/Verified pair is handled inline by the dispatcher.
        assert!(for_phase(Phase::Verifying).is_none());
        assert!(for_phase(Phase::Verified).is_none());
    }

    #[test]
    fn phase_names_stable() {
        // Name accessors must not drift; dashboards depend on the
        // exact strings (lowercase variant).
        assert_eq!(PendingPhase.name(), "pending");
        assert_eq!(CompilingPhase.name(), "compiling");
        assert_eq!(InitializingPhase.name(), "initializing");
        assert_eq!(PlanningPhase.name(), "planning");
        assert_eq!(ApplyingPhase.name(), "applying");
        assert_eq!(ReadyPhase.name(), "ready");
        assert_eq!(DriftedPhase.name(), "drifted");
        assert_eq!(FailedPhase.name(), "failed");
        assert_eq!(CompileBlockedPhase.name(), "compileblocked");
        assert_eq!(DestroyingPhase.name(), "destroying");
    }

    #[test]
    fn name_matches_record_phase_duration_label() {
        // The trait's name() is fed into Metrics::record_phase_duration
        // — must be lowercase to match the existing histogram label
        // values seeded by handle_compiling/initializing/planning/applying
        // in template_controller (C3).
        for phase in [
            Phase::Compiling,
            Phase::Initializing,
            Phase::Planning,
            Phase::Applying,
        ] {
            let p = for_phase(phase).unwrap();
            assert_eq!(p.name(), p.name().to_lowercase());
        }
    }
}
