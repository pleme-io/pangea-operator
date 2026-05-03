//! Post-reconcile pipeline — single entry point for hooks that fire
//! AFTER a reconcile cycle has updated status (success or failure).
//!
//! Counterpart to `pre_reconcile_pipeline` (S2). Today the pipeline is
//! just one stage — `reactive::apply_policy` (failure escalation /
//! phase timeout / verified-blocked) — but it's wrapped in a typed
//! entry point so future post-reconcile hooks land in one obvious
//! place rather than re-scattering across phase handlers.
//!
//! Stages, in order:
//!
//!   1. ReactivePolicy evaluation. Looks at consecutive failures,
//!      stuck phases, persistent verified-blocked, fires escalation
//!      actions (Alert / Suspend / Page). Documented at
//!      `controller::reactive`.
//!
//! Stages NOT in this pipeline (intentional):
//!
//!   * `controller::policy_cascade` — fires DURING plan/apply phase
//!     handlers, gates whether a specific change is allowed. Per-
//!     change policy is correctly scoped to the phase handlers, not
//!     a post-reconcile sweep.
//!   * Status patches — those happen INSIDE phase handlers because
//!     they need phase-specific data.

use crate::controller::ControllerState;
use crate::crd::InfrastructureTemplate;
use crate::error::Result;
use tracing::warn;

/// Run the post-reconcile pipeline for an `InfrastructureTemplate`.
///
/// Called at the end of every status update path that bumps the
/// failure or stuck-phase clocks. Best-effort: hook failures are
/// logged at warn but never fail the surrounding reconcile (the
/// reconcile already succeeded by the time we got here).
pub async fn run_for_template(
    template: &InfrastructureTemplate,
    state: &ControllerState,
) {
    if let Err(e) = run_reactive(template, state).await {
        warn!(error = %e, "post-reconcile reactive policy stage failed (non-fatal)");
    }
}

/// Stage 1 — ReactivePolicy. Wraps the existing
/// `template_controller::apply_reactive_policy` shape; pulled out
/// here so the pipeline reads as an explicit list of stages rather
/// than scattering pluck-from-controller calls.
async fn run_reactive(
    template: &InfrastructureTemplate,
    state: &ControllerState,
) -> Result<()> {
    super::template_controller::apply_reactive_policy_internal(template, state).await
}
