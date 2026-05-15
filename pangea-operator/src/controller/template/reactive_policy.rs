//! ReactivePolicy escalation stage of the post-reconcile pipeline.
//!
//! Lifted from `template_controller.rs` during T2 (continuation of R6).
//! Resolves the workspace cascade, evaluates whether any reactive
//! policy fires for the current template state, and applies the worst
//! escalation (Healthy condition flip, lastEscalatedAt/Reason fields,
//! autoSuspended=true on Suspend actions, Warning event emission, and
//! routing delivery via ntfy/Slack/GitHub).
//!
//! The "internal" wrapper is a stable cross-module entry point used by
//! `controller::post_reconcile_pipeline::run_for_template`. Phase
//! handlers should call into the pipeline rather than this module
//! directly so future post-reconcile hooks (audit emission,
//! notification routing) can be added in one place.

use crate::controller::ControllerState;
use crate::crd::InfrastructureTemplate;
use crate::error::Result;
use chrono::Utc;
use kube::runtime::events::EventType;
use kube::ResourceExt;

use super::events::record_event;

/// Public wrapper for the reactive-policy stage of the post-reconcile
/// pipeline. The pipeline calls into this; phase handlers should call
/// `controller::post_reconcile_pipeline::run_for_template(...)` to
/// invoke the full pipeline rather than this function directly.
pub async fn apply_reactive_policy_internal(
    template: &InfrastructureTemplate,
    state: &ControllerState,
) -> Result<()> {
    apply_reactive_policy(template, state).await
}

/// Resolve the cascade, evaluate, and apply the worst escalation if
/// any reactive policy fires. Patches `status.lastEscalatedAt`,
/// `status.lastEscalationReason`, the `Healthy` condition, and (for
/// Suspend actions) `status.autoSuspended`. Emits a Warning event +
/// a routing-formatted log line.
///
/// Idempotent across reconciles: when the same reason is already on
/// `status.lastEscalationReason`, we suppress re-emitting the event
/// (debounce). Once the bad state clears, future entries re-emit.
async fn apply_reactive_policy(
    template: &InfrastructureTemplate,
    state: &ControllerState,
) -> Result<()> {
    use crate::controller::reactive::{
        emit_escalation_log, evaluate, track_verified_blocked, workspace_reactive_policy,
        EffectiveReactivePolicy, Escalation,
    };
    use crate::crd::ReactiveAction;

    let workspace_policy = workspace_reactive_policy(&state.client, template).await;
    let effective = EffectiveReactivePolicy::resolve(
        template.spec.reactive_policy.as_ref(),
        workspace_policy.as_ref(),
    );

    // Track verified-blocked clock (start/preserve/clear) — must run
    // every reconcile so the clock is current before evaluate fires.
    let now = Utc::now();
    let new_blocked_since = template
        .status
        .as_ref()
        .and_then(|s| track_verified_blocked(s, now));

    // Compose a synthetic template with verified_blocked_since set,
    // so evaluate sees the freshest clock without us needing to
    // patch first. (We patch after evaluate decides.)
    let mut tmpl_for_eval = template.clone();
    if let Some(s) = tmpl_for_eval.status.as_mut() {
        s.verified_blocked_since = new_blocked_since;
    }

    let escalation = evaluate(&tmpl_for_eval, &effective, now);

    let name = template.name_any();
    let namespace = template.namespace().unwrap_or_default();

    // Healthy condition + tracking field deltas — patched even when
    // not triggered, so the verified-blocked clock + Healthy=True
    // stays current.
    let prior_status = template.status.clone().unwrap_or_default();
    let prior_reason = prior_status.last_escalation_reason.clone();

    let (healthy_status, healthy_reason, healthy_message, escalated_at, escalation_reason, suspend_now) =
        match &escalation {
            Escalation::Healthy => (
                "True",
                "NoEscalations".to_string(),
                "no reactive policies have fired".to_string(),
                prior_status.last_escalated_at,
                None::<String>,
                false,
            ),
            Escalation::Triggered { action, reason, message, routing } => {
                // Debounce: only emit event + log + routing-deliver
                // when the reason is new (or no prior). State-change-
                // driven, not every-reconcile — keeps ntfy from
                // pinging every 5min while the bad state persists.
                let is_new = prior_reason.as_deref() != Some(reason.as_str());
                if is_new {
                    emit_escalation_log(
                        &name,
                        &namespace,
                        *action,
                        reason,
                        message,
                        routing.as_ref(),
                    );
                    let action_label = match action {
                        ReactiveAction::Alert => "Alert",
                        ReactiveAction::Suspend => "Suspend",
                        ReactiveAction::Page => "Page",
                    };
                    let event_msg =
                        format!("[{action_label}] reason={reason}: {message}");
                    record_event(
                        template,
                        state,
                        EventType::Warning,
                        "ReactivePolicyTriggered",
                        &event_msg,
                    )
                    .await;
                    // Real routing delivery — ntfy now, Slack +
                    // GitHub stubbed. Best-effort; failures log but
                    // don't propagate.
                    let title = format!(
                        "[pangea] {action_label}: {namespace}/{name}",
                        namespace = namespace,
                        name = name
                    );
                    state
                        .routing_client
                        .deliver(*action, &title, &event_msg, routing.as_ref())
                        .await;
                }
                (
                    "False",
                    reason.clone(),
                    message.clone(),
                    Some(now),
                    Some(reason.clone()),
                    matches!(action, ReactiveAction::Suspend),
                )
            }
        };

    // Patch only the fields that may have changed.
    let mut conditions = prior_status.conditions.clone();
    // Drop any prior `Healthy` condition; emit a fresh one (preserve
    // transition time when status+reason+message all match).
    let prior_healthy = conditions
        .iter()
        .find(|c| c.r#type == "Healthy")
        .cloned();
    conditions.retain(|c| c.r#type != "Healthy");
    let healthy_transition = match prior_healthy {
        Some(p) if p.status == healthy_status
            && p.reason == healthy_reason
            && p.message == healthy_message =>
        {
            p.last_transition_time
        }
        _ => now,
    };
    conditions.push(crate::crd::Condition {
        r#type: "Healthy".to_string(),
        status: healthy_status.to_string(),
        last_transition_time: healthy_transition,
        reason: healthy_reason,
        message: healthy_message,
    });

    let new_auto_suspended = suspend_now || prior_status.auto_suspended;

    // Diff-gate: skip the PATCH when none of the observable fields
    // would actually change. Without this gate, every template
    // reconcile triggers a status PATCH from the post-reconcile
    // pipeline (this function runs unconditionally for every
    // template), each PATCH bumps `metadata.resourceVersion`, the
    // template controller's watch fires, reconcile runs again →
    // closed loop. The Healthy condition's transition time is
    // already preserved above; comparing the rest of the writes
    // against `prior_status` covers verifiedBlockedSince,
    // autoSuspended, lastEscalatedAt, lastEscalationReason.
    if reactive_policy_status_unchanged(
        &prior_status,
        &conditions,
        new_blocked_since,
        new_auto_suspended,
        escalated_at,
        escalation_reason.as_deref(),
    ) {
        tracing::debug!(
            "ReactivePolicy: no observable status change; skipping patch (avoids self-trigger watch loop)"
        );
        return Ok(());
    }

    let patch = serde_json::json!({
        "status": {
            "conditions": conditions,
            "verifiedBlockedSince": new_blocked_since,
            "autoSuspended": new_auto_suspended,
            "lastEscalatedAt": escalated_at,
            "lastEscalationReason": escalation_reason,
        }
    });
    crate::controller::status_patch::patch_status(template, &state.client, patch)
    .await?;

    // Self-heal: when the worst trigger is a PhaseTimeout for a
    // non-terminal phase, force the phase to Failed so the next
    // reconcile runs `handle_failed` (workspace.clean + reset to
    // Pending). Without this, the operator can sit in a non-terminal
    // phase indefinitely while reactive policy logs Alerts on every
    // reconcile — the rio cluster spent 2026-05-08 → 2026-05-14 stuck
    // in Applying on a stale plan because the Alert-only escalation
    // never punted the phase. Layer 1 (`is_self_healable_apply_error`)
    // catches the specific stale-plan case inline; this layer is the
    // broader net for any future stuck-phase condition.
    //
    // Gated on `is_new_phase_timeout`: only fires the first reconcile
    // after the timeout threshold is crossed, then `escalation_reason`
    // becomes the same value across reconciles and the gate stops it
    // re-firing. Failing-the-failed (template already Failed) is also
    // a no-op since Phase::Failed isn't in the timeout-eligible set.
    if let Escalation::Triggered { reason, .. } = &escalation {
        let is_new_phase_timeout = prior_reason.as_deref() != Some(reason.as_str())
            && reason.starts_with("PhaseTimeout:");
        if is_new_phase_timeout {
            if let Some(current_phase) = template.status.as_ref().and_then(|s| s.phase) {
                if phase_is_force_resettable(current_phase) {
                    let err = format!(
                        "ReactivePolicy: {reason} — forcing phase to Failed so handle_failed can recover (workspace clean + reset to Pending). Triggered after phase stuck past timeout threshold."
                    );
                    let force_patch = serde_json::json!({
                        "status": {
                            "phase": "Failed",
                            "lastError": err,
                            "failureCount": template
                                .status
                                .as_ref()
                                .map(|s| s.failure_count.saturating_add(1))
                                .unwrap_or(1),
                        }
                    });
                    if let Err(e) = crate::controller::status_patch::patch_status(
                        template,
                        &state.client,
                        force_patch,
                    )
                    .await
                    {
                        tracing::warn!(
                            error = %e,
                            "PhaseTimeout force-reset patch failed (non-fatal); next reconcile will retry"
                        );
                    } else {
                        tracing::warn!(
                            %reason,
                            current_phase = %current_phase,
                            "PhaseTimeout escalation: forced phase → Failed for self-heal recovery"
                        );
                        record_event(
                            template,
                            state,
                            EventType::Warning,
                            "PhaseTimeoutForceReset",
                            &err,
                        )
                        .await;
                    }
                }
            }
        }
    }
    Ok(())
}

/// Whether a `Phase` is one we will forcibly reset to `Failed` when a
/// `PhaseTimeout:*` escalation fires. The reconcile-driving phases
/// (everything that does work and can therefore get stuck) are in;
/// terminal/transient phases (`Ready`, `Failed`, `Pending`,
/// `Destroying`) are excluded because forcing them to Failed would be
/// either a no-op or destructive (e.g. yanking a happy `Ready`
/// template into a recovery cycle because some clock skew misread its
/// `phase_entered_at`).
fn phase_is_force_resettable(phase: crate::crd::Phase) -> bool {
    use crate::crd::Phase;
    matches!(
        phase,
        Phase::Compiling | Phase::Initializing | Phase::Planning | Phase::Applying,
    )
}

#[cfg(test)]
mod force_reset_tests {
    use super::phase_is_force_resettable;
    use crate::crd::Phase;

    #[test]
    fn force_resettable_for_working_phases() {
        assert!(phase_is_force_resettable(Phase::Compiling));
        assert!(phase_is_force_resettable(Phase::Initializing));
        assert!(phase_is_force_resettable(Phase::Planning));
        assert!(phase_is_force_resettable(Phase::Applying));
    }

    #[test]
    fn not_force_resettable_for_terminal_or_idle_phases() {
        assert!(!phase_is_force_resettable(Phase::Pending));
        assert!(!phase_is_force_resettable(Phase::Ready));
        assert!(!phase_is_force_resettable(Phase::Failed));
        assert!(!phase_is_force_resettable(Phase::Destroying));
    }
}

/// Returns `true` iff every observable field of the proposed reactive-
/// policy status PATCH equals what `prior_status` already carries
/// (conditions compared field-by-field including `lastTransitionTime`,
/// since the merge step above already preserves that timestamp when
/// content matches). When this returns `true` we can skip the PATCH —
/// otherwise it would refire this controller's watch and create a
/// self-trigger reconcile loop on every template reconcile, since
/// `apply_reactive_policy` runs unconditionally from
/// `post_reconcile_pipeline::run_for_template`.
fn reactive_policy_status_unchanged(
    prior_status: &crate::crd::InfrastructureTemplateStatus,
    new_conditions: &[crate::crd::Condition],
    new_blocked_since: Option<chrono::DateTime<chrono::Utc>>,
    new_auto_suspended: bool,
    new_escalated_at: Option<chrono::DateTime<chrono::Utc>>,
    new_escalation_reason: Option<&str>,
) -> bool {
    let conditions_unchanged = prior_status.conditions.len() == new_conditions.len()
        && prior_status
            .conditions
            .iter()
            .zip(new_conditions.iter())
            .all(|(p, n)| {
                p.r#type == n.r#type
                    && p.status == n.status
                    && p.reason == n.reason
                    && p.message == n.message
                    && p.last_transition_time == n.last_transition_time
            });

    conditions_unchanged
        && prior_status.verified_blocked_since == new_blocked_since
        && prior_status.auto_suspended == new_auto_suspended
        && prior_status.last_escalated_at == new_escalated_at
        && prior_status.last_escalation_reason.as_deref() == new_escalation_reason
}

#[cfg(test)]
mod tests {
    use super::reactive_policy_status_unchanged;
    use crate::crd::{Condition, InfrastructureTemplateStatus};
    use chrono::TimeZone;

    fn cond(typ: &str, status: &str, reason: &str, msg: &str) -> Condition {
        Condition {
            r#type: typ.into(),
            status: status.into(),
            reason: reason.into(),
            message: msg.into(),
            // Stable timestamp on purpose — the merge logic in
            // apply_reactive_policy preserves prev's timestamp when
            // content matches, so reaching this gate with equal
            // (type, status, reason, message) means the timestamps
            // also match. Tests reflect that contract.
            last_transition_time: chrono::Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).unwrap(),
        }
    }

    fn healthy_status() -> InfrastructureTemplateStatus {
        InfrastructureTemplateStatus {
            conditions: vec![cond("Healthy", "True", "NoEscalations", "no reactive policies have fired")],
            verified_blocked_since: None,
            auto_suspended: false,
            last_escalated_at: None,
            last_escalation_reason: None,
            ..Default::default()
        }
    }

    #[test]
    fn reactive_steady_state_skips_patch() {
        let prev = healthy_status();
        let new_cond = vec![cond("Healthy", "True", "NoEscalations", "no reactive policies have fired")];
        assert!(
            reactive_policy_status_unchanged(&prev, &new_cond, None, false, None, None),
            "must NOT patch when nothing observable changed (timestamp-only churn case)"
        );
    }

    #[test]
    fn reactive_healthy_to_alert_must_patch() {
        let prev = healthy_status();
        let new_cond = vec![cond("Healthy", "False", "PhaseTimeout", "stuck in Compiling for 30m")];
        let now = chrono::Utc.with_ymd_and_hms(2026, 5, 7, 0, 0, 0).unwrap();
        assert!(
            !reactive_policy_status_unchanged(
                &prev,
                &new_cond,
                None,
                false,
                Some(now),
                Some("PhaseTimeout"),
            ),
            "Healthy=True → False must force a PATCH"
        );
    }

    #[test]
    fn reactive_auto_suspend_flip_must_patch() {
        let prev = healthy_status();
        let new_cond = vec![cond("Healthy", "False", "ConsecutiveFailures", "5 consecutive failures")];
        let now = chrono::Utc.with_ymd_and_hms(2026, 5, 7, 0, 0, 0).unwrap();
        assert!(
            !reactive_policy_status_unchanged(
                &prev,
                &new_cond,
                None,
                true, // suspend escalation fired
                Some(now),
                Some("ConsecutiveFailures"),
            ),
            "auto_suspended flip must force a PATCH"
        );
    }

    #[test]
    fn reactive_verified_blocked_clock_advance_must_patch() {
        let prev = healthy_status();
        let new_cond = vec![cond("Healthy", "True", "NoEscalations", "no reactive policies have fired")];
        let blocked = chrono::Utc.with_ymd_and_hms(2026, 5, 7, 0, 0, 0).unwrap();
        assert!(
            !reactive_policy_status_unchanged(
                &prev,
                &new_cond,
                Some(blocked), // started tracking verified-blocked
                false,
                None,
                None,
            ),
            "verifiedBlockedSince changing from None → Some must force a PATCH"
        );
    }
}
