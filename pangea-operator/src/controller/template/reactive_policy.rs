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
    let conditions_unchanged = prior_status.conditions.len() == conditions.len()
        && prior_status.conditions.iter().zip(conditions.iter()).all(|(p, n)| {
            p.r#type == n.r#type
                && p.status == n.status
                && p.reason == n.reason
                && p.message == n.message
                && p.last_transition_time == n.last_transition_time
        });
    let nothing_changed = conditions_unchanged
        && prior_status.verified_blocked_since == new_blocked_since
        && prior_status.auto_suspended == new_auto_suspended
        && prior_status.last_escalated_at == escalated_at
        && prior_status.last_escalation_reason == escalation_reason;

    if nothing_changed {
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
    Ok(())
}
