//! Reactive policy evaluation — declarative responses to "things
//! didn't go to a good state".
//!
//! Three escalation paths:
//!   * `failureEscalation` — N consecutive Failed reconciles → action
//!   * `phaseTimeout`      — stuck in Compiling/Planning/Applying → action
//!   * `verifiedBlocked`   — Verified=False persistent → action
//!
//! Cascade levels (innermost wins per field):
//!   gem.policy.reactive → workspace.policy.reactive → template.spec.reactivePolicy
//!
//! Default policy (when nothing is set anywhere):
//!   failure: 5 → Alert
//!   phaseTimeout: 5m / 10m / 30m → Alert
//!   verifiedBlocked: 10m → Alert
//!
//! Action semantics:
//!   * Alert   — Warning event + Healthy=False condition + structured
//!               log line in routing-formatted shape. Reconcile loop
//!               continues unchanged.
//!   * Suspend — Patch status.autoSuspended=true. Reconcile entry
//!               short-circuits until operator-human clears.
//!   * Page    — Highest-urgency routing notify (priority bump +
//!               @here/@channel hints in the log line). No state
//!               change beyond Healthy=False.
//!
//! Today the operator only LOGS the routing intent (ntfy/Slack/GitHub)
//! in a structured form. Real delivery to those channels is a
//! follow-up — but the typed surface is in place so callers are
//! already declaring their intent correctly.

use chrono::{DateTime, Duration as ChronoDuration, Utc};
use std::time::Duration;
use tracing::warn;

use crate::crd::architecture_gem::{
    ApprovalRouting, FailureEscalation, PhaseTimeoutPolicy, ReactiveAction, ReactivePolicy,
    VerifiedBlockedPolicy,
};
use crate::crd::{InfrastructureTemplate, Phase};

/// The concrete, defaults-applied reactive policy the operator
/// evaluates against a template's status. Built by walking the
/// cascade (gem → workspace → template) and merging per-field, with
/// hard defaults filling any remaining holes.
#[derive(Debug, Clone)]
pub struct EffectiveReactivePolicy {
    pub failure: FailureEscalation,
    pub phase_timeout: PhaseTimeoutPolicy,
    pub verified_blocked: VerifiedBlockedPolicy,
}

impl Default for EffectiveReactivePolicy {
    fn default() -> Self {
        Self {
            failure: FailureEscalation {
                max_consecutive_failures: 5,
                on_exhaustion: ReactiveAction::Alert,
                routing: None,
            },
            phase_timeout: PhaseTimeoutPolicy {
                compiling: "5m".into(),
                planning: "10m".into(),
                applying: "30m".into(),
                on_timeout: ReactiveAction::Alert,
                routing: None,
            },
            verified_blocked: VerifiedBlockedPolicy {
                timeout: "10m".into(),
                on_blocked: ReactiveAction::Alert,
                routing: None,
            },
        }
    }
}

impl EffectiveReactivePolicy {
    /// Resolve the cascade, innermost wins per field.
    ///
    /// The per-field merge is the canonical `CascadePolicy` primitive
    /// (`impl` on [`ReactivePolicy`] in `crd::architecture_gem`; see
    /// `theory/CONVERGENCE-ADOPTION.md` §II.2). This method:
    ///   1. folds the layers innermost-wins via `CascadePolicy::resolve`
    ///      (workspace, then template on top — rightmost wins per field),
    ///   2. projects the merged `Option`-field policy onto the
    ///      hard-defaulted concrete `EffectiveReactivePolicy`, filling
    ///      any field no layer set with this type's `Default`.
    ///
    /// Behaviour is identical to the prior inline merge loop — the
    /// existing cascade tests below prove parity.
    pub fn resolve(
        template_policy: Option<&ReactivePolicy>,
        workspace_policy: Option<&ReactivePolicy>,
    ) -> Self {
        use shigoto_types::policy::CascadePolicy;
        // Merge the Option-field layers innermost-wins (workspace then
        // template). `resolve` starts from an all-`None` default and
        // applies each layer in order; the rightmost layer that sets a
        // field wins.
        let merged = ReactivePolicy::resolve(
            &[workspace_policy, template_policy],
            ReactivePolicy::default(),
        );
        // Project the merged layer onto the hard-defaulted concrete
        // policy: a field the cascade set overrides the default; an
        // unset field keeps the hard default.
        let mut effective = Self::default();
        if let Some(f) = merged.failure_escalation {
            effective.failure = f;
        }
        if let Some(p) = merged.phase_timeout {
            effective.phase_timeout = p;
        }
        if let Some(v) = merged.verified_blocked {
            effective.verified_blocked = v;
        }
        effective
    }
}

/// Outcome of evaluating reactive policy against a template's current
/// status. The template_controller takes one `Escalation` and applies
/// it (event + condition + autoSuspended patch + structured log).
#[derive(Debug, Clone, PartialEq)]
pub enum Escalation {
    /// No reactive policy fired this reconcile.
    Healthy,
    /// One or more reactive policies fired. Carries the worst-action
    /// across all triggered policies (Suspend > Page > Alert) plus
    /// the reason for the most-recent trigger.
    Triggered {
        action: ReactiveAction,
        reason: String,
        message: String,
        routing: Option<ApprovalRouting>,
    },
}

/// Evaluate every reactive escalation path against a template's
/// current status + effective policy. Returns the worst-action
/// trigger if any path fired, else Healthy.
///
/// The caller has already established `now` (typically `Utc::now()`)
/// — passed in for testability.
pub fn evaluate(
    template: &InfrastructureTemplate,
    policy: &EffectiveReactivePolicy,
    now: DateTime<Utc>,
) -> Escalation {
    let status = match template.status.as_ref() {
        Some(s) => s,
        None => return Escalation::Healthy,
    };

    let mut triggers: Vec<Trigger> = Vec::new();

    if let Some(t) = check_failure(status, &policy.failure) {
        triggers.push(t);
    }
    if let Some(t) = check_phase_timeout(status, &policy.phase_timeout, now) {
        triggers.push(t);
    }
    if let Some(t) = check_verified_blocked(status, &policy.verified_blocked, now) {
        triggers.push(t);
    }

    pick_worst(triggers)
}

#[derive(Debug, Clone)]
struct Trigger {
    action: ReactiveAction,
    reason: String,
    message: String,
    routing: Option<ApprovalRouting>,
}

fn pick_worst(triggers: Vec<Trigger>) -> Escalation {
    if triggers.is_empty() {
        return Escalation::Healthy;
    }
    // Suspend > Page > Alert (most disruptive wins for safety).
    let worst = triggers
        .into_iter()
        .max_by_key(|t| match t.action {
            ReactiveAction::Suspend => 3,
            ReactiveAction::Page => 2,
            ReactiveAction::Alert => 1,
        })
        .expect("non-empty by guard above");
    Escalation::Triggered {
        action: worst.action,
        reason: worst.reason,
        message: worst.message,
        routing: worst.routing,
    }
}

fn check_failure(
    status: &crate::crd::InfrastructureTemplateStatus,
    policy: &FailureEscalation,
) -> Option<Trigger> {
    if status.failure_count < policy.max_consecutive_failures {
        return None;
    }
    Some(Trigger {
        action: policy.on_exhaustion,
        reason: "FailureEscalation".into(),
        message: format!(
            "{} consecutive failed reconciles (threshold: {}); last error: {}",
            status.failure_count,
            policy.max_consecutive_failures,
            status.last_error.as_deref().unwrap_or("(none)")
        ),
        routing: policy.routing.clone(),
    })
}

fn check_phase_timeout(
    status: &crate::crd::InfrastructureTemplateStatus,
    policy: &PhaseTimeoutPolicy,
    now: DateTime<Utc>,
) -> Option<Trigger> {
    let phase = status.phase?;
    let entered = status.phase_entered_at?;
    let threshold_str = match phase {
        Phase::Compiling => &policy.compiling,
        Phase::Planning => &policy.planning,
        Phase::Applying => &policy.applying,
        // Terminal / non-reconcile-driving phases don't time out.
        _ => return None,
    };
    let threshold = parse_duration(threshold_str)?;
    let chrono_threshold = ChronoDuration::from_std(threshold).ok()?;
    if now.signed_duration_since(entered) < chrono_threshold {
        return None;
    }
    Some(Trigger {
        action: policy.on_timeout,
        reason: format!("PhaseTimeout:{phase}"),
        message: format!(
            "stuck in phase {} for {}s (threshold: {})",
            phase,
            now.signed_duration_since(entered).num_seconds(),
            threshold_str
        ),
        routing: policy.routing.clone(),
    })
}

fn check_verified_blocked(
    status: &crate::crd::InfrastructureTemplateStatus,
    policy: &VerifiedBlockedPolicy,
    now: DateTime<Utc>,
) -> Option<Trigger> {
    let blocked_since = status.verified_blocked_since?;
    let threshold = parse_duration(&policy.timeout)?;
    let chrono_threshold = ChronoDuration::from_std(threshold).ok()?;
    if now.signed_duration_since(blocked_since) < chrono_threshold {
        return None;
    }
    Some(Trigger {
        action: policy.on_blocked,
        reason: "VerifiedBlocked".into(),
        message: format!(
            "Verified=False for {}s (threshold: {})",
            now.signed_duration_since(blocked_since).num_seconds(),
            policy.timeout
        ),
        routing: policy.routing.clone(),
    })
}

/// Update the verified-blocked tracking field based on whether the
/// current status carries a `Verified=False` condition. Returns the
/// new value for `status.verified_blocked_since` (None if cleared).
pub fn track_verified_blocked(
    status: &crate::crd::InfrastructureTemplateStatus,
    now: DateTime<Utc>,
) -> Option<DateTime<Utc>> {
    use crate::crd::Condition as TplCondition;
    let verified_false = status
        .conditions
        .iter()
        .any(|c: &TplCondition| c.r#type == "Verified" && c.status == "False");
    match (verified_false, status.verified_blocked_since) {
        (true, Some(t)) => Some(t), // already tracked
        (true, None) => Some(now),  // first observation
        (false, _) => None,         // cleared
    }
}

/// Build the routing-formatted log payload for one escalation. This
/// is what the operator emits today; future iterations turn this into
/// an actual ntfy/Slack/GitHub-issue side effect via routing.
pub fn format_routing_log(
    template_name: &str,
    namespace: &str,
    action: ReactiveAction,
    reason: &str,
    message: &str,
    routing: Option<&ApprovalRouting>,
) -> String {
    let action_tag = match action {
        ReactiveAction::Alert => "ALERT",
        ReactiveAction::Suspend => "SUSPEND",
        ReactiveAction::Page => "PAGE",
    };
    let routing_target = routing
        .map(|r| {
            let mut parts = Vec::new();
            if let Some(t) = &r.ntfy_topic {
                parts.push(format!("ntfy={}", t));
            }
            if let Some(c) = &r.slack_channel {
                parts.push(format!("slack={}", c));
            }
            if let Some(g) = &r.github_issue_template {
                parts.push(format!("gh-issue={}", g));
            }
            if parts.is_empty() {
                "(none)".to_string()
            } else {
                parts.join(",")
            }
        })
        .unwrap_or_else(|| "(none)".to_string());
    format!(
        "[{action_tag}] template={namespace}/{template_name} reason={reason} routing={routing_target} :: {message}"
    )
}

/// Emit the structured log line for an escalation (today: tracing
/// only; tomorrow: ntfy/Slack/GitHub via routing).
pub fn emit_escalation_log(
    template_name: &str,
    namespace: &str,
    action: ReactiveAction,
    reason: &str,
    message: &str,
    routing: Option<&ApprovalRouting>,
) {
    let line = format_routing_log(template_name, namespace, action, reason, message, routing);
    match action {
        ReactiveAction::Alert => warn!(target: "pangea_operator::reactive", "{}", line),
        ReactiveAction::Suspend => warn!(target: "pangea_operator::reactive", "{}", line),
        ReactiveAction::Page => warn!(target: "pangea_operator::reactive::page", "{}", line),
    }
}

/// Look up the parent WorkspaceCatalog and extract its reactive
/// policy. Caller threads this into `EffectiveReactivePolicy::resolve`.
pub async fn workspace_reactive_policy(
    client: &kube::Client,
    template: &InfrastructureTemplate,
) -> Option<ReactivePolicy> {
    let wsc = crate::controller::workspace_catalog_controller::parent_catalog_for_template(
        client, template,
    )
    .await
    .ok()
    .flatten()?;
    wsc.spec.policy.reactive.clone()
}

/// Tiny duration parser — same shape as the existing
/// `parse_duration` in controller/mod.rs but without pulling in its
/// imports. Accepts `30s`, `5m`, `1h`. Returns None on failure.
fn parse_duration(s: &str) -> Option<Duration> {
    let trimmed = s.trim();
    if let Some(num) = trimmed.strip_suffix('s') {
        if let Ok(n) = num.parse::<u64>() {
            return Some(Duration::from_secs(n));
        }
    }
    if let Some(num) = trimmed.strip_suffix('m') {
        if let Ok(n) = num.parse::<u64>() {
            return Some(Duration::from_secs(n * 60));
        }
    }
    if let Some(num) = trimmed.strip_suffix('h') {
        if let Ok(n) = num.parse::<u64>() {
            return Some(Duration::from_secs(n * 3600));
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crd::{Condition, InfrastructureTemplateStatus};

    fn empty_status() -> InfrastructureTemplateStatus {
        InfrastructureTemplateStatus::default()
    }

    fn template_with_status(s: InfrastructureTemplateStatus) -> InfrastructureTemplate {
        let mut t: InfrastructureTemplate = serde_json::from_value(serde_json::json!({
            "apiVersion": "pangea.pleme.io/v1alpha1",
            "kind": "InfrastructureTemplate",
            "metadata": { "name": "test", "namespace": "default" },
            "spec": {
                "source": { "inline": "ignored" },
                "pangeaNamespace": "test"
            }
        }))
        .unwrap();
        t.status = Some(s);
        t
    }

    #[test]
    fn default_policy_has_alert_actions_and_5_failures() {
        let p = EffectiveReactivePolicy::default();
        assert_eq!(p.failure.max_consecutive_failures, 5);
        assert_eq!(p.failure.on_exhaustion, ReactiveAction::Alert);
        assert_eq!(p.phase_timeout.compiling, "5m");
        assert_eq!(p.phase_timeout.planning, "10m");
        assert_eq!(p.phase_timeout.applying, "30m");
        assert_eq!(p.phase_timeout.on_timeout, ReactiveAction::Alert);
        assert_eq!(p.verified_blocked.timeout, "10m");
        assert_eq!(p.verified_blocked.on_blocked, ReactiveAction::Alert);
    }

    #[test]
    fn cascade_template_overrides_workspace_per_field() {
        let workspace = ReactivePolicy {
            failure_escalation: Some(FailureEscalation {
                max_consecutive_failures: 3,
                on_exhaustion: ReactiveAction::Suspend,
                routing: None,
            }),
            phase_timeout: None,
            verified_blocked: None,
        };
        let template = ReactivePolicy {
            failure_escalation: Some(FailureEscalation {
                max_consecutive_failures: 10,
                on_exhaustion: ReactiveAction::Page,
                routing: None,
            }),
            phase_timeout: None,
            verified_blocked: None,
        };
        let eff = EffectiveReactivePolicy::resolve(Some(&template), Some(&workspace));
        // Template overrides workspace
        assert_eq!(eff.failure.max_consecutive_failures, 10);
        assert_eq!(eff.failure.on_exhaustion, ReactiveAction::Page);
        // phaseTimeout falls back to default
        assert_eq!(eff.phase_timeout.compiling, "5m");
    }

    #[test]
    fn cascade_workspace_used_when_template_unset() {
        let workspace = ReactivePolicy {
            failure_escalation: Some(FailureEscalation {
                max_consecutive_failures: 3,
                on_exhaustion: ReactiveAction::Suspend,
                routing: None,
            }),
            phase_timeout: None,
            verified_blocked: None,
        };
        let eff = EffectiveReactivePolicy::resolve(None, Some(&workspace));
        assert_eq!(eff.failure.max_consecutive_failures, 3);
        assert_eq!(eff.failure.on_exhaustion, ReactiveAction::Suspend);
    }

    #[test]
    fn no_escalation_when_failure_count_below_threshold() {
        let mut s = empty_status();
        s.failure_count = 2;
        let t = template_with_status(s);
        let result = evaluate(&t, &EffectiveReactivePolicy::default(), Utc::now());
        assert_eq!(result, Escalation::Healthy);
    }

    #[test]
    fn failure_escalation_fires_at_threshold() {
        let mut s = empty_status();
        s.failure_count = 5;
        s.last_error = Some("permission denied".into());
        let t = template_with_status(s);
        let result = evaluate(&t, &EffectiveReactivePolicy::default(), Utc::now());
        match result {
            Escalation::Triggered {
                action,
                reason,
                message,
                ..
            } => {
                assert_eq!(action, ReactiveAction::Alert);
                assert_eq!(reason, "FailureEscalation");
                assert!(message.contains("5 consecutive"));
                assert!(message.contains("permission denied"));
            }
            _ => panic!("expected Triggered"),
        }
    }

    #[test]
    fn phase_timeout_fires_after_threshold() {
        let mut s = empty_status();
        s.phase = Some(Phase::Planning);
        let now = Utc::now();
        s.phase_entered_at = Some(now - chrono::Duration::minutes(15));
        let t = template_with_status(s);
        let result = evaluate(&t, &EffectiveReactivePolicy::default(), now);
        match result {
            Escalation::Triggered { action, reason, .. } => {
                assert_eq!(action, ReactiveAction::Alert);
                assert!(reason.starts_with("PhaseTimeout:"));
            }
            _ => panic!("expected Triggered"),
        }
    }

    #[test]
    fn phase_timeout_does_not_fire_for_terminal_phase() {
        let mut s = empty_status();
        s.phase = Some(Phase::Ready);
        s.phase_entered_at = Some(Utc::now() - chrono::Duration::hours(24));
        let t = template_with_status(s);
        let result = evaluate(&t, &EffectiveReactivePolicy::default(), Utc::now());
        assert_eq!(result, Escalation::Healthy);
    }

    #[test]
    fn verified_blocked_fires_after_threshold() {
        let mut s = empty_status();
        let now = Utc::now();
        s.verified_blocked_since = Some(now - chrono::Duration::minutes(15));
        let t = template_with_status(s);
        let result = evaluate(&t, &EffectiveReactivePolicy::default(), now);
        match result {
            Escalation::Triggered { reason, .. } => {
                assert_eq!(reason, "VerifiedBlocked");
            }
            _ => panic!("expected Triggered"),
        }
    }

    #[test]
    fn worst_action_wins_when_multiple_triggers() {
        let mut s = empty_status();
        s.failure_count = 10;
        s.phase = Some(Phase::Planning);
        let now = Utc::now();
        s.phase_entered_at = Some(now - chrono::Duration::minutes(15));
        let t = template_with_status(s);
        let mut policy = EffectiveReactivePolicy::default();
        policy.failure.on_exhaustion = ReactiveAction::Alert;
        policy.phase_timeout.on_timeout = ReactiveAction::Suspend;
        let result = evaluate(&t, &policy, now);
        match result {
            Escalation::Triggered { action, .. } => {
                assert_eq!(action, ReactiveAction::Suspend, "Suspend > Alert");
            }
            _ => panic!("expected Triggered"),
        }
    }

    #[test]
    fn track_verified_blocked_starts_clock_on_first_false() {
        let mut s = empty_status();
        s.conditions = vec![Condition {
            r#type: "Verified".into(),
            status: "False".into(),
            reason: "GemNotLoaded".into(),
            message: "blocked".into(),
            last_transition_time: Utc::now(),
        }];
        let now = Utc::now();
        let result = track_verified_blocked(&s, now);
        assert_eq!(result, Some(now));
    }

    #[test]
    fn track_verified_blocked_preserves_clock_on_continued_false() {
        let mut s = empty_status();
        let started = Utc::now() - chrono::Duration::minutes(7);
        s.verified_blocked_since = Some(started);
        s.conditions = vec![Condition {
            r#type: "Verified".into(),
            status: "False".into(),
            reason: "GemNotLoaded".into(),
            message: "still blocked".into(),
            last_transition_time: Utc::now(),
        }];
        let result = track_verified_blocked(&s, Utc::now());
        assert_eq!(result, Some(started));
    }

    #[test]
    fn track_verified_blocked_clears_when_verified_true() {
        let mut s = empty_status();
        s.verified_blocked_since = Some(Utc::now() - chrono::Duration::minutes(7));
        s.conditions = vec![Condition {
            r#type: "Verified".into(),
            status: "True".into(),
            reason: "AllGemsLoaded".into(),
            message: "ok".into(),
            last_transition_time: Utc::now(),
        }];
        let result = track_verified_blocked(&s, Utc::now());
        assert_eq!(result, None);
    }

    #[test]
    fn format_routing_log_includes_routing_targets() {
        let line = format_routing_log(
            "my-tmpl",
            "default",
            ReactiveAction::Page,
            "FailureEscalation",
            "10 failures",
            Some(&ApprovalRouting {
                ntfy_topic: Some("rio-critical".into()),
                slack_channel: Some("#oncall".into()),
                github_issue_template: None,
            }),
        );
        assert!(line.contains("[PAGE]"));
        assert!(line.contains("template=default/my-tmpl"));
        assert!(line.contains("ntfy=rio-critical"));
        assert!(line.contains("slack=#oncall"));
        assert!(line.contains("10 failures"));
    }

    #[test]
    fn format_routing_log_handles_no_routing() {
        let line = format_routing_log(
            "t",
            "ns",
            ReactiveAction::Alert,
            "VerifiedBlocked",
            "blocked 700s",
            None,
        );
        assert!(line.contains("routing=(none)"));
    }
}
