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

/// The most recent moment we have evidence the phase was doing real
/// work — what `check_phase_timeout` actually measures from.
///
/// For every phase, entering it is evidence of life. For `Applying`
/// there is a second, better witness: the durable apply frontier
/// (`status.applyCursorAdvancedAt`, sampled from the `apply_cursor`
/// artifact row the resumable engine checkpoints as it lands
/// resources). Taking the LATER of the two is deliberate — it can only
/// ever declare a template alive for longer, never shorter, than the
/// pre-existing wall-clock rule. That is the safe direction here: a
/// false "alive" costs one slow cycle, a false "wedged" force-resets
/// real work (see `template::reactive_policy`'s force-reset).
fn liveness_witness(
    status: &crate::crd::InfrastructureTemplateStatus,
    phase: Phase,
    entered: DateTime<Utc>,
) -> (DateTime<Utc>, &'static str) {
    match (phase, status.apply_cursor_advanced_at) {
        (Phase::Applying, Some(advanced)) if advanced > entered => {
            (advanced, "applyCursorAdvancedAt")
        }
        _ => (entered, "phaseEnteredAt"),
    }
}

/// Is this phase stuck? A phase times out when nothing has witnessed
/// it making progress for longer than its threshold.
///
/// **Progress, not duration.** Before 2026-08-01 this compared
/// `now - phaseEnteredAt` and nothing else, so it could not tell
/// healthy-and-slow from wedged: the 846-repo / 2777-resource
/// `pleme-io-opensource` workspace measured a 651s plan against a 600s
/// bound on 2026-07-27 and was force-reset every cycle for the crime
/// of being large. For `Applying` the operator now has a real progress
/// term — the resumable engine's `ApplyCursor`, whose length is
/// monotonic across reconciles — so the predicate measures from the
/// last frontier advance instead. An apply that is still landing
/// resources is alive however long it has taken; one whose frontier
/// has not moved for the threshold is wedged.
///
/// This is a **better predicate, not an impossibility proof.** Nothing
/// here makes a wedge unrepresentable — see the module tests and the
/// `Compiling`/`Planning` arms below, which remain purely wall-clock
/// because magma's plan is a single non-resumable call
/// (`magma_plan::plan`) with no frontier to report.
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
    let (since, witness) = liveness_witness(status, phase, entered);
    if now.signed_duration_since(since) < chrono_threshold {
        return None;
    }
    Some(Trigger {
        action: policy.on_timeout,
        reason: format!("PhaseTimeout:{phase}"),
        // Name the witness: an operator reading this event needs to
        // know whether we judged by the clock or by the frontier, and
        // for Applying whether a frontier was available at all.
        message: format!(
            "no progress in phase {} for {}s measured from {} (threshold: {}; \
             phase entered {}s ago, appliedCount: {})",
            phase,
            now.signed_duration_since(since).num_seconds(),
            witness,
            threshold_str,
            now.signed_duration_since(entered).num_seconds(),
            status
                .apply_cursor_count
                .map_or_else(|| "(none)".to_string(), |c| c.to_string()),
        ),
        routing: policy.routing.clone(),
    })
}

/// Fold a freshly-observed apply frontier size into the tracked
/// `(applyCursorCount, applyCursorAdvancedAt)` pair. Pure; the caller
/// does the artifact-store read and the status patch.
///
/// Three cases, and each errs toward calling the template ALIVE:
///
///   * **Not sampled** (`observed == None` — not the Applying phase, no
///     artifact store, no cursor row yet, undecodable row) — carry the
///     prior pair through untouched. The judgement then falls back to
///     `phaseEnteredAt`, exactly the pre-existing wall-clock rule.
///   * **Changed** — stamp `now`. A *decrease* stamps too: the cursor
///     is plan-bound, so a smaller count means a different plan began
///     applying, which is fresh work rather than a stall.
///   * **Unchanged** — keep the prior timestamp, or stamp `now` if we
///     have never observed this template before. Never back-date: the
///     first sighting of a count tells us nothing about how long it
///     has been sitting there, and guessing "a long time" would
///     manufacture a wedge verdict out of no evidence.
pub fn track_apply_progress(
    prev_count: Option<u64>,
    prev_advanced_at: Option<DateTime<Utc>>,
    observed: Option<u64>,
    now: DateTime<Utc>,
) -> (Option<u64>, Option<DateTime<Utc>>) {
    match observed {
        None => (prev_count, prev_advanced_at),
        Some(count) if prev_count != Some(count) => (Some(count), Some(now)),
        Some(count) => (Some(count), prev_advanced_at.or(Some(now))),
    }
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

/// Progress-aware phase-timeout tests.
///
/// The incident: on 2026-07-27 the 846-repo / 2777-resource
/// `pleme-io-opensource` workspace on camelot-eks measured a 651s plan
/// against a 600s bound. Every command was killed just short of
/// completing, force-reset, and retried — `status.cycleCount` and the
/// state serial both frozen at 35 — because the only question the
/// operator knew how to ask was "how long has this phase lasted?".
/// Healthy-and-slow and wedged answer that question identically.
///
/// `check_phase_timeout` now asks a second question for `Applying`:
/// "has the durable apply frontier moved?". These tests pin BOTH
/// directions of that answer, because a predicate that can only ever
/// say "alive" is not a fix — it is the same blindness with the
/// opposite sign.
#[cfg(test)]
mod apply_progress_tests {
    use super::{evaluate, track_apply_progress, EffectiveReactivePolicy, Escalation};
    use crate::crd::{InfrastructureTemplate, InfrastructureTemplateStatus, Phase};
    use chrono::{DateTime, Duration as ChronoDuration, TimeZone, Utc};

    fn now() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 8, 1, 12, 0, 0).unwrap()
    }

    /// A template in `Applying`, entered `entered_mins_ago` minutes ago,
    /// whose frontier last advanced `advanced_mins_ago` minutes ago
    /// (`None` = never observed advancing).
    fn applying(
        entered_mins_ago: i64,
        advanced_mins_ago: Option<i64>,
        count: Option<u64>,
    ) -> InfrastructureTemplate {
        let mut s = InfrastructureTemplateStatus {
            phase: Some(Phase::Applying),
            phase_entered_at: Some(now() - ChronoDuration::minutes(entered_mins_ago)),
            apply_cursor_count: count,
            ..Default::default()
        };
        s.apply_cursor_advanced_at = advanced_mins_ago.map(|m| now() - ChronoDuration::minutes(m));
        let mut t: InfrastructureTemplate = serde_json::from_value(serde_json::json!({
            "apiVersion": "pangea.pleme.io/v1alpha1",
            "kind": "InfrastructureTemplate",
            "metadata": { "name": "pleme-io-opensource", "namespace": "pleme-io-opensource" },
            "spec": { "source": { "inline": "ignored" }, "pangeaNamespace": "test" }
        }))
        .unwrap();
        t.status = Some(s);
        t
    }

    // ── Direction 1: slow-but-advancing must NOT be reset ───────────

    /// The load-bearing case. Default `applying` threshold is 30m; this
    /// template has been applying for two HOURS — four times over the
    /// bound — but landed a resource one minute ago. It is the healthiest
    /// possible large workspace, and the pre-change predicate would have
    /// force-reset it every cycle forever.
    #[test]
    fn slow_but_advancing_apply_is_alive() {
        let t = applying(120, Some(1), Some(1_400));
        assert_eq!(
            evaluate(&t, &EffectiveReactivePolicy::default(), now()),
            Escalation::Healthy,
            "an apply still advancing its frontier is ALIVE however long it has taken — \
             judging it by wall clock is the 2026-07-27 pleme-io-opensource wedge"
        );
    }

    /// The frontier advanced 29 minutes ago against a 30m threshold —
    /// inside the bound by one minute. Still alive. Pins that the
    /// comparison is against the ADVANCE, not the phase entry (which is
    /// two hours stale here).
    #[test]
    fn advance_just_inside_the_threshold_is_alive() {
        let t = applying(120, Some(29), Some(1_400));
        assert_eq!(
            evaluate(&t, &EffectiveReactivePolicy::default(), now()),
            Escalation::Healthy,
        );
    }

    // ── Direction 2: genuinely stalled MUST be reset ────────────────

    /// The other half of the truth table. Same two-hour apply, but the
    /// frontier has not moved in 90 minutes — past the 30m bound. This
    /// is a real wedge and MUST still trigger, or the change would have
    /// traded one blindness for another.
    #[test]
    fn stalled_apply_still_triggers_phase_timeout() {
        let t = applying(120, Some(90), Some(1_400));
        match evaluate(&t, &EffectiveReactivePolicy::default(), now()) {
            Escalation::Triggered {
                reason, message, ..
            } => {
                assert_eq!(reason, "PhaseTimeout:Applying");
                assert!(
                    message.contains("applyCursorAdvancedAt"),
                    "the event must name the witness it judged by; got: {message}"
                );
                assert!(
                    message.contains("1400"),
                    "the event must carry the frontier size so an operator can see \
                     where it stopped; got: {message}"
                );
            }
            other => panic!("a frontier stalled 90m past a 30m bound must trigger; got {other:?}"),
        }
    }

    /// No frontier at all (tofu, DB-less, or an apply that has not
    /// checkpointed yet) falls back to the pre-existing wall-clock rule
    /// unchanged. Absence of a progress signal must not be read as
    /// presence of progress.
    #[test]
    fn no_frontier_falls_back_to_wall_clock() {
        let t = applying(120, None, None);
        match evaluate(&t, &EffectiveReactivePolicy::default(), now()) {
            Escalation::Triggered {
                reason, message, ..
            } => {
                assert_eq!(reason, "PhaseTimeout:Applying");
                assert!(
                    message.contains("phaseEnteredAt"),
                    "with no frontier the witness must be the phase clock; got: {message}"
                );
            }
            other => panic!("wall-clock fallback must still trigger; got {other:?}"),
        }
    }

    /// A stale `applyCursorAdvancedAt` left over from a PREVIOUS entry
    /// into Applying must not shorten the window. `liveness_witness`
    /// takes the later of the two, so an advance older than the phase
    /// entry is ignored in favour of the phase entry.
    #[test]
    fn advance_older_than_phase_entry_is_ignored() {
        // Entered Applying 5m ago; the last frontier advance was 3h ago
        // (a previous plan's). Threshold is 30m — must be Healthy.
        let t = applying(5, Some(180), Some(1_400));
        assert_eq!(
            evaluate(&t, &EffectiveReactivePolicy::default(), now()),
            Escalation::Healthy,
            "a pre-phase-entry advance must never make a fresh phase look stale"
        );
    }

    /// Planning has no frontier — magma's `plan` is one non-resumable
    /// call — so it stays purely wall-clock-judged. Pinned so a future
    /// reader does not assume plan is covered.
    #[test]
    fn planning_is_still_wall_clock_judged() {
        let mut t = applying(120, Some(1), Some(1_400));
        if let Some(s) = t.status.as_mut() {
            s.phase = Some(Phase::Planning);
        }
        match evaluate(&t, &EffectiveReactivePolicy::default(), now()) {
            Escalation::Triggered {
                reason, message, ..
            } => {
                assert_eq!(reason, "PhaseTimeout:Planning");
                assert!(
                    message.contains("phaseEnteredAt"),
                    "planning must ignore the apply frontier entirely; got: {message}"
                );
            }
            other => panic!("planning past 10m must still trigger; got {other:?}"),
        }
    }

    // ── The tracker's truth table ───────────────────────────────────

    #[test]
    fn unsampled_frontier_carries_the_prior_pair_through() {
        let earlier = now() - ChronoDuration::hours(1);
        assert_eq!(
            track_apply_progress(Some(7), Some(earlier), None, now()),
            (Some(7), Some(earlier)),
            "no reading must not disturb the record — the judgement falls back to the clock"
        );
    }

    #[test]
    fn a_growing_frontier_stamps_now() {
        let earlier = now() - ChronoDuration::hours(1);
        assert_eq!(
            track_apply_progress(Some(7), Some(earlier), Some(8), now()),
            (Some(8), Some(now())),
        );
    }

    #[test]
    fn a_shrinking_frontier_also_stamps_now() {
        // The cursor is plan-bound: a smaller count means a NEW plan
        // started applying. That is fresh work, not a stall.
        let earlier = now() - ChronoDuration::hours(1);
        assert_eq!(
            track_apply_progress(Some(900), Some(earlier), Some(3), now()),
            (Some(3), Some(now())),
        );
    }

    #[test]
    fn an_unchanged_frontier_keeps_the_prior_timestamp() {
        let earlier = now() - ChronoDuration::hours(1);
        assert_eq!(
            track_apply_progress(Some(7), Some(earlier), Some(7), now()),
            (Some(7), Some(earlier)),
            "an unchanged count must not refresh the clock — that would make every \
             stall look alive"
        );
    }

    #[test]
    fn a_first_observation_stamps_now_rather_than_back_dating() {
        // We have never watched this template before. How long the count
        // has been sitting at 7 is unknown; guessing "a long time" would
        // manufacture a wedge verdict out of no evidence.
        assert_eq!(
            track_apply_progress(None, None, Some(7), now()),
            (Some(7), Some(now())),
        );
    }
}
