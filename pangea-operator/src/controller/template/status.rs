//! Status-patching helpers for `InfrastructureTemplate` reconciliation.
//!
//! Lifted from `template_controller.rs` during T1 (continuation of R6).
//! Each function takes a `&InfrastructureTemplate` + new field values
//! and applies a server-side merge patch on `.status`. The patches
//! preserve fields not named in the patch (since the kube-rs Merge
//! patch is a shallow JSON merge, not a strategic merge).
//!
//! Why these belong in their own module: every reconcile path ends in
//! one of these functions, so they form a natural API surface. Group-
//! ing them together makes drift across patches (e.g. one fn forgets
//! to bump observedGeneration) immediately visible.

use chrono::Utc;
use tracing::info;

use crate::controller::reconciler::{conditions_for_phase, SourceFreshState};
use crate::controller::ControllerState;
use crate::crd::{
    InfrastructureTemplate, Phase, PolicyDecision, PolicyEvaluation, ResourceSummary,
};
use crate::error::Result;

/// Derive the git-edge freshness state from the template's recorded
/// revisions — a pure read of `spec.source` + `status`. This is what
/// gates the `Ready` condition (`conditions_for_phase`), making
/// "Ready while behind HEAD" unutterable on the status surface.
///
/// - non-git source              → `NotApplicable` (no edge to track)
/// - HEAD never observed          → `Unverified`   (can't claim at-HEAD)
/// - compiledRevision == observed → `Fresh`        (at the edge)
/// - otherwise (incl. compiled None) → `Behind`
#[must_use]
pub fn source_fresh_state(template: &InfrastructureTemplate) -> SourceFreshState {
    if template.spec.source.git_repository.is_none() {
        return SourceFreshState::NotApplicable;
    }
    let status = template.status.as_ref();
    let observed = status.and_then(|s| s.observed_head_revision.as_deref());
    let compiled = status.and_then(|s| s.compiled_revision.as_deref());
    match (compiled, observed) {
        (_, None) => SourceFreshState::Unverified,
        (Some(c), Some(o)) if c == o => SourceFreshState::Fresh,
        (_, Some(_)) => SourceFreshState::Behind,
    }
}

/// Update the phase in the template status.
///
/// Bumps `phase_entered_at` only on real transitions (the
/// ReactivePolicy phaseTimeout escalation measures against this).
/// Clears `last_error` + `failure_count` on non-Failed transitions.
pub async fn update_phase(
    template: &InfrastructureTemplate,
    phase: Phase,
    state: &ControllerState,
) -> Result<()> {
    let mut status = template.status.clone().unwrap_or_default();
    let phase_changed = status.phase != Some(phase);
    // lifecycle M1 — impose the typed FSM as the live authority. Validate this
    // transition against controller::lifecycle::TRANSITIONS. An edge the table
    // doesn't sanction is logged LOUDLY (table↔handler drift surfaces in
    // production) but behavior is preserved (we still apply `phase`), so this
    // is stability-safe. Once the guard runs clean over real reconciles, the
    // handlers migrate to compute the destination via Phase::advance outright.
    let prev_phase = status.phase.unwrap_or_default();
    if phase_changed && !prev_phase.edge_is_legal(phase) {
        tracing::warn!(
            prev = %prev_phase, next = %phase,
            "FSM ILLEGAL EDGE: {prev_phase} → {phase} is not in \
             controller::lifecycle::TRANSITIONS — handler made a transition the typed FSM \
             does not sanction. Behavior preserved; reconcile the table or the handler."
        );
    }
    status.phase = Some(phase);
    status.observed_generation = template.metadata.generation.unwrap_or(0);
    // Always set conditions so FluxCD healthChecks see current state.
    // The Ready condition is gated on the git-edge state — a phase==Ready
    // transition while behind HEAD still yields Ready=False.
    status.conditions = conditions_for_phase(phase, None, source_fresh_state(template));
    // ReactivePolicy: bump phase_entered_at only on real transitions
    // — that's what phaseTimeout escalation measures against.
    if phase_changed {
        status.phase_entered_at = Some(Utc::now());
    }

    // Clear error on non-Failed transitions
    if phase != Phase::Failed {
        status.last_error = None;
        status.failure_count = 0;
    }

    // Field-scoped status patch — write ONLY the fields a phase transition
    // OWNS. The previous `json!({ "status": status })` re-sent the ENTIRE
    // in-memory status (a stale reconcile-start snapshot) on every
    // transition, clobbering fields that sibling field-scoped helpers
    // (`update_compiled_revision`, `update_freshness_status`) had ALREADY
    // written earlier in the SAME reconcile: handle_compiling runs
    // `update_compiled_revision(HEAD)` then `update_phase(Initializing)`, and
    // the latter rewound `compiledRevision` back to the snapshot value. That
    // froze `compiledRevision`/`observedHeadRevision`, so the freshness gate
    // saw the compile as perpetually "behind HEAD" → bounced
    // Planning→Compiling forever (the +N-plan-never-applies wedge on every
    // DB-backed template). Mirroring the sibling helpers' field-scoped shape
    // makes the writes COMPOSE instead of clobber. (A JSON merge-patch only
    // touches the keys present here; `null` clears a field.)
    let mut status_patch = serde_json::json!({
        "phase": status.phase,
        "observedGeneration": status.observed_generation,
        "conditions": status.conditions,
    });
    if phase_changed {
        status_patch["phaseEnteredAt"] = serde_json::json!(status.phase_entered_at);
    }
    if phase != Phase::Failed {
        status_patch["lastError"] = serde_json::Value::Null;
        status_patch["failureCount"] = serde_json::json!(0u32);
    }
    let patch = serde_json::json!({ "status": status_patch });

    crate::controller::status_patch::patch_status(template, &state.client, patch).await?;

    // NOTE: `pangea_templates_by_phase` is a FLEET gauge set by the
    // fleet-status controller (`set_templates_by_phase`, reset-then-set over
    // every template each tick) — NOT a per-transition counter. Incrementing
    // it here was wrong: it counted transitions (monotone, never decremented)
    // and produced no series on a settled fleet. The single source of truth
    // for the current phase distribution is the fleet aggregation.

    info!(?phase, "Updated template phase");
    Ok(())
}

/// Update phase to a Failed-class state with an error message and
/// run the post-reconcile pipeline (ReactivePolicy escalation).
pub async fn update_phase_with_error(
    template: &InfrastructureTemplate,
    phase: Phase,
    error_msg: &str,
    state: &ControllerState,
) -> Result<()> {
    let mut status = template.status.clone().unwrap_or_default();
    let phase_changed = status.phase != Some(phase);
    // lifecycle M1 — impose the typed FSM as the live authority. Validate this
    // transition against controller::lifecycle::TRANSITIONS. An edge the table
    // doesn't sanction is logged LOUDLY (table↔handler drift surfaces in
    // production) but behavior is preserved (we still apply `phase`), so this
    // is stability-safe. Once the guard runs clean over real reconciles, the
    // handlers migrate to compute the destination via Phase::advance outright.
    let prev_phase = status.phase.unwrap_or_default();
    if phase_changed && !prev_phase.edge_is_legal(phase) {
        tracing::warn!(
            prev = %prev_phase, next = %phase,
            "FSM ILLEGAL EDGE: {prev_phase} → {phase} is not in \
             controller::lifecycle::TRANSITIONS — handler made a transition the typed FSM \
             does not sanction. Behavior preserved; reconcile the table or the handler."
        );
    }
    status.phase = Some(phase);
    status.observed_generation = template.metadata.generation.unwrap_or(0);
    status.last_error = Some(error_msg.to_string());
    status.failure_count = status.failure_count.saturating_add(1);
    status.conditions = conditions_for_phase(phase, Some(error_msg), source_fresh_state(template));
    if phase_changed {
        status.phase_entered_at = Some(Utc::now());
    }

    // Field-scoped status patch (see `update_phase`) — write only the fields a
    // failure transition owns, so it composes with the sibling field-scoped
    // helpers instead of clobbering compiledRevision/observedHeadRevision back
    // to the stale reconcile-start snapshot.
    let mut status_patch = serde_json::json!({
        "phase": status.phase,
        "observedGeneration": status.observed_generation,
        "conditions": status.conditions,
        "lastError": status.last_error,
        "failureCount": status.failure_count,
    });
    if phase_changed {
        status_patch["phaseEnteredAt"] = serde_json::json!(status.phase_entered_at);
    }
    let patch = serde_json::json!({ "status": status_patch });

    crate::controller::status_patch::patch_status(template, &state.client, patch).await?;

    // Post-reconcile pipeline — single ordered entry point for hooks
    // that fire AFTER status is patched. Today's pipeline runs the
    // ReactivePolicy stage (failure escalation / phase timeout /
    // verified-blocked); future hooks (audit emission, notification
    // routing) land in `controller::post_reconcile_pipeline`.
    crate::controller::post_reconcile_pipeline::run_for_template(template, state).await;

    Ok(())
}

/// Build a `{"status": {...}}` merge-patch body containing ONLY the
/// named fields, sourced from `status`'s own `Serialize` impl (so
/// each field's per-field `skip_serializing_if` behavior — e.g.
/// `Option::is_none` omitting a key rather than nulling it — is
/// preserved exactly; this changes WHICH keys reach the wire, never
/// HOW an individual owned key is encoded).
///
/// This is the guard against the "full-object clone → patch" clobber
/// class `update_phase`'s doc comment (above) explains in full: a
/// helper built from `template.status.clone()` — the immutable,
/// start-of-reconcile snapshot — can only ever emit the fields named
/// here, no matter what ELSE is populated on that snapshot (e.g. a
/// sibling field-scoped helper's fresher write to a field this
/// function doesn't own). A name typo'd out of `field_names` is a
/// silently-dropped PATCH key (safe: no write happens for that
/// field this round) rather than a silently-SENT stale value (unsafe:
/// clobbers whatever a sibling helper just wrote to the API server
/// moments earlier in the same reconcile pass — see
/// `update_plan_status`/`update_settling_status` for the concrete
/// incident this closes). Tier: only-mitigated (a runtime filter, not
/// a type that makes an unscoped patch unconstructible) — but every
/// status-writer in this file that owns more than 1-2 ad hoc fields
/// should route through it.
fn scoped_status_patch(
    status: &crate::crd::InfrastructureTemplateStatus,
    field_names: &[&str],
) -> serde_json::Value {
    let full = serde_json::to_value(status).unwrap_or_else(|e| {
        tracing::error!(error = %e, "InfrastructureTemplateStatus failed to serialize; \
                         emitting an empty (no-op) status patch rather than risk a partial/garbled write");
        serde_json::json!({})
    });
    let mut scoped = serde_json::Map::new();
    if let Some(obj) = full.as_object() {
        for name in field_names {
            if let Some(v) = obj.get(*name) {
                scoped.insert((*name).to_string(), v.clone());
            }
        }
    }
    serde_json::json!({ "status": scoped })
}

/// The 5 status fields a plan update owns.
const PLAN_STATUS_FIELDS: &[&str] = &[
    "resources",
    "planSummary",
    "lastPlannedAt",
    "driftDetails",
    "policyEvaluation",
];

/// Pure: build the field-scoped plan-status patch body — extracted so
/// it's unit-testable without a kube client (same pattern as
/// `build_apply_status`/`build_freshness_patch`, below/above).
fn build_plan_status_patch(
    prev_status: &crate::crd::InfrastructureTemplateStatus,
    resources: Option<ResourceSummary>,
    plan_summary: Option<&str>,
    drift_details: Vec<crate::crd::DriftDetail>,
    policy_evaluation: Option<PolicyEvaluation>,
    now: chrono::DateTime<chrono::Utc>,
) -> serde_json::Value {
    let mut status = prev_status.clone();
    status.resources = resources;
    status.plan_summary = plan_summary.map(|s| s.to_string());
    status.last_planned_at = Some(now);
    status.drift_details = drift_details;
    status.policy_evaluation = policy_evaluation;

    scoped_status_patch(&status, PLAN_STATUS_FIELDS)
}

/// Update status after a successful plan.
///
/// Field-scoped status patch (see `scoped_status_patch` +
/// `update_phase`'s doc comment for the full clobber-class
/// explanation) — write ONLY the 5 fields a plan OWNS. The previous
/// `json!({ "status": status })` shape re-sent the ENTIRE in-memory
/// status — sourced from `template.status.clone()`, the immutable
/// start-of-reconcile snapshot — which silently clobbered
/// `observedHeadRevision`/`lastFreshnessCheckAt` back to their stale
/// pre-reconcile values: `source_freshness_gate` (called earlier in
/// the SAME reconcile pass, in `handle_planning`) had already written
/// those fields fresh to the API server via `update_freshness_status`,
/// but that write never reaches this function's local `template`, so
/// re-sending the whole struct reverted it on (almost) every Planning
/// reconcile — the same clobber class `update_phase` was already
/// fixed for, just never migrated here.
pub async fn update_plan_status(
    template: &InfrastructureTemplate,
    resources: Option<ResourceSummary>,
    plan_summary: Option<&str>,
    drift_details: Vec<crate::crd::DriftDetail>,
    policy_evaluation: Option<PolicyEvaluation>,
    state: &ControllerState,
) -> Result<()> {
    let prev_status = template.status.clone().unwrap_or_default();
    let patch = build_plan_status_patch(
        &prev_status,
        resources,
        plan_summary,
        drift_details,
        policy_evaluation,
        Utc::now(),
    );

    crate::controller::status_patch::patch_status(template, &state.client, patch).await?;

    Ok(())
}

/// Update status after a successful apply.
/// Clears the pending/approved plan hashes, records the revision the
/// apply realized (threads `status.compiledRevision` into
/// `status.lastAppliedRevision` → the `ReconcileCycle.sourceRevision`
/// chain, previously dead — always `None` in production), and forces
/// the conditions to the Ready set so external observers see the new
/// posture.
pub async fn update_apply_status(
    template: &InfrastructureTemplate,
    outputs: Option<std::collections::BTreeMap<String, serde_json::Value>>,
    revision: Option<&str>,
    state: &ControllerState,
) -> Result<()> {
    let status = build_apply_status(
        template.status.clone().unwrap_or_default(),
        outputs,
        revision,
    );

    let patch = serde_json::json!({ "status": status });

    crate::controller::status_patch::patch_status(template, &state.client, patch).await?;

    Ok(())
}

/// Pure post-apply status construction — extracted from
/// `update_apply_status` so the lastAppliedRevision wiring is unit-
/// testable without a kube client (same pattern as the other pure
/// helpers in this module).
fn build_apply_status(
    mut status: crate::crd::InfrastructureTemplateStatus,
    outputs: Option<std::collections::BTreeMap<String, serde_json::Value>>,
    revision: Option<&str>,
) -> crate::crd::InfrastructureTemplateStatus {
    status.outputs = outputs;
    status.last_applied_at = Some(Utc::now());
    let source_fresh = if let Some(rev) = revision {
        status.last_applied_revision = Some(rev.to_string());
        // We just compiled + applied the HEAD we cloned — record it as the
        // observed git edge so the post-apply Ready condition reads Fresh.
        // The next handle_ready tick re-confirms via a live ls-remote.
        status.observed_head_revision = Some(rev.to_string());
        SourceFreshState::Fresh
    } else {
        SourceFreshState::NotApplicable
    };
    // Clear approval hashes after successful apply
    status.pending_plan_hash = None;
    status.approved_plan_hash = None;
    status.conditions = conditions_for_phase(Phase::Ready, None, source_fresh);
    status
}

/// Record the revision a successful compile consumed
/// (`status.compiledRevision` + `compiledAt`). Diff-gated on the
/// revision itself: a same-rev recompile (pod restart, drift bounce)
/// keeps the original `compiledAt` and skips the PATCH entirely —
/// no etcd churn, no self-trigger watch loop. `None` (inline /
/// configMap source) is a no-op: there is no revision to be stale
/// against.
pub async fn update_compiled_revision(
    template: &InfrastructureTemplate,
    revision: Option<&str>,
    state: &ControllerState,
) -> Result<()> {
    let Some(rev) = revision else {
        return Ok(());
    };
    let prev = template
        .status
        .as_ref()
        .and_then(|s| s.compiled_revision.as_deref());
    if prev == Some(rev) {
        tracing::debug!(
            revision = rev,
            "compiledRevision unchanged; skipping patch (avoids self-trigger watch loop)"
        );
        return Ok(());
    }

    let patch = serde_json::json!({
        "status": {
            "compiledRevision": rev,
            "compiledAt": Utc::now(),
        }
    });

    crate::controller::status_patch::patch_status(template, &state.client, patch).await?;

    Ok(())
}

/// The outcome of one remote-HEAD observation attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ObservationOutcome {
    /// `ls-remote` succeeded; carries the observed remote HEAD sha.
    Observed(String),
    /// `ls-remote` failed/unreachable — the attempt happened but produced
    /// no head. The ATTEMPT clock STILL advances so a wedged probe is
    /// visibly "checking + failing", never silently frozen.
    Unobserved,
}

/// Pure: the freshness status patch for one observation attempt, or
/// `None` to skip the PATCH (avoids a watch-loop self-trigger).
///
/// - `Observed(head)` → write `observedHeadRevision` + `lastFreshnessCheckAt`;
///   skip only when head is unchanged AND the last check was <30s ago.
/// - `Unobserved` → advance ONLY `lastFreshnessCheckAt` (never touch
///   `observedHeadRevision`); skip only when <30s since the last attempt.
///
/// This is the load-bearing correction: `lastDriftCheckAt` advances every
/// cycle (`update_drift_check_timestamp`), so the freshness ATTEMPT clock
/// must advance symmetrically. A failing probe can no longer freeze
/// `observedHeadRevision`/`lastFreshnessCheckAt` for days while drift
/// checks keep ticking — the exact rio wedge signature.
#[must_use]
pub fn build_freshness_patch(
    prev_head: Option<&str>,
    prev_check: Option<chrono::DateTime<chrono::Utc>>,
    outcome: &ObservationOutcome,
    now: chrono::DateTime<chrono::Utc>,
) -> Option<serde_json::Value> {
    match outcome {
        ObservationOutcome::Observed(head) => {
            if prev_head == Some(head.as_str()) && freshness_check_too_recent(prev_check, now) {
                return None;
            }
            Some(serde_json::json!({
                "status": { "observedHeadRevision": head, "lastFreshnessCheckAt": now }
            }))
        }
        ObservationOutcome::Unobserved => {
            if freshness_check_too_recent(prev_check, now) {
                return None;
            }
            Some(serde_json::json!({
                "status": { "lastFreshnessCheckAt": now }
            }))
        }
    }
}

/// Record one freshness observation ATTEMPT: `observedHeadRevision` on a
/// successful observation, and `lastFreshnessCheckAt` ALWAYS. Diff-gated +
/// 30s-throttled via [`build_freshness_patch`].
pub async fn update_freshness_status(
    template: &InfrastructureTemplate,
    outcome: &ObservationOutcome,
    state: &ControllerState,
) -> Result<()> {
    let now = Utc::now();
    let prev_head = template
        .status
        .as_ref()
        .and_then(|s| s.observed_head_revision.as_deref());
    let prev_check = template
        .status
        .as_ref()
        .and_then(|s| s.last_freshness_check_at);
    let Some(patch) = build_freshness_patch(prev_head, prev_check, outcome, now) else {
        tracing::debug!("freshness attempt: unchanged + <30s ago; skipping patch");
        return Ok(());
    };
    crate::controller::status_patch::patch_status(template, &state.client, patch).await?;
    Ok(())
}

/// Same 30s rate-limit shape as `drift_check_too_recent` — see that
/// helper's rationale.
fn freshness_check_too_recent(
    prev: Option<chrono::DateTime<chrono::Utc>>,
    now: chrono::DateTime<chrono::Utc>,
) -> bool {
    match prev {
        Some(p) => now.signed_duration_since(p) < chrono::Duration::seconds(30),
        None => false,
    }
}

#[cfg(test)]
mod freshness_patch_tests {
    use super::{build_freshness_patch, ObservationOutcome};
    use chrono::{Duration, Utc};

    #[test]
    fn unobserved_advances_attempt_clock_without_touching_head() {
        // THE rio fix: a failed probe still bumps lastFreshnessCheckAt and
        // does NOT write observedHeadRevision — so it can't silently freeze.
        let now = Utc::now();
        let patch = build_freshness_patch(
            Some("abc"),
            Some(now - Duration::minutes(40)),
            &ObservationOutcome::Unobserved,
            now,
        )
        .expect("a stale attempt must patch the timestamp");
        let st = &patch["status"];
        assert!(
            st.get("lastFreshnessCheckAt").is_some(),
            "attempt clock advances"
        );
        assert!(
            st.get("observedHeadRevision").is_none(),
            "head untouched on a failed probe"
        );
    }

    #[test]
    fn observed_unchanged_recent_skips_to_avoid_churn() {
        let now = Utc::now();
        assert!(build_freshness_patch(
            Some("abc"),
            Some(now - Duration::seconds(5)),
            &ObservationOutcome::Observed("abc".into()),
            now
        )
        .is_none());
    }

    #[test]
    fn observed_changed_writes_head_and_clock() {
        let now = Utc::now();
        let patch = build_freshness_patch(
            Some("abc"),
            Some(now - Duration::seconds(5)),
            &ObservationOutcome::Observed("def".into()),
            now,
        )
        .expect("HEAD advance must patch");
        assert_eq!(patch["status"]["observedHeadRevision"], "def");
        assert!(patch["status"].get("lastFreshnessCheckAt").is_some());
    }

    #[test]
    fn unobserved_too_recent_skips() {
        let now = Utc::now();
        assert!(build_freshness_patch(
            Some("abc"),
            Some(now - Duration::seconds(5)),
            &ObservationOutcome::Unobserved,
            now
        )
        .is_none());
    }
}

/// The 5 status fields a settling update owns.
const SETTLING_STATUS_FIELDS: &[&str] = &[
    "consecutiveDriftCycles",
    "stuckResources",
    "driftDetails",
    "conditions",
    "lastDriftCheckAt",
];

/// Pure: build the field-scoped settling-status patch body, or `None`
/// if nothing changed (mirrors the `settling_status_needs_patch`
/// diff-gate). Extracted so it's unit-testable without a kube client
/// (same pattern as `build_apply_status`/`build_freshness_patch`/
/// `build_plan_status_patch`).
fn build_settling_status_patch(
    prev_status: &crate::crd::InfrastructureTemplateStatus,
    outcome: &crate::controller::settling::SettlingOutcome,
    drift_details: &[crate::crd::DriftDetail],
    freshness: Option<&super::freshness::Freshness>,
    now: chrono::DateTime<chrono::Utc>,
) -> Option<serde_json::Value> {
    use crate::controller::settling::SettlingOutcome;

    let cycles = outcome.cycle_count();
    let stuck_addresses: Vec<String> = match outcome {
        SettlingOutcome::StuckByFingerprint {
            stuck_addresses, ..
        }
        | SettlingOutcome::StuckByCount {
            stuck_addresses, ..
        } => stuck_addresses.clone(),
        _ => Vec::new(),
    };

    let compiled_rev = prev_status.compiled_revision.as_deref();
    let (settled_status, settled_reason, settled_msg) = match outcome {
        SettlingOutcome::Settled => (
            "True",
            "Settled".to_string(),
            settled_message(compiled_rev, freshness),
        ),
        SettlingOutcome::Progressing { cycles } => (
            "False",
            "Reconciling".to_string(),
            format!("Drift detected; reconciling (cycle {}).", cycles),
        ),
        SettlingOutcome::StuckByFingerprint {
            cycles,
            fingerprint,
            ..
        } => (
            "False",
            "StuckByFingerprint".to_string(),
            format!(
                "Drift fingerprint {} unchanged across {} cycle(s) — system is not converging.",
                fingerprint, cycles
            ),
        ),
        SettlingOutcome::StuckByCount { cycles, .. } => (
            "False",
            "StuckByCount".to_string(),
            format!(
                "Exceeded max consecutive drift cycles ({}) without settling.",
                cycles
            ),
        ),
    };

    let mut status = prev_status.clone();
    status.consecutive_drift_cycles = cycles;
    status.stuck_resources = stuck_addresses.clone();
    if !drift_details.is_empty() {
        status.drift_details = drift_details.to_vec();
    } else if matches!(outcome, SettlingOutcome::Settled) {
        // Clear stale drift details once we've settled.
        status.drift_details = Vec::new();
    }

    // Compute the would-be Settled condition from the outcome,
    // preserving lastTransitionTime when status/reason/message all
    // match the prior Settled condition (canonical condition idiom).
    let prior_settled = prev_status
        .conditions
        .iter()
        .find(|c| c.r#type == "Settled")
        .cloned();
    let new_settled_transition = match prior_settled.as_ref() {
        Some(p)
            if p.status == settled_status
                && p.reason == settled_reason
                && p.message == settled_msg =>
        {
            p.last_transition_time
        }
        _ => now,
    };
    status.conditions.retain(|c| c.r#type != "Settled");
    status.conditions.push(crate::crd::Condition {
        r#type: "Settled".to_string(),
        status: settled_status.to_string(),
        last_transition_time: new_settled_transition,
        reason: settled_reason,
        message: settled_msg,
    });

    // Diff-gate the PATCH. The Settled condition above already
    // preserves transition time when content matches; the only
    // remaining always-fresh field would be `last_drift_check_at`,
    // which we update unconditionally below since callers expect
    // it to track the most recent drift run. Bumping it only when
    // we actually PATCH avoids restamping on no-op rounds.
    if !settling_status_needs_patch(prev_status, &status) {
        return None;
    }

    status.last_drift_check_at = Some(now);

    Some(scoped_status_patch(&status, SETTLING_STATUS_FIELDS))
}

/// Persist state-settling tracking fields to status.
///
/// Updates `consecutive_drift_cycles`, `stuck_resources`, and (when
/// drift was detected) `drift_details`. Also flips the `Settled`
/// condition to reflect the current outcome — this is what an
/// external observer (Flux healthCheck, Prometheus alert, kubectl
/// describe) reads to know whether the system has actually converged.
///
/// `freshness` is the Ready-phase gate's verdict for this drift run
/// (`None` for sources with no revision to be stale against). The
/// Settled message names the compiled revision + observed HEAD so
/// "no changes" is always uttered against a NAMED commit — and an
/// `Unknown` observation says "HEAD: unverified" instead of implying
/// a verification that never happened (tier-honest: freshness is a
/// C2 observation renewed per check).
///
/// Field-scoped status patch (see `scoped_status_patch` +
/// `update_phase`'s doc comment for the full clobber-class
/// explanation) — write ONLY the 5 fields a settling update OWNS. The
/// previous `json!({ "status": status })` shape re-sent the ENTIRE
/// in-memory status — sourced from `template.status.clone()`, the
/// immutable start-of-reconcile snapshot — which silently clobbered
/// `observedHeadRevision`/`lastFreshnessCheckAt` back to their stale
/// pre-reconcile values whenever `settling_status_needs_patch` decided
/// a PATCH was due: `source_freshness_gate` (called earlier in the
/// SAME reconcile pass, in `handle_ready`) had already written those
/// fields fresh to the API server via `update_freshness_status`, but
/// that write never reaches this function's local `template`, so
/// re-sending the whole struct reverted it — the same clobber class
/// `update_phase` was already fixed for, just never migrated here.
pub async fn update_settling_status(
    template: &InfrastructureTemplate,
    outcome: &crate::controller::settling::SettlingOutcome,
    drift_details: &[crate::crd::DriftDetail],
    freshness: Option<&super::freshness::Freshness>,
    state: &ControllerState,
) -> Result<()> {
    let prev_status = template.status.clone().unwrap_or_default();
    let Some(patch) =
        build_settling_status_patch(&prev_status, outcome, drift_details, freshness, Utc::now())
    else {
        tracing::debug!(
            "Settling status unchanged; skipping patch (avoids self-trigger watch loop)"
        );
        return Ok(());
    };

    crate::controller::status_patch::patch_status(template, &state.client, patch).await?;

    Ok(())
}

/// Compose the Settled condition message. Every arm states what was
/// actually verified — never more:
///   * no compiled revision (inline/configMap, or legacy CR pre-
///     recompile): the original revision-free wording;
///   * `Fresh`: "no changes against compiled revision X (HEAD: X)";
///   * `Unknown`: "(HEAD: unverified)" — the remote was unreachable
///     this round; the claim is explicitly weaker;
///   * `Stale`: unreachable in practice (the gate bounces to
///     Compiling before settling) — named honestly anyway rather
///     than rounded up, since the type admits it.
fn settled_message(
    compiled_rev: Option<&str>,
    freshness: Option<&super::freshness::Freshness>,
) -> String {
    use super::freshness::Freshness;
    match (compiled_rev, freshness) {
        (Some(rev), Some(Freshness::Fresh)) => format!(
            "Drift check found no changes against compiled revision {rev} (HEAD: {rev})."
        ),
        (Some(rev), Some(Freshness::Unknown)) => format!(
            "Drift check found no changes against compiled revision {rev} (HEAD: unverified)."
        ),
        (Some(rev), Some(Freshness::Stale { head, .. })) => format!(
            "Drift check found no changes against compiled revision {rev} (HEAD: {head} — STALE; recompile pending)."
        ),
        (Some(rev), None) => format!(
            "Drift check found no changes against compiled revision {rev}."
        ),
        (None, _) => {
            "Drift check found no changes — desired state matches actual state.".to_string()
        }
    }
}

/// Update only the last drift check timestamp.
///
/// Diff-gate against drift-check spam: even though the drift-check
/// gate at `template_controller::handle_drift_detection` already
/// rate-limits actual drift runs to `spec.refreshInterval`, every
/// drift run that DOES execute would PATCH a fresh `Utc::now()`
/// here, refire the template watch, schedule another reconcile, and
/// burn one extra reconcile pass before the gate re-skips. Skip
/// PATCHes that would advance the timestamp by less than 30 seconds
/// — operator UX still sees a fresh-ish "last checked" value, the
/// loop fires at most twice per minute even if the upstream gate
/// loosens, and the wall-clock semantic stays correct.
pub async fn update_drift_check_timestamp(
    template: &InfrastructureTemplate,
    state: &ControllerState,
) -> Result<()> {
    let now = Utc::now();
    let prev_check = template.status.as_ref().and_then(|s| s.last_drift_check_at);
    if drift_check_too_recent(prev_check, now) {
        tracing::debug!(
            "lastDriftCheckAt advanced <30s ago; skipping patch (avoids self-trigger watch loop)"
        );
        return Ok(());
    }

    let patch = serde_json::json!({
        "status": {
            "lastDriftCheckAt": now,
        }
    });

    crate::controller::status_patch::patch_status(template, &state.client, patch).await?;

    Ok(())
}

/// Update the pending plan hash for approval workflow.
/// Always nulls the approved hash so the operator can't accidentally
/// race a stale approval against a new plan.
pub async fn update_pending_plan_hash(
    template: &InfrastructureTemplate,
    plan_hash: &str,
    state: &ControllerState,
) -> Result<()> {
    let patch = serde_json::json!({
        "status": {
            "pendingPlanHash": plan_hash,
            "approvedPlanHash": null
        }
    });

    crate::controller::status_patch::patch_status(template, &state.client, patch).await?;

    Ok(())
}

/// Map the WorkspaceCatalog's `DriftReaction` enum (gem-cascade
/// shape) to the InfrastructureTemplate's `PolicyDecision` enum. The
/// two enums describe the same intent at different cascade levels but
/// have slightly different vocabularies — `Alert` exists at the
/// workspace level (notify-but-don't-block) but maps to `AutoApply`
/// at the template level because the alerting mechanism is separate
/// from the apply gate.
/// Returns `true` iff the previous drift-check timestamp is recent
/// enough that we should skip writing a fresh one. The 30s threshold
/// is below `DEFAULT_REQUEUE_INTERVAL` (5 min) but above the typical
/// reconcile latency, so steady-state drift loops settle on at most
/// 2 PATCHes/min for the timestamp field, breaking the self-trigger
/// watch loop without losing operator UX freshness.
fn drift_check_too_recent(
    prev: Option<chrono::DateTime<chrono::Utc>>,
    now: chrono::DateTime<chrono::Utc>,
) -> bool {
    match prev {
        Some(p) => now.signed_duration_since(p) < chrono::Duration::seconds(30),
        None => false,
    }
}

/// Returns `true` iff the proposed settling status differs from
/// `prev` on any observable field (drift_details, cycles, stuck
/// resources, conditions). Compares conditions field-by-field
/// including `lastTransitionTime` (the caller already merged the
/// Settled condition's transition time when content matches, so an
/// observed timestamp diff here is meaningful). Drift-details are
/// compared via JSON value because `DriftDetail` carries attribute
/// fields that do not derive `PartialEq`.
fn settling_status_needs_patch(
    prev: &crate::crd::InfrastructureTemplateStatus,
    new: &crate::crd::InfrastructureTemplateStatus,
) -> bool {
    let drift_changed = serde_json::to_value(&prev.drift_details).ok()
        != serde_json::to_value(&new.drift_details).ok();
    let cycles_changed = prev.consecutive_drift_cycles != new.consecutive_drift_cycles;
    let stuck_changed = prev.stuck_resources != new.stuck_resources;
    let conditions_changed = prev.conditions.len() != new.conditions.len()
        || prev
            .conditions
            .iter()
            .zip(new.conditions.iter())
            .any(|(p, n)| {
                p.r#type != n.r#type
                    || p.status != n.status
                    || p.reason != n.reason
                    || p.message != n.message
                    || p.last_transition_time != n.last_transition_time
            });

    drift_changed || cycles_changed || stuck_changed || conditions_changed
}

pub fn workspace_drift_reaction_to_policy_decision(
    dr: crate::crd::architecture_gem::DriftReaction,
) -> Option<PolicyDecision> {
    use crate::crd::architecture_gem::DriftReaction as DR;
    Some(match dr {
        DR::AutoApply => PolicyDecision::AutoApply,
        DR::RequireApproval => PolicyDecision::RequireApproval,
        DR::Refuse => PolicyDecision::Refuse,
        DR::Alert => PolicyDecision::AutoApply,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crd::architecture_gem::DriftReaction;

    #[test]
    fn workspace_drift_reaction_alert_collapses_to_auto_apply() {
        // The vocab carve-out: workspace-level Alert means
        // "notify-but-don't-block". At the template level there's no
        // direct equivalent — Alert collapses to AutoApply because
        // the alert mechanism (ReactivePolicy notifications) is
        // separate from the apply gate. Drift here would silently
        // upgrade Alert → Refuse.
        assert_eq!(
            workspace_drift_reaction_to_policy_decision(DriftReaction::Alert),
            Some(PolicyDecision::AutoApply)
        );
    }

    #[test]
    fn workspace_drift_reaction_passes_through_other_decisions() {
        assert_eq!(
            workspace_drift_reaction_to_policy_decision(DriftReaction::AutoApply),
            Some(PolicyDecision::AutoApply)
        );
        assert_eq!(
            workspace_drift_reaction_to_policy_decision(DriftReaction::RequireApproval),
            Some(PolicyDecision::RequireApproval)
        );
        assert_eq!(
            workspace_drift_reaction_to_policy_decision(DriftReaction::Refuse),
            Some(PolicyDecision::Refuse)
        );
    }

    // ── drift_check_too_recent rate-limit tests ────────────────
    use super::drift_check_too_recent;
    use chrono::TimeZone;

    fn t(secs: i64) -> chrono::DateTime<chrono::Utc> {
        chrono::Utc.with_ymd_and_hms(2026, 5, 7, 12, 0, 0).unwrap()
            + chrono::Duration::seconds(secs)
    }

    #[test]
    fn drift_check_no_prev_must_patch() {
        assert!(
            !drift_check_too_recent(None, t(0)),
            "first reconcile must NOT be rate-limited"
        );
    }

    #[test]
    fn drift_check_within_30s_throttles() {
        assert!(
            drift_check_too_recent(Some(t(0)), t(15)),
            "15s after prev check must skip"
        );
        assert!(
            drift_check_too_recent(Some(t(0)), t(29)),
            "29s after prev check must still skip (just under threshold)"
        );
    }

    #[test]
    fn drift_check_at_threshold_advances() {
        assert!(
            !drift_check_too_recent(Some(t(0)), t(30)),
            "exactly 30s elapsed must allow PATCH"
        );
        assert!(
            !drift_check_too_recent(Some(t(0)), t(60)),
            "60s elapsed must allow PATCH"
        );
    }

    // ── settling_status_needs_patch tests ───────────────────────
    use super::settling_status_needs_patch;
    use crate::crd::{Condition, InfrastructureTemplateStatus};

    fn cond(typ: &str, status: &str, reason: &str, msg: &str) -> Condition {
        Condition {
            r#type: typ.into(),
            status: status.into(),
            reason: reason.into(),
            message: msg.into(),
            last_transition_time: chrono::Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).unwrap(),
        }
    }

    fn settled_status() -> InfrastructureTemplateStatus {
        InfrastructureTemplateStatus {
            consecutive_drift_cycles: 0,
            stuck_resources: vec![],
            drift_details: vec![],
            conditions: vec![cond("Settled", "True", "Settled", "no drift")],
            ..Default::default()
        }
    }

    #[test]
    fn settling_no_change_skips_patch() {
        let prev = settled_status();
        let new = settled_status();
        assert!(
            !settling_status_needs_patch(&prev, &new),
            "byte-equal settling status must skip patch"
        );
    }

    #[test]
    fn settling_cycles_advance_must_patch() {
        let prev = settled_status();
        let mut new = settled_status();
        new.consecutive_drift_cycles = 1;
        assert!(
            settling_status_needs_patch(&prev, &new),
            "consecutive_drift_cycles change must force PATCH"
        );
    }

    #[test]
    fn settling_settled_to_stuck_must_patch() {
        let prev = settled_status();
        let mut new = settled_status();
        // condition flipped from Settled=True to Settled=False
        new.conditions = vec![cond(
            "Settled",
            "False",
            "StuckByCount",
            "Exceeded max cycles",
        )];
        assert!(
            settling_status_needs_patch(&prev, &new),
            "Settled True→False (status flip) must force PATCH"
        );
    }

    #[test]
    fn settling_new_stuck_resource_must_patch() {
        let prev = settled_status();
        let mut new = settled_status();
        new.stuck_resources = vec!["aws_eks_cluster.main".into()];
        assert!(
            settling_status_needs_patch(&prev, &new),
            "stuck_resources change must force PATCH"
        );
    }

    // ── build_apply_status — the lastAppliedRevision wiring ──────
    use super::build_apply_status;

    #[test]
    fn apply_status_writes_last_applied_revision() {
        // Kills the always-None production path: the revision the
        // apply realized lands on status, where the cycle receipt
        // (`cycle_receipts::record_reconcile_cycle`) reads it as
        // `sourceRevision`.
        let prior = InfrastructureTemplateStatus {
            compiled_revision: Some("abc123".into()),
            ..Default::default()
        };
        let out = build_apply_status(prior, None, Some("abc123"));
        assert_eq!(out.last_applied_revision.as_deref(), Some("abc123"));
        assert!(out.last_applied_at.is_some());
        assert!(out.pending_plan_hash.is_none());
        assert!(out.approved_plan_hash.is_none());
    }

    #[test]
    fn apply_status_without_revision_preserves_prior() {
        // Inline / configMap sources have no revision; an apply must
        // not clobber whatever lastAppliedRevision already recorded.
        let prior = InfrastructureTemplateStatus {
            last_applied_revision: Some("prior".into()),
            ..Default::default()
        };
        let out = build_apply_status(prior, None, None);
        assert_eq!(out.last_applied_revision.as_deref(), Some("prior"));
    }

    // ── build_plan_status_patch / build_settling_status_patch — the
    // freshness-field clobber regression (2026-07-12). Fails against
    // the pre-fix shape (`json!({ "status": template.status.clone() })`
    // sent the WHOLE struct, including whatever `observedHeadRevision`/
    // `lastFreshnessCheckAt` happened to be sitting on the immutable
    // start-of-reconcile snapshot); passes once the patch body is
    // field-scoped to only the fields the writer owns. ──────────────
    use super::{build_plan_status_patch, build_settling_status_patch};

    /// A status carrying freshness fields as they'd look mid-reconcile:
    /// `source_freshness_gate` observed a NEW head moments ago and
    /// PATCHed it straight to the API server, but this in-memory
    /// snapshot — built at the TOP of the reconcile, before that PATCH
    /// — still carries the STALE pre-gate values. A correct writer
    /// downstream in the same reconcile must never re-emit either key.
    fn status_with_stale_freshness_snapshot() -> InfrastructureTemplateStatus {
        InfrastructureTemplateStatus {
            observed_head_revision: Some("stale-pre-gate-sha".into()),
            last_freshness_check_at: Some(
                chrono::Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap(),
            ),
            compiled_revision: Some("compiled-sha".into()),
            ..Default::default()
        }
    }

    #[test]
    fn plan_status_patch_never_touches_freshness_fields() {
        let prev = status_with_stale_freshness_snapshot();
        let patch = build_plan_status_patch(&prev, None, Some("2 to add"), Vec::new(), None, t(0));
        let status_obj = patch["status"]
            .as_object()
            .expect("patch must carry a status object");
        assert!(
            !status_obj.contains_key("observedHeadRevision"),
            "a plan-status patch must never emit observedHeadRevision — \
             it does not own that field and would clobber \
             source_freshness_gate's fresher write from earlier in the \
             same reconcile; got: {status_obj:?}"
        );
        assert!(
            !status_obj.contains_key("lastFreshnessCheckAt"),
            "a plan-status patch must never emit lastFreshnessCheckAt; \
             got: {status_obj:?}"
        );
        // The fields it DOES own must still be present.
        assert_eq!(
            status_obj.get("planSummary").and_then(|v| v.as_str()),
            Some("2 to add")
        );
        assert!(status_obj.contains_key("lastPlannedAt"));
        assert!(status_obj.contains_key("driftDetails"));
    }

    #[test]
    fn settling_status_patch_never_touches_freshness_fields() {
        use crate::controller::settling::SettlingOutcome;

        let prev = status_with_stale_freshness_snapshot();
        let patch = build_settling_status_patch(
            &prev,
            &SettlingOutcome::Progressing { cycles: 1 },
            &[crate::crd::DriftDetail {
                address: "aws_eks_cluster.main".into(),
                action: "update".into(),
                risk: "low".into(),
                attributes: Default::default(),
                policy_decision: None,
                matched_policy: None,
            }],
            None,
            t(0),
        )
        .expect("a Settled-condition flip with new drift must always patch");
        let status_obj = patch["status"]
            .as_object()
            .expect("patch must carry a status object");
        assert!(
            !status_obj.contains_key("observedHeadRevision"),
            "a settling-status patch must never emit observedHeadRevision — \
             it does not own that field and would clobber \
             source_freshness_gate's fresher write from earlier in the \
             same reconcile; got: {status_obj:?}"
        );
        assert!(
            !status_obj.contains_key("lastFreshnessCheckAt"),
            "a settling-status patch must never emit lastFreshnessCheckAt; \
             got: {status_obj:?}"
        );
        // The fields it DOES own must still be present.
        assert!(status_obj.contains_key("consecutiveDriftCycles"));
        assert!(status_obj.contains_key("driftDetails"));
        assert!(status_obj.contains_key("conditions"));
        assert!(status_obj.contains_key("lastDriftCheckAt"));
    }

    // ── settled_message — "no changes" against a NAMED commit ────
    use super::super::freshness::Freshness;
    use super::settled_message;

    #[test]
    fn settled_message_fresh_names_the_revision() {
        let msg = settled_message(Some("abc123"), Some(&Freshness::Fresh));
        assert!(msg.contains("compiled revision abc123"), "got: {msg}");
        assert!(msg.contains("(HEAD: abc123)"), "got: {msg}");
    }

    #[test]
    fn settled_message_unknown_admits_unverified_head() {
        // The C2 honesty arm: an unreachable remote must NOT imply a
        // verification that never happened.
        let msg = settled_message(Some("abc123"), Some(&Freshness::Unknown));
        assert!(msg.contains("(HEAD: unverified)"), "got: {msg}");
    }

    #[test]
    fn settled_message_without_revision_keeps_legacy_wording() {
        let msg = settled_message(None, None);
        assert!(
            msg.contains("desired state matches actual state"),
            "got: {msg}"
        );
    }

    // ── freshness_check_too_recent throttle ──────────────────────
    use super::freshness_check_too_recent;

    #[test]
    fn freshness_check_throttle_mirrors_drift_check_shape() {
        assert!(
            !freshness_check_too_recent(None, t(0)),
            "first check must patch"
        );
        assert!(
            freshness_check_too_recent(Some(t(0)), t(15)),
            "15s later skips"
        );
        assert!(
            !freshness_check_too_recent(Some(t(0)), t(30)),
            "30s later patches"
        );
    }
}
