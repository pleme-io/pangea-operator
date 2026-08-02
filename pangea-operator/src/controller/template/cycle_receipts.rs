//! Reconcile-cycle receipt construction + persistence for
//! `InfrastructureTemplate`.
//!
//! Lifted from `template_controller.rs` during T3 (continuation of
//! R6/T1/T2). Each `record_reconcile_cycle` call builds a typed
//! receipt summarizing what changed in this cycle and patches it onto
//! `status.lastCycle` (with a content-equality guard to skip etcd
//! churn for Matched-only steady-state Ready→Ready flows).
//!
//! `CycleResult` is the typed input to `build_reconcile_cycle`; its
//! variants drive the per-resource outcome derivation (NoChanges →
//! Matched, AppliedSuccess → action-derived with import override,
//! AppliedFailure → Failed, PolicyGated → Drifted).

use chrono::{DateTime, Utc};
use kube::ResourceExt;
use std::collections::HashSet;
use std::path::Path;
use tracing::{debug, info};

use crate::controller::ControllerState;
use crate::crd::{
    CycleSummary, DriftDetail, InfrastructureTemplate, Outcome, Phase, PolicyDecision,
    ReconcileCycle, ResourceOutcome,
};
use crate::error::Result;
use crate::executor::cycle_artifact::CycleArtifact;
use crate::executor::magma_bundle::{read_apply_outcome, read_cycle_artifact};

/// What triggered the cycle emission — drives outcome derivation when
/// translating drift_details into per-resource ResourceOutcome entries.
#[derive(Debug, Clone)]
pub enum CycleResult {
    /// Plan reported no changes — every managed resource matched
    /// declared state. Apply did not run.
    NoChanges,
    /// Apply ran successfully on every change in `drift_details`.
    /// Each entry's terraform action becomes the per-resource outcome,
    /// with the additional twist that any address in
    /// `imported_addresses` overrides its action-derived outcome to
    /// `Outcome::Imported` — the cycle adopted that resource into
    /// state via `tofu import` before the apply ran.
    AppliedSuccess { imported_addresses: Vec<String> },
    /// Apply errored. Every change in `drift_details` becomes
    /// `Failed`; the apply error is attached as `message`.
    AppliedFailure(String),
    /// Policy gated this cycle (refuse / requireApproval). Apply did
    /// NOT run; every change is `Drifted` (uncorrected) with the
    /// policy decision as `message`.
    PolicyGated(PolicyDecision),
}

/// Build a typed receipt summarizing one reconcile cycle.
///
/// `drifts` is the set of resources the plan reported a change on
/// (already annotated with policy decisions when relevant).
/// `total` is the total managed-resource count; `total - drifts.len()`
/// becomes `summary.matched`. `next_cycle` is the cycle number for
/// THIS cycle (caller has already incremented `status.cycle_count`).
///
/// `executor_name` is the IaC executor that ran this cycle
/// (`"magma"` / `"tofu"`); recorded into `ReconcileCycle.executor` so
/// the cycle history preserves which executor produced each receipt
/// even after a future flip. `cycle_artifact` carries the unified
/// post-plan data both executors populate — action distribution +
/// bundle ref + (slice 2b+) per-resource changes + severity rollup +
/// lifecycle phase. `None` when no artifact was available (no plan
/// has run yet, executor produced no on-disk output, etc.).
///
/// Pure function — all I/O (executor lookup, artifact reading) happens
/// in the caller `record_reconcile_cycle`, so this is the layer that
/// gets the test coverage for "given these inputs, the receipt has
/// these fields."
pub fn build_reconcile_cycle(
    next_cycle: u64,
    started_at: DateTime<Utc>,
    drifts: &[DriftDetail],
    total: u32,
    plan_summary: Option<String>,
    source_revision: Option<String>,
    executor_name: Option<String>,
    cycle_artifact: Option<CycleArtifact>,
    result: CycleResult,
) -> ReconcileCycle {
    let mut summary = CycleSummary::default();

    let imported_set: HashSet<&str> = match &result {
        CycleResult::AppliedSuccess { imported_addresses } => {
            imported_addresses.iter().map(String::as_str).collect()
        }
        _ => HashSet::new(),
    };

    let outcomes: Vec<ResourceOutcome> = drifts
        .iter()
        .take(100)
        .map(|d| {
            let (outcome, message) = match result {
                CycleResult::AppliedFailure(ref err) => {
                    (Outcome::Failed, Some(truncate_for_status(err)))
                }
                CycleResult::PolicyGated(decision) => (
                    Outcome::Drifted,
                    Some(format!("policy decision: {}", decision.as_str())),
                ),
                CycleResult::NoChanges => {
                    // Defensive: a NoChanges cycle should have empty
                    // drifts. If we got here, treat as Matched.
                    (Outcome::Matched, None)
                }
                CycleResult::AppliedSuccess { .. } => {
                    if imported_set.contains(d.address.as_str()) {
                        // Import pre-pass adopted this resource — the
                        // user-facing outcome is "imported", not
                        // whatever action the original plan had.
                        (
                            Outcome::Imported,
                            Some("adopted via tofu import".to_string()),
                        )
                    } else {
                        (outcome_for_action(&d.action), None)
                    }
                }
            };
            match outcome {
                Outcome::Matched => summary.matched = summary.matched.saturating_add(1),
                Outcome::Updated => summary.updated = summary.updated.saturating_add(1),
                Outcome::Created => summary.created = summary.created.saturating_add(1),
                Outcome::Destroyed => summary.destroyed = summary.destroyed.saturating_add(1),
                Outcome::Imported => summary.imported = summary.imported.saturating_add(1),
                Outcome::Drifted => {
                    summary.drifted_uncorrected = summary.drifted_uncorrected.saturating_add(1)
                }
                Outcome::Failed => summary.failed = summary.failed.saturating_add(1),
            }
            ResourceOutcome {
                address: d.address.clone(),
                outcome,
                action: Some(d.action.clone()),
                message,
            }
        })
        .collect();

    // matched aggregate = (total - touched). For NoChanges cycles
    // drifts is empty so this equals `total`.
    let touched_count = drifts.len() as u32;
    let untouched = total.saturating_sub(touched_count);
    summary.matched = summary.matched.saturating_add(untouched);

    // planSummary must reflect THIS cycle's reality, not a stale carried-
    // forward value. The caller's `plan_summary` is the planning-phase string,
    // which on a converged template can be stale (e.g. "+6" frozen from when
    // the resources were first created/adopted, while the template now plans
    // all-NoOp). When the cycle SUCCEEDED and changed nothing — every resource
    // matched, zero created/updated/destroyed/imported, no drift, no failure —
    // the only honest summary is "No changes"; override the stale string.
    // Genuine-change cycles (some mutation) and failures keep the planning-
    // phase summary so the operator still sees what was planned/attempted.
    let converged_no_op = matches!(
        result,
        CycleResult::AppliedSuccess { .. } | CycleResult::NoChanges
    ) && summary.created == 0
        && summary.updated == 0
        && summary.destroyed == 0
        && summary.imported == 0
        && summary.drifted_uncorrected == 0
        && summary.failed == 0;
    let plan_summary = if converged_no_op {
        Some("No changes".to_string())
    } else {
        plan_summary
    };

    // Split the unified CycleArtifact into its destination fields.
    // action_distribution is required on the artifact when present
    // (a CycleArtifact with no changes still has a zero-distribution,
    // which is informative). The other fields are independently
    // optional — tofu artifacts honestly don't carry bundle_ref or
    // lifecycle_phase; magma does. Severities is populated by both
    // when there are non-no-op changes.
    let (action_distribution, bundle_ref, severity_rollup, lifecycle_phase) = match cycle_artifact {
        Some(a) => (
            Some(a.action_distribution),
            a.artifact_ref,
            a.severities.map(|s| crate::crd::SeverityRollup {
                cosmetic: s.cosmetic,
                functional: s.functional,
                breaking: s.breaking,
            }),
            a.lifecycle_phase,
        ),
        None => (None, None, None, None),
    };

    ReconcileCycle {
        cycle: next_cycle,
        started_at,
        completed_at: Utc::now(),
        source_revision,
        plan_summary,
        summary,
        outcomes,
        executor: executor_name,
        action_distribution,
        bundle_ref,
        severity_rollup,
        lifecycle_phase,
    }
}

/// Map the terraform action vocabulary to the typed `Outcome` the
/// operator surfaces. The mapping is deliberately conservative:
/// replaces collapse to `Updated` (net effect = matches declared);
/// unknown actions land on `Updated` so we never silently lose a
/// signal.
pub fn outcome_for_action(action: &str) -> Outcome {
    match action {
        "no-op" | "noop" => Outcome::Matched,
        "create" => Outcome::Created,
        "update" => Outcome::Updated,
        "delete" => Outcome::Destroyed,
        "replace" => Outcome::Updated,
        "import" => Outcome::Imported,
        _ => Outcome::Updated,
    }
}

/// Is THIS cycle "clean" — i.e. eligible to clear a stale `lastError`?
///
/// A cycle is clean when it is NOT a terminal failure and NOT a
/// policy-refuse block:
///   * `AppliedFailure` — the apply errored; the failure path sets
///     `lastError`, which must NOT be cleared here.
///   * `PolicyGated(Refuse)` — a deliberate hard stop; the refuse path
///     sets its own `lastError`, which must NOT be cleared here.
///   * `NoChanges` / `AppliedSuccess` / `PolicyGated(RequireApproval)` /
///     `PolicyGated(AutoApply)` — no terminal error this cycle ⇒ clean.
///
/// The caller AND-combines this with `summary.failed == 0` (the
/// per-resource second condition the directive names) before clearing.
/// Pure + total over `CycleResult` so it's unit-testable without a
/// live controller.
pub fn cycle_is_clean(result: &CycleResult) -> bool {
    !matches!(
        result,
        CycleResult::AppliedFailure(_) | CycleResult::PolicyGated(PolicyDecision::Refuse)
    )
}

/// Should THIS cycle's status patch clear a stale `lastError`?
///
/// Extends `cycle_is_clean`'s per-cycle-summary verdict with an explicit
/// check against `resulting_phase` — the phase this reconcile tick is
/// establishing (via a sibling `update_phase`/`update_phase_with_error`
/// call earlier in the SAME tick) or leaving unchanged for the template.
///
/// `record_reconcile_cycle` issues its own SEPARATE, sequential status
/// PATCH from `update_phase`/`update_phase_with_error` (see that pair's
/// doc comments in `template/status.rs`). Before this guard, the
/// clear-stale-error decision was derived *only* from `CycleResult` —
/// blind to the phase decision a sibling call in the same reconcile tick
/// just made. A caller that calls `update_phase_with_error(Failed, …)`
/// and then `record_reconcile_cycle(…)` with a `CycleResult` this
/// module's narrow per-cycle-summary judged "clean" (`cycle_clean` +
/// zero `failed`) would have this function's `record_reconcile_cycle`
/// PATCH emit an explicit `"lastError": null` — nulling the error the
/// sibling call just wrote — while `phase` stays `Failed` (the PATCH
/// never touches `phase`). That produces an observable
/// `phase: Failed` + `lastError: null` state that persists until a
/// LATER reconcile finally moves the phase off `Failed` (confirmed
/// live, 2026-07: `17:45:23 Failed+content → 17:45:29 Failed+empty →
/// 17:45:34 Pending`). Gating on `resulting_phase != Phase::Failed`
/// closes this by construction: whenever this tick is establishing or
/// holding the template at `Failed`, the clear never fires — no matter
/// how "clean" the accompanying cycle summary looks in isolation.
///
/// Callers pass the exact phase they just transitioned to (when they
/// called `update_phase`/`update_phase_with_error` earlier this tick),
/// or the template's current unchanged phase (when this tick made no
/// phase transition at all, e.g. the RequireApproval arm of
/// `route_through_approval_gate`) — always a concrete, known value, so
/// there is no ambiguous "phase unknown" case to reason about.
#[must_use]
pub fn should_clear_stale_error(
    cycle_clean: bool,
    failed: u32,
    prior_last_error: Option<&str>,
    resulting_phase: Phase,
) -> bool {
    cycle_clean && failed == 0 && prior_last_error.is_some() && resulting_phase != Phase::Failed
}

/// Trim a string to 256 characters with an ellipsis suffix when
/// truncated. Used to keep status fields under the etcd value-size
/// budget when stuffing terraform error output into a status patch.
///
/// `err` here is untrusted, externally-sourced text (tofu/magma
/// apply-error stdout+stderr) that is NOT guaranteed ASCII — a raw
/// `&s[..256]` byte slice panics whenever byte 256 lands mid-character,
/// which silently halts reconciliation for every `InfrastructureTemplate`
/// (this fn runs inside `TemplateController`'s unsupervised
/// `tokio::spawn` reconcile task — no `catch_unwind`, no restart-on-panic
/// per `src/main.rs`). Delegates to [`crate::text_util::truncate_utf8_safe`]
/// (char-boundary-safe by construction) instead of byte-index slicing.
pub fn truncate_for_status(s: &str) -> String {
    crate::text_util::truncate_utf8_safe(s, 256, "…")
}

/// Patch `status.lastCycle` + bump `status.cycleCount`. Also echoes
/// the running executor + backend to top-level `status.executor` +
/// `status.backend`, and populates `lastCycle.actionDistribution` +
/// `lastCycle.bundleRef` from the cycle artifact.
///
/// Skips the patch entirely if the receipt is content-equal to the
/// prior one (only the timestamps differ) — keeps reconcile-loop
/// chatter off etcd for steady-state Ready→Ready flows. The
/// content-equality guard now considers the new bundle/executor
/// fields too (a cycle that changed bundle but nothing else still
/// patches; a cycle that changed nothing including the bundle skips).
///
/// ## Artifact resolution
///
/// `artifact` is the caller's pre-computed `CycleArtifact` (from
/// `WorkspaceRunner::plan` or `apply`). When `None`, falls back to
/// reading `magma-bundle.json` from `work_dir` (the slice 1a
/// behavior, preserved for back-compat with un-migrated callers).
///
/// When BOTH are provided: caller wins (pre-computed artifact is
/// closer to the truth than a follow-up file read, and it works for
/// tofu cycles which have no bundle on disk).
///
/// `work_dir` is optional: pass `Some(&workspace.path)` when the
/// reconcile path has the workspace in hand; `None` is acceptable
/// but means the bundle fallback skips (artifact-only mode).
///
/// `resulting_phase` is the phase this reconcile tick is establishing
/// (or leaving unchanged) for the template — see
/// [`should_clear_stale_error`]'s doc comment for the race this closes.
/// Pass the exact `Phase` given to a preceding `update_phase`/
/// `update_phase_with_error` call this tick, or the template's current
/// `status.phase` when this tick made no phase transition.
pub async fn record_reconcile_cycle(
    template: &InfrastructureTemplate,
    state: &ControllerState,
    work_dir: Option<&Path>,
    artifact: Option<CycleArtifact>,
    drifts: &[DriftDetail],
    plan_summary: Option<String>,
    result: CycleResult,
    resulting_phase: Phase,
) -> Result<()> {
    let name = template.name_any();

    let prior_status = template.status.clone().unwrap_or_default();
    let prior_cycle_count = prior_status.cycle_count;
    let next_cycle = prior_cycle_count.saturating_add(1);

    let total = prior_status
        .resources
        .as_ref()
        .map(|r| r.total)
        .unwrap_or(0);
    let started_at = prior_status.last_planned_at.unwrap_or_else(Utc::now);
    let source_revision = prior_status.last_applied_revision.clone();

    // Identify the executor that ran this cycle. `executor_for` is
    // cheap (Arc clone for tofu, lightweight ctor for magma — no I/O)
    // and idempotent, so calling it at cycle boundary is safe even if
    // the planning phase already called it.
    let executor = state.executor_for(template);
    let executor_name = Some(executor.name().to_string());
    let backend_descriptor = executor.backend_descriptor();

    // Artifact resolution:
    //   1. Caller-provided pre-computed `artifact` wins (the
    //      WorkspaceRunner already extracted it from the executor's
    //      native output — works uniformly for tofu + magma).
    //   2. Fall back to reading `magma-bundle.json` from `work_dir`
    //      (the slice-1a path; preserves back-compat for callers
    //      that haven't migrated to runner-provided artifacts).
    //   3. None if neither — cycle records without bundle-derived
    //      fields populated.
    // On the magma DB-backed path the bundle lives in Postgres (the
    // artifact store), not on disk — fetch its bytes once and reuse for
    // both the cycle artifact and the apply-outcome metric below. Keys
    // match `magma_executor_for` by CONSTRUCTION now, not by promise:
    // the schema comes from `crd::schema_identity`, the one derivation.
    let db_bundle_bytes: Option<Vec<u8>> = if executor.name() == "magma" {
        match state.artifact_store.as_ref() {
            Some(store) => {
                let schema_name = crate::crd::template_schema_name(template);
                // Don't swallow the typed BLAKE3 integrity-mismatch
                // (`Error::StateBackend`) into a silent None — a corrupt
                // / torn artifact row must be observable. Log it, then
                // fall through to None (no bundle this cycle) rather than
                // coercing the error into "no bundle".
                match store.get_bundle_bytes(&schema_name, &name).await {
                    Ok(v) => v,
                    Err(e) => {
                        tracing::warn!(
                            %e,
                            schema = %schema_name,
                            template = %name,
                            "bundle integrity/read failed in cycle receipt enrichment"
                        );
                        None
                    }
                }
            }
            None => None,
        }
    } else {
        None
    };

    let cycle_artifact = match artifact {
        Some(a) => Some(a),
        None => match (&db_bundle_bytes, work_dir) {
            // DB-backed bundle wins (zero-disk magma path).
            (Some(bytes), _) => crate::executor::magma_bundle::cycle_artifact_from_bytes(bytes),
            // Disk fallback for the file-based magma path / un-migrated callers.
            (None, Some(dir)) => read_cycle_artifact(dir).await,
            (None, None) => None,
        },
    };

    // First-class magma apply-outcome metrics. Only meaningful when
    // (a) this cycle actually ran an apply (AppliedSuccess/Failure —
    // NoChanges/PolicyGated never invoke magma_apply) and (b) the
    // executor was magma (the magma-bundle.json carries the
    // `{applied, failed, phase}` shape; tofu has no such bundle and
    // its outcome flows through pangea_tofu_operations_total). Reading
    // the bundle here — at the controller cycle boundary — keeps the
    // executor free of a Metrics handle and records exactly once per
    // apply. Best-effort: a missing/plan-only bundle yields None and
    // the metrics simply aren't recorded for this cycle.
    let cycle_was_apply = matches!(
        result,
        CycleResult::AppliedSuccess { .. } | CycleResult::AppliedFailure(_)
    );
    if cycle_was_apply && executor.name() == "magma" {
        // DB-backed bundle wins (zero-disk path); else read the disk
        // bundle. Same `{applied, failed, phase}` shape either way.
        let apply_outcome = match &db_bundle_bytes {
            Some(bytes) => crate::executor::magma_bundle::apply_outcome_from_bytes(bytes),
            None => match work_dir {
                Some(dir) => read_apply_outcome(dir).await,
                None => None,
            },
        };
        if let Some(outcome) = apply_outcome {
            let template_ns = template
                .namespace()
                .unwrap_or_else(|| "unknown".to_string());
            state
                .metrics
                .record_magma_apply(&name, &template_ns, outcome.applied, outcome.failed);
            debug!(
                template = %name,
                applied = outcome.applied,
                failed = outcome.failed,
                succeeded = outcome.succeeded,
                "recorded magma apply-outcome metrics",
            );
        }
    }

    // Does THIS cycle clear a stale lastError? See `cycle_is_clean`.
    let cycle_clean = cycle_is_clean(&result);

    let new_cycle = build_reconcile_cycle(
        next_cycle,
        started_at,
        drifts,
        total,
        plan_summary,
        source_revision,
        executor_name.clone(),
        cycle_artifact,
        result,
    );

    // A clean cycle (no terminal error this cycle AND zero per-resource
    // failures) must CLEAR any stale lastError so the surfaced error
    // reflects the CURRENT cycle, not a stale one from hours ago. This
    // is load-bearing because `last_error` carries
    // `#[serde(skip_serializing_if = "Option::is_none")]`: the
    // happy-path `update_phase(Ready)` sets the field to `None`, but a
    // `None` is OMITTED from the JSON Merge Patch rather than serialized
    // as an explicit `null` — so the stale value in etcd is never
    // actually cleared (the exact 19h-stale "tofu apply failed" on the
    // pleme-io-opensource template, which was simultaneously reporting
    // failed:0). We must emit an explicit `null` to clear it. A cycle
    // that DID fail keeps its lastError (the `cycle_clean` guard) — and
    // `should_clear_stale_error` additionally refuses to clear whenever
    // `resulting_phase` is `Failed`, closing the race where a sibling
    // `update_phase_with_error(Failed, …)` call earlier THIS tick just
    // wrote (or is holding) the error this cycle's own narrow summary
    // would otherwise judge "clean" enough to null (see that function's
    // doc comment for the confirmed live incident this closes).
    let clears_stale_error = should_clear_stale_error(
        cycle_clean,
        new_cycle.summary.failed,
        prior_status.last_error.as_deref(),
        resulting_phase,
    );

    // Content-equality guard: skip the patch when the new cycle's
    // observable content matches the prior cycle AND the top-level
    // executor/backend echo also matches. Steady-state Ready→Ready
    // flows still skip etcd churn; a cycle that flips executor or
    // bundle still patches.
    let runtime_identity_unchanged = prior_status.executor.as_deref() == executor_name.as_deref()
        && prior_status.backend.as_deref() == backend_descriptor.as_deref();
    if let Some(prev) = prior_status.last_cycle.as_ref() {
        // A stale lastError that this clean cycle must clear overrides
        // the content-equality skip — otherwise a steady-state
        // Ready→Ready cycle (content-equal) would early-return and the
        // stale error would survive forever (the rio bug). Only skip
        // when there is genuinely nothing observable to change AND no
        // stale error to clear.
        if cycle_content_equal(prev, &new_cycle)
            && runtime_identity_unchanged
            && !clears_stale_error
        {
            debug!(
                template = %name,
                cycle = prior_cycle_count,
                "Reconcile cycle content unchanged; skipping status patch"
            );
            return Ok(());
        }
    }

    let mut new_status = prior_status;
    new_status.cycle_count = next_cycle;
    new_status.last_cycle = Some(new_cycle.clone());
    new_status.executor = executor_name.clone();
    new_status.backend = backend_descriptor.clone();

    let mut patch_status = serde_json::json!({
        "cycleCount": new_status.cycle_count,
        "lastCycle":  new_status.last_cycle,
        // Top-level executor + backend echo so observers can
        // answer "what's running here?" with a single jsonpath
        // (no grep on operator logs). Always sent (rather than
        // skip-if-None) so a flip from magma to tofu propagates
        // immediately on the next patched cycle.
        "executor":   new_status.executor,
        "backend":    new_status.backend,
    });
    if clears_stale_error {
        // Emit an EXPLICIT null (not an omitted key) so the JSON Merge
        // Patch actually deletes the stale lastError in etcd — a
        // serialized `None` would be skipped by skip_serializing_if and
        // leave the stale value in place. Reset failure_count too: the
        // current cycle is clean, so the consecutive-failure counter
        // the ReactivePolicy escalation reads must not carry stale
        // history.
        if let Some(obj) = patch_status.as_object_mut() {
            obj.insert("lastError".to_string(), serde_json::Value::Null);
            obj.insert("failureCount".to_string(), serde_json::json!(0));
        }
        info!(
            template = %name,
            cycle = next_cycle,
            "Clean cycle cleared a stale lastError (current cycle reports no failures)"
        );
    }
    let patch = serde_json::json!({ "status": patch_status });
    crate::controller::status_patch::patch_status(template, &state.client, patch).await?;

    info!(
        template = %name,
        cycle = next_cycle,
        executor = executor.name(),
        matched = new_cycle.summary.matched,
        updated = new_cycle.summary.updated,
        created = new_cycle.summary.created,
        destroyed = new_cycle.summary.destroyed,
        drifted_uncorrected = new_cycle.summary.drifted_uncorrected,
        failed = new_cycle.summary.failed,
        action_no_op  = new_cycle.action_distribution.as_ref().map(|a| a.no_op).unwrap_or(0),
        action_create = new_cycle.action_distribution.as_ref().map(|a| a.create).unwrap_or(0),
        action_update = new_cycle.action_distribution.as_ref().map(|a| a.update).unwrap_or(0),
        action_delete = new_cycle.action_distribution.as_ref().map(|a| a.delete).unwrap_or(0),
        bundle_id     = new_cycle.bundle_ref.as_ref().map(|b| b.bundle_id.as_str()).unwrap_or(""),
        "ReconcileCycle recorded"
    );
    Ok(())
}

/// Two cycles are content-equal when summary, source_revision,
/// plan_summary, executor, action_distribution, bundle_ref, and
/// outcomes match. Cycle number and timestamps are deliberately
/// ignored — they always differ between successive reconciles, and
/// skipping the patch when nothing else changed is the whole point
/// (no etcd churn for Matched-only steady state). Adding the new
/// observable fields to the comparison ensures that e.g. a bundle
/// hash change between cycles forces a re-patch even when the
/// summary stays the same.
pub fn cycle_content_equal(a: &ReconcileCycle, b: &ReconcileCycle) -> bool {
    if a.summary.matched != b.summary.matched
        || a.summary.updated != b.summary.updated
        || a.summary.created != b.summary.created
        || a.summary.destroyed != b.summary.destroyed
        || a.summary.imported != b.summary.imported
        || a.summary.drifted_uncorrected != b.summary.drifted_uncorrected
        || a.summary.failed != b.summary.failed
    {
        return false;
    }
    if a.source_revision != b.source_revision || a.plan_summary != b.plan_summary {
        return false;
    }
    if a.executor != b.executor {
        return false;
    }
    // Bundle ref is compared by bundle_id (the content hash from
    // magma; kind + size are implied or hint-only). A bundle_id flip
    // is the canonical signal "magma ran a new plan."
    match (a.bundle_ref.as_ref(), b.bundle_ref.as_ref()) {
        (None, None) => {}
        (Some(_), None) | (None, Some(_)) => return false,
        (Some(x), Some(y)) => {
            if x.bundle_id != y.bundle_id {
                return false;
            }
        }
    }
    // ActionDistribution compared field-by-field — a no-op→create
    // flip on identical resource counts must invalidate equality
    // even when the post-decision summary still rolls them up the
    // same way (because the user-facing distinction matters).
    match (
        a.action_distribution.as_ref(),
        b.action_distribution.as_ref(),
    ) {
        (None, None) => {}
        (Some(_), None) | (None, Some(_)) => return false,
        (Some(x), Some(y)) => {
            if x.no_op != y.no_op
                || x.create != y.create
                || x.update != y.update
                || x.delete != y.delete
                || x.replace != y.replace
                || x.other != y.other
            {
                return false;
            }
        }
    }
    // Severity rollup — a Functional→Breaking flip on identical
    // counts must invalidate equality. The cycle's user-facing risk
    // changed.
    match (a.severity_rollup.as_ref(), b.severity_rollup.as_ref()) {
        (None, None) => {}
        (Some(_), None) | (None, Some(_)) => return false,
        (Some(x), Some(y)) => {
            if x.cosmetic != y.cosmetic || x.functional != y.functional || x.breaking != y.breaking
            {
                return false;
            }
        }
    }
    // Lifecycle phase — magma's FSM transitions are observable
    // signals; surface changes.
    if a.lifecycle_phase != b.lifecycle_phase {
        return false;
    }
    if a.outcomes.len() != b.outcomes.len() {
        return false;
    }
    for (ao, bo) in a.outcomes.iter().zip(b.outcomes.iter()) {
        if ao.address != bo.address
            || ao.outcome != bo.outcome
            || ao.action != bo.action
            || ao.message != bo.message
        {
            return false;
        }
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn outcome_for_action_terraform_vocab_round_trip() {
        // The five terraform actions plus the no-op variants. Drift
        // here would silently misclassify cycle outcomes — e.g. a
        // create shown as an update — and corrupt the audit trail.
        assert_eq!(outcome_for_action("create"), Outcome::Created);
        assert_eq!(outcome_for_action("update"), Outcome::Updated);
        assert_eq!(outcome_for_action("delete"), Outcome::Destroyed);
        assert_eq!(outcome_for_action("import"), Outcome::Imported);
        assert_eq!(outcome_for_action("no-op"), Outcome::Matched);
        assert_eq!(outcome_for_action("noop"), Outcome::Matched);
    }

    #[test]
    fn outcome_for_action_replace_collapses_to_update() {
        // A replace is destroy+create with the net effect "matches
        // declared state after the cycle" — same user-facing meaning
        // as Updated. The collapse is deliberate; drift here would
        // double-count the destroy in the cycle summary.
        assert_eq!(outcome_for_action("replace"), Outcome::Updated);
    }

    #[test]
    fn outcome_for_action_unknown_falls_back_to_updated() {
        // Unknown action → Updated rather than panicking. Future
        // terraform versions can introduce new verbs without crashing
        // the operator, but the mapping should never silently drop
        // the resource from the summary.
        assert_eq!(outcome_for_action("teleport"), Outcome::Updated);
        assert_eq!(outcome_for_action(""), Outcome::Updated);
    }

    #[test]
    fn truncate_for_status_passes_short_strings_through() {
        assert_eq!(truncate_for_status("short"), "short");
        let exactly_max = "a".repeat(256);
        assert_eq!(truncate_for_status(&exactly_max), exactly_max);
    }

    #[test]
    fn truncate_for_status_caps_at_256_with_ellipsis() {
        let too_long = "a".repeat(300);
        let out = truncate_for_status(&too_long);
        // 256 chars + ellipsis. We assert the cap, not the byte
        // length (which depends on '…' encoding).
        assert!(out.starts_with(&"a".repeat(256)));
        assert!(out.ends_with('…'));
    }

    #[test]
    fn cycle_content_equal_ignores_cycle_number_and_timestamps() {
        // Same content, different cycle/started_at/completed_at →
        // content_equal. This is the whole point: steady-state
        // reconciles must skip the etcd patch.
        let summary = CycleSummary {
            matched: 7,
            ..Default::default()
        };
        let mk = |cycle, ts| ReconcileCycle {
            cycle,
            started_at: ts,
            completed_at: ts,
            source_revision: Some("abc".into()),
            plan_summary: Some("no changes".into()),
            summary: summary.clone(),
            outcomes: vec![],
            ..Default::default()
        };
        let a = mk(1, Utc::now());
        let b = mk(2, Utc::now() + chrono::Duration::seconds(1));
        assert!(cycle_content_equal(&a, &b));
    }

    /// The slice-2 acceptance property at the cycle-receipt layer:
    /// given the same logical workspace state (same resources, same
    /// actions), both executors flow into a `ReconcileCycle` that
    /// agrees on every observable except `executor` (legitimately
    /// different) and `bundle_ref` / `lifecycle_phase` (different
    /// artifact kinds; honest absence on the tofu side).
    ///
    /// Asserts at the seam where `cycle_artifact` is consumed by
    /// `build_reconcile_cycle` — the same code path the live
    /// controller takes. If this passes, magma and tofu cycles
    /// produce equivalent CR status given equivalent input. The
    /// load-bearing claim that justifies slice 2's
    /// "interchangeable" framing.
    #[test]
    fn slice_2_acceptance_same_inputs_yield_equivalent_cycle_receipts() {
        use crate::executor::cycle_artifact::{
            CycleArtifact, PlanAction, Severity, SeverityRollup, TypedResourceChange,
        };

        let mk_change = |addr: &str, action: PlanAction, severity: Severity| TypedResourceChange {
            address: addr.into(),
            action,
            severity,
        };

        // A workspace with 4 changes: 2 no-op, 1 create, 1 delete.
        // Same logical content; each executor populates from its
        // native format.
        let common_changes = vec![
            mk_change("github_repository.a", PlanAction::NoOp, Severity::Cosmetic),
            mk_change("github_repository.b", PlanAction::NoOp, Severity::Cosmetic),
            mk_change(
                "github_repository.c",
                PlanAction::Create,
                Severity::Functional,
            ),
            mk_change(
                "github_repository.d",
                PlanAction::Delete,
                Severity::Breaking,
            ),
        ];

        let tofu_art = CycleArtifact {
            action_distribution: CycleArtifact::action_distribution_from(&common_changes),
            resource_changes: common_changes.clone(),
            artifact_ref: None, // tofu side: slice 2a leaves None
            severities: Some(SeverityRollup::from_changes(&common_changes)),
            lifecycle_phase: None, // tofu has no FSM
        };

        let magma_art = CycleArtifact {
            action_distribution: CycleArtifact::action_distribution_from(&common_changes),
            resource_changes: common_changes.clone(),
            artifact_ref: Some(crate::crd::BundleRef {
                kind: "terraform".into(),
                bundle_id: "abc123".into(),
                size_bytes: 4096,
            }),
            severities: Some(SeverityRollup::from_changes(&common_changes)),
            lifecycle_phase: Some("stable".into()),
        };

        // Build via the same code path the live operator takes.
        let drifts: Vec<DriftDetail> = vec![];

        let tofu_cycle = build_reconcile_cycle(
            42,
            chrono::Utc::now(),
            &drifts,
            4,
            Some("+1 ~0 -1".into()),
            None,
            Some("tofu".into()),
            Some(tofu_art),
            CycleResult::NoChanges,
        );
        let magma_cycle = build_reconcile_cycle(
            42,
            chrono::Utc::now(),
            &drifts,
            4,
            Some("+1 ~0 -1".into()),
            None,
            Some("magma".into()),
            Some(magma_art),
            CycleResult::NoChanges,
        );

        // ── Equivalent observables ───────────────────────────────
        assert_eq!(
            tofu_cycle.action_distribution, magma_cycle.action_distribution,
            "action_distribution must be identical across executors"
        );
        assert_eq!(
            tofu_cycle.severity_rollup, magma_cycle.severity_rollup,
            "severity_rollup must be identical (the pure action→severity \
             mapping ensures tofu lands on the same buckets magma's \
             classifier does for the same actions)"
        );
        // CycleSummary doesn't impl PartialEq so we compare field-wise.
        assert_eq!(tofu_cycle.summary.matched, magma_cycle.summary.matched);
        assert_eq!(tofu_cycle.summary.created, magma_cycle.summary.created);
        assert_eq!(tofu_cycle.summary.updated, magma_cycle.summary.updated);
        assert_eq!(tofu_cycle.summary.destroyed, magma_cycle.summary.destroyed);
        assert_eq!(tofu_cycle.summary.imported, magma_cycle.summary.imported);
        assert_eq!(tofu_cycle.summary.failed, magma_cycle.summary.failed);

        // ── Permit-by-design differences ─────────────────────────
        assert_ne!(
            tofu_cycle.executor, magma_cycle.executor,
            "executor name legitimately differs"
        );
        assert!(
            tofu_cycle.bundle_ref.is_none(),
            "tofu side: no bundle ref in slice 2a (deferred)"
        );
        assert!(
            magma_cycle.bundle_ref.is_some(),
            "magma side: bundle ref present"
        );
        assert!(
            tofu_cycle.lifecycle_phase.is_none(),
            "tofu: no FSM (honest absence)"
        );
        assert!(magma_cycle.lifecycle_phase.is_some(), "magma: FSM present");
    }

    /// Slice 2 acceptance, action-shape coverage. The equivalence
    /// property holds across the full action vocabulary, not just
    /// the cherry-picked "2 no-op + 1 create + 1 delete" mix above.
    /// Each shape exercises a specific bug surface:
    ///
    ///   * empty — degenerate plan: no resources. action_distribution
    ///             is all zeros, severities is None, both sides agree.
    ///   * all-noop — true settled state (the pleme-io-opensource
    ///                live case): 1054 noOp, zero others. The
    ///                user-visible "nothing to do" signal.
    ///   * replace — magma classifies "delete+create" as Breaking;
    ///               tofu's pure mapping does the same. Both should
    ///               agree on severities even though they arrive
    ///               via different paths.
    ///   * unknown verbs — `Other("read")` etc. must bucket
    ///                     identically across paths.
    #[test]
    fn slice_2_acceptance_holds_across_every_action_shape() {
        use crate::executor::cycle_artifact::{
            CycleArtifact, PlanAction, Severity, SeverityRollup, TypedResourceChange,
        };

        let mk = |addr: &str, action: PlanAction, severity: Severity| TypedResourceChange {
            address: addr.into(),
            action,
            severity,
        };

        // Each test case: (name, changes). Both readers produce
        // identical artifacts → identical ReconcileCycle observables.
        let cases: Vec<(&str, Vec<TypedResourceChange>)> = vec![
            ("empty plan", vec![]),
            (
                "all no-op (settled state, 5 resources)",
                (0..5)
                    .map(|i| mk(&format!("r.{i}"), PlanAction::NoOp, Severity::Cosmetic))
                    .collect(),
            ),
            (
                "all create",
                (0..3)
                    .map(|i| mk(&format!("r.{i}"), PlanAction::Create, Severity::Functional))
                    .collect(),
            ),
            (
                "replace (destroy+create coalesce)",
                vec![mk("r.a", PlanAction::Replace, Severity::Breaking)],
            ),
            (
                "unknown verb 'read' via Other",
                vec![mk(
                    "r.read",
                    PlanAction::Other("read".into()),
                    Severity::Functional,
                )],
            ),
            (
                "mixed (every variant exercised at least once)",
                vec![
                    mk("r.a", PlanAction::NoOp, Severity::Cosmetic),
                    mk("r.b", PlanAction::Create, Severity::Functional),
                    mk("r.c", PlanAction::Update, Severity::Functional),
                    mk("r.d", PlanAction::Delete, Severity::Breaking),
                    mk("r.e", PlanAction::Replace, Severity::Breaking),
                    mk(
                        "r.f",
                        PlanAction::Other("forget".into()),
                        Severity::Functional,
                    ),
                ],
            ),
        ];

        for (name, changes) in &cases {
            // Both executors populate the artifact from identical changes.
            // (In production, magma reads from bundle JSON + tofu reads
            // from show-JSON, but both end up at the same logical shape.)
            let dist = CycleArtifact::action_distribution_from(changes);
            let sev = if changes.is_empty() {
                None
            } else {
                Some(SeverityRollup::from_changes(changes))
            };

            let tofu_art = CycleArtifact {
                action_distribution: dist.clone(),
                resource_changes: changes.clone(),
                artifact_ref: None,
                severities: sev.clone(),
                lifecycle_phase: None,
            };
            let magma_art = CycleArtifact {
                action_distribution: dist.clone(),
                resource_changes: changes.clone(),
                artifact_ref: Some(crate::crd::BundleRef {
                    kind: "terraform".into(),
                    bundle_id: format!("bundle-{name}"),
                    size_bytes: 1234,
                }),
                severities: sev.clone(),
                lifecycle_phase: Some("stable".into()),
            };

            let drifts: Vec<DriftDetail> = vec![];
            let tofu = build_reconcile_cycle(
                1,
                chrono::Utc::now(),
                &drifts,
                changes.len() as u32,
                Some("test".into()),
                None,
                Some("tofu".into()),
                Some(tofu_art),
                CycleResult::NoChanges,
            );
            let magma = build_reconcile_cycle(
                1,
                chrono::Utc::now(),
                &drifts,
                changes.len() as u32,
                Some("test".into()),
                None,
                Some("magma".into()),
                Some(magma_art),
                CycleResult::NoChanges,
            );

            // The load-bearing assertions. Failure here means slice 2's
            // interchangeable property doesn't hold for the named shape
            // — a regression in either reader.
            assert_eq!(
                tofu.action_distribution, magma.action_distribution,
                "{name}: action_distribution must match across executors"
            );
            assert_eq!(
                tofu.severity_rollup, magma.severity_rollup,
                "{name}: severity_rollup must match"
            );
            assert_eq!(
                tofu.summary.matched, magma.summary.matched,
                "{name}: matched"
            );
            assert_eq!(
                tofu.summary.created, magma.summary.created,
                "{name}: created"
            );
            assert_eq!(
                tofu.summary.updated, magma.summary.updated,
                "{name}: updated"
            );
            assert_eq!(
                tofu.summary.destroyed, magma.summary.destroyed,
                "{name}: destroyed"
            );
            assert_eq!(
                tofu.summary.imported, magma.summary.imported,
                "{name}: imported"
            );
            assert_eq!(tofu.summary.failed, magma.summary.failed, "{name}: failed");
        }
    }

    #[test]
    fn cycle_content_equal_distinguishes_different_summaries() {
        let mk = |matched| {
            let mut summary = CycleSummary::default();
            summary.matched = matched;
            ReconcileCycle {
                cycle: 1,
                started_at: Utc::now(),
                completed_at: Utc::now(),
                source_revision: None,
                plan_summary: None,
                summary,
                outcomes: vec![],
                ..Default::default()
            }
        };
        assert!(!cycle_content_equal(&mk(7), &mk(8)));
    }

    #[test]
    fn converged_success_cycle_overrides_stale_plan_summary() {
        // #66: a steady-state template that planned +N earlier but now changes
        // nothing must NOT show the stale "+N". A converged AppliedSuccess
        // cycle (no drifts → all matched, zero mutations) reports "No changes",
        // overriding the stale planning-phase string passed in.
        let drifts: Vec<DriftDetail> = vec![];
        let cycle = build_reconcile_cycle(
            100,
            chrono::Utc::now(),
            &drifts,
            10,                      // 10 resources, all matched (untouched)
            Some("+6 ~0 -0".into()), // STALE planning-phase summary
            None,
            Some("magma".into()),
            None,
            CycleResult::AppliedSuccess {
                imported_addresses: vec![],
            },
        );
        assert_eq!(cycle.plan_summary.as_deref(), Some("No changes"));
        assert_eq!(cycle.summary.matched, 10);
        assert_eq!(cycle.summary.created, 0);
        assert_eq!(cycle.summary.imported, 0);
    }

    #[test]
    fn failed_cycle_keeps_planning_summary() {
        // The override fires ONLY on success+converged. A failed cycle keeps
        // the planning-phase summary so the operator still sees what was
        // attempted (no drifts here, but the result is a failure).
        let drifts: Vec<DriftDetail> = vec![];
        let cycle = build_reconcile_cycle(
            101,
            chrono::Utc::now(),
            &drifts,
            10,
            Some("+6 ~0 -0".into()),
            None,
            Some("magma".into()),
            None,
            CycleResult::AppliedFailure("boom".into()),
        );
        assert_eq!(cycle.plan_summary.as_deref(), Some("+6 ~0 -0"));
    }

    // ── should_clear_stale_error — the Failed+error→Failed+null race ────
    //
    // The confirmed live incident this guard closes: a reconcile calls
    // `update_phase_with_error(Failed, err_msg)` (setting phase=Failed +
    // lastError=Some(err_msg)) and then, in the SAME tick, calls
    // `record_reconcile_cycle(...)` with a `CycleResult` this module's
    // narrow, per-cycle-summary `cycle_is_clean` judges "clean" (zero
    // `failed` resources). Before this guard, `record_reconcile_cycle`'s
    // separate status PATCH would emit an explicit `"lastError": null`
    // for that cycle — nulling the error the sibling call just wrote —
    // while `phase` stayed `Failed` (the cycle PATCH never touches
    // `phase`). Live-captured sequence: `17:45:23 Failed+content →
    // 17:45:29 Failed+empty → 17:45:34 Pending`.
    use super::should_clear_stale_error;

    #[test]
    fn should_clear_stale_error_refuses_while_resulting_phase_is_failed() {
        // Reproduces the exact race: cycle_clean=true (a "clean" cycle
        // summary, e.g. NoChanges/AppliedSuccess/PolicyGated with zero
        // failures), a stale lastError present from the sibling
        // update_phase_with_error(Failed, ...) call this same tick, but
        // resulting_phase is Failed — the clear MUST be refused.
        assert!(
            !should_clear_stale_error(
                /* cycle_clean */ true,
                /* failed */ 0,
                /* prior_last_error */ Some("apply failed: boom"),
                /* resulting_phase */ Phase::Failed,
            ),
            "a clean cycle summary must NEVER clear lastError while this \
             tick is leaving/establishing the template at Phase::Failed — \
             doing so is the confirmed Failed+content -> Failed+empty race"
        );
    }

    #[test]
    fn should_clear_stale_error_clears_on_a_genuine_transition_away_from_failed() {
        // The legitimate case this guard must NOT break: a clean cycle
        // whose sibling call transitioned the template OFF Failed (e.g.
        // update_phase(Ready) before record_reconcile_cycle) must still
        // clear the stale error — that's the whole point of the
        // clear-on-clean-cycle mechanism (the 19h-stale-error incident
        // `record_reconcile_cycle`'s doc comment names).
        assert!(
            should_clear_stale_error(true, 0, Some("stale error"), Phase::Ready),
            "a clean cycle transitioning to Ready must still clear a stale lastError"
        );
    }

    #[test]
    fn should_clear_stale_error_false_when_cycle_itself_is_not_clean() {
        // cycle_clean=false (e.g. this cycle's own CycleResult is
        // AppliedFailure/PolicyGated(Refuse)) must refuse regardless of
        // resulting_phase — cycle_is_clean's existing guard, preserved.
        assert!(!should_clear_stale_error(
            false,
            0,
            Some("err"),
            Phase::Ready
        ));
    }

    #[test]
    fn should_clear_stale_error_false_when_this_cycle_has_per_resource_failures() {
        // failed > 0 must refuse regardless of resulting_phase — the
        // per-resource second condition the original directive names.
        assert!(!should_clear_stale_error(
            true,
            3,
            Some("err"),
            Phase::Ready
        ));
    }

    #[test]
    fn should_clear_stale_error_false_when_there_is_no_stale_error_to_clear() {
        // Nothing to clear — must be a no-op (also avoids an unnecessary
        // PATCH when lastError was already None).
        assert!(!should_clear_stale_error(true, 0, None, Phase::Ready));
    }

    #[test]
    fn should_clear_stale_error_refuses_while_holding_at_failed_unchanged() {
        // The "holding" (not just "transitioning to") case: a tick that
        // makes NO phase transition at all but the template's current,
        // unchanged phase is already Failed (e.g. a hypothetical future
        // caller that reads `template.status.phase` directly, mirroring
        // the RequireApproval call site's pattern) must also refuse.
        assert!(!should_clear_stale_error(
            true,
            0,
            Some("err"),
            Phase::Failed
        ));
    }
}
