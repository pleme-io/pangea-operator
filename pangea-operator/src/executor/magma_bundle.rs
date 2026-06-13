//! Read + summarize the magma-bundle.json artifact a magma reconcile
//! produces.
//!
//! ## Why this exists at this layer
//!
//! `MagmaExecutor::plan` writes `magma-bundle.json` into the workspace
//! directory (see `magma.rs::bundle_checkpoint_path`). The bundle is
//! the canonical record of what magma classified + computed for the
//! cycle: every plan change with action + severity + before/after,
//! the drift summary, the lifecycle FSM, the audit chain.
//!
//! Until this module landed, the bundle was only readable by
//! `kubectl exec`-ing the operator pod + parsing it by hand with
//! `ruby -rjson`. That made even a green cycle expensive to verify
//! (8+ shell roundtrips for the pleme-io-opensource cutover —
//! see memory/project_operator_observability_backlog.md).
//!
//! The reader's job: turn that raw JSON into the typed
//! `ActionDistribution` + `BundleRef` the CR status carries. The
//! `record_reconcile_cycle` controller hook calls
//! `read_bundle_artifacts(work_dir)` and stamps the result into
//! `status.lastCycle.actionDistribution` + `bundleRef`. After that,
//! `kubectl get itr X -o jsonpath='{.status.lastCycle.actionDistribution}'`
//! is the answer instead of a sequence of execs.
//!
//! ## Why magma-bundle is parsed as `serde_json::Value`
//!
//! magma-bundle's authoritative shape lives in the magma crates
//! (`magma_bundle`, etc.), but those crates are only linked in when
//! the `executor_magma` feature is enabled — and the bundle reader
//! must work in every build (the field appears in CR status
//! unconditionally so YAML consumers / dashboards don't have a
//! feature-gated schema). Parsing as a Value gives a feature-agnostic
//! reader with a tiny surface (the four fields it needs: `kind`,
//! `bundle_id`, `plan.changes[].action`).

use std::path::Path;

use tokio::fs;
use tracing::debug;

use crate::crd::{ActionDistribution, BundleRef};
use crate::executor::cycle_artifact::{
    CycleArtifact, PlanAction, Severity, SeverityRollup, TypedResourceChange,
};

/// Artifacts derived from `magma-bundle.json` for one reconcile cycle.
/// Both fields are optional independently — a bundle that's missing
/// `plan.changes` still yields a `BundleRef` (so observers can verify
/// the artifact hash), and a bundle that fails to parse yields
/// nothing.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct BundleArtifacts {
    /// Distribution of plan action verbs across the bundle's
    /// `plan.changes`. `None` if the bundle lacked a `plan.changes`
    /// array (e.g. a malformed bundle from a stale executor version).
    pub action_distribution: Option<ActionDistribution>,
    /// Reference to the bundle file itself — `kind`, `bundle_id`,
    /// `sha256`, `size_bytes`.
    pub bundle_ref: Option<BundleRef>,
}

/// Apply-stage outcome derived from `magma-bundle.json`'s `outcome` +
/// `lifecycle.current` fields. Distinct from the plan-stage
/// `BundleArtifacts` / `CycleArtifact` (which parse `plan.changes`):
/// this carries the magma APPLY result — the `{applied, failed, phase}`
/// shape the operator's first-class magma metrics record.
///
/// `None` from `read_apply_outcome` when the bundle has no `outcome`
/// field at all (a plan-only bundle, where magma planned but never
/// applied). A bundle WITH an outcome whose arrays are empty is a real
/// signal (`applied 0 / failed 0` — apply ran, touched nothing) and
/// yields `Some`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ApplyOutcome {
    /// `outcome.applied.len()` — resources successfully applied. The
    /// "1494" half of the live cutover's `applied 1494 / failed 14`.
    pub applied: u64,
    /// `outcome.failed.len()` — resources that failed to apply. THE
    /// signal the first-class metric surfaces (`14`).
    pub failed: u64,
    /// Whether the apply's lifecycle FSM terminated cleanly. `true` iff
    /// `failed == 0` AND `lifecycle.current` is not `failed`. The metric
    /// recorder ultimately derives the `phase` label from `failed`, but
    /// this honors the bundle's own FSM verdict so a future
    /// failed-but-zero-failed-changes shape (e.g. a verification failure
    /// after a clean apply) still reports `Failed`.
    pub succeeded: bool,
}

/// Derive an [`ApplyOutcome`] from in-memory bundle bytes (the
/// `serde_json::to_vec(&Bundle)` blob the artifact store holds in
/// Postgres). The DB-backed sibling of [`read_apply_outcome`]: same
/// parsing, no disk. `None` when the bytes don't parse or carry no
/// `outcome` (a plan-only bundle).
pub fn apply_outcome_from_bytes(bytes: &[u8]) -> Option<ApplyOutcome> {
    let value: serde_json::Value = serde_json::from_slice(bytes).ok()?;
    apply_outcome_from(&value)
}

/// Derive a [`CycleArtifact`] from in-memory bundle bytes. The
/// DB-backed sibling of [`read_cycle_artifact`]: identical extraction
/// (plan.changes → typed resource changes + severity rollup +
/// lifecycle phase + bundle ref), no disk read. `None` when the bytes
/// don't parse.
pub fn cycle_artifact_from_bytes(bytes: &[u8]) -> Option<CycleArtifact> {
    let value: serde_json::Value = serde_json::from_slice(bytes).ok()?;
    let resource_changes = resource_changes_from(&value);
    let action_distribution = CycleArtifact::action_distribution_from(&resource_changes);
    let severities = if resource_changes.is_empty() {
        None
    } else {
        Some(SeverityRollup::from_changes(&resource_changes))
    };
    Some(CycleArtifact {
        action_distribution,
        resource_changes,
        artifact_ref:    bundle_ref_from(&value, bytes),
        severities,
        lifecycle_phase: lifecycle_phase_from(&value),
    })
}

/// Read the magma APPLY outcome from `work_dir/magma-bundle.json`.
///
/// Returns `None` when:
///   * the file is missing (no apply ran, or a tofu cycle),
///   * the JSON is malformed,
///   * the bundle has no `outcome` object (a plan-only bundle — magma
///     planned but the apply stage never wrote an outcome).
///
/// Best-effort by design (mirrors `read_bundle_artifacts`): cycle
/// recording must not fail when the bundle is unavailable. The caller
/// (`record_reconcile_cycle`) simply skips the magma apply metrics
/// when this is `None`.
pub async fn read_apply_outcome(work_dir: &Path) -> Option<ApplyOutcome> {
    let path = work_dir.join("magma-bundle.json");
    let bytes = match fs::read(&path).await {
        Ok(b) => b,
        Err(e) => {
            debug!(path = %path.display(), error = %e, "no magma-bundle.json (no ApplyOutcome)");
            return None;
        }
    };
    let value: serde_json::Value = match serde_json::from_slice(&bytes) {
        Ok(v) => v,
        Err(e) => {
            debug!(path = %path.display(), error = %e, "magma-bundle.json parse failed (no ApplyOutcome)");
            return None;
        }
    };
    apply_outcome_from(&value)
}

/// Extract an `ApplyOutcome` from a parsed bundle value. `None` when
/// the bundle has no `outcome` object — that's a plan-only bundle, not
/// an apply, so there's no apply outcome to record.
///
/// `applied` / `failed` are the lengths of `outcome.applied` /
/// `outcome.failed` (the magma_converge::Outcome serialization). The
/// `succeeded` flag also consults `lifecycle.current`: an apply that
/// reached the FSM's `failed` phase reports `succeeded == false` even
/// if (defensively) the failed array were empty.
fn apply_outcome_from(value: &serde_json::Value) -> Option<ApplyOutcome> {
    let outcome = value.get("outcome")?;
    // A null `outcome` (plan-only bundle serializes `outcome: null`)
    // is the same as absent — nothing applied.
    if outcome.is_null() {
        return None;
    }
    let applied = outcome
        .get("applied")
        .and_then(|v| v.as_array())
        .map(|a| a.len() as u64)
        .unwrap_or(0);
    let failed = outcome
        .get("failed")
        .and_then(|v| v.as_array())
        .map(|a| a.len() as u64)
        .unwrap_or(0);
    // magma-fsm serializes Phase as snake_case ("stable", "failed", …).
    let phase_is_failed = value
        .get("lifecycle")
        .and_then(|v| v.get("current"))
        .and_then(|v| v.as_str())
        .map(|p| p.eq_ignore_ascii_case("failed"))
        .unwrap_or(false);
    Some(ApplyOutcome {
        applied,
        failed,
        succeeded: failed == 0 && !phase_is_failed,
    })
}

/// Read + summarize the magma-bundle.json at `work_dir/magma-bundle.json`.
///
/// Returns `None` on every "not really an error" outcome — missing file
/// (workspace not yet planned, or tofu cycle), parse failure (bundle
/// truncated mid-write), or empty bundle. Errors don't propagate
/// because cycle recording must not fail when the bundle is just
/// unavailable; status simply gets `None` for the bundle-derived
/// fields.
///
/// I/O is async-tokio so this can be called from the controller's
/// async reconcile loop without blocking the runtime.
pub async fn read_bundle_artifacts(work_dir: &Path) -> Option<BundleArtifacts> {
    let path = work_dir.join("magma-bundle.json");
    let bytes = match fs::read(&path).await {
        Ok(b) => b,
        Err(e) => {
            debug!(path = %path.display(), error = %e, "no magma-bundle.json (skipping bundle-derived status fields)");
            return None;
        }
    };

    let value: serde_json::Value = match serde_json::from_slice(&bytes) {
        Ok(v) => v,
        Err(e) => {
            debug!(path = %path.display(), error = %e, "magma-bundle.json parse failed (skipping bundle-derived status fields)");
            return None;
        }
    };

    Some(BundleArtifacts {
        action_distribution: action_distribution_from(&value),
        bundle_ref:          bundle_ref_from(&value, &bytes),
    })
}

/// Read the magma-bundle.json at `work_dir/magma-bundle.json` and
/// produce a unified `CycleArtifact` — the slice-2 typed shape both
/// executors populate.
///
/// Where this differs from `read_bundle_artifacts`: it extracts the
/// FULL `plan.changes` array as `TypedResourceChange[]` (action +
/// severity per resource), the `lifecycle.current` FSM phase, and
/// derives the `SeverityRollup` from the bundle's native severities
/// (cosmetic/functional/critical → Cosmetic/Functional/Breaking) —
/// none of which the slice-1a `BundleArtifacts` carried.
///
/// `None` semantics: missing file (workspace not yet planned, or tofu
/// cycle — tofu cycles use `from_tofu_plan_show_json` instead) or
/// parse failure. The reader is best-effort by design; cycle
/// recording falls back to None and the controller patches what it
/// has.
pub async fn read_cycle_artifact(work_dir: &Path) -> Option<CycleArtifact> {
    let path = work_dir.join("magma-bundle.json");
    let bytes = match fs::read(&path).await {
        Ok(b) => b,
        Err(e) => {
            debug!(path = %path.display(), error = %e, "no magma-bundle.json (no CycleArtifact)");
            return None;
        }
    };

    let value: serde_json::Value = match serde_json::from_slice(&bytes) {
        Ok(v) => v,
        Err(e) => {
            debug!(path = %path.display(), error = %e, "magma-bundle.json parse failed (no CycleArtifact)");
            return None;
        }
    };

    let resource_changes = resource_changes_from(&value);
    let action_distribution = CycleArtifact::action_distribution_from(&resource_changes);
    let severities = if resource_changes.is_empty() {
        None
    } else {
        Some(SeverityRollup::from_changes(&resource_changes))
    };

    Some(CycleArtifact {
        action_distribution,
        resource_changes,
        artifact_ref:    bundle_ref_from(&value, &bytes),
        severities,
        lifecycle_phase: lifecycle_phase_from(&value),
    })
}

/// Derive a `BundleRef` from the parsed bundle + raw bytes. Returns
/// `None` only if the bundle lacks both `kind` AND `bundle_id` — at
/// that point we have nothing referential to record. The `bundle_id`
/// is already the content fingerprint (BLAKE3 over the canonical
/// representation, produced by magma_bundle when the bundle is
/// minted); size is from the raw bytes for capacity-planning UX.
fn bundle_ref_from(value: &serde_json::Value, raw: &[u8]) -> Option<BundleRef> {
    let kind = value.get("kind").and_then(|v| v.as_str())?;
    let bundle_id = value.get("bundle_id").and_then(|v| v.as_str())?;

    Some(BundleRef {
        kind:       kind.to_string(),
        bundle_id:  bundle_id.to_string(),
        size_bytes: raw.len() as u64,
    })
}

/// Extract `TypedResourceChange[]` from the bundle's `plan.changes`
/// array. Each change yields (address, action, severity); the bundle
/// carries all three natively. Empty when `plan.changes` is missing
/// or not an array.
///
/// The severity vocabulary in the bundle is magma-drift's
/// `ChangeSeverity` — `"cosmetic"`, `"functional"`, `"critical"` —
/// projected to the operator's `Severity` (Cosmetic/Functional/
/// Breaking). Unknown severity strings fall back to the
/// action-derived default via `action_to_severity`.
fn resource_changes_from(value: &serde_json::Value) -> Vec<TypedResourceChange> {
    let Some(changes) = value.get("plan").and_then(|v| v.get("changes")).and_then(|v| v.as_array())
    else {
        return Vec::new();
    };
    changes
        .iter()
        .filter_map(|c| {
            let address = c.get("address").and_then(|v| v.as_str())?.to_string();
            let raw_action = c.get("action").and_then(|v| v.as_str()).unwrap_or("");
            let action = PlanAction::parse(raw_action);
            let severity = c
                .get("severity")
                .and_then(|v| v.as_str())
                .map(map_severity_with_fallback)
                .unwrap_or_else(|| crate::executor::cycle_artifact::action_to_severity(&action));
            Some(TypedResourceChange { address, action, severity })
        })
        .collect()
}

/// Project the bundle's severity string into the operator's
/// `Severity` enum. `"critical"` becomes `Breaking` (the operator's
/// outer-axis name to avoid colliding with log-level severity).
/// Unknown strings fall back to `Functional` — the same conservative
/// default `action_to_severity` uses for `Other` actions.
fn map_severity_with_fallback(raw: &str) -> Severity {
    match raw.to_ascii_lowercase().as_str() {
        "cosmetic"             => Severity::Cosmetic,
        "functional"           => Severity::Functional,
        "critical" | "breaking" => Severity::Breaking,
        _                      => Severity::Functional,
    }
}

/// Extract the lifecycle FSM phase from the bundle's `lifecycle.current`
/// field. Magma-only; tofu cycles will always leave this `None`.
fn lifecycle_phase_from(value: &serde_json::Value) -> Option<String> {
    value
        .get("lifecycle")
        .and_then(|v| v.get("current"))
        .and_then(|v| v.as_str())
        .map(str::to_string)
}

/// Derive an `ActionDistribution` by bucketing every change's `action`
/// in `plan.changes`. Returns `None` only when `plan.changes` is
/// missing or not an array — a present-but-empty array yields a
/// fully-zero distribution (which IS informative: "the plan ran but
/// touched nothing").
fn action_distribution_from(value: &serde_json::Value) -> Option<ActionDistribution> {
    let changes = value.get("plan")?.get("changes")?.as_array()?;
    let mut dist = ActionDistribution::default();
    for change in changes {
        let action = change.get("action").and_then(|v| v.as_str()).unwrap_or("");
        // Tofu's bundle has used both underscore and hyphen for no-op
        // across versions — bucket both. Same for the literal "noop".
        match action {
            "no_op" | "no-op" | "noop" => dist.no_op = dist.no_op.saturating_add(1),
            "create"  => dist.create  = dist.create.saturating_add(1),
            "update"  => dist.update  = dist.update.saturating_add(1),
            "delete"  => dist.delete  = dist.delete.saturating_add(1),
            "replace" => dist.replace = dist.replace.saturating_add(1),
            // Catch-all preserves the count fidelity — a future tofu
            // vocab addition (read, forget, …) shows up as `other`
            // rather than silently disappearing from the rollup.
            _         => dist.other   = dist.other.saturating_add(1),
        }
    }
    Some(dist)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// Production-shape bundle (drawn from the actual pleme-io-opensource
    /// cycle 2 bundle `6ed9680d05a3…`): kind=terraform, bundle_id,
    /// plan.changes[] with action=no_op throughout.
    #[test]
    fn action_distribution_from_production_no_op_bundle() {
        let v = json!({
            "kind": "terraform",
            "bundle_id": "6ed9680d05a3362db0f4af3ad085739988e8c5c0769f72dcb6aa6765396e4f3a",
            "plan": {
                "changes": (0..5).map(|i| json!({
                    "address": format!("github_repository.r{}", i),
                    "action": "no_op",
                    "severity": "cosmetic"
                })).collect::<Vec<_>>(),
            }
        });
        let dist = action_distribution_from(&v).expect("plan.changes present");
        assert_eq!(dist.no_op, 5);
        assert_eq!(dist.create, 0);
        assert_eq!(dist.update, 0);
        assert_eq!(dist.delete, 0);
        assert_eq!(dist.replace, 0);
        assert_eq!(dist.other, 0);
    }

    #[test]
    fn action_distribution_buckets_every_known_verb() {
        let v = json!({
            "plan": {
                "changes": [
                    {"action": "no_op"},
                    {"action": "no-op"},
                    {"action": "noop"},
                    {"action": "create"},
                    {"action": "create"},
                    {"action": "update"},
                    {"action": "delete"},
                    {"action": "replace"},
                ]
            }
        });
        let dist = action_distribution_from(&v).unwrap();
        assert_eq!(dist.no_op, 3, "no_op + no-op + noop all bucket together");
        assert_eq!(dist.create, 2);
        assert_eq!(dist.update, 1);
        assert_eq!(dist.delete, 1);
        assert_eq!(dist.replace, 1);
        assert_eq!(dist.other, 0);
    }

    #[test]
    fn action_distribution_unknown_verb_goes_to_other_not_dropped() {
        // The whole point of `other`: a future tofu vocab addition
        // (`read`, `forget`, an unknown string) must still appear in
        // the rollup so the totals stay faithful to plan.changes.len().
        let v = json!({
            "plan": {
                "changes": [
                    {"action": "create"},
                    {"action": "read"},
                    {"action": "forget"},
                    {"action": "newverb"},
                    {"action": ""},
                ]
            }
        });
        let dist = action_distribution_from(&v).unwrap();
        assert_eq!(dist.create, 1);
        assert_eq!(dist.other, 4, "read + forget + newverb + empty all → other");
        // Total preserved: 1 + 4 = 5 = plan.changes.len()
        let total = dist.no_op + dist.create + dist.update + dist.delete + dist.replace + dist.other;
        assert_eq!(total, 5);
    }

    #[test]
    fn action_distribution_present_but_empty_yields_zeros() {
        // A bundle with an empty plan.changes IS informative — the
        // plan ran but classified nothing. Must yield Some(zeroes),
        // not None.
        let v = json!({ "plan": { "changes": [] } });
        let dist = action_distribution_from(&v).unwrap();
        assert_eq!(dist.no_op, 0);
        assert_eq!(dist.create, 0);
        assert_eq!(dist.other, 0);
    }

    #[test]
    fn action_distribution_missing_plan_changes_yields_none() {
        // Distinguishable from "empty array" — None means we couldn't
        // count, not "we counted zero."
        assert!(action_distribution_from(&json!({})).is_none());
        assert!(action_distribution_from(&json!({"plan": {}})).is_none());
        assert!(action_distribution_from(&json!({"plan": {"changes": "not-an-array"}})).is_none());
    }

    #[test]
    fn bundle_ref_from_production_shape() {
        let raw = br#"{"kind":"terraform","bundle_id":"abc123"}"#;
        let v: serde_json::Value = serde_json::from_slice(raw).unwrap();
        let r = bundle_ref_from(&v, raw).expect("kind + bundle_id present");
        assert_eq!(r.kind, "terraform");
        assert_eq!(r.bundle_id, "abc123");
        assert_eq!(r.size_bytes, raw.len() as u64);
    }

    #[test]
    fn bundle_ref_missing_kind_or_id_yields_none() {
        let raw = br#"{"bundle_id":"abc"}"#;
        let v: serde_json::Value = serde_json::from_slice(raw).unwrap();
        assert!(bundle_ref_from(&v, raw).is_none(), "missing kind → None");

        let raw = br#"{"kind":"terraform"}"#;
        let v: serde_json::Value = serde_json::from_slice(raw).unwrap();
        assert!(bundle_ref_from(&v, raw).is_none(), "missing bundle_id → None");
    }

    #[tokio::test]
    async fn read_bundle_artifacts_handles_missing_file() {
        let dir = tempfile::tempdir().unwrap();
        // No magma-bundle.json in the dir.
        assert_eq!(read_bundle_artifacts(dir.path()).await, None);
    }

    #[tokio::test]
    async fn read_bundle_artifacts_handles_malformed_json() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("magma-bundle.json");
        tokio::fs::write(&path, b"{not json").await.unwrap();
        assert_eq!(read_bundle_artifacts(dir.path()).await, None);
    }

    // ── read_cycle_artifact (slice 2) ─────────────────────────────

    #[tokio::test]
    async fn read_cycle_artifact_end_to_end_with_all_fields() {
        let dir = tempfile::tempdir().unwrap();
        let bundle = json!({
            "kind": "terraform",
            "bundle_id": "6ed9680d05a3",
            "plan": {
                "changes": [
                    {"address": "github_repository.r1", "action": "no_op",  "severity": "cosmetic"},
                    {"address": "github_repository.r2", "action": "no_op",  "severity": "cosmetic"},
                    {"address": "github_repository.r3", "action": "create", "severity": "functional"},
                    {"address": "github_repository.r4", "action": "delete", "severity": "critical"},
                ]
            },
            "lifecycle": {"current": "planning"}
        });
        let raw = serde_json::to_vec(&bundle).unwrap();
        tokio::fs::write(dir.path().join("magma-bundle.json"), &raw).await.unwrap();

        let art = read_cycle_artifact(dir.path()).await.unwrap();
        // ActionDistribution faithfully reflects every change.
        assert_eq!(art.action_distribution.no_op,  2);
        assert_eq!(art.action_distribution.create, 1);
        assert_eq!(art.action_distribution.delete, 1);
        // Resource changes carry per-resource action + severity (and
        // the "critical" projects to Breaking — the operator's
        // outer-axis name).
        assert_eq!(art.resource_changes.len(), 4);
        assert_eq!(art.resource_changes[0].address, "github_repository.r1");
        assert_eq!(art.resource_changes[0].action,  PlanAction::NoOp);
        assert_eq!(art.resource_changes[0].severity, Severity::Cosmetic);
        assert_eq!(art.resource_changes[3].action,  PlanAction::Delete);
        assert_eq!(art.resource_changes[3].severity, Severity::Breaking);
        // SeverityRollup matches per-resource breakdown.
        let rollup = art.severities.unwrap();
        assert_eq!(rollup.cosmetic,   2);
        assert_eq!(rollup.functional, 1);
        assert_eq!(rollup.breaking,   1);
        // Lifecycle phase came through.
        assert_eq!(art.lifecycle_phase.as_deref(), Some("planning"));
        // Bundle ref is populated.
        let bref = art.artifact_ref.unwrap();
        assert_eq!(bref.kind, "terraform");
        assert_eq!(bref.bundle_id, "6ed9680d05a3");
    }

    #[tokio::test]
    async fn read_cycle_artifact_falls_back_to_action_severity_when_unset() {
        // Production magma bundles include severity, but a partial
        // bundle from a different magma version might not. The reader
        // falls back to the pure action→severity mapping so the
        // SeverityRollup is still populated honestly.
        let dir = tempfile::tempdir().unwrap();
        let bundle = json!({
            "kind": "terraform",
            "bundle_id": "abc",
            "plan": {
                "changes": [
                    {"address": "r.a", "action": "delete"},
                    {"address": "r.b", "action": "create"},
                ]
            }
        });
        tokio::fs::write(
            dir.path().join("magma-bundle.json"),
            serde_json::to_vec(&bundle).unwrap(),
        ).await.unwrap();

        let art = read_cycle_artifact(dir.path()).await.unwrap();
        assert_eq!(art.resource_changes[0].severity, Severity::Breaking,    "delete defaults to Breaking");
        assert_eq!(art.resource_changes[1].severity, Severity::Functional,  "create defaults to Functional");
    }

    #[tokio::test]
    async fn read_cycle_artifact_handles_missing_file() {
        let dir = tempfile::tempdir().unwrap();
        assert!(read_cycle_artifact(dir.path()).await.is_none());
    }

    #[tokio::test]
    async fn read_cycle_artifact_handles_malformed_json() {
        let dir = tempfile::tempdir().unwrap();
        tokio::fs::write(dir.path().join("magma-bundle.json"), b"{not json").await.unwrap();
        assert!(read_cycle_artifact(dir.path()).await.is_none());
    }

    #[tokio::test]
    async fn read_cycle_artifact_handles_resourceless_bundle() {
        // A magma bundle with no plan.changes (e.g. a workspace
        // that errored before plan ran) still yields a CycleArtifact
        // — the artifact_ref + lifecycle survive, just with no
        // resource_changes + no severities.
        let dir = tempfile::tempdir().unwrap();
        let bundle = json!({"kind": "terraform", "bundle_id": "xyz"});
        tokio::fs::write(
            dir.path().join("magma-bundle.json"),
            serde_json::to_vec(&bundle).unwrap(),
        ).await.unwrap();

        let art = read_cycle_artifact(dir.path()).await.unwrap();
        assert!(art.resource_changes.is_empty());
        assert!(art.severities.is_none(), "no resources → no rollup");
        assert!(art.artifact_ref.is_some(), "bundle_ref still derives");
        assert!(art.lifecycle_phase.is_none());
    }

    // ── read_apply_outcome (magma apply-outcome metrics) ───────────

    #[test]
    fn apply_outcome_from_production_partial_failure_shape() {
        // The shape that motivated the first-class metrics: the live
        // pleme-io-opensource cutover applied 1494 resources with 14
        // failed, terminal phase Failed. (Sized down to keep the test
        // fixture small; counts match the structure, not the magnitude.)
        let v = json!({
            "kind": "terraform",
            "bundle_id": "abc",
            "outcome": {
                "plan_id": "deadbeef",
                "kind": "terraform",
                "applied": (0..1494).map(|i| json!({
                    "address": format!("github_repository.r{i}"),
                    "action": "create"
                })).collect::<Vec<_>>(),
                "failed": (0..14).map(|i| json!({
                    "address": format!("github_repository.f{i}"),
                    "action": "create",
                    "error": "rate limited"
                })).collect::<Vec<_>>(),
            },
            "lifecycle": {"current": "failed"},
        });
        let o = apply_outcome_from(&v).expect("outcome present");
        assert_eq!(o.applied, 1494);
        assert_eq!(o.failed, 14);
        assert!(!o.succeeded, "14 failures + Failed phase → not succeeded");
    }

    #[test]
    fn apply_outcome_from_clean_apply_is_succeeded() {
        let v = json!({
            "outcome": {
                "applied": [
                    {"address": "r.a", "action": "create"},
                    {"address": "r.b", "action": "update"},
                ],
                "failed": [],
            },
            "lifecycle": {"current": "stable"},
        });
        let o = apply_outcome_from(&v).unwrap();
        assert_eq!(o.applied, 2);
        assert_eq!(o.failed, 0);
        assert!(o.succeeded, "zero failures + Stable phase → succeeded");
    }

    #[test]
    fn apply_outcome_failed_phase_overrides_zero_failed_changes() {
        // Defensive: an apply that reached the FSM Failed phase reports
        // not-succeeded even if (somehow) the failed array is empty —
        // e.g. a post-apply verification failure. The `succeeded` flag
        // honors the bundle's own FSM verdict.
        let v = json!({
            "outcome": { "applied": [{"address": "r.a", "action": "create"}], "failed": [] },
            "lifecycle": {"current": "failed"},
        });
        let o = apply_outcome_from(&v).unwrap();
        assert_eq!(o.failed, 0);
        assert!(!o.succeeded, "Failed phase forces succeeded=false");
    }

    #[test]
    fn apply_outcome_empty_arrays_is_some_zero_zero() {
        // An apply that ran but touched nothing IS a signal (apply
        // executed, 0 applied, 0 failed) — must be Some, not None.
        let v = json!({
            "outcome": { "applied": [], "failed": [] },
            "lifecycle": {"current": "stable"},
        });
        let o = apply_outcome_from(&v).unwrap();
        assert_eq!(o.applied, 0);
        assert_eq!(o.failed, 0);
        assert!(o.succeeded);
    }

    #[test]
    fn apply_outcome_plan_only_bundle_yields_none() {
        // A plan-stage bundle has no `outcome` (or `outcome: null`) —
        // there's no apply to record. None means "skip the apply
        // metrics", distinct from "apply ran and did nothing".
        assert!(apply_outcome_from(&json!({"plan": {"changes": []}})).is_none());
        assert!(apply_outcome_from(&json!({"outcome": serde_json::Value::Null})).is_none());
    }

    #[tokio::test]
    async fn read_apply_outcome_handles_missing_file() {
        let dir = tempfile::tempdir().unwrap();
        assert!(read_apply_outcome(dir.path()).await.is_none());
    }

    #[tokio::test]
    async fn read_apply_outcome_end_to_end() {
        let dir = tempfile::tempdir().unwrap();
        let bundle = json!({
            "kind": "terraform",
            "bundle_id": "xyz",
            "outcome": {
                "applied": [{"address": "r.a", "action": "create"}],
                "failed":  [{"address": "r.b", "action": "create", "error": "boom"}],
            },
            "lifecycle": {"current": "failed"},
        });
        tokio::fs::write(
            dir.path().join("magma-bundle.json"),
            serde_json::to_vec(&bundle).unwrap(),
        ).await.unwrap();

        let o = read_apply_outcome(dir.path()).await.unwrap();
        assert_eq!(o.applied, 1);
        assert_eq!(o.failed, 1);
        assert!(!o.succeeded);
    }

    #[tokio::test]
    async fn read_bundle_artifacts_end_to_end() {
        let dir = tempfile::tempdir().unwrap();
        let bundle = json!({
            "kind": "terraform",
            "bundle_id": "deadbeef",
            "plan": {
                "changes": [
                    {"action": "no_op"},
                    {"action": "no_op"},
                    {"action": "create"},
                ]
            }
        });
        let raw = serde_json::to_vec(&bundle).unwrap();
        tokio::fs::write(dir.path().join("magma-bundle.json"), &raw)
            .await
            .unwrap();

        let arts = read_bundle_artifacts(dir.path()).await.unwrap();
        let dist = arts.action_distribution.unwrap();
        assert_eq!(dist.no_op, 2);
        assert_eq!(dist.create, 1);
        let bref = arts.bundle_ref.unwrap();
        assert_eq!(bref.kind, "terraform");
        assert_eq!(bref.bundle_id, "deadbeef");
        assert_eq!(bref.size_bytes, raw.len() as u64);
    }

    // ── DB-backed byte helpers (zero-disk magma path) ─────────────────

    #[test]
    fn cycle_artifact_from_bytes_matches_disk_reader() {
        // The DB-backed reader (`cycle_artifact_from_bytes`) must derive
        // the EXACT same CycleArtifact the disk reader derives from the
        // same bundle bytes — the two paths are interchangeable.
        let bundle = json!({
            "kind": "terraform",
            "bundle_id": "6ed9680d05a3",
            "plan": {
                "changes": [
                    {"address": "github_repository.r1", "action": "no_op",  "severity": "cosmetic"},
                    {"address": "github_repository.r2", "action": "create", "severity": "functional"},
                    {"address": "github_repository.r3", "action": "delete", "severity": "critical"},
                ]
            },
            "lifecycle": {"current": "applying"}
        });
        let bytes = serde_json::to_vec(&bundle).unwrap();
        let art = cycle_artifact_from_bytes(&bytes).unwrap();
        assert_eq!(art.action_distribution.no_op, 1);
        assert_eq!(art.action_distribution.create, 1);
        assert_eq!(art.action_distribution.delete, 1);
        assert_eq!(art.resource_changes.len(), 3);
        assert_eq!(art.resource_changes[2].severity, Severity::Breaking);
        let rollup = art.severities.unwrap();
        assert_eq!(rollup.cosmetic, 1);
        assert_eq!(rollup.functional, 1);
        assert_eq!(rollup.breaking, 1);
        assert_eq!(art.lifecycle_phase.as_deref(), Some("applying"));
        assert_eq!(art.artifact_ref.unwrap().bundle_id, "6ed9680d05a3");
    }

    #[test]
    fn cycle_artifact_from_bytes_handles_garbage() {
        // Non-bundle bytes yield None, never a panic.
        assert!(cycle_artifact_from_bytes(b"{not json").is_none());
    }

    #[test]
    fn apply_outcome_from_bytes_matches_disk_reader() {
        let bundle = json!({
            "kind": "terraform",
            "bundle_id": "xyz",
            "outcome": {
                "applied": [{"address": "r.a", "action": "create"}],
                "failed":  [{"address": "r.b", "action": "create", "error": "boom"}],
            },
            "lifecycle": {"current": "failed"},
        });
        let bytes = serde_json::to_vec(&bundle).unwrap();
        let o = apply_outcome_from_bytes(&bytes).unwrap();
        assert_eq!(o.applied, 1);
        assert_eq!(o.failed, 1);
        assert!(!o.succeeded);
    }

    #[test]
    fn apply_outcome_from_bytes_plan_only_is_none() {
        // A plan-only bundle (no `outcome`) → None; garbage → None.
        let plan_only = serde_json::to_vec(&json!({"plan": {"changes": []}})).unwrap();
        assert!(apply_outcome_from_bytes(&plan_only).is_none());
        assert!(apply_outcome_from_bytes(b"not json").is_none());
    }
}
