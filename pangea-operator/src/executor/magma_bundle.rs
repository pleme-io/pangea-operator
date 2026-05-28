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
}
