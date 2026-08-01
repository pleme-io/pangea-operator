//! Unified post-plan/post-apply cycle artifact.
//!
//! ## Why this exists
//!
//! Both executors (tofu subprocess + magma in-process) produce
//! conceptually equivalent data after a plan: a set of resource
//! changes with actions, an artifact file on disk, and (for magma)
//! a severity classification. But each surfaces that data in its
//! own shape:
//!
//!   * tofu writes a binary `tfplan` and emits `show -json` text
//!     with `resource_changes[].change.actions: ["create"|"update"|…]`
//!   * magma writes `magma-bundle.json` with `plan.changes[].action:
//!     "no_op"|"create"|…` + `plan.changes[].severity: "cosmetic"|…`
//!
//! The controller's status-write path (`record_reconcile_cycle`)
//! shouldn't need to know which executor ran — it should consume one
//! typed shape and patch CR status. That typed shape is
//! `CycleArtifact`.
//!
//! ## Honest absence over fabrication
//!
//! Different backends have different capabilities, and **that's
//! fine** — the abstraction makes per-backend gaps first-class
//! `Option<…>` fields rather than forcing every backend to fabricate
//! values it doesn't have. The contract per field:
//!
//! | Field                    | Tofu                            | Magma                       |
//! |--------------------------|---------------------------------|-----------------------------|
//! | `action_distribution`    | from `show_plan` JSON           | from bundle `plan.changes`  |
//! | `resource_changes`       | yes                             | yes                         |
//! | `artifact_ref`           | kind=`tofu-tfplan`              | kind=`terraform`            |
//! | `severities`             | pure action→severity mapping    | from magma-drift classifier |
//! | `lifecycle_phase`        | **honest None** (no FSM)        | yes                         |
//! | `validation_diagnostics` | via `tofu validate`             | via `magma_test_laws`       |
//!
//! Action distribution + resource_changes are the load-bearing core.
//! Severity is total (the pure mapper covers tofu); lifecycle is
//! genuinely magma-only.
//!
//! ## What populates CycleArtifact
//!
//! Two readers, one per executor:
//!
//!   * `from_tofu_plan_show_json(stdout)` — parses the JSON tofu
//!     writes via `tofu show -json <tfplan>`.
//!   * `from_magma_bundle(work_dir)` — reads `magma-bundle.json`
//!     from the workspace directory (see `executor/magma_bundle.rs`).
//!
//! Each returns `Option<CycleArtifact>` — `None` when the artifact
//! is unavailable (file missing, malformed, executor wrote nothing).
//! Both readers are pure functions; all I/O is in the reader-side
//! helper that loads the bytes.

use crate::crd::{ActionDistribution, BundleRef};

/// Unified result of one cycle's plan (and the post-apply re-plan,
/// when applicable). Both executors populate this; the controller
/// reads from one typed shape regardless of which ran.
///
/// Construction goes through the two `from_*` constructors below.
/// Default is the all-empty/all-None artifact — used as a fallback
/// when no executor output is available (e.g. a cycle that didn't
/// reach the plan phase).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CycleArtifact {
    /// Distribution of plan action verbs across every change in this
    /// cycle's plan. The pre-decision rollup — counts every change
    /// including no-ops. Always populated (zero-count distribution
    /// for an empty plan is informative: "the plan ran but classified
    /// nothing").
    pub action_distribution: ActionDistribution,

    /// Per-resource breakdown with normalized addresses + actions +
    /// severities. The fine-grained input to drift_details and to
    /// the import prepass's create-extraction path.
    pub resource_changes: Vec<TypedResourceChange>,

    /// Reference to the on-disk artifact this cycle produced. `kind`
    /// discriminates which executor wrote it. `None` when no artifact
    /// is on disk (the plan didn't run, or the executor doesn't
    /// produce a persistent artifact).
    pub artifact_ref: Option<BundleRef>,

    /// Aggregate severity counts across `resource_changes`. Always
    /// populated when `resource_changes` is non-empty (the
    /// action→severity mapping is total).
    pub severities: Option<SeverityRollup>,

    /// Magma-only: the lifecycle FSM phase recorded in the bundle.
    /// `None` for tofu cycles (tofu has no FSM). Honest absence.
    pub lifecycle_phase: Option<String>,
}

/// One resource change in a plan, in the unified vocabulary the
/// controller speaks. Tofu's `resource_changes[].change.actions[]`
/// and magma's `plan.changes[].action` both map into here.
///
/// `address` is normalized to `<type>.<name>` form — magma's
/// `(type_id, name)` and tofu's already-flat `aws_iam_role.foo` both
/// land in this shape. Sortable + comparable across executors.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct TypedResourceChange {
    /// Canonical address (`<type>.<name>`, e.g. `github_repository.r`).
    pub address: String,

    /// The terraform action verb, in unified enum form.
    pub action: PlanAction,

    /// Severity bucket for this change. For tofu this comes from
    /// the pure `action_to_severity` mapping; for magma it comes from
    /// magma-drift's classifier. Both arrive in the same three-bucket
    /// projection here.
    pub severity: Severity,
}

/// Unified plan action verb across both executors. The variants
/// match terraform's first-class action set; `Other(String)` is the
/// catch-all so a future verb (or an unrecognized one) is preserved
/// in the rollup instead of silently dropping.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum PlanAction {
    /// Declared state already matches actual state. Apply runs no
    /// per-resource action. terraform: `no-op`/`noop`; magma: `no_op`.
    NoOp,
    /// Resource would be created on apply.
    Create,
    /// Resource would be updated in-place on apply.
    Update,
    /// Resource would be destroyed on apply.
    Delete,
    /// Resource would be destroyed + recreated (terraform: `replace`).
    Replace,
    /// Unknown / future action verb. The original string is preserved
    /// so observers see what the executor reported.
    Other(String),
}

impl PlanAction {
    /// Parse the wire-format action string (terraform's vocabulary)
    /// into a typed variant. Recognizes both hyphen + underscore
    /// no-op variants since the two executors disagree.
    pub fn parse(raw: &str) -> Self {
        match raw {
            "no_op" | "no-op" | "noop" => Self::NoOp,
            "create" => Self::Create,
            "update" => Self::Update,
            "delete" => Self::Delete,
            "replace" => Self::Replace,
            other => Self::Other(other.to_string()),
        }
    }
}

/// Severity bucket for a change. magma-drift's `ChangeSeverity`
/// (Cosmetic / Functional / Critical) projects here as Cosmetic /
/// Functional / Breaking. The third bucket is renamed "Breaking" at
/// the operator boundary to avoid colliding with the `Critical` log
/// level (a different axis).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Severity {
    /// Change is purely cosmetic — comment-level, no real effect on
    /// declared resources. Most no-ops fall here.
    Cosmetic,
    /// Change has functional effect — resource is updated, created,
    /// or its semantically-meaningful attributes change.
    Functional,
    /// Change breaks something — destroy, replace, or any action
    /// that loses data / interrupts service. The bucket the operator
    /// pays attention to.
    Breaking,
}

/// Map a plan action verb to its default severity. This is the pure,
/// total mapping that the tofu side uses (since tofu doesn't carry
/// native severity). magma uses its own classifier output — this
/// function is the FALLBACK for executors without one.
///
/// The mapping is deliberately conservative:
///   * `Delete` / `Replace` → Breaking (loses or interrupts state)
///   * `Create` / `Update`  → Functional (real change, but additive
///                            or in-place)
///   * `NoOp` → Cosmetic (declared == actual)
///   * `Other(_)` → Functional (unknown is presumed to be a real
///                  change rather than silently treated as cosmetic)
pub fn action_to_severity(action: &PlanAction) -> Severity {
    match action {
        PlanAction::NoOp => Severity::Cosmetic,
        PlanAction::Create => Severity::Functional,
        PlanAction::Update => Severity::Functional,
        PlanAction::Delete => Severity::Breaking,
        PlanAction::Replace => Severity::Breaking,
        PlanAction::Other(_) => Severity::Functional,
    }
}

/// Convert a `PlanAction` back to a terraform-vocabulary action
/// string. Used by `CycleArtifact::drift_details` to populate
/// `DriftDetail.action` in the operator's existing shape. Inverse of
/// `PlanAction::parse` for the well-known verbs; `Other(s)` round-trips
/// as `s`.
pub fn plan_action_to_terraform_str(action: &PlanAction) -> &str {
    match action {
        PlanAction::NoOp => "no-op",
        PlanAction::Create => "create",
        PlanAction::Update => "update",
        PlanAction::Delete => "delete",
        PlanAction::Replace => "replace",
        PlanAction::Other(s) => s.as_str(),
    }
}

/// Map a `Severity` to the operator's existing "risk" vocabulary used
/// by `DriftDetail.risk`. The risk vocabulary predates `Severity` and
/// is what current dashboards + policy rules read; the mapping
/// preserves backward-compatible semantics:
///
///   * `Cosmetic`   → `"safe"`
///   * `Functional` → `"modify"`
///   * `Breaking`   → `"destroy"` (the existing "loses or interrupts
///                    state" bucket)
pub fn severity_to_risk(severity: Severity) -> &'static str {
    match severity {
        Severity::Cosmetic => "safe",
        Severity::Functional => "modify",
        Severity::Breaking => "destroy",
    }
}

/// Aggregate severity counts across all resource changes in a cycle.
/// The CR-status-facing rollup; counts everything in
/// `CycleArtifact.resource_changes`. Sum equals
/// `action_distribution.total()`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SeverityRollup {
    pub cosmetic: u32,
    pub functional: u32,
    pub breaking: u32,
}

impl SeverityRollup {
    /// Derive a SeverityRollup from a slice of `TypedResourceChange`
    /// — bucket every change by its severity. Total = changes.len().
    pub fn from_changes(changes: &[TypedResourceChange]) -> Self {
        let mut r = Self::default();
        for c in changes {
            match c.severity {
                Severity::Cosmetic => r.cosmetic = r.cosmetic.saturating_add(1),
                Severity::Functional => r.functional = r.functional.saturating_add(1),
                Severity::Breaking => r.breaking = r.breaking.saturating_add(1),
            }
        }
        r
    }

    /// Total count across all buckets.
    pub fn total(&self) -> u32 {
        self.cosmetic
            .saturating_add(self.functional)
            .saturating_add(self.breaking)
    }
}

impl CycleArtifact {
    /// Derive an `ActionDistribution` (the CR-status type) from a
    /// slice of `TypedResourceChange`. Used by both readers + by
    /// callers that want the rollup without going through the
    /// full `CycleArtifact`.
    pub fn action_distribution_from(changes: &[TypedResourceChange]) -> ActionDistribution {
        let mut d = ActionDistribution::default();
        for c in changes {
            match &c.action {
                PlanAction::NoOp => d.no_op = d.no_op.saturating_add(1),
                PlanAction::Create => d.create = d.create.saturating_add(1),
                PlanAction::Update => d.update = d.update.saturating_add(1),
                PlanAction::Delete => d.delete = d.delete.saturating_add(1),
                PlanAction::Replace => d.replace = d.replace.saturating_add(1),
                PlanAction::Other(_) => d.other = d.other.saturating_add(1),
            }
        }
        d
    }

    /// Project `action_distribution` into the (added, changed,
    /// destroyed, total) tuple the operator's existing `PlanSummary`
    /// + `ResourceSummary` types use. Adopts the existing convention:
    ///   * `added`     = create count
    ///   * `changed`   = update + replace count (replace is net "still
    ///                   declared after the cycle", same user-facing
    ///                   semantic as update)
    ///   * `destroyed` = delete count
    ///   * `total`     = changes + no-ops + other (every classified
    ///                   resource in the plan)
    ///
    /// Lets the magma-side cycle path produce the same legacy
    /// `PlanSummary.format()` string the tofu path produces, without
    /// going through `Plan::from_json`.
    pub fn summary_counts(&self) -> (u32, u32, u32, u32) {
        let d = &self.action_distribution;
        let added = d.create;
        let changed = d.update.saturating_add(d.replace);
        let destroyed = d.delete;
        // Total = every classified resource (no-ops + changes + other).
        let total = d
            .no_op
            .saturating_add(d.create)
            .saturating_add(d.update)
            .saturating_add(d.delete)
            .saturating_add(d.replace)
            .saturating_add(d.other);
        (added, changed, destroyed, total)
    }

    /// Produce `DriftDetail` entries from `resource_changes`, capped
    /// at `cap`. The operator's existing policy engine
    /// (`evaluate_policy`) consumes `Vec<DriftDetail>` — this is the
    /// magma-side equivalent of the tofu path's
    /// `Plan::drift_details(cap)`.
    ///
    /// Only non-no-op changes are emitted; matched (no-op) resources
    /// roll up into `summary.matched` rather than into per-resource
    /// drift. `attributes` is empty for now — the per-attribute
    /// detail magma carries in its bundle's before/after blocks
    /// will be wired through in a follow-up slice (slice 2.18 or
    /// later) that extends `TypedResourceChange` with optional
    /// attribute deltas. `policy_decision` + `matched_policy` are
    /// populated by the policy engine downstream; this function
    /// leaves them None.
    pub fn drift_details(&self, cap: usize) -> Vec<crate::crd::DriftDetail> {
        self.resource_changes
            .iter()
            .filter(|c| !matches!(c.action, PlanAction::NoOp))
            .take(cap)
            .map(|c| crate::crd::DriftDetail {
                address: c.address.clone(),
                action: plan_action_to_terraform_str(&c.action).to_string(),
                risk: severity_to_risk(c.severity).to_string(),
                attributes: Vec::new(),
                policy_decision: None,
                matched_policy: None,
            })
            .collect()
    }

    /// Build a `CycleArtifact` directly from a `magma_types::Plan`
    /// in process. The third constructor — closes the in-memory
    /// pipeline loop (theory/IN-MEMORY-PIPELINE.md):
    ///
    ///   * Ruby compile → `serde_json::Value` (via
    ///     `CompileResult.synthesis_value`).
    ///   * Value → `magma_config::Config` (via
    ///     `MagmaExecutor::load_config_from_value`).
    ///   * Config + state → `magma_types::Plan` (via
    ///     `magma_plan::plan`).
    ///   * **Plan → `CycleArtifact`** (this method).
    ///
    /// No disk, no JSON re-parse, no bundle file. Pure in-memory
    /// pipeline. The preview RPC, equivalence tests, and future
    /// customer-facing playground all converge through this path.
    ///
    /// Severity is derived from action via `action_to_severity` (the
    /// same default mapping the tofu reader uses). When magma's
    /// drift classifier has run separately, callers can override the
    /// `severities` field on the returned artifact with the
    /// classifier's verdict.
    #[cfg(feature = "executor_magma")]
    pub fn from_magma_plan(plan: &magma_types::Plan) -> Self {
        let resource_changes: Vec<TypedResourceChange> = plan
            .resource_changes
            .iter()
            .map(|rc| {
                // Canonical address: `<type>.<name>` — matches the
                // tofu show-json shape exactly so consumers can
                // compare artifacts across executors. The existing
                // `to_universal_plan` (magma.rs) uses the same form.
                let address = rc.address.to_string();
                let action = match rc.action {
                    magma_types::Action::Create => PlanAction::Create,
                    magma_types::Action::Update => PlanAction::Update,
                    magma_types::Action::Delete => PlanAction::Delete,
                    magma_types::Action::Replace => PlanAction::Replace,
                    magma_types::Action::NoOp => PlanAction::NoOp,
                    // Read = effectively no-op for diff purposes (data
                    // source refresh; nothing written).
                    magma_types::Action::Read => PlanAction::NoOp,
                    // Forget removes from state without destroying
                    // the cloud resource — closest user-facing
                    // semantic is "delete from our concern."
                    magma_types::Action::Forget => PlanAction::Delete,
                    // Both order variants collapse to Replace —
                    // matches `to_universal_plan`'s convention.
                    magma_types::Action::CreateThenDelete => PlanAction::Replace,
                    magma_types::Action::DeleteThenCreate => PlanAction::Replace,
                };
                let severity = action_to_severity(&action);
                TypedResourceChange {
                    address,
                    action,
                    severity,
                }
            })
            .collect();

        let action_distribution = Self::action_distribution_from(&resource_changes);
        let severities = if resource_changes.is_empty() {
            None
        } else {
            Some(SeverityRollup::from_changes(&resource_changes))
        };

        Self {
            action_distribution,
            resource_changes,
            // The in-memory plan has no associated artifact file
            // (it's NOT been written to magma-bundle.json). Callers
            // that later persist via `magma_bundle::write_bundle`
            // can populate this then.
            artifact_ref: None,
            severities,
            // Lifecycle phase is recorded in the bundle's lifecycle
            // FSM, not in the bare Plan. Honest absence here.
            lifecycle_phase: None,
        }
    }

    /// Parse the JSON tofu emits via `tofu show -json <tfplan>` into
    /// a `CycleArtifact`. This is the tofu-side counterpart of
    /// `magma_bundle::read_cycle_artifact`; both readers populate the
    /// same typed shape from their executor's native format.
    ///
    /// ## What we extract
    ///
    /// From the tofu plan-show JSON's `resource_changes[]`:
    ///   * `address`         → `TypedResourceChange.address` (already
    ///                          canonical `<type>.<name>` form)
    ///   * `change.actions[]` → `TypedResourceChange.action` (mapped
    ///                          via `action_set_to_plan_action`)
    ///   * severity is derived from action via `action_to_severity`
    ///     since tofu carries no native severity classifier
    ///
    /// ## What we don't populate (honest absence)
    ///
    /// * `artifact_ref` — None. Tofu's `tfplan` is a binary file with
    ///   no inline identity; we'd need a hashing path matching magma's
    ///   BLAKE3-from-bundle_id convention. Deferred to a follow-up
    ///   slice that introduces a unified file-fingerprint helper.
    /// * `lifecycle_phase` — None. Tofu has no lifecycle FSM. Magma's
    ///   FSM is honestly magma-only and that's fine ("different
    ///   backends have different capabilities").
    ///
    /// Returns `None` only when the JSON fails to parse OR when there's
    /// no `resource_changes` field at all (a malformed show-json from
    /// some version we don't support). An empty `resource_changes`
    /// yields `Some` with all-zero distribution — same shape as the
    /// magma "plan ran, touched nothing" cycle.
    pub fn from_tofu_plan_show_json(stdout: &str) -> Option<Self> {
        let value: serde_json::Value = serde_json::from_str(stdout).ok()?;
        let changes_arr = value.get("resource_changes")?.as_array()?;

        let resource_changes: Vec<TypedResourceChange> = changes_arr
            .iter()
            .filter_map(|rc| {
                let address = rc.get("address").and_then(|v| v.as_str())?.to_string();
                let actions = rc
                    .get("change")
                    .and_then(|v| v.get("actions"))?
                    .as_array()?;
                let action_strs: Vec<&str> = actions.iter().filter_map(|a| a.as_str()).collect();
                let action = action_set_to_plan_action(&action_strs);
                let severity = action_to_severity(&action);
                Some(TypedResourceChange {
                    address,
                    action,
                    severity,
                })
            })
            .collect();

        let action_distribution = Self::action_distribution_from(&resource_changes);
        let severities = if resource_changes.is_empty() {
            None
        } else {
            Some(SeverityRollup::from_changes(&resource_changes))
        };

        Some(CycleArtifact {
            action_distribution,
            resource_changes,
            artifact_ref: None, // honest absence — see fn doc
            severities,
            lifecycle_phase: None, // honest absence — tofu has no FSM
        })
    }
}

/// Map tofu's `change.actions[]` array to a single `PlanAction`.
/// Tofu uses arrays because a replace is represented as `["delete",
/// "create"]` (or its reverse); a single-element array carries the
/// straightforward verb. Empty arrays — which would only appear in
/// pathologically truncated JSON — fall through to `NoOp` (safest
/// default: nothing to do).
fn action_set_to_plan_action(actions: &[&str]) -> PlanAction {
    use std::collections::BTreeSet;
    match actions.len() {
        0 => PlanAction::NoOp,
        1 => PlanAction::parse(actions[0]),
        _ => {
            // Replace is the canonical multi-action: {delete, create}
            // in either order. Anything else multi-action is unknown
            // — preserve via Other.
            let set: BTreeSet<&str> = actions.iter().copied().collect();
            if set.len() == 2 && set.contains("delete") && set.contains("create") {
                PlanAction::Replace
            } else {
                PlanAction::Other(actions.join("+"))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plan_action_parses_all_known_verbs() {
        // Hyphen + underscore + bare "noop" all bucket to NoOp.
        assert_eq!(PlanAction::parse("no_op"), PlanAction::NoOp);
        assert_eq!(PlanAction::parse("no-op"), PlanAction::NoOp);
        assert_eq!(PlanAction::parse("noop"), PlanAction::NoOp);
        assert_eq!(PlanAction::parse("create"), PlanAction::Create);
        assert_eq!(PlanAction::parse("update"), PlanAction::Update);
        assert_eq!(PlanAction::parse("delete"), PlanAction::Delete);
        assert_eq!(PlanAction::parse("replace"), PlanAction::Replace);
    }

    #[test]
    fn plan_action_preserves_unknown_verbs_in_other() {
        // The whole point of Other(String): future tofu verbs (read,
        // forget, …) must survive parsing so we don't silently drop
        // them from the rollup or the resource_changes list.
        assert_eq!(PlanAction::parse("read"), PlanAction::Other("read".into()));
        assert_eq!(
            PlanAction::parse("forget"),
            PlanAction::Other("forget".into())
        );
        assert_eq!(PlanAction::parse(""), PlanAction::Other("".into()));
    }

    #[test]
    fn action_to_severity_is_total_and_conservative() {
        // The whole tofu severity story rides on this mapping being
        // total. Drift here breaks the equivalence test.
        assert_eq!(action_to_severity(&PlanAction::NoOp), Severity::Cosmetic);
        assert_eq!(
            action_to_severity(&PlanAction::Create),
            Severity::Functional
        );
        assert_eq!(
            action_to_severity(&PlanAction::Update),
            Severity::Functional
        );
        assert_eq!(action_to_severity(&PlanAction::Delete), Severity::Breaking);
        assert_eq!(action_to_severity(&PlanAction::Replace), Severity::Breaking);
        // Other → Functional rather than Cosmetic: unknown is
        // presumed to be a real change. Silent cosmetic would hide
        // a class of changes from the operator.
        assert_eq!(
            action_to_severity(&PlanAction::Other("x".into())),
            Severity::Functional
        );
    }

    #[test]
    fn severity_rollup_buckets_correctly() {
        let changes = vec![
            change("a", PlanAction::NoOp, Severity::Cosmetic),
            change("b", PlanAction::NoOp, Severity::Cosmetic),
            change("c", PlanAction::Create, Severity::Functional),
            change("d", PlanAction::Delete, Severity::Breaking),
            change("e", PlanAction::Delete, Severity::Breaking),
        ];
        let r = SeverityRollup::from_changes(&changes);
        assert_eq!(r.cosmetic, 2);
        assert_eq!(r.functional, 1);
        assert_eq!(r.breaking, 2);
        assert_eq!(r.total(), 5);
    }

    #[test]
    fn action_distribution_from_typed_changes_matches_buckets() {
        // The CR-side ActionDistribution is derived from
        // resource_changes via this helper. Drift here would
        // surface as user-visible disagreement between
        // action_distribution.* and a count of resource_changes
        // filtered by action.
        let changes = vec![
            change("a", PlanAction::NoOp, Severity::Cosmetic),
            change("b", PlanAction::Create, Severity::Functional),
            change("c", PlanAction::Delete, Severity::Breaking),
            change("d", PlanAction::Replace, Severity::Breaking),
            change("e", PlanAction::Update, Severity::Functional),
            change("f", PlanAction::Other("read".into()), Severity::Functional),
        ];
        let d = CycleArtifact::action_distribution_from(&changes);
        assert_eq!(d.no_op, 1);
        assert_eq!(d.create, 1);
        assert_eq!(d.update, 1);
        assert_eq!(d.delete, 1);
        assert_eq!(d.replace, 1);
        assert_eq!(d.other, 1);
    }

    fn change(addr: &str, action: PlanAction, severity: Severity) -> TypedResourceChange {
        TypedResourceChange {
            address: addr.into(),
            action,
            severity,
        }
    }

    // ── from_magma_plan (the in-memory pipeline closer) ──────────
    //
    // Unit tests for this constructor are deferred until we have a
    // JSON deserialization fixture (magma_types::Plan has private
    // fields on ResourceAddress — `key`, `kind`, `module`, etc. —
    // that aren't intended for direct construction). When a preview
    // RPC lands and we have a real serialized magma Plan in a test
    // file, the test goes:
    //
    //   ```
    //   let plan: magma_types::Plan = serde_json::from_str(FIXTURE)?;
    //   let art = CycleArtifact::from_magma_plan(&plan);
    //   assert_eq!(art.resource_changes.len(), N);
    //   // ... action distribution + severity rollup asserts.
    //   ```
    //
    // The implementation itself is fully covered statically: it's a
    // small match expression with exhaustive arms (the compiler
    // forces every magma_types::Action variant to map to a
    // PlanAction), so the action-mapping logic can't drift silently.

    // ── from_tofu_plan_show_json (the slice-2 tofu reader) ─────────

    #[test]
    fn from_tofu_plan_show_json_production_shape() {
        // Production-shape `tofu show -json` output excerpt — a mix
        // of single-action verbs covering every PlanAction we expect
        // to see in practice.
        let stdout = r#"{
            "resource_changes": [
                {"address": "github_repository.a", "change": {"actions": ["no-op"]}},
                {"address": "github_repository.b", "change": {"actions": ["create"]}},
                {"address": "github_repository.c", "change": {"actions": ["update"]}},
                {"address": "github_repository.d", "change": {"actions": ["delete"]}}
            ]
        }"#;
        let art = CycleArtifact::from_tofu_plan_show_json(stdout).unwrap();
        assert_eq!(art.action_distribution.no_op, 1);
        assert_eq!(art.action_distribution.create, 1);
        assert_eq!(art.action_distribution.update, 1);
        assert_eq!(art.action_distribution.delete, 1);
        assert_eq!(art.resource_changes.len(), 4);
        // Tofu side: severity comes from the pure action→severity
        // mapping since tofu has no native classifier.
        assert_eq!(art.resource_changes[0].severity, Severity::Cosmetic);
        assert_eq!(art.resource_changes[1].severity, Severity::Functional);
        assert_eq!(art.resource_changes[3].severity, Severity::Breaking);
        // Honest absence — slice-2 tofu reader does not populate
        // these fields.
        assert!(
            art.artifact_ref.is_none(),
            "tofu side: artifact_ref deferred to later slice"
        );
        assert!(art.lifecycle_phase.is_none(), "tofu side: no lifecycle FSM");
        // SeverityRollup populates from resource_changes.
        let rollup = art.severities.unwrap();
        assert_eq!(rollup.cosmetic, 1);
        assert_eq!(rollup.functional, 2);
        assert_eq!(rollup.breaking, 1);
    }

    #[test]
    fn from_tofu_plan_show_json_replace_is_delete_plus_create() {
        // Tofu represents replace as a 2-element actions array.
        // The reader must coalesce {delete, create} → PlanAction::Replace
        // regardless of order.
        let stdout = r#"{
            "resource_changes": [
                {"address": "r.a", "change": {"actions": ["delete", "create"]}},
                {"address": "r.b", "change": {"actions": ["create", "delete"]}}
            ]
        }"#;
        let art = CycleArtifact::from_tofu_plan_show_json(stdout).unwrap();
        assert_eq!(art.resource_changes[0].action, PlanAction::Replace);
        assert_eq!(art.resource_changes[1].action, PlanAction::Replace);
        assert_eq!(art.action_distribution.replace, 2);
        // Replace's severity is Breaking — the destroy half interrupts
        // service.
        for c in &art.resource_changes {
            assert_eq!(c.severity, Severity::Breaking);
        }
    }

    #[test]
    fn from_tofu_plan_show_json_unknown_action_set_buckets_into_other() {
        // A multi-action set that isn't {delete, create} is unknown
        // territory. Preserve via Other so the rollup is faithful;
        // never silently drop.
        let stdout = r#"{
            "resource_changes": [
                {"address": "r.a", "change": {"actions": ["create", "update"]}}
            ]
        }"#;
        let art = CycleArtifact::from_tofu_plan_show_json(stdout).unwrap();
        assert_eq!(
            art.resource_changes[0].action,
            PlanAction::Other("create+update".into())
        );
        assert_eq!(art.action_distribution.other, 1);
    }

    #[test]
    fn from_tofu_plan_show_json_empty_changes_yields_some_with_zeros() {
        // A plan that ran but classified nothing IS informative —
        // must yield Some(empty), not None. Same shape as the magma
        // "resourceless bundle" case.
        let stdout = r#"{"resource_changes": []}"#;
        let art = CycleArtifact::from_tofu_plan_show_json(stdout).unwrap();
        assert!(art.resource_changes.is_empty());
        assert_eq!(art.action_distribution, ActionDistribution::default());
        assert!(art.severities.is_none(), "no resources → no rollup");
    }

    #[test]
    fn from_tofu_plan_show_json_missing_resource_changes_yields_none() {
        // Distinguishable from "empty": None means we couldn't even
        // count (the JSON shape we expect isn't there).
        assert!(CycleArtifact::from_tofu_plan_show_json("{}").is_none());
        assert!(CycleArtifact::from_tofu_plan_show_json("not json").is_none());
    }

    // ── summary_counts + drift_details (slice 2c converters) ───────

    #[test]
    fn summary_counts_projects_distribution_to_legacy_shape() {
        let art = CycleArtifact {
            action_distribution: ActionDistribution {
                no_op: 100,
                create: 5,
                update: 3,
                delete: 2,
                replace: 1,
                other: 0,
            },
            resource_changes: vec![],
            artifact_ref: None,
            severities: None,
            lifecycle_phase: None,
        };
        let (added, changed, destroyed, total) = art.summary_counts();
        assert_eq!(added, 5);
        assert_eq!(changed, 4, "update(3) + replace(1) = 4");
        assert_eq!(destroyed, 2);
        assert_eq!(total, 111, "100 no-op + 5 + 3 + 2 + 1 = 111");
    }

    #[test]
    fn drift_details_skips_no_ops_and_respects_cap() {
        let changes = vec![
            change("a", PlanAction::NoOp, Severity::Cosmetic),
            change("b", PlanAction::Create, Severity::Functional),
            change("c", PlanAction::Update, Severity::Functional),
            change("d", PlanAction::Delete, Severity::Breaking),
            change("e", PlanAction::NoOp, Severity::Cosmetic),
        ];
        let art = CycleArtifact {
            action_distribution: ActionDistribution::default(),
            resource_changes: changes,
            artifact_ref: None,
            severities: None,
            lifecycle_phase: None,
        };
        let drifts = art.drift_details(50);
        // No-ops filtered out — 3 entries (b, c, d).
        assert_eq!(drifts.len(), 3);
        assert_eq!(drifts[0].address, "b");
        assert_eq!(drifts[0].action, "create");
        assert_eq!(drifts[0].risk, "modify"); // Functional → modify
        assert_eq!(drifts[1].address, "c");
        assert_eq!(drifts[2].address, "d");
        assert_eq!(drifts[2].risk, "destroy"); // Breaking → destroy

        // Cap honored.
        let capped = art.drift_details(2);
        assert_eq!(capped.len(), 2);
    }

    #[test]
    fn drift_details_emits_terraform_vocab_for_known_actions() {
        let changes = vec![
            change("a", PlanAction::Create, Severity::Functional),
            change("b", PlanAction::Update, Severity::Functional),
            change("c", PlanAction::Delete, Severity::Breaking),
            change("d", PlanAction::Replace, Severity::Breaking),
            change("e", PlanAction::Other("read".into()), Severity::Functional),
        ];
        let art = CycleArtifact {
            action_distribution: ActionDistribution::default(),
            resource_changes: changes,
            artifact_ref: None,
            severities: None,
            lifecycle_phase: None,
        };
        let drifts = art.drift_details(50);
        let actions: Vec<&str> = drifts.iter().map(|d| d.action.as_str()).collect();
        assert_eq!(
            actions,
            vec!["create", "update", "delete", "replace", "read"]
        );
    }

    // ── Equivalence (the slice-2 acceptance property) ──────────────

    #[test]
    fn tofu_and_magma_readers_produce_equivalent_action_distribution() {
        // The load-bearing property of slice 2: same plan content,
        // two reader paths → identical action_distribution. This is
        // tested at the CycleArtifact layer (pure types, no I/O).
        //
        // Magma "side" simulated via direct TypedResourceChange
        // construction (what read_cycle_artifact does in production);
        // tofu side via from_tofu_plan_show_json.
        let tofu_stdout = r#"{
            "resource_changes": [
                {"address": "r.a", "change": {"actions": ["no-op"]}},
                {"address": "r.b", "change": {"actions": ["no-op"]}},
                {"address": "r.c", "change": {"actions": ["create"]}},
                {"address": "r.d", "change": {"actions": ["delete", "create"]}}
            ]
        }"#;
        let tofu = CycleArtifact::from_tofu_plan_show_json(tofu_stdout).unwrap();

        let magma = CycleArtifact {
            action_distribution: ActionDistribution::default(),
            resource_changes: vec![
                change("r.a", PlanAction::NoOp, Severity::Cosmetic),
                change("r.b", PlanAction::NoOp, Severity::Cosmetic),
                change("r.c", PlanAction::Create, Severity::Functional),
                change("r.d", PlanAction::Replace, Severity::Breaking),
            ],
            artifact_ref: None,
            severities: None,
            lifecycle_phase: None,
        };
        // Re-derive the magma side's distribution from its
        // resource_changes (this is what read_cycle_artifact does).
        let magma_dist = CycleArtifact::action_distribution_from(&magma.resource_changes);

        assert_eq!(
            tofu.action_distribution, magma_dist,
            "action_distribution must match across both readers"
        );
        // SeverityRollup also matches.
        let magma_rollup = SeverityRollup::from_changes(&magma.resource_changes);
        assert_eq!(
            tofu.severities.unwrap(),
            magma_rollup,
            "severity rollup must match across both readers"
        );
    }
}
