//! `PlannedChange` — the typed, executor-agnostic border for import
//! discovery.
//!
//! # Why this exists
//!
//! The import prepass (`controller::template_controller::run_import_prepass`)
//! needs to know, for a computed plan, *which addresses are create-actions*
//! and *what attributes a create WOULD have written* (the plan's
//! `change.after`) so it can adopt an already-existing cloud resource via
//! `import` instead of blindly re-creating it (the create-that-exists → 422
//! → cycle-Failed wedge).
//!
//! Historically that answer came from `executor.show_plan(work_dir,
//! plan_file)` returning a stringly tofu-`show -json` payload the prepass
//! re-parsed. That single seam carried TWO defects on the magma
//! (DB-backed, disk-free) path:
//!
//!   1. **disk-dependence** — `MagmaExecutor::show_plan` did an
//!      unconditional `tokio::fs::read(plan_file)`, but on the DB-backed
//!      path the plan lives only in Postgres, so the file was absent →
//!      `Err` → import silently disabled → magma planned every existing
//!      repo as Create → 422 → cycle `Failed` (the breathe class).
//!   2. **tofu-format-dependence** — even *with* a file, magma
//!      re-serialized a magma-shape JSON that the tofu-shape parsers
//!      (`/change/actions`, `/change/after`, `address` as string) read as
//!      empty.
//!
//! `PlannedChange` replaces that seam with a typed value carrying exactly
//! what discovery needs — a rendered address, the action, the planned
//! `after` attrs, and the resource kind class — with **NO filesystem path**
//! and **NO stringly tofu-format channel**. Both executors PRODUCE it (magma
//! off its typed `Plan`, the tofu path via the legacy fallback); import
//! discovery CONSUMES it.
//!
//! Per the org ★★ MAGMA-NATIVE EXECUTION directive (disk-free by default)
//! and ★★ TYPED-SPEC + INTERPRETER TRIPLET (typed border, not a stringly
//! channel).

/// A single planned resource change, projected out of an executor's typed
/// plan into the executor-agnostic shape import discovery consumes.
///
/// This is deliberately **not** the full magma `ResourceChange` — it
/// carries only the four fields the prepass reads, so the border stays a
/// stable contract independent of magma's (or any future executor's)
/// internal plan representation.
#[derive(Debug, Clone, PartialEq)]
pub struct PlannedChange {
    /// `format!("{}.{}", type_id, name)` — the exact `{type}.{name}`
    /// address form `to_universal_plan` and the tofu-format strings use,
    /// so `naturalIds` / `importHints` / create-set lookups never miss.
    ///
    /// Tier-honest: this drops the `module` / `key` components (as the
    /// whole magma path already does). Keyed / nested resources are a
    /// named pre-existing gap — the pleme-io-opensource `github_repository`
    /// fleet is flat / unkeyed, so this is exact for the breathe class; a
    /// shape-complete renderer is a later phase.
    pub address: String,

    /// The plan action for this address. Import discovery filters on
    /// `Create`.
    pub action: PlanAction,

    /// The planned `change.after` block — the attributes the apply WOULD
    /// have written if the resource didn't already exist. Import discovery
    /// substitutes these into `naturalIds` templates (`{{ .planned.name }}`
    /// etc.). `None` maps to `serde_json::Value::Null` downstream, which
    /// `substitute_with_planned` treats identically to a missing attribute.
    pub after: Option<serde_json::Value>,

    /// The resource kind class. Import discovery adopts `Managed` resources
    /// only — never a Data source, an Output, or a Local value.
    pub kind: ResourceKindClass,

    /// The provider-native import id the EXECUTOR derived for this change,
    /// when it could derive one it stands behind.
    ///
    /// # Why the executor and not the prepass
    ///
    /// A composite import id is very often keyed on a **parent's real name**
    /// (`github_issue_label` imports by `<repo-name>:<label>`), and [`after`]
    /// holds that parent as an unresolved reference —
    /// `"${github_repository.caixa_tlisp_handoff.name}"`. Turning that into
    /// `caixa-tlisp-handoff` needs the workspace's STATE, which only the
    /// executor can read. The prepass has no state and no way to get it, so
    /// any id it composes itself out of `after` alone is either the raw
    /// `${…}` literal or a resource-name guess — the exact defect that made
    /// every proactive import 404 and left the creates to collide forever.
    ///
    /// `None` means "no id I stand behind" — either no executor-side
    /// derivation (the tofu / legacy path) or a derivation the executor
    /// refused to round up. The prepass then falls back to the declared
    /// `importHints` / `naturalIds` layers.
    ///
    /// [`after`]: PlannedChange::after
    pub import_id: Option<DerivedImportId>,
}

/// An import id an executor derived, plus whether it is exact.
///
/// Deliberately **not** magma's `natural_id::ImportId`: this border must
/// compile without the `executor_magma` feature, and it must not name a
/// second executor's types either. `exact` is the projection of magma's
/// four-way `Confidence` onto the only question the prepass asks — *may I
/// dispatch an import RPC against this id?*
///
/// Never round up: a convention-guessed parent (`tag_forge` where the repo
/// is really `tag-forge`) and a last-resort address name are both
/// `exact: false`. An inexact id names a resource that may not be the one
/// planned, and a wrong adoption is worse than no adoption — so the prepass
/// refuses it rather than dispatching and hoping.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DerivedImportId {
    pub id: String,
    pub exact: bool,
}

/// The plan action for a [`PlannedChange`]. A total projection of the
/// executor's action surface (magma's 9-variant `Action`, tofu's action
/// list) onto the six shapes discovery distinguishes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlanAction {
    NoOp,
    Create,
    Read,
    Update,
    Replace,
    Delete,
}

/// The resource kind class for a [`PlannedChange`]. Import discovery only
/// ever adopts [`ResourceKindClass::Managed`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResourceKindClass {
    Managed,
    Data,
    Output,
    Local,
}

// ── magma projections ──────────────────────────────────────────────
//
// Feature-gated: the `From<&magma_types::_>` impls are the only place
// this border touches magma. The border types themselves are always
// compiled (the trait method + tofu path reference them regardless of
// the `executor_magma` feature).

/// Total map from magma's `Action` onto [`PlanAction`]. Mirrors
/// `to_universal_plan`'s action folding (magma.rs): the compound
/// `*ThenDelete`/`*ThenCreate` actions fold to `Replace`, `Forget` to
/// `Delete`, `Read` stays distinct.
#[cfg(feature = "executor_magma")]
impl From<&magma_types::Action> for PlanAction {
    fn from(a: &magma_types::Action) -> Self {
        match a {
            magma_types::Action::NoOp => PlanAction::NoOp,
            magma_types::Action::Create => PlanAction::Create,
            magma_types::Action::Read => PlanAction::Read,
            magma_types::Action::Update => PlanAction::Update,
            magma_types::Action::Replace => PlanAction::Replace,
            magma_types::Action::Delete => PlanAction::Delete,
            magma_types::Action::Forget => PlanAction::Delete,
            magma_types::Action::CreateThenDelete => PlanAction::Replace,
            magma_types::Action::DeleteThenCreate => PlanAction::Replace,
        }
    }
}

/// Total map from magma's `ResourceKind` onto [`ResourceKindClass`].
/// magma's `Variable` kind folds to `Local` (both are non-provider,
/// non-adoptable local values; discovery excludes them the same way).
#[cfg(feature = "executor_magma")]
impl From<&magma_types::ResourceKind> for ResourceKindClass {
    fn from(k: &magma_types::ResourceKind) -> Self {
        match k {
            magma_types::ResourceKind::Managed => ResourceKindClass::Managed,
            magma_types::ResourceKind::Data => ResourceKindClass::Data,
            magma_types::ResourceKind::Output => ResourceKindClass::Output,
            magma_types::ResourceKind::Local => ResourceKindClass::Local,
            magma_types::ResourceKind::Variable => ResourceKindClass::Local,
        }
    }
}

#[cfg(all(test, feature = "executor_magma"))]
mod magma_projection_tests {
    use super::*;

    #[test]
    fn action_projection_is_total_and_matches_universal_folding() {
        assert_eq!(
            PlanAction::from(&magma_types::Action::Create),
            PlanAction::Create
        );
        assert_eq!(
            PlanAction::from(&magma_types::Action::NoOp),
            PlanAction::NoOp
        );
        assert_eq!(
            PlanAction::from(&magma_types::Action::Read),
            PlanAction::Read
        );
        assert_eq!(
            PlanAction::from(&magma_types::Action::Update),
            PlanAction::Update
        );
        assert_eq!(
            PlanAction::from(&magma_types::Action::Replace),
            PlanAction::Replace
        );
        assert_eq!(
            PlanAction::from(&magma_types::Action::Delete),
            PlanAction::Delete
        );
        assert_eq!(
            PlanAction::from(&magma_types::Action::Forget),
            PlanAction::Delete
        );
        assert_eq!(
            PlanAction::from(&magma_types::Action::CreateThenDelete),
            PlanAction::Replace
        );
        assert_eq!(
            PlanAction::from(&magma_types::Action::DeleteThenCreate),
            PlanAction::Replace
        );
    }

    #[test]
    fn kind_projection_maps_managed_and_folds_variable_to_local() {
        assert_eq!(
            ResourceKindClass::from(&magma_types::ResourceKind::Managed),
            ResourceKindClass::Managed
        );
        assert_eq!(
            ResourceKindClass::from(&magma_types::ResourceKind::Data),
            ResourceKindClass::Data
        );
        assert_eq!(
            ResourceKindClass::from(&magma_types::ResourceKind::Variable),
            ResourceKindClass::Local
        );
    }
}
