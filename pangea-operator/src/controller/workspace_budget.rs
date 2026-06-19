//! Per-workspace concurrency budget (lifecycle S1) — the operator's alias onto
//! the generic [`super::scheduling::FairBudget`].
//!
//! The admission-control primitive (scope-fair concurrency caps, RAII permits,
//! cross-scope isolation) is generic and lives in [`super::scheduling`] next to
//! the fair-priority scheduler, so the whole "bound work + never starve a
//! tenant + converge in the most valuable order" substrate is one extractable
//! unit. Here the *scope* is the **workspace** (the reconcile/state/apply seam —
//! see [`super::workspace_lifecycle`]): bound how many of a workspace's
//! templates do expensive (compile/plan/apply) work at once, so a wedged
//! workspace can never starve the others (`feedback_pangea_template_envfetch_compile_wedge`).
//!
//! Proofs (per-scope cap, global cap, cross-scope isolation, RAII release) live
//! with the generic primitive in [`super::scheduling`].

pub use super::scheduling::{BudgetConfig, BudgetPermit, FairBudget as WorkspaceBudgets};
