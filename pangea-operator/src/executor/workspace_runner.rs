//! `WorkspaceRunner` — the typed, executor-agnostic abstraction
//! the controller's phase handlers speak.
//!
//! ## Why this exists (and why it's NOT a replacement for `IacExecutor`)
//!
//! `IacExecutor` (`executor/iac_executor.rs`) is shaped after the tofu
//! CLI's verbs — `init`, `plan`, `apply`, `destroy`, `show_plan`,
//! `refresh`, `import`. Its return type, `TofuResult`, is literally
//! `(exit_code, stdout, stderr, success, duration)`. `MagmaExecutor`
//! fakes this surface by stuffing magma-shaped JSON into
//! `TofuResult.stdout` and synthesizing exit codes. The leak: every
//! phase handler that wants typed plan data has to re-parse JSON out
//! of `stdout`, and the parse path silently diverges between
//! executors (a real, latent interchangeability bug today —
//! `extract_create_addresses_from_plan` expects tofu shape and gets
//! magma shape when magma runs).
//!
//! `WorkspaceRunner` is the typed layer above. Its methods return
//! `CycleArtifact` — the unified, executor-agnostic shape — so the
//! controller speaks one vocabulary regardless of which executor
//! actually ran.
//!
//! The verb-level callers (`run_import_prepass`, `conflict.rs`) that
//! genuinely speak tofu's vocabulary continue to hold
//! `Arc<dyn IacExecutor>` directly. `WorkspaceRunner` is layered ON
//! TOP, not as a replacement. Two seams, one consumer per seam.
//!
//! ## The contract (honest about capability differences)
//!
//! Different backends have different capabilities, and **that's
//! fine** — the abstraction surfaces per-backend gaps as
//! `Option<…>` fields rather than forcing every backend to
//! fabricate values it doesn't have. See `cycle_artifact.rs` for the
//! capability matrix.
//!
//! ## Migration story (in case scope expands)
//!
//! - Slice 2b: this trait + impls land. No controller-side caller
//!   yet. The trait surface is visible to tests + ready for use.
//! - Slice 2c: phase handlers (`handle_planning`, `handle_applying`,
//!   `handle_ready`, `handle_destroying`) consume `WorkspaceRunner`
//!   directly. The duplicate-JSON-reparse path in `handle_planning`
//!   collapses into one `runner.plan(workspace).await?` call. Tofu
//!   cycles start populating `action_distribution` + `bundle_ref`
//!   on CR status — the observable payoff.

use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;

use crate::error::Result;
use crate::executor::cycle_artifact::{CycleArtifact, Severity};
use crate::executor::iac_executor::IacExecutor;
use crate::executor::tofu::TofuResult;
use crate::executor::workspace::Workspace;

/// Result of a `WorkspaceRunner::plan` call.
///
/// The `artifact` is the unified post-plan shape both executors
/// populate; the rest is per-executor "raw output" retained for the
/// callers that still need it (the import prepass reads the raw JSON
/// to extract create-addresses for tofu's natural-id substitution;
/// conflict.rs reads `stdout`/`stderr` substrings).
///
/// As slice 2c migrates verb-level callers onto `CycleArtifact`, the
/// raw fields can be removed.
#[derive(Debug)]
pub struct PlanResult {
    /// Unified typed plan output — action distribution + per-resource
    /// changes + severities + (magma) lifecycle phase + (magma)
    /// bundle ref. Always populated when the plan succeeded; `None`
    /// when the executor produced no artifact (rare: plan errored
    /// before classification, or the on-disk file is missing).
    pub artifact: Option<CycleArtifact>,
    /// Path to the on-disk plan file (`tfplan` binary for tofu,
    /// `magma-bundle.json` for magma). `None` when the executor
    /// doesn't checkpoint to a fixed path.
    pub plan_file: Option<PathBuf>,
    /// Raw stdout from the underlying executor. Retained for the
    /// verb-level callers that haven't migrated yet (the import
    /// prepass, conflict resolver). New code should consume
    /// `artifact` instead.
    pub raw_stdout: String,
    /// Did the plan report changes? Mirrors tofu's `-detailed-exitcode
    /// == 2`. True when at least one resource has a non-no-op action.
    pub has_changes: bool,
}

/// Result of a `WorkspaceRunner::apply` (or `destroy`) call. Shape
/// mirrors `PlanResult` — the artifact reflects the post-apply
/// re-plan (or destroy outcome); `failed` lists resource addresses
/// that errored during apply (empty on success).
#[derive(Debug)]
pub struct ApplyResult {
    pub artifact:   Option<CycleArtifact>,
    pub raw_stdout: String,
    /// Resource addresses that errored during apply. Empty on
    /// success; populated from the executor's failure output. Used
    /// by `cycle_receipts` to mark per-resource Failed outcomes.
    pub failed:     Vec<String>,
    /// Was the apply successful overall?
    pub success:    bool,
}

/// Result of a `WorkspaceRunner::compile` call.
///
/// Placeholder for slice 3 — today's controller compiles via the
/// Ruby pool independently of the executor. When slice 3 unifies
/// compile through the runner trait, `CompileResult` will carry the
/// rendered `main.tf.json` + any compile diagnostics.
#[derive(Debug, Default)]
pub struct CompileResult {
    /// Whether the compile produced a `main.tf.json` on disk.
    pub rendered: bool,
}

/// One validation diagnostic produced by `WorkspaceRunner::validate`.
/// Tofu populates these from `tofu validate`'s structured output;
/// magma populates from `magma_test_laws::preflight` violations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Diagnostic {
    pub severity: Severity,
    pub message:  String,
    /// Optional resource address the diagnostic refers to. Magma's
    /// preflight violations often name a specific resource;
    /// `tofu validate` diagnostics often don't.
    pub address:  Option<String>,
}

/// The typed, executor-agnostic abstraction the controller's phase
/// handlers speak. Each method returns a unified `CycleArtifact` (or
/// equivalent typed result) regardless of which executor backs the
/// implementation.
///
/// Each impl wraps an `IacExecutor` — see `TofuWorkspaceRunner` and
/// `MagmaWorkspaceRunner` below. The runner does the artifact
/// reading + the conversion from the executor's native shape to the
/// unified shape; the controller just consumes the typed result.
#[async_trait]
pub trait WorkspaceRunner: Send + Sync + 'static {
    /// Stable identifier for the backing executor — `"tofu"`,
    /// `"magma"`, `"recording"` (test mock). Mirrors
    /// `IacExecutor::name()` so the runner can be queried for
    /// identity without unwrapping its inner executor.
    fn name(&self) -> &'static str;

    /// Short human-readable identifier for the state backend the
    /// underlying executor reads/writes. `Some("pg/<schema>")` for
    /// magma, `None` for tofu (the backend is workspace-declared).
    fn backend_descriptor(&self) -> Option<String>;

    /// Compile the workspace's Ruby DSL → `main.tf.json`. Placeholder
    /// today; the compile path lives in the controller's Ruby pool
    /// for now (slice 3 wires it through here).
    async fn compile(&self, workspace: &Workspace) -> Result<CompileResult>;

    /// Run `plan` against the workspace + read back the typed artifact.
    /// Both executors produce equivalent shape via the unified
    /// `CycleArtifact` regardless of which native format they wrote.
    async fn plan(&self, workspace: &Workspace) -> Result<PlanResult>;

    /// Apply the most recently planned changes. The returned
    /// `ApplyResult.artifact` reflects the post-apply state (a
    /// re-plan executed after apply by the underlying executor).
    async fn apply(&self, workspace: &Workspace, auto_approve: bool) -> Result<ApplyResult>;

    /// Destroy all resources in the workspace's state.
    async fn destroy(&self, workspace: &Workspace, auto_approve: bool) -> Result<ApplyResult>;

    /// Run the executor's validation pass. Tofu wraps `tofu validate`;
    /// magma wraps `magma_test_laws::preflight::check_workspace_full`.
    /// Returns diagnostics — empty vec means clean.
    async fn validate(&self, workspace: &Workspace) -> Result<Vec<Diagnostic>>;

    /// Borrow the underlying `IacExecutor` for the verb-level callers
    /// that still need it (the import prepass, conflict resolver).
    /// New code should NOT reach for this — it's the escape hatch
    /// that keeps slice-2c migration incremental. Slice 3 retires it.
    fn iac_executor(&self) -> Arc<dyn IacExecutor>;
}

/// `WorkspaceRunner` for tofu workspaces — wraps `TofuExecutor`
/// (or any `IacExecutor` shaped like it). Plan/apply translate
/// tofu's subprocess output into the unified `CycleArtifact` via
/// `cycle_artifact::from_tofu_plan_show_json`.
pub struct TofuWorkspaceRunner {
    inner: Arc<dyn IacExecutor>,
}

impl TofuWorkspaceRunner {
    /// Construct a tofu runner over the given `IacExecutor`. The
    /// inner executor MUST be tofu-shaped — its plan/show_plan
    /// output must conform to `tofu show -json` format. Passing
    /// anything else is a bug (the runner's name claim diverges
    /// from the inner executor's name).
    pub fn new(inner: Arc<dyn IacExecutor>) -> Self {
        Self { inner }
    }
}

#[async_trait]
impl WorkspaceRunner for TofuWorkspaceRunner {
    fn name(&self) -> &'static str {
        "tofu"
    }

    fn backend_descriptor(&self) -> Option<String> {
        self.inner.backend_descriptor()
    }

    async fn compile(&self, _workspace: &Workspace) -> Result<CompileResult> {
        // Slice 2b: no-op (the controller's Ruby pool still handles
        // compile out-of-band). Slice 3 wires this through.
        Ok(CompileResult { rendered: false })
    }

    async fn plan(&self, workspace: &Workspace) -> Result<PlanResult> {
        let plan_path = workspace.plan_path();
        let plan = self.inner.plan(&workspace.path, Some(&plan_path), &[]).await?;
        // `-detailed-exitcode == 2` means "changes pending"
        let has_changes = plan.exit_code == 2;
        // Run `tofu show -json <tfplan>` to get the typed JSON.
        // This is what `handle_planning` already does today; the
        // runner just centralizes it.
        let show = match self.inner.show_plan(&workspace.path, &plan_path).await {
            Ok(r)  => r.stdout,
            Err(_) => String::new(),
        };
        let artifact = CycleArtifact::from_tofu_plan_show_json(&show);
        Ok(PlanResult {
            artifact,
            plan_file: Some(plan_path),
            raw_stdout: plan.stdout,
            has_changes,
        })
    }

    async fn apply(&self, workspace: &Workspace, auto_approve: bool) -> Result<ApplyResult> {
        let plan_path = workspace.plan_path();
        let r = self.inner.apply(&workspace.path, Some(&plan_path), auto_approve).await?;
        // Tofu apply has no native "artifact" — the post-apply re-plan
        // would produce one, but slice 2b doesn't run it. Returns
        // None; slice 2c's handle_applying explicitly re-plans + uses
        // the new artifact.
        Ok(ApplyResult {
            artifact:   None,
            failed:     extract_failed_addresses_from_tofu(&r),
            raw_stdout: r.stdout,
            success:    r.success,
        })
    }

    async fn destroy(&self, workspace: &Workspace, auto_approve: bool) -> Result<ApplyResult> {
        let r = self.inner.destroy(&workspace.path, auto_approve).await?;
        Ok(ApplyResult {
            artifact:   None,
            failed:     extract_failed_addresses_from_tofu(&r),
            raw_stdout: r.stdout,
            success:    r.success,
        })
    }

    async fn validate(&self, _workspace: &Workspace) -> Result<Vec<Diagnostic>> {
        // Slice 2b: no-op (`tofu validate` plumbing lands in
        // slice 2.17 alongside magma's preflight surface).
        Ok(Vec::new())
    }

    fn iac_executor(&self) -> Arc<dyn IacExecutor> {
        Arc::clone(&self.inner)
    }
}

/// `WorkspaceRunner` for magma workspaces — wraps `MagmaExecutor`
/// (or any `IacExecutor` shaped like it). Plan/apply read the
/// unified `CycleArtifact` from `magma-bundle.json` via
/// `magma_bundle::read_cycle_artifact`.
pub struct MagmaWorkspaceRunner {
    inner: Arc<dyn IacExecutor>,
}

impl MagmaWorkspaceRunner {
    /// Construct a magma runner. The inner executor MUST be magma —
    /// its plan call must produce a `magma-bundle.json` in the
    /// workspace dir.
    pub fn new(inner: Arc<dyn IacExecutor>) -> Self {
        Self { inner }
    }
}

#[async_trait]
impl WorkspaceRunner for MagmaWorkspaceRunner {
    fn name(&self) -> &'static str {
        "magma"
    }

    fn backend_descriptor(&self) -> Option<String> {
        self.inner.backend_descriptor()
    }

    async fn compile(&self, _workspace: &Workspace) -> Result<CompileResult> {
        Ok(CompileResult { rendered: false })
    }

    async fn plan(&self, workspace: &Workspace) -> Result<PlanResult> {
        let plan_path = workspace.path.join("magma-plan.json");
        let plan = self.inner.plan(&workspace.path, Some(&plan_path), &[]).await?;
        let has_changes = plan.exit_code == 2;
        // Magma writes magma-bundle.json into the workspace dir as
        // part of plan. Read it via the unified reader; the returned
        // artifact carries action distribution + per-resource changes
        // + severities + (when present) lifecycle_phase + bundle_ref.
        let artifact = crate::executor::magma_bundle::read_cycle_artifact(&workspace.path).await;
        Ok(PlanResult {
            artifact,
            plan_file: Some(plan_path),
            raw_stdout: plan.stdout,
            has_changes,
        })
    }

    async fn apply(&self, workspace: &Workspace, auto_approve: bool) -> Result<ApplyResult> {
        let plan_path = workspace.path.join("magma-plan.json");
        let r = self.inner.apply(&workspace.path, Some(&plan_path), auto_approve).await?;
        // Magma's apply updates magma-bundle.json with the apply
        // outcome — re-read to surface the post-apply artifact.
        let artifact = crate::executor::magma_bundle::read_cycle_artifact(&workspace.path).await;
        Ok(ApplyResult {
            artifact,
            failed:     Vec::new(), // magma surfaces failures via Error::MagmaExecution today
            raw_stdout: r.stdout,
            success:    r.success,
        })
    }

    async fn destroy(&self, workspace: &Workspace, auto_approve: bool) -> Result<ApplyResult> {
        let r = self.inner.destroy(&workspace.path, auto_approve).await?;
        Ok(ApplyResult {
            artifact:   None,
            failed:     Vec::new(),
            raw_stdout: r.stdout,
            success:    r.success,
        })
    }

    async fn validate(&self, _workspace: &Workspace) -> Result<Vec<Diagnostic>> {
        // Slice 2.17 wires this through `magma_test_laws::preflight`.
        Ok(Vec::new())
    }

    fn iac_executor(&self) -> Arc<dyn IacExecutor> {
        Arc::clone(&self.inner)
    }
}

/// Extract per-resource failure addresses from a tofu apply's stdout.
/// Tofu apply prints `Error: <address>: ...` lines for each failed
/// resource; this helper greps them out. Best-effort — returns an
/// empty vec on any parse difficulty. Used by `TofuWorkspaceRunner::
/// apply` to populate `ApplyResult.failed`.
fn extract_failed_addresses_from_tofu(r: &TofuResult) -> Vec<String> {
    if r.success {
        return Vec::new();
    }
    let mut out = Vec::new();
    for line in r.stdout.lines().chain(r.stderr.lines()) {
        // Tofu's failure lines have the shape:
        //   Error: <address>: <message>
        // or:
        //   ╷
        //   │ Error: <address>: <message>
        let trimmed = line.trim_start_matches(|c: char| !c.is_alphanumeric());
        if let Some(rest) = trimmed.strip_prefix("Error: ") {
            if let Some(addr_end) = rest.find(": ") {
                let addr = rest[..addr_end].trim();
                // Plausible tofu address: must contain a dot
                // (`<type>.<name>` or `module.x.<type>.<name>`).
                if addr.contains('.') && !addr.contains('\n') {
                    out.push(addr.to_string());
                }
            }
        }
    }
    out.sort();
    out.dedup();
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn workspace_runner_trait_is_object_safe() {
        // The whole point of an Arc<dyn WorkspaceRunner> story:
        // verify object safety at the type level. If a future method
        // breaks it (Self return, generics), this fails to compile.
        fn _accepts(_r: Arc<dyn WorkspaceRunner>) {}
    }

    #[test]
    fn extract_failed_addresses_finds_addresses_in_error_lines() {
        let r = TofuResult {
            exit_code: 1,
            stdout: "Error: github_repository.foo: provider error: rate limit\n\
                     Error: github_team.bar: not found".to_string(),
            stderr: String::new(),
            success: false,
            duration: std::time::Duration::from_secs(1),
        };
        let addrs = extract_failed_addresses_from_tofu(&r);
        assert_eq!(addrs, vec!["github_repository.foo".to_string(), "github_team.bar".to_string()]);
    }

    #[test]
    fn extract_failed_addresses_handles_boxed_tofu_diagnostics() {
        // Tofu's terminal output decorates Error lines with box-drawing
        // characters when running on a TTY. The extractor needs to
        // strip those before finding the address.
        let r = TofuResult {
            exit_code: 1,
            stdout: "╷\n│ Error: github_repository.foo: bad\n│\n╵".to_string(),
            stderr: String::new(),
            success: false,
            duration: std::time::Duration::from_secs(1),
        };
        let addrs = extract_failed_addresses_from_tofu(&r);
        assert_eq!(addrs, vec!["github_repository.foo".to_string()]);
    }

    #[test]
    fn extract_failed_addresses_returns_empty_on_success() {
        let r = TofuResult {
            exit_code: 0,
            stdout: "Apply complete!".to_string(),
            stderr: String::new(),
            success: true,
            duration: std::time::Duration::from_secs(1),
        };
        assert!(extract_failed_addresses_from_tofu(&r).is_empty());
    }

    #[test]
    fn extract_failed_addresses_dedups_repeated_addresses() {
        // Sometimes tofu prints the same Error line in both stdout
        // and stderr (or twice in stdout). Dedup so the failed list
        // doesn't double-count.
        let r = TofuResult {
            exit_code: 1,
            stdout: "Error: r.a: x\nError: r.b: y".to_string(),
            stderr: "Error: r.a: x".to_string(),
            success: false,
            duration: std::time::Duration::from_secs(1),
        };
        let addrs = extract_failed_addresses_from_tofu(&r);
        assert_eq!(addrs, vec!["r.a".to_string(), "r.b".to_string()]);
    }
}
