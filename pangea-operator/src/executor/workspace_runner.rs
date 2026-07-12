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
/// Carries every flavor of output the controller's existing callers
/// need so the migration doesn't double-call the executor:
///   * `artifact` — typed unified shape for the new status fields.
///   * `raw_show_json` — the `tofu show -json` output (tofu side) for
///     the legacy `Plan::from_json` → `DriftDetail` path that
///     `evaluate_policy` consumes. Empty on the magma side (magma
///     doesn't produce show-JSON; the equivalent live in `artifact`).
///   * `raw_stdout` — the underlying executor's plan stdout. Kept for
///     verb-level callers (`conflict.rs` reads substrings, the import
///     prepass reads show-plan output via the inner `IacExecutor`).
///   * `plan_file` — path to the on-disk plan artifact.
///   * `has_changes` — mirrors tofu's `-detailed-exitcode == 2`.
#[derive(Debug)]
pub struct PlanResult {
    /// Unified typed plan output — action distribution + per-resource
    /// changes + severities + (magma) lifecycle phase + (magma)
    /// bundle ref. `None` only when the executor produced no
    /// artifact (rare: plan errored before classification).
    pub artifact: Option<CycleArtifact>,
    /// Path to the on-disk plan file (`tfplan` binary for tofu,
    /// `magma-bundle.json` for magma).
    pub plan_file: Option<PathBuf>,
    /// Raw stdout from the underlying executor's `plan` call.
    pub raw_stdout: String,
    /// The `tofu show -json <plan_file>` output. The tofu runner
    /// computes it inline (one show_plan call) so callers needing
    /// the legacy `Plan::from_json` path don't have to issue a
    /// second show_plan. Empty on the magma side — magma doesn't
    /// emit tofu-format show-JSON; consumers wanting per-resource
    /// detail should use `artifact.resource_changes` instead.
    pub raw_show_json: String,
    /// Did the plan report changes? Mirrors tofu's `-detailed-exitcode
    /// == 2`. True when at least one resource has a non-no-op action.
    pub has_changes: bool,
    /// True iff the underlying executor's plan call succeeded
    /// (exit_code 0 or 2 — both mean "plan ran"; 1 means error).
    pub success: bool,
    /// stderr from the executor's plan call. Used by error-path
    /// callers that need to surface diagnostics.
    pub raw_stderr: String,
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
    ///
    /// ## Contract: MUST be safe to call unconditionally
    ///
    /// Callers (`handle_destroying`) call this on every workspace whose
    /// CR is being deleted, with **no backend-specific pre-check** for
    /// "was this workspace ever actually applied." That check used to
    /// live in the phase handler as `workspace.file_exists(".terraform")`
    /// — a tofu-only signal (`.terraform` is the directory `tofu init`
    /// creates). `MagmaExecutor::init` is a documented no-op and never
    /// creates that directory, so the guard was permanently false for
    /// every magma-executed template (magma has been the fleet default
    /// since 2026-06-02) — `destroy` was silently never invoked and real
    /// cloud infrastructure leaked on every CR deletion. See
    /// `docs/postmortems/` for the sibling state-corruption incident this
    /// class of bug belongs to.
    ///
    /// The fix moves the "never applied" case INSIDE each implementation,
    /// where it belongs (it's backend-specific), and turns it into a
    /// genuine no-op rather than a caller-side skip:
    ///   - `MagmaWorkspaceRunner`: `MagmaExecutor::destroy` reads state
    ///     from the backend and diffs an empty state to zero resource
    ///     deletes — already a real no-op, proven by
    ///     `destroy_against_empty_state_succeeds`.
    ///   - `TofuWorkspaceRunner`: runs `tofu init` first (idempotent) if
    ///     `.terraform` is missing, so `tofu destroy` always runs against
    ///     an initialized workspace — with no prior apply that's tofu's
    ///     own "No objects need to be destroyed" no-op, not an error.
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
        // `-detailed-exitcode == 2` means "changes pending". 0 means
        // success no changes; 1 means error.
        let has_changes = plan.exit_code == 2;
        let plan_success = plan.exit_code == 0 || plan.exit_code == 2;
        // Run `tofu show -json <tfplan>` once to get the typed JSON.
        // Carry both the parsed artifact AND the raw stdout — the
        // legacy `Plan::from_json` consumers (drift_details, policy
        // evaluation) need the raw form until they migrate to the
        // typed `CycleArtifact.resource_changes` path.
        let raw_show_json = match self.inner.show_plan(&workspace.path, &plan_path).await {
            Ok(r)  => r.stdout,
            Err(_) => String::new(),
        };
        let artifact = CycleArtifact::from_tofu_plan_show_json(&raw_show_json);
        Ok(PlanResult {
            artifact,
            plan_file: Some(plan_path),
            raw_stdout: plan.stdout,
            raw_show_json,
            has_changes,
            success: plan_success,
            raw_stderr: plan.stderr,
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
        // tofu refuses to run any state-touching command against an
        // uninitialized working directory ("Error: Backend
        // initialization required"). A workspace whose CR never made
        // it past Compiling (or was created and deleted before its
        // first Planning cycle) legitimately has no `.terraform` dir.
        // `tofu init` is idempotent — run it first so `destroy` always
        // sees an initialized workspace and degrades to tofu's own
        // "No objects need to be destroyed" no-op instead of erroring
        // (or, pre-fix, instead of the caller skipping destroy
        // entirely and orphaning real infrastructure — see the trait
        // doc on `WorkspaceRunner::destroy`).
        if !workspace.file_exists(".terraform") {
            self.inner.init(&workspace.path, &[]).await?;
        }
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
/// (or any `IacExecutor` shaped like it).
///
/// ## Where the cycle artifact comes from — DB-backed vs disk
///
/// On the production magma path the durable bundle lives in **Postgres**
/// (the `ArtifactStore`), not on disk: the executor's plan/apply persist
/// it via `put_bundle` / `put_apply_result`, and the cycle-receipt
/// enrichment downstream (`cycle_receipts.rs`) resolves it back through
/// `get_bundle_bytes`. The runner therefore does NOT read
/// `magma-bundle.json` from the workspace dir when an `artifact_store`
/// is wired — that disk file does not exist on the zero-disk path, and
/// reading it was a latent zero-disk hole (an unconditional disk read on
/// the Some(store) path). The `PlanResult.artifact` / `ApplyResult.
/// artifact` are simply `None` on the DB path; the receipt is enriched
/// from the DB downstream.
///
/// When `artifact_store` is `None` (DB-less unit tests + the documented
/// disk fallback) the runner keeps reading `magma-bundle.json` via
/// `magma_bundle::read_cycle_artifact`, preserving tofu/test parity.
pub struct MagmaWorkspaceRunner {
    inner: Arc<dyn IacExecutor>,
    /// `Some` on the production DB-backed path → the bundle lives in
    /// Postgres, so skip the workspace-dir `magma-bundle.json` disk
    /// read entirely (zero-disk). `None` → DB-less path, read the disk
    /// bundle as before. Parallels how `MagmaExecutorConfig.
    /// artifact_store` selects the executor's own DB-vs-disk routing.
    artifact_store: Option<Arc<crate::backend::ArtifactStore>>,
}

impl MagmaWorkspaceRunner {
    /// Construct a magma runner. The inner executor MUST be magma —
    /// its plan call must produce a `magma-bundle.json` in the
    /// workspace dir.
    ///
    /// `artifact_store` mirrors the executor's own routing: `Some` on
    /// the DB-backed zero-disk path (skip the `magma-bundle.json` disk
    /// read; the bundle is in Postgres), `None` on the disk-fallback /
    /// test path (read the disk bundle). Construct it from the same
    /// `ControllerState.artifact_store` the `MagmaExecutorConfig` is
    /// wired from — see `executor_runner_for`.
    pub fn new(
        inner: Arc<dyn IacExecutor>,
        artifact_store: Option<Arc<crate::backend::ArtifactStore>>,
    ) -> Self {
        Self { inner, artifact_store }
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
        let plan_success = plan.exit_code == 0 || plan.exit_code == 2;
        // On the DB-backed path the bundle is in Postgres (the executor
        // persisted it via `put_bundle`); there is NO `magma-bundle.json`
        // on disk to read (zero-disk). Return `None` here — the cycle
        // receipt is enriched from the DB downstream
        // (`cycle_receipts.rs` → `get_bundle_bytes`). On the disk-fallback
        // path (`artifact_store` None) read the disk bundle as before.
        let artifact = if self.artifact_store.is_some() {
            None
        } else {
            crate::executor::magma_bundle::read_cycle_artifact(&workspace.path).await
        };
        Ok(PlanResult {
            artifact,
            plan_file: Some(plan_path),
            raw_stdout: plan.stdout,
            // Magma doesn't emit tofu-format show-JSON. Consumers that
            // need per-resource detail should read `artifact.resource_changes`
            // instead of `Plan::from_json`. Empty here.
            raw_show_json: String::new(),
            has_changes,
            success: plan_success,
            raw_stderr: plan.stderr,
        })
    }

    async fn apply(&self, workspace: &Workspace, auto_approve: bool) -> Result<ApplyResult> {
        let plan_path = workspace.path.join("magma-plan.json");
        let r = self.inner.apply(&workspace.path, Some(&plan_path), auto_approve).await?;
        // DB-backed path: the post-apply bundle is committed to Postgres
        // (atomically with state via `put_apply_result`); there is no
        // `magma-bundle.json` on disk to re-read (zero-disk). `None` here;
        // the receipt is resolved from the DB downstream. Disk-fallback
        // path (`artifact_store` None): re-read the disk bundle to surface
        // the post-apply artifact, as before.
        let artifact = if self.artifact_store.is_some() {
            None
        } else {
            crate::executor::magma_bundle::read_cycle_artifact(&workspace.path).await
        };
        Ok(ApplyResult {
            artifact,
            failed:     Vec::new(), // magma surfaces failures via Error::MagmaExecution today
            raw_stdout: r.stdout,
            success:    r.success,
        })
    }

    async fn destroy(&self, workspace: &Workspace, auto_approve: bool) -> Result<ApplyResult> {
        // No pre-check needed: `MagmaExecutor::destroy` reads state from
        // the backend and diffs it to zero deletes when empty — already
        // a real no-op for a workspace that was never applied (proven by
        // `magma.rs`'s `destroy_against_empty_state_succeeds`). See the
        // trait doc on `WorkspaceRunner::destroy` for why callers must
        // NOT gate this call on a tofu-specific artifact like
        // `.terraform` — that guard is permanently false for magma
        // (`init` is a documented no-op) and was silently skipping every
        // magma destroy fleet-wide.
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
    use crate::executor::recording::RecordingExecutor;
    use crate::executor::workspace::WorkspaceManager;

    #[test]
    fn workspace_runner_trait_is_object_safe() {
        // The whole point of an Arc<dyn WorkspaceRunner> story:
        // verify object safety at the type level. If a future method
        // breaks it (Self return, generics), this fails to compile.
        fn _accepts(_r: Arc<dyn WorkspaceRunner>) {}
    }

    /// Regression test for the fleet-wide bug where `handle_destroying`
    /// gated its only call to `runner.destroy()` on
    /// `workspace.file_exists(".terraform")` — a tofu-only artifact.
    /// The guard was moved OUT of the phase handler and INTO
    /// `TofuWorkspaceRunner::destroy` itself, which now runs `init`
    /// first when `.terraform` is missing so `destroy` always sees an
    /// initialized workspace. This proves that repair: a workspace
    /// that was never `tofu init`-ed still gets a real `destroy` call
    /// (init, then destroy — never neither).
    #[tokio::test]
    async fn tofu_destroy_initializes_first_when_never_initialized() {
        let recorder = Arc::new(RecordingExecutor::new());
        let runner = TofuWorkspaceRunner::new(Arc::clone(&recorder) as Arc<dyn IacExecutor>);

        let tmp = tempfile::tempdir().unwrap();
        let manager = WorkspaceManager::new(tmp.path().to_path_buf());
        let workspace = manager.get_or_create("ns", "never-initialized").await.unwrap();
        assert!(!workspace.file_exists(".terraform"), "fixture must start uninitialized");

        let result = runner.destroy(&workspace, true).await.unwrap();
        assert!(result.success, "destroy against a never-initialized tofu workspace must succeed");

        let calls: Vec<&'static str> =
            recorder.recorded_calls().iter().map(|c| c.command).collect();
        assert_eq!(
            calls,
            vec!["init", "destroy"],
            "destroy() must init an uninitialized workspace before destroying, \
             never silently skip the destroy call: got {calls:?}"
        );
    }

    /// Sibling of the above: when `.terraform` already exists (the
    /// normal post-apply case), `destroy()` must NOT re-init — only
    /// `destroy` is called.
    #[tokio::test]
    async fn tofu_destroy_skips_init_when_already_initialized() {
        let recorder = Arc::new(RecordingExecutor::new());
        let runner = TofuWorkspaceRunner::new(Arc::clone(&recorder) as Arc<dyn IacExecutor>);

        let tmp = tempfile::tempdir().unwrap();
        let manager = WorkspaceManager::new(tmp.path().to_path_buf());
        let workspace = manager.get_or_create("ns", "already-initialized").await.unwrap();
        tokio::fs::create_dir(workspace.path.join(".terraform")).await.unwrap();

        let result = runner.destroy(&workspace, true).await.unwrap();
        assert!(result.success);

        let calls: Vec<&'static str> =
            recorder.recorded_calls().iter().map(|c| c.command).collect();
        assert_eq!(calls, vec!["destroy"], "an already-initialized workspace must not re-init: got {calls:?}");
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
            failed_changes: Vec::new(),
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
            failed_changes: Vec::new(),
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
            failed_changes: Vec::new(),
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
            failed_changes: Vec::new(),
        };
        let addrs = extract_failed_addresses_from_tofu(&r);
        assert_eq!(addrs, vec!["r.a".to_string(), "r.b".to_string()]);
    }
}
