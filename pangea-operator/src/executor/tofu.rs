//! OpenTofu command execution.

use crate::error::{Error, Result};
use crate::executor::iac_executor::IacExecutor;
use async_trait::async_trait;
use std::collections::HashMap;
use std::path::Path;
use std::process::Stdio;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Command;
use tracing::{debug, error, info, warn};

/// OpenTofu command executor.
pub struct TofuExecutor {
    /// Path to the tofu binary.
    binary: std::path::PathBuf,

    /// Command timeout.
    timeout: Duration,

    /// Whether to enable verbose output.
    verbose: bool,
}

/// OpenTofu command types.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TofuCommand {
    Init,
    Plan,
    Apply,
    Destroy,
    Show,
    Output,
    Refresh,
    Import,
}

impl TofuCommand {
    fn as_str(&self) -> &'static str {
        match self {
            TofuCommand::Init => "init",
            TofuCommand::Plan => "plan",
            TofuCommand::Apply => "apply",
            TofuCommand::Destroy => "destroy",
            TofuCommand::Show => "show",
            TofuCommand::Output => "output",
            TofuCommand::Refresh => "refresh",
            TofuCommand::Import => "import",
        }
    }
}

/// Result of a tofu command execution.
#[derive(Debug, Clone)]
pub struct TofuResult {
    /// Exit code.
    pub exit_code: i32,

    /// Standard output.
    pub stdout: String,

    /// Standard error.
    pub stderr: String,

    /// Whether the command succeeded.
    pub success: bool,

    /// Execution duration.
    pub duration: Duration,
}

impl TofuResult {
    /// Check if the plan has changes.
    pub fn has_changes(&self) -> bool {
        // OpenTofu returns exit code 2 when there are changes
        self.exit_code == 2 || self.stdout.contains("Plan:")
    }
}

/// Common args every state-mutating tofu invocation shares: `-input=false`
/// (no interactive prompts in a controller) + `-no-color` (no ANSI in logs).
/// Centralized per the prime directive — three+ copies of this literal
/// pair previously lived inline in init/plan/apply/destroy/refresh/import.
const BASE_ARGS: &[&str] = &["-input=false", "-no-color"];

/// Bump tofu's default parallelism (10) for plan + apply + destroy.
///
/// On templates whose resource graph fans out to hundreds of provider API
/// calls (e.g. pleme-io-opensource with ~600 GitHub resources, or large
/// AWS workspaces), the bottleneck is provider concurrency — not CPU.
/// 20 roughly halves wall-time on those templates while staying well
/// below typical provider rate-limit thresholds (GitHub: 5000/hr authed;
/// AWS: per-service). Empirically validated against pleme-io-opensource
/// 2026-05-18.
///
/// Future direction: thread `spec.parallelism` through the
/// InfrastructureTemplate CRD so per-template values can override this
/// default when the upstream provider has tighter or looser limits.
const PARALLELISM_FLAG: &str = "-parallelism=20";

/// Collapse repeated identical warnings during long-running ops. Trims
/// log volume by 30-50 % on templates that produce the same deprecation
/// or attribute-default warning per-resource.
const COMPACT_WARNINGS_FLAG: &str = "-compact-warnings";

impl TofuExecutor {
    /// Create a new executor.
    pub fn new(binary: std::path::PathBuf, timeout: Duration, verbose: bool) -> Self {
        Self {
            binary,
            timeout,
            verbose,
        }
    }

    /// Run `tofu init`.
    pub async fn init(&self, work_dir: &Path, extra_args: &[&str]) -> Result<TofuResult> {
        let mut args: Vec<&str> = BASE_ARGS.to_vec();
        args.extend(extra_args);
        self.execute(TofuCommand::Init, work_dir, &args, &HashMap::new()).await
    }

    /// Run `tofu plan`.
    pub async fn plan(
        &self,
        work_dir: &Path,
        plan_file: Option<&Path>,
        extra_args: &[&str],
    ) -> Result<TofuResult> {
        let mut args: Vec<&str> = BASE_ARGS.to_vec();
        args.push("-detailed-exitcode");
        args.push(PARALLELISM_FLAG);
        args.push(COMPACT_WARNINGS_FLAG);

        if let Some(pf) = plan_file {
            args.push("-out");
            args.push(pf.to_str().unwrap());
        }

        args.extend(extra_args);
        self.execute(TofuCommand::Plan, work_dir, &args, &HashMap::new()).await
    }

    /// Run `tofu apply`.
    ///
    /// When applying a saved plan, we pass `-refresh=false` to skip any
    /// refresh-during-apply. The saved plan already embeds the refreshed state
    /// snapshot at plan time; a fresh refresh during apply can race against
    /// in-flight external changes (e.g. the busy GitHub API across ~600 tracked
    /// resources on the pleme-io-opensource template) and bump the pg backend's
    /// state serial mid-apply, causing OpenTofu to reject the plan as stale:
    ///
    ///   Error: Saved plan is stale — The given plan file can no longer be
    ///   applied because the state was changed by another operation after the
    ///   plan was created.
    ///
    /// Empirically observed 2026-05-18 on rio: pleme-io-opensource exhausted
    /// `maxRetries=3` with the same race on every cycle. `-refresh=false` is
    /// the canonical OpenTofu fix for this class of bug.
    ///
    /// Parallelism + compact-warnings inherit the same tuning as `plan` —
    /// see the file-level constants above.
    pub async fn apply(
        &self,
        work_dir: &Path,
        plan_file: Option<&Path>,
        auto_approve: bool,
    ) -> Result<TofuResult> {
        let mut args: Vec<&str> = BASE_ARGS.to_vec();
        args.push(PARALLELISM_FLAG);
        args.push(COMPACT_WARNINGS_FLAG);

        if auto_approve {
            args.push("-auto-approve");
        }

        if let Some(pf) = plan_file {
            args.push("-refresh=false");
            args.push(pf.to_str().unwrap());
        }

        self.execute(TofuCommand::Apply, work_dir, &args, &HashMap::new()).await
    }

    /// Run `tofu destroy`.
    pub async fn destroy(&self, work_dir: &Path, auto_approve: bool) -> Result<TofuResult> {
        let mut args: Vec<&str> = BASE_ARGS.to_vec();
        args.push(PARALLELISM_FLAG);

        if auto_approve {
            args.push("-auto-approve");
        }

        self.execute(TofuCommand::Destroy, work_dir, &args, &HashMap::new()).await
    }

    /// Run `tofu show` to parse plan output.
    pub async fn show_plan(&self, work_dir: &Path, plan_file: &Path) -> Result<TofuResult> {
        let args = vec!["-json", plan_file.to_str().unwrap()];
        self.execute(TofuCommand::Show, work_dir, &args, &HashMap::new()).await
    }

    /// Run `tofu output` to get outputs.
    pub async fn output(&self, work_dir: &Path) -> Result<TofuResult> {
        let args = vec!["-json"];
        self.execute(TofuCommand::Output, work_dir, &args, &HashMap::new()).await
    }

    /// Run `tofu refresh`.
    pub async fn refresh(&self, work_dir: &Path) -> Result<TofuResult> {
        let args = BASE_ARGS.to_vec();
        self.execute(TofuCommand::Refresh, work_dir, &args, &HashMap::new()).await
    }

    /// Run `tofu import <address> <id>` — adopt an out-of-band cloud
    /// resource into tofu state without recreating it. The next plan
    /// will see the resource as already-managed; if attribute drift
    /// exists the next apply settles it via update.
    ///
    /// Idempotent at the operator level: callers should check
    /// drift-details for the address before importing — re-importing
    /// an already-imported address fails with "resource already
    /// managed", which is harmless but noisy.
    pub async fn import(
        &self,
        work_dir: &Path,
        address: &str,
        id: &str,
    ) -> Result<TofuResult> {
        let mut args: Vec<&str> = BASE_ARGS.to_vec();
        args.push(address);
        args.push(id);
        self.execute(TofuCommand::Import, work_dir, &args, &HashMap::new())
            .await
    }

    /// Execute a tofu command.
    async fn execute(
        &self,
        command: TofuCommand,
        work_dir: &Path,
        args: &[&str],
        env: &HashMap<String, String>,
    ) -> Result<TofuResult> {
        let start = std::time::Instant::now();
        let cmd_str = command.as_str();

        info!(
            command = cmd_str,
            work_dir = ?work_dir,
            args = ?args,
            "Executing tofu command"
        );

        let mut cmd = Command::new(&self.binary);
        cmd.arg(cmd_str)
            .args(args)
            .current_dir(work_dir)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .env("TF_IN_AUTOMATION", "1")
            .env("TF_INPUT", "0");

        // Add custom environment variables
        for (key, value) in env {
            cmd.env(key, value);
        }

        let mut child = cmd.spawn().map_err(|e| {
            Error::TofuExecution(format!("Failed to spawn tofu: {}", e))
        })?;

        let stdout_handle = child.stdout.take().unwrap();
        let stderr_handle = child.stderr.take().unwrap();

        // Read output streams
        let stdout_task = tokio::spawn(async move {
            let mut lines = Vec::new();
            let mut reader = BufReader::new(stdout_handle).lines();
            while let Ok(Some(line)) = reader.next_line().await {
                lines.push(line);
            }
            lines.join("\n")
        });

        let stderr_task = tokio::spawn(async move {
            let mut lines = Vec::new();
            let mut reader = BufReader::new(stderr_handle).lines();
            while let Ok(Some(line)) = reader.next_line().await {
                lines.push(line);
            }
            lines.join("\n")
        });

        // Wait with timeout
        let wait_result = tokio::time::timeout(self.timeout, child.wait()).await;

        let status = match wait_result {
            Ok(Ok(status)) => status,
            Ok(Err(e)) => {
                return Err(Error::TofuExecution(format!("Failed to wait for tofu: {}", e)));
            }
            Err(_) => {
                // Timeout - kill the process
                error!(command = cmd_str, "Tofu command timed out");
                let _ = child.kill().await;
                return Err(Error::Timeout(self.timeout.as_secs()));
            }
        };

        let stdout = stdout_task.await.unwrap_or_default();
        let stderr = stderr_task.await.unwrap_or_default();

        let exit_code = status.code().unwrap_or(-1);
        let success = status.success() || (command == TofuCommand::Plan && exit_code == 2);
        let duration = start.elapsed();

        if self.verbose {
            debug!(stdout = %stdout, "Tofu stdout");
            if !stderr.is_empty() {
                debug!(stderr = %stderr, "Tofu stderr");
            }
        }

        if success {
            info!(
                command = cmd_str,
                exit_code,
                duration_secs = duration.as_secs_f64(),
                "Tofu command completed"
            );
        } else {
            warn!(
                command = cmd_str,
                exit_code,
                stderr = %stderr,
                "Tofu command failed"
            );
        }

        Ok(TofuResult {
            exit_code,
            stdout,
            stderr,
            success,
            duration,
        })
    }
}

/// `IacExecutor` implementation that delegates to the inherent
/// `TofuExecutor` methods. The trait is the public interface used by
/// `ControllerState`; the inherent methods are kept for backward
/// compatibility with code that holds a concrete `TofuExecutor` (e.g.
/// helper functions that take `&TofuExecutor` directly).
///
/// New tofu subcommands should be added to BOTH the trait (for
/// testability) and the inherent impl (for direct callers) — or
/// alternatively, add only to the trait once all callers migrate to
/// `&dyn IacExecutor`.
#[async_trait]
impl IacExecutor for TofuExecutor {
    async fn init(&self, work_dir: &Path, extra_args: &[&str]) -> Result<TofuResult> {
        TofuExecutor::init(self, work_dir, extra_args).await
    }

    async fn plan(
        &self,
        work_dir: &Path,
        plan_file: Option<&Path>,
        extra_args: &[&str],
    ) -> Result<TofuResult> {
        TofuExecutor::plan(self, work_dir, plan_file, extra_args).await
    }

    async fn apply(
        &self,
        work_dir: &Path,
        plan_file: Option<&Path>,
        auto_approve: bool,
    ) -> Result<TofuResult> {
        TofuExecutor::apply(self, work_dir, plan_file, auto_approve).await
    }

    async fn destroy(&self, work_dir: &Path, auto_approve: bool) -> Result<TofuResult> {
        TofuExecutor::destroy(self, work_dir, auto_approve).await
    }

    async fn show_plan(&self, work_dir: &Path, plan_file: &Path) -> Result<TofuResult> {
        TofuExecutor::show_plan(self, work_dir, plan_file).await
    }

    async fn output(&self, work_dir: &Path) -> Result<TofuResult> {
        TofuExecutor::output(self, work_dir).await
    }

    async fn refresh(&self, work_dir: &Path) -> Result<TofuResult> {
        TofuExecutor::refresh(self, work_dir).await
    }

    async fn import(
        &self,
        work_dir: &Path,
        address: &str,
        id: &str,
    ) -> Result<TofuResult> {
        TofuExecutor::import(self, work_dir, address, id).await
    }
}
