//! Recording `IacExecutor` for unit + property tests.
//!
//! Records every call's command, work_dir, args. Returns canned
//! `TofuResult` values from a pre-loaded queue, OR a default
//! "success, no diff" response if the queue is empty.
//!
//! This is the test-time counterpart to `TofuExecutor`. The reconciler
//! takes `Arc<dyn IacExecutor>`, so tests substitute a `RecordingExecutor`
//! and assert on `recorded_calls()` after exercising the controller.
//!
//! Not used at runtime — `cfg(test)`-gated nothing here; the type is
//! always available so integration tests outside the lib (in
//! `tests/`) can use it too.

use crate::error::Result;
use crate::executor::iac_executor::IacExecutor;
use crate::executor::tofu::TofuResult;

use async_trait::async_trait;
use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::Duration;

/// One recorded call to the executor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordedCall {
    /// Subcommand name: "init", "plan", "apply", "destroy", "show",
    /// "output", "refresh", "import".
    pub command: &'static str,

    /// Working directory passed to the call.
    pub work_dir: PathBuf,

    /// Stringified args (joined with space) for easy assertion. The
    /// raw `Vec<String>` is also available via `args_vec`.
    pub args: String,

    /// Raw arg vector — useful when an assertion needs structure.
    pub args_vec: Vec<String>,
}

/// Recording mock implementation of `IacExecutor`.
pub struct RecordingExecutor {
    calls: Mutex<Vec<RecordedCall>>,
    canned: Mutex<VecDeque<TofuResult>>,
}

impl RecordingExecutor {
    /// Construct an executor that returns `default_success()` for
    /// every call until `push_response` is used to queue specific
    /// canned outputs.
    pub fn new() -> Self {
        Self {
            calls: Mutex::new(Vec::new()),
            canned: Mutex::new(VecDeque::new()),
        }
    }

    /// Queue a canned response. Responses are returned FIFO. When the
    /// queue empties, subsequent calls receive `default_success()`.
    pub fn push_response(&self, result: TofuResult) {
        self.canned.lock().unwrap().push_back(result);
    }

    /// Snapshot of every recorded call in chronological order.
    pub fn recorded_calls(&self) -> Vec<RecordedCall> {
        self.calls.lock().unwrap().clone()
    }

    /// Number of calls recorded so far.
    pub fn call_count(&self) -> usize {
        self.calls.lock().unwrap().len()
    }

    /// Default response when the canned queue is empty: exit_code=0,
    /// success=true, empty stdout/stderr.
    fn default_success() -> TofuResult {
        TofuResult {
            exit_code: 0,
            stdout: String::new(),
            stderr: String::new(),
            success: true,
            duration: Duration::from_millis(0),
            failed_changes: Vec::new(),
        }
    }

    fn record_and_respond(
        &self,
        command: &'static str,
        work_dir: &Path,
        args: Vec<String>,
    ) -> TofuResult {
        let args_str = args.join(" ");
        self.calls.lock().unwrap().push(RecordedCall {
            command,
            work_dir: work_dir.to_path_buf(),
            args: args_str,
            args_vec: args,
        });
        self.canned
            .lock()
            .unwrap()
            .pop_front()
            .unwrap_or_else(Self::default_success)
    }
}

impl Default for RecordingExecutor {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl IacExecutor for RecordingExecutor {
    fn name(&self) -> &'static str {
        "recording"
    }

    /// Test mock has no state backend — it never persists anything.
    fn backend_descriptor(&self) -> Option<String> {
        None
    }

    async fn init(&self, work_dir: &Path, extra_args: &[&str]) -> Result<TofuResult> {
        Ok(self.record_and_respond(
            "init",
            work_dir,
            extra_args.iter().map(|s| s.to_string()).collect(),
        ))
    }

    async fn plan(
        &self,
        work_dir: &Path,
        plan_file: Option<&Path>,
        extra_args: &[&str],
    ) -> Result<TofuResult> {
        let mut args: Vec<String> = extra_args.iter().map(|s| s.to_string()).collect();
        if let Some(pf) = plan_file {
            args.push(format!("-out={}", pf.display()));
        }
        Ok(self.record_and_respond("plan", work_dir, args))
    }

    async fn apply(
        &self,
        work_dir: &Path,
        plan_file: Option<&Path>,
        auto_approve: bool,
    ) -> Result<TofuResult> {
        let mut args: Vec<String> = Vec::new();
        if auto_approve {
            args.push("-auto-approve".into());
        }
        if let Some(pf) = plan_file {
            args.push(pf.display().to_string());
        }
        Ok(self.record_and_respond("apply", work_dir, args))
    }

    async fn destroy(&self, work_dir: &Path, auto_approve: bool) -> Result<TofuResult> {
        let mut args: Vec<String> = Vec::new();
        if auto_approve {
            args.push("-auto-approve".into());
        }
        Ok(self.record_and_respond("destroy", work_dir, args))
    }

    async fn show_plan(&self, work_dir: &Path, plan_file: &Path) -> Result<TofuResult> {
        Ok(self.record_and_respond(
            "show",
            work_dir,
            vec!["-json".into(), plan_file.display().to_string()],
        ))
    }

    async fn output(&self, work_dir: &Path) -> Result<TofuResult> {
        Ok(self.record_and_respond("output", work_dir, vec!["-json".into()]))
    }

    async fn refresh(&self, work_dir: &Path) -> Result<TofuResult> {
        Ok(self.record_and_respond("refresh", work_dir, Vec::new()))
    }

    async fn import(
        &self,
        work_dir: &Path,
        address: &str,
        id: &str,
    ) -> Result<TofuResult> {
        Ok(self.record_and_respond(
            "import",
            work_dir,
            vec![address.to_string(), id.to_string()],
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn records_calls_in_order() {
        let exec = RecordingExecutor::new();
        let dir = Path::new("/tmp/wsa");

        exec.init(dir, &["-input=false"]).await.unwrap();
        exec.plan(dir, Some(Path::new("/tmp/plan")), &[]).await.unwrap();
        exec.apply(dir, Some(Path::new("/tmp/plan")), true).await.unwrap();

        let calls = exec.recorded_calls();
        assert_eq!(calls.len(), 3);
        assert_eq!(calls[0].command, "init");
        assert_eq!(calls[1].command, "plan");
        assert_eq!(calls[2].command, "apply");
        assert!(calls[0].args.contains("-input=false"));
        assert!(calls[2].args.contains("-auto-approve"));
        assert!(calls[2].args.contains("/tmp/plan"));
    }

    #[tokio::test]
    async fn returns_canned_responses_fifo() {
        let exec = RecordingExecutor::new();
        exec.push_response(TofuResult {
            exit_code: 2,
            stdout: "Plan: 3 to add, 1 to change, 0 to destroy".into(),
            stderr: String::new(),
            success: true,
            duration: Duration::from_millis(100),
            failed_changes: Vec::new(),
        });
        exec.push_response(TofuResult {
            exit_code: 0,
            stdout: "Apply complete!".into(),
            stderr: String::new(),
            success: true,
            duration: Duration::from_millis(500),
            failed_changes: Vec::new(),
        });

        let plan = exec.plan(Path::new("/w"), None, &[]).await.unwrap();
        assert_eq!(plan.exit_code, 2);
        assert!(plan.has_changes());

        let apply = exec.apply(Path::new("/w"), None, true).await.unwrap();
        assert_eq!(apply.exit_code, 0);

        // Queue empty — subsequent calls get default_success.
        let init = exec.init(Path::new("/w"), &[]).await.unwrap();
        assert_eq!(init.exit_code, 0);
        assert!(init.stdout.is_empty());
    }

    #[tokio::test]
    async fn records_import_address_and_id() {
        let exec = RecordingExecutor::new();
        exec.import(Path::new("/w"), "github_repository.foo", "foo")
            .await
            .unwrap();

        let calls = exec.recorded_calls();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].command, "import");
        assert_eq!(calls[0].args_vec, vec!["github_repository.foo", "foo"]);
    }

    #[tokio::test]
    async fn dyn_dispatch_works() {
        // Confirm the trait object pattern callers will use compiles
        // and runs the right impl.
        let exec: std::sync::Arc<dyn IacExecutor> =
            std::sync::Arc::new(RecordingExecutor::new());
        exec.refresh(Path::new("/w")).await.unwrap();
    }
}
