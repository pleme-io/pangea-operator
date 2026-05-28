//! Typed `IacExecutor` trait — abstraction over `tofu` CLI calls.
//!
//! Reasons this exists:
//!
//!   1. **Testability.** The reconciler can no longer be unit-tested
//!      against `tofu` because `tofu` requires a real cloud account
//!      and a populated workspace. With this trait, tests inject a
//!      `RecordingExecutor` that returns canned responses + asserts on
//!      what the controller called.
//!
//!   2. **Fault injection.** Property tests around apply atomicity,
//!      auto-import, and settling-policy escalation all need to
//!      simulate failures (network drops, OOM, exit-code-1, garbled
//!      JSON). A trait makes that injectable.
//!
//!   3. **Future swap.** Today's executor calls `tofu` (`opentofu`).
//!      A future variant could call `terraform`, or speak directly
//!      to provider APIs without tofu. The trait is the seam.
//!
//! The real implementation (`TofuExecutor`) lives in `executor/tofu.rs`
//! and matches this trait surface 1:1. The unit-test mock
//! (`RecordingExecutor`) lives in `executor/recording.rs`.

use crate::error::Result;
use crate::executor::tofu::TofuResult;

use async_trait::async_trait;
use std::path::Path;

/// Async trait covering every `tofu` subcommand the operator invokes.
///
/// Object-safe by design — `Arc<dyn IacExecutor>` is what
/// `ControllerState` holds. New methods MUST be added here whenever
/// a new tofu subcommand becomes load-bearing for any controller.
///
/// Idempotency contract: implementations are expected to be safe
/// against duplicate invocations within a reconcile cycle. Operators
/// retry on transient failures; double-apply, double-import, and
/// double-init must not corrupt state.
#[async_trait]
pub trait IacExecutor: Send + Sync + 'static {
    /// Stable identifier for THIS executor — `"tofu"` for the
    /// subprocess executor, `"magma"` for the typed-native executor,
    /// `"recording"` for the test mock. Surfaced into CR status as
    /// `status.executor` so observers can answer "what's actually
    /// running here?" via a single `kubectl get` instead of grepping
    /// the operator's startup logs. Stable enough to use as a metric
    /// label.
    fn name(&self) -> &'static str;

    /// Short human-readable identifier for the state backend this
    /// executor reads/writes. `"pg/<db_name>"` for Postgres-backed
    /// state (the magma path's canonical case via the shared
    /// `StateBackend` trait), `"local"` for filesystem-backed,
    /// `None` for stateless executors (the test mock). Surfaced into
    /// `status.backend`.
    fn backend_descriptor(&self) -> Option<String>;

    /// `tofu init` — initialize backend + provider plugins.
    async fn init(&self, work_dir: &Path, extra_args: &[&str]) -> Result<TofuResult>;

    /// `tofu plan` — compute the diff between desired and current state.
    /// `plan_file`, when Some, instructs tofu to write the binary plan
    /// to that path so a subsequent `apply` can consume it
    /// deterministically.
    async fn plan(
        &self,
        work_dir: &Path,
        plan_file: Option<&Path>,
        extra_args: &[&str],
    ) -> Result<TofuResult>;

    /// `tofu apply` — converge cloud state to plan. Pass `auto_approve`
    /// for non-interactive operation.
    async fn apply(
        &self,
        work_dir: &Path,
        plan_file: Option<&Path>,
        auto_approve: bool,
    ) -> Result<TofuResult>;

    /// `tofu destroy` — tear down all resources in the workspace.
    async fn destroy(&self, work_dir: &Path, auto_approve: bool) -> Result<TofuResult>;

    /// `tofu show -json <plan_file>` — emit the parsed plan as JSON.
    /// Critical input to drift-detail population + auto-import.
    async fn show_plan(&self, work_dir: &Path, plan_file: &Path) -> Result<TofuResult>;

    /// `tofu output -json` — emit workspace outputs as JSON.
    async fn output(&self, work_dir: &Path) -> Result<TofuResult>;

    /// `tofu refresh` — read cloud state into local state without
    /// applying any changes.
    async fn refresh(&self, work_dir: &Path) -> Result<TofuResult>;

    /// `tofu import <address> <id>` — adopt an out-of-band cloud
    /// resource into state without recreating it. The auto-import
    /// path drives this for every create-action whose natural-id
    /// can be substituted from plan attributes.
    async fn import(
        &self,
        work_dir: &Path,
        address: &str,
        id: &str,
    ) -> Result<TofuResult>;
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Object-safety check — `Arc<dyn IacExecutor>` must compile.
    /// If this fails, someone added a method that broke object-safety
    /// (e.g., generic params, `Self` return type).
    #[test]
    fn iac_executor_is_object_safe() {
        fn _accepts_dyn(_e: std::sync::Arc<dyn IacExecutor>) {}
        // The real test is that this compiles. No runtime assertion
        // needed — Rust enforces the property.
    }
}
