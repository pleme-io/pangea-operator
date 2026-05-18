//! `MagmaExecutor` — magma-backed alternative to `TofuExecutor`.
//!
//! Implements the operator's `IacExecutor` trait by calling magma
//! library APIs in-process. No fork+exec. No subprocess. No
//! Terraform-shell parsing. Per `theory/MAGMA-OPERATOR-BACKEND.md`.
//!
//! # Coexistence with TofuExecutor
//!
//! Both `MagmaExecutor` and `TofuExecutor` implement the same
//! `IacExecutor` trait. Controllers pick one at startup (via
//! `PANGEA_EXECUTOR=magma|tofu` env var) or per-CR (in M0.12).
//! Removing tofu is explicitly out of scope; both backends ship
//! side-by-side indefinitely.
//!
//! # State backend bridge
//!
//! The canonical adapter lives in the `magma-operator-backend`
//! crate (`OperatorBackend<S: AsyncStateStore>` + the tofu-shape
//! converter). This file ships a thin glue type that bridges
//! pangea-operator's existing `StateBackend` trait into
//! `AsyncStateStore` — about 60 lines of adapter code; no
//! conversion logic (magma-operator-backend owns that).
//!
//! # In-process pipeline
//!
//! ```text
//! pangea_types::Value (already in-pod from pangea-ruby-eval)
//!   ↓ to_terraform_json()
//! serde_json::Value (terraform-shaped)
//!   ↓ magma_config::Config::from_json
//! magma::config::Config
//!   ↓ magma_plan::plan(&cfg, &state)
//! magma::types::Plan
//!   ↓ checkpoint to disk (restart safety) + magma_apply::run_plan
//! magma::types::ApplyOutcome
//!   ↓ write_state via OperatorBackend → StateBackendAsync
//! PG row (operator's existing storage)
//! ```
//!
//! The disk checkpoint is restart-safety only — `tokio::spawn`d
//! reconciles can be SIGKILLed mid-apply; the next reconcile reads
//! the checkpoint, re-derives the plan_id via BLAKE3, and resumes.
//! When this binary owns the full reconcile lifecycle, M0.13+ may
//! collapse the checkpoint to `Arc<Plan>` in-heap.
//!
//! # M0.10 scope
//!
//! Skeleton `IacExecutor` impl. `init` is a no-op (magma needs no
//! provider download — providers are typed-imported). `plan` /
//! `apply` / `destroy` / `show_plan` / `output` are wired to magma
//! APIs and tested via `InMemoryStateBackend`. `import` returns
//! NotImplemented (lands in M0.11 alongside auto-import parity).

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use magma_operator_backend::{AsyncStateStore, BackendShape, OperatorBackend, StoreError};

use crate::backend::state_backend::StateBackend;
use crate::error::{Error, Result};
use crate::executor::iac_executor::IacExecutor;
use crate::executor::tofu::TofuResult;

// ── StateBackendAsync — adapter into magma-operator-backend ────────

/// Wraps the operator's `StateBackend` (which is keyed by the
/// `schema`/`template`/`state` triple) into a single-key
/// `AsyncStateStore` that magma-operator-backend's `OperatorBackend`
/// consumes. One of these per reconcile.
pub struct StateBackendAsync<S: StateBackend + ?Sized> {
    inner:         Arc<S>,
    schema_name:   String,
    template_name: String,
    state_name:    String,
}

impl<S: StateBackend + ?Sized> StateBackendAsync<S> {
    pub fn new(
        inner: Arc<S>,
        schema_name: impl Into<String>,
        template_name: impl Into<String>,
        state_name: impl Into<String>,
    ) -> Self {
        Self {
            inner,
            schema_name:   schema_name.into(),
            template_name: template_name.into(),
            state_name:    state_name.into(),
        }
    }
}

#[async_trait]
impl<S: StateBackend + ?Sized> AsyncStateStore for StateBackendAsync<S> {
    async fn load_state_bytes(&self) -> std::result::Result<Option<Vec<u8>>, StoreError> {
        let entry = self
            .inner
            .get_state(&self.schema_name, &self.template_name, &self.state_name)
            .await
            .map_err(|e| StoreError::Inner(e.to_string()))?;
        Ok(entry.and_then(|e| e.data))
    }

    async fn save_state_bytes(&self, bytes: &[u8]) -> std::result::Result<(), StoreError> {
        self.inner
            .save_state(&self.schema_name, &self.template_name, &self.state_name, bytes)
            .await
            .map_err(|e| StoreError::Inner(e.to_string()))?;
        Ok(())
    }
}

// ── MagmaExecutor — IacExecutor impl ──────────────────────────────

/// Configuration for the `MagmaExecutor`. Cloneable manually to
/// avoid forcing a `Clone` bound on the StateBackend impl
/// (`Arc<S>` is always cheap to clone regardless of `S`).
pub struct MagmaExecutorConfig<S: StateBackend + ?Sized> {
    /// State backend (operator-side; PG or in-memory).
    pub state_backend: Arc<S>,
    /// Schema name (PG schema / namespace key).
    pub schema_name:   String,
    /// Template name (CR name).
    pub template_name: String,
    /// State name (workspace state slot, typically "default").
    pub state_name:    String,
    /// On-disk encoding for the state bytes. `Magma` (default) is
    /// the typed magma format; `Tofu` is the OpenTofu-readable
    /// format (use this when the same state is read by both
    /// backends).
    pub backend_shape: BackendShape,
    /// Whether to write a typed plan checkpoint to the workspace
    /// dir between plan + apply for restart safety. Default true.
    pub plan_checkpoint: bool,
}

impl<S: StateBackend + ?Sized> Default for MagmaExecutorConfig<S>
where
    Arc<S>: Default,
{
    fn default() -> Self {
        Self {
            state_backend:   Default::default(),
            schema_name:     "default".into(),
            template_name:   "default".into(),
            state_name:      "default".into(),
            backend_shape:   BackendShape::Magma,
            plan_checkpoint: true,
        }
    }
}

impl<S: StateBackend + ?Sized> Clone for MagmaExecutorConfig<S> {
    fn clone(&self) -> Self {
        Self {
            state_backend:   Arc::clone(&self.state_backend),
            schema_name:     self.schema_name.clone(),
            template_name:   self.template_name.clone(),
            state_name:      self.state_name.clone(),
            backend_shape:   self.backend_shape,
            plan_checkpoint: self.plan_checkpoint,
        }
    }
}

/// Magma-backed `IacExecutor`. Drives `magma_plan::plan` +
/// `magma_apply::run_plan` in-process. No subprocess.
pub struct MagmaExecutor<S: StateBackend + ?Sized> {
    cfg: MagmaExecutorConfig<S>,
}

impl<S: StateBackend + ?Sized> MagmaExecutor<S> {
    pub fn new(cfg: MagmaExecutorConfig<S>) -> Self {
        Self { cfg }
    }

    /// Build the magma-side backend handle. One per call (cheap).
    fn make_backend(&self) -> OperatorBackend<StateBackendAsync<S>> {
        let store = Arc::new(StateBackendAsync::new(
            Arc::clone(&self.cfg.state_backend),
            self.cfg.schema_name.clone(),
            self.cfg.template_name.clone(),
            self.cfg.state_name.clone(),
        ));
        OperatorBackend::with_shape(store, self.cfg.backend_shape)
    }

    fn plan_checkpoint_path(work_dir: &Path) -> PathBuf {
        work_dir.join("magma-plan.json")
    }

    async fn load_config(work_dir: &Path) -> Result<magma_config::Config> {
        let path = work_dir.join("main.tf.json");
        let bytes = tokio::fs::read(&path).await.map_err(Error::Io)?;
        let value: serde_json::Value = serde_json::from_slice(&bytes)
            .map_err(|e| Error::MagmaExecution(format!("parse main.tf.json: {e}")))?;
        magma_config::Config::from_json(value)
            .map_err(|e| Error::MagmaExecution(format!("magma_config: {e}")))
    }
}

fn ok_tofu_result(stdout: String, started: Instant) -> TofuResult {
    TofuResult {
        exit_code: 0,
        stdout,
        stderr:    String::new(),
        success:   true,
        duration:  started.elapsed(),
    }
}

fn changes_tofu_result(stdout: String, started: Instant) -> TofuResult {
    // tofu's `-detailed-exitcode` returns 2 when changes are pending.
    // We honor the same contract so callers' `has_changes()` works.
    TofuResult {
        exit_code: 2,
        stdout,
        stderr:    String::new(),
        success:   true,
        duration:  started.elapsed(),
    }
}

fn err_tofu_result(stderr: String, started: Instant) -> TofuResult {
    TofuResult {
        exit_code: 1,
        stdout:    String::new(),
        stderr,
        success:   false,
        duration:  started.elapsed(),
    }
}

#[async_trait]
impl<S> IacExecutor for MagmaExecutor<S>
where
    S: StateBackend + ?Sized,
{
    async fn init(&self, _work_dir: &Path, _extra_args: &[&str]) -> Result<TofuResult> {
        let started = Instant::now();
        Ok(ok_tofu_result(
            "magma init: no-op (providers typed-imported)\n".into(),
            started,
        ))
    }

    async fn plan(
        &self,
        work_dir: &Path,
        plan_file: Option<&Path>,
        _extra_args: &[&str],
    ) -> Result<TofuResult> {
        let started = Instant::now();
        let cfg = Self::load_config(work_dir).await?;
        let backend = self.make_backend();
        let state = magma_backend::Backend::read_state(&backend)
            .await
            .map_err(|e| Error::MagmaExecution(format!("read state: {e}")))?;
        let plan = magma_plan::plan(&cfg, &state)
            .map_err(|e| Error::MagmaExecution(format!("magma_plan: {e}")))?;

        if self.cfg.plan_checkpoint {
            let checkpoint = plan_file
                .map(PathBuf::from)
                .unwrap_or_else(|| Self::plan_checkpoint_path(work_dir));
            let bytes = serde_json::to_vec_pretty(&plan)
                .map_err(|e| Error::MagmaExecution(format!("encode plan: {e}")))?;
            tokio::fs::write(&checkpoint, &bytes).await.map_err(Error::Io)?;
        }

        let stdout = serde_json::to_string_pretty(&serde_json::json!({
            "plan_id":          hex::encode(plan.id.0),
            "created_at":       plan.created_at,
            "resource_changes": plan.resource_changes.len(),
            "changes":          plan.resource_changes,
        }))
        .unwrap_or_default();

        let has_changes = plan
            .resource_changes
            .iter()
            .any(|c| !matches!(c.action, magma_types::Action::NoOp));
        Ok(if has_changes {
            changes_tofu_result(stdout, started)
        } else {
            ok_tofu_result(stdout, started)
        })
    }

    async fn apply(
        &self,
        work_dir: &Path,
        plan_file: Option<&Path>,
        _auto_approve: bool,
    ) -> Result<TofuResult> {
        let started = Instant::now();
        let checkpoint = plan_file
            .map(PathBuf::from)
            .unwrap_or_else(|| Self::plan_checkpoint_path(work_dir));
        let bytes = tokio::fs::read(&checkpoint).await.map_err(Error::Io)?;
        let plan: magma_types::Plan = serde_json::from_slice(&bytes)
            .map_err(|e| Error::MagmaExecution(format!("decode plan checkpoint: {e}")))?;

        let backend = self.make_backend();
        let mut state = magma_backend::Backend::read_state(&backend)
            .await
            .map_err(|e| Error::MagmaExecution(format!("read state: {e}")))?;
        let outcome = magma_apply::run_plan(&plan, &mut state)
            .map_err(|e| Error::MagmaExecution(format!("magma_apply: {e}")))?;
        magma_backend::Backend::write_state(&backend, &state)
            .await
            .map_err(|e| Error::MagmaExecution(format!("write state: {e}")))?;

        let stdout = serde_json::to_string_pretty(&serde_json::json!({
            "plan_id":     hex::encode(outcome.plan_id.0),
            "applied":     outcome.applied.len(),
            "failed":      outcome.failed.len(),
            "started_at":  outcome.started_at,
            "finished_at": outcome.finished_at,
        }))
        .unwrap_or_default();
        Ok(if outcome.failed.is_empty() {
            ok_tofu_result(stdout, started)
        } else {
            err_tofu_result(stdout, started)
        })
    }

    async fn destroy(&self, work_dir: &Path, _auto_approve: bool) -> Result<TofuResult> {
        let started = Instant::now();
        let backend = self.make_backend();
        let mut state = magma_backend::Backend::read_state(&backend)
            .await
            .map_err(|e| Error::MagmaExecution(format!("read state: {e}")))?;

        let resource_changes: Vec<magma_types::ResourceChange> = state
            .resources
            .iter()
            .map(|r| magma_types::ResourceChange {
                address: r.address.clone(),
                action:  magma_types::Action::Delete,
                before:  r.instances.first().map(|i| i.attributes.clone()),
                after:   None,
                reasons: vec![magma_types::ChangeReason::DeletedResource],
            })
            .collect();
        let plan = magma_types::Plan {
            id:               magma_types::PlanId([0u8; 32]),
            created_at:       chrono::Utc::now(),
            config_root:      work_dir.to_path_buf(),
            variables:        Default::default(),
            resource_changes,
            output_changes:   vec![],
        };
        let outcome = magma_apply::run_plan(&plan, &mut state)
            .map_err(|e| Error::MagmaExecution(format!("magma_apply destroy: {e}")))?;
        magma_backend::Backend::write_state(&backend, &state)
            .await
            .map_err(|e| Error::MagmaExecution(format!("write state: {e}")))?;

        let stdout = format!("magma destroy: removed {} resources\n", outcome.applied.len());
        Ok(if outcome.failed.is_empty() {
            ok_tofu_result(stdout, started)
        } else {
            err_tofu_result(stdout, started)
        })
    }

    async fn show_plan(&self, _work_dir: &Path, plan_file: &Path) -> Result<TofuResult> {
        let started = Instant::now();
        let bytes = tokio::fs::read(plan_file).await.map_err(Error::Io)?;
        let plan: magma_types::Plan = serde_json::from_slice(&bytes)
            .map_err(|e| Error::MagmaExecution(format!("decode plan: {e}")))?;
        let stdout = serde_json::to_string_pretty(&plan)
            .map_err(|e| Error::MagmaExecution(format!("encode plan: {e}")))?;
        Ok(ok_tofu_result(stdout, started))
    }

    async fn output(&self, _work_dir: &Path) -> Result<TofuResult> {
        let started = Instant::now();
        let backend = self.make_backend();
        let state = magma_backend::Backend::read_state(&backend)
            .await
            .map_err(|e| Error::MagmaExecution(format!("read state: {e}")))?;
        let outputs: serde_json::Map<String, serde_json::Value> = state
            .outputs
            .iter()
            .map(|(k, v)| {
                (
                    k.clone(),
                    serde_json::json!({ "value": v.value, "sensitive": v.sensitive }),
                )
            })
            .collect();
        let stdout = serde_json::to_string_pretty(&serde_json::Value::Object(outputs))
            .map_err(|e| Error::MagmaExecution(format!("encode outputs: {e}")))?;
        Ok(ok_tofu_result(stdout, started))
    }

    async fn refresh(&self, _work_dir: &Path) -> Result<TofuResult> {
        let started = Instant::now();
        Ok(ok_tofu_result(
            "magma refresh: no-op in M0.10\n".into(),
            started,
        ))
    }

    async fn import(
        &self,
        _work_dir: &Path,
        address: &str,
        id: &str,
    ) -> Result<TofuResult> {
        let started = Instant::now();
        let stderr = format!(
            "magma import: not yet implemented for {address} ({id}); falls back to recreate\n",
        );
        Ok(err_tofu_result(stderr, started))
    }
}

// ── Tests ──────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::state_backend::InMemoryStateBackend;
    use serde_json::json;

    fn fixture_config() -> MagmaExecutorConfig<InMemoryStateBackend> {
        MagmaExecutorConfig {
            state_backend:   Arc::new(InMemoryStateBackend::new()),
            schema_name:     "test_schema".into(),
            template_name:   "test_template".into(),
            state_name:      "default".into(),
            backend_shape:   BackendShape::Magma,
            plan_checkpoint: true,
        }
    }

    async fn render_workspace(dir: &Path, body: &serde_json::Value) {
        tokio::fs::create_dir_all(dir).await.unwrap();
        tokio::fs::write(
            dir.join("main.tf.json"),
            serde_json::to_string_pretty(body).unwrap(),
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn init_succeeds_without_doing_anything() {
        let exec = MagmaExecutor::new(fixture_config());
        let tmp = tempfile::tempdir().unwrap();
        let result = exec.init(tmp.path(), &[]).await.unwrap();
        assert!(result.success);
        assert_eq!(result.exit_code, 0);
        assert!(result.stdout.contains("no-op"));
    }

    #[tokio::test]
    async fn plan_then_apply_roundtrips_through_in_memory_backend() {
        let exec = MagmaExecutor::new(fixture_config());
        let tmp = tempfile::tempdir().unwrap();
        render_workspace(
            tmp.path(),
            &json!({
                "provider": { "aws": { "region": "us-east-1" } },
                "resource": { "aws_iam_role": { "node": { "name": "mexec" } } },
            }),
        )
        .await;

        // First plan: empty state → all resources are creations → exit 2.
        let plan_result = exec.plan(tmp.path(), None, &[]).await.unwrap();
        assert!(plan_result.success);
        assert_eq!(plan_result.exit_code, 2, "first plan should signal changes");
        assert!(plan_result.stdout.contains("plan_id"));
        assert!(tmp.path().join("magma-plan.json").exists());

        // Apply consumes the checkpoint.
        let apply_result = exec.apply(tmp.path(), None, true).await.unwrap();
        assert!(apply_result.success, "apply failed: {}", apply_result.stdout);
        assert_eq!(apply_result.exit_code, 0);

        // Second plan: state now contains the resource → no changes.
        let plan2 = exec.plan(tmp.path(), None, &[]).await.unwrap();
        assert!(plan2.success);
        assert_eq!(plan2.exit_code, 0, "second plan should be a no-op");
    }

    #[tokio::test]
    async fn show_plan_reads_typed_plan_from_checkpoint() {
        let exec = MagmaExecutor::new(fixture_config());
        let tmp = tempfile::tempdir().unwrap();
        render_workspace(
            tmp.path(),
            &json!({
                "provider": { "aws": { "region": "us-east-1" } },
                "resource": { "aws_iam_role": { "r": { "name": "x" } } },
            }),
        )
        .await;
        exec.plan(tmp.path(), None, &[]).await.unwrap();
        let checkpoint = tmp.path().join("magma-plan.json");
        let shown = exec.show_plan(tmp.path(), &checkpoint).await.unwrap();
        assert!(shown.success);
        // magma::types::Plan serializes its fields as `id` (PlanId)
        // and `resource_changes` (Vec). show_plan emits the typed
        // Plan as pretty JSON, so both field names must appear.
        assert!(shown.stdout.contains("\"id\""),
                "missing id field in shown plan:\n{}", shown.stdout);
        assert!(shown.stdout.contains("resource_changes"));
    }

    #[tokio::test]
    async fn destroy_against_empty_state_succeeds() {
        let exec = MagmaExecutor::new(fixture_config());
        let tmp = tempfile::tempdir().unwrap();
        let result = exec.destroy(tmp.path(), true).await.unwrap();
        assert!(result.success);
        assert!(result.stdout.contains("removed 0 resources"));
    }

    #[tokio::test]
    async fn destroy_after_apply_removes_resources() {
        let exec = MagmaExecutor::new(fixture_config());
        let tmp = tempfile::tempdir().unwrap();
        render_workspace(
            tmp.path(),
            &json!({
                "provider": { "aws": { "region": "us-east-1" } },
                "resource": { "aws_iam_role": { "r": { "name": "x" } } },
            }),
        )
        .await;
        exec.plan(tmp.path(), None, &[]).await.unwrap();
        exec.apply(tmp.path(), None, true).await.unwrap();

        let result = exec.destroy(tmp.path(), true).await.unwrap();
        assert!(result.success);
        assert!(result.stdout.contains("removed 1 resources"));
    }

    #[tokio::test]
    async fn import_returns_not_implemented_typed_failure() {
        let exec = MagmaExecutor::new(fixture_config());
        let tmp = tempfile::tempdir().unwrap();
        let result = exec.import(tmp.path(), "aws_iam_role.x", "arn:1").await.unwrap();
        assert!(!result.success);
        assert!(result.stderr.contains("not yet implemented"));
    }

    #[tokio::test]
    async fn output_emits_json_map() {
        let exec = MagmaExecutor::new(fixture_config());
        let tmp = tempfile::tempdir().unwrap();
        let result = exec.output(tmp.path()).await.unwrap();
        assert!(result.success);
        let parsed: serde_json::Value = serde_json::from_str(&result.stdout).unwrap();
        assert!(parsed.is_object());
    }

    #[tokio::test]
    async fn refresh_is_no_op_in_m010() {
        let exec = MagmaExecutor::new(fixture_config());
        let tmp = tempfile::tempdir().unwrap();
        let result = exec.refresh(tmp.path()).await.unwrap();
        assert!(result.success);
    }

    // ── StateBackendAsync (the operator-side adapter) ─────────────

    #[tokio::test]
    async fn state_backend_async_round_trips_bytes_through_store() {
        let store: Arc<InMemoryStateBackend> = Arc::new(InMemoryStateBackend::new());
        let async_store = StateBackendAsync::new(Arc::clone(&store), "s", "t", "default");
        async_store.save_state_bytes(b"magma-bytes").await.unwrap();
        let got = async_store.load_state_bytes().await.unwrap();
        assert_eq!(got.as_deref(), Some(&b"magma-bytes"[..]));
    }

    #[tokio::test]
    async fn restart_resume_apply_uses_checkpoint_from_separate_instance() {
        // Restart-safety claim: a MagmaExecutor process can be killed
        // between `plan` and `apply`; a fresh MagmaExecutor instance
        // pointed at the SAME state backend + workspace dir can
        // resume `apply` from the on-disk plan checkpoint. This test
        // simulates that by constructing TWO independent
        // MagmaExecutor instances over a SHARED InMemoryStateBackend.
        let store: Arc<InMemoryStateBackend> = Arc::new(InMemoryStateBackend::new());
        let make_executor = || MagmaExecutor::new(MagmaExecutorConfig {
            state_backend:   Arc::clone(&store),
            schema_name:     "s".into(),
            template_name:   "t".into(),
            state_name:      "default".into(),
            backend_shape:   BackendShape::Magma,
            plan_checkpoint: true,
        });

        let tmp = tempfile::tempdir().unwrap();
        render_workspace(
            tmp.path(),
            &json!({
                "provider": { "aws": { "region": "us-east-1" } },
                "resource": { "aws_iam_role": { "r": { "name": "restart-resume" } } },
            }),
        )
        .await;

        // Instance #1: plan, then "crash" (drop without applying).
        let exec1 = make_executor();
        let plan_result = exec1.plan(tmp.path(), None, &[]).await.unwrap();
        assert_eq!(plan_result.exit_code, 2, "plan should signal changes");
        drop(exec1);

        let checkpoint = tmp.path().join("magma-plan.json");
        assert!(checkpoint.exists(), "checkpoint must survive across instances");

        let state_before = store.get_state("s", "t", "default").await.unwrap();
        assert!(state_before.is_none(), "state must be untouched until apply");

        // Instance #2: apply from the existing checkpoint.
        let exec2 = make_executor();
        let apply_result = exec2.apply(tmp.path(), None, true).await.unwrap();
        assert!(apply_result.success, "instance 2 must apply from checkpoint");
        assert_eq!(apply_result.exit_code, 0);

        let state_after = store
            .get_state("s", "t", "default").await.unwrap()
            .expect("state must exist after apply");
        let parsed: serde_json::Value =
            serde_json::from_slice(&state_after.data.unwrap()).unwrap();
        assert!(parsed["resources"].as_array().unwrap().len() >= 1);
    }

    #[tokio::test]
    async fn separate_state_names_are_isolated() {
        // Two MagmaExecutor instances pointed at the SAME backend
        // but with different state_name slots must never see each
        // other's resources.
        let store: Arc<InMemoryStateBackend> = Arc::new(InMemoryStateBackend::new());
        let make = |state_name: &str| MagmaExecutor::new(MagmaExecutorConfig {
            state_backend:   Arc::clone(&store),
            schema_name:     "s".into(),
            template_name:   "t".into(),
            state_name:      state_name.into(),
            backend_shape:   BackendShape::Magma,
            plan_checkpoint: true,
        });

        let tmp_a = tempfile::tempdir().unwrap();
        let tmp_b = tempfile::tempdir().unwrap();
        render_workspace(
            tmp_a.path(),
            &json!({
                "provider": { "aws": { "region": "us-east-1" } },
                "resource": { "aws_iam_role": { "rA": { "name": "A" } } },
            }),
        ).await;
        render_workspace(
            tmp_b.path(),
            &json!({
                "provider": { "aws": { "region": "us-east-1" } },
                "resource": { "aws_iam_role": { "rB": { "name": "B" } } },
            }),
        ).await;

        let exec_a = make("slot_a");
        let exec_b = make("slot_b");
        exec_a.plan(tmp_a.path(), None, &[]).await.unwrap();
        exec_a.apply(tmp_a.path(), None, true).await.unwrap();
        exec_b.plan(tmp_b.path(), None, &[]).await.unwrap();
        exec_b.apply(tmp_b.path(), None, true).await.unwrap();

        let entry_a = store.get_state("s", "t", "slot_a").await.unwrap().unwrap();
        let entry_b = store.get_state("s", "t", "slot_b").await.unwrap().unwrap();
        let parsed_a: serde_json::Value =
            serde_json::from_slice(&entry_a.data.unwrap()).unwrap();
        let parsed_b: serde_json::Value =
            serde_json::from_slice(&entry_b.data.unwrap()).unwrap();
        let names_a: Vec<&str> = parsed_a["resources"]
            .as_array().unwrap().iter()
            .map(|r| r["address"]["name"].as_str().unwrap_or(""))
            .collect();
        let names_b: Vec<&str> = parsed_b["resources"]
            .as_array().unwrap().iter()
            .map(|r| r["address"]["name"].as_str().unwrap_or(""))
            .collect();
        assert_eq!(names_a, vec!["rA"]);
        assert_eq!(names_b, vec!["rB"]);
    }

    #[tokio::test]
    async fn plan_with_explicit_plan_file_path_writes_there() {
        let exec = MagmaExecutor::new(fixture_config());
        let tmp = tempfile::tempdir().unwrap();
        render_workspace(
            tmp.path(),
            &json!({
                "provider": { "aws": { "region": "us-east-1" } },
                "resource": { "aws_iam_role": { "r": { "name": "x" } } },
            }),
        )
        .await;

        let custom_plan = tmp.path().join("custom-plan.json");
        exec.plan(tmp.path(), Some(&custom_plan), &[]).await.unwrap();
        assert!(custom_plan.exists(), "plan written to explicit path");
        assert!(
            !tmp.path().join("magma-plan.json").exists(),
            "default checkpoint should not exist when explicit plan_file is set",
        );
        let apply_result = exec.apply(tmp.path(), Some(&custom_plan), true).await.unwrap();
        assert!(apply_result.success);
    }

    #[tokio::test]
    async fn plan_checkpoint_disabled_skips_disk_write() {
        let cfg = MagmaExecutorConfig {
            plan_checkpoint: false,
            ..fixture_config()
        };
        let exec = MagmaExecutor::new(cfg);
        let tmp = tempfile::tempdir().unwrap();
        render_workspace(
            tmp.path(),
            &json!({
                "provider": { "aws": { "region": "us-east-1" } },
                "resource": { "aws_iam_role": { "r": { "name": "x" } } },
            }),
        )
        .await;
        exec.plan(tmp.path(), None, &[]).await.unwrap();
        assert!(!tmp.path().join("magma-plan.json").exists());
    }

    #[tokio::test]
    async fn tofu_shape_persistence_writes_canonical_provider_form() {
        let cfg = MagmaExecutorConfig {
            state_backend:   Arc::new(InMemoryStateBackend::new()),
            schema_name:     "s".into(),
            template_name:   "t".into(),
            state_name:      "default".into(),
            backend_shape:   BackendShape::Tofu,
            plan_checkpoint: true,
        };
        let exec = MagmaExecutor::new(cfg.clone());
        let tmp = tempfile::tempdir().unwrap();
        render_workspace(
            tmp.path(),
            &json!({
                "provider": { "aws": { "region": "us-east-1" } },
                "resource": { "aws_iam_role": { "r": { "name": "x" } } },
            }),
        )
        .await;
        exec.plan(tmp.path(), None, &[]).await.unwrap();
        exec.apply(tmp.path(), None, true).await.unwrap();

        // The state row should be a parseable tofu-shaped JSON with
        // the canonical provider form (i.e. cross-executor readable).
        let entry = cfg
            .state_backend
            .get_state("s", "t", "default")
            .await
            .unwrap()
            .expect("state row should exist");
        let bytes = entry.data.unwrap();
        let parsed: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(parsed["version"], 4);
        let provider = parsed["resources"][0]["provider"].as_str().unwrap();
        assert!(
            provider.contains("registry.terraform.io/hashicorp/aws"),
            "expected canonical tofu provider form, got: {provider}",
        );
    }
}
