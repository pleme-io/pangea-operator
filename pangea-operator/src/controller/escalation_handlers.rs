//! `EscalationActionHandler` trait + per-variant implementations.
//!
//! The escalation ladder (`escalation.rs`) decides WHICH action to
//! take. This module decides HOW to take it. Each `EscalationAction`
//! variant has one handler; the dispatcher matches the action to the
//! handler at the call site.
//!
//! ## Why a trait, not a `match`
//!
//! `EscalationAction` has 5 variants; each variant's execution
//! semantics are independent (Retry is a no-op; PauseAndAlert flips
//! a status field; RefreshSource invalidates a cache; etc.). With a
//! `match` arm, every new failure surface that consumes the ladder
//! has to re-implement the variant dispatch. With a trait, the
//! dispatch is `state.handler_for(action).execute(&ctx).await?` —
//! one line, identical at every consumer.
//!
//! The trait also lets each handler's tests stay isolated: the
//! `PauseAndAlertHandler` test asserts the status patch shape
//! without spinning up a fake `ControllerState`; the dispatch lookup
//! is tested separately. Composition by trait is a tighter test
//! seam than a `match` arm.
//!
//! ## Why each handler returns a patch, not applies it
//!
//! The handlers are pure-ish: they compute the desired status delta
//! + event payload + return. The caller does the actual
//! `patch_status` + `record_event` apply. This keeps handlers
//! testable (no kube-rs mocking) and lets the caller combine the
//! handler's patch with its own (consecutive count, lastError) into
//! one server-side merge.
//!
//! ## The pattern (controller-detection-axis fix-step composition)
//!
//! ```text
//! handle_compile_failure
//!   → detection axis    (typed ConflictDetector emits Conflicts)
//!   → recurrence axis   (anomaly_tracker counts signature)
//!   → escalation axis   (EscalationLadder picks action)
//!   → DISPATCH          (this module: action → handler.execute)
//!   → apply             (caller: patch_status + record_event)
//! ```
//!
//! `project_controller_detection_axis.md` documents the full axis;
//! `project_escalation_ladder.md` documents the action picker;
//! `project_anomaly_recurrence.md` documents the signature surface.

use super::escalation::EscalationAction;
use crate::crd::InfrastructureTemplate;
use async_trait::async_trait;
use std::sync::Arc;
use std::time::Duration;

/// Inputs to one handler's `execute`. Owned by the caller; passed by
/// shared reference so handlers don't take ownership.
///
/// Adding a field here is a contract-extending change: existing
/// handlers continue to compile (they ignore unknown fields), new
/// handlers can rely on it. Removing or renaming a field requires
/// touching every handler — that's the cost of the contract.
#[derive(Debug)]
pub struct EscalationContext<'a> {
    /// The CR currently being handled.
    pub template: &'a InfrastructureTemplate,
    /// The action the ladder recommended (matches the dispatched
    /// handler). Carrying it explicitly lets the handler emit its
    /// own label + depth into the event message without a re-pick.
    pub action: EscalationAction,
    /// How long the template has been non-Ready (from the ladder's
    /// duration_unready input).
    pub duration_unready: Duration,
    /// Current `consecutiveCompileFailures` count after this failure.
    pub consecutive_failures: u32,
    /// The raw error string from the underlying failure.
    pub last_error: String,
    /// `error_signature` of `last_error` — stable join key for
    /// dashboards.
    pub error_signature: String,
}

/// What a handler produces. The caller merges into its own status
/// patch + emits the Event.
#[derive(Debug, Clone, PartialEq)]
pub struct EscalationOutcome {
    /// Status fields to merge. Use `serde_json::json!({})` for "no
    /// status change" (the dispatcher will skip the patch call).
    pub status_patch: serde_json::Value,
    /// Event `reason` string. Stable for metric/event-routing.
    pub event_reason: &'static str,
    /// Event `message` body. Free-form; humans read this.
    pub event_message: String,
}

/// One escalation handler. Each implementation knows how to ENACT
/// one variant of `EscalationAction`.
///
/// Implementations MUST be idempotent: running `execute` twice with
/// the same context produces the same outcome (or at worst a no-op
/// on the second call). The reconciliation loop may re-fire the same
/// handler across cycles; idempotence keeps that safe.
#[async_trait]
pub trait EscalationActionHandler: Send + Sync {
    /// Which action this handler implements. The dispatcher uses this
    /// to route. Implementations return a stable variant — same
    /// pattern as `EscalationAction::label()`.
    fn action(&self) -> EscalationAction;

    /// Apply the corrective action. Pure-ish: computes the status
    /// patch + event payload. The caller does the apply.
    async fn execute(&self, ctx: &EscalationContext<'_>) -> anyhow::Result<EscalationOutcome>;
}

// ── Handler implementations ────────────────────────────────────────

/// No-op handler — the shallowest rung. Returns an empty status
/// patch + a benign event. The reconcile loop's normal retry
/// semantics resume.
pub struct RetryHandler;

#[async_trait]
impl EscalationActionHandler for RetryHandler {
    fn action(&self) -> EscalationAction {
        EscalationAction::Retry
    }

    async fn execute(&self, ctx: &EscalationContext<'_>) -> anyhow::Result<EscalationOutcome> {
        Ok(EscalationOutcome {
            status_patch: serde_json::json!({}),
            event_reason: "EscalationLadderRetry",
            event_message: format!(
                "Recovery ladder: Retry (depth 0, after {}s unready). Normal retry semantics apply.",
                ctx.duration_unready.as_secs(),
            ),
        })
    }
}

/// Deepest rung — set `status.autoSuspended=true`. The reconcile
/// entry checks `auto_suspended` and halts further work on this CR.
/// Humans clear via `kubectl patch ... --type=merge \
///   -p '{"status":{"autoSuspended":false}}' --subresource status`.
pub struct PauseAndAlertHandler;

#[async_trait]
impl EscalationActionHandler for PauseAndAlertHandler {
    fn action(&self) -> EscalationAction {
        EscalationAction::PauseAndAlert
    }

    async fn execute(&self, ctx: &EscalationContext<'_>) -> anyhow::Result<EscalationOutcome> {
        Ok(EscalationOutcome {
            status_patch: serde_json::json!({
                "status": { "autoSuspended": true }
            }),
            event_reason: "EscalationLadderPause",
            event_message: format!(
                "Auto-suspended after {}s unready (consecutive failures: {}). \
                 Recovery ladder reached PauseAndAlert (depth 4). Last error \
                 signature: {}. Clear with: kubectl patch -n <ns> it/<name> \
                 --type=merge -p '{{\"status\":{{\"autoSuspended\":false}}}}' \
                 --subresource status.",
                ctx.duration_unready.as_secs(),
                ctx.consecutive_failures,
                ctx.error_signature,
            ),
        })
    }
}

/// Trait — drop the workspace's cached `_repo` clone so the next
/// reconcile re-pulls source. The seam lets tests inject a fake
/// invalidator without `WorkspaceManager` setup; production wires
/// `Arc<WorkspaceManager>` via its blanket impl below.
#[async_trait]
pub trait WorkspaceCacheInvalidator: Send + Sync {
    /// Invalidate the cached workspace for (namespace, name). MUST be
    /// idempotent — calling twice with the same key is no harm.
    async fn invalidate(&self, namespace: &str, name: &str) -> anyhow::Result<()>;
}

/// Blanket impl — production wires the real `WorkspaceManager`.
///
/// MUST call `invalidate_repo_cache`, never `delete_workspace`.
/// `delete_workspace` removes the WHOLE workspace directory —
/// `terraform.tfstate` and `.backup` included — and is reserved for
/// the CR-deletion lifecycle (see its doc comment). This handler
/// fires from the escalation ladder's `RefreshSource` rung any time a
/// previously-Ready template's compile starts failing (e.g. a bad
/// commit on `gitRepository`), which is exactly the moment real,
/// live-applied Terraform state is sitting in this workspace and must
/// survive. `invalidate_repo_cache` is idempotent and structurally
/// scoped to the `_repo` subdirectory only — see its doc comment.
#[async_trait]
impl WorkspaceCacheInvalidator for crate::executor::WorkspaceManager {
    async fn invalidate(&self, namespace: &str, name: &str) -> anyhow::Result<()> {
        self.invalidate_repo_cache(namespace, name)
            .await
            .map_err(|e| anyhow::anyhow!("WorkspaceManager::invalidate_repo_cache: {e}"))
    }
}

/// Slice-5 first real handler: drop the workspace's cached `_repo`
/// clone via the `WorkspaceCacheInvalidator` trait. Next reconcile
/// re-pulls source, eliminating the "the source moved but our clone
/// is stale" failure mode at depth 1 of the ladder.
///
/// Construct with `RefreshSourceHandler::new(invalidator)`; the
/// registry's default builder accepts the invalidator as a
/// constructor arg.
pub struct RefreshSourceHandler {
    invalidator: Arc<dyn WorkspaceCacheInvalidator>,
}

impl RefreshSourceHandler {
    pub fn new(invalidator: Arc<dyn WorkspaceCacheInvalidator>) -> Self {
        Self { invalidator }
    }
}

#[async_trait]
impl EscalationActionHandler for RefreshSourceHandler {
    fn action(&self) -> EscalationAction {
        EscalationAction::RefreshSource
    }

    async fn execute(&self, ctx: &EscalationContext<'_>) -> anyhow::Result<EscalationOutcome> {
        let namespace = ctx
            .template
            .metadata
            .namespace
            .as_deref()
            .unwrap_or_default();
        let name = ctx.template.metadata.name.as_deref().unwrap_or_default();

        // Invalidate via the trait. On failure we still emit the
        // Event (so operators see the attempt) but propagate the
        // error so the caller's handler-error path fires.
        match self.invalidator.invalidate(namespace, name).await {
            Ok(()) => Ok(EscalationOutcome {
                status_patch: serde_json::json!({}),
                event_reason: "EscalationLadderRefreshSource",
                event_message: format!(
                    "Recovery ladder: RefreshSource (depth 1, after {}s unready). \
                     Workspace clone invalidated; next reconcile will re-pull source.",
                    ctx.duration_unready.as_secs(),
                ),
            }),
            Err(e) => Err(anyhow::anyhow!(
                "RefreshSource invalidation failed for {namespace}/{name}: {e}"
            )),
        }
    }
}

/// No-op invalidator — succeeds without touching disk. Useful for
/// tests + as the default when production hasn't wired the real
/// `WorkspaceManager` yet.
pub struct NoopInvalidator;

#[async_trait]
impl WorkspaceCacheInvalidator for NoopInvalidator {
    async fn invalidate(&self, _namespace: &str, _name: &str) -> anyhow::Result<()> {
        Ok(())
    }
}

/// TODO (slice-5): purge `$LOADED_FEATURES` under the gem-cache
/// prefix + re-broadcast every gem lib to every pool worker.
pub struct ReloadGemsHandler;

#[async_trait]
impl EscalationActionHandler for ReloadGemsHandler {
    fn action(&self) -> EscalationAction {
        EscalationAction::ReloadGems
    }

    async fn execute(&self, ctx: &EscalationContext<'_>) -> anyhow::Result<EscalationOutcome> {
        Ok(EscalationOutcome {
            status_patch: serde_json::json!({}),
            event_reason: "EscalationLadderReloadGems",
            event_message: format!(
                "Recovery ladder: ReloadGems (depth 2, after {}s unready). Gem-cache \
                 invalidation deferred to slice-5; this event records the recommendation.",
                ctx.duration_unready.as_secs(),
            ),
        })
    }
}

/// TODO (slice-5): kill + respawn every Ruby pool worker.
pub struct RecycleWorkersHandler;

#[async_trait]
impl EscalationActionHandler for RecycleWorkersHandler {
    fn action(&self) -> EscalationAction {
        EscalationAction::RecycleWorkers
    }

    async fn execute(&self, ctx: &EscalationContext<'_>) -> anyhow::Result<EscalationOutcome> {
        Ok(EscalationOutcome {
            status_patch: serde_json::json!({}),
            event_reason: "EscalationLadderRecycleWorkers",
            event_message: format!(
                "Recovery ladder: RecycleWorkers (depth 3, after {}s unready). Pool-worker \
                 recycle deferred to slice-5; this event records the recommendation.",
                ctx.duration_unready.as_secs(),
            ),
        })
    }
}

// ── Registry / dispatch ─────────────────────────────────────────────

/// Bundle of every action's handler. The dispatcher picks one by
/// matching the recommended action. Constructed once at startup +
/// shared via `ControllerState`.
///
/// Adding a new `EscalationAction` variant → add a corresponding
/// handler field + match arm. The trait's blanket implementation
/// makes the compile-time check happen: forget a variant and the
/// match in `dispatch` won't be exhaustive.
pub struct EscalationHandlerRegistry {
    retry: Arc<dyn EscalationActionHandler>,
    refresh_source: Arc<dyn EscalationActionHandler>,
    reload_gems: Arc<dyn EscalationActionHandler>,
    recycle_workers: Arc<dyn EscalationActionHandler>,
    pause_and_alert: Arc<dyn EscalationActionHandler>,
}

impl EscalationHandlerRegistry {
    /// Default registry — one implementation per action variant.
    /// Slice-5: `RefreshSource` requires a `WorkspaceCacheInvalidator`
    /// (production wires `Arc<WorkspaceManager>`); `ReloadGems` +
    /// `RecycleWorkers` remain no-op stubs until their dependencies
    /// land.
    pub fn pangea_default(invalidator: Arc<dyn WorkspaceCacheInvalidator>) -> Self {
        Self {
            retry: Arc::new(RetryHandler),
            refresh_source: Arc::new(RefreshSourceHandler::new(invalidator)),
            reload_gems: Arc::new(ReloadGemsHandler),
            recycle_workers: Arc::new(RecycleWorkersHandler),
            pause_and_alert: Arc::new(PauseAndAlertHandler),
        }
    }

    /// Test/no-op registry — wires a noop invalidator that always
    /// succeeds without touching disk. Use in unit tests that don't
    /// need the real workspace lifecycle.
    pub fn pangea_default_noop() -> Self {
        Self::pangea_default(Arc::new(NoopInvalidator))
    }

    /// Dispatch: pick the handler for the given action. Total over
    /// every variant — adding a variant forces an explicit arm here
    /// at compile time.
    pub fn handler_for(&self, action: EscalationAction) -> &Arc<dyn EscalationActionHandler> {
        match action {
            EscalationAction::Retry => &self.retry,
            EscalationAction::RefreshSource => &self.refresh_source,
            EscalationAction::ReloadGems => &self.reload_gems,
            EscalationAction::RecycleWorkers => &self.recycle_workers,
            EscalationAction::PauseAndAlert => &self.pause_and_alert,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    /// Build a minimal context for handler tests. Most handlers don't
    /// touch `template` (slice-5 will). Using a stub `template` is
    /// fine for v1 trait shape verification.
    fn minimal_ctx(action: EscalationAction) -> EscalationContext<'static> {
        // Static template stub via the same JSON-payload pattern other
        // controller tests use (see fleet_status_controller's
        // `fake_template`). InfrastructureTemplateSpec doesn't impl
        // Default by design (every field is meaningful), so we round-
        // trip through serde to construct one cheaply.
        static TEMPLATE: once_cell::sync::OnceCell<InfrastructureTemplate> =
            once_cell::sync::OnceCell::new();
        let template = TEMPLATE.get_or_init(|| {
            let payload = serde_json::json!({
                "apiVersion": "pangea.pleme.io/v1alpha1",
                "kind": "InfrastructureTemplate",
                "metadata": { "name": "test-template", "namespace": "test-ns" },
                "spec": {
                    "source": { "raw": "" },
                    "pangeaNamespace": "default"
                },
                "status": null
            });
            serde_json::from_value::<InfrastructureTemplate>(payload)
                .expect("test stub template must deserialize")
        });
        EscalationContext {
            template,
            action,
            duration_unready: Duration::from_secs(900),
            consecutive_failures: 5,
            last_error: "test error".to_string(),
            error_signature: "abcdef123456".to_string(),
        }
    }

    #[tokio::test]
    async fn retry_handler_returns_empty_status_patch() {
        let h = RetryHandler;
        let ctx = minimal_ctx(EscalationAction::Retry);
        let outcome = h.execute(&ctx).await.unwrap();
        assert_eq!(outcome.status_patch, serde_json::json!({}));
        assert_eq!(outcome.event_reason, "EscalationLadderRetry");
    }

    #[tokio::test]
    async fn pause_and_alert_handler_sets_auto_suspended_true() {
        let h = PauseAndAlertHandler;
        let ctx = minimal_ctx(EscalationAction::PauseAndAlert);
        let outcome = h.execute(&ctx).await.unwrap();
        assert_eq!(
            outcome.status_patch["status"]["autoSuspended"],
            serde_json::Value::Bool(true),
            "PauseAndAlert must request autoSuspended=true"
        );
        assert_eq!(outcome.event_reason, "EscalationLadderPause");
        // Error signature included in the event message so operators
        // can grep `kubectl get events` by the bug class.
        assert!(outcome.event_message.contains("abcdef123456"));
    }

    #[tokio::test]
    async fn slice5_handlers_emit_record_event_no_status_change_yet() {
        // The shallower-but-not-yet-implemented handlers emit only an
        // Event recording the recommendation. Behavioral change lands
        // in slice-5. This test pins the "record-only" contract for
        // the still-stubbed handlers.
        let h = RefreshSourceHandler::new(Arc::new(NoopInvalidator));
        let ctx = minimal_ctx(EscalationAction::RefreshSource);
        let outcome = h.execute(&ctx).await.unwrap();
        // RefreshSource now has a real impl — invalidate succeeded via
        // NoopInvalidator. Status patch stays empty (refresh doesn't
        // mutate status); event reason carries the recommendation.
        assert_eq!(outcome.status_patch, serde_json::json!({}));
        assert_eq!(outcome.event_reason, "EscalationLadderRefreshSource");
        assert!(
            outcome
                .event_message
                .contains("Workspace clone invalidated"),
            "RefreshSource event message must reflect the action taken"
        );

        let h = ReloadGemsHandler;
        let ctx = minimal_ctx(EscalationAction::ReloadGems);
        let outcome = h.execute(&ctx).await.unwrap();
        assert_eq!(outcome.status_patch, serde_json::json!({}));
        assert_eq!(outcome.event_reason, "EscalationLadderReloadGems");

        let h = RecycleWorkersHandler;
        let ctx = minimal_ctx(EscalationAction::RecycleWorkers);
        let outcome = h.execute(&ctx).await.unwrap();
        assert_eq!(outcome.status_patch, serde_json::json!({}));
        assert_eq!(outcome.event_reason, "EscalationLadderRecycleWorkers");
    }

    #[tokio::test]
    async fn refresh_source_handler_propagates_invalidator_failure() {
        // When the invalidator fails (disk error, perm issue, race),
        // RefreshSourceHandler MUST return Err so the caller's
        // handler-error path logs + proceeds with the base escalation
        // patch. Without this, a stuck invalidation would silently
        // succeed and the operator would never retry the cleanup.

        struct FailingInvalidator;
        #[async_trait]
        impl WorkspaceCacheInvalidator for FailingInvalidator {
            async fn invalidate(&self, _ns: &str, _name: &str) -> anyhow::Result<()> {
                Err(anyhow::anyhow!("simulated disk failure"))
            }
        }

        let h = RefreshSourceHandler::new(Arc::new(FailingInvalidator));
        let ctx = minimal_ctx(EscalationAction::RefreshSource);
        let err = h
            .execute(&ctx)
            .await
            .expect_err("handler must surface error");
        assert!(err.to_string().contains("simulated disk failure"));
    }

    #[tokio::test]
    async fn refresh_source_handler_calls_invalidator_with_template_identity() {
        // Verifies the handler passes the CORRECT (namespace, name)
        // pair from the template — the trait dispatch must not lose
        // identity, or invalidations would hit the wrong workspace.

        struct CapturingInvalidator {
            captured: Arc<Mutex<Option<(String, String)>>>,
        }
        #[async_trait]
        impl WorkspaceCacheInvalidator for CapturingInvalidator {
            async fn invalidate(&self, ns: &str, name: &str) -> anyhow::Result<()> {
                *self.captured.lock().unwrap() = Some((ns.to_string(), name.to_string()));
                Ok(())
            }
        }

        let captured = Arc::new(Mutex::new(None));
        let inv = Arc::new(CapturingInvalidator {
            captured: captured.clone(),
        });
        let h = RefreshSourceHandler::new(inv);
        let ctx = minimal_ctx(EscalationAction::RefreshSource);
        h.execute(&ctx).await.unwrap();

        let captured = captured.lock().unwrap().clone().expect("invalidate called");
        assert_eq!(captured.0, "test-ns");
        assert_eq!(captured.1, "test-template");
    }

    #[tokio::test]
    async fn registry_dispatches_each_action_to_its_handler() {
        // The registry must route every variant to a handler whose
        // `.action()` matches the dispatch key. Catches accidental
        // wiring (e.g. registering RetryHandler under the
        // PauseAndAlert slot, which would silently break recovery).
        let reg = EscalationHandlerRegistry::pangea_default_noop();
        for action in [
            EscalationAction::Retry,
            EscalationAction::RefreshSource,
            EscalationAction::ReloadGems,
            EscalationAction::RecycleWorkers,
            EscalationAction::PauseAndAlert,
        ] {
            let handler = reg.handler_for(action);
            assert_eq!(
                handler.action(),
                action,
                "registry's handler for {action:?} reports its own action mismatched"
            );
        }
    }

    // ── Regression: real WorkspaceCacheInvalidator must never touch state ──
    //
    // The prior `WorkspaceCacheInvalidator for WorkspaceManager` blanket
    // impl called `delete_workspace`, which `fs::remove_dir_all()`s the
    // WHOLE workspace directory. That handler is live-wired in
    // production (`ControllerState::new` →
    // `EscalationHandlerRegistry::pangea_default(workspace_manager)`)
    // and fires from `handle_compile_failure` any time a previously-
    // Ready template's compile starts failing for ~5 minutes — exactly
    // when real, live-applied Terraform state is sitting in that
    // workspace. This test builds the REAL `WorkspaceManager` (not a
    // stub) against a temp dir, seeds it with `terraform.tfstate` +
    // `.backup` + a populated `_repo` clone the way a genuinely-applied
    // template would have, invokes the invalidator exactly as
    // `RefreshSourceHandler` does, and asserts state survives. On the
    // pre-fix code this test fails: `terraform.tfstate` is gone after
    // `invalidate()`.
    #[tokio::test]
    async fn real_workspace_manager_invalidator_preserves_state_and_only_clears_repo_cache() {
        use crate::executor::WorkspaceManager;

        let base = tempfile::tempdir().expect("create temp base dir");
        let wm = WorkspaceManager::new(base.path().to_path_buf());
        let ws = wm
            .get_or_create("test-ns", "test-template")
            .await
            .expect("create workspace");

        // Seed exactly what a live, previously-applied template's
        // workspace looks like: real state + its tofu backup + a
        // populated `_repo` clone (the thing the handler is actually
        // meant to invalidate).
        tokio::fs::write(ws.state_path(), br#"{"version":4,"serial":7}"#)
            .await
            .expect("seed terraform.tfstate");
        tokio::fs::write(ws.state_backup_path(), br#"{"version":4,"serial":6}"#)
            .await
            .expect("seed terraform.tfstate.backup");
        let repo_dir = ws.path.join("_repo");
        tokio::fs::create_dir_all(&repo_dir)
            .await
            .expect("seed _repo dir");
        tokio::fs::write(repo_dir.join("main.tf.json.tmpl"), b"stale-source")
            .await
            .expect("seed _repo contents");

        // Exercise the SAME trait object production wires — the
        // blanket impl, not a direct call to a specific method — so a
        // future re-wiring of `invalidate()` to some other unsafe
        // primitive is caught by this test too.
        let invalidator: Arc<dyn WorkspaceCacheInvalidator> = Arc::new(wm);
        invalidator
            .invalidate("test-ns", "test-template")
            .await
            .expect("invalidate must succeed");

        assert!(
            ws.state_path().exists(),
            "terraform.tfstate must survive RefreshSource cache invalidation \
             (this is the 2026-07-12 camelot-eks duplicate-VPC bug's second \
             code path — a live template's state must never be wiped just \
             because its source stopped compiling)"
        );
        assert_eq!(
            tokio::fs::read(ws.state_path()).await.unwrap(),
            br#"{"version":4,"serial":7}"#,
            "state CONTENT must be untouched, not just the filename surviving"
        );
        assert!(
            ws.state_backup_path().exists(),
            "terraform.tfstate.backup must also survive"
        );
        assert!(
            !repo_dir.exists(),
            "the cached _repo clone IS what invalidate() is meant to drop"
        );
    }
}
