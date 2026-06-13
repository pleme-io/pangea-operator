//! Kubernetes controller components for the Pangea Operator.
//!
//! This module contains the reconciliation logic for all custom resources:
//! InfrastructureTemplate, PangeaNamespace, InfrastructureFlow, PackerBuild,
//! AmiTest, and ImagePipeline.

pub mod architecture_gem_controller;
pub mod conflict;
pub mod anomaly_emitter;
pub mod anomaly_tracker;
pub mod error_policy;
pub mod escalation;
pub mod escalation_handlers;
pub mod finalizer;
pub mod fleet_status_controller;
pub mod generation_filter;
pub mod import;
pub mod status_patch;
pub mod operator_policy_cache;
pub mod operator_policy_controller;
pub mod policy_gate;
pub mod policy_pipeline;
pub mod post_reconcile_pipeline;
pub mod reactive;
pub mod routing;
pub mod status;
pub mod template_phase;
pub mod workspace_catalog_controller;
mod flow_controller;
pub mod flow_scheduler;
pub mod policy_cascade;
mod reconciler;
pub mod settling;
mod template;
mod template_controller;
mod namespace_controller;
mod packer_build_controller;
mod ami_test_controller;
mod image_pipeline_controller;
mod synthesizer_format_controller;
mod compliance_schedule_controller;
mod compliance_binding_controller;
mod dashboard_controller;

pub use reconciler::*;
pub use flow_controller::FlowController;
pub use template_controller::TemplateController;
pub use namespace_controller::NamespaceController;
pub use packer_build_controller::PackerBuildController;
pub use ami_test_controller::AmiTestController;
pub use image_pipeline_controller::ImagePipelineController;
pub use synthesizer_format_controller::SynthesizerFormatController;
pub use compliance_schedule_controller::ComplianceScheduleController;
pub use compliance_binding_controller::ComplianceBindingController;
pub use dashboard_controller::DashboardController;
pub use operator_policy_controller::OperatorPolicyController;

use crate::backend::StateBackend;
use crate::error::Result;
use crate::executor::{
    ExecutorBackend, ExecutorConfig, IacExecutor, PackerExecutor, TofuExecutor, WorkspaceManager,
};
use kube::Client;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;

/// Shared state for all controllers.
#[derive(Clone)]
pub struct ControllerState {
    /// Kubernetes client.
    pub client: Client,

    /// Metrics registry.
    pub metrics: Arc<crate::observability::Metrics>,

    /// PostgreSQL connection pool (if configured).
    pub db_pool: Option<Arc<RwLock<sqlx::PgPool>>>,

    /// IaC executor for running infrastructure commands. Polymorphic
    /// over `Arc<dyn IacExecutor>`. This field holds the always-built
    /// `TofuExecutor` (subprocess) — it is the explicit fallback when
    /// magma is the resolved backend but unavailable (feature off or
    /// no state backend). Per `theory/MAGMA-OPERATOR-BACKEND.md` §VI
    /// and `docs/design/0005-autonomic-convergence-on-magma.md`. Use
    /// `executor_for(template)` to pick the per-CR backend (magma by
    /// default, tofu on explicit opt-out).
    pub executor: Arc<dyn IacExecutor>,

    /// The operator-wide default backend choice, derived from the
    /// `PANGEA_EXECUTOR` env var at startup (falls back to `Magma`).
    /// `executor_for(template)` uses this as the fallback when a CR
    /// doesn't set its own `spec.executor`.
    pub default_backend: ExecutorBackend,

    /// Shared state backend (OpenTofu state in PostgreSQL via the
    /// operator's `StateStore`). `Some` once a PG pool is wired in
    /// (`with_db_pool`); `None` otherwise. The magma executor reads
    /// and writes the SAME tofu-format state rows through this
    /// backend (keyed by the per-CR schema/template/state triple),
    /// so a per-CR magma↔tofu switch never forks state. When `None`,
    /// `executor_for` cannot build a `MagmaExecutor` and falls back
    /// to tofu.
    pub state_backend: Option<Arc<dyn StateBackend>>,

    /// Postgres-backed durable artifact store for the magma execution
    /// path (rendered config / plan / bundle, plus the atomic
    /// state+bundle apply commit). `Some` once a PG pool is wired in
    /// (`with_db_pool`); `None` otherwise. When `Some`, the
    /// `MagmaExecutor` runs fully DB-backed + zero-disk (per the org
    /// ★★ MAGMA-NATIVE EXECUTION directive); when `None`, magma falls
    /// back to its workspace-dir disk artifacts. Shares the one pool
    /// the `state_backend` derives from.
    pub artifact_store: Option<Arc<crate::backend::ArtifactStore>>,

    /// Packer executor for running AMI build commands.
    pub packer_executor: Arc<PackerExecutor>,

    /// Workspace manager for isolated template directories.
    pub workspace_manager: Arc<WorkspaceManager>,

    /// Compiler backend — HTTP sidecar today, embedded magnus when
    /// `embedded_ruby` is feature-on + `PANGEA_COMPILER_BACKEND=embedded`.
    /// See `theory/PANGEA-WORKSPACE-RECONCILIATION.md` § M8.2.
    pub compiler_backend: Arc<dyn crate::ruby::CompilerBackend>,

    /// Routing-delivery client — pings ntfy / Slack / GitHub when a
    /// reactive policy fires. Configured via PANGEA_NTFY_BASE_URL
    /// (defaults to https://ntfy.sh).
    pub routing_client: Arc<routing::RoutingClient>,

    /// In-memory snapshot of `OperatorPolicy/default`. Read by every
    /// reconciler via `policy_gate::check_operator_policy` to honor
    /// the fleet-wide kill-switch + per-controller suspends. Updated
    /// by the `operator_policy_watcher` task.
    pub operator_policy: Arc<operator_policy_cache::OperatorPolicyCache>,

    /// Anomaly recurrence tracker — observes (template, error_signature)
    /// pairs and counts recurrence. Per-process in v1 (lost on
    /// restart); slice-N swaps for sqlx-backed persistent impl
    /// behind the same `RecurrenceObserver` trait so the swap is
    /// local. Read by `handle_compile_failure` to surface "this
    /// error has now repeated N times" in tracing + (slice-4)
    /// `.status.anomalies[]`. See
    /// `project_controller_detection_axis.md` (known-unknowns axis).
    pub anomaly_tracker: Arc<dyn anomaly_tracker::RecurrenceObserver>,

    /// Registry of escalation-action handlers — one impl per
    /// `EscalationAction` variant. Read by `handle_compile_failure`
    /// (and future failure paths) to dispatch the recovery action
    /// behind a trait. Slice-5 RefreshSource / ReloadGems /
    /// RecycleWorkers handlers slot in by replacing the no-op impls
    /// in `EscalationHandlerRegistry::pangea_default()`; the trait
    /// shape stays so call sites don't change.
    /// See `project_escalation_ladder.md`.
    pub escalation_handlers: Arc<escalation_handlers::EscalationHandlerRegistry>,

    /// Composite anomaly emitter — fans the per-failure
    /// `AnomalySummary` to every configured sink (tracing, Prometheus,
    /// future: status field, k8s Events, GraphQL stream). Slice-4
    /// adds new emitters by appending to this composite, not by
    /// editing each call site. See `anomaly_emitter.rs` +
    /// `project_controller_detection_axis.md` (expose axis).
    pub anomaly_emitter: Arc<dyn anomaly_emitter::AnomalyEmitter>,
}

impl ControllerState {
    /// Create new controller state with executor configuration.
    pub async fn new(
        client: Client,
        metrics: Arc<crate::observability::Metrics>,
        executor_config: ExecutorConfig,
        compiler_backend: Arc<dyn crate::ruby::CompilerBackend>,
    ) -> Result<Self> {
        // Default backend selection: env var → magma default. The
        // CR-level override happens in `executor_for(template)`.
        let default_backend = ExecutorBackend::resolve(
            None,
            std::env::var("PANGEA_EXECUTOR").ok().as_deref(),
        );
        let executor: Arc<dyn IacExecutor> = Arc::new(TofuExecutor::new(
            executor_config.tofu_binary.clone(),
            Duration::from_secs(executor_config.timeout_secs),
            executor_config.verbose,
        ));

        let packer_executor = Arc::new(PackerExecutor::new(
            executor_config.packer_binary.clone(),
            Duration::from_secs(executor_config.packer_timeout_secs),
            executor_config.verbose,
        ));

        let workspace_manager = Arc::new(WorkspaceManager::new(
            executor_config.workspace_base.clone(),
        ));

        // Ensure workspace base directory exists
        workspace_manager.init().await?;

        let anomaly_emitter_arc: Arc<dyn anomaly_emitter::AnomalyEmitter> = Arc::new(
            anomaly_emitter::CompositeEmitter::pangea_default(metrics.clone()),
        );

        Ok(Self {
            client,
            metrics,
            db_pool: None,
            executor,
            default_backend,
            state_backend: None,
            artifact_store: None,
            packer_executor,
            workspace_manager: workspace_manager.clone(),
            compiler_backend,
            routing_client: Arc::new(routing::RoutingClient::from_env()),
            operator_policy: Arc::new(operator_policy_cache::OperatorPolicyCache::new_permissive()),
            anomaly_tracker: Arc::new(anomaly_tracker::InMemoryRecurrenceTracker::new()),
            escalation_handlers: Arc::new(
                escalation_handlers::EscalationHandlerRegistry::pangea_default(
                    workspace_manager.clone(),
                ),
            ),
            anomaly_emitter: anomaly_emitter_arc,
        })
    }

    /// Resolve which `IacExecutor` impl handles a CR. Honors the
    /// CR's `spec.executor` first, then `default_backend` (which
    /// itself came from `PANGEA_EXECUTOR` or the `Magma` default).
    ///
    /// Routing:
    ///   * Resolved == `Magma`, the `executor_magma` feature is on,
    ///     AND a state backend is available → build a fresh
    ///     `MagmaExecutor` whose state backend is the operator's
    ///     `PostgresStateBackend`, encoded in `BackendShape::Tofu`
    ///     so it reads/writes the SAME state bytes tofu writes, keyed
    ///     by the per-CR `(schema, template, state)` triple.
    ///   * Otherwise (resolved == `Tofu`, feature off, or no state
    ///     backend) → the always-built shared `TofuExecutor`. Never
    ///     panics: a missing backend silently falls back to tofu.
    ///
    /// Per `theory/MAGMA-OPERATOR-BACKEND.md` §VI and
    /// `docs/design/0005-autonomic-convergence-on-magma.md`.
    pub fn executor_for(
        &self,
        template: &crate::crd::InfrastructureTemplate,
    ) -> Arc<dyn IacExecutor> {
        let chosen = ExecutorBackend::resolve(
            template.spec.executor.as_deref(),
            Some(self.default_backend.label()),
        );

        match chosen {
            ExecutorBackend::Magma => self.magma_executor_for(template),
            ExecutorBackend::Tofu => Arc::clone(&self.executor),
        }
    }

    /// Like [`executor_for`](Self::executor_for) but enforces the
    /// `PANGEA_FORBID_TOFU` config gate: when the env var is truthy AND
    /// resolution selects tofu (whether by `spec.executor=tofu` or as
    /// the fallback), this returns a typed [`Error::TofuForbidden`]
    /// naming the template instead of silently building a tofu
    /// executor.
    ///
    /// Per the org ★★ MAGMA-NATIVE directive, tofu may only run by
    /// explicit config, never a silent fallback. The reconcile path
    /// (plan / apply) resolves through this checked variant so a
    /// forbidden tofu selection surfaces as a loud cycle error
    /// (`status.lastError`) and the apply never runs tofu. The
    /// infallible `executor_for` stays for non-mutating call sites
    /// (cycle-receipt labeling) where the choice is only being read,
    /// not used to mutate infrastructure.
    ///
    /// This makes the "silently ran tofu for spec.executor=magma"
    /// class (flake.nix:358 comment, caught by the rio-health-check
    /// canary 2026-05-27) unrepresentable on the apply path.
    pub fn executor_for_checked(
        &self,
        template: &crate::crd::InfrastructureTemplate,
    ) -> Result<Arc<dyn IacExecutor>> {
        use kube::ResourceExt;

        let chosen = ExecutorBackend::resolve(
            template.spec.executor.as_deref(),
            Some(self.default_backend.label()),
        );

        if chosen == ExecutorBackend::Tofu
            && crate::executor::backend_select::forbid_tofu_from_env()
        {
            return Err(crate::error::Error::TofuForbidden {
                template: template.name_any(),
                reason: "PANGEA_FORBID_TOFU is set".to_string(),
            });
        }

        Ok(match chosen {
            ExecutorBackend::Magma => self.magma_executor_for(template),
            ExecutorBackend::Tofu => Arc::clone(&self.executor),
        })
    }

    /// Build a magma-backed `IacExecutor` for this CR, or fall back
    /// to the shared tofu executor when magma is unavailable.
    ///
    /// When the `executor_magma` feature is OFF this is a thin
    /// shim that always returns the tofu executor, so the resolver
    /// above compiles and behaves identically in a tofu-only build.
    #[cfg(feature = "executor_magma")]
    fn magma_executor_for(
        &self,
        template: &crate::crd::InfrastructureTemplate,
    ) -> Arc<dyn IacExecutor> {
        use crate::executor::{MagmaExecutor, MagmaExecutorConfig};
        use kube::ResourceExt;
        use magma_operator_backend::BackendShape;

        // Magma needs the shared state backend to read/write the
        // existing tofu-format state. No backend → fall back to tofu
        // (never panic).
        let Some(state_backend) = self.state_backend.clone() else {
            tracing::warn!(
                template = %template.name_any(),
                "executor=magma requested but no state backend is wired in; \
                 falling back to tofu",
            );
            return Arc::clone(&self.executor);
        };

        // Per-CR state key. MUST match the keys tofu's pg backend
        // (BackendConfigGenerator) uses so magma reads tofu's SAME live
        // rows — no data migration:
        //   * schema_name   = "pangea_{spec.pangeaNamespace}"
        //                     (PangeaNamespace::schema_name(), default
        //                      prefix "pangea_").
        //   * template_name = the CR name (`name_any()`).
        //   * state_name    = "default" (OpenTofu default workspace).
        // TofuPgStateBackend turns these into the live OpenTofu pg
        // table `"{schema}_{template}_states".states` (verified live).
        let schema_name = format!("pangea_{}", template.spec.pangea_namespace);
        let template_name = template.name_any();
        let state_name = "default".to_string();

        let cfg = MagmaExecutorConfig {
            state_backend,
            schema_name,
            template_name,
            state_name,
            // tofu-format bytes so magma + tofu read the same state.
            backend_shape:   BackendShape::Tofu,
            plan_checkpoint: true,
            preflight_laws:  true,
            drift_policy:    magma_drift::DriftPolicy::conservative_default(),
            audit_log_path:  None,
            // When the artifact store is wired (DB pool live), the
            // magma path runs fully DB-backed + zero-disk: rendered
            // config / plan / bundle in Postgres, the state+bundle
            // apply commit atomic. None → workspace-dir disk fallback.
            artifact_store:  self.artifact_store.clone(),
        };

        Arc::new(MagmaExecutor::new(cfg)) as Arc<dyn IacExecutor>
    }

    /// Tofu-only build: magma is not compiled in, so any resolved
    /// `Magma` choice degrades to the shared tofu executor.
    #[cfg(not(feature = "executor_magma"))]
    fn magma_executor_for(
        &self,
        template: &crate::crd::InfrastructureTemplate,
    ) -> Arc<dyn IacExecutor> {
        use kube::ResourceExt;
        // Loud, not silent: a spec.executor=magma CR on an operator built
        // WITHOUT executor_magma must not quietly run tofu (that masked a
        // non-cutover during the 2026-05-27 canary). Surface it every time.
        tracing::warn!(
            template = %template.name_any(),
            "spec.executor=magma but this operator was built WITHOUT the \
             executor_magma feature — running tofu. Rebuild the image with \
             executor_magma (and regenerate Cargo.nix) to enable magma."
        );
        Arc::clone(&self.executor)
    }

    /// Build the typed `WorkspaceRunner` for this CR's reconcile path.
    ///
    /// Mirrors `executor_for` but wraps the chosen `IacExecutor` in
    /// the matching typed runner (`MagmaWorkspaceRunner` /
    /// `TofuWorkspaceRunner`). The controller's phase handlers
    /// consume this typed surface (slice 2c) instead of reaching for
    /// the raw `IacExecutor` + reparsing JSON.
    ///
    /// Dispatch is on the inner executor's `.name()` so the test mock
    /// (`RecordingExecutor`, name=`"recording"`) cleanly falls through
    /// to the tofu runner (which speaks tofu-shaped JSON, matching
    /// the mock's canned output).
    pub fn executor_runner_for(
        &self,
        template: &crate::crd::InfrastructureTemplate,
    ) -> Arc<dyn crate::executor::workspace_runner::WorkspaceRunner> {
        use crate::executor::workspace_runner::{
            MagmaWorkspaceRunner, TofuWorkspaceRunner, WorkspaceRunner,
        };
        let exec = self.executor_for(template);
        match exec.name() {
            // Thread the artifact store so the magma runner knows it's on
            // the DB-backed zero-disk path and skips the `magma-bundle.json`
            // disk read (the bundle lives in Postgres). Mirrors how
            // `magma_executor_for` wires `MagmaExecutorConfig.artifact_store`.
            "magma" => Arc::new(MagmaWorkspaceRunner::new(exec, self.artifact_store.clone()))
                as Arc<dyn WorkspaceRunner>,
            _       => Arc::new(TofuWorkspaceRunner::new(exec)) as Arc<dyn WorkspaceRunner>,
        }
    }

    /// Set the database pool.
    ///
    /// Also constructs the shared `StateBackend` (a
    /// `PostgresStateBackend` over the operator's `StateStore`) from
    /// the same pool, so `executor_for` can build a `MagmaExecutor`
    /// that reads/writes the existing tofu-format state rows. Both
    /// the `db_pool` handle and the `state_backend` are derived from
    /// one shared `Arc<PgPool>`.
    pub fn with_db_pool(mut self, pool: sqlx::PgPool) -> Self {
        let shared = Arc::new(pool);
        // magma reads/writes tofu's LIVE OpenTofu pg-backend state in
        // place — schema "{ns}_{cr}_states", table `states`, text data —
        // so NO data migration is needed. magma is tofu state-format
        // compatible; BackendShape::Tofu (set in executor_for) decodes
        // these bytes directly. Verified vs the live pangea_state DB
        // 2026-05-27. (magma-backend has no native pg impl; the
        // operator-provided AsyncStateStore is the canonical hookup.)
        // Wrap the live tofu-format state backend in the bounded
        // connect-retry decorator. During an unsupervised CNPG
        // switchover the `pangea-database-rw` service has no ready
        // primary for a few seconds (5432 refuses); sqlx does not
        // retry the initial dial, so a bare state read/write would
        // surface a hard cycle error. The decorator retries ONLY
        // connection-level errors (refused / closed / pool timeout)
        // with bounded exponential backoff (200ms→~3s, ~10s cap) —
        // a transient refuse recovers inside the window instead of
        // becoming a cycle error. Every StateBackend op is idempotent
        // (reads, upserts, idempotent deletes) so a retry never
        // double-applies. See backend/retry.rs.
        let live_backend: Arc<dyn crate::backend::StateBackend> =
            Arc::new(crate::backend::TofuPgStateBackend::new(Arc::clone(&shared)));
        self.state_backend = Some(Arc::new(crate::backend::RetryingStateBackend::new(
            live_backend,
        )));
        // Durable artifact store on the SAME pool: rendered config /
        // plan / bundle (and the atomic state+bundle apply commit) all
        // live in Postgres, so the magma path is zero-disk. The
        // `pangea_meta.artifacts` table is ensured at startup in main.rs
        // (the caller has the async context this sync builder lacks).
        // Per the org ★★ MAGMA-NATIVE EXECUTION directive.
        self.artifact_store = Some(Arc::new(crate::backend::ArtifactStore::new(Arc::clone(
            &shared,
        ))));
        // `db_pool` wraps the pool itself; reuse the same Arc'd pool.
        self.db_pool = Some(Arc::new(RwLock::new((*shared).clone())));
        self
    }
}
