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
//!   ↓ magma_apply::engine::refresh_then_plan(&cfg, &mut state, ctx) (real
//!     ReadResource refresh, Terraform-parity, then magma_plan::plan)
//! magma::types::Plan
//!   ↓ checkpoint to disk (restart safety) + magma_apply::engine::run_plan_with_providers
//! magma::types::ApplyOutcome
//!   ↓ write_state via OperatorBackend → StateBackendAsync
//! PG row (operator's existing storage)
//! ```
//!
//! The "checkpoint to disk" arrow above describes the **disk-fallback**
//! path (`artifact_store` = `None`). On the production **DB-backed**
//! path (`artifact_store` = `Some`) the rendered config, the plan
//! checkpoint, and the bundle all live in Postgres (`put_rendered_config`
//! / `put_plan` / the atomic `put_apply_result`), and the tofu-format
//! state lives in the OpenTofu pg-backend states table — NOTHING durable
//! is written to the pod-local workspace dir on that path (★★
//! MAGMA-NATIVE EXECUTION). The only sanctioned filesystem reaches that
//! remain on the DB-backed path are workspace *input* acquisition — the
//! gRPC provider-plugin binaries the OS must `exec`, and reading
//! `Gemfile.lock` / the cloned source tree — none of which is durable
//! execution state.
//!
//! On the disk-fallback path the checkpoint is restart-safety only —
//! `tokio::spawn`d reconciles can be SIGKILLed mid-apply; the next
//! reconcile reads the checkpoint, re-derives the plan_id via BLAKE3,
//! and resumes. The DB-backed path gets the same restart safety from
//! Postgres (a pod roll loses nothing — state + plan + bundle are
//! durable in the DB).
//!
//! # M0.10 scope
//!
//! Skeleton `IacExecutor` impl. `init` is a no-op (magma needs no
//! provider download — providers are typed-imported). `plan` /
//! `apply` / `destroy` / `show_plan` / `output` are wired to magma
//! APIs and tested via `InMemoryStateBackend`. `import` returns
//! NotImplemented (lands in M0.11 alongside auto-import parity).

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex as StdMutex, OnceLock};
use std::time::Instant;

use async_trait::async_trait;
use magma_operator_backend::{AsyncStateStore, BackendShape, OperatorBackend, StoreError};

use crate::backend::state_backend::StateBackend;
use crate::error::{Error, Result};
use crate::executor::iac_executor::IacExecutor;
use crate::executor::plan_change::PlannedChange;
use crate::executor::tofu::TofuResult;

// ── Import concurrency guard ──────────────────────────────────────
//
// `template_controller::run_import_prepass` dispatches every resolved
// import target CONCURRENTLY (`buffer_unordered(10)` — see
// `try_import`'s doc comment: "the pg backend's advisory lock
// naturally serializes the state-write step"). That comment is
// tofu-specific: a `tofu import` subprocess's OWN pg backend takes a
// per-command advisory lock around its read-modify-write, so N
// concurrent `tofu import` invocations against the same workspace are
// safe by construction.
//
// magma has no equivalent. `MagmaExecutor::import()` below reads the
// whole state row, mutates it in memory via the import prepass, and
// writes the whole row back — and `StateBackend::save_state` is a
// plain last-write-wins upsert (no compare-and-swap, no advisory
// lock). Worse, `ControllerState::magma_executor_with_provider_configs`
// constructs a FRESH `MagmaExecutor` per call site (`try_import`'s
// concurrent dispatch shares ONE credential-aware instance across its
// own `buffer_unordered` fan-out via `Arc::clone`, but `conflict.rs`'s
// `resolve_conflicts_post_apply` builds its own separate instance), so
// a lock field on `&self` still can't serialize the general case —
// concurrent `import()` calls for the SAME template from DIFFERENT call
// sites (or different reconcile ticks) never share an instance.
//
// This keyed, process-wide registry closes that gap: every
// `MagmaExecutor::import()` call acquires the lock for its
// `(schema_name, template_name, state_name)` triple before its
// read-modify-write critical section, so concurrent imports against
// the SAME workspace serialize (closing the lost-update race) while
// imports against DIFFERENT templates still run fully in parallel.
static IMPORT_STATE_LOCKS: OnceLock<StdMutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>> =
    OnceLock::new();

fn import_state_lock_key(schema_name: &str, template_name: &str, state_name: &str) -> String {
    format!("{schema_name}/{template_name}/{state_name}")
}

async fn acquire_import_state_lock(key: String) -> tokio::sync::OwnedMutexGuard<()> {
    let registry = IMPORT_STATE_LOCKS.get_or_init(|| StdMutex::new(HashMap::new()));
    let mutex = {
        // A poisoned std Mutex here would mean an earlier holder
        // panicked while holding ONLY the registry lock (a bare map
        // insert) — never while holding the returned tokio mutex.
        // Recovering the inner map is safe; the registry's own
        // consistency isn't affected by a panic elsewhere.
        let mut map = registry
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        Arc::clone(
            map.entry(key)
                .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(()))),
        )
    };
    mutex.lock_owned().await
}

/// TEST-ONLY [`magma_apply::ImportEnvironment`], selected when
/// `structural_apply` is set — mirroring the shape `apply()`/`destroy()`
/// already use for the structural (no-real-provider) unit-test path.
/// CI has no provider binaries, so a real
/// `magma_apply::ConfiguredImportEnvironment` can't dial anything here;
/// this canned, in-memory echo lets `MagmaExecutor::import()`'s OWN
/// logic (locking, state read/write, error surfacing) be exercised
/// without one. A sentinel id of `"missing"` fails, mirroring
/// `mock_provider`'s convention in the magma repo's own integration
/// suite, so the FailedImport path is testable too.
struct StructuralImportEnvironment;

#[async_trait]
impl magma_apply::ImportEnvironment for StructuralImportEnvironment {
    async fn import_resource_state(
        &self,
        type_name: &str,
        id: &str,
        // Added upstream (magma ac93c84): `ImportResourceState` is answered by
        // whichever configured instance receives it, so the caller now names
        // the instance that actually holds the resource instead of letting the
        // implementation infer it from the type prefix — inference always
        // yielded the DEFAULT instance, which silently adopted the wrong
        // resource when a same-id object existed in the default account.
        // Ignored here on purpose: this is the structural (no-real-provider)
        // echo used by unit tests, which dials nothing, so there is no RPC for
        // an instance to be routed to. A real environment must honour it.
        _provider: &magma_types::ProviderInstance,
    ) -> std::result::Result<Vec<magma_types::ImportedInstance>, String> {
        if id == "missing" {
            return Err(format!(
                "structural test environment: no resource with id {id:?}"
            ));
        }
        Ok(vec![magma_types::ImportedInstance {
            type_name: type_name.to_string(),
            attributes: serde_json::json!({ "id": id, "name": id }),
            private: Vec::new(),
        }])
    }
}

/// Typed outcome of reading the persisted plan from the DB-backed
/// artifact store. The three states let `apply()` / `planned_changes()`
/// tell a genuinely-missing OR stale plan row (regenerate) from a
/// torn/corrupt one (hard error — surfaced by `read_db_plan`'s `?`) from
/// the non-DB-backed disk/unit-test path.
///
/// This is the never-stuck fix: a MISSING (absent OR stale) row is a
/// cache-miss to regenerate from, NOT a terminal `Err` that wedges the
/// reconcile.
enum DbPlanRead {
    /// DB-backed executor, plan row present AND still current (its
    /// `source_revision` matches the render currently in the store).
    Present(magma_types::Plan),
    /// DB-backed executor, plan row genuinely absent OR STALE (its
    /// `source_revision` no longer matches the render's — the plan was
    /// computed from an older revision). Callers regenerate the plan
    /// rather than dead-ending or applying a stale (noOp) plan.
    Missing,
    /// No `artifact_store` — the disk-fallback / unit-test path.
    NotDbBacked,
}

/// The apply-phase plan-reuse decision (the sibling of
/// `rendered_config_is_current` in the controller). A persisted plan is
/// reusable **iff** the `source_revision` it was computed from equals the
/// `source_revision` of the `rendered_config` now in the store. The
/// render fix keeps the render's recorded revision current (it re-derives
/// on a moved source HEAD / changed inline content), so this enforces
/// "the plan must have been computed from the render currently in the
/// store" — reject a plan derived from an older render (which would apply
/// a noOp and silently never converge a source change).
///
///   * `(Some(p), Some(r))` — reuse iff `p == r`.
///   * `(None, _)` — the plan carries no recorded revision (a legacy /
///     NULL row) ⇒ stale ⇒ recompute once (converge-by-one-recompute).
///   * `(Some(_), None)` — a stored plan but no derivable render revision
///     (no render row, or a legacy/NULL render row). Can't prove the plan
///     is current ⇒ stale ⇒ recompute. Safe: the very first plan on the
///     DB path is preceded by a compile that stamps the render revision,
///     so a genuine first-plan reaches `apply()` with a matching render
///     revision, not this arm.
fn plan_reuse_is_current(plan_revision: Option<&str>, render_revision: Option<&str>) -> bool {
    match (plan_revision, render_revision) {
        (Some(plan_rev), Some(render_rev)) => plan_rev == render_rev,
        (None, _) => false,
        (Some(_), None) => false,
    }
}

// ── StateBackendAsync — adapter into magma-operator-backend ────────

/// Wraps the operator's `StateBackend` (which is keyed by the
/// `schema`/`template`/`state` triple) into a single-key
/// `AsyncStateStore` that magma-operator-backend's `OperatorBackend`
/// consumes. One of these per reconcile.
pub struct StateBackendAsync<S: StateBackend + ?Sized> {
    inner: Arc<S>,
    schema_name: String,
    template_name: String,
    state_name: String,
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
            schema_name: schema_name.into(),
            template_name: template_name.into(),
            state_name: state_name.into(),
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
            .save_state(
                &self.schema_name,
                &self.template_name,
                &self.state_name,
                bytes,
            )
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
    pub schema_name: String,
    /// Template name (CR name).
    pub template_name: String,
    /// State name (workspace state slot, typically "default").
    pub state_name: String,
    /// On-disk encoding for the state bytes. `Magma` (default) is
    /// the typed magma format; `Tofu` is the OpenTofu-readable
    /// format (use this when the same state is read by both
    /// backends).
    pub backend_shape: BackendShape,
    /// Whether to write a typed plan checkpoint to the workspace
    /// dir between plan + apply for restart safety. Default true.
    pub plan_checkpoint: bool,
    /// Bounded work per apply cycle — MAGMA-OPERATOR-BACKEND.md §II-ter /
    /// M0.16. `Some(secs)` runs apply through the resumable engine, yielding
    /// an `ApplyCursor` when the quantum is spent; `None` runs to completion
    /// exactly as before.
    ///
    /// **This is a scheduling quantum, not a deadline.** Running out of it is
    /// `CycleOutcome::Yielded` — a modelled transition, not an error — so
    /// there is no such thing as a quantum too small to be feasible; a small
    /// one simply does less work per cycle. That is the whole difference from
    /// `PANGEA_TIMEOUT`, which destroys the cycle's work when it fires.
    ///
    /// Default `Some(120)`. Chosen well below the operator's own
    /// `PANGEA_TIMEOUT` (600s) so a cycle yields and *persists its frontier*
    /// long before the deadline can discard it — which is what makes progress
    /// monotonic across reconciles even while that deadline still exists.
    /// Override with `PANGEA_MAGMA_APPLY_QUANTUM_SECS`; `0` disables
    /// resumption (run-to-completion, the pre-M0.16 behaviour).
    pub apply_quantum_secs: Option<u64>,
    /// Whether to run the universal substrate law battery on the
    /// rendered Config before plan/apply. Catches malformed
    /// workspaces (dangling refs, missing providers, duplicate
    /// addresses, null outputs) at the controller layer — they
    /// never reach the live backend. Default true for production
    /// safety; tests with minimal fixtures may opt out.
    pub preflight_laws: bool,
    /// Drift classification policy. Every plan's resource changes
    /// are classified per this policy into AutoCorrect /
    /// AutoCorrectWithAlert / RequireApproval / Refuse. Plans
    /// containing changes that policy refuses or holds for
    /// approval are surfaced in stdout JSON + CR status; the
    /// reconcile loop can use that to halt or escalate. Default
    /// is `DriftPolicy::conservative_default()` (auto-fix
    /// Cosmetic, alert+auto-fix Functional, require-approval for
    /// Critical).
    pub drift_policy: magma_drift::DriftPolicy,
    /// Optional path to a JSON-lines audit log. When set, every
    /// reconcile emits typed magma-stream events (PlanComputed,
    /// DriftClassified, ApplyOutcome) into the file. The chain is
    /// BLAKE3-Merkle-linked so post-hoc audits can verify
    /// non-tampering. Captured events are also threaded into
    /// Bundle.audit so the compliance artifact carries them.
    /// None = no audit log (events emit only into the in-memory
    /// stream captured into Bundle.audit).
    pub audit_log_path: Option<PathBuf>,
    /// Optional Postgres-backed artifact store. When `Some`, the
    /// magma path is **fully DB-backed, zero-disk**: the
    /// compile-rendered config, the typed plan, and the compliance
    /// bundle are read/written through Postgres, and NOTHING durable
    /// touches the pod-local workspace dir on the magma path. The
    /// post-apply state + bundle land together in ONE transaction
    /// (`put_apply_result`). When `None`, the executor falls back to
    /// the disk behavior (`main.tf.json` / `magma-plan.json` /
    /// `magma-bundle.json` in `work_dir`) — this keeps DB-less unit
    /// tests (InMemoryStateBackend, no `PgPool`) compiling + green.
    /// Per the org ★★ MAGMA-NATIVE EXECUTION directive +
    /// `theory/MAGMA-OPERATOR-BACKEND.md` §II-bis.
    pub artifact_store: Option<Arc<crate::backend::ArtifactStore>>,
    /// Provider config-objects resolved from
    /// `spec.providerCredentials`, keyed by terraform provider local
    /// name (`cloudflare` → `{"api_token": …}`, `aws` → `{"region",
    /// "access_key", "secret_key", "token"?}`, `github` → `{"token",
    /// "owner"?}`).
    ///
    /// Forwarded into magma's `ApplyContext` via `with_provider_config`
    /// at `apply`/`destroy` time. WITHOUT this, magma's in-process
    /// provider-RPC apply hands a provider whose creds live ONLY in
    /// `spec.providerCredentials` (not in a rendered `provider "x" {}`
    /// block) a null config — every real RPC then fails with
    /// "Service was not ready: channel closed". The operator resolves
    /// these in the async controller layer (`provider_creds.rs`) and
    /// threads them here; the executor just forwards them.
    ///
    /// **Merge precedence with rendered-config provider blocks:** these
    /// are the BASE (authoritative credentials); a rendered
    /// `provider "x" {}` block AUGMENTS/overrides per-attribute for
    /// non-secret tuning. Default empty (no spec-cred forwarding — the
    /// rendered config / pod-env fallback alone applies, the pre-fix
    /// behavior).
    pub provider_configs: std::collections::BTreeMap<String, serde_json::Value>,
    /// TEST-ONLY: use magma's structural in-memory apply (`run_plan`)
    /// instead of the real provider-RPC apply (`run_plan_with_providers`).
    /// Production leaves this `false` — apply/destroy drive real provider
    /// binaries over gRPC. Unit tests (InMemoryStateBackend, no provider
    /// binaries available) set it `true` so the plan→apply→state→destroy
    /// plumbing + lifecycle FSM are exercised without spawning a real
    /// provider (which CI has none of — that path is integration-tested).
    pub structural_apply: bool,
}

impl<S: StateBackend + ?Sized> Default for MagmaExecutorConfig<S>
where
    Arc<S>: Default,
{
    fn default() -> Self {
        Self {
            state_backend: Default::default(),
            schema_name: "default".into(),
            template_name: "default".into(),
            state_name: "default".into(),
            backend_shape: BackendShape::Magma,
            plan_checkpoint: true,
            apply_quantum_secs: Some(120),
            preflight_laws: true,
            drift_policy: magma_drift::DriftPolicy::conservative_default(),
            audit_log_path: None,
            artifact_store: None,
            provider_configs: std::collections::BTreeMap::new(),
            structural_apply: false,
        }
    }
}

impl<S: StateBackend + ?Sized> Clone for MagmaExecutorConfig<S> {
    fn clone(&self) -> Self {
        Self {
            state_backend: Arc::clone(&self.state_backend),
            schema_name: self.schema_name.clone(),
            template_name: self.template_name.clone(),
            state_name: self.state_name.clone(),
            backend_shape: self.backend_shape,
            plan_checkpoint: self.plan_checkpoint,
            apply_quantum_secs: self.apply_quantum_secs,
            preflight_laws: self.preflight_laws,
            drift_policy: self.drift_policy.clone(),
            audit_log_path: self.audit_log_path.clone(),
            artifact_store: self.artifact_store.clone(),
            provider_configs: self.provider_configs.clone(),
            structural_apply: self.structural_apply,
        }
    }
}

/// Magma-backed `IacExecutor`. Drives `magma_plan::plan` +
/// `magma_apply::engine::run_plan_with_providers` (real provider-RPC
/// apply) in-process. No subprocess.
pub struct MagmaExecutor<S: StateBackend + ?Sized> {
    cfg: MagmaExecutorConfig<S>,
}

/// Durable sink for apply progress, backed by the operator's Postgres
/// artifact store. MAGMA-OPERATOR-BACKEND.md §II-ter / M0.16.
///
/// # Why this is debounced, and why that is safe
///
/// `CheckpointSink::checkpoint` is called **after every applied node and
/// every newly-cached data-source read**. Writing on each call would be
/// O(N) writes — and because a checkpoint carries the whole `State`, each
/// write is O(N) bytes, so the total is **O(N²)**. For the 2,478-resource
/// workspace that motivated §II-ter that is on the order of a hundred
/// gigabytes of Postgres traffic to protect a four-hour apply: a "safety"
/// mechanism that would itself be the bottleneck.
///
/// So writes are throttled to at most one per [`Self::MIN_INTERVAL`]. The
/// cost becomes O(duration / interval) writes — bounded by *time*, not by
/// resource count, which is the property that has to hold for this to scale.
///
/// Skipping a write is safe in a way that skipping is usually not, and the
/// reason is specific rather than hand-waved: an interruption between two
/// persisted checkpoints loses the *cursor entries* for the nodes applied in
/// between, so those nodes are re-applied on resume. magma's cursor states
/// that its skip predicate is **safety-monotone** — losing entries can only
/// cause *more* re-application, never *less*. Re-applying an already-applied
/// change is the idempotent case the provider protocol already handles;
/// *skipping* an unapplied one would not be. The debounce can therefore only
/// cost work, never correctness.
///
/// The debounce sets *granularity*, not durability of the final answer: the
/// apply loop writes the cursor exactly at each yield boundary regardless of
/// this throttle, so a cycle that ends always records its true frontier.
struct StoreCheckpointSink {
    store: Arc<crate::backend::ArtifactStore>,
    schema: String,
    template: String,
    debounce: Debounce,
}

/// Rate-limits an event to at most one per interval.
///
/// Split out from [`StoreCheckpointSink`] so the decision logic is testable
/// without a live Postgres — the sink itself needs an `ArtifactStore`, which
/// needs a `PgPool`, which a unit test has no business standing up. The
/// property under test (never two writes inside one interval, always a write
/// once the interval has passed) is exactly what bounds checkpoint traffic to
/// O(duration/interval) instead of O(resource-count).
struct Debounce {
    /// Instant of the last permitted event. `Mutex` rather than atomics
    /// because the pair (decide, stamp) must be one critical section — two
    /// concurrent checkpoints must not both observe a stale instant and both
    /// write. `std::sync::Mutex` is correct here: the guard is dropped before
    /// any `.await`.
    last: StdMutex<Option<Instant>>,
    min: std::time::Duration,
}

impl Debounce {
    fn new(min: std::time::Duration) -> Self {
        Self {
            last: StdMutex::new(None),
            min,
        }
    }

    /// Whether enough time has passed to permit the event, stamping the clock
    /// if so. One critical section; the guard is dropped before returning.
    fn allow(&self) -> bool {
        let now = Instant::now();
        let mut last = match self.last.lock() {
            Ok(g) => g,
            // A poisoned mutex means another checkpoint panicked. Permit
            // rather than skip: erring toward durability is the safe
            // direction, and the debounce is an optimization, not a
            // correctness mechanism.
            Err(p) => p.into_inner(),
        };
        match *last {
            Some(prev) if now.duration_since(prev) < self.min => false,
            _ => {
                *last = Some(now);
                true
            }
        }
    }
}

impl StoreCheckpointSink {
    /// Minimum wall-clock gap between persisted checkpoints.
    ///
    /// The quantity this trades is "how much work an interruption re-does":
    /// at most one interval's worth. Five seconds keeps that re-do small
    /// while bounding writes to ~12/minute regardless of whether the
    /// workspace holds 10 resources or 10,000.
    const MIN_INTERVAL: std::time::Duration = std::time::Duration::from_secs(5);

    fn new(store: Arc<crate::backend::ArtifactStore>, schema: String, template: String) -> Self {
        Self {
            store,
            schema,
            template,
            debounce: Debounce::new(Self::MIN_INTERVAL),
        }
    }
}

#[async_trait]
impl magma_apply::checkpoint::CheckpointSink for StoreCheckpointSink {
    async fn checkpoint(
        &self,
        _state: &magma_types::State,
        cursor: &magma_apply::cursor::ApplyCursor,
    ) -> std::result::Result<(), magma_apply::checkpoint::CheckpointError> {
        if !self.debounce.allow() {
            return Ok(());
        }
        self.store
            .put_apply_cursor(&self.schema, &self.template, cursor)
            .await
            .map(|_| ())
            // A failed checkpoint stops the apply by contract — the engine
            // will not continue growing the set of undurable work. Surface
            // the cause; do not swallow it into a skip.
            .map_err(|e| {
                magma_apply::checkpoint::CheckpointError::new(format!(
                    "persist apply cursor ({}/{}): {e}",
                    self.schema, self.template
                ))
            })
    }
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

    /// Provision the OpenTofu pg-backend `<schema>_<template>_states.states`
    /// table BEFORE any state read. With `PANGEA_FORBID_TOFU=true`, `tofu init`
    /// (which would `CREATE SCHEMA … states`) never runs, so a brand-new
    /// template has no `.states` table and the very first `read_state` fails
    /// "relation does not exist" — even though an absent state IS simply an
    /// empty state (a first plan = all-creates). Idempotent (`IF NOT EXISTS`);
    /// a no-op once the table exists and a no-op on the in-memory path (no
    /// artifact store). Mirrors the same ensure already done on the WRITE path
    /// (`put_apply_result`) — the read path needed it too.
    async fn ensure_state_table(&self) -> Result<()> {
        if let Some(store) = self.cfg.artifact_store.as_ref() {
            store
                .ensure_tofu_states_table(&self.cfg.schema_name, &self.cfg.template_name)
                .await?;
        }
        Ok(())
    }

    /// Read the typed plan the plan phase persisted, **from the DB-backed
    /// artifact store only** — ZERO filesystem.
    ///
    /// Returns a typed three-state [`DbPlanRead`] so the caller can tell a
    /// genuinely-MISSING-OR-STALE plan row (a cache-miss → regenerate)
    /// apart from a torn/corrupt one (a hard `Err`) and from the
    /// non-DB-backed path:
    ///
    ///   * `DbPlanRead::Present(plan)` — DB-backed, plan row present AND
    ///     current (its `source_revision` matches the render's).
    ///   * `DbPlanRead::Missing`       — DB-backed, **no plan row OR a
    ///     STALE one** (the row was never written, was deleted, or its
    ///     `source_revision` no longer matches the render currently in the
    ///     store — the plan was computed from an older revision). This is a
    ///     CACHE-MISS, not an error: `apply()` / `planned_changes()`
    ///     regenerate the plan rather than dead-ending or applying a stale
    ///     (noOp) plan. It mirrors the compile-cache-miss path
    ///     (recompute-not-fail). Previously an absent row surfaced as a
    ///     LOUD `Err(MagmaExecution)` (→ STUCK until a pod restart) and a
    ///     stale-but-present row was applied blindly (a source change
    ///     silently never converged — the stale-plan class this fixes).
    ///   * `DbPlanRead::NotDbBacked`   — no `artifact_store` (disk-fallback
    ///     / unit-test path). The caller reads the disk checkpoint (apply)
    ///     or routes to the legacy `show_plan` path (planned_changes).
    ///
    /// A TORN/CORRUPT row is NOT one of these states: `get_plan` re-verifies
    /// the stored blob's BLAKE3 content hash and returns `Err` on mismatch,
    /// which propagates through the `?` below unchanged (a corrupt row must
    /// surface loudly — regenerating over garbage would mask real
    /// data-integrity failures).
    ///
    /// Dedups the DB-plan read shared by `apply()` and `planned_changes()`.
    async fn read_db_plan(&self) -> Result<DbPlanRead> {
        match &self.cfg.artifact_store {
            Some(store) => {
                // `?` propagates a torn/corrupt row as a hard Err (BLAKE3
                // mismatch / serde failure). `Ok(None)` = genuinely absent
                // = a cache-miss the caller regenerates from.
                let Some(plan) = store
                    .get_plan(&self.cfg.schema_name, &self.cfg.template_name)
                    .await?
                else {
                    return Ok(DbPlanRead::Missing);
                };

                // Revision gate (sibling of the controller's rendered_config
                // reuse gate): a stored plan is reusable ONLY if the revision
                // it was computed from still matches the render currently in
                // the store. The render fix keeps the render's revision
                // current on a moved source HEAD / changed inline content; a
                // plan whose recorded revision is older was computed from a
                // stale render → applying it re-executes a noOp and a source
                // change (org.yaml) silently never converges. A stale (or
                // legacy/NULL-revision) plan is a CACHE-MISS the caller
                // regenerates from — not a hard error, not a stale apply.
                let plan_revision = store
                    .get_plan_revision(&self.cfg.schema_name, &self.cfg.template_name)
                    .await?;
                let render_revision = store
                    .get_rendered_config_revision(&self.cfg.schema_name, &self.cfg.template_name)
                    .await?;
                if plan_reuse_is_current(plan_revision.as_deref(), render_revision.as_deref()) {
                    Ok(DbPlanRead::Present(plan))
                } else {
                    tracing::info!(
                        schema = %self.cfg.schema_name,
                        template = %self.cfg.template_name,
                        plan_revision = plan_revision.as_deref().unwrap_or("(none)"),
                        render_revision = render_revision.as_deref().unwrap_or("(none)"),
                        "read_db_plan: cached plan is stale relative to the current render \
                         (revision mismatch) — treating as a cache-miss so the plan recomputes"
                    );
                    Ok(DbPlanRead::Missing)
                }
            }
            None => Ok(DbPlanRead::NotDbBacked),
        }
    }

    /// Run the plan through magma's **resumable** apply engine, resuming from
    /// any durably-recorded frontier and persisting a new one each time a
    /// cycle yields. MAGMA-OPERATOR-BACKEND.md §II-ter / M0.16.
    ///
    /// # Why this exists
    ///
    /// The pre-M0.16 call was
    /// `run_plan_with_providers(&plan, &mut state, &ctx)`, whose own comment
    /// reads *"No quantum ⇒ no yield point ⇒ `Completed` is the only
    /// reachable arm."* — i.e. it is the resumable engine with resumption
    /// switched off. With it off, a cycle killed by `PANGEA_TIMEOUT` (or a
    /// pod roll) threw away everything it had done and the next reconcile
    /// re-ran the plan from the beginning, **re-spending the provider's
    /// rate-limit budget on work already applied**. That re-spend is why a
    /// sufficiently large workspace could never converge: each attempt
    /// restarts the same prefix and never reaches the tail.
    ///
    /// With resumption on, progress is **monotonic across reconciles** — each
    /// cycle starts from the frontier the last one left, so N cycles converge
    /// for any N. Note this holds *even while `PANGEA_TIMEOUT` still exists*:
    /// the deadline can still end a cycle, but it can no longer destroy the
    /// work that cycle completed. Removing the deadline outright, and
    /// yielding at the *reconcile* level so other workspaces get a turn, is
    /// M0.16b — deliberately not in this increment, because it edits the
    /// CI-guarded `lifecycle::TRANSITIONS` table.
    ///
    /// # Loop, not one shot
    ///
    /// This keeps looping while the engine yields, so the observable contract
    /// of `apply()` is unchanged (it still returns a completed `ApplyOutcome`
    /// or errors). What changes is that the frontier is durable *between*
    /// iterations, so an interruption at any point resumes rather than
    /// restarts.
    ///
    /// `Stalled` is a hard error on purpose: it means the quantum cannot
    /// cover the cycle's fixed prologue, so retrying unchanged cannot
    /// converge. Silently looping on it is the exact naive-chunking failure
    /// magma's `CycleOutcome` split it out to make impossible.
    async fn run_apply_resumable(
        &self,
        plan: &magma_types::Plan,
        state: &mut magma_types::State,
        ctx: &magma_apply::engine::ApplyContext,
    ) -> Result<magma_apply::ApplyOutcome> {
        use magma_apply::cursor::{CycleOutcome, Quantum};

        // `None`/0 ⇒ no quantum ⇒ no yield point ⇒ exactly the pre-M0.16
        // run-to-completion path, byte-for-byte. Resumption is opt-out.
        let quantum = self.cfg.apply_quantum_secs.and_then(Quantum::from_secs);
        let Some(quantum) = quantum else {
            return Ok(magma_apply::engine::run_plan_with_providers(plan, state, ctx).await);
        };

        // Only the DB-backed path can persist a frontier. Without a store
        // there is nowhere durable to put a cursor, so resumption would be a
        // lie — fall back to run-to-completion rather than pretend.
        let Some(store) = self.cfg.artifact_store.clone() else {
            return Ok(magma_apply::engine::run_plan_with_providers(plan, state, ctx).await);
        };

        let sink = StoreCheckpointSink::new(
            store.clone(),
            self.cfg.schema_name.clone(),
            self.cfg.template_name.clone(),
        );

        // Resume from a durable frontier if one exists AND belongs to THIS
        // plan. `ApplyCursor::resume` compares the cursor's BLAKE3 `PlanId`
        // against this plan's, so a cursor left by a different plan is
        // rejected here rather than silently skipping the wrong changes.
        let mut carried = store
            .get_apply_cursor(&self.cfg.schema_name, &self.cfg.template_name)
            .await?;

        let mut cycles: u32 = 0;
        loop {
            cycles += 1;
            let resume = carried.as_ref().and_then(|c| c.resume(plan));
            if cycles == 1 {
                tracing::info!(
                    schema = %self.cfg.schema_name,
                    template = %self.cfg.template_name,
                    quantum_secs = quantum.as_duration().as_secs(),
                    resuming = resume.is_some(),
                    "magma apply: resumable engine"
                );
            }

            let outcome = magma_apply::engine::run_plan_with_providers_resumable(
                plan,
                state,
                ctx,
                resume,
                Some(quantum),
                Some(&sink),
            )
            .await;

            match outcome {
                CycleOutcome::Completed { outcome, stats } => {
                    tracing::info!(
                        schema = %self.cfg.schema_name,
                        template = %self.cfg.template_name,
                        cycles,
                        ?stats,
                        "magma apply: completed"
                    );
                    return Ok(outcome);
                }
                CycleOutcome::Yielded {
                    cursor,
                    progress,
                    stats,
                    ..
                } => {
                    // Persist the frontier before continuing. The sink has
                    // already been writing it mid-cycle (debounced); this is
                    // the exact-at-the-boundary write, so an interruption
                    // between cycles resumes from the true frontier rather
                    // than the last debounced one.
                    store
                        .put_apply_cursor(&self.cfg.schema_name, &self.cfg.template_name, &cursor)
                        .await?;
                    tracing::info!(
                        schema = %self.cfg.schema_name,
                        template = %self.cfg.template_name,
                        cycles,
                        ?progress,
                        ?stats,
                        "magma apply: yielded — frontier persisted, continuing"
                    );
                    carried = Some(cursor);
                }
                CycleOutcome::Stalled {
                    cursor,
                    stats,
                    partial,
                } => {
                    // Persist even here: the frontier is real work, and the
                    // operator should resume from it once the prologue or the
                    // quantum changes.
                    store
                        .put_apply_cursor(&self.cfg.schema_name, &self.cfg.template_name, &cursor)
                        .await?;
                    return Err(Error::MagmaExecution(format!(
                        "apply stalled after {cycles} cycle(s): {}. stats={stats:?}",
                        diagnose_stall(&stats, &partial.failed)
                    )));
                }
            }
        }
    }

    fn bundle_checkpoint_path(work_dir: &Path) -> PathBuf {
        work_dir.join("magma-bundle.json")
    }

    fn plan_checkpoint_path(work_dir: &Path) -> PathBuf {
        work_dir.join("magma-plan.json")
    }

    async fn load_config(work_dir: &Path) -> Result<magma_config::Config> {
        let path = work_dir.join("main.tf.json");
        let bytes = tokio::fs::read(&path).await.map_err(Error::Io)?;
        let value: serde_json::Value = serde_json::from_slice(&bytes)
            .map_err(|e| Error::MagmaExecution(format!("parse main.tf.json: {e}")))?;
        Self::load_config_from_value(value)
    }

    /// Build a `magma_config::Config` directly from an in-memory JSON
    /// value, skipping the disk round-trip.
    ///
    /// The in-process Ruby compile (`pangea-ruby-eval`) produces the
    /// workspace's synthesis as `serde_json::Value` BEFORE
    /// owner.rs writes it to `main.tf.json` for tofu compatibility +
    /// debugging. For pure-magma paths (preview, smoke validation,
    /// future GraphQL workspace-preview RPC) the file write is
    /// gratuitous — magma's `Config::from_json` only needs the value.
    ///
    /// Sister to `load_config`: same final type, no I/O. Both routes
    /// share the same parse error surface so callers see consistent
    /// `Error::MagmaExecution` regardless of source.
    ///
    /// This is the first primitive of the in-memory exploit
    /// (theory/IN-MEMORY-PIPELINE.md): both Ruby + magma live in
    /// process, so workspace preview, equivalence tests, and future
    /// customer-facing previews can run the full compile→plan
    /// pipeline without touching disk or k8s state.
    pub fn load_config_from_value(value: serde_json::Value) -> Result<magma_config::Config> {
        magma_config::Config::from_json(value)
            .map_err(|e| Error::MagmaExecution(format!("magma_config: {e}")))
    }

    /// Load the rendered terraform `Config` the way the configured
    /// substrate dictates: from Postgres when an `artifact_store` is
    /// wired (the DB-backed, zero-disk path — the compile phase
    /// persisted the rendered JSON via `put_rendered_config`), else
    /// from `work_dir/main.tf.json` (the disk fallback that keeps
    /// DB-less unit tests + the tofu-compat debug file working).
    ///
    /// A `Some(store)` that has no rendered-config row yet is a typed
    /// error, not a silent disk fallthrough: on the DB path the compile
    /// phase MUST have persisted the config before plan/apply runs.
    async fn load_config_routed(&self, work_dir: &Path) -> Result<magma_config::Config> {
        match &self.cfg.artifact_store {
            Some(store) => {
                let value = store
                    .get_rendered_config(&self.cfg.schema_name, &self.cfg.template_name)
                    .await?
                    .ok_or_else(|| {
                        Error::MagmaExecution(format!(
                            "no rendered config in artifact store for {}/{}; \
                             the compile phase must persist it before plan/apply",
                            self.cfg.schema_name, self.cfg.template_name
                        ))
                    })?;
                Self::load_config_from_value(value)
            }
            None => Self::load_config(work_dir).await,
        }
    }

    /// Encode a magma `State` into the durable on-disk byte form that
    /// matches the configured `BackendShape` — identical to what
    /// `OperatorBackend::write_state` would persist. `Tofu` shape →
    /// `magma_to_tofu` (cross-executor-readable JSON, the production
    /// default); `Magma` shape → typed serde JSON. Used by the atomic
    /// apply op so the state row written inside the transaction is
    /// byte-identical to a non-atomic `write_state`.
    fn encode_state_bytes(&self, state: &magma_types::State) -> Result<Vec<u8>> {
        match self.cfg.backend_shape {
            BackendShape::Tofu => magma_operator_backend::magma_to_tofu(state)
                .map_err(|e| Error::MagmaExecution(format!("encode tofu state: {e}"))),
            BackendShape::Magma => serde_json::to_vec_pretty(state)
                .map_err(|e| Error::MagmaExecution(format!("encode magma state: {e}"))),
        }
    }

    /// Build the per-provider `ApplyContext` config map for an
    /// apply/destroy, folding two sources:
    ///
    ///   1. **rendered-config provider blocks** (`cfg.providers`) — the
    ///      `provider "<name>" { .. }` blocks the Ruby DSL emitted.
    ///   2. **`spec.providerCredentials`** (`self.cfg.provider_configs`)
    ///      — credentials the operator resolved from k8s Secrets in the
    ///      controller layer.
    ///
    /// **Merge precedence:** `spec.providerCredentials` is the BASE
    /// (authoritative credentials); a rendered block AUGMENTS/overrides
    /// per-attribute for non-secret tuning, but never erases a real base
    /// credential with a null/empty value (see
    /// `provider_creds::merge_provider_config`). A provider present in
    /// only one source passes through unchanged.
    ///
    /// This closes the credential-drop CLASS: any provider whose creds
    /// live in `spec.providerCredentials` reaches magma regardless of
    /// whether the renderer emits a `provider "<name>" {}` block (rio
    /// renders none for cloudflare, which is why the live apply hit
    /// "channel closed").
    /// Share of the credential's hourly budget this operator may spend.
    /// The remainder is left for everything else on that credential — CI,
    /// releases, humans. Measured 2026-08-08: with no share at all the
    /// operator's convergence consumed the whole 5,000 req/hr GitHub budget
    /// and the org's release pipeline failed three times with "API rate limit
    /// exceeded". Override with `PANGEA_PACER_QUOTA_PCT`.
    fn pacer_quota_pct() -> f64 {
        std::env::var("PANGEA_PACER_QUOTA_PCT")
            .ok()
            .and_then(|v| v.parse::<f64>().ok())
            .filter(|v| *v > 0.0 && *v <= 1.0)
            .unwrap_or(0.7)
    }

    /// The ONE pacer for this template's credential set, or `None` to keep
    /// magma's per-context default.
    ///
    /// Scoped deliberately to GitHub. A rate ceiling is a per-provider fact,
    /// so applying GitHub's 5,000 req/hr to an AWS workspace would throttle it
    /// for no reason; every non-GitHub provider keeps exactly today's
    /// behaviour. Widening this means adding the provider's real limit, not
    /// reusing this one.
    ///
    /// Keyed by a hash of the resolved provider configs — which carry the
    /// credential — so two templates sharing a token share a bucket, and two
    /// holding different tokens do not. Keying by template or workspace would
    /// hand each caller its own bucket, pacing each one while multiplying the
    /// credential's real ceiling by the caller count: pangea-operator builds
    /// an ApplyContext at four sites, and the pleme-io-opensource estate is
    /// five shards, so the ceiling was ~18,000 req/hr against a 5,000 budget.
    fn shared_pacer_for(
        configs: &std::collections::BTreeMap<String, serde_json::Value>,
    ) -> Option<std::sync::Arc<magma_apply::engine::Pacer>> {
        // GitHub's documented primary budget for an authenticated user.
        const GITHUB_BUDGET_RPH: f64 = 5000.0;
        if !configs.keys().any(|k| k.starts_with("github")) {
            return None;
        }
        let bytes = serde_json::to_vec(configs).ok()?;
        let key = blake3::hash(&bytes).to_hex().to_string();
        magma_apply::engine::shared_pacer(&key, Self::pacer_quota_pct(), GITHUB_BUDGET_RPH)
    }

    fn build_provider_configs(
        &self,
        cfg: &magma_config::Config,
    ) -> std::collections::BTreeMap<String, serde_json::Value> {
        use crate::controller::template::provider_creds::merge_provider_config;
        use std::collections::BTreeMap;

        // Start from the rendered-config blocks.
        let mut rendered: BTreeMap<String, serde_json::Value> = BTreeMap::new();
        for (name, pc) in &cfg.providers {
            let fields: serde_json::Map<String, serde_json::Value> =
                pc.fields.clone().into_iter().collect();
            // `cfg.providers` is keyed by the typed `ProviderInstance` since
            // magma db764fa (provider aliases), so this plain-JSON map takes
            // its string form rather than the key itself.
            //
            // CAVEAT, recorded rather than papered over: `name()` drops the
            // alias, so two instances of one provider type differing ONLY by
            // alias collapse to the same key and the last wins. That is what
            // this map did before aliases existed (it was keyed by a bare type
            // string), so it is not a regression — but neither is it
            // alias-correct, and a multi-account estate expressed via aliases
            // is exactly what db764fa exists to support. The fix needs a
            // decision about the rendered JSON's shape, so it is not taken
            // silently here.
            rendered.insert(name.name().to_string(), serde_json::Value::Object(fields));
        }

        let mut out: BTreeMap<String, serde_json::Value> = BTreeMap::new();

        // spec.providerCredentials base, merged with any rendered block.
        for (name, base) in &self.cfg.provider_configs {
            let merged = merge_provider_config(base, rendered.get(name));
            out.insert(name.clone(), merged);
        }

        // Rendered-only providers (no spec.providerCredentials entry)
        // pass through unchanged.
        for (name, value) in rendered {
            out.entry(name).or_insert(value);
        }

        out
    }

    /// Resolve the provider-config map an `apply()`/`destroy()` call
    /// forwards into its `ApplyContext`, routing rendered-config load
    /// failures to the base `spec.providerCredentials` map instead of an
    /// empty one. `load_config_routed` failing (a transient artifact-store
    /// read, an evicted DB row between the plan-phase load and this
    /// independent reload) must never silently drop `self.cfg.provider_configs`
    /// — that base map lives in memory already and needs no rendered
    /// config to be forwarded. Doing so used to leave a real mutating RPC
    /// (an `AttachRolePolicy`, a `DestroyResource`) to fall back to the
    /// provider's own ambient credential chain (pod env / EC2 instance
    /// role) — the exact class `build_provider_configs`'s own doc comment
    /// names for the credential-blind-executor incident, reached here
    /// through a second, un-audited door. `phase` is `"apply"`/`"destroy"`,
    /// forwarded into the error log so a stuck-empty-context incident is
    /// traceable to which call path hit it.
    async fn provider_configs_for(
        &self,
        work_dir: &Path,
        phase: &'static str,
    ) -> std::collections::BTreeMap<String, serde_json::Value> {
        match self.load_config_routed(work_dir).await {
            Ok(cfg) => self.build_provider_configs(&cfg),
            Err(e) => {
                tracing::error!(
                    error = %e,
                    phase,
                    schema = %self.cfg.schema_name,
                    template = %self.cfg.template_name,
                    "load_config_routed failed -- falling back to \
                     spec.providerCredentials base only (no rendered \
                     provider-block augmentation); this must never silently \
                     drop credentials to the provider's ambient fallback"
                );
                self.cfg.provider_configs.clone()
            }
        }
    }

    /// The routing decision `plan()` makes before calling
    /// `magma_apply::engine::refresh_then_plan`: build the real
    /// `ApplyContext` a plan-time refresh needs, or `None` to skip
    /// refresh entirely. Isolated into its own method (rather than
    /// inlined in `plan()`) specifically so the routing decision is
    /// unit-testable without dialing a real provider binary — see
    /// `refresh_ctx_for_*` in the test module below.
    ///
    /// * `structural_apply` (TEST-ONLY: CI has no provider binaries) ⇒
    ///   `None` — `refresh_then_plan(cfg, state, None)` behaves exactly
    ///   like the pre-fix bare `magma_plan::plan(cfg, state)` call, so
    ///   every existing `structural_apply: true` unit test in this file
    ///   keeps its old behavior unchanged.
    /// * production (`structural_apply: false`) ⇒ `Some`, built the
    ///   SAME way `apply()`/`destroy()` build their own `ApplyContext`
    ///   (`build_provider_configs`, folding `spec.providerCredentials`
    ///   with the rendered config's own `provider "x" {}` blocks) — a
    ///   refresh `ReadResource` RPC needs the same real provider
    ///   credentials a mutating apply does.
    fn refresh_ctx_for(
        &self,
        work_dir: &Path,
        cfg: &magma_config::Config,
    ) -> Option<magma_apply::engine::ApplyContext> {
        if self.cfg.structural_apply {
            return None;
        }
        let mut ctx = magma_apply::engine::ApplyContext::new(work_dir.to_path_buf());
        for (name, value) in self.build_provider_configs(cfg) {
            ctx = ctx.with_provider_config(name, value);
        }
        // ONE pacer per credential, shared across every ApplyContext this
        // process builds. Without it each context paced independently and
        // the credential's real ceiling was multiplied by the context count
        // — four sites here, times five shards on pleme-io-opensource.
        if let Some(p) = Self::shared_pacer_for(&self.build_provider_configs(cfg)) {
            ctx = ctx.with_shared_pacer(p);
        }
        Some(ctx)
    }
}

fn ok_tofu_result(stdout: String, started: Instant) -> TofuResult {
    TofuResult {
        exit_code: 0,
        stdout,
        stderr: String::new(),
        success: true,
        duration: started.elapsed(),
        failed_changes: Vec::new(),
    }
}

/// Emit typed magma-stream events (PlanComputed, DriftClassified,
/// optional ApplyOutcome) into:
/// * an in-process `InMemorySink` (captured into the returned
///   Vec<Event> + threaded into Bundle.audit)
/// * an optional `JsonLinesSink` at `audit_log_path` (durable
///   on-disk audit log, BLAKE3-chained per magma_stream's contract)
///
/// Returns the captured event chain — every event's hash is
/// linked to the previous (`prev_hash`) so the consumer can
/// verify chain integrity with `magma_stream::verify_chain`.
async fn emit_stream_events(
    audit_log_path: &Option<PathBuf>,
    plan: &magma_converge::Plan,
    drift: &magma_drift::DriftReport,
    outcome: Option<&magma_converge::Outcome>,
) -> Vec<magma_stream::Event> {
    use std::sync::Arc;

    let in_mem = Arc::new(magma_stream::InMemorySink::new("audit_capture"));
    let mut stream = magma_stream::PlanStream::new();
    stream.register(in_mem.clone());
    if let Some(path) = audit_log_path.as_ref() {
        stream.register(Arc::new(magma_stream::JsonLinesSink::new(
            "audit_log_jsonl",
            path,
        )));
    }
    // PlanComputed
    stream.emit_plan("terraform", plan).await;
    // DriftClassified
    stream.emit_drift(drift).await;
    // ApplyOutcome (apply-stage only)
    if let Some(o) = outcome {
        stream.emit_outcome(o).await;
    }
    in_mem.events()
}

/// If the workspace ships a `Gemfile.lock`, parse it through
/// magma-rubygems + return the BLAKE3 attestation of the gem
/// closure. None when the workspace has no Gemfile.lock or when
/// parsing fails (operator continues without gem-tree attestation;
/// the bundle just lacks the field).
///
/// Once magma-rubygems M5 materializes the gem tree end-to-end,
/// this helper produces the live `VirtualGemTree::attestation`;
/// today it produces the lockfile-only attestation as a
/// forward-compatible bridge.
async fn compute_gem_tree_attestation(work_dir: &Path) -> Option<String> {
    // NOTE (zero-disk invariant): reading `Gemfile.lock` here is
    // workspace *input* acquisition — the same sanctioned filesystem
    // class as loading the gRPC provider-plugin binaries — NOT durable
    // operator execution state. The zero-disk invariant
    // (★★ MAGMA-NATIVE EXECUTION) covers execution state (rendered
    // config / plan / bundle / tofu state), which on the magma path all
    // live in Postgres now. The lockfile is an input the compiler/source
    // tree provides; attesting it does not write durable state to disk.
    let gemfile_lock = work_dir.join("Gemfile.lock");
    if !gemfile_lock.exists() {
        return None;
    }
    #[cfg(feature = "magma_rubygems")]
    {
        let source = tokio::fs::read_to_string(&gemfile_lock).await.ok()?;
        let lock = magma_rubygems::lockfile::parse(&source).ok()?;
        Some(magma_rubygems::attestation::attest_lockfile(&lock))
    }
    // Without the feature there is no gem attester linked, and there is
    // nothing to fall back to — this returns None rather than a wrong
    // answer. A build that omits `magma_rubygems` is one targeting an
    // environment that cannot run Ruby at all, where a Gemfile.lock in
    // the workspace is already anomalous.
    #[cfg(not(feature = "magma_rubygems"))]
    {
        tracing::debug!(
            path = %gemfile_lock.display(),
            "Gemfile.lock present but this build omits magma_rubygems; not attested"
        );
        None
    }
}

/// Convert a `magma_types::Plan` (Terraform-shape, granular Action
/// surface) into a `magma_converge::Plan` (universal shape with
/// derived severity). Lets the operator route any
/// magma-plan-produced plan through magma-drift's classifier
/// without touching the Reconciler trait machinery.
fn to_universal_plan(plan: &magma_types::Plan) -> magma_converge::Plan {
    let changes: Vec<magma_converge::Change> = plan
        .resource_changes
        .iter()
        .map(|rc| {
            let address = rc.address.to_string();
            let action = match rc.action {
                magma_types::Action::Create => magma_converge::Action::Create,
                magma_types::Action::Update => magma_converge::Action::Update,
                magma_types::Action::Delete => magma_converge::Action::Delete,
                magma_types::Action::Replace => magma_converge::Action::Replace,
                magma_types::Action::NoOp => magma_converge::Action::NoOp,
                magma_types::Action::Read => magma_converge::Action::NoOp,
                magma_types::Action::Forget => magma_converge::Action::Delete,
                magma_types::Action::CreateThenDelete => magma_converge::Action::Replace,
                magma_types::Action::DeleteThenCreate => magma_converge::Action::Replace,
            };
            magma_converge::change(address, action, rc.before.clone(), rc.after.clone())
        })
        .collect();
    magma_converge::Plan::new("terraform", changes)
}

/// Drive `lifecycle` through `Applying -> Verifying -> final_phase`,
/// first resetting through the one path `magma_fsm`'s static transition
/// table guarantees is legal from EVERY phase: `X -> Idle -> Planning`
/// (the unconditional `(_, Idle) => true` catch-all + the unconditional
/// `(Idle, Planning)` / `(Planning, Applying)` arms — see the vendored
/// `magma-fsm::is_transition_allowed` table). The reset is skipped when
/// `current` is already `Planning`/`Approving`, the two phases that can
/// enter `Applying` directly.
///
/// BUG THIS CLOSES: `MagmaExecutor::apply()` can legitimately run more
/// than once per reconcile tick — the stale-plan self-heal retry
/// (`template_controller`) and the conflict-resolution import+re-apply
/// loop (`controller::conflict::resolve_conflicts_post_apply`) both call
/// `apply()` again within the same tick. Each call re-loads the PRIOR
/// call's persisted bundle and its `lifecycle.current`, which can
/// legally be `Stable`, `Failed`, `Verifying`, `Retrying`, or (after a
/// crash-restart) `Applying`/`Refused` — none of which
/// `magma_fsm::is_transition_allowed` permits transitioning directly
/// into `Applying` from. Before this fix, the three transition attempts
/// were silently discarded via `let _ =`, so e.g. a `Failed`-phase
/// bundle made every subsequent transition illegal + silently dropped,
/// freezing `lifecycle.current` at `"failed"` forever even when a later
/// retry in the SAME reconcile tick actually converged. That stale
/// value flows straight into `status.lastCycle.lifecyclePhase` — the
/// operator's primary "declare and observe" surface.
///
/// Tier: only-mitigated. The reset makes every `.transition()` call
/// below provably succeed against the CURRENT `magma_fsm` transition
/// graph (verified by reading `magma-fsm/src/lib.rs`), but `magma_fsm`
/// is an external crate this repo doesn't own, so the guarantee is a
/// runtime invariant, not a compile-time one. Any future divergence
/// between this invariant and magma-fsm's real graph now surfaces as a
/// loud `tracing::error!` instead of a silently-stale status field.
fn advance_lifecycle_through_apply(
    lifecycle: &mut magma_fsm::LifecycleState,
    plan_phase_id: magma_converge::PlanId,
    final_phase: magma_fsm::Phase,
    final_reason: &str,
) {
    if !matches!(
        lifecycle.current,
        magma_fsm::Phase::Planning | magma_fsm::Phase::Approving
    ) {
        if lifecycle.current != magma_fsm::Phase::Idle {
            if let Err(e) = lifecycle.transition(
                magma_fsm::Phase::Idle,
                None,
                "magma_executor::apply (reset before re-entrant apply)",
            ) {
                tracing::error!(
                    error = %e,
                    from = ?lifecycle.current,
                    "magma_executor::apply: lifecycle reset to Idle failed — magma_fsm's \
                     transition graph no longer matches this code's invariant; \
                     lifecycle_phase in status will be stale"
                );
            }
        }
        if let Err(e) = lifecycle.transition(
            magma_fsm::Phase::Planning,
            None,
            "magma_executor::apply (synthesized planning)",
        ) {
            tracing::error!(
                error = %e,
                from = ?lifecycle.current,
                "magma_executor::apply: lifecycle transition to Planning failed — \
                 magma_fsm's transition graph no longer matches this code's invariant; \
                 lifecycle_phase in status will be stale"
            );
        }
    }
    if let Err(e) = lifecycle.transition(
        magma_fsm::Phase::Applying,
        Some(plan_phase_id.clone()),
        "magma_executor::apply",
    ) {
        tracing::error!(
            error = %e,
            from = ?lifecycle.current,
            to = "Applying",
            "magma_executor::apply: lifecycle transition failed after reset — \
             lifecycle_phase in status will be stale"
        );
    }
    if let Err(e) = lifecycle.transition(
        magma_fsm::Phase::Verifying,
        Some(plan_phase_id.clone()),
        "post-apply verification",
    ) {
        tracing::error!(
            error = %e,
            from = ?lifecycle.current,
            to = "Verifying",
            "magma_executor::apply: lifecycle transition failed after reset — \
             lifecycle_phase in status will be stale"
        );
    }
    if let Err(e) = lifecycle.transition(final_phase, Some(plan_phase_id), final_reason) {
        tracing::error!(
            error = %e,
            from = ?lifecycle.current,
            to = ?final_phase,
            "magma_executor::apply: lifecycle transition to final phase failed after reset — \
             lifecycle_phase in status will be stale"
        );
    }
}

/// Project a `magma_types::Plan` into the executor-agnostic
/// [`PlannedChange`] border for import discovery. Renders each address
/// as `{type}.{name}` — byte-identical to `to_universal_plan` (above)
/// and the tofu-format strings — so `naturalIds` / `importHints` /
/// create-set lookups never miss. No filesystem, no tofu-format channel.
///
/// Tier-honest: the address drops `module` / `key` (as the whole magma
/// path already does); keyed / nested resources are a named pre-existing
/// gap (the pleme-io-opensource `github_repository` fleet is flat).
///
/// `state` is the workspace's CURRENT state, and it is the whole reason
/// this function can hand the prepass a usable import id: a composite id's
/// parent component is an unresolved `${github_repository.<n>.attr}`
/// reference in `after`, and only state says what `<n>` is really NAMED.
/// See [`state_resolution_map`].
fn planned_changes_from_magma_plan(
    plan: &magma_types::Plan,
    state: &magma_types::State,
) -> Vec<PlannedChange> {
    use crate::executor::plan_change::{PlanAction, ResourceKindClass};
    let state_map = state_resolution_map(state);
    // What THIS PLAN will name each resource, for a parent that does not exist
    // in state yet — built by magma so the prepass and magma's own apply loop
    // cannot disagree about the shape `derive` expects (see
    // `natural_id::declared_map`). Without it, a composite id whose parent is
    // being created in the same plan falls back to the parent's RESOURCE name,
    // which every underscore-sanitized name (`tag_forge` vs `tag-forge`) gets
    // wrong — the id is then marked non-exact and adoption correctly refuses it.
    let declared_map = magma_apply::natural_id::declared_map(&plan.resource_changes);
    plan.resource_changes
        .iter()
        .map(|rc| PlannedChange {
            address: rc.address.to_string(),
            action: PlanAction::from(&rc.action),
            after: rc.after.clone(),
            kind: ResourceKindClass::from(&rc.address.kind),
            import_id: derive_import_id(rc, &state_map, &declared_map),
        })
        .collect()
}

/// An empty state — the fallback when the pre-apply state read fails.
///
/// `magma_types::State` has no `Default` (a lineage is a real identity, not
/// a zero value), so this spells out the one shape that means "I know
/// nothing": no resources, so every parent-keyed import id downgrades to
/// inexact and the prepass refuses it. Never used as a state to WRITE.
fn empty_state() -> magma_types::State {
    magma_types::State {
        version: 4,
        terraform_version: String::new(),
        serial: 0,
        lineage: uuid::Uuid::nil(),
        outputs: Default::default(),
        resources: Vec::new(),
    }
}

/// magma's apply-time resolution map — `type → {resource-name → attributes}`
/// — rebuilt from state for the PRE-apply prepass.
///
/// Shaped to match `magma_apply::engine`'s own seeding of `state_map` from
/// `state.resources` so `natural_id::derive` behaves identically on the
/// proactive path and the reactive one. Only the first instance is taken;
/// magma's own seeding does the same.
fn state_resolution_map(
    state: &magma_types::State,
) -> std::collections::HashMap<String, serde_json::Value> {
    let mut sm: std::collections::HashMap<String, serde_json::Value> =
        std::collections::HashMap::new();
    for r in &state.resources {
        let Some(inst) = r.instances.first() else {
            continue;
        };
        let entry = sm
            .entry(r.address.type_id.0.clone())
            .or_insert_with(|| serde_json::Value::Object(serde_json::Map::new()));
        if let serde_json::Value::Object(map) = entry {
            map.insert(r.address.name.clone(), inst.attributes.clone());
        }
    }
    sm
}

/// Derive a create-action's provider-native import id via magma's ONE
/// import-id catalog (`magma_apply::natural_id::{CATALOG, derive}`).
///
/// This is the load-bearing call. It replaces the operator's own
/// `bundled_natural_ids` template table, which was a second copy of the
/// same knowledge and — because its `{{ .planned.<attr> }}` substitution
/// was a raw passthrough over `after` — emitted
/// `${github_repository.banken.name}:bug` as an import id. Every such id
/// 404s, so nothing was ever adopted and the colliding creates re-failed
/// every cycle.
///
/// Non-creates get `None`: nothing else can collide, so nothing else needs
/// an adoption id. An INEXACT derivation is carried through as
/// `exact: false` rather than dropped, so the prepass can log *why* it
/// refused instead of silently finding nothing.
fn derive_import_id(
    rc: &magma_types::ResourceChange,
    state_map: &std::collections::HashMap<String, serde_json::Value>,
    declared_map: &std::collections::HashMap<String, serde_json::Value>,
) -> Option<crate::executor::plan_change::DerivedImportId> {
    use crate::executor::plan_change::DerivedImportId;
    if rc.action != magma_types::Action::Create {
        return None;
    }
    // `after` is passed as BOTH the raw and the "resolved" side on purpose:
    // the prepass runs before apply, so no reference substitution has
    // happened and there is no second, resolved view to offer. `derive`
    // reads the raw side for `ParentName` components either way — that is
    // the only side that still says WHICH parent a reference points at.
    magma_apply::natural_id::derive(rc, rc.after.as_ref(), state_map, declared_map).map(|i| {
        DerivedImportId {
            id: i.id,
            exact: i.confidence.is_exact(),
        }
    })
}

fn changes_tofu_result(stdout: String, started: Instant) -> TofuResult {
    // tofu's `-detailed-exitcode` returns 2 when changes are pending.
    // We honor the same contract so callers' `has_changes()` works.
    TofuResult {
        exit_code: 2,
        stdout,
        stderr: String::new(),
        success: true,
        duration: started.elapsed(),
        failed_changes: Vec::new(),
    }
}

fn err_tofu_result(stderr: String, started: Instant) -> TofuResult {
    TofuResult {
        exit_code: 1,
        stdout: String::new(),
        stderr,
        success: false,
        duration: started.elapsed(),
        failed_changes: Vec::new(),
    }
}

/// Like [`err_tofu_result`] but carries the structured per-resource
/// failures OUT of the apply engine so the controller's reacting FSM can
/// classify each one (theory/ANOMALY-REACTIVE-RECONCILE.md §VII.2). The
/// `stdout` still carries the count-only JSON summary for log/debug parity;
/// the structured `failed_changes` is the load-bearing border.
fn err_tofu_result_with_failures(
    stdout: String,
    started: Instant,
    failed_changes: Vec<crate::executor::tofu::FailedChangeRecord>,
) -> TofuResult {
    TofuResult {
        exit_code: 1,
        stdout,
        stderr: String::new(),
        success: false,
        duration: started.elapsed(),
        failed_changes,
    }
}

#[async_trait]
impl<S> IacExecutor for MagmaExecutor<S>
where
    S: StateBackend + ?Sized,
{
    fn name(&self) -> &'static str {
        "magma"
    }

    /// Magma owns its state backend directly via the `StateBackend`
    /// trait; the canonical pleme-io deployment wires Postgres, so
    /// report `pg/<schema>` where schema is the PG schema this
    /// executor reads/writes (one schema per pangea namespace). The
    /// schema is the load-bearing key — `pangea_state.<schema>` is
    /// the table — and is what an observer needs to query state by
    /// hand.
    fn backend_descriptor(&self) -> Option<String> {
        Some(format!("pg/{}", self.cfg.schema_name))
    }

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
        let cfg = self.load_config_routed(work_dir).await?;

        // Preflight: run the universal substrate law battery before
        // even reading state. Catches malformed workspaces
        // (dangling refs, missing providers, duplicate addresses,
        // null outputs) at the controller layer — they never reach
        // the live backend. See theory/TESTING-SUBSTRATE.md §IV.
        if self.cfg.preflight_laws {
            let violations = magma_test_laws::preflight::check_workspace_full(&cfg);
            if !violations.is_empty() {
                let summary = violations
                    .iter()
                    .map(|v| format!("{}: {}", v.law, v.message))
                    .collect::<Vec<_>>()
                    .join("; ");
                return Err(Error::MagmaExecution(format!(
                    "substrate preflight violations: {summary}"
                )));
            }
        }

        let backend = self.make_backend();
        self.ensure_state_table().await?;
        let mut state = magma_backend::Backend::read_state(&backend)
            .await
            .map_err(|e| Error::MagmaExecution(format!("read state: {e}")))?;

        // Terraform-parity plan-time refresh — the ONE place this
        // executor's `plan()` goes instead of a bare `magma_plan::plan`
        // call, so a plan cycle gets real Terraform's implicit-refresh
        // guarantee: every state instance is re-read from the live
        // provider (`ReadResource`) BEFORE it is diffed against config.
        //
        // BUG THIS CLOSES (camelot-eks live incident, ground-truthed
        // 2026-07-19 against the real Postgres state row): the import
        // path (`MagmaExecutor::import` /
        // `magma_apply::ConfiguredImportEnvironment`) writes only an
        // `ImportResourceState` STUB — id-only, no follow-up
        // `ReadResource` — so nearly every other attribute of an
        // adopted resource is recorded as JSON `null`
        // (`aws_vpc.camelot-eks-vpc`'s stored state had `cidr_block`,
        // `tags`, `enable_dns_support`, `enable_dns_hostnames`, … all
        // `null`; only `id`/`region` populated). A bare
        // `magma_plan::plan(&cfg, &state)` diffs config directly
        // against that stub, so EVERY real (non-null) config value
        // reads as a false "changed" attribute — 53 of 54 camelot-eks
        // resources planned as needing an update, when the real drift
        // was far smaller. `refresh_then_plan` backfills the stub from
        // the live provider first, so the diff runs against real
        // attributes.
        //
        // `structural_apply` (TEST-ONLY: no real provider binaries in
        // CI) skips refresh entirely — `ctx = None` behaves exactly
        // like the pre-fix bare `plan` call. In production the
        // `ApplyContext` is built the SAME way `apply()` builds its own
        // (`build_provider_configs`, folding `spec.providerCredentials`
        // with the rendered config's own `provider "x" {}` blocks) —
        // a refresh `ReadResource` RPC needs the same real provider
        // credentials a mutating apply does, and `handle_planning`
        // already constructs this executor via the credential-aware
        // `executor_runner_for_with_creds` for exactly this reason (a
        // data-source read at plan time is already a real provider
        // RPC).
        let refresh_ctx = self.refresh_ctx_for(work_dir, &cfg);
        let (plan, refresh_report) =
            magma_apply::engine::refresh_then_plan(&cfg, &mut state, refresh_ctx.as_ref())
                .await
                .map_err(|e| Error::MagmaExecution(format!("magma_plan: {e}")))?;

        // Terraform parity (mirrors magma-cli's `cmd_plan`/`cmd_apply` —
        // see `report_refresh`'s call sites there): persist the
        // refreshed state even though `plan` itself makes no other
        // change. Without this, the backfill above is transient —
        // visible only to THIS cycle's diff — and the underlying
        // Postgres state row stays the null-stub the import path wrote:
        // a resource that refresh-plans as NoOp is never touched by a
        // later `apply()` (which folds provider-returned state only for
        // CHANGED resources), so its stub would never self-heal.
        // `refresh_report` is only `Some` when refresh actually ran
        // (`structural_apply` false), matching the write's gate above.
        if refresh_report.is_some() {
            magma_backend::Backend::write_state(&backend, &state)
                .await
                .map_err(|e| Error::MagmaExecution(format!("write refreshed state: {e}")))?;
        }

        // The revision this plan was COMPUTED FROM: the source_revision of
        // the rendered_config it was derived from (loaded above via
        // load_config_routed → get_rendered_config). Stamping the plan with
        // it lets apply() reject a cached plan produced from an OLDER render
        // than the one now in the store — the sibling of the rendered_config
        // stale-render fix. On the disk-fallback path there is no artifact
        // store, so no revision to read (the disk plan is never revision-
        // gated — a single-workspace file, not a cross-cycle cache).
        let plan_source_revision: Option<String> = match &self.cfg.artifact_store {
            Some(store) => {
                store
                    .get_rendered_config_revision(&self.cfg.schema_name, &self.cfg.template_name)
                    .await?
            }
            None => None,
        };

        // Persist the typed plan so apply() (a separate reconcile)
        // picks it up. DB-backed path → Postgres (`put_plan`), zero
        // disk. Disk fallback → `magma-plan.json` checkpoint.
        match &self.cfg.artifact_store {
            Some(store) => {
                store
                    .put_plan(
                        &self.cfg.schema_name,
                        &self.cfg.template_name,
                        &plan,
                        plan_source_revision.as_deref(),
                    )
                    .await?;
            }
            None if self.cfg.plan_checkpoint => {
                let checkpoint = plan_file
                    .map(PathBuf::from)
                    .unwrap_or_else(|| Self::plan_checkpoint_path(work_dir));
                let bytes = serde_json::to_vec_pretty(&plan)
                    .map_err(|e| Error::MagmaExecution(format!("encode plan: {e}")))?;
                tokio::fs::write(&checkpoint, &bytes)
                    .await
                    .map_err(Error::Io)?;
            }
            None => {}
        }

        // Classify the plan per the configured DriftPolicy. Every
        // change is routed into AutoCorrect / AutoCorrectWithAlert /
        // RequireApproval / Refuse so the reconcile loop can act
        // (auto-apply, surface alert, halt for approval, refuse).
        let universal_plan = to_universal_plan(&plan);
        let drift_report = magma_drift::classify(&universal_plan, &self.cfg.drift_policy);

        // Lifecycle FSM: every reconcile threads through typed
        // phases. plan() transitions Idle -> Planning -> Approving
        // (if changes pending). apply() picks up from Planning and
        // transitions Applying -> Verifying -> Stable / Failed.
        let mut lifecycle = magma_fsm::LifecycleState::new();
        lifecycle
            .transition(magma_fsm::Phase::Planning, None, "magma_executor::plan")
            .map_err(|e| Error::MagmaExecution(format!("fsm transition: {e}")))?;

        // Emit typed events into a magma-stream PlanStream so the
        // BLAKE3-chained audit log captures every lifecycle stage.
        // Always-on InMemorySink captures events for the Bundle;
        // optional JsonLinesSink durably persists to disk.
        let audit_events = emit_stream_events(
            &self.cfg.audit_log_path,
            &universal_plan,
            &drift_report,
            None, // no outcome at plan-stage
        )
        .await;

        // Build a typed Bundle: plan + drift + lifecycle + audit
        // chain + optional gem-tree attestation. The bundle's
        // BLAKE3 hash is the compliance-export identity that
        // follows this reconcile through apply + verification.
        let workspace_label = self.cfg.schema_name.clone() + "/" + &self.cfg.template_name;
        let gem_tree_attestation = compute_gem_tree_attestation(work_dir).await;
        let bundle = magma_bundle::Bundle::new_with_gem_tree(
            "terraform",
            workspace_label,
            universal_plan.clone(),
            None, // no outcome at plan-stage
            drift_report.clone(),
            lifecycle.clone(),
            audit_events,
            gem_tree_attestation,
        )
        .map_err(|e| Error::MagmaExecution(format!("magma_bundle::new: {e}")))?;

        // Persist the plan-stage bundle so apply() (potentially a
        // separate reconcile) can pick it up and continue the
        // lifecycle on the same typed identity. DB-backed path →
        // Postgres (`put_bundle`), zero disk. Disk fallback →
        // `magma-bundle.json`.
        match &self.cfg.artifact_store {
            Some(store) => {
                store
                    .put_bundle(
                        &self.cfg.schema_name,
                        &self.cfg.template_name,
                        &bundle,
                        plan_source_revision.as_deref(),
                    )
                    .await?;
            }
            None => {
                let bundle_path = Self::bundle_checkpoint_path(work_dir);
                let bundle_bytes = serde_json::to_vec_pretty(&bundle)
                    .map_err(|e| Error::MagmaExecution(format!("encode bundle: {e}")))?;
                tokio::fs::write(&bundle_path, &bundle_bytes)
                    .await
                    .map_err(Error::Io)?;
            }
        }

        let stdout = serde_json::to_string_pretty(&serde_json::json!({
            "plan_id":          hex::encode(plan.id.0),
            "created_at":       plan.created_at,
            "resource_changes": plan.resource_changes.len(),
            "changes":          plan.resource_changes,
            "drift": {
                "summary": drift_report.summary,
                "decisions": drift_report.events.iter().map(|e| serde_json::json!({
                    "address":  e.address,
                    "action":   e.action,
                    "severity": e.severity,
                    "decision": e.decision,
                    "matched_policy": e.matched_policy,
                })).collect::<Vec<_>>(),
            },
            "bundle": {
                "bundle_id": bundle.bundle_id,
                "phase":     format!("{:?}", lifecycle.current),
            },
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
        // Read the typed plan the plan() phase persisted. DB-backed path
        // → Postgres (`get_plan`) — a normal cache read. A torn/corrupt
        // row is still a hard error (`read_db_plan`'s `?` surfaces the
        // BLAKE3 mismatch). A genuinely-MISSING row is a CACHE-MISS, not
        // a dead-end: regenerate the plan in-process and re-read it, the
        // same recompute-not-fail shape as the compile-cache-miss path.
        // Before this, a deleted/absent plan artifact os-error-2-wedged
        // the reconcile ("produced no analyzable output" → "No changes
        // detected" → STUCK until a pod restart).
        let plan: magma_types::Plan = match self.read_db_plan().await? {
            DbPlanRead::Present(plan) => plan,
            DbPlanRead::Missing => {
                // Cache-miss on the DB path: the plan row is gone but the
                // rendered config is still durable, so recompute the plan
                // (`plan()` re-persists the row via `put_plan`) and read
                // it back. Never terminal.
                tracing::warn!(
                    schema = %self.cfg.schema_name,
                    template = %self.cfg.template_name,
                    "apply: no persisted plan row (cache-miss) — regenerating the plan in-process before apply"
                );
                self.plan(work_dir, plan_file, &[]).await?;
                match self.read_db_plan().await? {
                    DbPlanRead::Present(plan) => plan,
                    // A regenerate that still can't produce a plan row is a
                    // genuine failure (the store isn't accepting writes) —
                    // surface it loudly rather than silently no-op'ing.
                    DbPlanRead::Missing => {
                        return Err(Error::MagmaExecution(format!(
                            "apply: plan row still absent after in-process regeneration for {}/{}; \
                             the artifact store did not persist the regenerated plan",
                            self.cfg.schema_name, self.cfg.template_name
                        )));
                    }
                    DbPlanRead::NotDbBacked => unreachable!(
                        "artifact_store presence does not change within a single apply()"
                    ),
                }
            }
            DbPlanRead::NotDbBacked => {
                // Disk fallback (no artifact store — the unit-test /
                // interim disk mode). Reads the `magma-plan.json`
                // checkpoint from the workspace dir.
                let checkpoint = plan_file
                    .map(PathBuf::from)
                    .unwrap_or_else(|| Self::plan_checkpoint_path(work_dir));
                let bytes = tokio::fs::read(&checkpoint).await.map_err(Error::Io)?;
                serde_json::from_slice(&bytes)
                    .map_err(|e| Error::MagmaExecution(format!("decode plan checkpoint: {e}")))?
            }
        };

        let backend = self.make_backend();
        self.ensure_state_table().await?;
        let mut state = magma_backend::Backend::read_state(&backend)
            .await
            .map_err(|e| Error::MagmaExecution(format!("read state: {e}")))?;

        // REAL apply: drive the providers over gRPC (spawn -> configure ->
        // plan -> apply), folding each resource's provider-returned new_state
        // into magma state, with apply-graph `${type.name.attr}` topological
        // reference resolution (run_plan_with_providers resolves the dependency
        // graph + substitutes refs; the state-level run_plan was a structural
        // no-op that could not create real cloud resources). Provider binaries
        // resolve under the workspace's `.terraform/providers` or the Nix-baked
        // `$MAGMA_PROVIDER_DIR`; provider credentials come from the rendered
        // workspace's `provider "<name>" { .. }` blocks (forwarded below) and
        // fall back to the pod env (GITHUB_TOKEN/GITHUB_OWNER/AWS_*/...) -- the
        // standard terraform-provider contract. No subprocess to tofu.
        // `run_plan_with_providers` is infallible: per-change failures land in
        // `outcome.failed` and drive the lifecycle to `Failed` (handled below),
        // so the bundle + partial successes are still recorded. ApplyContext::new
        // installs magma's default samba mutation pacer (1 req/s) so a bulk
        // apply can't burst past a provider's secondary rate limit.
        // Per the MAGMA-NATIVE EXECUTION directive.
        let mut ctx = magma_apply::engine::ApplyContext::new(work_dir.to_path_buf());
        // Forward each provider's ConfigureProvider value. Two sources,
        // merged per `build_provider_configs`:
        //   * rendered `provider "<name>" { .. }` blocks (the Ruby DSL's
        //     output — owner, token, ... resolved from akeyless by the
        //     Pangea architecture), and
        //   * `spec.providerCredentials` resolved from k8s Secrets in the
        //     controller layer (`self.cfg.provider_configs`) — the BASE
        //     credentials. This is what was MISSING: rio renders no
        //     cloudflare provider block, so without the spec-cred base the
        //     cloudflare provider got a null api_token and every real RPC
        //     failed ("channel closed"). Absent in both -> no per-provider
        //     block, and the provider falls back to its own env credentials.
        //
        // See `provider_configs_for`'s doc comment for the load-failure
        // fallback this now goes through — a load error no longer drops
        // `self.cfg.provider_configs` to empty.
        let provider_cfgs = self.provider_configs_for(work_dir, "apply").await;
        // ONE pacer per credential, shared across every ApplyContext this
        // process builds. Without it each context paced independently and the
        // credential's real ceiling was multiplied by the context count —
        // four sites here, times five shards on pleme-io-opensource, against
        // a budget the credential holds once.
        if let Some(p) = Self::shared_pacer_for(&provider_cfgs) {
            ctx = ctx.with_shared_pacer(p);
        }
        for (name, value) in provider_cfgs {
            ctx = ctx.with_provider_config(name, value);
        }
        let outcome = if self.cfg.structural_apply {
            // TEST-ONLY structural apply: mutate magma state directly, no
            // provider-RPC (CI has no provider binaries). See `structural_apply`.
            magma_apply::run_plan(&plan, &mut state)
                .map_err(|e| Error::MagmaExecution(format!("structural apply: {e}")))?
        } else {
            self.run_apply_resumable(&plan, &mut state, &ctx).await?
        };
        // DB-backed path: DO NOT write state here. The state write is
        // deferred into the atomic `put_apply_result` op below so the
        // state row + the apply receipt (bundle) land in ONE Postgres
        // transaction — together-or-not-at-all. Disk fallback: write
        // state now via the magma backend (state + disk bundle are two
        // separate writes, the legacy behavior).
        if self.cfg.artifact_store.is_none() {
            magma_backend::Backend::write_state(&backend, &state)
                .await
                .map_err(|e| Error::MagmaExecution(format!("write state: {e}")))?;
        }

        // Continue the lifecycle FSM: read the plan-stage bundle
        // (if present) + transition through Applying -> Verifying
        // -> Stable/Failed, then re-emit the bundle with the apply
        // outcome attached. Restart-safe: a fresh executor picks up
        // the durable bundle (Postgres on the DB path, disk on the
        // fallback).
        let bundle_path = Self::bundle_checkpoint_path(work_dir);
        let prev_bundle: Option<magma_bundle::Bundle> = match &self.cfg.artifact_store {
            Some(store) => {
                store
                    .get_bundle(&self.cfg.schema_name, &self.cfg.template_name)
                    .await?
            }
            None if bundle_path.exists() => tokio::fs::read(&bundle_path)
                .await
                .ok()
                .as_deref()
                .and_then(|b| serde_json::from_slice::<magma_bundle::Bundle>(b).ok()),
            None => None,
        };
        // `prev_bundle`'s lifecycle can be ANY phase here — including
        // one left behind by an earlier `apply()` call in this SAME
        // reconcile tick (self-heal retry / conflict-resolution
        // re-apply). `advance_lifecycle_through_apply` resets through
        // the universally-legal `Idle -> Planning` path before
        // attempting `Applying` whenever `current` isn't already a
        // phase that can enter `Applying` directly — see its doc
        // comment for the full failure mode this closes.
        let mut lifecycle = match prev_bundle {
            Some(prev) => prev.lifecycle,
            None => magma_fsm::LifecycleState::new(),
        };
        let plan_phase_id = magma_converge::PlanId(hex::encode(outcome.plan_id.0));
        let final_phase = if outcome.failed.is_empty() {
            magma_fsm::Phase::Stable
        } else {
            magma_fsm::Phase::Failed
        };
        let final_reason = if outcome.failed.is_empty() {
            "apply succeeded; state converged"
        } else {
            "apply produced failed changes"
        };
        advance_lifecycle_through_apply(
            &mut lifecycle,
            plan_phase_id.clone(),
            final_phase,
            final_reason,
        );

        // Re-shape the magma_types::Plan + Outcome into universal
        // form so they can be stored in the Bundle alongside drift
        // + lifecycle. Drift is classified again on the typed
        // universal plan (deterministic — same plan + policy =
        // same report).
        let universal_plan = to_universal_plan(&plan);
        let drift = magma_drift::classify(&universal_plan, &self.cfg.drift_policy);
        let universal_outcome = magma_converge::Outcome {
            plan_id: plan_phase_id.clone(),
            kind: "terraform".into(),
            applied: outcome
                .applied
                .iter()
                .map(|a| magma_converge::AppliedChange {
                    address: a.address.to_string(),
                    action: match a.action {
                        magma_types::Action::Create => magma_converge::Action::Create,
                        magma_types::Action::Update => magma_converge::Action::Update,
                        magma_types::Action::Delete => magma_converge::Action::Delete,
                        magma_types::Action::Replace => magma_converge::Action::Replace,
                        _ => magma_converge::Action::NoOp,
                    },
                })
                .collect(),
            failed: outcome
                .failed
                .iter()
                .map(|f| magma_converge::FailedChange {
                    address: f.address.to_string(),
                    action: match f.action {
                        magma_types::Action::Create => magma_converge::Action::Create,
                        magma_types::Action::Update => magma_converge::Action::Update,
                        magma_types::Action::Delete => magma_converge::Action::Delete,
                        magma_types::Action::Replace => magma_converge::Action::Replace,
                        _ => magma_converge::Action::NoOp,
                    },
                    error: f.reason.clone(),
                })
                .collect(),
            started_at: outcome.started_at,
            finished_at: outcome.finished_at,
        };
        // Emit apply-stage events into the audit chain (PlanComputed
        // + DriftClassified + ApplyOutcome). The chain is appended
        // to the audit_log_path if configured + captured into the
        // final Bundle.audit field.
        let audit_events = emit_stream_events(
            &self.cfg.audit_log_path,
            &universal_plan,
            &drift,
            Some(&universal_outcome),
        )
        .await;
        let workspace_label = self.cfg.schema_name.clone() + "/" + &self.cfg.template_name;
        let gem_tree_attestation = compute_gem_tree_attestation(work_dir).await;
        let final_bundle = magma_bundle::Bundle::new_with_gem_tree(
            "terraform",
            workspace_label,
            universal_plan,
            Some(universal_outcome),
            drift,
            lifecycle.clone(),
            audit_events,
            gem_tree_attestation,
        )
        .map_err(|e| Error::MagmaExecution(format!("magma_bundle::new: {e}")))?;

        // Persist the post-apply state + bundle. DB-backed path → ONE
        // atomic Postgres transaction (`put_apply_result`): the state
        // row (encoded in the configured BackendShape, byte-identical
        // to `write_state`) and the bundle artifact commit together or
        // roll back together — a half-applied reconcile (state advanced
        // without a receipt, or receipt without state) is
        // unrepresentable. Disk fallback → the bundle file (state was
        // already written above via the magma backend; two writes).
        match &self.cfg.artifact_store {
            Some(store) => {
                let state_bytes = self.encode_state_bytes(&state)?;
                let bundle_bytes = serde_json::to_vec(&final_bundle)
                    .map_err(|e| Error::MagmaExecution(format!("encode bundle: {e}")))?;
                let state_table = crate::backend::ArtifactStore::live_state_table(
                    &self.cfg.schema_name,
                    &self.cfg.template_name,
                )?;
                store
                    .put_apply_result(
                        &self.cfg.schema_name,
                        &self.cfg.template_name,
                        &state_table,
                        &self.cfg.state_name,
                        &state_bytes,
                        &bundle_bytes,
                    )
                    .await?;
            }
            None => {
                let bundle_bytes = serde_json::to_vec_pretty(&final_bundle)
                    .map_err(|e| Error::MagmaExecution(format!("encode bundle: {e}")))?;
                tokio::fs::write(&bundle_path, &bundle_bytes)
                    .await
                    .map_err(Error::Io)?;
            }
        }

        let stdout = serde_json::to_string_pretty(&serde_json::json!({
            "plan_id":     hex::encode(outcome.plan_id.0),
            "applied":     outcome.applied.len(),
            "failed":      outcome.failed.len(),
            "started_at":  outcome.started_at,
            "finished_at": outcome.finished_at,
            "bundle": {
                "bundle_id": final_bundle.bundle_id,
                "phase":     format!("{:?}", lifecycle.current),
                "lifecycle_transitions": lifecycle.history.len(),
            },
        }))
        .unwrap_or_default();
        Ok(if outcome.failed.is_empty() {
            ok_tofu_result(stdout, started)
        } else {
            // Surface the structured per-resource failures OUT to the
            // controller (the reacting FSM) instead of collapsing them into
            // the count-only `stdout`. Each `FailedChange.reason` is the wire
            // `controller::anomaly::classify` matches on; the address is
            // rendered `<type>.<name>` exactly as the universal bundle does
            // (line ~805) so addresses stay coherent across surfaces.
            let failed_changes = outcome
                .failed
                .iter()
                .map(|f| crate::executor::tofu::FailedChangeRecord {
                    address: f.address.to_string(),
                    action: f.action.to_string(),
                    reason: f.reason.clone(),
                })
                .collect();
            err_tofu_result_with_failures(stdout, started, failed_changes)
        })
    }

    async fn destroy(&self, work_dir: &Path, _auto_approve: bool) -> Result<TofuResult> {
        let started = Instant::now();
        let backend = self.make_backend();
        self.ensure_state_table().await?;
        let mut state = magma_backend::Backend::read_state(&backend)
            .await
            .map_err(|e| Error::MagmaExecution(format!("read state: {e}")))?;

        let resource_changes: Vec<magma_types::ResourceChange> = state
            .resources
            .iter()
            .map(|r| magma_types::ResourceChange {
                address: r.address.clone(),
                action: magma_types::Action::Delete,
                before: r.instances.first().map(|i| i.attributes.clone()),
                after: None,
                reasons: vec![magma_types::ChangeReason::DeletedResource],
                // Meta-arguments are declared on a CONFIG block, and this is a
                // synthetic destroy-everything plan built from STATE — there is
                // no config here to carry any. Empty is the accurate value, not
                // a placeholder.
                meta: magma_types::ResourceMeta::default(),
            })
            .collect();
        let plan = magma_types::Plan {
            id: magma_types::PlanId([0u8; 32]),
            created_at: chrono::Utc::now(),
            config_root: work_dir.to_path_buf(),
            variables: Default::default(),
            resource_changes,
            output_changes: vec![],
            // A synthesized destroy plan performs no refresh, so it carries
            // the default (unqualified) observation rather than claiming one.
            observation: Default::default(),
        };
        // REAL destroy: drive providers over gRPC so each resource's Delete is
        // a real provider DestroyResource RPC (the state-level run_plan only
        // mutated state structurally and never reached the cloud). Same
        // ApplyContext shape as apply -- provider creds from the rendered
        // workspace's provider blocks, falling back to the pod env. Infallible:
        // per-resource delete failures collect into `outcome.failed` and flip
        // the TofuResult to an error below.
        let mut ctx = magma_apply::engine::ApplyContext::new(work_dir.to_path_buf());
        // Provider creds: same two-source merge as apply
        // (`build_provider_configs`) — `spec.providerCredentials` base
        // (resolved from k8s Secrets) augmented by any rendered
        // `provider "<name>" {}` block. Routed: Postgres on the
        // DB-backed path, `main.tf.json` on the disk fallback. A destroy
        // must reach the provider with real creds for the DestroyResource
        // RPC, same as apply.
        //
        // Same credential-drop class as `apply()` above — see
        // `provider_configs_for`'s doc comment for the load-failure
        // fallback this now goes through.
        let provider_cfgs = self.provider_configs_for(work_dir, "destroy").await;
        // ONE pacer per credential, shared across every ApplyContext this
        // process builds. Without it each context paced independently and the
        // credential's real ceiling was multiplied by the context count —
        // four sites here, times five shards on pleme-io-opensource, against
        // a budget the credential holds once.
        if let Some(p) = Self::shared_pacer_for(&provider_cfgs) {
            ctx = ctx.with_shared_pacer(p);
        }
        for (name, value) in provider_cfgs {
            ctx = ctx.with_provider_config(name, value);
        }
        let outcome = if self.cfg.structural_apply {
            // TEST-ONLY structural destroy: remove from magma state directly,
            // no provider DestroyResource RPC. See `structural_apply`.
            magma_apply::run_plan(&plan, &mut state)
                .map_err(|e| Error::MagmaExecution(format!("structural destroy: {e}")))?
        } else {
            magma_apply::engine::run_plan_with_providers(&plan, &mut state, &ctx).await
        };
        // Destroy emits no bundle, so there is no state+receipt atomicity
        // pairing here — the emptied state goes straight through the
        // state backend (Postgres) via the magma backend on both paths.
        magma_backend::Backend::write_state(&backend, &state)
            .await
            .map_err(|e| Error::MagmaExecution(format!("write state: {e}")))?;

        let stdout = format!(
            "magma destroy: removed {} resources\n",
            outcome.applied.len()
        );
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
        self.ensure_state_table().await?;
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

    /// Adopt an out-of-band cloud resource into magma state via the
    /// provider's real `ImportResourceState` RPC — closing the
    /// production root cause behind the repeated "+N create" plans for
    /// infrastructure that was already real: this method used to be a
    /// hard stub (`Err` every call), so `run_import_prepass`
    /// (`template_controller.rs`) never actually absorbed anything for
    /// a magma-executor CR and every subsequent `plan()` re-proposed a
    /// Create for resources that already existed.
    ///
    /// Runs BEFORE `plan()` (per `run_import_prepass`'s call site in
    /// `handle_applying`), so the state this mutates is exactly what
    /// the FOLLOWING `plan()` reads — never an "import during apply"
    /// flow. Reuses `magma_apply::run_explicit_prepass` +
    /// `magma_apply::ConfiguredImportEnvironment` (which dials +
    /// configures the real provider — see that type's doc for why the
    /// raw/unconfigured `PluginImportEnvironment` is NOT used here);
    /// see [`StructuralImportEnvironment`] for the CI/unit-test path
    /// with no real provider binaries.
    ///
    /// Concurrency: `run_import_prepass` dispatches resolved import
    /// targets CONCURRENTLY (`buffer_unordered(10)`); the
    /// `acquire_import_state_lock` guard above serializes this
    /// method's read-state → mutate → write-state critical section per
    /// `(schema_name, template_name, state_name)` so concurrent imports
    /// against the SAME workspace can't race a lost update (see that
    /// guard's doc for the full "why").
    async fn import(&self, work_dir: &Path, address: &str, id: &str) -> Result<TofuResult> {
        let started = Instant::now();

        let lock_key = import_state_lock_key(
            &self.cfg.schema_name,
            &self.cfg.template_name,
            &self.cfg.state_name,
        );
        let _state_guard = acquire_import_state_lock(lock_key).await;

        self.ensure_state_table().await?;
        let backend = self.make_backend();
        let mut state = magma_backend::Backend::read_state(&backend)
            .await
            .map_err(|e| Error::MagmaExecution(format!("read state: {e}")))?;

        let directives = magma_types::ImportDirectives::default().with_explicit(address, id);

        let outcome = if self.cfg.structural_apply {
            let env = StructuralImportEnvironment;
            magma_apply::run_explicit_prepass(&env, &directives, &mut state)
                .await
                .map_err(|e| Error::MagmaExecution(format!("import prepass: {e}")))?
        } else {
            // Same ApplyContext shape as apply()/destroy() — provider
            // creds from the rendered config's `provider "<name>" {}`
            // blocks merged with `spec.providerCredentials`
            // (`build_provider_configs`). Best-effort config load: an
            // unreadable/unrendered config still lets the import run
            // with an empty provider config (the provider falls back to
            // its own env credentials), matching `apply()`'s existing
            // `if let Ok(cfg) = ...` tolerance.
            let mut ctx = magma_apply::engine::ApplyContext::new(work_dir.to_path_buf());
            if let Ok(cfg) = self.load_config_routed(work_dir).await {
                for (name, value) in self.build_provider_configs(&cfg) {
                    ctx = ctx.with_provider_config(name, value);
                }
                // ONE pacer per credential, shared across every ApplyContext this
                // process builds. Without it each context paced independently and
                // the credential's real ceiling was multiplied by the context count
                // — four sites here, times five shards on pleme-io-opensource.
                if let Some(p) = Self::shared_pacer_for(&self.build_provider_configs(&cfg)) {
                    ctx = ctx.with_shared_pacer(p);
                }
            }
            let env = magma_apply::ConfiguredImportEnvironment::new(&ctx);
            magma_apply::run_explicit_prepass(&env, &directives, &mut state)
                .await
                .map_err(|e| Error::MagmaExecution(format!("import prepass: {e}")))?
        };

        if let Some(failed) = outcome.failed.first() {
            return Ok(err_tofu_result(
                format!(
                    "magma import failed for {address} ({id}): {}\n",
                    failed.reason
                ),
                started,
            ));
        }

        // Persist the adopted state so the plan() that follows this
        // prepass reads it. No bundle to pair it with here (import is a
        // pre-plan state-adoption step, not a plan/apply outcome), so
        // this is a plain write — the same call `apply()`'s disk-fallback
        // branch makes, always taken here regardless of `artifact_store`
        // since there's no atomic state+bundle transaction to fold into.
        magma_backend::Backend::write_state(&backend, &state)
            .await
            .map_err(|e| Error::MagmaExecution(format!("write state: {e}")))?;

        let absorbed = outcome
            .imported
            .first()
            .map(|i| i.absorbed)
            .unwrap_or(false);
        let stdout = format!(
            "magma import: {address} ({id}) {}\n",
            if absorbed {
                "adopted into state"
            } else {
                "already present in state (no-op)"
            }
        );
        Ok(ok_tofu_result(stdout, started))
    }

    /// Magma-native, disk-free import-discovery readback (the P1a fix).
    ///
    /// On the DB-backed (production) path this reads the SAME typed plan
    /// row `apply()` consumes (`read_db_plan` → `ArtifactStore::get_plan`)
    /// — **ZERO filesystem** — and maps each magma `ResourceChange` into
    /// the executor-agnostic [`PlannedChange`] border. No tofu-format
    /// re-serialization, no `tokio::fs::read`.
    ///
    /// A genuinely-MISSING plan row (cache-miss) returns `Ok(None)`, NOT
    /// a hard `Err`. `planned_changes()` is the pre-apply IMPORT-discovery
    /// prepass; it has no `work_dir`, so it can't regenerate the plan
    /// itself — but the `apply()` that follows in the same reconcile WILL
    /// regenerate the missing plan (see `apply()`), so the correct
    /// behavior here is to degrade to the legacy `show_plan` / prior-drifts
    /// discovery path (the `Ok(None)` arm the controller already handles)
    /// rather than wedge the whole cycle. Previously a missing row was a
    /// LOUD `Err` that the controller turned into a stuck no-op.
    ///
    /// A TORN/CORRUPT row is still a hard `Err` (propagated from
    /// `read_db_plan`'s BLAKE3-verifying `?`) — a data-integrity failure
    /// must surface, never be regenerated over.
    ///
    /// On the disk-fallback path (no `artifact_store` — unit tests /
    /// interim disk mode) this also returns `Ok(None)`, so the prepass
    /// routes to the legacy `show_plan` path unchanged. Tier: the DB path
    /// is disk-free by-construction (no `&Path` reaches this method); the
    /// disk-fallback `None` arm is only-mitigated interim, retired when the
    /// artifact store is always wired.
    async fn planned_changes(&self) -> Result<Option<Vec<PlannedChange>>> {
        match self.read_db_plan().await? {
            DbPlanRead::Present(plan) => {
                // State is read HERE, alongside the plan, because a composite
                // import id's parent component is an unresolved reference in
                // the plan and only state carries the parent's real name. A
                // state read that fails is NOT fatal to discovery: fall back
                // to an empty map, in which every parent-keyed derivation
                // downgrades to inexact and is refused by the prepass — a
                // missed adoption, never a wrong one.
                let backend = self.make_backend();
                let state = match magma_backend::Backend::read_state(&backend).await {
                    Ok(s) => s,
                    Err(e) => {
                        tracing::warn!(
                            error = %e,
                            "planned_changes: state read failed — import ids will derive \
                             without parent resolution and inexact ones will be refused"
                        );
                        empty_state()
                    }
                };
                Ok(Some(planned_changes_from_magma_plan(&plan, &state)))
            }
            // Cache-miss OR not-DB-backed → route to the legacy discovery
            // path. The following apply() regenerates the plan on the DB
            // path; degrading discovery here is non-fatal.
            DbPlanRead::Missing | DbPlanRead::NotDbBacked => Ok(None),
        }
    }
}

// ── Tests ──────────────────────────────────────────────────────────

/// Explain a `Stalled` cycle from what the cycle actually measured.
///
/// A stall means one thing mechanically -- a cycle completed zero nodes and so
/// retrying it unchanged cannot converge -- but it has genuinely different
/// causes, and the fix for one is a no-op for the others. This used to emit a
/// single hardcoded sentence blaming the quantum for all of them:
///
/// > the quantum (120s) cannot cover this cycle's fixed prologue, so retrying
/// > unchanged cannot converge. Raise PANGEA_MAGMA_APPLY_QUANTUM_SECS or
/// > reduce the prologue.
///
/// Observed live on camelot-eks 2026-07-30 against
/// `prologue_ms: 11, elapsed_ms: 9607, quantum_ms: Some(120000),
/// nodes_attempted: 9, nodes_completed: 0`. The prologue was ELEVEN
/// MILLISECONDS against a two-minute quantum and the cycle used 8% of its
/// budget, so the stated cause was not merely unproven, it was contradicted by
/// the numbers printed in the same string. Following the instruction (raising
/// `PANGEA_MAGMA_APPLY_QUANTUM_SECS`) would have changed nothing, because the
/// real problem was that all 9 attempted nodes failed.
///
/// So the message is derived from the evidence rather than asserted. The
/// distinguishing signal is cheap: a prologue that genuinely does not fit
/// leaves no room to attempt anything, whereas nodes that fail attempt work
/// and complete none of it.
/// Render the DISTINCT provider errors behind a stall, inline.
///
/// This exists because the message it feeds used to end with "look at the
/// provider errors for those nodes" — and there was nowhere to look. The
/// per-node reasons are carried on `ApplyOutcome::failed` all the way to the
/// stall arm, and the call site then dropped them on the floor with a `..`
/// pattern. So the operator's own diagnostic instructed the reader to consult
/// evidence it had just discarded. Measured 2026-08-01 on
/// camelot-eks-shaar-concentrator: 20/20 nodes failed, and neither the CR
/// status nor any log line named a single cause.
///
/// Deduplicated on purpose: 20 nodes failing the same way is ONE fact, and
/// printing it twenty times buries it. The count carries the multiplicity.
/// Capped so a pathological plan cannot produce an unreadable status field.
fn fmt_provider_errors(failed: &[magma_apply::FailedChange]) -> String {
    use std::collections::BTreeMap;
    if failed.is_empty() {
        // Worth stating rather than rendering nothing: "no failures recorded"
        // alongside "all nodes failed" is itself a bug report about the
        // engine, and a silent empty string would hide that contradiction.
        return " No per-node failure reasons were recorded, which contradicts                 the counts above — suspect the apply engine, not the provider."
            .to_string();
    }
    let mut by_reason: BTreeMap<&str, (usize, &magma_apply::FailedChange)> = BTreeMap::new();
    for f in failed {
        let e = by_reason.entry(f.reason.as_str()).or_insert((0, f));
        e.0 += 1;
    }
    let total_distinct = by_reason.len();
    let mut out = format!(" Provider errors ({total_distinct} distinct):");
    for (reason, (count, sample)) in by_reason.iter().take(3) {
        let reason = reason.trim();
        let reason = if reason.len() > 400 {
            format!("{}…", &reason[..400])
        } else {
            reason.to_string()
        };
        out.push_str(&format!(
            " [x{count}, e.g. {} {:?}] {reason};",
            sample.address, sample.action
        ));
    }
    if total_distinct > 3 {
        out.push_str(&format!(" (+{} more distinct)", total_distinct - 3));
    }
    out
}

#[cfg(test)]
mod stall_provider_error_tests {
    use super::*;
    use magma_apply::FailedChange;
    use magma_types::{Action, ModulePath, ResourceAddress, ResourceKind, ResourceTypeId};

    fn addr(name: &str) -> ResourceAddress {
        ResourceAddress {
            module: ModulePath(vec![]),
            kind: ResourceKind::Managed,
            type_id: ResourceTypeId("aws_ssm_parameter".to_string()),
            name: name.to_string(),
            key: None,
        }
    }

    fn fail(name: &str, reason: &str) -> FailedChange {
        FailedChange {
            address: addr(name),
            action: Action::Create,
            reason: reason.to_string(),
        }
    }

    /// The regression this whole change exists for: the stall message used to
    /// end with "look at the provider errors for those nodes" while the caller
    /// discarded them, so there was nowhere to look. Measured 2026-08-01 on
    /// camelot-eks-shaar-concentrator — 20/20 failed, zero causes recorded
    /// anywhere.
    #[test]
    fn stall_message_prints_the_provider_errors_instead_of_pointing_at_them() {
        let stats = magma_apply::CycleStats {
            nodes_attempted: 20,
            nodes_completed: 0,
            nodes_remaining: 20,
            nodes_failed: 20,
            ..Default::default()
        };
        let failed: Vec<FailedChange> = (0..20)
            .map(|i| {
                fail(
                    &format!("p{i}"),
                    "AccessDenied: not authorized to perform ssm:PutParameter",
                )
            })
            .collect();

        let msg = diagnose_stall(&stats, &failed);

        assert!(
            msg.contains("AccessDenied"),
            "the actual provider error must be IN the message, got: {msg}"
        );
        assert!(
            !msg.contains("look at the provider errors"),
            "must not send the reader somewhere with nothing to read, got: {msg}"
        );
        // Deduplicated: 20 identical failures are ONE fact plus a count.
        assert!(
            msg.contains("1 distinct") && msg.contains("x20"),
            "identical reasons must collapse to one entry with a count, got: {msg}"
        );
    }

    #[test]
    fn distinct_reasons_are_capped_but_the_remainder_is_declared() {
        let stats = magma_apply::CycleStats {
            nodes_attempted: 5,
            nodes_completed: 0,
            nodes_remaining: 5,
            nodes_failed: 5,
            ..Default::default()
        };
        let failed: Vec<FailedChange> = (0..5)
            .map(|i| fail(&format!("p{i}"), &format!("distinct error number {i}")))
            .collect();

        let msg = diagnose_stall(&stats, &failed);
        assert!(msg.contains("5 distinct"), "got: {msg}");
        // Capped at 3 shown, and the truncation is STATED rather than silent —
        // a hidden cap is how "covered everything" gets believed.
        assert!(
            msg.contains("+2 more distinct"),
            "cap must be declared, got: {msg}"
        );
    }
}

fn diagnose_stall(stats: &magma_apply::CycleStats, failed: &[magma_apply::FailedChange]) -> String {
    let quantum_ms = stats.quantum_ms.unwrap_or(0);

    // The original hypothesis, now only claimed when it is actually true.
    if quantum_ms > 0 && stats.prologue_ms >= quantum_ms {
        return format!(
            "the fixed prologue ({}ms) meets or exceeds the quantum ({}ms), so no \
             cycle can reach its first mutation. Raise \
             PANGEA_MAGMA_APPLY_QUANTUM_SECS above the prologue, or reduce the \
             prologue",
            stats.prologue_ms, quantum_ms
        );
    }

    if stats.nodes_attempted > 0 && stats.nodes_completed == 0 {
        let budget = if quantum_ms > 0 {
            format!(
                " The cycle used {}ms of its {}ms quantum, so the quantum is NOT the \
                 constraint and raising it will not help.",
                stats.elapsed_ms, quantum_ms
            )
        } else {
            String::new()
        };
        return format!(
            "all {} attempted node(s) failed and {} completed ({} still remaining), so \
             retrying unchanged cannot converge. This is a per-resource apply failure, \
             not a scheduling problem.{}{}",
            stats.nodes_attempted,
            stats.nodes_completed,
            stats.nodes_remaining,
            budget,
            fmt_provider_errors(failed),
        );
    }

    if stats.nodes_attempted == 0 {
        return format!(
            "no node was attempted at all across {} wave(s) (max wave width {}), so the \
             cycle had nothing it could run. Suspect the dependency graph or the \
             frontier cursor rather than any timing knob",
            stats.waves_entered, stats.max_wave_width
        );
    }

    // Completed > 0 yet still classified as stalled: not a shape this function
    // claims to understand, and saying so beats inventing a cause.
    format!(
        "cycle made no forward progress for reasons not covered by the known stall \
         shapes ({} attempted, {} completed, {} remaining, {}ms elapsed of {}ms \
         quantum). Read the raw stats below rather than trusting this sentence",
        stats.nodes_attempted,
        stats.nodes_completed,
        stats.nodes_remaining,
        stats.elapsed_ms,
        quantum_ms
    )
}

#[cfg(test)]
mod pacer_wiring_tests {
    /// Every ApplyContext this executor builds gets the SHARED pacer.
    ///
    /// magma builds a fresh per-context bucket by default, so a context
    /// constructed without `with_shared_pacer` is not unpaced — it is paced
    /// INDEPENDENTLY, which is worse: it looks correct at every call site
    /// while multiplying the credential's real ceiling by the number of
    /// contexts. Measured 2026-08-08: four sites here times five shards put
    /// the ceiling near 18,000 req/hr against a 5,000 req/hr budget, and
    /// nothing in the logs said so.
    ///
    /// Counted at the SOURCE because the defect is a missing call — a
    /// behavioural test would need a live provider and would still pass, since
    /// an independently-paced context applies perfectly well.
    #[test]
    fn every_apply_context_receives_the_shared_pacer() {
        // PER-SITE, not a count. Equal totals is the weaker property and it
        // passes on a file where one site is wired twice and another not at
        // all — which is exactly the shape a copy-paste produces.
        let src = include_str!("magma.rs");
        let lines: Vec<&str> = src.lines().collect();
        const WINDOW: usize = 40;
        let unwired: Vec<usize> = lines
            .iter()
            .enumerate()
            .filter(|(_, l)| l.contains("ApplyContext::new(work_dir.to_path_buf())"))
            .filter(|(i, _)| {
                let end = (i + WINDOW).min(lines.len());
                !lines[*i..end]
                    .iter()
                    .any(|l| l.contains("with_shared_pacer"))
            })
            .map(|(i, _)| i + 1)
            .collect();
        assert!(
            unwired.is_empty(),
            "ApplyContext built without the shared pacer at line(s) {unwired:?} \
             — an un-shared context is not unpaced, it paces INDEPENDENTLY, \
             which looks correct at the call site while multiplying the \
             credential's real ceiling by the number of contexts"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::state_backend::InMemoryStateBackend;
    use serde_json::json;

    // ── M0.16: bounded, resumable apply (MAGMA-OPERATOR-BACKEND.md §II-ter) ──

    /// The debounce must not permit two writes inside one interval.
    ///
    /// This is the property that keeps checkpointing from being O(N²): the
    /// sink is invoked after *every* applied node, and each write carries the
    /// whole state, so an undebounced sink on the 2,478-resource workspace
    /// that motivated §II-ter would issue thousands of whole-state writes.
    #[test]
    fn debounce_permits_one_event_per_interval() {
        let d = Debounce::new(std::time::Duration::from_secs(60));
        assert!(d.allow(), "first event must always be permitted");
        for i in 0..1_000 {
            assert!(
                !d.allow(),
                "event {i} inside the interval must be suppressed — an \
                 undebounced sink is O(N) whole-state writes"
            );
        }
    }

    /// …and it must not suppress forever: once the interval passes, the next
    /// event is permitted. A debounce that only ever allowed the first write
    /// would satisfy the test above while making checkpointing useless.
    #[test]
    fn debounce_permits_again_after_the_interval() {
        // Zero interval ⇒ every event is outside it ⇒ always permitted.
        let d = Debounce::new(std::time::Duration::ZERO);
        for i in 0..100 {
            assert!(d.allow(), "event {i} must be permitted at a zero interval");
        }
    }

    /// The quantum is opt-out, and `0` is the documented way to opt out.
    ///
    /// `Quantum::new` returns `None` for a zero duration by construction, so
    /// "run-to-completion" is reachable through the same field rather than
    /// needing a second flag. Pins the mapping the executor relies on:
    /// `Some(0)` and `None` must both mean run-to-completion.
    #[test]
    fn zero_quantum_means_run_to_completion() {
        use magma_apply::cursor::Quantum;
        assert!(
            Quantum::from_secs(0).is_none(),
            "0 must disable resumption, not create a zero-length quantum"
        );
        assert!(Quantum::from_secs(120).is_some());

        // The executor's resolution: cfg field → Option<Quantum>.
        let resolve = |secs: Option<u64>| secs.and_then(Quantum::from_secs);
        assert!(resolve(None).is_none(), "unset ⇒ run-to-completion");
        assert!(resolve(Some(0)).is_none(), "0 ⇒ run-to-completion");
        assert!(resolve(Some(120)).is_some(), "120 ⇒ resumable");
    }

    /// A cursor only resumes into the plan it was built for.
    ///
    /// This is the invariant that makes persisting a cursor across reconciles
    /// safe: a cursor left behind by a *different* plan must be rejected
    /// rather than silently skipping changes it never applied. It is also why
    /// the store needs no delete verb — a stale row is self-invalidating.
    #[test]
    fn cursor_refuses_to_resume_into_a_different_plan() {
        use magma_apply::cursor::ApplyCursor;
        use magma_types::{Plan, PlanId};

        let plan_with = |id: u8| Plan {
            id: PlanId([id; 32]),
            created_at: chrono::Utc::now(),
            config_root: std::path::PathBuf::new(),
            variables: std::collections::HashMap::new(),
            resource_changes: Vec::new(),
            observation: Default::default(),
            output_changes: Vec::new(),
        };

        let plan_a = plan_with(7);
        let plan_b = plan_with(9);
        assert_ne!(
            plan_a.id, plan_b.id,
            "fixture precondition: the two plans must have different ids"
        );

        let cursor = ApplyCursor::empty(plan_a.id.clone());
        assert!(
            cursor.resume(&plan_a).is_some(),
            "a cursor must resume into its own plan"
        );
        assert!(
            cursor.resume(&plan_b).is_none(),
            "a cursor must REFUSE a different plan — otherwise a stale row \
             would skip changes that were never applied"
        );
    }

    fn fixture_config() -> MagmaExecutorConfig<InMemoryStateBackend> {
        MagmaExecutorConfig {
            // Unit tests have no provider binaries — structural apply.
            structural_apply: true,
            state_backend: Arc::new(InMemoryStateBackend::new()),
            schema_name: "test_schema".into(),
            template_name: "test_template".into(),
            state_name: "default".into(),
            backend_shape: BackendShape::Magma,
            plan_checkpoint: true,
            apply_quantum_secs: None,
            // Test fixtures use minimal Pangea shapes that omit
            // `terraform.required_providers`; preflight would
            // reject them. Production code paths use the
            // Default which has preflight_laws: true.
            preflight_laws: false,
            drift_policy: magma_drift::DriftPolicy::conservative_default(),
            audit_log_path: None,
            artifact_store: None,
            provider_configs: std::collections::BTreeMap::new(),
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
        assert!(
            apply_result.success,
            "apply failed: {}",
            apply_result.stdout
        );
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
        assert!(
            shown.stdout.contains("\"id\""),
            "missing id field in shown plan:\n{}",
            shown.stdout
        );
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

    /// Regression test for the fleet-wide bug (found in
    /// `handle_destroying`, `controller/template_controller.rs`): the
    /// phase handler used to gate its only call to `runner.destroy()`
    /// on `workspace.file_exists(".terraform")` — a tofu-only artifact
    /// that `MagmaExecutor::init` (a documented no-op, see above) never
    /// creates. On magma — the fleet-default executor since
    /// 2026-06-02 — that guard was permanently false, so destroy was
    /// silently never invoked on CR deletion and real cloud
    /// infrastructure leaked, untracked, forever.
    ///
    /// This reproduces the exact shape at the `WorkspaceRunner` layer
    /// `handle_destroying` now calls unconditionally (the guard was
    /// removed from the phase handler entirely): a workspace directory
    /// with NO `.terraform` and NO prior apply, driven through
    /// `MagmaWorkspaceRunner::destroy` — the real call site, not the
    /// raw `IacExecutor` — proving the fix holds at the layer the
    /// controller actually calls.
    #[tokio::test]
    async fn workspace_runner_destroy_succeeds_on_never_applied_magma_workspace() {
        use crate::executor::workspace::WorkspaceManager;
        use crate::executor::workspace_runner::{MagmaWorkspaceRunner, WorkspaceRunner};

        let exec: Arc<dyn IacExecutor> = Arc::new(MagmaExecutor::new(fixture_config()));
        let runner = MagmaWorkspaceRunner::new(exec, None, std::time::Duration::from_secs(600));

        let tmp = tempfile::tempdir().unwrap();
        let manager = WorkspaceManager::new(tmp.path().to_path_buf());
        let workspace = manager
            .get_or_create("test-ns", "never-applied")
            .await
            .unwrap();
        assert!(
            !workspace.file_exists(".terraform"),
            "magma must never create .terraform — that's the whole bug"
        );

        let result = runner.destroy(&workspace, true).await.unwrap();
        assert!(
            result.success,
            "destroy against a never-applied magma workspace must succeed as a real no-op, \
             never be silently skipped"
        );
        assert!(result.raw_stdout.contains("removed 0 resources"));
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
    async fn import_adopts_a_resource_into_state() {
        let exec = MagmaExecutor::new(fixture_config());
        let tmp = tempfile::tempdir().unwrap();

        let result = exec
            .import(
                tmp.path(),
                "aws_iam_role.adopted",
                "arn:aws:iam::123:role/adopted",
            )
            .await
            .unwrap();
        assert!(result.success, "import should succeed: {}", result.stdout);
        assert!(result.stdout.contains("adopted into state"));

        let backend = exec.make_backend();
        let state = magma_backend::Backend::read_state(&backend).await.unwrap();
        assert_eq!(state.resources.len(), 1);
        assert_eq!(state.resources[0].address.type_id.0, "aws_iam_role");
        assert_eq!(state.resources[0].address.name, "adopted");
    }

    /// Mechanism pin for the credential-drop class closed in
    /// `controller::template_controller::try_import` and
    /// `controller::conflict::resolve_conflicts_post_apply` (both used to
    /// build their `IacExecutor` via the sync, credential-blind
    /// `ControllerState::executor_for`, whose magma arm always
    /// constructs a `MagmaExecutor` with an EMPTY `provider_configs` map
    /// — see `magma_executor_for`'s doc comment: "no
    /// spec.providerCredentials forwarding ... the mutating apply/destroy
    /// path resolves creds via `executor_for_checked_with_creds`"; those
    /// two call sites were the mutating exceptions that never got
    /// updated). `import()` / `apply()` / `destroy()` all thread
    /// credentials into the provider RPC exclusively via
    /// `build_provider_configs`, which folds the rendered config's own
    /// `provider "<name>" {}` blocks with `self.cfg.provider_configs` —
    /// the ONLY carrier for `spec.providerCredentials` once it has been
    /// resolved by the controller layer. A provider with NO rendered
    /// block (rio's cloudflare provider; an AWS provider block that
    /// deliberately omits explicit keys) gets ZERO credentials unless
    /// `provider_configs` was populated at CONSTRUCTION time — this is
    /// the live incident's exact shape: an `AttachRolePolicy` RPC issued
    /// through an executor built via the credential-blind constructor
    /// silently ran under the pod's ambient EC2 instance-role credentials
    /// instead of the CR's declared `spec.providerCredentials`.
    #[tokio::test]
    async fn build_provider_configs_drops_spec_provider_credentials_when_constructed_credential_blind(
    ) {
        let tmp = tempfile::tempdir().unwrap();
        render_workspace(
            tmp.path(),
            &json!({
                // No top-level "provider" block at all — the exact shape
                // rio's rendered config has for cloudflare, and any
                // template whose AWS identity is meant to come from
                // spec.providerCredentials rather than an inline
                // provider block.
                "resource": { "aws_iam_role": { "r": { "name": "x" } } },
            }),
        )
        .await;
        let cfg = MagmaExecutor::<InMemoryStateBackend>::load_config(tmp.path())
            .await
            .unwrap();

        // Credential-BLIND construction — what the pre-fix
        // `state.executor_for()` built for `try_import` /
        // `resolve_conflicts_post_apply` (`provider_configs` empty, the
        // same shape `magma_executor_for` produces).
        let blind = MagmaExecutor::new(fixture_config());
        assert!(
            blind.build_provider_configs(&cfg).is_empty(),
            "an executor built with an empty provider_configs map must \
             produce NO provider credentials for a provider with no \
             rendered block — this is the exact silent-ambient-credential \
             fallback the live incident hit"
        );

        // Credential-AWARE construction — what
        // `state.executor_for_checked_with_creds()` builds
        // (spec.providerCredentials resolved by the controller layer and
        // threaded into MagmaExecutorConfig.provider_configs). Both
        // fixed call sites now route through this constructor.
        let mut aware_cfg = fixture_config();
        aware_cfg.provider_configs.insert(
            "aws".to_string(),
            json!({
                "region": "us-east-1",
                "access_key": "AKIA_FAKE",
                "secret_key": "fake-secret",
            }),
        );
        let aware = MagmaExecutor::new(aware_cfg);
        let built = aware.build_provider_configs(&cfg);
        assert_eq!(
            built
                .get("aws")
                .and_then(|v| v.get("access_key"))
                .and_then(|v| v.as_str()),
            Some("AKIA_FAKE"),
            "an executor built WITH provider_configs must carry \
             spec.providerCredentials through to the provider RPC even \
             when the rendered config has no matching provider block"
        );
    }

    /// The SECOND, still-open door onto the same credential-drop class:
    /// even a credential-AWARE executor (`self.cfg.provider_configs`
    /// correctly populated, exactly like `executor_for_checked_with_creds`
    /// builds it) used to drop those base credentials to empty inside
    /// `apply()`/`destroy()` themselves whenever `load_config_routed`
    /// returned `Err` — a transient artifact-store read, an evicted DB
    /// row between the plan-phase load and this independent reload. The
    /// pre-fix code was `if let Ok(cfg) = self.load_config_routed(...).await
    /// { forward the merged map }` with NO `else` — a load failure meant
    /// the entire forwarding block, base credentials included, was
    /// silently skipped, leaving `ApplyContext.provider_configs` empty for
    /// a REAL mutating apply/destroy RPC. `provider_configs_for` is the
    /// extracted, directly-testable seam both `apply()` and `destroy()`
    /// now share; this pins that a `load_config_routed` failure (here:
    /// `work_dir` has no `main.tf.json` at all, so disk-path
    /// `load_config` errors) still returns the base
    /// `spec.providerCredentials` map unmerged, not an empty one.
    #[tokio::test]
    async fn provider_configs_for_falls_back_to_spec_credentials_on_load_failure() {
        let mut cfg = fixture_config();
        cfg.provider_configs.insert(
            "aws".to_string(),
            json!({
                "region": "us-east-1",
                "access_key": "AKIA_FAKE",
                "secret_key": "fake-secret",
            }),
        );
        let exec = MagmaExecutor::new(cfg);

        // No `render_workspace` call — `work_dir` has no `main.tf.json`,
        // so `load_config_routed`'s disk-path `load_config` errors.
        let tmp = tempfile::tempdir().unwrap();

        let resolved = exec.provider_configs_for(tmp.path(), "apply").await;
        assert_eq!(
            resolved
                .get("aws")
                .and_then(|v| v.get("access_key"))
                .and_then(|v| v.as_str()),
            Some("AKIA_FAKE"),
            "a load_config_routed failure must still forward \
             self.cfg.provider_configs (the base spec.providerCredentials \
             map) — dropping it silently leaves the real apply/destroy RPC \
             to the provider's own ambient credential chain"
        );
    }

    /// The load-bearing regression this whole fix closes: a
    /// subsequently-planned resource that was just imported must NOT
    /// show up as a Create — `plan()` reads the SAME state `import()`
    /// just wrote.
    #[tokio::test]
    async fn import_then_plan_no_longer_proposes_a_create_for_the_adopted_resource() {
        let exec = MagmaExecutor::new(fixture_config());
        let tmp = tempfile::tempdir().unwrap();
        render_workspace(
            tmp.path(),
            &json!({
                "provider": { "aws": { "region": "us-east-1" } },
                "resource": { "aws_iam_role": { "adopted": { "name": "adopted" } } },
            }),
        )
        .await;

        let import_result = exec
            .import(tmp.path(), "aws_iam_role.adopted", "adopted")
            .await
            .unwrap();
        assert!(import_result.success, "{}", import_result.stdout);

        let plan_result = exec.plan(tmp.path(), None, &[]).await.unwrap();
        assert!(plan_result.success);
        assert_eq!(
            plan_result.exit_code, 0,
            "the imported resource must plan as NoOp, not Create: {}",
            plan_result.stdout
        );
    }

    /// The camelot-eks live-incident regression: `magma_plan::plan()`
    /// (pinned via this repo's `magma-plan` git dependency) used to
    /// HARDCODE `Action::NoOp` for every resource present in BOTH
    /// config and state — regardless of whether the config's declared
    /// attributes actually diverged from what was recorded. An
    /// imported/adopted resource is the textbook trigger for this: the
    /// real production import path
    /// (`magma_apply::ConfiguredImportEnvironment`, stood in here by
    /// `StructuralImportEnvironment`) writes only an
    /// `ImportResourceState` STUB — id-only, no follow-up
    /// `ReadResource` — so nearly every other attribute in the written
    /// state is either absent or null. Ground-truthed live 2026-07-19
    /// against the real Postgres state row for
    /// `aws_eks_node_group.camelot-eks_controllers_ng` (schema
    /// `pangea_camelot-dev_camelot-eks_states`): `instance_types` was
    /// stored as JSON `null`, not the stale AND not the corrected
    /// instance-type list. Before magma commit e357b4d
    /// ("magma-plan: config-subset drift detection for in-both
    /// resources"), a subsequent Planning cycle against a corrected
    /// `main.tf.json` reported this resource as `noOp` — which, had a
    /// human approved the resulting auto/require-approval gate,
    /// would have applied blind and left the node group's tiny
    /// instance types (and the ARC CI-runner memory starvation they
    /// caused) in place forever, since magma's apply step never
    /// touches a resource its own plan says needs zero changes.
    ///
    /// This test reproduces that exact shape at the layer
    /// `handle_planning` actually calls (`MagmaExecutor::plan()`, not
    /// the isolated `magma-plan` unit tests) so a future regression in
    /// EITHER the `magma-plan` dependency OR this crate's own
    /// `to_universal_plan` / `CycleArtifact` projection surfaces here.
    #[tokio::test]
    async fn import_then_plan_surfaces_drift_when_config_diverges_from_the_imported_state() {
        let exec = MagmaExecutor::new(fixture_config());
        let tmp = tempfile::tempdir().unwrap();

        // `StructuralImportEnvironment::import_resource_state` echoes
        // `{"id": id, "name": id}` for the imported address — the
        // stand-in for the real provider's id-only ImportResourceState
        // stub. Recorded state: name == "stale-instance-type".
        let import_result = exec
            .import(tmp.path(), "aws_iam_role.adopted", "stale-instance-type")
            .await
            .unwrap();
        assert!(import_result.success, "{}", import_result.stdout);

        // The rendered config now declares a DIFFERENT value for the
        // SAME declared attribute the import echoed — the exact shape
        // of the camelot-eks bug (instance_types corrected in
        // main.tf.json after the node group was already adopted into
        // state).
        render_workspace(
            tmp.path(),
            &json!({
                "provider": { "aws": { "region": "us-east-1" } },
                "resource": {
                    "aws_iam_role": {
                        "adopted": { "name": "corrected-instance-type" },
                    },
                },
            }),
        )
        .await;

        let plan_result = exec.plan(tmp.path(), None, &[]).await.unwrap();
        assert!(plan_result.success);
        assert_eq!(
            plan_result.exit_code, 2,
            "a config-declared attribute that diverges from the \
             imported (stub) state must be reported as a real change \
             — NEVER silently swallowed as NoOp, which would let an \
             auto/require-approval gate apply blind: {}",
            plan_result.stdout
        );
        assert!(
            plan_result.stdout.contains("\"action\": \"update\""),
            "the drifted resource must classify as Action::Update, not \
             hardcoded NoOp: {}",
            plan_result.stdout
        );
    }

    // ── refresh_ctx_for routing (the camelot-eks plan-time-refresh fix) ──
    //
    // The call-site bug this closes: `plan()` called bare
    // `magma_plan::plan(&cfg, &state)` against whatever state was last
    // written, never `magma_apply::engine::refresh_then_plan`, so an
    // import-adopted resource's null-attribute stub (see the
    // `import_then_plan_surfaces_drift_when_config_diverges_from_the_imported_state`
    // test above for that half of the incident) was NEVER backfilled from
    // the live provider before diffing — every real config value read as
    // false drift.
    //
    // A genuinely live `ReadResource` RPC needs a real provider binary
    // this test environment doesn't have, so these tests prove the
    // ROUTING is correct — `plan()`'s call now reaches
    // `refresh_then_plan` with the right `Some`/`None` argument shape —
    // rather than exercising a real refresh end-to-end (that is
    // magma's own job; see `magma-test/tests/integration_refresh_then_plan.rs`
    // in the magma repo, which proves `refresh_then_plan` itself dials a
    // real mock provider and backfills a null-stub instance).

    /// `structural_apply: true` (every unit test's `fixture_config` in
    /// this file — CI has no provider binaries) must skip refresh
    /// entirely, so `refresh_then_plan(cfg, state, None)` behaves
    /// EXACTLY like the pre-fix bare `magma_plan::plan` call. If this
    /// regressed to always building `Some`, every `structural_apply:
    /// true` test in this module would attempt to spawn a real provider
    /// process and fail/hang in CI.
    #[tokio::test]
    async fn refresh_ctx_for_skips_refresh_under_structural_apply() {
        let exec = MagmaExecutor::new(fixture_config());
        let tmp = tempfile::tempdir().unwrap();
        let cfg = magma_config::Config::from_json(json!({
            "provider": { "aws": { "region": "us-east-1" } },
        }))
        .unwrap();

        assert!(
            exec.refresh_ctx_for(tmp.path(), &cfg).is_none(),
            "structural_apply: true must route plan() through \
             refresh_then_plan(cfg, state, None) — the exact pre-fix \
             bare-plan behavior — never attempt a real provider RPC"
        );
    }

    /// The actual fix: outside test mode (`structural_apply: false`,
    /// the production default — see `MagmaExecutorConfig`'s `Default`
    /// impl), `plan()` must build a REAL `ApplyContext` and route
    /// through `refresh_then_plan(cfg, state, Some(&ctx))`, not bare
    /// `plan`. Also pins the credential-forwarding shape: a plan-time
    /// refresh RPC needs the same real provider credentials
    /// `apply()`/`destroy()` already thread through
    /// `build_provider_configs` (`spec.providerCredentials` merged with
    /// the rendered config's own `provider "x" {}` blocks) — without
    /// this, a refresh `ReadResource` against a provider with no
    /// rendered block (rio's cloudflare provider is the documented
    /// live example) would dial with a null config exactly like the
    /// credential-drop incident `build_provider_configs_drops_spec_provider_credentials_when_constructed_credential_blind`
    /// above closed for apply/destroy.
    #[tokio::test]
    async fn refresh_ctx_for_builds_a_real_credential_aware_context_in_production_mode() {
        let tmp = tempfile::tempdir().unwrap();
        let cfg = magma_config::Config::from_json(json!({
            // No rendered "provider" block for aws at all — the exact
            // shape a provider whose credentials live ONLY in
            // spec.providerCredentials has (rio's cloudflare provider).
            "resource": { "aws_iam_role": { "r": { "name": "x" } } },
        }))
        .unwrap();

        let mut aware_cfg = fixture_config();
        aware_cfg.structural_apply = false;
        aware_cfg.provider_configs.insert(
            "aws".to_string(),
            json!({
                "region": "us-east-1",
                "access_key": "AKIA_FAKE",
                "secret_key": "fake-secret",
            }),
        );
        let exec = MagmaExecutor::new(aware_cfg);

        let ctx = exec.refresh_ctx_for(tmp.path(), &cfg).expect(
            "structural_apply: false must route plan() through \
                 refresh_then_plan(cfg, state, Some(&ctx)) — a real \
                 ApplyContext, not None",
        );
        assert_eq!(
            ctx.workspace_dir,
            tmp.path(),
            "the refresh ApplyContext must be scoped to the same \
             work_dir plan() was called with (provider plugin \
             resolution reads from it)"
        );
        assert_eq!(
            // Keyed by the typed `ProviderInstance` since magma db764fa
            // (provider aliases), so a bare `"aws"` no longer indexes it.
            // Looked up by `name()` rather than by constructing the default
            // instance: what this test asserts is that the aws credentials
            // arrived at all, which is true of whichever instance carries
            // them.
            ctx.provider_configs
                .iter()
                .find(|(instance, _)| instance.name() == "aws")
                .map(|(_, v)| v)
                .and_then(|v| v.get("access_key"))
                .and_then(|v| v.as_str()),
            Some("AKIA_FAKE"),
            "the refresh ApplyContext must carry spec.providerCredentials \
             through to the ReadResource RPC even when the rendered \
             config has no matching provider block — the exact \
             credential-drop shape build_provider_configs already \
             closed for apply()/destroy()"
        );
    }

    #[tokio::test]
    async fn import_is_idempotent_when_already_in_state() {
        let exec = MagmaExecutor::new(fixture_config());
        let tmp = tempfile::tempdir().unwrap();

        let first = exec
            .import(tmp.path(), "aws_iam_role.adopted", "role-id")
            .await
            .unwrap();
        assert!(first.success);
        assert!(first.stdout.contains("adopted into state"));

        let second = exec
            .import(tmp.path(), "aws_iam_role.adopted", "role-id")
            .await
            .unwrap();
        assert!(second.success);
        assert!(second.stdout.contains("already present in state"));

        let backend = exec.make_backend();
        let state = magma_backend::Backend::read_state(&backend).await.unwrap();
        assert_eq!(state.resources.len(), 1, "idempotent: no duplicate");
    }

    #[tokio::test]
    async fn import_surfaces_a_failed_id_as_a_typed_failure_not_a_hard_error() {
        let exec = MagmaExecutor::new(fixture_config());
        let tmp = tempfile::tempdir().unwrap();

        let result = exec
            .import(tmp.path(), "aws_iam_role.gone", "missing")
            .await
            .unwrap();
        assert!(!result.success);
        assert!(result.stderr.contains("no resource with id"));

        // A failed import must never be silently persisted into state.
        let backend = exec.make_backend();
        let state = magma_backend::Backend::read_state(&backend).await.unwrap();
        assert_eq!(state.resources.len(), 0);
    }

    /// The concurrency-race regression `acquire_import_state_lock` closes:
    /// `run_import_prepass` (template_controller.rs) dispatches every
    /// resolved import target CONCURRENTLY via `buffer_unordered(10)`,
    /// building a FRESH `MagmaExecutor` per call
    /// (`state.executor_for(template)`) that all share the SAME
    /// underlying state backend. Without the lock, N concurrent
    /// read-modify-write cycles against the same workspace race and only
    /// the last writer's single resource survives.
    #[tokio::test]
    async fn import_serializes_concurrent_calls_against_the_same_workspace() {
        let shared_backend = Arc::new(InMemoryStateBackend::new());
        let build = || {
            MagmaExecutor::new(MagmaExecutorConfig {
                state_backend: Arc::clone(&shared_backend),
                ..fixture_config()
            })
        };
        let tmp = tempfile::tempdir().unwrap();

        let e1 = build();
        let e2 = build();
        let e3 = build();
        let (r1, r2, r3) = tokio::join!(
            e1.import(tmp.path(), "aws_iam_role.one", "id-1"),
            e2.import(tmp.path(), "aws_iam_role.two", "id-2"),
            e3.import(tmp.path(), "aws_iam_role.three", "id-3"),
        );
        assert!(r1.unwrap().success);
        assert!(r2.unwrap().success);
        assert!(r3.unwrap().success);

        let read_exec = build();
        let backend = read_exec.make_backend();
        let state = magma_backend::Backend::read_state(&backend).await.unwrap();
        assert_eq!(
            state.resources.len(),
            3,
            "all three concurrent imports must land — a lost update means the \
             in-process lock isn't serializing the read-modify-write cycle"
        );
    }

    #[tokio::test]
    async fn planned_changes_returns_none_on_disk_fallback() {
        // A magma executor WITHOUT an artifact store (the unit-test /
        // interim disk-fallback path) produces no typed changes here — it
        // returns Ok(None) so the import prepass routes to the legacy
        // `show_plan` path. This exercises the tofu/None branch the fix
        // preserves for back-compat.
        let exec = MagmaExecutor::new(fixture_config());
        let out = exec.planned_changes().await.unwrap();
        assert!(out.is_none(), "disk-fallback magma must return Ok(None)");
    }

    // ── plan_reuse_is_current — the apply-phase plan-reuse gate ──────────
    //
    // The sibling of the controller's `rendered_config_is_current` gate.
    // These pin the decision `read_db_plan` makes when a plan row IS
    // present: reuse it (→ `DbPlanRead::Present`) ONLY when its recorded
    // `source_revision` still matches the render's current revision;
    // otherwise treat it as stale (→ `DbPlanRead::Missing` → the apply /
    // planned_changes path regenerates the plan from the fresh render+state
    // rather than applying a stale noOp plan). This is the last staleness
    // layer: without it, a git-HEAD advance (render re-derived to
    // enabled=true) left the apply reusing a plan computed from the OLD
    // render (enabled=false vs state enabled=false → noOp), so an org.yaml
    // change silently never converged.

    #[test]
    fn plan_stale_when_git_head_advanced_forces_recompute() {
        // THE HEADLINE REGRESSION. A cached plan computed at the prior HEAD
        // is stale once the render re-derives at the new HEAD (the render
        // fix stamps the render's source_revision to the new HEAD). A stale
        // plan must NOT be reused — it recomputes against the fresh render.
        let old_head = "1111111111111111111111111111111111111111";
        let new_head = "2222222222222222222222222222222222222222";
        assert!(
            !plan_reuse_is_current(Some(old_head), Some(new_head)),
            "a plan computed at an OLD HEAD must be stale once the render \
             advances to a NEW HEAD — reuse it and a source change never converges"
        );
    }

    #[test]
    fn plan_current_when_revisions_match_is_reused() {
        // Steady state: the plan was computed from the render currently in
        // the store (same revision) → reuse it (the normal apply path).
        let head = "abc1234abc1234abc1234abc1234abc1234abc12";
        assert!(
            plan_reuse_is_current(Some(head), Some(head)),
            "a plan whose revision matches the current render must be reused"
        );
    }

    #[test]
    fn plan_stale_when_inline_content_revision_changed() {
        // Non-git (inline / configMap) source: the render's revision is a
        // `cm:` content hash. A changed source content ⇒ new render revision
        // ⇒ the plan computed at the old content hash is stale.
        let old_rev = "cm:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let new_rev = "cm:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
        assert!(
            !plan_reuse_is_current(Some(old_rev), Some(new_rev)),
            "a plan computed at an old inline-content revision must be stale after an edit"
        );
    }

    #[test]
    fn plan_legacy_null_revision_forces_recompute() {
        // A legacy plan row written before plan revisions were recorded
        // carries NULL — it can't be proven current, so recompute once
        // (converge-by-one-recompute, matching the render gate's legacy arm).
        let head = "9999999999999999999999999999999999999999";
        assert!(
            !plan_reuse_is_current(None, Some(head)),
            "a legacy/NULL-revision plan must not be reused — recompute once"
        );
        // And a plan present with no derivable render revision (no render row
        // / legacy render): also can't be proven current → recompute.
        assert!(
            !plan_reuse_is_current(Some(head), None),
            "a plan with no derivable current render revision must not be reused"
        );
        // Both unknown → recompute.
        assert!(!plan_reuse_is_current(None, None));
    }

    #[tokio::test]
    async fn read_db_plan_reports_not_db_backed_without_artifact_store() {
        // The disk-fallback fixture has `artifact_store: None`. `read_db_plan`
        // must classify that as `NotDbBacked` (routes to the disk checkpoint
        // in apply / the legacy show_plan in planned_changes) — NOT as a
        // Missing cache-miss (which would regenerate) and NOT as an Err.
        let exec = MagmaExecutor::new(fixture_config());
        let read = exec.read_db_plan().await.unwrap();
        assert!(
            matches!(read, DbPlanRead::NotDbBacked),
            "no artifact store → DbPlanRead::NotDbBacked (disk-fallback path)"
        );
    }

    #[tokio::test]
    async fn apply_regenerates_missing_disk_checkpoint_is_a_hard_io_error_but_present_plan_applies()
    {
        // Regression guard for the Reaction-B refactor: the DB-backed
        // never-stuck regeneration path needs a live Postgres ArtifactStore
        // (integration-only), but the disk-fallback contract must be
        // unchanged by the three-state `DbPlanRead` refactor. A plan()
        // followed by apply() (DbPlanRead::NotDbBacked → disk checkpoint)
        // must still round-trip through the in-memory backend.
        let exec = MagmaExecutor::new(fixture_config());
        let tmp = tempfile::tempdir().unwrap();
        render_workspace(
            tmp.path(),
            &json!({
                "provider": { "aws": { "region": "us-east-1" } },
                "resource": { "aws_iam_role": { "node": { "name": "regen" } } },
            }),
        )
        .await;
        exec.plan(tmp.path(), None, &[]).await.unwrap();
        let apply = exec.apply(tmp.path(), None, true).await.unwrap();
        assert!(
            apply.success,
            "disk-fallback apply-after-plan must still succeed post-refactor: {}",
            apply.stderr
        );
    }

    #[test]
    fn planned_changes_from_magma_plan_maps_create_managed_only() {
        use crate::executor::plan_change::{PlanAction, ResourceKindClass};

        let addr =
            |kind: magma_types::ResourceKind, ty: &str, name: &str| magma_types::ResourceAddress {
                module: magma_types::ModulePath::root(),
                kind,
                type_id: magma_types::ResourceTypeId(ty.into()),
                name: name.into(),
                key: None,
            };
        let plan = magma_types::Plan {
            id: magma_types::PlanId([0u8; 32]),
            created_at: chrono::Utc::now(),
            config_root: std::path::PathBuf::from("/nonexistent"),
            variables: Default::default(),
            resource_changes: vec![
                // A create-that-exists managed resource — the breathe class.
                magma_types::ResourceChange {
                    address: addr(
                        magma_types::ResourceKind::Managed,
                        "github_repository",
                        "breathe",
                    ),
                    action: magma_types::Action::Create,
                    before: None,
                    after: Some(json!({ "name": "breathe" })),
                    reasons: vec![magma_types::ChangeReason::NewResource],
                    meta: Default::default(),
                },
                // A data source read — must NOT appear in discovery.
                magma_types::ResourceChange {
                    address: addr(magma_types::ResourceKind::Data, "github_user", "me"),
                    action: magma_types::Action::Read,
                    before: None,
                    after: Some(json!({ "login": "me" })),
                    reasons: vec![],
                    meta: Default::default(),
                },
            ],
            output_changes: vec![],
            observation: Default::default(),
        };

        // No state: the mapper still must be total, and every derived id it
        // produces for a create must be honestly labelled.
        let changes = planned_changes_from_magma_plan(&plan, &empty_state());
        assert_eq!(changes.len(), 2, "mapper is total over resource_changes");

        // The managed create renders `{type}.{name}`, carries its planned
        // `after`, and is classed Managed.
        let create = &changes[0];
        assert_eq!(create.address, "github_repository.breathe");
        assert_eq!(create.action, PlanAction::Create);
        assert_eq!(create.kind, ResourceKindClass::Managed);
        assert_eq!(
            create
                .after
                .as_ref()
                .and_then(|a| a.get("name"))
                .and_then(|v| v.as_str()),
            Some("breathe")
        );

        // The data source is Read/Data — discovery excludes it downstream.
        assert_eq!(changes[1].action, PlanAction::Read);
        assert_eq!(changes[1].kind, ResourceKindClass::Data);
    }

    // ── the pleme-io-opensource wedge: a parent-keyed import id ─────────
    //
    // 49 `github_issue_label` creates + 5 `github_branch_protection` creates
    // failed every cycle against resources that plainly exist on GitHub.
    // The proactive prepass composed their import ids by substituting the
    // plan's `after` into a template, and `after.repository` on a label is
    // an unresolved reference — `"${github_repository.banken.name}"` — so
    // the id was `${github_repository.banken.name}:bug`. Every GET 404'd,
    // nothing was adopted, and the creates re-collided forever.

    fn state_with_repo(resource_name: &str, real_name: &str) -> magma_types::State {
        let mut st = empty_state();
        st.resources.push(magma_types::StateResource {
            address: magma_types::ResourceAddress {
                module: magma_types::ModulePath::root(),
                kind: magma_types::ResourceKind::Managed,
                type_id: magma_types::ResourceTypeId("github_repository".into()),
                name: resource_name.into(),
                key: None,
            },
            provider: magma_types::ProviderReference {
                source: "registry.terraform.io/integrations/github".into(),
                name: "github".into(),
                alias: None,
            },
            instances: vec![magma_types::StateInstance {
                index_key: None,
                attributes: json!({ "name": real_name, "node_id": "R_kgDOTdnhyQ" }),
                schema_version: 0,
                sensitive_attribute_paths: vec![],
                private: vec![],
                dependencies: vec![],
                status: magma_types::InstanceStatus::Ready,
            }],
        });
        st
    }

    fn create(ty: &str, name: &str, after: serde_json::Value) -> magma_types::ResourceChange {
        magma_types::ResourceChange {
            address: magma_types::ResourceAddress {
                module: magma_types::ModulePath::root(),
                kind: magma_types::ResourceKind::Managed,
                type_id: magma_types::ResourceTypeId(ty.into()),
                name: name.into(),
                key: None,
            },
            action: magma_types::Action::Create,
            before: None,
            after: Some(after),
            reasons: vec![magma_types::ChangeReason::NewResource],
            meta: Default::default(),
        }
    }

    fn plan_of(changes: Vec<magma_types::ResourceChange>) -> magma_types::Plan {
        magma_types::Plan {
            id: magma_types::PlanId([0u8; 32]),
            created_at: chrono::Utc::now(),
            config_root: std::path::PathBuf::from("/nonexistent"),
            variables: Default::default(),
            resource_changes: changes,
            output_changes: vec![],
            observation: Default::default(),
        }
    }

    /// The exact live case. `github_issue_label.banken-label-bug` must yield
    /// `banken:bug` — the format the GitHub provider's importer accepts
    /// (`repository:name`, integrations/github 6.13.0) — and must carry NO
    /// trace of the reference it was composed from.
    #[test]
    fn a_label_create_derives_the_repo_scoped_import_id_not_the_raw_reference() {
        let plan = plan_of(vec![create(
            "github_issue_label",
            "banken-label-bug",
            json!({ "repository": "${github_repository.banken.name}", "name": "bug" }),
        )]);
        let changes = planned_changes_from_magma_plan(&plan, &state_with_repo("banken", "banken"));
        let got = changes[0].import_id.clone().expect("an id is derivable");
        assert_eq!(got.id, "banken:bug");
        assert!(got.exact, "a fully state-resolved parent is exact");
        assert!(
            !got.id.contains("${"),
            "raw reference leaked into the import id"
        );
    }

    /// Five of the nine label parents are underscore-sanitized in state
    /// (`caixa_tlisp_handoff`) while the repository is really named
    /// `caixa-tlisp-handoff`. Resolving the parent through state's own
    /// `name` attribute — rather than off the resource address — is what
    /// makes those 29 labels derivable at all.
    #[test]
    fn an_underscored_resource_name_resolves_to_the_real_repo_name() {
        let plan = plan_of(vec![create(
            "github_issue_label",
            "caixa-tlisp-handoff-label-bug",
            json!({ "repository": "${github_repository.caixa_tlisp_handoff.name}", "name": "bug" }),
        )]);
        let changes = planned_changes_from_magma_plan(
            &plan,
            &state_with_repo("caixa_tlisp_handoff", "caixa-tlisp-handoff"),
        );
        let got = changes[0].import_id.clone().expect("an id is derivable");
        assert_eq!(got.id, "caixa-tlisp-handoff:bug");
        assert!(got.exact);
    }

    /// A branch protection's `repository_id` is a `${…node_id}` reference,
    /// yet the importer keys on `repository:pattern`. Neither the raw
    /// reference nor the node id it projects is importable.
    #[test]
    fn a_branch_protection_derives_repo_name_colon_pattern() {
        let plan = plan_of(vec![create(
            "github_branch_protection",
            "cse_lint_main",
            json!({ "repository_id": "${github_repository.cse_lint.node_id}", "pattern": "main" }),
        )]);
        let changes =
            planned_changes_from_magma_plan(&plan, &state_with_repo("cse_lint", "cse-lint"));
        let got = changes[0].import_id.clone().expect("an id is derivable");
        assert_eq!(got.id, "cse-lint:main");
        assert!(got.exact);
        assert!(
            !got.id.contains("R_kgDO"),
            "node id leaked into the import id"
        );
    }

    /// Never round up. With the parent absent from state the only available
    /// id is the convention guess, and it must be marked inexact so the
    /// prepass refuses to dispatch it.
    #[test]
    fn an_unresolvable_parent_is_carried_as_inexact_never_as_exact() {
        let plan = plan_of(vec![create(
            "github_issue_label",
            "tag-forge-label-bug",
            json!({ "repository": "${github_repository.tag_forge.name}", "name": "bug" }),
        )]);
        let changes = planned_changes_from_magma_plan(&plan, &empty_state());
        let got = changes[0]
            .import_id
            .clone()
            .expect("a guess is still returned");
        assert_eq!(got.id, "tag_forge:bug");
        assert!(
            !got.exact,
            "a convention-guessed parent must never be labelled exact — \
             tag_forge is not the repo name, tag-forge is"
        );
    }

    /// Only creates can collide, so only creates get an adoption id.
    #[test]
    fn a_non_create_never_derives_an_import_id() {
        let mut ch = create(
            "github_issue_label",
            "l",
            json!({ "repository": "r", "name": "bug" }),
        );
        ch.action = magma_types::Action::Update;
        let changes = planned_changes_from_magma_plan(&plan_of(vec![ch]), &empty_state());
        assert_eq!(changes[0].import_id, None);
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
        let make_executor = || {
            MagmaExecutor::new(MagmaExecutorConfig {
                structural_apply: true,
                state_backend: Arc::clone(&store),
                schema_name: "s".into(),
                template_name: "t".into(),
                state_name: "default".into(),
                backend_shape: BackendShape::Magma,
                plan_checkpoint: true,
                apply_quantum_secs: None,
                preflight_laws: false,
                drift_policy: magma_drift::DriftPolicy::conservative_default(),
                audit_log_path: None,
                artifact_store: None,
                provider_configs: std::collections::BTreeMap::new(),
            })
        };

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
        assert!(
            checkpoint.exists(),
            "checkpoint must survive across instances"
        );

        let state_before = store.get_state("s", "t", "default").await.unwrap();
        assert!(
            state_before.is_none(),
            "state must be untouched until apply"
        );

        // Instance #2: apply from the existing checkpoint.
        let exec2 = make_executor();
        let apply_result = exec2.apply(tmp.path(), None, true).await.unwrap();
        assert!(
            apply_result.success,
            "instance 2 must apply from checkpoint"
        );
        assert_eq!(apply_result.exit_code, 0);

        let state_after = store
            .get_state("s", "t", "default")
            .await
            .unwrap()
            .expect("state must exist after apply");
        let parsed: serde_json::Value = serde_json::from_slice(&state_after.data.unwrap()).unwrap();
        assert!(parsed["resources"].as_array().unwrap().len() >= 1);
    }

    #[tokio::test]
    async fn separate_state_names_are_isolated() {
        // Two MagmaExecutor instances pointed at the SAME backend
        // but with different state_name slots must never see each
        // other's resources.
        let store: Arc<InMemoryStateBackend> = Arc::new(InMemoryStateBackend::new());
        let make = |state_name: &str| {
            MagmaExecutor::new(MagmaExecutorConfig {
                structural_apply: true,
                state_backend: Arc::clone(&store),
                schema_name: "s".into(),
                template_name: "t".into(),
                state_name: state_name.into(),
                backend_shape: BackendShape::Magma,
                plan_checkpoint: true,
                apply_quantum_secs: None,
                preflight_laws: false,
                drift_policy: magma_drift::DriftPolicy::conservative_default(),
                audit_log_path: None,
                artifact_store: None,
                provider_configs: std::collections::BTreeMap::new(),
            })
        };

        let tmp_a = tempfile::tempdir().unwrap();
        let tmp_b = tempfile::tempdir().unwrap();
        render_workspace(
            tmp_a.path(),
            &json!({
                "provider": { "aws": { "region": "us-east-1" } },
                "resource": { "aws_iam_role": { "rA": { "name": "A" } } },
            }),
        )
        .await;
        render_workspace(
            tmp_b.path(),
            &json!({
                "provider": { "aws": { "region": "us-east-1" } },
                "resource": { "aws_iam_role": { "rB": { "name": "B" } } },
            }),
        )
        .await;

        let exec_a = make("slot_a");
        let exec_b = make("slot_b");
        exec_a.plan(tmp_a.path(), None, &[]).await.unwrap();
        exec_a.apply(tmp_a.path(), None, true).await.unwrap();
        exec_b.plan(tmp_b.path(), None, &[]).await.unwrap();
        exec_b.apply(tmp_b.path(), None, true).await.unwrap();

        let entry_a = store.get_state("s", "t", "slot_a").await.unwrap().unwrap();
        let entry_b = store.get_state("s", "t", "slot_b").await.unwrap().unwrap();
        let parsed_a: serde_json::Value = serde_json::from_slice(&entry_a.data.unwrap()).unwrap();
        let parsed_b: serde_json::Value = serde_json::from_slice(&entry_b.data.unwrap()).unwrap();
        let names_a: Vec<&str> = parsed_a["resources"]
            .as_array()
            .unwrap()
            .iter()
            .map(|r| r["address"]["name"].as_str().unwrap_or(""))
            .collect();
        let names_b: Vec<&str> = parsed_b["resources"]
            .as_array()
            .unwrap()
            .iter()
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
        exec.plan(tmp.path(), Some(&custom_plan), &[])
            .await
            .unwrap();
        assert!(custom_plan.exists(), "plan written to explicit path");
        assert!(
            !tmp.path().join("magma-plan.json").exists(),
            "default checkpoint should not exist when explicit plan_file is set",
        );
        let apply_result = exec
            .apply(tmp.path(), Some(&custom_plan), true)
            .await
            .unwrap();
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
            structural_apply: true,
            state_backend: Arc::new(InMemoryStateBackend::new()),
            schema_name: "s".into(),
            template_name: "t".into(),
            state_name: "default".into(),
            backend_shape: BackendShape::Tofu,
            plan_checkpoint: true,
            apply_quantum_secs: None,
            preflight_laws: false,
            drift_policy: magma_drift::DriftPolicy::conservative_default(),
            audit_log_path: None,
            artifact_store: None,
            provider_configs: std::collections::BTreeMap::new(),
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
        // magma's `tfstate_v4` canonicalizes provider references to
        // `registry.opentofu.org` (what a real `tofu apply` actually
        // writes), not `registry.terraform.io` — see
        // `magma-operator-backend/src/lib.rs`'s own module doc example
        // and `magma-state/src/tfstate_v4.rs:35`. This assertion
        // pre-dates that (correct, upstream-aligned) magma behavior
        // change and was never updated to match.
        assert!(
            provider.contains("registry.opentofu.org/hashicorp/aws"),
            "expected canonical tofu provider form, got: {provider}",
        );
    }

    #[tokio::test]
    async fn preflight_rejects_dangling_reference() {
        // With preflight enabled, a malformed workspace (dangling
        // reference) is refused at the controller layer — the live
        // backend never sees it. This is the substrate's "promises
        // become theorems" property in operator form.
        let cfg = MagmaExecutorConfig {
            structural_apply: true, // (moot: this test rejects at preflight, never applies)
            state_backend: Arc::new(InMemoryStateBackend::new()),
            schema_name: "s".into(),
            template_name: "t".into(),
            state_name: "default".into(),
            backend_shape: BackendShape::Magma,
            plan_checkpoint: true,
            apply_quantum_secs: None,
            preflight_laws: true, // production default
            drift_policy: magma_drift::DriftPolicy::conservative_default(),
            audit_log_path: None,
            artifact_store: None,
            provider_configs: std::collections::BTreeMap::new(),
        };
        let exec = MagmaExecutor::new(cfg);
        let tmp = tempfile::tempdir().unwrap();
        render_workspace(
            tmp.path(),
            &json!({
                "terraform": {
                    "required_providers": {
                        "aws": { "source": "hashicorp/aws" }
                    }
                },
                "resource": {
                    "aws_subnet": {
                        "web": {
                            // Dangling — aws_vpc.main is not declared.
                            "vpc_id":     "${aws_vpc.main.id}",
                            "cidr_block": "10.0.1.0/24"
                        }
                    }
                }
            }),
        )
        .await;
        let result = exec.plan(tmp.path(), None, &[]).await;
        match result {
            Err(Error::MagmaExecution(msg)) => {
                assert!(
                    msg.contains("substrate preflight violations"),
                    "expected preflight rejection, got: {msg}",
                );
                assert!(
                    msg.contains("dangling reference"),
                    "expected dangling-ref reason in error, got: {msg}",
                );
            }
            other => panic!("expected MagmaExecution preflight error, got: {other:?}"),
        }
    }

    #[tokio::test]
    async fn audit_log_writes_chained_jsonl_events() {
        // With audit_log_path set, plan() writes BLAKE3-chained
        // typed events to the configured path. The same events
        // are captured into Bundle.audit; chain verification
        // against the on-disk file passes.
        let tmp = tempfile::tempdir().unwrap();
        let log_path = tmp.path().join("audit.jsonl");
        let mut cfg = fixture_config();
        cfg.audit_log_path = Some(log_path.clone());
        let exec = MagmaExecutor::new(cfg);
        render_workspace(
            tmp.path(),
            &json!({
                "provider": { "aws": { "region": "us-east-1" } },
                "resource": { "aws_iam_role": { "n": { "name": "audit-role" } } }
            }),
        )
        .await;
        exec.plan(tmp.path(), None, &[]).await.unwrap();

        // Audit file exists + contains JSONL events.
        assert!(log_path.exists());
        let text = std::fs::read_to_string(&log_path).unwrap();
        let event_count = text.lines().filter(|l| !l.trim().is_empty()).count();
        // plan() emits 2 events: PlanComputed + DriftClassified.
        assert_eq!(event_count, 2);

        // Bundle.audit captured the same chain.
        let bundle_path = tmp.path().join("magma-bundle.json");
        let bundle: magma_bundle::Bundle =
            serde_json::from_slice(&std::fs::read(&bundle_path).unwrap()).unwrap();
        assert_eq!(bundle.audit.len(), 2);
        // verify the chain end-to-end.
        magma_stream::verify_chain(&bundle.audit).unwrap();
    }

    // Asserts the attestation is PRESENT, which is precisely what the
    // feature controls — a Ruby-free build has no gem attester linked and
    // correctly returns None. Gated rather than softened: weakening the
    // assertion to "present or None" would make it pass in both builds
    // while proving nothing in either.
    #[cfg(feature = "magma_rubygems")]
    #[tokio::test]
    async fn plan_carries_gem_tree_attestation_when_gemfile_lock_present() {
        // When the workspace ships a Gemfile.lock, the bundle
        // emitted by plan() carries the BLAKE3 attestation of the
        // resolved gem closure. Compliance teams export the bundle
        // + verify the gem-tree identity end-to-end.
        let exec = MagmaExecutor::new(fixture_config());
        let tmp = tempfile::tempdir().unwrap();
        render_workspace(
            tmp.path(),
            &json!({
                "provider": { "aws": { "region": "us-east-1" } },
                "resource": { "aws_iam_role": { "node": { "name": "n" } } }
            }),
        )
        .await;
        // Drop a minimal Pangea-shaped Gemfile.lock into the workspace.
        let lock = r#"GEM
  remote: https://rubygems.org/
  specs:
    rspec (3.12.0)

PLATFORMS
  ruby

DEPENDENCIES
  rspec (~> 3.12)

BUNDLED WITH
   2.5.22
"#;
        tokio::fs::write(tmp.path().join("Gemfile.lock"), lock)
            .await
            .unwrap();

        exec.plan(tmp.path(), None, &[]).await.unwrap();
        let bundle_path = tmp.path().join("magma-bundle.json");
        let bytes = std::fs::read(&bundle_path).unwrap();
        let bundle: magma_bundle::Bundle = serde_json::from_slice(&bytes).unwrap();
        bundle.verify().unwrap();
        let attestation = bundle.gem_tree_attestation.as_deref().unwrap();
        assert_eq!(
            attestation.len(),
            64,
            "gem-tree attestation should be 64-hex BLAKE3"
        );
    }

    #[tokio::test]
    async fn plan_omits_gem_tree_attestation_when_no_gemfile_lock() {
        let exec = MagmaExecutor::new(fixture_config());
        let tmp = tempfile::tempdir().unwrap();
        render_workspace(
            tmp.path(),
            &json!({
                "provider": { "aws": { "region": "us-east-1" } },
                "resource": { "aws_iam_role": { "node": { "name": "n" } } }
            }),
        )
        .await;
        // No Gemfile.lock at workspace root.
        exec.plan(tmp.path(), None, &[]).await.unwrap();
        let bundle_path = tmp.path().join("magma-bundle.json");
        let bytes = std::fs::read(&bundle_path).unwrap();
        let bundle: magma_bundle::Bundle = serde_json::from_slice(&bytes).unwrap();
        assert!(bundle.gem_tree_attestation.is_none());
    }

    #[tokio::test]
    async fn plan_emits_bundle_to_disk_with_matching_plan_id() {
        let exec = MagmaExecutor::new(fixture_config());
        let tmp = tempfile::tempdir().unwrap();
        render_workspace(
            tmp.path(),
            &json!({
                "provider": { "aws": { "region": "us-east-1" } },
                "resource": {
                    "aws_iam_role": { "node": { "name": "bundle-role" } }
                }
            }),
        )
        .await;
        let result = exec.plan(tmp.path(), None, &[]).await.unwrap();

        // Stdout JSON carries the bundle_id + phase.
        let parsed: serde_json::Value = serde_json::from_str(&result.stdout).unwrap();
        assert!(
            parsed["bundle"].is_object(),
            "plan stdout missing bundle block"
        );
        let bundle_id = parsed["bundle"]["bundle_id"].as_str().unwrap();
        assert_eq!(
            bundle_id.len(),
            64,
            "bundle_id should be 64-char BLAKE3 hex"
        );
        assert_eq!(parsed["bundle"]["phase"], "Planning");

        // Bundle file materialized on disk.
        let bundle_path = tmp.path().join("magma-bundle.json");
        assert!(
            bundle_path.exists(),
            "magma-bundle.json should exist after plan"
        );
        let bytes = std::fs::read(&bundle_path).unwrap();
        let bundle: magma_bundle::Bundle = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(bundle.bundle_id, bundle_id);
        bundle
            .verify()
            .unwrap_or_else(|e| panic!("bundle.verify(): {e:?}"));
    }

    #[tokio::test]
    async fn apply_walks_lifecycle_to_stable_and_re_emits_bundle() {
        let exec = MagmaExecutor::new(fixture_config());
        let tmp = tempfile::tempdir().unwrap();
        render_workspace(
            tmp.path(),
            &json!({
                "provider": { "aws": { "region": "us-east-1" } },
                "resource": {
                    "aws_iam_role": { "node": { "name": "lifecycle-role" } }
                }
            }),
        )
        .await;
        exec.plan(tmp.path(), None, &[]).await.unwrap();
        let result = exec.apply(tmp.path(), None, false).await.unwrap();

        // Apply stdout carries the post-apply bundle reference.
        let parsed: serde_json::Value = serde_json::from_str(&result.stdout).unwrap();
        assert_eq!(parsed["bundle"]["phase"], "Stable");
        // Apply contributes 3 transitions (Applying, Verifying, Stable)
        // on top of plan()'s Planning transition; total = 4.
        assert_eq!(parsed["bundle"]["lifecycle_transitions"], 4);

        // Re-read bundle: should have Outcome + Stable phase + verify.
        let bundle_path = tmp.path().join("magma-bundle.json");
        let bytes = std::fs::read(&bundle_path).unwrap();
        let bundle: magma_bundle::Bundle = serde_json::from_slice(&bytes).unwrap();
        bundle
            .verify()
            .unwrap_or_else(|e| panic!("post-apply bundle.verify(): {e:?}"));
        assert_eq!(bundle.lifecycle.current, magma_fsm::Phase::Stable);
        assert!(
            bundle.outcome.is_some(),
            "post-apply bundle should carry an Outcome"
        );
        assert!(
            bundle.fully_succeeded(),
            "post-apply bundle.fully_succeeded()"
        );
    }

    // ── Regression: `advance_lifecycle_through_apply` recovers from
    //    every re-entrant starting phase ─────────────────────────────
    //
    // `MagmaExecutor::apply()` can be called more than once in a
    // single reconcile tick (the stale-plan self-heal retry in
    // `template_controller`, and the conflict-resolution import+
    // re-apply loop in `controller::conflict`). Each call re-loads
    // the PRIOR call's persisted `lifecycle.current`, which can be
    // ANY phase -- not just the `Planning`/`Approving` states that
    // `magma_fsm::is_transition_allowed` permits transitioning
    // directly into `Applying` from. Before the fix, the three
    // `.transition()` calls in `apply()` silently discarded their
    // `Err` via `let _ =`, so a re-entrant call starting from e.g.
    // `Failed` could never reach `Applying`/`Verifying`/`Stable`
    // again -- `lifecycle.current` froze at `"failed"` forever, even
    // when the retry actually converged, and that stale value flows
    // straight into `status.lastCycle.lifecyclePhase`.

    #[test]
    fn advance_lifecycle_through_apply_recovers_from_prior_failed_phase() {
        let mut lifecycle = magma_fsm::LifecycleState::new();
        // Drive it to Failed via the only legal path, simulating a
        // first apply() call in this reconcile tick that failed.
        lifecycle
            .transition(magma_fsm::Phase::Planning, None, "first apply: plan")
            .unwrap();
        lifecycle
            .transition(magma_fsm::Phase::Applying, None, "first apply: applying")
            .unwrap();
        lifecycle
            .transition(
                magma_fsm::Phase::Failed,
                None,
                "first apply: transient failure",
            )
            .unwrap();
        assert_eq!(lifecycle.current, magma_fsm::Phase::Failed);

        // Second apply() call in the SAME reconcile tick (e.g. the
        // stale-plan self-heal retry) -- this time it converges.
        advance_lifecycle_through_apply(
            &mut lifecycle,
            magma_converge::PlanId("retry-plan".into()),
            magma_fsm::Phase::Stable,
            "apply succeeded; state converged",
        );

        // Before the fix: `let _ =` silently dropped every transition
        // attempted from `Failed`, so `lifecycle.current` stayed
        // `Failed` here -- this assertion would have failed. After
        // the fix: the guaranteed `Failed -> Idle -> Planning ->
        // Applying -> Verifying -> Stable` reset path lands on the
        // TRUE outcome of this second, successful apply.
        assert_eq!(
            lifecycle.current,
            magma_fsm::Phase::Stable,
            "a converged retry must not stay stuck at a stale prior Failed phase"
        );
    }

    #[test]
    fn advance_lifecycle_through_apply_recovers_from_every_re_entrant_phase() {
        // Every phase `magma_fsm::Phase` defines is a phase a
        // re-loaded `prev_bundle.lifecycle.current` could legally be
        // (Idle only via `LifecycleState::new()`, which the `None`
        // branch already covers). Prove the helper reaches the
        // requested final phase from every one of them, not just the
        // originally-reported `Failed` case.
        for start in [
            magma_fsm::Phase::Idle,
            magma_fsm::Phase::Planning,
            magma_fsm::Phase::Approving,
            magma_fsm::Phase::Applying,
            magma_fsm::Phase::Verifying,
            magma_fsm::Phase::Stable,
            magma_fsm::Phase::Failed,
            magma_fsm::Phase::Retrying,
            magma_fsm::Phase::Refused,
        ] {
            let mut lifecycle = magma_fsm::LifecycleState {
                current: start,
                entered_at: chrono::Utc::now(),
                history: vec![],
            };
            advance_lifecycle_through_apply(
                &mut lifecycle,
                magma_converge::PlanId(format!("plan-from-{start:?}")),
                magma_fsm::Phase::Stable,
                "apply succeeded; state converged",
            );
            assert_eq!(
                lifecycle.current,
                magma_fsm::Phase::Stable,
                "re-entrant apply starting from {start:?} must reach Stable, not get stuck"
            );
        }
    }

    #[test]
    fn advance_lifecycle_through_apply_skips_reset_when_already_reachable() {
        // When `current` is already `Planning` (the normal single-shot
        // path, e.g. right after `plan()`), no reset hop through Idle
        // is needed -- preserves the existing 3-transition-per-apply
        // history shape asserted by
        // `apply_walks_lifecycle_to_stable_and_re_emits_bundle`.
        let mut lifecycle = magma_fsm::LifecycleState::new();
        lifecycle
            .transition(magma_fsm::Phase::Planning, None, "plan()")
            .unwrap();
        assert_eq!(lifecycle.len(), 1);

        advance_lifecycle_through_apply(
            &mut lifecycle,
            magma_converge::PlanId("p".into()),
            magma_fsm::Phase::Stable,
            "apply succeeded; state converged",
        );

        assert_eq!(lifecycle.current, magma_fsm::Phase::Stable);
        // Applying, Verifying, Stable -- no extra Idle/Planning reset hop.
        assert_eq!(lifecycle.len(), 4);
    }

    #[tokio::test]
    async fn plan_stdout_includes_drift_classification() {
        // Every plan emits a typed drift classification using the
        // configured DriftPolicy. Auto-fix Cosmetic, alert+auto-fix
        // Functional, require-approval for Critical (conservative
        // default).
        let exec = MagmaExecutor::new(fixture_config());
        let tmp = tempfile::tempdir().unwrap();
        render_workspace(
            tmp.path(),
            &json!({
                "provider": { "aws": { "region": "us-east-1" } },
                "resource": {
                    "aws_iam_role": { "node": { "name": "drift-role" } }
                }
            }),
        )
        .await;
        let result = exec.plan(tmp.path(), None, &[]).await.unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&result.stdout).unwrap();
        assert!(
            parsed["drift"].is_object(),
            "plan stdout missing drift block"
        );
        // Create of Functional severity routes to AutoCorrectWithAlert
        // under the conservative_default policy.
        let decisions = parsed["drift"]["decisions"].as_array().unwrap();
        assert_eq!(decisions.len(), 1);
        assert_eq!(decisions[0]["decision"], "auto_correct_with_alert");
        // Summary reflects the 1-change classification.
        let summary = &parsed["drift"]["summary"];
        assert_eq!(summary["total_changes"], 1);
        assert_eq!(summary["auto_corrected_with_alert"], 1);
    }
}

#[cfg(test)]
mod stall_diagnosis_tests {
    use super::diagnose_stall;
    use magma_apply::CycleStats;

    /// The exact stats from camelot-eks 2026-07-30. The old message told the
    /// reader to raise the quantum; these numbers say the quantum was 92%
    /// unused. This test exists so that advice can never come back.
    #[test]
    fn the_live_stall_is_not_blamed_on_the_quantum() {
        let stats = CycleStats {
            prologue_ms: 11,
            elapsed_ms: 9607,
            quantum_ms: Some(120_000),
            nodes_attempted: 9,
            nodes_completed: 0,
            nodes_remaining: 9,
            waves_entered: 2,
            max_wave_width: 8,
            node_rpc_ms_total: 9038,
            ..Default::default()
        };
        let msg = diagnose_stall(&stats, &[]);
        // With NO recorded reasons, the message must SAY the counts contradict
        // each other rather than render nothing — an empty failure list beside
        // "all 9 failed" is a bug in the engine, and silence would hide it.
        assert!(
            msg.contains("contradicts"),
            "an empty failure list beside a nonzero failure count must be called out, got: {msg}"
        );

        assert!(
            msg.contains("all 9 attempted node(s) failed"),
            "must name the real cause, got: {msg}"
        );
        assert!(
            msg.contains("raising it will not help"),
            "must say the quantum is not the constraint, got: {msg}"
        );
        assert!(
            !msg.contains("PANGEA_MAGMA_APPLY_QUANTUM_SECS"),
            "must NOT send the reader to a knob that cannot help, got: {msg}"
        );
    }

    /// The original hypothesis is still reachable -- when it is actually true.
    #[test]
    fn a_prologue_that_really_does_not_fit_still_says_so() {
        let stats = CycleStats {
            prologue_ms: 130_000,
            elapsed_ms: 130_000,
            quantum_ms: Some(120_000),
            nodes_attempted: 0,
            ..Default::default()
        };
        let msg = diagnose_stall(&stats, &[]);
        assert!(msg.contains("prologue"), "got: {msg}");
        assert!(
            msg.contains("PANGEA_MAGMA_APPLY_QUANTUM_SECS"),
            "this is the one case where that knob IS the fix, got: {msg}"
        );
    }

    #[test]
    fn nothing_attempted_points_at_the_graph_not_a_timer() {
        let stats = CycleStats {
            prologue_ms: 5,
            elapsed_ms: 40,
            quantum_ms: Some(120_000),
            nodes_attempted: 0,
            waves_entered: 1,
            max_wave_width: 0,
            ..Default::default()
        };
        let msg = diagnose_stall(&stats, &[]);
        assert!(msg.contains("no node was attempted"), "got: {msg}");
        assert!(
            !msg.contains("PANGEA_MAGMA_APPLY_QUANTUM_SECS"),
            "a timing knob cannot fix an empty wave, got: {msg}"
        );
    }

    /// An unrecognised shape must admit that rather than pick a cause.
    #[test]
    fn an_unknown_shape_says_it_is_unknown() {
        let stats = CycleStats {
            prologue_ms: 5,
            elapsed_ms: 100,
            quantum_ms: Some(120_000),
            nodes_attempted: 4,
            nodes_completed: 2,
            nodes_remaining: 2,
            ..Default::default()
        };
        let msg = diagnose_stall(&stats, &[]);
        assert!(
            msg.contains("not covered by the known stall shapes"),
            "got: {msg}"
        );
    }
}
