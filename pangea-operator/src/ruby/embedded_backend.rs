//! Embedded backend — sends typed requests to the [`RubyOwner`] thread.
//!
//! Cheap clone, async-friendly. Implements [`CompilerBackend`] so
//! reconcilers don't know whether they're talking to a sidecar or to
//! magnus.

use async_trait::async_trait;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot, Mutex};

use super::backend::{
    ArchListing, BackendError, CompileAnyRequest, CompileAnyResult, CompileRequest,
    CompileResult, CompilerBackend, FixtureOutcome, GemSource, SmokeRequest, SourceKind,
};
use super::gem_cache::GemCache;
use super::owner::RubyRequest;
use super::pool::RubyPool;

#[derive(Debug, Clone)]
struct PreparedGem {
    /// gem ref (the `(name, ref)` part of the cache key — used for
    /// idempotent skip on repeat prepare_gem calls).
    git_ref: String,
    /// Filesystem path the gem cloned to. smoke_test uses this to
    /// resolve relative fixture paths that Ruby's `Gem.loaded_specs`
    /// can't reach (the gem was cloned, not gem-installed).
    gem_path: PathBuf,
}

#[derive(Clone)]
pub struct EmbeddedCompilerBackend {
    /// Pool of ruby owner threads. One-shot requests round-robin
    /// across workers; PrependLoadPath broadcasts to all.
    pool: Arc<RubyPool>,
    cache: Option<GemCache>,
    /// Map of `gem_name → PreparedGem`. prepare_gem populates;
    /// smoke_test reads to resolve relative fixture paths.
    prepared: Arc<Mutex<std::collections::HashMap<String, PreparedGem>>>,
    /// Optional metrics handle. When present, every dispatcher call
    /// instruments: bumps `compile_queue_depth` before send, records
    /// `compile_request_seconds` after response, decrements depth.
    /// Optional so test/CLI callers can construct without a Metrics.
    metrics: Option<Arc<crate::observability::Metrics>>,
}

impl EmbeddedCompilerBackend {
    /// Construct without a gem cache — `prepare_gem` becomes a no-op.
    /// Useful for tests + for environments where gems are still
    /// image-baked (transitional M8.4 deployments).
    pub fn new(pool: Arc<RubyPool>) -> Self {
        Self {
            pool,
            cache: None,
            prepared: Arc::new(Mutex::new(Default::default())),
            metrics: None,
        }
    }

    /// Construct with an active gem cache. `prepare_gem` clones +
    /// broadcasts $LOAD_PATH prepend to every pool worker on first
    /// call per (name, ref). Production shape under M8.4.2.
    pub fn with_cache(pool: Arc<RubyPool>, cache: GemCache) -> Self {
        Self {
            pool,
            cache: Some(cache),
            prepared: Arc::new(Mutex::new(Default::default())),
            metrics: None,
        }
    }

    /// Attach a metrics handle. Every subsequent dispatcher call
    /// records `compile_queue_depth` (gauge) + `compile_request_seconds`
    /// (histogram). Returns self by value so it composes with the
    /// constructors:
    ///
    /// ```ignore
    /// EmbeddedCompilerBackend::with_cache(pool, cache).with_metrics(m)
    /// ```
    pub fn with_metrics(mut self, metrics: Arc<crate::observability::Metrics>) -> Self {
        self.metrics = Some(metrics);
        self
    }

    /// Wrap a send+await pair in queue-depth + request-duration
    /// instrumentation. The `body` closure receives a round-robin-
    /// picked `mpsc::Sender<RubyRequest>` and runs the actual send +
    /// oneshot await — see the four CompilerBackend methods for the
    /// call shape. Drops the gauge increment + histogram observation
    /// on the floor when no metrics handle is attached.
    async fn instrumented<F, Fut, T>(&self, body: F) -> Result<T, BackendError>
    where
        F: FnOnce(mpsc::Sender<RubyRequest>) -> Fut,
        Fut: std::future::Future<Output = Result<T, BackendError>>,
    {
        let started = std::time::Instant::now();
        if let Some(m) = &self.metrics {
            m.compile_queue_depth.inc();
        }
        let tx = self.pool.next_sender();
        let result = body(tx).await;
        if let Some(m) = &self.metrics {
            m.compile_queue_depth.dec();
            m.compile_request_seconds
                .observe(started.elapsed().as_secs_f64());
        }
        result
    }
}

#[async_trait]
impl CompilerBackend for EmbeddedCompilerBackend {
    async fn prepare_gem(&self, source: &GemSource) -> Result<(), BackendError> {
        // Dispatch on source.kind. Ruby is the existing M8.4.2 path;
        // Lisp + Wasm are reserved for terreno (M2 of TERRENO.md) +
        // the wasmtime backend respectively. Both return a typed
        // NotImplemented condition until their evaluators land —
        // the controller surfaces this as `GemPrepareFailed`.
        match source.kind {
            SourceKind::Ruby => {}
            SourceKind::Lisp => {
                return Err(BackendError::Ruby(
                    "source.kind=Lisp not yet implemented (terreno M2; theory/TERRENO.md)"
                        .into(),
                ));
            }
            SourceKind::Wasm => {
                return Err(BackendError::Ruby(
                    "source.kind=Wasm not yet implemented (M2+; wasmtime integration pending)"
                        .into(),
                ));
            }
        }

        let cache = match &self.cache {
            Some(c) => c,
            None => return Ok(()), // no cache → caller's gems are pre-bundled.
        };

        // Skip if we've already prepared this (name, ref).
        {
            let prepared = self.prepared.lock().await;
            if let Some(existing) = prepared.get(&source.name) {
                if existing.git_ref == source.git_ref {
                    return Ok(());
                }
            }
        }

        let entry = cache
            .ensure(&source.name, &source.git_url, &source.git_ref)
            .await
            .map_err(|e| BackendError::Ruby(format!("gem cache: {e}")))?;

        // Slice 2c+: defense-in-depth. Before broadcasting the gem's
        // lib to every pool worker, run the load-path conflict
        // detector against the OTHER cached gem libs we've already
        // prepared. Catches the same bug class as the workspace
        // shield (two paths exposing the same logical Ruby file)
        // at an earlier site — if two architecture gems ship the
        // same file, broadcasting the second one creates a latent
        // double-load that won't surface until compile.
        //
        // Findings here are warnings only (don't refuse the
        // broadcast); the workspace-compile shield is the
        // authoritative guard. This emits early audit signal so
        // the conflict is named at its source.
        {
            let prepared = self.prepared.lock().await;
            let existing_libs: Vec<std::path::PathBuf> = prepared
                .values()
                .map(|p| p.gem_path.join("lib"))
                .filter(|p| p.is_dir())
                .collect();
            let detector = pangea_ruby_eval::LoadPathConflictDetector::pangea();
            // We pass entry.lib_path as the "new" path + existing
            // cached libs as the "existing" $LOAD_PATH. Same trait
            // shape, different site.
            use pangea_ruby_eval::ConflictDetector;
            let ctx = pangea_ruby_eval::CompileContext::new()
                .with_load_paths(vec![entry.lib_path.clone()]);
            for c in detector.detect(&ctx, &existing_libs) {
                tracing::warn!(
                    gem = %source.name,
                    git_ref = %source.git_ref,
                    detector = c.detector,
                    category = %c.category,
                    warning = %c.message,
                    "prepare_gem load-path conflict — gem broadcast will create double-load risk"
                );
            }
        }

        // Broadcast lib/ prepend to every pool worker — each VM has
        // its own $LOAD_PATH, so we have to fan out. The gem clone
        // itself happened once on the shared filesystem above; only
        // the path-update is per-VM.
        self.pool
            .broadcast_prepend_load_path(entry.lib_path.clone())
            .await?;

        // Mark prepared for this process lifetime; remember the gem
        // path so smoke_test can resolve relative fixture paths
        // without consulting Ruby's Gem.loaded_specs (which is empty
        // for cloned-via-cache gems — they were never gem-installed).
        let mut prepared = self.prepared.lock().await;
        prepared.insert(
            source.name.clone(),
            PreparedGem {
                git_ref: source.git_ref.clone(),
                gem_path: entry.gem_path,
            },
        );
        Ok(())
    }

    async fn list_architectures(&self, gem: &str) -> Result<ArchListing, BackendError> {
        let gem = gem.to_string();
        self.instrumented(|tx| async move {
            let (rtx, rrx) = oneshot::channel();
            tx.send(RubyRequest::ListArchitectures { gem, respond: rtx })
                .await
                .map_err(|_| BackendError::Ruby("ruby owner channel closed".into()))?;
            rrx.await
                .map_err(|_| BackendError::Ruby("ruby owner reply lost".into()))?
        })
        .await
    }

    async fn smoke_test(&self, req: SmokeRequest) -> Result<FixtureOutcome, BackendError> {
        // Resolve relative fixture paths via our prepared-gem map
        // BEFORE forwarding to the owner thread. The owner's
        // resolve_fixture_path falls back to Ruby's
        // `Gem.loaded_specs[gem].full_gem_path` which is empty for
        // cloned-via-cache gems; absolute paths skip that path
        // entirely.
        let mut req = req;
        if !std::path::Path::new(&req.fixture_path).is_absolute() {
            let prepared = self.prepared.lock().await;
            if let Some(pg) = prepared.get(&req.gem) {
                let abs = pg.gem_path.join(&req.fixture_path);
                req.fixture_path = abs.to_string_lossy().into_owned();
            }
        }

        self.instrumented(|tx| async move {
            let (rtx, rrx) = oneshot::channel();
            tx.send(RubyRequest::SmokeTest { req, respond: rtx })
                .await
                .map_err(|_| BackendError::Ruby("ruby owner channel closed".into()))?;
            rrx.await
                .map_err(|_| BackendError::Ruby("ruby owner reply lost".into()))?
        })
        .await
    }

    async fn compile(&self, req: CompileRequest) -> Result<CompileResult, BackendError> {
        // M8.4: real implementation. Owner thread runs the
        // captured-block + instance_eval pattern; returns the
        // pretty-serialized terraform_json string.
        self.instrumented(|tx| async move {
            let (rtx, rrx) = oneshot::channel();
            tx.send(RubyRequest::Compile { req, respond: rtx })
                .await
                .map_err(|_| BackendError::Ruby("ruby owner channel closed".into()))?;
            rrx.await
                .map_err(|_| BackendError::Ruby("ruby owner reply lost".into()))?
        })
        .await
    }

    async fn compile_any(
        &self,
        _req: CompileAnyRequest,
    ) -> Result<CompileAnyResult, BackendError> {
        Err(BackendError::Ruby(
            "embedded /compile-any not yet implemented (M8.4); set PANGEA_COMPILER_BACKEND=http"
                .into(),
        ))
    }

    async fn render_dashboard(
        &self,
        ruby: String,
        extend_modules: Vec<String>,
    ) -> Result<String, BackendError> {
        // Dashboard render runs the inline DSL Ruby on a pool worker
        // and returns the Grafana JSON string. The owner thread does
        // the require-extend-modules + eval + string-extraction; this
        // dispatcher just ships the request + awaits the reply.
        self.instrumented(|tx| async move {
            let (rtx, rrx) = oneshot::channel();
            tx.send(RubyRequest::RenderDashboard {
                ruby,
                extend_modules,
                respond: rtx,
            })
            .await
            .map_err(|_| BackendError::Ruby("ruby owner channel closed".into()))?;
            rrx.await
                .map_err(|_| BackendError::Ruby("ruby owner reply lost".into()))?
        })
        .await
    }

    async fn render_alert(
        &self,
        ruby: String,
        extend_modules: Vec<String>,
        backend: super::backend::AlertRenderBackend,
    ) -> Result<String, BackendError> {
        // Alert render runs the inline DSL Ruby on a pool worker: the
        // owner thread requires the pangea-alerts renderers, evals the
        // inline Ruby to a `Pangea::Alerts` AST, and renders it with the
        // `backend`-selected renderer into a rule-manifest JSON string.
        // This dispatcher just ships the request + awaits the reply.
        self.instrumented(|tx| async move {
            let (rtx, rrx) = oneshot::channel();
            tx.send(RubyRequest::RenderAlert {
                ruby,
                extend_modules,
                backend,
                respond: rtx,
            })
            .await
            .map_err(|_| BackendError::Ruby("ruby owner channel closed".into()))?;
            rrx.await
                .map_err(|_| BackendError::Ruby("ruby owner reply lost".into()))?
        })
        .await
    }
}
