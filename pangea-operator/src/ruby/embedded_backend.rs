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
    tx: mpsc::Sender<RubyRequest>,
    cache: Option<GemCache>,
    /// Map of `gem_name → PreparedGem`. prepare_gem populates;
    /// smoke_test reads to resolve relative fixture paths.
    prepared: Arc<Mutex<std::collections::HashMap<String, PreparedGem>>>,
}

impl EmbeddedCompilerBackend {
    /// Construct without a gem cache — `prepare_gem` becomes a no-op.
    /// Useful for tests + for environments where gems are still
    /// image-baked (transitional M8.4 deployments).
    pub fn new(tx: mpsc::Sender<RubyRequest>) -> Self {
        Self {
            tx,
            cache: None,
            prepared: Arc::new(Mutex::new(Default::default())),
        }
    }

    /// Construct with an active gem cache. `prepare_gem` clones +
    /// prepends $LOAD_PATH on first call per (name, ref). Production
    /// shape under M8.4.2.
    pub fn with_cache(tx: mpsc::Sender<RubyRequest>, cache: GemCache) -> Self {
        Self {
            tx,
            cache: Some(cache),
            prepared: Arc::new(Mutex::new(Default::default())),
        }
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

        // Prepend lib/ to the embedded interpreter's $LOAD_PATH.
        // The Ruby snippet is idempotent (`unless $LOAD_PATH.include?`).
        let (rtx, rrx) = oneshot::channel();
        self.tx
            .send(RubyRequest::PrependLoadPath {
                path: entry.lib_path.clone(),
                respond: rtx,
            })
            .await
            .map_err(|_| BackendError::Ruby("ruby owner channel closed".into()))?;
        rrx.await
            .map_err(|_| BackendError::Ruby("prepend reply lost".into()))??;

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
        let (rtx, rrx) = oneshot::channel();
        self.tx
            .send(RubyRequest::ListArchitectures {
                gem: gem.to_string(),
                respond: rtx,
            })
            .await
            .map_err(|_| BackendError::Ruby("ruby owner channel closed".into()))?;
        rrx.await
            .map_err(|_| BackendError::Ruby("ruby owner reply lost".into()))?
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

        let (rtx, rrx) = oneshot::channel();
        self.tx
            .send(RubyRequest::SmokeTest { req, respond: rtx })
            .await
            .map_err(|_| BackendError::Ruby("ruby owner channel closed".into()))?;
        rrx.await
            .map_err(|_| BackendError::Ruby("ruby owner reply lost".into()))?
    }

    async fn compile(&self, req: CompileRequest) -> Result<CompileResult, BackendError> {
        // M8.4: real implementation. Owner thread runs the
        // captured-block + instance_eval pattern; returns the
        // pretty-serialized terraform_json string.
        let (rtx, rrx) = oneshot::channel();
        self.tx
            .send(RubyRequest::Compile { req, respond: rtx })
            .await
            .map_err(|_| BackendError::Ruby("ruby owner channel closed".into()))?;
        rrx.await
            .map_err(|_| BackendError::Ruby("ruby owner reply lost".into()))?
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
}
