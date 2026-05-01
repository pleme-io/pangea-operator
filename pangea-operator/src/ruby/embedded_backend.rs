//! Embedded backend — sends typed requests to the [`RubyOwner`] thread.
//!
//! Cheap clone, async-friendly. Implements [`CompilerBackend`] so
//! reconcilers don't know whether they're talking to a sidecar or to
//! magnus.

use async_trait::async_trait;
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot, Mutex};

use super::backend::{
    ArchListing, BackendError, CompileAnyRequest, CompileAnyResult, CompileRequest,
    CompileResult, CompilerBackend, FixtureOutcome, GemSource, SmokeRequest,
};
use super::gem_cache::GemCache;
use super::owner::RubyRequest;

#[derive(Clone)]
pub struct EmbeddedCompilerBackend {
    tx: mpsc::Sender<RubyRequest>,
    cache: Option<GemCache>,
    /// Tracks which (name, ref) pairs we've already prepended on
    /// $LOAD_PATH so we don't redundantly send PrependLoadPath
    /// requests across reconcile cycles. The Ruby snippet that runs
    /// is idempotent at the Ruby level too; this is just a small
    /// channel-traffic optimization.
    prepared: Arc<Mutex<std::collections::HashSet<(String, String)>>>,
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
        let cache = match &self.cache {
            Some(c) => c,
            None => return Ok(()), // no cache → caller's gems are pre-bundled.
        };

        // Skip if we've already prepared this (name, ref).
        {
            let prepared = self.prepared.lock().await;
            if prepared.contains(&(source.name.clone(), source.git_ref.clone())) {
                return Ok(());
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
                path: entry.lib_path,
                respond: rtx,
            })
            .await
            .map_err(|_| BackendError::Ruby("ruby owner channel closed".into()))?;
        rrx.await
            .map_err(|_| BackendError::Ruby("prepend reply lost".into()))??;

        // Mark prepared for this process lifetime.
        let mut prepared = self.prepared.lock().await;
        prepared.insert((source.name.clone(), source.git_ref.clone()));
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
