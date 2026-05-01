//! Embedded backend — sends typed requests to the [`RubyOwner`] thread.
//!
//! Cheap clone, async-friendly. Implements [`CompilerBackend`] so
//! reconcilers don't know whether they're talking to a sidecar or to
//! magnus.

use async_trait::async_trait;
use tokio::sync::{mpsc, oneshot};

use super::backend::{
    ArchListing, BackendError, CompileAnyRequest, CompileAnyResult, CompileRequest,
    CompileResult, CompilerBackend, FixtureOutcome, SmokeRequest,
};
use super::owner::RubyRequest;

#[derive(Clone)]
pub struct EmbeddedCompilerBackend {
    tx: mpsc::Sender<RubyRequest>,
}

impl EmbeddedCompilerBackend {
    pub fn new(tx: mpsc::Sender<RubyRequest>) -> Self {
        Self { tx }
    }
}

#[async_trait]
impl CompilerBackend for EmbeddedCompilerBackend {
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

    async fn compile(&self, _req: CompileRequest) -> Result<CompileResult, BackendError> {
        // M8.2.2: trait surface complete; embedded /compile lands in
        // M8.4 alongside per-CR clone-cache + the captured-block
        // pattern. Surfaces as a typed condition on InfrastructureTemplate
        // so the operator-human can flip back to HTTP if they tried
        // embedded prematurely.
        Err(BackendError::Ruby(
            "embedded /compile not yet implemented (M8.4); set PANGEA_COMPILER_BACKEND=http"
                .into(),
        ))
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
