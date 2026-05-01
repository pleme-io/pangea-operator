//! Embedded backend — sends typed requests to the [`RubyOwner`] thread.
//!
//! Cheap clone, async-friendly. Implements [`CompilerBackend`] so
//! reconcilers don't know whether they're talking to a sidecar or to
//! magnus.

use async_trait::async_trait;
use tokio::sync::{mpsc, oneshot};

use super::backend::{
    ArchListing, BackendError, CompilerBackend, FixtureOutcome, SmokeRequest,
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
}
