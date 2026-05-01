//! Compiler backend trait + typed payloads.
//!
//! One interface, two implementations:
//! - [`crate::ruby::HttpCompilerBackend`] — wraps a reqwest client +
//!   compiler sidecar URL. The current shape, exists today.
//! - [`crate::ruby::EmbeddedCompilerBackend`] (feature `embedded_ruby`)
//!   — sends typed requests to a [`crate::ruby::RubyOwner`] thread that
//!   owns the magnus interpreter.
//!
//! The trait is async-trait-free (returns `Pin<Box<dyn Future>>` to
//! avoid an extra dep) — callers use `.await` normally.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// What `GET /v1/architectures?gem=…` returns. Mirrors
/// `pangea-compiler/app.rb` lines 265-291.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArchListing {
    pub gem: String,
    pub classes: Vec<String>,
    #[serde(default)]
    pub version: Option<String>,
}

/// What `POST /v1/architectures/smoke-test` accepts. Mirrors
/// `pangea-compiler/app.rb` lines 293-365.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SmokeRequest {
    pub gem: String,
    pub class_name: String,
    pub fixture_path: String,
}

/// The smoke-test outcome. Backend-agnostic, intentionally narrower
/// than the controller's `FixtureResult` (which adds a timestamp).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FixtureOutcome {
    pub passed: bool,
    #[serde(default)]
    pub error: Option<String>,
    #[serde(default)]
    pub input_hash: Option<String>,
}

/// Backend-side errors. The trait deliberately collapses HTTP vs Ruby
/// failure modes into one type so reconcilers can handle both
/// uniformly.
#[derive(Debug, Error)]
pub enum BackendError {
    #[error("transport: {0}")]
    Transport(String),

    #[error("compiler: {0}")]
    Compiler(String),

    #[error("ruby evaluator: {0}")]
    Ruby(String),

    #[error("backend not initialized")]
    NotInitialized,
}

#[async_trait]
pub trait CompilerBackend: Send + Sync {
    /// Equivalent of `GET /v1/architectures?gem=<gem>`.
    async fn list_architectures(&self, gem: &str) -> Result<ArchListing, BackendError>;

    /// Equivalent of `POST /v1/architectures/smoke-test`.
    async fn smoke_test(&self, req: SmokeRequest) -> Result<FixtureOutcome, BackendError>;
}
