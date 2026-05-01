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
use std::collections::HashMap;
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

#[cfg(feature = "embedded_ruby")]
impl From<pangea_ruby_eval::EvalError> for BackendError {
    fn from(e: pangea_ruby_eval::EvalError) -> Self {
        BackendError::Ruby(e.to_string())
    }
}

/// Mirrors the `/compile` request shape from `pangea-compiler/app.rb`
/// lines 391-498. Either `source` (legacy inline-eval mode) or
/// `template_path` + `rubylib_paths` (gitRepository mode) must be
/// provided.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CompileRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub template_path: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub rubylib_paths: Vec<String>,
    #[serde(default)]
    pub variables: HashMap<String, serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub template_name: Option<String>,
}

/// Mirrors the `/compile` response shape — only the `terraform_json`
/// field is consumed by the controller; we drop the rest.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompileResult {
    pub terraform_json: String,
}

/// Mirrors the `/compile-any` request shape (synthesizer-driven
/// formats). Either `format` (registered name like "packer") or
/// `format_definition` (inline CRD spec) must be provided.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompileAnyRequest {
    pub source: String,
    #[serde(default)]
    pub variables: HashMap<String, serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub format: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub format_definition: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompileAnyResult {
    pub output_json: String,
    pub format: String,
}

/// Where a gem's source code lives + which evaluator should run it.
/// Passed to `prepare_gem` so the embedded backend can clone +
/// register it with the right interpreter. HTTP backends ignore
/// `kind` since the sidecar's bundle is image-baked.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GemSource {
    pub name: String,
    pub git_url: String,
    pub git_ref: String,
    /// Which evaluator runs this gem's source files.
    #[serde(default)]
    pub kind: SourceKind,
}

/// Backend-side mirror of `crd::architecture_gem::SourceKind`. We
/// keep them as separate enums (one for CRD schema, one for backend
/// trait) so the trait stays free of kube-rs / schemars deps.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum SourceKind {
    /// Pangea Ruby DSL (Track A) — magnus-embedded CRuby.
    #[default]
    Ruby,
    /// terreno tatara-lisp (Track B). Reserved; terreno-eval (M2)
    /// wires the actual dispatch.
    Lisp,
    /// Typed WIT-shaped wasm component. Reserved for M2+.
    Wasm,
}

#[async_trait]
pub trait CompilerBackend: Send + Sync {
    /// Make a gem available for subsequent `list_architectures` /
    /// `smoke_test` / `compile` calls.
    ///
    /// HTTP backend → no-op success (sidecar's bundle is image-baked
    /// at compiler-image build time).
    ///
    /// Embedded backend → idempotent `git clone` into the gem cache
    /// at `$PANGEA_GEM_CACHE_DIR/{name}-{ref}/` and prepend
    /// `<that>/lib` to the embedded interpreter's `$LOAD_PATH`. Once
    /// returned Ok, the controller knows `require '{name}'` will
    /// resolve.
    ///
    /// Default impl is no-op so HTTP backends don't have to opt in.
    async fn prepare_gem(&self, _source: &GemSource) -> Result<(), BackendError> {
        Ok(())
    }

    /// Equivalent of `GET /v1/architectures?gem=<gem>`.
    async fn list_architectures(&self, gem: &str) -> Result<ArchListing, BackendError>;

    /// Equivalent of `POST /v1/architectures/smoke-test`.
    async fn smoke_test(&self, req: SmokeRequest) -> Result<FixtureOutcome, BackendError>;

    /// Equivalent of `POST /compile`. Returns the synthesized
    /// Terraform JSON as a string.
    async fn compile(&self, req: CompileRequest) -> Result<CompileResult, BackendError>;

    /// Equivalent of `POST /compile-any` (synthesizer-driven).
    /// Used by both packer builds (`format = Some("packer")`) and the
    /// SynthesizerFormat CRD (`format_definition = Some(spec)`).
    async fn compile_any(
        &self,
        req: CompileAnyRequest,
    ) -> Result<CompileAnyResult, BackendError>;
}
