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

/// Mirrors the `/compile` response shape. Carries BOTH the
/// pretty-serialized JSON string (for disk write + tofu/debug
/// consumption) AND the in-memory typed value (for direct magma
/// + future in-process consumers).
///
/// The dual surface is the core of the in-memory exploit
/// (theory/IN-MEMORY-PIPELINE.md): pangea-ruby-eval produces the
/// synthesis as `serde_json::Value` natively; serializing to a
/// string is for disk/tofu, not magma. Callers that stay in-process
/// (preview, magma-plan, equivalence tests) read `synthesis_value`
/// directly + skip the parse round-trip.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompileResult {
    /// Pretty-serialized Terraform JSON. The form written to
    /// `main.tf.json` for tofu compatibility + human debugging.
    pub terraform_json: String,
    /// The same content as a typed JSON value. Callers passing
    /// straight to magma should consume this instead of re-parsing
    /// `terraform_json`. `Option` for HTTP-backend compatibility
    /// (the legacy HTTP sidecar didn't return the typed value;
    /// embedded backend always populates).
    #[serde(default)]
    pub synthesis_value: Option<serde_json::Value>,
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

    /// Render a `PangeaDashboard`'s inline Ruby into a Grafana
    /// dashboard JSON **string**.
    ///
    /// The inline Ruby (authored against the typed
    /// `Pangea::Dashboards::Library` + `Render::Grafana` vocabulary)
    /// evaluates to a JSON string — the dashboard `config_json` — as
    /// its last expression. This method evaluates that Ruby and returns
    /// the JSON string verbatim; the controller then upserts it into a
    /// sidecar-labelled ConfigMap.
    ///
    /// `extend_modules` lists Ruby modules the dashboard DSL expects to
    /// be in scope (e.g. `["Pangea::Grafana"]`); the embedded backend
    /// makes them requireable before the eval. The embedded backend is
    /// the only impl with a real evaluator — HTTP returns a typed
    /// `BackendError` (rio runs embedded).
    ///
    /// Default impl errors so backends that don't support dashboard
    /// rendering surface a typed gap rather than a silent wrong answer.
    async fn render_dashboard(
        &self,
        _ruby: String,
        _extend_modules: Vec<String>,
    ) -> Result<String, BackendError> {
        Err(BackendError::Ruby(
            "render_dashboard requires the embedded backend".into(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── BackendError display contract ──
    //
    // The error::Error::Backend From impl in compiler-side controllers
    // formats these via Display. Drift in the prefix would silently
    // change reconciler log output.

    #[test]
    fn backend_error_display_includes_kind_prefix() {
        assert_eq!(
            format!("{}", BackendError::Transport("conn refused".into())),
            "transport: conn refused"
        );
        assert_eq!(
            format!("{}", BackendError::Compiler("syntax error".into())),
            "compiler: syntax error"
        );
        assert_eq!(
            format!("{}", BackendError::Ruby("NameError".into())),
            "ruby evaluator: NameError"
        );
        assert_eq!(
            format!("{}", BackendError::NotInitialized),
            "backend not initialized"
        );
    }

    // ── SourceKind serde defaults + round-trip ──
    //
    // The default = Ruby behavior is load-bearing: any pre-M2 CR
    // without an explicit `kind` field continues to land on the
    // magnus path, preserving backward compatibility. A drift to a
    // non-Ruby default would silently re-route Ruby gems into the
    // Lisp evaluator.

    #[test]
    fn source_kind_default_is_ruby() {
        assert_eq!(SourceKind::default(), SourceKind::Ruby);
    }

    #[test]
    fn source_kind_serde_round_trip() {
        for k in [SourceKind::Ruby, SourceKind::Lisp, SourceKind::Wasm] {
            let json = serde_json::to_string(&k).expect("serialize");
            let back: SourceKind = serde_json::from_str(&json).expect("deserialize");
            assert_eq!(k, back);
        }
    }

    #[test]
    fn source_kind_unknown_kind_fails_to_deserialize() {
        let bad = serde_json::from_str::<SourceKind>("\"Quantum\"");
        assert!(bad.is_err(), "unknown SourceKind must error, not silently default");
    }

    // ── Payload shape coverage ──

    #[test]
    fn arch_listing_round_trip_with_optional_version() {
        let with = ArchListing {
            gem: "pangea-aws".into(),
            classes: vec!["Foo".into(), "Bar".into()],
            version: Some("0.42.0".into()),
        };
        let json = serde_json::to_string(&with).unwrap();
        let back: ArchListing = serde_json::from_str(&json).unwrap();
        assert_eq!(back.gem, "pangea-aws");
        assert_eq!(back.classes.len(), 2);
        assert_eq!(back.version.as_deref(), Some("0.42.0"));
    }

    #[test]
    fn arch_listing_accepts_missing_version() {
        // The compiler sidecar emits the field, but the trait must
        // tolerate a payload that omits it (defensive against
        // older sidecars and also clearer than a forced null).
        let raw = r#"{"gem":"pangea-aws","classes":[]}"#;
        let listing: ArchListing = serde_json::from_str(raw).unwrap();
        assert!(listing.version.is_none());
    }

    #[test]
    fn fixture_outcome_skipped_fields_default() {
        // FixtureOutcome with only `passed` is the smallest payload
        // a backend can return. Ensure the optional fields default
        // cleanly so reconcilers don't crash on a minimal sidecar
        // response.
        let raw = r#"{"passed":true}"#;
        let outcome: FixtureOutcome = serde_json::from_str(raw).unwrap();
        assert!(outcome.passed);
        assert!(outcome.error.is_none());
        assert!(outcome.input_hash.is_none());
    }

    #[test]
    fn compile_request_skip_serializing_keeps_payload_small() {
        // An empty CompileRequest should serialize to {} rather than
        // {"source":null,...} — the compiler sidecar's content-type
        // negotiation is finicky about extra null fields.
        let req = CompileRequest::default();
        let json = serde_json::to_string(&req).unwrap();
        // Empty defaults except for "variables" which is "{}":
        // the request must NOT include null source/template_path.
        assert!(!json.contains("null"), "got: {json}");
        assert!(!json.contains("template_path"), "got: {json}");
    }

    #[test]
    fn compile_any_request_with_format_definition_round_trips() {
        let req = CompileAnyRequest {
            source: "puts 'hi'".into(),
            variables: HashMap::new(),
            format: None,
            format_definition: Some(serde_json::json!({"sections": []})),
        };
        let json = serde_json::to_string(&req).unwrap();
        let back: CompileAnyRequest = serde_json::from_str(&json).unwrap();
        assert_eq!(back.source, "puts 'hi'");
        assert!(back.format.is_none());
        assert!(back.format_definition.is_some());
    }

    // ── Default impl on the trait ──

    struct DummyBackend;
    #[async_trait]
    impl CompilerBackend for DummyBackend {
        async fn list_architectures(&self, _gem: &str) -> Result<ArchListing, BackendError> {
            Err(BackendError::NotInitialized)
        }
        async fn smoke_test(&self, _: SmokeRequest) -> Result<FixtureOutcome, BackendError> {
            Err(BackendError::NotInitialized)
        }
        async fn compile(&self, _: CompileRequest) -> Result<CompileResult, BackendError> {
            Err(BackendError::NotInitialized)
        }
        async fn compile_any(&self, _: CompileAnyRequest) -> Result<CompileAnyResult, BackendError> {
            Err(BackendError::NotInitialized)
        }
    }

    #[tokio::test]
    async fn prepare_gem_default_impl_returns_ok() {
        // HTTP backends (the existing path today) don't override
        // prepare_gem — the default no-op is what makes their
        // `controller.run()` work. A future trait-method addition
        // shouldn't accidentally remove this default.
        let dummy = DummyBackend;
        let src = GemSource {
            name: "pangea-test".into(),
            git_url: "https://example.invalid/repo".into(),
            git_ref: "main".into(),
            kind: SourceKind::Ruby,
        };
        assert!(dummy.prepare_gem(&src).await.is_ok());
    }
}
