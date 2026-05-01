//! Compiler backend abstraction — HTTP sidecar (today) + embedded
//! magnus/CRuby (M8.4+).
//!
//! Controllers call into this module instead of `reqwest`-ing the
//! compiler sidecar directly. This is the M8.2 seam that lets us flip
//! between sidecar and embedded paths via config/helm flag without
//! touching reconciler code.
//!
//! See `theory/PANGEA-WORKSPACE-RECONCILIATION.md` § M8.

pub mod backend;
pub mod http_backend;

#[cfg(feature = "embedded_ruby")]
pub mod embedded_backend;

#[cfg(feature = "embedded_ruby")]
pub mod gem_cache;

#[cfg(feature = "embedded_ruby")]
pub mod owner;

pub use backend::{
    ArchListing, BackendError, CompileAnyRequest, CompileAnyResult, CompileRequest,
    CompileResult, CompilerBackend, FixtureOutcome, GemSource, SmokeRequest,
};
pub use http_backend::HttpCompilerBackend;

#[cfg(feature = "embedded_ruby")]
pub use embedded_backend::EmbeddedCompilerBackend;

#[cfg(feature = "embedded_ruby")]
pub use gem_cache::{GemCache, GemCacheError, GemEntry};

#[cfg(feature = "embedded_ruby")]
pub use owner::{RubyOwner, RubyRequest};
