//! Error types for the Pangea Operator.

use thiserror::Error;

/// Main error type for the Pangea Operator.
#[derive(Error, Debug)]
pub enum Error {
    /// Kubernetes API error.
    #[error("Kubernetes API error: {0}")]
    Kube(#[from] kube::Error),

    /// Serialization/deserialization error.
    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    /// YAML serialization error.
    #[error("YAML error: {0}")]
    Yaml(#[from] serde_yaml::Error),

    /// Database error.
    #[error("Database error: {0}")]
    Database(#[from] sqlx::Error),

    /// IO error.
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    /// Template compilation error.
    #[error("Template compilation failed: {0}")]
    Compilation(String),

    /// OpenTofu execution error.
    #[error("OpenTofu execution failed: {0}")]
    TofuExecution(String),

    /// Packer execution error.
    #[error("Packer execution failed: {0}")]
    PackerExecution(String),

    /// Packer manifest parse error.
    #[error("Packer manifest parse error: {0}")]
    PackerManifest(String),

    /// AMI test failure.
    #[error("AMI test failed: {0}")]
    AmiTestFailed(String),

    /// Image pipeline error.
    #[error("Image pipeline error: {0}")]
    ImagePipeline(String),

    /// Health check failure.
    #[error("Health check failed: {0}")]
    HealthCheckFailed(String),

    /// Assertion failure.
    #[error("Assertion failed: {0}")]
    AssertionFailed(String),

    /// State backend error.
    #[error("State backend error: {0}")]
    StateBackend(String),

    /// Configuration error.
    #[error("Configuration error: {0}")]
    Config(String),

    /// Secret not found.
    #[error("Secret not found: {namespace}/{name}")]
    SecretNotFound { namespace: String, name: String },

    /// Invalid template source.
    #[error("Invalid template source: {0}")]
    InvalidSource(String),

    /// Reconciliation timeout.
    #[error("Reconciliation timeout after {0} seconds")]
    Timeout(u64),

    /// Lock acquisition failed.
    #[error("Failed to acquire state lock: {0}")]
    LockFailed(String),

    /// PangeaNamespace not found.
    #[error("PangeaNamespace not found: {0}")]
    NamespaceNotFound(String),

    /// GraphQL error.
    #[cfg(feature = "graphql")]
    #[error("GraphQL error: {0}")]
    GraphQL(String),

    /// gRPC error.
    #[cfg(feature = "grpc")]
    #[error("gRPC error: {0}")]
    Grpc(String),

    /// Generic error with context.
    #[error("{context}: {source}")]
    WithContext {
        context: String,
        #[source]
        source: Box<Error>,
    },
}

impl Error {
    /// Add context to an error.
    pub fn context(self, context: impl Into<String>) -> Self {
        Error::WithContext {
            context: context.into(),
            source: Box::new(self),
        }
    }

    /// Check if this error is retryable.
    ///
    /// Compilation errors are retryable: the most common failure mode is
    /// the compiler sidecar not being ready yet at operator startup
    /// (race between operator init and compiler Bundler.setup, ~5s).
    /// Real source errors will be retried too, but the controller's
    /// failure_count keeps accumulating so the template eventually moves
    /// to Failed if the user's source is genuinely broken.
    ///
    /// `WithContext` recurses into the inner error so `.context(...)`
    /// preserves retryability. Without this, wrapping a retryable
    /// error with operator context (a common pattern at every
    /// reconcile boundary) silently flipped it to non-retryable and
    /// the controller would back off ~5x longer than intended.
    pub fn is_retryable(&self) -> bool {
        match self {
            Error::Kube(_)
            | Error::Database(_)
            | Error::Timeout(_)
            | Error::LockFailed(_)
            | Error::Io(_)
            | Error::PackerExecution(_)
            | Error::AmiTestFailed(_)
            | Error::Compilation(_) => true,
            // Preserve retryability through context wrappers — the
            // wrapper's job is to add information, not change the
            // error classification.
            Error::WithContext { source, .. } => source.is_retryable(),
            _ => false,
        }
    }
}

/// Result type alias for Pangea operations.
pub type Result<T> = std::result::Result<T, Error>;

/// Extension trait for adding context to results.
pub trait ResultExt<T> {
    /// Add context to an error result.
    fn context(self, context: impl Into<String>) -> Result<T>;
}

impl<T> ResultExt<T> for Result<T> {
    fn context(self, context: impl Into<String>) -> Result<T> {
        self.map_err(|e| e.context(context))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_display_string_variants() {
        let e = Error::Compilation("syntax error".into());
        assert_eq!(format!("{}", e), "Template compilation failed: syntax error");

        let e = Error::TofuExecution("plan failed".into());
        assert_eq!(format!("{}", e), "OpenTofu execution failed: plan failed");

        let e = Error::Config("bad config".into());
        assert_eq!(format!("{}", e), "Configuration error: bad config");

        let e = Error::StateBackend("connection refused".into());
        assert_eq!(format!("{}", e), "State backend error: connection refused");
    }

    #[test]
    fn test_error_display_secret_not_found() {
        let e = Error::SecretNotFound {
            namespace: "default".into(),
            name: "db-creds".into(),
        };
        assert_eq!(format!("{}", e), "Secret not found: default/db-creds");
    }

    #[test]
    fn test_error_display_timeout() {
        let e = Error::Timeout(300);
        assert_eq!(format!("{}", e), "Reconciliation timeout after 300 seconds");
    }

    #[test]
    fn test_error_context_wraps() {
        let inner = Error::Compilation("parse error".into());
        let wrapped = inner.context("while processing template");
        match &wrapped {
            Error::WithContext { context, source } => {
                assert_eq!(context, "while processing template");
                assert!(matches!(source.as_ref(), Error::Compilation(_)));
            }
            _ => panic!("Expected WithContext variant"),
        }
        assert!(format!("{}", wrapped).contains("while processing template"));
    }

    #[test]
    fn test_error_context_nested() {
        let inner = Error::Config("missing field".into());
        let wrapped1 = inner.context("loading namespace");
        let wrapped2 = wrapped1.context("during reconciliation");
        assert!(format!("{}", wrapped2).contains("during reconciliation"));
    }

    #[test]
    fn test_is_retryable_true_variants() {
        assert!(Error::Timeout(60).is_retryable());
        assert!(Error::LockFailed("locked".into()).is_retryable());
        assert!(Error::PackerExecution("timeout".into()).is_retryable());
        assert!(Error::AmiTestFailed("connection error".into()).is_retryable());
        assert!(Error::Io(std::io::Error::new(std::io::ErrorKind::Other, "io")).is_retryable());
        // Compilation errors are retryable to absorb the compiler-sidecar
        // startup race; persistent source errors get caught via failure_count.
        assert!(Error::Compilation("compiler unreachable".into()).is_retryable());
    }

    #[test]
    fn test_is_retryable_false_variants() {
        assert!(!Error::Config("invalid".into()).is_retryable());
        assert!(!Error::InvalidSource("missing".into()).is_retryable());
        assert!(!Error::StateBackend("bad".into()).is_retryable());
        assert!(!Error::PackerManifest("missing".into()).is_retryable());
        assert!(!Error::SecretNotFound {
            namespace: "ns".into(),
            name: "s".into(),
        }.is_retryable());
        assert!(!Error::NamespaceNotFound("ns".into()).is_retryable());
        assert!(!Error::HealthCheckFailed("fail".into()).is_retryable());
        assert!(!Error::AssertionFailed("fail".into()).is_retryable());
        assert!(!Error::ImagePipeline("fail".into()).is_retryable());
    }

    #[test]
    fn test_result_ext_context_on_err() {
        let result: Result<i32> = Err(Error::Config("bad".into()));
        let with_ctx = result.context("loading config");
        assert!(with_ctx.is_err());
        let err = with_ctx.unwrap_err();
        assert!(matches!(err, Error::WithContext { .. }));
    }

    #[test]
    fn test_result_ext_context_on_ok() {
        let result: Result<i32> = Ok(42);
        let with_ctx = result.context("loading config");
        assert_eq!(with_ctx.unwrap(), 42);
    }

    #[test]
    fn test_error_from_serde_json() {
        let result: std::result::Result<serde_json::Value, _> = serde_json::from_str("{invalid}");
        let err: Error = result.unwrap_err().into();
        assert!(matches!(err, Error::Serialization(_)));
        assert!(format!("{}", err).contains("Serialization error"));
    }

    #[test]
    fn test_error_from_io() {
        let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "file not found");
        let err: Error = io_err.into();
        assert!(matches!(err, Error::Io(_)));
        assert!(format!("{}", err).contains("IO error"));
    }

    #[test]
    fn test_with_context_display() {
        let inner = Error::Timeout(30);
        let wrapped = inner.context("applying step vpc");
        let display = format!("{}", wrapped);
        assert_eq!(display, "applying step vpc: Reconciliation timeout after 30 seconds");
    }

    // ── T4: retryability classification audit ──

    #[test]
    fn with_context_preserves_retryability_of_inner_error() {
        // Reproducer for the pre-T4 bug: wrapping a retryable error
        // with `.context(...)` flipped it to non-retryable because
        // is_retryable() didn't recurse into WithContext::source.
        // Real-world consequence: the operator backed off the
        // tiered_backoff non-retryable path (300s) instead of
        // retryable (60s) for any error that passed through a
        // `.context(...)` wrapper — every reconcile error message
        // used `.context()` to add operator-level context, so this
        // affected basically every retryable error in practice.
        let retryable = Error::Timeout(60);
        assert!(retryable.is_retryable(), "Timeout is retryable");
        let wrapped = retryable.context("during apply phase");
        assert!(
            wrapped.is_retryable(),
            "WithContext must preserve inner retryability"
        );
    }

    #[test]
    fn with_context_preserves_non_retryability_of_inner_error() {
        // Symmetry: non-retryable stays non-retryable through
        // wrapping. Otherwise WithContext would hide config errors
        // behind a retry storm.
        let perm = Error::Config("bad config".into());
        assert!(!perm.is_retryable());
        let wrapped = perm.context("loading config");
        assert!(!wrapped.is_retryable());
    }

    #[test]
    fn nested_with_context_recurses_through_layers() {
        // Multi-level wrapping must still find the original
        // retryable underneath — common in fleet code where each
        // boundary adds its own context.
        let inner = Error::LockFailed("state lock".into());
        let one = inner.context("during plan");
        let two = one.context("reconciling template foo");
        let three = two.context("worker tick");
        assert!(three.is_retryable());
    }
}
