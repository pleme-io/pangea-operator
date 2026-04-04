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
    pub fn is_retryable(&self) -> bool {
        matches!(
            self,
            Error::Kube(_)
                | Error::Database(_)
                | Error::Timeout(_)
                | Error::LockFailed(_)
                | Error::Io(_)
                | Error::PackerExecution(_)
                | Error::AmiTestFailed(_)
        )
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
