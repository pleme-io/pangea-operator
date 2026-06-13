//! OpenTofu executor for infrastructure operations.
//!
//! Manages the execution of OpenTofu commands (init, plan, apply, destroy)
//! in isolated workspaces with proper credential and state handling.

pub mod backend_select;
pub mod iac_executor;
pub mod recording;
mod tofu;
mod workspace;
mod plan;
pub mod packer;
pub mod policy;
pub mod variable_resolver;
// Magma-backed alternative IacExecutor (feature-gated). Per
// theory/MAGMA-OPERATOR-BACKEND.md. Both `TofuExecutor` and
// `MagmaExecutor` ship side-by-side; the controller selects at
// startup or per-CR.
#[cfg(feature = "executor_magma")]
pub mod magma;
// Bundle reader — NOT feature-gated. The CR-status fields it produces
// (`ActionDistribution`, `BundleRef`) are always part of the schema so
// YAML consumers / dashboards don't have a feature-conditional shape;
// in non-magma builds the reader simply returns None on every call
// (no magma-bundle.json on disk). See memory/project_operator_
// observability_backlog.md for why this lives here.
pub mod magma_bundle;
// Unified cycle artifact — the typed "what just happened" shape both
// executors populate. Slice 2 of the observability program. Not
// feature-gated; the type system stays uniform whether or not magma
// is linked in. See module docs for the per-backend capability map.
pub mod cycle_artifact;
// WorkspaceRunner — the typed, executor-agnostic abstraction the
// controller's phase handlers speak. Layered ON TOP of IacExecutor
// (not a replacement). See module docs for the migration story.
pub mod workspace_runner;

pub use backend_select::ExecutorBackend;
pub use iac_executor::IacExecutor;
pub use recording::{RecordedCall, RecordingExecutor};
pub use tofu::{TofuExecutor, TofuCommand, TofuResult, FailedChangeRecord};
pub use workspace::{Workspace, WorkspaceManager};
pub use plan::{Plan, PlanSummary, ResourceChange, ChangeType};
pub use packer::{PackerExecutor, PackerCommand, PackerResult, parse_packer_manifest, parse_packer_manifest_region};
pub use policy::{evaluate as evaluate_policy, is_configured as policy_is_configured, PolicyOutcome};

#[cfg(feature = "executor_magma")]
pub use magma::{MagmaExecutor, MagmaExecutorConfig, StateBackendAsync};

use std::path::PathBuf;

/// Configuration for the executor.
#[derive(Debug, Clone)]
pub struct ExecutorConfig {
    /// Path to the OpenTofu binary.
    pub tofu_binary: PathBuf,

    /// Path to the Packer binary.
    pub packer_binary: PathBuf,

    /// Base directory for workspaces.
    pub workspace_base: PathBuf,

    /// Command execution timeout in seconds.
    pub timeout_secs: u64,

    /// Packer command execution timeout in seconds.
    pub packer_timeout_secs: u64,

    /// Path to Ruby binary for compilation.
    pub ruby_binary: Option<PathBuf>,

    /// Whether to enable verbose output.
    pub verbose: bool,
}

impl Default for ExecutorConfig {
    fn default() -> Self {
        Self {
            tofu_binary: PathBuf::from("tofu"),
            packer_binary: PathBuf::from("packer"),
            workspace_base: PathBuf::from("/tmp/pangea-workspaces"),
            timeout_secs: 600,        // 10 minutes
            packer_timeout_secs: 2700, // 45 minutes
            ruby_binary: None,
            verbose: false,
        }
    }
}

impl ExecutorConfig {
    /// Create from environment variables.
    pub fn from_env() -> Self {
        let tofu_binary = std::env::var("TOFU_BINARY")
            .map(PathBuf::from)
            .unwrap_or_else(|_| PathBuf::from("tofu"));

        let packer_binary = std::env::var("PACKER_BINARY")
            .map(PathBuf::from)
            .unwrap_or_else(|_| PathBuf::from("packer"));

        let workspace_base = std::env::var("PANGEA_WORKSPACE_BASE")
            .map(PathBuf::from)
            .unwrap_or_else(|_| PathBuf::from("/tmp/pangea-workspaces"));

        let timeout_secs = std::env::var("PANGEA_TIMEOUT")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(600);

        let packer_timeout_secs = std::env::var("PACKER_TIMEOUT")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(2700);

        let ruby_binary = std::env::var("RUBY_BINARY")
            .map(PathBuf::from)
            .ok();

        let verbose = std::env::var("PANGEA_VERBOSE")
            .map(|s| s == "1" || s.to_lowercase() == "true")
            .unwrap_or(false);

        Self {
            tofu_binary,
            packer_binary,
            workspace_base,
            timeout_secs,
            packer_timeout_secs,
            ruby_binary,
            verbose,
        }
    }
}
