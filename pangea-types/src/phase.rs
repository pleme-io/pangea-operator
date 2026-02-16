//! Phase enum for template lifecycle.

use serde::{Deserialize, Serialize};

/// Phase represents the current state of an InfrastructureTemplate.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
#[cfg_attr(feature = "server", derive(async_graphql::Enum))]
pub enum Phase {
    /// Template is pending initial processing.
    #[default]
    Pending,
    /// Ruby DSL is being compiled to Terraform JSON.
    Compiling,
    /// Terraform is being initialized (providers, backend).
    Initializing,
    /// Terraform plan is being generated.
    Planning,
    /// Terraform apply is in progress.
    Applying,
    /// Template is successfully applied and stable.
    Ready,
    /// Drift detected between desired and actual state.
    Drifted,
    /// An error occurred during processing.
    Failed,
    /// Template is being destroyed.
    Destroying,
}

impl std::fmt::Display for Phase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Phase::Pending => write!(f, "Pending"),
            Phase::Compiling => write!(f, "Compiling"),
            Phase::Initializing => write!(f, "Initializing"),
            Phase::Planning => write!(f, "Planning"),
            Phase::Applying => write!(f, "Applying"),
            Phase::Ready => write!(f, "Ready"),
            Phase::Drifted => write!(f, "Drifted"),
            Phase::Failed => write!(f, "Failed"),
            Phase::Destroying => write!(f, "Destroying"),
        }
    }
}

impl Phase {
    /// Returns true if this phase represents an active operation.
    pub fn is_active(&self) -> bool {
        matches!(
            self,
            Phase::Compiling
                | Phase::Initializing
                | Phase::Planning
                | Phase::Applying
                | Phase::Destroying
        )
    }

    /// Returns true if this phase represents a terminal state.
    pub fn is_terminal(&self) -> bool {
        matches!(self, Phase::Ready | Phase::Failed)
    }

    /// Returns true if this phase requires attention.
    pub fn needs_attention(&self) -> bool {
        matches!(self, Phase::Failed | Phase::Drifted)
    }

    /// Get CSS class for this phase (for UI styling).
    pub fn css_class(&self) -> &'static str {
        match self {
            Phase::Ready => "phase-ready",
            Phase::Failed => "phase-failed",
            Phase::Drifted => "phase-drifted",
            Phase::Pending | Phase::Compiling | Phase::Initializing => "phase-pending",
            Phase::Planning | Phase::Applying | Phase::Destroying => "phase-in-progress",
        }
    }
}
