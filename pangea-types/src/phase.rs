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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_phase_default_is_pending() {
        assert_eq!(Phase::default(), Phase::Pending);
    }

    #[test]
    fn test_phase_display_all_variants() {
        assert_eq!(format!("{}", Phase::Pending), "Pending");
        assert_eq!(format!("{}", Phase::Compiling), "Compiling");
        assert_eq!(format!("{}", Phase::Initializing), "Initializing");
        assert_eq!(format!("{}", Phase::Planning), "Planning");
        assert_eq!(format!("{}", Phase::Applying), "Applying");
        assert_eq!(format!("{}", Phase::Ready), "Ready");
        assert_eq!(format!("{}", Phase::Drifted), "Drifted");
        assert_eq!(format!("{}", Phase::Failed), "Failed");
        assert_eq!(format!("{}", Phase::Destroying), "Destroying");
    }

    #[test]
    fn test_is_active_active_phases() {
        assert!(Phase::Compiling.is_active());
        assert!(Phase::Initializing.is_active());
        assert!(Phase::Planning.is_active());
        assert!(Phase::Applying.is_active());
        assert!(Phase::Destroying.is_active());
    }

    #[test]
    fn test_is_active_inactive_phases() {
        assert!(!Phase::Pending.is_active());
        assert!(!Phase::Ready.is_active());
        assert!(!Phase::Drifted.is_active());
        assert!(!Phase::Failed.is_active());
    }

    #[test]
    fn test_is_terminal() {
        assert!(Phase::Ready.is_terminal());
        assert!(Phase::Failed.is_terminal());
        assert!(!Phase::Pending.is_terminal());
        assert!(!Phase::Compiling.is_terminal());
        assert!(!Phase::Drifted.is_terminal());
        assert!(!Phase::Destroying.is_terminal());
    }

    #[test]
    fn test_needs_attention() {
        assert!(Phase::Failed.needs_attention());
        assert!(Phase::Drifted.needs_attention());
        assert!(!Phase::Ready.needs_attention());
        assert!(!Phase::Pending.needs_attention());
        assert!(!Phase::Applying.needs_attention());
    }

    #[test]
    fn test_css_class_all_variants() {
        assert_eq!(Phase::Ready.css_class(), "phase-ready");
        assert_eq!(Phase::Failed.css_class(), "phase-failed");
        assert_eq!(Phase::Drifted.css_class(), "phase-drifted");
        assert_eq!(Phase::Pending.css_class(), "phase-pending");
        assert_eq!(Phase::Compiling.css_class(), "phase-pending");
        assert_eq!(Phase::Initializing.css_class(), "phase-pending");
        assert_eq!(Phase::Planning.css_class(), "phase-in-progress");
        assert_eq!(Phase::Applying.css_class(), "phase-in-progress");
        assert_eq!(Phase::Destroying.css_class(), "phase-in-progress");
    }

    #[test]
    fn test_phase_serde_roundtrip() {
        for phase in [
            Phase::Pending, Phase::Compiling, Phase::Initializing,
            Phase::Planning, Phase::Applying, Phase::Ready,
            Phase::Drifted, Phase::Failed, Phase::Destroying,
        ] {
            let json = serde_json::to_string(&phase).unwrap();
            let back: Phase = serde_json::from_str(&json).unwrap();
            assert_eq!(phase, back);
        }
    }

    #[test]
    fn test_phase_serde_screaming_snake_case() {
        let json = serde_json::to_string(&Phase::Pending).unwrap();
        assert_eq!(json, r#""PENDING""#);

        let json = serde_json::to_string(&Phase::Drifted).unwrap();
        assert_eq!(json, r#""DRIFTED""#);
    }

    #[test]
    fn test_phase_deserialize_invalid() {
        let result: Result<Phase, _> = serde_json::from_str(r#""INVALID_PHASE""#);
        assert!(result.is_err());
    }

    #[test]
    fn test_phase_is_active_and_terminal_mutually_exclusive() {
        for phase in [
            Phase::Pending, Phase::Compiling, Phase::Initializing,
            Phase::Planning, Phase::Applying, Phase::Ready,
            Phase::Drifted, Phase::Failed, Phase::Destroying,
        ] {
            assert!(
                !(phase.is_active() && phase.is_terminal()),
                "{:?} should not be both active and terminal",
                phase
            );
        }
    }
}
