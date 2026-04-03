//! Core reconciliation types and utilities.

use crate::crd::Phase;
use chrono::Utc;
use std::time::Duration;

/// Default requeue interval for successful reconciliation.
pub const DEFAULT_REQUEUE_INTERVAL: Duration = Duration::from_secs(300); // 5 minutes

/// Short requeue interval for pending operations.
pub const SHORT_REQUEUE_INTERVAL: Duration = Duration::from_secs(30);

/// Error requeue interval with backoff.
pub const ERROR_REQUEUE_INTERVAL: Duration = Duration::from_secs(60);

/// Action to take after reconciliation.
#[derive(Debug, Clone)]
pub enum ReconcileAction {
    /// Requeue after the specified duration.
    Requeue(Duration),

    /// Do not requeue (finalizer removed, etc.).
    Done,
}

impl Default for ReconcileAction {
    fn default() -> Self {
        ReconcileAction::Requeue(DEFAULT_REQUEUE_INTERVAL)
    }
}

/// Parse a duration string like "5m", "1h", "30s" into a Duration.
pub fn parse_duration(s: &str) -> Option<Duration> {
    let s = s.trim();
    if s.is_empty() {
        return None;
    }

    let (num_str, unit) = if s.ends_with("ms") {
        (&s[..s.len() - 2], "ms")
    } else {
        let last_char = s.chars().last()?;
        if last_char.is_ascii_digit() {
            (s, "s") // Default to seconds
        } else {
            (&s[..s.len() - 1], &s[s.len() - 1..])
        }
    };

    let num: u64 = num_str.parse().ok()?;

    match unit {
        "ms" => Some(Duration::from_millis(num)),
        "s" => Some(Duration::from_secs(num)),
        "m" => Some(Duration::from_secs(num * 60)),
        "h" => Some(Duration::from_secs(num * 3600)),
        "d" => Some(Duration::from_secs(num * 86400)),
        _ => None,
    }
}

/// Calculate exponential backoff duration.
pub fn exponential_backoff(attempt: u32, base_secs: u32, max_secs: u32) -> Duration {
    let backoff = base_secs.saturating_mul(2u32.saturating_pow(attempt));
    Duration::from_secs(backoff.min(max_secs) as u64)
}

/// Determine next phase based on current phase.
pub fn next_phase(current: Phase, success: bool) -> Phase {
    if !success {
        return Phase::Failed;
    }

    match current {
        Phase::Pending => Phase::Compiling,
        Phase::Compiling => Phase::Initializing,
        Phase::Initializing => Phase::Planning,
        Phase::Planning => Phase::Applying,
        Phase::Applying => Phase::Ready,
        Phase::Ready => Phase::Ready,
        Phase::Drifted => Phase::Planning,
        Phase::Failed => Phase::Pending, // Retry from beginning
        Phase::Destroying => Phase::Pending,
    }
}

/// Create a Kubernetes-style condition.
pub fn create_condition(
    condition_type: &str,
    status: bool,
    reason: &str,
    message: &str,
) -> crate::crd::Condition {
    crate::crd::Condition {
        r#type: condition_type.to_string(),
        status: if status { "True" } else { "False" }.to_string(),
        last_transition_time: Utc::now(),
        reason: reason.to_string(),
        message: message.to_string(),
    }
}

/// Generate the canonical set of conditions for a given phase.
///
/// FluxCD healthChecks watches `status.conditions[type=Ready]`. This
/// function ensures every phase transition emits well-defined conditions
/// so FluxCD can determine readiness at any point.
pub fn conditions_for_phase(phase: Phase, error_msg: Option<&str>) -> Vec<crate::crd::Condition> {
    let (ready, reconciling, drift) = match phase {
        Phase::Pending => (false, false, false),
        Phase::Compiling => (false, true, false),
        Phase::Initializing => (false, true, false),
        Phase::Planning => (false, true, false),
        Phase::Applying => (false, true, false),
        Phase::Ready => (true, false, false),
        Phase::Drifted => (false, false, true),
        Phase::Failed => (false, false, false),
        Phase::Destroying => (false, false, false),
    };

    let reason = format!("{}", phase);

    let message = match (phase, error_msg) {
        (_, Some(msg)) => msg.to_string(),
        (Phase::Ready, _) => "Infrastructure is up to date".into(),
        (Phase::Drifted, _) => "Infrastructure drift detected".into(),
        (Phase::Failed, _) => "Operation failed".into(),
        (Phase::Pending, _) => "Waiting to be processed".into(),
        (Phase::Destroying, _) => "Infrastructure is being destroyed".into(),
        (phase, _) => format!("{} in progress", phase),
    };

    vec![
        create_condition("Ready", ready, &reason, &message),
        create_condition("Reconciling", reconciling, &reason, &message),
        create_condition("DriftDetected", drift, &reason, &message),
    ]
}

/// Generate conditions for a suspended template.
pub fn conditions_for_suspended() -> Vec<crate::crd::Condition> {
    vec![
        create_condition("Ready", false, "Suspended", "Template reconciliation is suspended"),
        create_condition("Reconciling", false, "Suspended", "Template reconciliation is suspended"),
        create_condition("DriftDetected", false, "Suspended", "Template reconciliation is suspended"),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_duration() {
        assert_eq!(parse_duration("5m"), Some(Duration::from_secs(300)));
        assert_eq!(parse_duration("1h"), Some(Duration::from_secs(3600)));
        assert_eq!(parse_duration("30s"), Some(Duration::from_secs(30)));
        assert_eq!(parse_duration("100ms"), Some(Duration::from_millis(100)));
        assert_eq!(parse_duration("1d"), Some(Duration::from_secs(86400)));
        assert_eq!(parse_duration(""), None);
    }

    #[test]
    fn test_exponential_backoff() {
        assert_eq!(exponential_backoff(0, 30, 600), Duration::from_secs(30));
        assert_eq!(exponential_backoff(1, 30, 600), Duration::from_secs(60));
        assert_eq!(exponential_backoff(2, 30, 600), Duration::from_secs(120));
        assert_eq!(exponential_backoff(5, 30, 600), Duration::from_secs(600)); // Capped
    }

    #[test]
    fn test_next_phase() {
        assert_eq!(next_phase(Phase::Pending, true), Phase::Compiling);
        assert_eq!(next_phase(Phase::Compiling, true), Phase::Initializing);
        assert_eq!(next_phase(Phase::Planning, false), Phase::Failed);
    }

    #[test]
    fn test_conditions_for_phase_ready() {
        let conditions = conditions_for_phase(Phase::Ready, None);
        assert_eq!(conditions.len(), 3);
        assert_eq!(conditions[0].r#type, "Ready");
        assert_eq!(conditions[0].status, "True");
        assert_eq!(conditions[1].r#type, "Reconciling");
        assert_eq!(conditions[1].status, "False");
        assert_eq!(conditions[2].r#type, "DriftDetected");
        assert_eq!(conditions[2].status, "False");
    }

    #[test]
    fn test_conditions_for_phase_compiling() {
        let conditions = conditions_for_phase(Phase::Compiling, None);
        assert_eq!(conditions[0].r#type, "Ready");
        assert_eq!(conditions[0].status, "False");
        assert_eq!(conditions[1].r#type, "Reconciling");
        assert_eq!(conditions[1].status, "True");
        assert_eq!(conditions[2].r#type, "DriftDetected");
        assert_eq!(conditions[2].status, "False");
    }

    #[test]
    fn test_conditions_for_phase_drifted() {
        let conditions = conditions_for_phase(Phase::Drifted, None);
        assert_eq!(conditions[0].status, "False"); // Ready
        assert_eq!(conditions[1].status, "False"); // Reconciling
        assert_eq!(conditions[2].status, "True"); // DriftDetected
    }

    #[test]
    fn test_conditions_for_phase_failed_with_error() {
        let conditions = conditions_for_phase(Phase::Failed, Some("tofu plan failed"));
        assert_eq!(conditions[0].status, "False");
        assert_eq!(conditions[0].reason, "Failed");
        assert_eq!(conditions[0].message, "tofu plan failed");
    }
}
