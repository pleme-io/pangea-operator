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

/// Default reconcile concurrency for a single controller (kube-rs's
/// `Controller::run` returns a stream; this is the parallelism we
/// feed it via `for_each_concurrent`). Pre-2026-05 the operator used
/// `for_each` (effectively serial), which meant a fast-cycling
/// template like `cloudflare-pleme` (apply ~2s, requeue 30s) could
/// monopolize the queue and starve siblings like `pleme-io-opensource`.
///
/// 4 is a balance: enough parallelism that one tight loop doesn't
/// dominate, low enough that we don't slam tofu/PG with N parallel
/// applies that fight for the same workspace dir or state lock.
/// Raise via `PANGEA_RECONCILE_WORKERS` env var when the fleet has
/// more than ~10 active templates per controller.
pub const DEFAULT_RECONCILE_WORKERS: usize = 4;

/// Read `PANGEA_RECONCILE_WORKERS` from the environment, falling back
/// to `DEFAULT_RECONCILE_WORKERS`. Clamps the value to the inclusive
/// range [1, 32] — 0 would deadlock the stream and >32 is unlikely
/// to be desired (would need significant infra-side rework first).
pub fn reconcile_workers_from_env() -> usize {
    let v = std::env::var("PANGEA_RECONCILE_WORKERS")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(DEFAULT_RECONCILE_WORKERS);
    v.clamp(1, 32)
}

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
        Phase::Pending => Phase::Verifying,
        Phase::Verifying => Phase::Verified,
        Phase::Verified => Phase::Compiling,
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
        // M2 — Verifying = checking ArchitectureGem registry; Verified
        // = registry green, ready to compile. Both report Reconciling=
        // True so FluxCD knows progress is happening.
        Phase::Verifying => (false, true, false),
        Phase::Verified => (false, true, false),
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
    fn test_parse_duration_bare_number_defaults_to_seconds() {
        assert_eq!(parse_duration("60"), Some(Duration::from_secs(60)));
        assert_eq!(parse_duration("0"), Some(Duration::from_secs(0)));
    }

    #[test]
    fn test_parse_duration_whitespace_trimmed() {
        assert_eq!(parse_duration("  5m  "), Some(Duration::from_secs(300)));
    }

    #[test]
    fn test_parse_duration_invalid_unit() {
        assert_eq!(parse_duration("5x"), None);
        assert_eq!(parse_duration("10y"), None);
    }

    #[test]
    fn test_parse_duration_non_numeric() {
        assert_eq!(parse_duration("abcs"), None);
    }

    #[test]
    fn test_exponential_backoff() {
        assert_eq!(exponential_backoff(0, 30, 600), Duration::from_secs(30));
        assert_eq!(exponential_backoff(1, 30, 600), Duration::from_secs(60));
        assert_eq!(exponential_backoff(2, 30, 600), Duration::from_secs(120));
        assert_eq!(exponential_backoff(5, 30, 600), Duration::from_secs(600)); // Capped
    }

    #[test]
    fn test_exponential_backoff_capped_at_max() {
        let result = exponential_backoff(10, 30, 600);
        assert_eq!(result, Duration::from_secs(600));
    }

    #[test]
    fn test_exponential_backoff_zero_base() {
        assert_eq!(exponential_backoff(5, 0, 600), Duration::from_secs(0));
    }

    #[test]
    fn test_exponential_backoff_large_attempt_saturates() {
        let result = exponential_backoff(31, 30, 3600);
        assert!(result <= Duration::from_secs(3600));
    }

    #[test]
    fn test_next_phase() {
        // M2 inserts Verifying + Verified between Pending and Compiling.
        assert_eq!(next_phase(Phase::Pending, true), Phase::Verifying);
        assert_eq!(next_phase(Phase::Verifying, true), Phase::Verified);
        assert_eq!(next_phase(Phase::Verified, true), Phase::Compiling);
        assert_eq!(next_phase(Phase::Compiling, true), Phase::Initializing);
        assert_eq!(next_phase(Phase::Planning, false), Phase::Failed);
    }

    #[test]
    fn test_next_phase_full_success_chain() {
        assert_eq!(next_phase(Phase::Pending, true), Phase::Verifying);
        assert_eq!(next_phase(Phase::Verifying, true), Phase::Verified);
        assert_eq!(next_phase(Phase::Verified, true), Phase::Compiling);
        assert_eq!(next_phase(Phase::Compiling, true), Phase::Initializing);
        assert_eq!(next_phase(Phase::Initializing, true), Phase::Planning);
        assert_eq!(next_phase(Phase::Planning, true), Phase::Applying);
        assert_eq!(next_phase(Phase::Applying, true), Phase::Ready);
        assert_eq!(next_phase(Phase::Ready, true), Phase::Ready);
    }

    #[test]
    fn test_next_phase_failure_always_returns_failed() {
        for phase in [
            Phase::Pending, Phase::Verifying, Phase::Verified,
            Phase::Compiling, Phase::Initializing,
            Phase::Planning, Phase::Applying, Phase::Ready,
            Phase::Drifted, Phase::Failed, Phase::Destroying,
        ] {
            assert_eq!(next_phase(phase, false), Phase::Failed,
                "Phase {:?} with failure should go to Failed", phase);
        }
    }

    #[test]
    fn test_next_phase_drifted_success_goes_to_planning() {
        assert_eq!(next_phase(Phase::Drifted, true), Phase::Planning);
    }

    #[test]
    fn test_next_phase_failed_success_retries_from_pending() {
        assert_eq!(next_phase(Phase::Failed, true), Phase::Pending);
    }

    #[test]
    fn test_next_phase_destroying_success_goes_to_pending() {
        assert_eq!(next_phase(Phase::Destroying, true), Phase::Pending);
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

    #[test]
    fn test_conditions_for_phase_pending() {
        let conditions = conditions_for_phase(Phase::Pending, None);
        assert_eq!(conditions[0].status, "False"); // Not Ready
        assert_eq!(conditions[1].status, "False"); // Not Reconciling
        assert_eq!(conditions[2].status, "False"); // No drift
        assert!(conditions[0].message.contains("Waiting"));
    }

    #[test]
    fn test_conditions_for_phase_destroying() {
        let conditions = conditions_for_phase(Phase::Destroying, None);
        assert_eq!(conditions[0].status, "False");
        assert_eq!(conditions[1].status, "False");
        assert_eq!(conditions[2].status, "False");
        assert!(conditions[0].message.contains("destroyed"));
    }

    #[test]
    fn test_conditions_for_phase_active_phases_reconciling() {
        for phase in [Phase::Compiling, Phase::Initializing, Phase::Planning, Phase::Applying] {
            let conditions = conditions_for_phase(phase, None);
            assert_eq!(conditions[1].status, "True",
                "Phase {:?} should have Reconciling=True", phase);
        }
    }

    #[test]
    fn test_conditions_for_phase_custom_error_overrides_message() {
        let conditions = conditions_for_phase(Phase::Ready, Some("override msg"));
        assert_eq!(conditions[0].message, "override msg");
    }

    #[test]
    fn test_conditions_for_suspended() {
        let conditions = conditions_for_suspended();
        assert_eq!(conditions.len(), 3);

        assert_eq!(conditions[0].r#type, "Ready");
        assert_eq!(conditions[0].status, "False");
        assert_eq!(conditions[0].reason, "Suspended");

        assert_eq!(conditions[1].r#type, "Reconciling");
        assert_eq!(conditions[1].status, "False");
        assert_eq!(conditions[1].reason, "Suspended");

        assert_eq!(conditions[2].r#type, "DriftDetected");
        assert_eq!(conditions[2].status, "False");
        assert_eq!(conditions[2].reason, "Suspended");

        for c in &conditions {
            assert!(c.message.contains("suspended"));
        }
    }

    #[test]
    fn test_create_condition_true() {
        let c = create_condition("TestType", true, "TestReason", "test message");
        assert_eq!(c.r#type, "TestType");
        assert_eq!(c.status, "True");
        assert_eq!(c.reason, "TestReason");
        assert_eq!(c.message, "test message");
    }

    #[test]
    fn test_create_condition_false() {
        let c = create_condition("TestType", false, "TestReason", "test message");
        assert_eq!(c.status, "False");
    }

    #[test]
    fn test_reconcile_action_default() {
        let action = ReconcileAction::default();
        match action {
            ReconcileAction::Requeue(d) => assert_eq!(d, DEFAULT_REQUEUE_INTERVAL),
            _ => panic!("Expected Requeue"),
        }
    }

    #[test]
    fn test_requeue_intervals_ordering() {
        assert!(SHORT_REQUEUE_INTERVAL < DEFAULT_REQUEUE_INTERVAL);
        assert!(ERROR_REQUEUE_INTERVAL < DEFAULT_REQUEUE_INTERVAL);
        assert!(SHORT_REQUEUE_INTERVAL < ERROR_REQUEUE_INTERVAL);
    }

    // ── Item C — workqueue concurrency tests ──────────────────────
    //
    // Reproducer of the rio incident: cloudflare-pleme's tight cycle
    // (apply ~2s, requeue 30s) ran on the only worker, starving
    // pleme-io-opensource for hours. Switching to
    // for_each_concurrent with PANGEA_RECONCILE_WORKERS controls the
    // parallelism. These tests exercise the env-var parsing layer.

    #[test]
    fn workers_default_when_env_unset() {
        std::env::remove_var("PANGEA_RECONCILE_WORKERS");
        assert_eq!(reconcile_workers_from_env(), DEFAULT_RECONCILE_WORKERS);
    }

    #[test]
    fn workers_clamped_to_min_one() {
        // 0 would deadlock for_each_concurrent (it would never make
        // progress) — clamp to at least 1 to keep the operator alive.
        std::env::set_var("PANGEA_RECONCILE_WORKERS", "0");
        assert_eq!(reconcile_workers_from_env(), 1);
        std::env::remove_var("PANGEA_RECONCILE_WORKERS");
    }

    #[test]
    fn workers_clamped_to_max_thirty_two() {
        // 32 is the upper guard. Larger values would need infra-side
        // rework (PG pool sizing, tofu workspace dir contention).
        std::env::set_var("PANGEA_RECONCILE_WORKERS", "1000");
        assert_eq!(reconcile_workers_from_env(), 32);
        std::env::remove_var("PANGEA_RECONCILE_WORKERS");
    }

    #[test]
    fn workers_honors_valid_value() {
        std::env::set_var("PANGEA_RECONCILE_WORKERS", "8");
        assert_eq!(reconcile_workers_from_env(), 8);
        std::env::remove_var("PANGEA_RECONCILE_WORKERS");
    }

    #[test]
    fn workers_falls_back_on_garbage() {
        std::env::set_var("PANGEA_RECONCILE_WORKERS", "not-a-number");
        assert_eq!(reconcile_workers_from_env(), DEFAULT_RECONCILE_WORKERS);
        std::env::remove_var("PANGEA_RECONCILE_WORKERS");
    }

    #[test]
    fn default_workers_is_safe_starting_point() {
        // 4 chosen as a balance: enough parallelism that a tight loop
        // doesn't dominate, low enough not to slam tofu/PG. Pin the
        // value so a future change is intentional.
        assert_eq!(DEFAULT_RECONCILE_WORKERS, 4);
    }
}
