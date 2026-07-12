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

/// Clamp a raw worker-count value to the safe inclusive range [1, 32],
/// falling back to [`DEFAULT_RECONCILE_WORKERS`] when `raw` is `None`
/// (env var unset or failed to parse).
///
/// 0 would deadlock `for_each_concurrent` (it would never make
/// progress); >32 is unlikely to be desired (would need significant
/// infra-side rework first — PG pool sizing, tofu workspace dir
/// contention).
///
/// Pure — no I/O, no process-global state — so callers (including
/// tests) pass a plain `Option<usize>` instead of round-tripping
/// through `std::env`. This is the load-bearing half of the split:
/// [`reconcile_workers_from_env`] performs the one environment read
/// and hands the raw value here.
pub fn clamp_reconcile_workers(raw: Option<usize>) -> usize {
    raw.unwrap_or(DEFAULT_RECONCILE_WORKERS).clamp(1, 32)
}

/// Read `PANGEA_RECONCILE_WORKERS` from the environment and clamp it
/// via [`clamp_reconcile_workers`]. This is the only place in the
/// crate that reads this env var; call it once at startup (see
/// `TemplateController::run`) and thread the resulting `usize`
/// through as a plain value from there on — never re-read the
/// environment mid-run.
pub fn reconcile_workers_from_env() -> usize {
    let raw = std::env::var("PANGEA_RECONCILE_WORKERS")
        .ok()
        .and_then(|s| s.parse::<usize>().ok());
    clamp_reconcile_workers(raw)
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

    let (num_str, unit) = if let Some(stripped) = s.strip_suffix("ms") {
        (stripped, "ms")
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
        Phase::CompileBlocked => Phase::Compiling, // Self-heals: retry the compile
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
/// Whether the template's compiled config is at the source's git edge.
/// Derived from `status.compiledRevision` vs `status.observedHeadRevision`
/// (see `template::status::source_fresh_state`). This is what gates the
/// `Ready` condition: `Ready=True` is structurally impossible while a git
/// source is `Behind` or `Unverified` — ending the "Ready while behind
/// HEAD" lie. Tier-honest: this is a runtime C2 observation surface
/// (parse-time-rejected on the condition), not a compile error —
/// `Phase::Ready` stays a constructible enum.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SourceFreshState {
    /// compiledRevision == observed remote HEAD — at the git edge.
    Fresh,
    /// The remote moved past the compiled revision (or none was ever
    /// compiled) — behind the edge; Ready cannot be True.
    Behind,
    /// HEAD has not been observed yet (pre-first-observation, or the
    /// probe is failing) — cannot claim at-HEAD; Ready cannot be True.
    Unverified,
    /// Non-git source (inline / configMap) — no git edge to track;
    /// freshness does not gate Ready.
    NotApplicable,
}

impl SourceFreshState {
    /// Does this state permit `Ready=True`? Only when we have positively
    /// established at-HEAD (or there is no git edge to track).
    #[must_use]
    pub fn permits_ready(self) -> bool {
        matches!(self, SourceFreshState::Fresh | SourceFreshState::NotApplicable)
    }
}

pub fn conditions_for_phase(
    phase: Phase,
    error_msg: Option<&str>,
    source_fresh: SourceFreshState,
) -> Vec<crate::crd::Condition> {
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
        // THE git-edge gate: Ready is `true` ONLY when the source is at
        // HEAD (or non-git). A `Behind`/`Unverified` git source forces
        // Ready=False even while phase==Ready — so "Ready while behind
        // HEAD" cannot be advertised. The Phase enum no longer alone
        // determines the Ready condition.
        Phase::Ready => (source_fresh.permits_ready(), false, false),
        Phase::Drifted => (false, false, true),
        Phase::Failed => (false, false, false),
        // Self-healing park: actively retrying the compile on
        // backoff, so progress IS being attempted (Reconciling=True),
        // but the system is not Ready.
        Phase::CompileBlocked => (false, true, false),
        Phase::Destroying => (false, false, false),
    };

    let reason = format!("{}", phase);

    let message = match (phase, error_msg) {
        (_, Some(msg)) => msg.to_string(),
        // Ready-but-behind/unverified is reachable: name it honestly.
        (Phase::Ready, _) if !source_fresh.permits_ready() => match source_fresh {
            SourceFreshState::Behind => {
                "Source HEAD has advanced past the applied revision — recompiling".into()
            }
            _ => "Source HEAD not yet verified — observing the git edge".into(),
        },
        (Phase::Ready, _) => "Infrastructure is up to date".into(),
        (Phase::Drifted, _) => "Infrastructure drift detected".into(),
        (Phase::Failed, _) => "Operation failed".into(),
        (Phase::CompileBlocked, _) => {
            "Source HEAD does not compile — retrying on backoff; \
             resumes automatically when a compiling commit lands"
                .into()
        }
        (Phase::Pending, _) => "Waiting to be processed".into(),
        (Phase::Destroying, _) => "Infrastructure is being destroyed".into(),
        (phase, _) => format!("{} in progress", phase),
    };

    // The SourceFresh condition makes the git-edge state first-class on
    // the status surface (a printer column + a Flux/Prometheus signal).
    let (sf_ok, sf_reason) = match source_fresh {
        SourceFreshState::Fresh => (true, "AtHead"),
        SourceFreshState::NotApplicable => (true, "NoGitSource"),
        SourceFreshState::Behind => (false, "BehindHead"),
        SourceFreshState::Unverified => (false, "Unverified"),
    };
    let sf_msg = match source_fresh {
        SourceFreshState::Fresh => "Compiled revision is at the source's git HEAD",
        SourceFreshState::NotApplicable => "Non-git source; no git edge to track",
        SourceFreshState::Behind => "Compiled revision is behind the source's git HEAD",
        SourceFreshState::Unverified => "Source git HEAD not yet observed",
    };

    vec![
        create_condition("Ready", ready, &reason, &message),
        create_condition("Reconciling", reconciling, &reason, &message),
        create_condition("DriftDetected", drift, &reason, &message),
        create_condition("SourceFresh", sf_ok, sf_reason, sf_msg),
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
        let conditions = conditions_for_phase(Phase::Ready, None, SourceFreshState::Fresh);
        assert_eq!(conditions.len(), 4);
        assert_eq!(conditions[0].r#type, "Ready");
        assert_eq!(conditions[0].status, "True");
        assert_eq!(conditions[1].r#type, "Reconciling");
        assert_eq!(conditions[1].status, "False");
        assert_eq!(conditions[2].r#type, "DriftDetected");
        assert_eq!(conditions[2].status, "False");
        assert_eq!(conditions[3].r#type, "SourceFresh");
        assert_eq!(conditions[3].status, "True");
    }

    #[test]
    fn ready_condition_false_when_behind_head() {
        // THE headline structural invariant: phase==Ready + Behind HEAD ⇒
        // Ready=False. "Ready while behind HEAD" is unutterable.
        let behind = conditions_for_phase(Phase::Ready, None, SourceFreshState::Behind);
        assert_eq!(behind[0].r#type, "Ready");
        assert_eq!(behind[0].status, "False");
        assert_eq!(behind[3].r#type, "SourceFresh");
        assert_eq!(behind[3].status, "False");
        // Unverified (HEAD never observed) also cannot claim Ready.
        let unverified = conditions_for_phase(Phase::Ready, None, SourceFreshState::Unverified);
        assert_eq!(unverified[0].status, "False");
        // Fresh ⇒ Ready=True.
        let fresh = conditions_for_phase(Phase::Ready, None, SourceFreshState::Fresh);
        assert_eq!(fresh[0].status, "True");
        // Non-git source ⇒ Ready=True (no edge to track).
        let na = conditions_for_phase(Phase::Ready, None, SourceFreshState::NotApplicable);
        assert_eq!(na[0].status, "True");
        assert_eq!(na[3].status, "True");
    }

    #[test]
    fn test_conditions_for_phase_compiling() {
        let conditions = conditions_for_phase(Phase::Compiling, None, SourceFreshState::NotApplicable);
        assert_eq!(conditions[0].r#type, "Ready");
        assert_eq!(conditions[0].status, "False");
        assert_eq!(conditions[1].r#type, "Reconciling");
        assert_eq!(conditions[1].status, "True");
        assert_eq!(conditions[2].r#type, "DriftDetected");
        assert_eq!(conditions[2].status, "False");
    }

    #[test]
    fn test_conditions_for_phase_drifted() {
        let conditions = conditions_for_phase(Phase::Drifted, None, SourceFreshState::Fresh);
        assert_eq!(conditions[0].status, "False"); // Ready
        assert_eq!(conditions[1].status, "False"); // Reconciling
        assert_eq!(conditions[2].status, "True"); // DriftDetected
    }

    #[test]
    fn test_conditions_for_phase_failed_with_error() {
        let conditions = conditions_for_phase(Phase::Failed, Some("tofu plan failed"), SourceFreshState::Fresh);
        assert_eq!(conditions[0].status, "False");
        assert_eq!(conditions[0].reason, "Failed");
        assert_eq!(conditions[0].message, "tofu plan failed");
    }

    #[test]
    fn test_conditions_for_phase_pending() {
        let conditions = conditions_for_phase(Phase::Pending, None, SourceFreshState::NotApplicable);
        assert_eq!(conditions[0].status, "False"); // Not Ready
        assert_eq!(conditions[1].status, "False"); // Not Reconciling
        assert_eq!(conditions[2].status, "False"); // No drift
        assert!(conditions[0].message.contains("Waiting"));
    }

    #[test]
    fn test_conditions_for_phase_destroying() {
        let conditions = conditions_for_phase(Phase::Destroying, None, SourceFreshState::Fresh);
        assert_eq!(conditions[0].status, "False");
        assert_eq!(conditions[1].status, "False");
        assert_eq!(conditions[2].status, "False");
        assert!(conditions[0].message.contains("destroyed"));
    }

    #[test]
    fn test_conditions_for_phase_active_phases_reconciling() {
        for phase in [Phase::Compiling, Phase::Initializing, Phase::Planning, Phase::Applying] {
            let conditions = conditions_for_phase(phase, None, SourceFreshState::NotApplicable);
            assert_eq!(conditions[1].status, "True",
                "Phase {:?} should have Reconciling=True", phase);
        }
    }

    #[test]
    fn test_conditions_for_phase_custom_error_overrides_message() {
        let conditions = conditions_for_phase(Phase::Ready, Some("override msg"), SourceFreshState::Fresh);
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
    // parallelism.
    //
    // These tests call the pure `clamp_reconcile_workers` directly
    // with literal `Option<usize>` inputs — no `std::env::set_var` /
    // `remove_var`. Root cause of a 2026-07 CI flake
    // (`workers_clamped_to_min_one` observed `left: 4, right: 1`
    // under `cargo test --workspace`'s default parallel execution):
    // `std::env` is process-global, so N tests in this module setting
    // and clearing the same `PANGEA_RECONCILE_WORKERS` key raced each
    // other's reads — no amount of ordering within a single test body
    // fixes a race between *different* test threads. The fix is
    // structural, not a lock: the function under test no longer
    // touches the environment at all, so there is nothing left to
    // race, for these tests or any future ones.

    #[test]
    fn workers_default_when_raw_none() {
        assert_eq!(clamp_reconcile_workers(None), DEFAULT_RECONCILE_WORKERS);
    }

    #[test]
    fn workers_clamped_to_min_one() {
        // 0 would deadlock for_each_concurrent (it would never make
        // progress) — clamp to at least 1 to keep the operator alive.
        assert_eq!(clamp_reconcile_workers(Some(0)), 1);
    }

    #[test]
    fn workers_clamped_to_max_thirty_two() {
        // 32 is the upper guard. Larger values would need infra-side
        // rework (PG pool sizing, tofu workspace dir contention).
        assert_eq!(clamp_reconcile_workers(Some(1000)), 32);
    }

    #[test]
    fn workers_honors_valid_value() {
        assert_eq!(clamp_reconcile_workers(Some(8)), 8);
    }

    #[test]
    fn workers_falls_back_on_garbage() {
        // "not-a-number" never reaches `clamp_reconcile_workers` in
        // production — `reconcile_workers_from_env`'s `.parse().ok()`
        // turns a bad string into `None` before calling here. Exercise
        // that boundary directly instead of round-tripping it through
        // an env var.
        let raw: Option<usize> = "not-a-number".parse().ok();
        assert_eq!(raw, None);
        assert_eq!(clamp_reconcile_workers(raw), DEFAULT_RECONCILE_WORKERS);
    }

    #[test]
    fn default_workers_is_safe_starting_point() {
        // 4 chosen as a balance: enough parallelism that a tight loop
        // doesn't dominate, low enough not to slam tofu/PG. Pin the
        // value so a future change is intentional.
        assert_eq!(DEFAULT_RECONCILE_WORKERS, 4);
    }

    // ── The one test that still touches the environment ───────────
    //
    // Everything above proves the clamping algebra without touching
    // `std::env`. This test is the sole remaining check that
    // `reconcile_workers_from_env` — the thin wrapper that actually
    // reads `PANGEA_RECONCILE_WORKERS` at startup — wires the real
    // entry point through to `clamp_reconcile_workers` correctly. It
    // is the only test in this module (or file) that mutates process
    // env, and is guarded by a local mutex so it can never race a
    // sibling — matching the manual-mutex-guard pattern already used
    // for this same reason in `config.rs` (no `serial_test` dependency
    // needed for a single test).
    static ENV_VAR_TEST_GUARD: std::sync::Mutex<()> = std::sync::Mutex::new(());

    #[test]
    fn reconcile_workers_from_env_reads_and_clamps_the_real_env_var() {
        let _guard = ENV_VAR_TEST_GUARD
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);

        std::env::remove_var("PANGEA_RECONCILE_WORKERS");
        assert_eq!(reconcile_workers_from_env(), DEFAULT_RECONCILE_WORKERS);

        std::env::set_var("PANGEA_RECONCILE_WORKERS", "8");
        assert_eq!(reconcile_workers_from_env(), 8);

        std::env::set_var("PANGEA_RECONCILE_WORKERS", "1000");
        assert_eq!(reconcile_workers_from_env(), 32);

        std::env::remove_var("PANGEA_RECONCILE_WORKERS");
        assert_eq!(reconcile_workers_from_env(), DEFAULT_RECONCILE_WORKERS);
    }
}
