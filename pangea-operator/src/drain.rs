//! Waiting for in-flight reconcile work before controllers are aborted.
//!
//! ## The bug this exists to kill
//!
//! Shutdown used to be `sleep(5s)` then a hard `.abort()` on every
//! controller. Five seconds is a plausible-looking number that is wrong by
//! three orders of magnitude: a real cycle for a large workspace
//! (`pleme-io-opensource`: 846 repos / ~2777 resources) is a measured ~11m
//! plan plus ~11m apply. So *every* SIGTERM landed mid-cycle and discarded
//! the work, and the operator restarted into `Planning` from the top.
//!
//! On a cluster with active node consolidation that is not an edge case, it
//! is the steady state. Observed on camelot-eks 2026-07-30: 52 restarts in
//! 23h — one roughly every 26 minutes, against cycles needing ~22 minutes of
//! uninterrupted life. The workspace sat in `Applying` for 28 HOURS without
//! ever converging while every status condition read healthy, because a
//! SIGTERM exit is indistinguishable from a clean one: exit 0, phase
//! preserved, no escalation. Nothing was broken, so nothing complained.
//!
//! ## Why waiting on the budget is the right signal
//!
//! The operator already knows, exactly and in memory, whether expensive work
//! is running: `ControllerState.workspace_budgets` hands out an RAII
//! [`BudgetPermit`] around `Compiling`/`Planning`/`Applying` and *only* those
//! phases, freed on drop for both `Ok` and `Err`. `total_in_flight()` is
//! therefore already a truthful count of "cycles that would be destroyed if
//! we aborted right now" — no new tracking, no second source of truth to
//! drift, and no way to leak a phantom count and hang forever, because the
//! permit is released by the type system rather than by a code path someone
//! has to remember to write.
//!
//! ## Bounded, deliberately
//!
//! The wait is bounded and the bound is configuration, because the real
//! ceiling is not ours: the kubelet sends SIGKILL at
//! `terminationGracePeriodSeconds` regardless of what we would prefer.
//! Waiting past that buys nothing and costs the post-drain work — the pool
//! close, the final status write — which get skipped when we are killed
//! rather than allowed to finish. The chart derives this budget and the
//! grace period from a single value so the two cannot drift apart; see
//! `charts/pangea-operator/templates/deployment.yaml`.
//!
//! Hitting the deadline is a real event with a real cost, so it is a typed
//! outcome ([`DrainOutcome::Deadline`]) carrying the count of cycles about to
//! be discarded — logged at WARN. The old code could not distinguish "drained
//! cleanly" from "destroyed 3 cycles", because it never asked.

use std::time::Duration;

use tokio::time::Instant;

/// What the drain wait actually achieved. Distinguishing these is the point:
/// a clean drain and a deadline-hit drain look identical from the outside
/// (both are followed by `.abort()` and exit 0) but one of them just threw
/// away tens of minutes of provider work.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DrainOutcome {
    /// Nothing expensive was running; the wait returned without sleeping.
    /// The common case for an idle operator, and the reason a large budget
    /// costs nothing in practice.
    Idle,
    /// In-flight work completed within the budget. The cycles committed
    /// their state instead of being retried from scratch after restart.
    Drained { waited: Duration },
    /// The budget ran out with work still running. Those cycles are about to
    /// be aborted and will restart from the top. Not a crash — Postgres
    /// rolls back the uncommitted transaction, so no half-applied state can
    /// persist — but it is wasted work, and it is what the old unconditional
    /// 5-second sleep did on every single shutdown.
    Deadline {
        still_in_flight: usize,
        waited: Duration,
    },
}

impl DrainOutcome {
    /// True when no work was discarded. `Idle` counts: there was nothing to
    /// lose.
    pub fn is_clean(&self) -> bool {
        !matches!(self, DrainOutcome::Deadline { .. })
    }
}

/// Poll `in_flight` until it reports zero or `budget` elapses.
///
/// Takes the counter as a closure rather than the budget itself so the
/// policy here stays testable without standing up a `ControllerState`, and
/// so a future caller can drain on a different signal without rewriting the
/// loop.
///
/// A zero `budget` is honoured literally — the pre-check still runs, so an
/// idle operator returns [`DrainOutcome::Idle`] and exits immediately rather
/// than being told it hit a deadline it never waited for.
pub async fn await_in_flight<F>(in_flight: F, budget: Duration, poll: Duration) -> DrainOutcome
where
    F: Fn() -> usize,
{
    // Check before sleeping. An idle operator is the overwhelmingly common
    // case (a rolling upgrade between cycles, a leadership handoff), and it
    // should exit now rather than pay a fixed toll — which is the other half
    // of what the old fixed sleep got wrong: it was simultaneously far too
    // short for a busy operator and pure latency for an idle one.
    if in_flight() == 0 {
        return DrainOutcome::Idle;
    }

    let started = Instant::now();
    // Never poll less often than we are willing to wait; otherwise a budget
    // smaller than one poll interval would overshoot the SIGKILL it exists
    // to stay inside.
    let poll = poll.min(budget).max(Duration::from_millis(1));

    loop {
        tokio::time::sleep(poll).await;

        let remaining = in_flight();
        let waited = started.elapsed();

        if remaining == 0 {
            return DrainOutcome::Drained { waited };
        }
        if waited >= budget {
            return DrainOutcome::Deadline {
                still_in_flight: remaining,
                waited,
            };
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    const POLL: Duration = Duration::from_millis(5);

    #[tokio::test]
    async fn idle_returns_immediately_without_paying_the_budget() {
        // The old code slept its full 5s even with nothing running. This is
        // the regression guard for the latency half of that bug.
        let started = std::time::Instant::now();
        let outcome = await_in_flight(|| 0, Duration::from_secs(30), POLL).await;
        assert_eq!(outcome, DrainOutcome::Idle);
        assert!(
            started.elapsed() < Duration::from_millis(50),
            "an idle drain must not wait; took {:?}",
            started.elapsed()
        );
    }

    #[tokio::test]
    async fn waits_for_work_to_finish_then_reports_clean() {
        let count = Arc::new(AtomicUsize::new(2));
        let finisher = count.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(20)).await;
            finisher.store(0, Ordering::SeqCst);
        });

        let outcome = await_in_flight(
            move || count.load(Ordering::SeqCst),
            Duration::from_secs(5),
            POLL,
        )
        .await;

        assert!(
            matches!(outcome, DrainOutcome::Drained { .. }),
            "work finished inside the budget, got {outcome:?}"
        );
        assert!(outcome.is_clean());
    }

    #[tokio::test]
    async fn work_that_outlasts_the_budget_reports_what_is_being_discarded() {
        // The load-bearing assertion of the whole module: a shutdown that
        // destroys 3 cycles must say so, not exit 0 in silence.
        let outcome = await_in_flight(|| 3, Duration::from_millis(30), POLL).await;

        match outcome {
            DrainOutcome::Deadline {
                still_in_flight,
                waited,
            } => {
                assert_eq!(still_in_flight, 3);
                assert!(waited >= Duration::from_millis(30));
            }
            other => panic!("expected a deadline, got {other:?}"),
        }
        assert!(!outcome.is_clean());
    }

    #[tokio::test]
    async fn a_zero_budget_still_short_circuits_when_idle() {
        assert_eq!(
            await_in_flight(|| 0, Duration::ZERO, POLL).await,
            DrainOutcome::Idle,
            "a zero budget must not turn an idle drain into a deadline"
        );
    }

    #[tokio::test]
    async fn a_poll_longer_than_the_budget_does_not_overshoot() {
        // Guards the SIGKILL boundary: if poll > budget the naive loop sleeps
        // a full poll interval past the deadline, spending grace we do not
        // have on a check whose answer we already know.
        let started = std::time::Instant::now();
        let outcome =
            await_in_flight(|| 1, Duration::from_millis(20), Duration::from_secs(10)).await;
        assert!(matches!(outcome, DrainOutcome::Deadline { .. }));
        assert!(
            started.elapsed() < Duration::from_secs(1),
            "clamped poll should return near the budget, took {:?}",
            started.elapsed()
        );
    }

    #[tokio::test]
    async fn drains_against_the_real_budget_type() {
        // The counter above is a stand-in; this proves the actual production
        // signal behaves the way the drain assumes — nonzero while a permit
        // is held, zero once RAII frees it.
        use crate::controller::scheduling::{BudgetConfig, FairBudget};

        let budget = Arc::new(FairBudget::new(BudgetConfig {
            per_scope: 4,
            global: 16,
        }));
        let permit = budget.try_acquire("pleme-io-opensource").expect("slot");
        assert_eq!(budget.total_in_flight(), 1);

        let releasing = budget.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(20)).await;
            drop(permit);
            let _ = releasing;
        });

        let probe = budget.clone();
        let outcome = await_in_flight(
            move || probe.total_in_flight(),
            Duration::from_secs(5),
            POLL,
        )
        .await;

        assert!(
            matches!(outcome, DrainOutcome::Drained { .. }),
            "dropping the permit must end the drain, got {outcome:?}"
        );
    }
}
