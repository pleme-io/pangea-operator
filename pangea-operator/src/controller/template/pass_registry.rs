use std::collections::HashMap;
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

use chrono::{DateTime, Duration, Utc};
use futures::future::{AbortHandle, Abortable};

use crate::crd::StallState;

pub fn pass_key(namespace: &str, name: &str) -> String {
    [namespace, name].join("/")
}

struct Entry {
    id: u64,
    abort: AbortHandle,
    last_sign: DateTime<Utc>,
}

#[derive(Default)]
pub struct PassRegistry {
    entries: Mutex<HashMap<String, Entry>>,
    next: AtomicU64,
}

struct Registration<'a> {
    registry: &'a PassRegistry,
    key: String,
    id: u64,
}

impl Drop for Registration<'_> {
    fn drop(&mut self) {
        let mut entries = self.registry.lock();
        if entries.get(&self.key).is_some_and(|e| e.id == self.id) {
            entries.remove(&self.key);
        }
    }
}

impl PassRegistry {
    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<String, Entry>> {
        self.entries.lock().unwrap_or_else(|p| p.into_inner())
    }

    pub async fn run<F: Future>(&self, key: String, work: F) -> Option<F::Output> {
        let (abort, registration) = AbortHandle::new_pair();
        let id = self.next.fetch_add(1, Ordering::Relaxed);
        self.lock().insert(
            key.clone(),
            Entry {
                id,
                abort,
                last_sign: Utc::now(),
            },
        );
        let _registered = Registration {
            registry: self,
            key,
            id,
        };
        Abortable::new(work, registration).await.ok()
    }

    pub fn beat_landed(&self, key: &str, at: DateTime<Utc>) {
        if let Some(e) = self.lock().get_mut(key) {
            e.last_sign = e.last_sign.max(at);
        }
    }

    pub fn holds(&self, key: &str) -> bool {
        self.lock().contains_key(key)
    }

    pub fn last_sign(&self, key: &str) -> Option<DateTime<Utc>> {
        self.lock().get(key).map(|e| e.last_sign)
    }

    pub fn preempt(
        &self,
        key: &str,
        stall: StallState,
        now: DateTime<Utc>,
        heartbeat: Duration,
    ) -> bool {
        let mut entries = self.lock();
        let stuck = entries
            .get(key)
            .is_some_and(|e| local_pass_is_stuck(stall, e.last_sign, now, heartbeat));
        if !stuck {
            return false;
        }
        if let Some(e) = entries.remove(key) {
            e.abort.abort();
        }
        true
    }
}

pub fn local_pass_is_stuck(
    stall: StallState,
    last_sign: DateTime<Utc>,
    now: DateTime<Utc>,
    heartbeat: Duration,
) -> bool {
    match stall {
        StallState::NoProgress => true,
        other => other.redispatches() && now - last_sign > heartbeat,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::controller::scheduling::{BudgetConfig, FairBudget};
    use std::sync::Arc;

    #[tokio::test]
    async fn a_finished_pass_leaves_no_entry() {
        let r = PassRegistry::default();
        assert_eq!(r.run("ns/t".into(), async { 7 }).await, Some(7));
        assert!(!r.holds("ns/t"));
    }

    #[tokio::test]
    async fn preempting_a_hung_pass_returns_it_and_releases_its_budget_slot() {
        let r = Arc::new(PassRegistry::default());
        let budgets = FairBudget::new(BudgetConfig {
            per_scope: 1,
            global: 1,
        });
        let held = Arc::clone(&budgets);
        let runner = Arc::clone(&r);
        let pass = tokio::spawn(async move {
            runner
                .run("ns/t".into(), async move {
                    let _permit = held.try_acquire("ws").expect("a free slot");
                    futures::future::pending::<()>().await;
                })
                .await
        });
        while !r.holds("ns/t") || budgets.in_flight("ws") == 0 {
            tokio::task::yield_now().await;
        }
        let later = Utc::now() + Duration::minutes(10);
        assert!(r.preempt(
            "ns/t",
            StallState::HeartbeatLost,
            later,
            Duration::minutes(5)
        ));
        let out = tokio::time::timeout(std::time::Duration::from_secs(5), pass)
            .await
            .expect("the preempted pass returns")
            .expect("the task joins");
        assert_eq!(out, None);
        assert_eq!(budgets.in_flight("ws"), 0);
        assert!(budgets.try_acquire("ws").is_some());
        assert!(!r.holds("ns/t"));
    }

    #[test]
    fn a_pass_whose_own_beat_landed_recently_is_not_stuck_on_a_stale_heartbeat_verdict() {
        let t0 = Utc::now();
        let five = Duration::minutes(5);
        assert!(!local_pass_is_stuck(
            StallState::HeartbeatLost,
            t0,
            t0 + Duration::minutes(4),
            five
        ));
        assert!(local_pass_is_stuck(
            StallState::HeartbeatLost,
            t0,
            t0 + Duration::minutes(6),
            five
        ));
        assert!(local_pass_is_stuck(StallState::NoProgress, t0, t0, five));
        assert!(!local_pass_is_stuck(StallState::QueuedBehind, t0, t0, five));
        assert!(!local_pass_is_stuck(StallState::Unnamed, t0, t0, five));
    }

    #[test]
    fn receipt_2026_10_27_an_orphan_verdict_never_preempts_the_new_incarnations_live_reconcile() {
        let boot = Utc::now();
        let five = Duration::minutes(5);
        for stall in [StallState::OrphanedPhase, StallState::PendingNotStarted] {
            assert!(
                !local_pass_is_stuck(stall, boot, boot + Duration::seconds(82), five),
                "{stall}: a compile 82s into the new incarnation is live"
            );
            assert!(local_pass_is_stuck(
                stall,
                boot,
                boot + Duration::minutes(6),
                five
            ));
        }
    }

    #[tokio::test]
    async fn a_live_beat_protects_the_pass_from_preemption() {
        let r = Arc::new(PassRegistry::default());
        let runner = Arc::clone(&r);
        let pass = tokio::spawn(async move {
            runner
                .run("ns/t".into(), futures::future::pending::<()>())
                .await
        });
        while !r.holds("ns/t") {
            tokio::task::yield_now().await;
        }
        let later = Utc::now() + Duration::minutes(10);
        r.beat_landed("ns/t", later - Duration::seconds(30));
        assert!(!r.preempt(
            "ns/t",
            StallState::HeartbeatLost,
            later,
            Duration::minutes(5)
        ));
        assert!(r.holds("ns/t"));
        pass.abort();
    }
}
