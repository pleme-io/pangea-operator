use chrono::{DateTime, Utc};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use crate::crd::{InFlightCall, LiveProgress, Phase, ProgressStep};

pub type Clock = Arc<dyn Fn() -> DateTime<Utc> + Send + Sync>;

pub fn system_clock() -> Clock {
    Arc::new(Utc::now)
}

#[derive(Clone)]
pub struct ProgressTracker {
    inner: Arc<Mutex<LiveProgress>>,
    revision: Arc<AtomicU64>,
    clock: Clock,
}

impl std::fmt::Debug for ProgressTracker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProgressTracker")
            .field("snapshot", &self.snapshot())
            .finish()
    }
}

impl ProgressTracker {
    pub fn start(phase: Phase, pod: Option<String>) -> Self {
        Self::with_clock(phase, pod, system_clock())
    }

    pub fn with_clock(phase: Phase, pod: Option<String>, clock: Clock) -> Self {
        let now = clock();
        Self {
            inner: Arc::new(Mutex::new(LiveProgress::new(phase, now, pod))),
            revision: Arc::new(AtomicU64::new(0)),
            clock,
        }
    }

    pub fn from_snapshot(snapshot: LiveProgress, clock: Clock) -> Self {
        Self {
            inner: Arc::new(Mutex::new(snapshot)),
            revision: Arc::new(AtomicU64::new(0)),
            clock,
        }
    }

    fn mutate(&self, f: impl FnOnce(&mut LiveProgress, DateTime<Utc>)) {
        let now = (self.clock)();
        let mut guard = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        f(&mut guard, now);
        self.revision.fetch_add(1, Ordering::SeqCst);
    }

    pub fn now(&self) -> DateTime<Utc> {
        (self.clock)()
    }

    pub fn step(&self, step: ProgressStep) {
        self.mutate(|p, now| {
            if p.step != Some(step) {
                p.step = Some(step);
                p.step_started_at = Some(now);
                p.in_flight = None;
            }
            p.last_progress_at = now;
        });
    }

    pub fn total(&self, total: u64) {
        self.mutate(|p, _| p.resources_total = Some(total));
    }

    pub fn begin(&self, address: &str, rpc: Option<&str>) {
        self.mutate(|p, now| {
            p.in_flight = Some(InFlightCall {
                address: address.to_string(),
                rpc: rpc.map(str::to_string),
                since: now,
            });
        });
    }

    pub fn advance(&self, address: &str) {
        self.mutate(|p, now| {
            p.resources_done = p.resources_done.saturating_add(1);
            if p.in_flight.as_ref().map(|c| c.address.as_str()) == Some(address) {
                p.in_flight = None;
            }
            p.last_progress_at = now;
        });
    }

    pub fn advance_to(&self, done: u64) {
        self.mutate(|p, now| {
            if p.resources_done != done {
                p.resources_done = done;
                p.last_progress_at = now;
            }
        });
    }

    pub fn credential_expires_at(&self, at: Option<DateTime<Utc>>) {
        self.mutate(|p, _| p.credential_expires_at = at);
    }

    pub fn finish(&self) {
        self.mutate(|p, now| {
            p.finished_at = Some(now);
            p.in_flight = None;
        });
    }

    pub fn last(&self) -> DateTime<Utc> {
        self.inner
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .last_progress_at
    }

    pub fn seconds_since_progress(&self) -> i64 {
        (self.now() - self.last()).num_seconds().max(0)
    }

    pub fn snapshot(&self) -> LiveProgress {
        self.inner.lock().unwrap_or_else(|p| p.into_inner()).clone()
    }

    pub fn revision(&self) -> u64 {
        self.revision.load(Ordering::SeqCst)
    }

    pub fn same_as(&self, other: &ProgressTracker) -> bool {
        Arc::ptr_eq(&self.inner, &other.inner)
    }
}

pub fn template_key(namespace: &str, name: &str) -> String {
    [namespace, name].join("/")
}

#[derive(Default)]
pub struct ProgressRegistry {
    trackers: Mutex<HashMap<String, ProgressTracker>>,
    clock: Option<Clock>,
}

impl std::fmt::Debug for ProgressRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProgressRegistry")
            .field("len", &self.len())
            .finish()
    }
}

impl ProgressRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_clock(clock: Clock) -> Self {
        Self {
            trackers: Mutex::new(HashMap::new()),
            clock: Some(clock),
        }
    }

    fn clock(&self) -> Clock {
        self.clock.clone().unwrap_or_else(system_clock)
    }

    fn map(&self) -> std::sync::MutexGuard<'_, HashMap<String, ProgressTracker>> {
        self.trackers.lock().unwrap_or_else(|p| p.into_inner())
    }

    pub fn begin_phase(&self, key: &str, phase: Phase, pod: Option<String>) -> ProgressTracker {
        let mut map = self.map();
        if let Some(existing) = map.get(key) {
            let snap = existing.snapshot();
            if snap.phase == phase && snap.finished_at.is_none() {
                return existing.clone();
            }
            if snap.phase == phase {
                let resumed = existing.clone();
                resumed.mutate(|p, _| p.finished_at = None);
                return resumed;
            }
        }
        let tracker = ProgressTracker::with_clock(phase, pod, self.clock());
        map.insert(key.to_string(), tracker.clone());
        tracker
    }

    pub fn adopt(&self, key: &str, snapshot: LiveProgress) -> ProgressTracker {
        let tracker = ProgressTracker::from_snapshot(snapshot, self.clock());
        self.map().insert(key.to_string(), tracker.clone());
        tracker
    }

    pub fn get(&self, key: &str) -> Option<ProgressTracker> {
        self.map().get(key).cloned()
    }

    pub fn contains(&self, key: &str) -> bool {
        self.map().contains_key(key)
    }

    pub fn remove(&self, key: &str) -> Option<ProgressTracker> {
        self.map().remove(key)
    }

    pub fn len(&self) -> usize {
        self.map().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn snapshot_all(&self) -> Vec<(String, LiveProgress)> {
        self.map()
            .iter()
            .map(|(k, t)| (k.clone(), t.snapshot()))
            .collect()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PublishThrottle {
    pub interval: chrono::Duration,
    pub every_resources: u64,
}

impl Default for PublishThrottle {
    fn default() -> Self {
        Self {
            interval: chrono::Duration::seconds(15),
            every_resources: 50,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Published {
    pub at: DateTime<Utc>,
    pub revision: u64,
    pub resources_done: u64,
    pub step: Option<ProgressStep>,
}

impl PublishThrottle {
    pub fn due(
        &self,
        last: Option<&Published>,
        now: DateTime<Utc>,
        revision: u64,
        snapshot: &LiveProgress,
    ) -> bool {
        let Some(last) = last else {
            return true;
        };
        if revision == last.revision {
            return false;
        }
        if snapshot.step != last.step || snapshot.finished_at.is_some() {
            return true;
        }
        if snapshot.resources_done.saturating_sub(last.resources_done) >= self.every_resources {
            return true;
        }
        now - last.at >= self.interval
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicI64;

    fn fake_clock(start: i64) -> (Clock, Arc<AtomicI64>) {
        let secs = Arc::new(AtomicI64::new(start));
        let s = secs.clone();
        let clock: Clock =
            Arc::new(move || DateTime::<Utc>::from_timestamp(s.load(Ordering::SeqCst), 0).unwrap());
        (clock, secs)
    }

    fn at(secs: i64) -> DateTime<Utc> {
        DateTime::<Utc>::from_timestamp(secs, 0).unwrap()
    }

    #[test]
    fn advance_moves_last_progress_and_clears_matching_in_flight() {
        let (clock, secs) = fake_clock(1_000);
        let t = ProgressTracker::with_clock(Phase::Applying, Some("pod-a".into()), clock);
        t.step(ProgressStep::Apply);
        t.total(3);
        secs.store(1_010, Ordering::SeqCst);
        t.begin("github_repository.a", Some("ApplyResourceChange"));
        assert_eq!(t.last(), at(1_000));
        secs.store(1_020, Ordering::SeqCst);
        t.advance("github_repository.a");
        let snap = t.snapshot();
        assert_eq!(snap.resources_done, 1);
        assert_eq!(snap.resources_total, Some(3));
        assert!(snap.in_flight.is_none());
        assert_eq!(snap.last_progress_at, at(1_020));
    }

    #[test]
    fn begin_alone_is_not_progress() {
        let (clock, secs) = fake_clock(0);
        let t = ProgressTracker::with_clock(Phase::Planning, None, clock);
        secs.store(500, Ordering::SeqCst);
        t.begin("x.y", Some("ReadResource"));
        assert_eq!(t.seconds_since_progress(), 500);
        let inflight = t.snapshot().in_flight.unwrap();
        assert_eq!(inflight.address, "x.y");
        assert_eq!(inflight.since, at(500));
    }

    #[test]
    fn advance_to_only_counts_a_change() {
        let (clock, secs) = fake_clock(0);
        let t = ProgressTracker::with_clock(Phase::Applying, None, clock);
        secs.store(10, Ordering::SeqCst);
        t.advance_to(0);
        assert_eq!(t.last(), at(0));
        t.advance_to(4);
        assert_eq!(t.last(), at(10));
        assert_eq!(t.snapshot().resources_done, 4);
    }

    #[test]
    fn step_change_restamps_step_started_at() {
        let (clock, secs) = fake_clock(0);
        let t = ProgressTracker::with_clock(Phase::Planning, None, clock);
        t.step(ProgressStep::Compile);
        secs.store(30, Ordering::SeqCst);
        t.step(ProgressStep::Plan);
        let snap = t.snapshot();
        assert_eq!(snap.step, Some(ProgressStep::Plan));
        assert_eq!(snap.step_started_at, Some(at(30)));
        assert_eq!(snap.phase_started_at, at(0));
    }

    #[test]
    fn registry_keeps_the_tracker_within_a_phase_and_replaces_it_across_phases() {
        let (clock, _) = fake_clock(0);
        let reg = ProgressRegistry::with_clock(clock);
        let a = reg.begin_phase("ns/t", Phase::Planning, None);
        let b = reg.begin_phase("ns/t", Phase::Planning, None);
        assert!(a.same_as(&b));
        let c = reg.begin_phase("ns/t", Phase::Applying, None);
        assert!(!a.same_as(&c));
        assert_eq!(reg.get("ns/t").unwrap().snapshot().phase, Phase::Applying);
    }

    #[test]
    fn throttle_publishes_first_then_on_interval_or_resource_batch() {
        let throttle = PublishThrottle::default();
        let mut snap = LiveProgress::new(Phase::Applying, at(0), None);
        assert!(throttle.due(None, at(0), 0, &snap));
        let last = Published {
            at: at(0),
            revision: 1,
            resources_done: 0,
            step: None,
        };
        assert!(!throttle.due(Some(&last), at(100), 1, &snap));
        snap.resources_done = 3;
        assert!(!throttle.due(Some(&last), at(5), 2, &snap));
        assert!(throttle.due(Some(&last), at(15), 2, &snap));
        snap.resources_done = 50;
        assert!(throttle.due(Some(&last), at(1), 3, &snap));
        snap.resources_done = 0;
        snap.step = Some(ProgressStep::Apply);
        assert!(throttle.due(Some(&last), at(1), 4, &snap));
    }
}
