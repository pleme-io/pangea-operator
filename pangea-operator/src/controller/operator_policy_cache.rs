//! In-memory snapshot of the cluster's `OperatorPolicy/default`.
//!
//! Every reconciler reads this on every reconcile via `policy_gate`.
//! Reads must be sub-microsecond and lock-free in the steady state.
//! Writes (policy CR change) are rare — at most one per spec mutation.
//!
//! The data type is decoupled from the kube-rs watcher loop so unit
//! tests can manipulate the snapshot directly without spinning up
//! a fake API server.
//!
//! Default state when no `OperatorPolicy/default` exists: fully
//! permissive (no suspends). This preserves backwards compatibility
//! with deployments pre-dating the primitive.

use crate::crd::{OperatorPolicySpec, OPERATOR_POLICY_SINGLETON};

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

/// Lock-protected snapshot + skipped-count counter.
///
/// Cheap to clone (Arc-wrapped); pass into every controller via
/// `ControllerState`.
pub struct OperatorPolicyCache {
    snapshot: RwLock<Arc<OperatorPolicySpec>>,
    skipped: AtomicU64,
}

impl OperatorPolicyCache {
    /// Construct a cache initialized to the default-allow state. All
    /// controllers proceed normally until the watcher reports a
    /// non-default `OperatorPolicy/default`.
    pub fn new_permissive() -> Self {
        Self {
            snapshot: RwLock::new(Arc::new(OperatorPolicySpec {
                global_suspend: false,
                global_suspend_reason: None,
                controller_suspend: Default::default(),
            })),
            skipped: AtomicU64::new(0),
        }
    }

    /// Cheap read of the current spec. Holds the read lock only long
    /// enough to clone the `Arc`. Returns an `Arc` so callers can
    /// inspect fields without holding the lock.
    pub fn read(&self) -> Arc<OperatorPolicySpec> {
        self.snapshot
            .read()
            .expect("operator-policy snapshot lock poisoned")
            .clone()
    }

    /// Replace the snapshot. Called by the watcher loop on
    /// `OperatorPolicy/default` Apply / Delete events.
    pub fn store(&self, spec: OperatorPolicySpec) {
        let mut guard = self
            .snapshot
            .write()
            .expect("operator-policy snapshot lock poisoned");
        *guard = Arc::new(spec);
    }

    /// Increment the skipped-reconcile counter. The
    /// `operator_policy_controller` reads this and copies it into
    /// `OperatorPolicy.status.reconcilesSkipped` on every reconcile.
    pub fn bump_skipped(&self) {
        self.skipped.fetch_add(1, Ordering::Relaxed);
    }

    /// Read the current skipped-reconcile count.
    pub fn skipped(&self) -> u64 {
        self.skipped.load(Ordering::Relaxed)
    }

    /// Returns true iff the given object name corresponds to the
    /// honored singleton. Used by both the watcher loop and the
    /// reconciler to filter out non-`default` instances.
    pub fn is_singleton_name(name: &str) -> bool {
        name == OPERATOR_POLICY_SINGLETON
    }
}

impl Default for OperatorPolicyCache {
    fn default() -> Self {
        Self::new_permissive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crd::ControllerSuspend;

    #[test]
    fn permissive_default_has_no_suspends() {
        let cache = OperatorPolicyCache::new_permissive();
        let spec = cache.read();
        assert!(!spec.global_suspend);
        assert!(spec.global_suspend_reason.is_none());
        assert_eq!(spec.controller_suspend, ControllerSuspend::default());
        assert_eq!(cache.skipped(), 0);
    }

    #[test]
    fn store_replaces_snapshot() {
        let cache = OperatorPolicyCache::new_permissive();
        cache.store(OperatorPolicySpec {
            global_suspend: true,
            global_suspend_reason: Some("test".into()),
            controller_suspend: ControllerSuspend::default(),
        });
        let spec = cache.read();
        assert!(spec.global_suspend);
        assert_eq!(spec.global_suspend_reason.as_deref(), Some("test"));
    }

    #[test]
    fn bump_skipped_increments() {
        let cache = OperatorPolicyCache::new_permissive();
        assert_eq!(cache.skipped(), 0);
        cache.bump_skipped();
        cache.bump_skipped();
        cache.bump_skipped();
        assert_eq!(cache.skipped(), 3);
    }

    #[test]
    fn is_singleton_name_only_default() {
        assert!(OperatorPolicyCache::is_singleton_name("default"));
        assert!(!OperatorPolicyCache::is_singleton_name("custom"));
        assert!(!OperatorPolicyCache::is_singleton_name(""));
        assert!(!OperatorPolicyCache::is_singleton_name("Default"));
    }

    #[test]
    fn read_after_concurrent_writes_observes_latest() {
        // Sanity: the lock-protected snapshot resolves writes in some
        // order and reads always see a fully-applied spec (no torn
        // reads).
        let cache = Arc::new(OperatorPolicyCache::new_permissive());
        let writers: Vec<_> = (0..10)
            .map(|i| {
                let cache = cache.clone();
                std::thread::spawn(move || {
                    cache.store(OperatorPolicySpec {
                        global_suspend: i % 2 == 0,
                        global_suspend_reason: Some(format!("writer-{}", i)),
                        controller_suspend: ControllerSuspend::default(),
                    });
                })
            })
            .collect();
        for w in writers {
            w.join().unwrap();
        }
        // After all writers, snapshot is one of the writes — exact
        // identity isn't deterministic, but it MUST be a valid spec
        // (not torn).
        let spec = cache.read();
        let reason = spec.global_suspend_reason.as_deref().unwrap_or("");
        assert!(reason.starts_with("writer-"), "got reason={}", reason);
    }
}
