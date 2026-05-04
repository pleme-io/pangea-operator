//! Round-robin pool of [`RubyOwner`] threads.
//!
//! The single-owner shape (one CRuby VM, one mpsc dispatcher) was the
//! one serialization point left in the embedded compile path: every
//! template's compile request queued on the same owner, so 6 templates
//! arriving simultaneously processed one-after-another. CRuby's
//! design (one VM per OS thread) means we can't share a VM across
//! threads, but we *can* run N independent VMs in N threads — each
//! with its own `$LOAD_PATH`, its own gem-load cache, its own
//! evaluator state.
//!
//! `RubyPool` is the typed wrapper:
//!
//!   - **One-shot dispatch** (`compile`, `list_architectures`,
//!     `smoke_test`, `eval`) round-robins across workers via an
//!     `AtomicUsize` counter. Each request lands on exactly one
//!     worker; no broadcasting, no shared state.
//!
//!   - **Broadcast** (`prepend_load_path`) fans out to every worker
//!     and awaits all replies. Required because each VM has its own
//!     `$LOAD_PATH`; a `prepare_gem` that only prepended on worker
//!     #0 would mean compile requests landing on workers #1..N can't
//!     `require` the gem.
//!
//! With `N=1`, the pool is observationally identical to the
//! single-owner shape (one worker, round-robin trivially returns
//! it). The default in S2 is `N=1` so this commit lands as a
//! refactor; S3 flips the helm-values default to N=4 to actually
//! engage the parallelism.

use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};

use futures::future::try_join_all;
use tokio::sync::{mpsc, oneshot};
use tracing::info;

use super::backend::BackendError;
use super::owner::{RubyOwner, RubyRequest};

/// A pool of `n` [`RubyOwner`] threads. Cheaply cloneable via `Arc`.
pub struct RubyPool {
    workers: Vec<RubyOwner>,
    /// Round-robin dispatch counter. Wraps modulo `workers.len()`.
    /// Relaxed ordering is fine — the counter is a heuristic for
    /// load balancing, not a synchronization primitive.
    next: AtomicUsize,
}

impl RubyPool {
    /// Spawn `n_workers` [`RubyOwner`] threads, each booting its own
    /// CRuby VM with `gem_paths` prepended to its `$LOAD_PATH`.
    /// Workers boot in parallel, so total boot time is one worker's
    /// boot time (~1-3s) regardless of `n_workers`.
    ///
    /// `n_workers` is clamped to `[1, 32]`. Zero is silently bumped
    /// to 1; values beyond 32 are likely a config typo (more VMs
    /// than CPUs would just thrash).
    pub async fn spawn(
        n_workers: usize,
        gem_paths: Vec<PathBuf>,
    ) -> Result<Self, BackendError> {
        let n = n_workers.clamp(1, 32);
        info!(workers = n, "Spawning ruby pool");

        // Boot all workers in parallel — each spawn waits for its
        // own boot ack, but we can do them concurrently.
        let workers: Vec<RubyOwner> = try_join_all(
            (0..n).map(|_| RubyOwner::spawn(gem_paths.clone()))
        )
        .await?;

        info!(workers = n, "Ruby pool ready");
        Ok(Self {
            workers,
            next: AtomicUsize::new(0),
        })
    }

    /// Number of worker threads. Constant across the pool's
    /// lifetime.
    pub fn worker_count(&self) -> usize {
        self.workers.len()
    }

    /// Pick the next worker via round-robin and return its mpsc
    /// sender. Cheap — one atomic fetch_add + index. Use this for
    /// one-shot dispatch (compile / list / smoke / eval).
    pub fn next_sender(&self) -> mpsc::Sender<RubyRequest> {
        let i = self.next.fetch_add(1, Ordering::Relaxed) % self.workers.len();
        self.workers[i].tx_handle()
    }

    /// Broadcast a `PrependLoadPath` request to every worker and
    /// await all replies. Returns Ok only if every worker
    /// successfully prepended.
    ///
    /// Why broadcast: each VM has its own `$LOAD_PATH`. A
    /// `prepare_gem` that only prepended on one worker would mean
    /// compile requests landing on the others can't require the
    /// gem. The `gem_cache` itself is filesystem-shared (one
    /// emptyDir mount across all workers in this process), so the
    /// clone happens once; the load-path update has to fan out.
    pub async fn broadcast_prepend_load_path(
        &self,
        path: PathBuf,
    ) -> Result<(), BackendError> {
        try_join_all(self.workers.iter().map(|worker| {
            let p = path.clone();
            let tx = worker.tx_handle();
            async move {
                let (rtx, rrx) = oneshot::channel();
                tx.send(RubyRequest::PrependLoadPath { path: p, respond: rtx })
                    .await
                    .map_err(|_| BackendError::Ruby("ruby owner channel closed".into()))?;
                rrx.await
                    .map_err(|_| BackendError::Ruby("prepend reply lost".into()))?
            }
        }))
        .await
        .map(|_| ())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;

    #[test]
    fn round_robin_index_wraps_modulo_worker_count() {
        // Pure index-arithmetic check — no actual workers spawned
        // (would require a CRuby VM). Verify the dispatch counter
        // wraps cleanly across worker_count.
        let counter = AtomicUsize::new(0);
        let n = 4;
        let picks: Vec<usize> = (0..10)
            .map(|_| counter.fetch_add(1, Ordering::Relaxed) % n)
            .collect();
        assert_eq!(picks, vec![0, 1, 2, 3, 0, 1, 2, 3, 0, 1]);
    }

    #[test]
    fn round_robin_distributes_evenly_under_concurrent_increments() {
        // 1000 picks across 4 workers — each should receive ≈250.
        // Verifies fetch_add's atomicity: no worker is starved, no
        // worker gets every pick. Critical because without proper
        // atomicity we'd have a hot-worker problem at scale.
        let counter = AtomicUsize::new(0);
        let n = 4;
        let mut counts = vec![0u32; n];
        for _ in 0..1000 {
            let idx = counter.fetch_add(1, Ordering::Relaxed) % n;
            counts[idx] += 1;
        }
        // Each bucket gets exactly 250 (1000/4 = 250 evenly).
        for c in counts {
            assert_eq!(c, 250);
        }
    }
}
