//! process_backend.rs — `ProcessIsolatedCompilerBackend`.
//!
//! The durable, by-construction fix for cross-template magnus-VM
//! contamination: every VM-side op runs in a fresh
//! `pangea-operator --compile-worker` child (see [`super::worker`]) that
//! boots its own interpreter, serves the one request, and exits. The
//! parent here is **magnus-free** — it only clones gems to the shared
//! cache (so workers can read them off disk) and shuttles framed
//! [`WireRequest`]/[`WireReply`] over the child's stdin/stdout.
//!
//! Drop-in at the [`CompilerBackend`] trait seam: reconcilers depend only
//! on `Arc<dyn CompilerBackend>`, so selecting this over
//! [`super::EmbeddedCompilerBackend`] is a one-line backend-selection
//! change with zero upstream impact.
//!
//! Concurrency is bounded by a semaphore (= the embedded pool's worker
//! count) so simultaneous reconciles don't fork an unbounded herd of
//! magnus boots.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::Semaphore;

use super::backend::{
    ArchListing, BackendError, CompileAnyRequest, CompileAnyResult, CompileRequest, CompileResult,
    CompilerBackend, FixtureOutcome, GemSource, SmokeRequest, SourceKind,
};
use super::gem_cache::GemCache;
use super::wire::{read_framed, write_framed, WireReply, WireRequest};

/// Process-per-compile compiler backend. See module docs.
#[derive(Clone)]
pub struct ProcessIsolatedCompilerBackend {
    /// Shared on-disk gem cache. `prepare_gem` clones into it (no
    /// broadcast — workers read it off disk at boot).
    cache: Option<GemCache>,
    /// Bounds concurrent worker processes (avoids a magnus-boot
    /// thundering herd on simultaneous reconciles).
    sem: Arc<Semaphore>,
    /// Path to this binary; re-exec'd as `--compile-worker`. One binary,
    /// one libruby linkage, one Nix derivation.
    exe: PathBuf,
}

impl ProcessIsolatedCompilerBackend {
    /// `max_concurrent` = the embedded pool's worker count
    /// (PANGEA_RUBY_WORKERS). The worker binary is `current_exe()`.
    pub fn new(cache: GemCache, max_concurrent: usize) -> Result<Self, BackendError> {
        let exe = std::env::current_exe()
            .map_err(|e| BackendError::Transport(format!("resolve current_exe: {e}")))?;
        Ok(Self {
            cache: Some(cache),
            sem: Arc::new(Semaphore::new(max_concurrent.max(1))),
            exe,
        })
    }

    /// Run one request in a fresh worker process. Acquires a concurrency
    /// permit, then drives the blocking spawn+IPC on a blocking thread.
    async fn run_worker(&self, req: WireRequest) -> Result<WireReply, BackendError> {
        let _permit = self
            .sem
            .acquire()
            .await
            .map_err(|e| BackendError::Transport(format!("semaphore closed: {e}")))?;
        let exe = self.exe.clone();
        tokio::task::spawn_blocking(move || run_worker_sync(&exe, &req))
            .await
            .map_err(|e| BackendError::Transport(format!("compile-worker task join: {e}")))?
    }
}

/// Spawn `pangea-operator --compile-worker`, write the framed request to
/// its stdin, read the framed reply from its stdout, reap it. A worker
/// that dies before replying (crash, OOM, non-zero exit) surfaces as a
/// retryable [`BackendError::Transport`] — never a hang.
fn run_worker_sync(exe: &Path, req: &WireRequest) -> Result<WireReply, BackendError> {
    use std::process::{Command, Stdio};

    let mut child = Command::new(exe)
        .arg("--compile-worker")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        // Worker (and its Ruby) stderr flows to the operator's stderr =
        // the operator log stream. stdout is the reply channel only.
        .stderr(Stdio::inherit())
        .spawn()
        .map_err(|e| BackendError::Transport(format!("spawn compile-worker: {e}")))?;

    // Write the request, then drop stdin (EOF). read_framed reads exactly
    // one length-prefixed frame, so the worker reads it before we read its
    // reply — no deadlock (request is small; reply streams while we read).
    {
        let mut stdin = child
            .stdin
            .take()
            .ok_or_else(|| BackendError::Transport("compile-worker: no stdin handle".into()))?;
        write_framed(&mut stdin, req)
            .map_err(|e| BackendError::Transport(format!("write request to worker: {e}")))?;
    }

    let reply: std::io::Result<WireReply> = {
        let mut stdout = child
            .stdout
            .take()
            .ok_or_else(|| BackendError::Transport("compile-worker: no stdout handle".into()))?;
        read_framed(&mut stdout)
    };

    let status = child
        .wait()
        .map_err(|e| BackendError::Transport(format!("wait compile-worker: {e}")))?;

    match reply {
        Ok(r) => Ok(r),
        // No framed reply: the worker crashed/exited before answering.
        // Retryable transport error — the reconciler backs off + retries
        // on a fresh process.
        Err(e) => Err(BackendError::Transport(format!(
            "compile-worker produced no reply (exit {status}): {e}"
        ))),
    }
}

/// A worker reply whose variant doesn't match the request is a protocol
/// bug; surface it as transport (the parent asked for X, got Y).
fn variant_mismatch(got: &WireReply) -> BackendError {
    BackendError::Transport(format!("compile-worker returned wrong reply variant: {got:?}"))
}

#[async_trait]
impl CompilerBackend for ProcessIsolatedCompilerBackend {
    async fn prepare_gem(&self, source: &GemSource) -> Result<(), BackendError> {
        // Mirror the embedded backend's source-kind dispatch.
        match source.kind {
            SourceKind::Ruby => {}
            SourceKind::Lisp => {
                return Err(BackendError::Ruby(
                    "source.kind=Lisp not yet implemented (terreno M2; theory/TERRENO.md)".into(),
                ));
            }
            SourceKind::Wasm => {
                return Err(BackendError::Ruby(
                    "source.kind=Wasm not yet implemented (M2+; wasmtime integration pending)"
                        .into(),
                ));
            }
        }
        // Clone into the shared on-disk cache. NO broadcast: each fresh
        // worker reads <cache>/*/lib off disk at boot. GemCache::ensure is
        // the mutable-ref-aware clone (git fetch + reset --hard), so
        // architectures hotfixes on `main` reach the next worker.
        if let Some(cache) = &self.cache {
            cache
                .ensure(&source.name, &source.git_url, &source.git_ref)
                .await
                .map_err(|e| BackendError::Ruby(format!("gem cache: {e}")))?;
        }
        Ok(())
    }

    async fn list_architectures(&self, gem: &str) -> Result<ArchListing, BackendError> {
        match self
            .run_worker(WireRequest::ListArchitectures {
                gem: gem.to_string(),
            })
            .await?
        {
            WireReply::ListArchitectures(r) => r.map_err(BackendError::from),
            other => Err(variant_mismatch(&other)),
        }
    }

    async fn smoke_test(&self, req: SmokeRequest) -> Result<FixtureOutcome, BackendError> {
        match self.run_worker(WireRequest::Smoke(req)).await? {
            WireReply::Smoke(r) => r.map_err(BackendError::from),
            other => Err(variant_mismatch(&other)),
        }
    }

    async fn compile(&self, req: CompileRequest) -> Result<CompileResult, BackendError> {
        match self.run_worker(WireRequest::Compile(req)).await? {
            WireReply::Compile(r) => r.map_err(BackendError::from),
            other => Err(variant_mismatch(&other)),
        }
    }

    async fn compile_any(&self, req: CompileAnyRequest) -> Result<CompileAnyResult, BackendError> {
        match self.run_worker(WireRequest::CompileAny(req)).await? {
            WireReply::CompileAny(r) => r.map_err(BackendError::from),
            other => Err(variant_mismatch(&other)),
        }
    }
}
