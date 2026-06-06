//! worker.rs — the `pangea-operator --compile-worker` child entrypoint.
//!
//! Process-per-compile isolation: a fresh OS process boots exactly ONE
//! magnus CRuby VM, serves ONE [`WireRequest`] against a *virgin*
//! interpreter, writes the [`WireReply`], and exits. Because each compile
//! gets its own process, the process-global mutable state that caused
//! order-dependent cross-template contamination ($LOADED_FEATURES,
//! $LOAD_PATH, `Pangea::*` constants AND autoload tables) cannot survive
//! from one compile into the next — contamination is *unrepresentable by
//! construction*, not mitigated by an ever-growing purge ruleset.
//!
//! IPC: a framed [`WireRequest`] arrives on stdin; the framed
//! [`WireReply`] leaves on the *saved real* stdout. Before booting Ruby
//! we point fd 1 at fd 2 (stderr) so any `puts` / C-level warning during
//! boot or compile lands in the operator log stream instead of corrupting
//! the length-prefixed reply frame.
//!
//! The dispatch targets are the same pure functions the in-thread
//! [`crate::ruby::RubyOwner`] loop calls ([`super::owner::compile_template`]
//! et al.) — one compile implementation, two execution strategies.

use std::io;
use std::os::fd::FromRawFd;

use pangea_ruby_eval::{boot_ruby_unchecked, RubyEvaluator};

use super::backend::BackendError;
use super::owner::{compile_template, list_architectures, smoke_test};
use super::wire::{read_framed, write_framed, WireError, WireReply, WireRequest};

/// Entry point for `pangea-operator --compile-worker`. Returns a process
/// exit code; the caller (`main`) should `std::process::exit` with it.
/// MUST run on a thread that does ALL magnus work synchronously (the
/// caller invokes this before the tokio runtime starts, on the main
/// thread).
pub fn run_compile_worker() -> u8 {
    // Save the real stdout (the reply channel), then redirect fd 1 -> fd 2
    // so Ruby/C stdout noise can't corrupt the framed reply.
    let reply_fd = unsafe { libc::dup(libc::STDOUT_FILENO) };
    if reply_fd < 0 {
        eprintln!("compile-worker: dup(stdout) failed");
        return 2;
    }
    if unsafe { libc::dup2(libc::STDERR_FILENO, libc::STDOUT_FILENO) } < 0 {
        eprintln!("compile-worker: dup2(stderr -> stdout) failed");
        return 2;
    }
    // SAFETY: reply_fd is a fresh, owned dup of the original stdout.
    let mut reply_out = unsafe { std::fs::File::from_raw_fd(reply_fd) };

    // Read the single request BEFORE paying the magnus boot — a malformed
    // frame fails fast and cheap.
    let req: WireRequest = match read_framed(&mut io::stdin().lock()) {
        Ok(r) => r,
        Err(e) => {
            eprintln!("compile-worker: read request frame: {e}");
            return 2;
        }
    };

    // Boot a fresh VM. SAFETY: this process boots Ruby exactly once, here.
    let _cleanup = unsafe { boot_ruby_unchecked() };
    let evaluator = match RubyEvaluator::new() {
        Ok(e) => e,
        Err(e) => {
            let reply =
                error_reply(&req, BackendError::Ruby(format!("RubyEvaluator::new: {e}")));
            let _ = write_framed(&mut reply_out, &reply);
            return 1;
        }
    };

    // Prepend the prepared gem-cache libs to $LOAD_PATH. The parent's
    // `prepare_gem` cloned them to the shared gem cache on disk; here we
    // read them off disk — the per-process equivalent of the long-lived
    // pool's `broadcast_prepend_load_path`. Per-request workspace
    // `rubylib_paths` are added later by `compile_template`'s
    // `with_load_paths`; the baked image gems come from `RUBYLIB`.
    if let Err(e) = prepend_gem_cache_libs(&evaluator) {
        // Non-fatal: RUBYLIB + the request's rubylib_paths may still
        // resolve everything. Surface to logs, keep going.
        eprintln!("compile-worker: gem-cache load-path setup: {e}");
    }

    let reply = dispatch(&evaluator, &req);
    if let Err(e) = write_framed(&mut reply_out, &reply) {
        eprintln!("compile-worker: write reply frame: {e}");
        return 1;
    }
    0
}

/// Dispatch the request against the virgin interpreter. Mirrors the
/// `RubyOwner` loop's match, reusing the same pure VM-op functions.
fn dispatch(ev: &RubyEvaluator, req: &WireRequest) -> WireReply {
    match req {
        WireRequest::Compile(r) => {
            WireReply::Compile(compile_template(ev, r).map_err(WireError::from))
        }
        WireRequest::Smoke(r) => WireReply::Smoke(smoke_test(ev, r).map_err(WireError::from)),
        WireRequest::ListArchitectures { gem } => {
            WireReply::ListArchitectures(list_architectures(ev, gem).map_err(WireError::from))
        }
        // Parity with the embedded backend, whose compile_any is a stub
        // (M8.4). Process-isolation doesn't change that; compile_any
        // callers use PANGEA_COMPILER_BACKEND=http until it lands.
        WireRequest::CompileAny(_) => WireReply::CompileAny(Err(WireError::Ruby(
            "embedded /compile-any not yet implemented (M8.4); set PANGEA_COMPILER_BACKEND=http"
                .into(),
        ))),
    }
}

/// Build a reply of the right variant carrying `e`, so a pre-dispatch
/// failure (e.g. VM boot) still gives the parent a typed answer it can
/// match to its request.
fn error_reply(req: &WireRequest, e: BackendError) -> WireReply {
    let we = WireError::from(&e);
    match req {
        WireRequest::Compile(_) => WireReply::Compile(Err(we)),
        WireRequest::Smoke(_) => WireReply::Smoke(Err(we)),
        WireRequest::ListArchitectures { .. } => WireReply::ListArchitectures(Err(we)),
        WireRequest::CompileAny(_) => WireReply::CompileAny(Err(we)),
    }
}

/// Prepend `<PANGEA_GEM_CACHE_DIR>/*/lib` to `$LOAD_PATH` (idempotent via
/// the Ruby-side `include?` guard).
fn prepend_gem_cache_libs(ev: &RubyEvaluator) -> Result<(), BackendError> {
    let base =
        std::env::var("PANGEA_GEM_CACHE_DIR").unwrap_or_else(|_| "/var/pangea/gems".to_string());
    let dir = std::path::Path::new(&base);
    if !dir.is_dir() {
        return Ok(());
    }
    for entry in std::fs::read_dir(dir)
        .map_err(|e| BackendError::Ruby(format!("read gem cache {base}: {e}")))?
    {
        let entry = entry.map_err(|e| BackendError::Ruby(format!("gem cache entry: {e}")))?;
        let lib = entry.path().join("lib");
        if lib.is_dir() {
            let escaped = lib
                .to_string_lossy()
                .replace('\\', "\\\\")
                .replace('"', "\\\"");
            ev.eval_string(&format!(
                r#"$LOAD_PATH.unshift("{escaped}") unless $LOAD_PATH.include?("{escaped}")"#
            ))
            .map_err(|e| BackendError::Ruby(format!("$LOAD_PATH.unshift {}: {e}", lib.display())))?;
        }
    }
    Ok(())
}
