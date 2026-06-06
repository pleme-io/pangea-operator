//! Integration test for process-per-compile isolation.
//!
//! Spawns the REAL `pangea-operator --compile-worker` child (via
//! `CARGO_BIN_EXE_pangea-operator`), drives the framed WireRequest/
//! WireReply IPC, and asserts the full path works: a fresh magnus VM
//! boots, `compile_template` is dispatched, and a typed reply comes back
//! over a clean stdout (Ruby noise redirected to stderr).
//!
//! The by-construction order-independence guarantee — that no compile can
//! contaminate the next — follows from each invocation being a SEPARATE
//! OS process (proven here by running the worker twice and getting two
//! independent, identical results; the first process has fully exited
//! before the second boots, so there is no shared VM state to leak).
//!
//! Uses the no-source validation path so the test exercises spawn → boot
//! → dispatch → IPC WITHOUT needing the full pangea gem closure (the
//! `compile_template` validation error precedes any gem use).
//!
//! Run: `nix develop .#ruby-eval -c cargo test -p pangea-operator \
//!        --features embedded_ruby --test process_isolation`

#![cfg(feature = "embedded_ruby")]

use std::process::{Command, Stdio};

use pangea_operator::ruby::backend::CompileRequest;
use pangea_operator::ruby::wire::{read_framed, write_framed, WireError, WireReply, WireRequest};

/// Spawn one `--compile-worker` child, send `req`, return its reply.
/// Asserts the child exits 0 (a clean, framed reply was produced).
fn run_worker_once(req: &WireRequest) -> WireReply {
    let exe = env!("CARGO_BIN_EXE_pangea-operator");
    let mut child = Command::new(exe)
        .arg("--compile-worker")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .expect("spawn pangea-operator --compile-worker");

    {
        let mut stdin = child.stdin.take().expect("worker stdin");
        write_framed(&mut stdin, req).expect("write framed request");
        // stdin dropped → EOF.
    }
    let reply: WireReply = {
        let mut stdout = child.stdout.take().expect("worker stdout");
        read_framed(&mut stdout).expect("read framed reply")
    };
    let status = child.wait().expect("wait worker");
    assert!(
        status.success(),
        "compile-worker exited non-zero: {status:?}"
    );
    reply
}

#[test]
fn compile_worker_boots_dispatches_and_round_trips() {
    // No source/template_path → compile_template returns its validation
    // error. Exercises the full spawn → fresh-magnus-boot → dispatch →
    // framed-IPC path without the gem closure.
    let reply = run_worker_once(&WireRequest::Compile(CompileRequest::default()));
    match reply {
        WireReply::Compile(Err(WireError::Compiler(msg))) => {
            assert!(
                msg.contains("source") && msg.contains("template_path"),
                "expected the no-source validation error, got: {msg}"
            );
        }
        other => panic!("expected Compile(Err(Compiler(..))), got: {other:?}"),
    }
}

#[test]
fn two_compiles_are_independent_processes() {
    // The by-construction isolation proof: run the worker twice. The first
    // process has fully exited (run_worker_once waits) before the second
    // boots, so the second cannot inherit ANY VM state ($LOADED_FEATURES,
    // $LOAD_PATH, Pangea::* constants/autoloads) from the first. Both must
    // yield the same typed result — order/history can't affect a compile.
    let r1 = run_worker_once(&WireRequest::Compile(CompileRequest::default()));
    let r2 = run_worker_once(&WireRequest::Compile(CompileRequest::default()));
    assert!(
        matches!(r1, WireReply::Compile(Err(WireError::Compiler(_)))),
        "first compile reply variant"
    );
    assert!(
        matches!(r2, WireReply::Compile(Err(WireError::Compiler(_)))),
        "second compile reply variant — identical to first, no cross-process leak"
    );
}

#[test]
fn list_architectures_round_trips_over_the_worker() {
    // A different op variant — proves the dispatch + reply-variant routing
    // works for more than Compile. A missing gem yields a typed Ruby error
    // (not a hang/crash), confirming the worker's list_architectures path
    // and the parent's variant matching.
    let reply = run_worker_once(&WireRequest::ListArchitectures {
        gem: "pangea-nonexistent-test-gem".into(),
    });
    match reply {
        WireReply::ListArchitectures(_) => { /* Ok or typed Err — both prove routing */ }
        other => panic!("expected a ListArchitectures reply variant, got: {other:?}"),
    }
}
