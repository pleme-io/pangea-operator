//! wire.rs — the typed IPC protocol for **process-isolated compiles**.
//!
//! ## Why this exists
//!
//! magnus permits exactly ONE CRuby VM per OS process (`Ruby already
//! initialized` on a second boot). The operator therefore historically
//! ran every template compile through one long-lived VM — and
//! process-global mutable state (`$LOADED_FEATURES`, `$LOAD_PATH`,
//! `Pangea::*` constants AND their autoload tables) accumulated across
//! compiles, producing order-dependent cross-template contamination
//! that no in-VM purge could fully reach (a stale absent-gem autoload on
//! `Pangea::Resources` is neither a `$LOADED_FEATURES` row nor a purged
//! constant — root-caused 2026-06-05).
//!
//! The durable fix is to put the process boundary exactly where the
//! one-VM-per-process constraint already is: each compile runs in a
//! fresh `pangea-operator --compile-worker` child that boots its own
//! VM, serves the request against a virgin interpreter, and exits.
//! Cross-compile state accumulation becomes *unrepresentable*. Both ends
//! are the SAME binary (one image, one libruby linkage, one Nix
//! derivation); they speak this protocol over a length-prefixed framed
//! pipe.
//!
//! ## The wire
//!
//! `u32` little-endian length prefix + a `serde_json` body. Every
//! [`CompileRequest`]/[`CompileResult`]/[`SmokeRequest`]/… already
//! derives `Serialize`/`Deserialize`; the only shim is [`WireError`],
//! because [`BackendError`] is `thiserror` (not `Serialize`). The
//! parent reconstitutes a `BackendError` from the `WireError` so callers
//! upstream of the [`crate::ruby::CompilerBackend`] trait see exactly
//! one error type whether the backend is in-thread or process-isolated.

use serde::{Deserialize, Serialize};
use std::io::{self, Read, Write};

use super::backend::{
    ArchListing, BackendError, CompileAnyRequest, CompileAnyResult, CompileRequest, CompileResult,
    FixtureOutcome, SmokeRequest,
};

/// Serde-able mirror of [`BackendError`]. Crosses the process boundary
/// and is reconstituted into a `BackendError` on the parent side, so the
/// process boundary is invisible to `CompilerBackend` callers.
///
/// Kept structurally identical to `BackendError` (same four variants,
/// same `String` payloads) so the `From` impls are total + lossless —
/// a new `BackendError` variant is a compile error here until mirrored,
/// which is the intended forcing function.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum WireError {
    Transport(String),
    Compiler(String),
    Ruby(String),
    NotInitialized,
}

impl From<&BackendError> for WireError {
    fn from(e: &BackendError) -> Self {
        match e {
            BackendError::Transport(s) => WireError::Transport(s.clone()),
            BackendError::Compiler(s) => WireError::Compiler(s.clone()),
            BackendError::Ruby(s) => WireError::Ruby(s.clone()),
            BackendError::NotInitialized => WireError::NotInitialized,
        }
    }
}

impl From<BackendError> for WireError {
    fn from(e: BackendError) -> Self {
        (&e).into()
    }
}

impl From<WireError> for BackendError {
    fn from(e: WireError) -> Self {
        match e {
            WireError::Transport(s) => BackendError::Transport(s),
            WireError::Compiler(s) => BackendError::Compiler(s),
            WireError::Ruby(s) => BackendError::Ruby(s),
            WireError::NotInitialized => BackendError::NotInitialized,
        }
    }
}

/// One request the parent sends a worker. Process-per-compile serves a
/// single request then exits, but the enum covers all four VM-side
/// [`crate::ruby::CompilerBackend`] ops so a future recycled-worker pool
/// can serve several over one boot. `prepare_gem` is intentionally
/// absent: it is a shared-filesystem `git` clone (no VM state), so it
/// stays on the parent and the worker simply reads the gem cache + the
/// request's `rubylib_paths` off disk at boot.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WireRequest {
    Compile(CompileRequest),
    Smoke(SmokeRequest),
    ListArchitectures { gem: String },
    CompileAny(CompileAnyRequest),
}

/// A worker's reply — each variant is the matching op's `Result` with
/// the error mapped to [`WireError`]. The variant tag also lets the
/// parent assert it got the reply kind it asked for (a mismatch is a
/// protocol bug, surfaced as `Transport`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WireReply {
    Compile(Result<CompileResult, WireError>),
    Smoke(Result<FixtureOutcome, WireError>),
    ListArchitectures(Result<ArchListing, WireError>),
    CompileAny(Result<CompileAnyResult, WireError>),
}

/// Maximum frame size (64 MiB). A synthesized Terraform JSON for the
/// largest workspace (~1100 resources) is well under this; the cap
/// turns a corrupt/garbage length prefix into a clean error instead of
/// an OOM allocation.
pub const MAX_FRAME_BYTES: usize = 64 * 1024 * 1024;

/// Write one length-prefixed `serde_json` frame. Flushes so the peer's
/// `read_framed` unblocks immediately (pipes are unbuffered-sensitive).
pub fn write_framed<W: Write, T: Serialize>(w: &mut W, msg: &T) -> io::Result<()> {
    let body =
        serde_json::to_vec(msg).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    let len = u32::try_from(body.len())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "frame exceeds u32"))?;
    w.write_all(&len.to_le_bytes())?;
    w.write_all(&body)?;
    w.flush()
}

/// Read one length-prefixed `serde_json` frame. Bounded by
/// [`MAX_FRAME_BYTES`]. EOF before the length prefix surfaces as
/// `UnexpectedEof` (the parent maps a worker that died before replying
/// to a retryable transport error).
pub fn read_framed<R: Read, T: for<'de> Deserialize<'de>>(r: &mut R) -> io::Result<T> {
    let mut len_buf = [0u8; 4];
    r.read_exact(&mut len_buf)?;
    let len = u32::from_le_bytes(len_buf) as usize;
    if len > MAX_FRAME_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("frame {len} exceeds MAX_FRAME_BYTES {MAX_FRAME_BYTES}"),
        ));
    }
    let mut body = vec![0u8; len];
    r.read_exact(&mut body)?;
    serde_json::from_slice(&body).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wire_error_round_trips_every_backend_variant_losslessly() {
        // Forcing function: this list must cover every BackendError
        // variant. If a variant is added without mirroring it in
        // WireError, the From impl above stops compiling — caught here.
        let cases = [
            BackendError::Transport("conn refused".into()),
            BackendError::Compiler("syntax error".into()),
            BackendError::Ruby("NameError: undefined".into()),
            BackendError::NotInitialized,
        ];
        for be in cases {
            let wire: WireError = (&be).into();
            let json = serde_json::to_vec(&wire).expect("serialize WireError");
            let back: WireError = serde_json::from_slice(&json).expect("deserialize WireError");
            assert_eq!(wire, back, "WireError serde round-trip");
            // And the Display contract survives the BackendError round-trip.
            let be2: BackendError = back.into();
            assert_eq!(be.to_string(), be2.to_string(), "BackendError Display preserved");
        }
    }

    #[test]
    fn framing_round_trips_a_request_through_a_pipe_buffer() {
        let req = WireRequest::Compile(CompileRequest {
            source: Some("template :t do; end".into()),
            template_name: Some("t".into()),
            rubylib_paths: vec!["/var/pangea/gems/pangea-architectures-main/lib".into()],
            ..Default::default()
        });
        let mut buf: Vec<u8> = Vec::new();
        write_framed(&mut buf, &req).expect("write");
        // Prefix is u32-LE length of the json body.
        let body_len = u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
        assert_eq!(body_len, buf.len() - 4, "length prefix matches body");
        let mut cursor = std::io::Cursor::new(buf);
        let back: WireRequest = read_framed(&mut cursor).expect("read");
        match back {
            WireRequest::Compile(c) => {
                assert_eq!(c.template_name.as_deref(), Some("t"));
                assert_eq!(c.rubylib_paths.len(), 1);
            }
            other => panic!("wrong variant: {other:?}"),
        }
    }

    #[test]
    fn framing_round_trips_a_reply_with_typed_synthesis_value() {
        let reply = WireReply::Compile(Ok(CompileResult {
            terraform_json: "{\"resource\":{}}".into(),
            synthesis_value: Some(serde_json::json!({"resource": {}})),
        }));
        let mut buf: Vec<u8> = Vec::new();
        write_framed(&mut buf, &reply).expect("write");
        let mut cursor = std::io::Cursor::new(buf);
        let back: WireReply = read_framed(&mut cursor).expect("read");
        match back {
            WireReply::Compile(Ok(r)) => {
                assert!(r.synthesis_value.is_some(), "typed value crosses the wire");
                assert!(r.terraform_json.contains("resource"));
            }
            other => panic!("wrong variant: {other:?}"),
        }
    }

    #[test]
    fn framing_carries_an_error_reply() {
        let reply = WireReply::Compile(Err(WireError::Ruby(
            "cannot load such file -- pangea-grafana".into(),
        )));
        let mut buf: Vec<u8> = Vec::new();
        write_framed(&mut buf, &reply).expect("write");
        let mut cursor = std::io::Cursor::new(buf);
        let back: WireReply = read_framed(&mut cursor).expect("read");
        match back {
            WireReply::Compile(Err(e)) => {
                let be: BackendError = e.into();
                assert!(matches!(be, BackendError::Ruby(_)));
            }
            other => panic!("wrong variant: {other:?}"),
        }
    }

    #[test]
    fn read_framed_rejects_an_oversized_length_prefix() {
        // A garbage/corrupt prefix must error cleanly, never OOM-allocate.
        let mut buf = Vec::new();
        buf.extend_from_slice(&u32::MAX.to_le_bytes());
        let mut cursor = std::io::Cursor::new(buf);
        let r: io::Result<WireRequest> = read_framed(&mut cursor);
        assert!(r.is_err(), "oversized frame must be rejected");
    }

    #[test]
    fn read_framed_surfaces_eof_before_prefix() {
        // Worker died before replying → UnexpectedEof → parent maps to a
        // retryable transport error.
        let mut cursor = std::io::Cursor::new(Vec::<u8>::new());
        let r: io::Result<WireReply> = read_framed(&mut cursor);
        assert_eq!(r.unwrap_err().kind(), io::ErrorKind::UnexpectedEof);
    }
}
