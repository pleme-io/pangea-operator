//! A log writer that cannot emit an oversized record.
//!
//! ## What this seals
//!
//! `skip_all` on every `#[instrument]` (see
//! `tests/instrument_never_dumps_a_cr.rs`) removes the *known* way a 49 KB log
//! line got produced: a CR Debug-recorded into a span and then stamped onto
//! every event inside it. That fix is total for spans, and it is where the
//! real defect was.
//!
//! It does nothing about the next one. Any hand-written `info!(?big_struct)`,
//! any dependency's own logging, any future field on a status that grows with
//! the fleet, reintroduces the same failure. The source-side fix removes an
//! instance of the class; this removes the class's *consequence*, whatever
//! produces it.
//!
//! The consequence is worth restating, because it is not "noisy logs".
//! `tracing_subscriber`'s stdout writer is a synchronous mutex. A large record
//! is written on the tokio worker thread that emitted it, and backs up against
//! the container runtime's pipe. Starve the workers and nothing runs —
//! including `async fn health() -> &'static str { "OK" }`. The kubelet's 5s
//! liveness timeout then expires, three times, and the container is restarted
//! mid-cycle, discarding ~22 minutes of plan+apply that is immediately redone,
//! producing the same logs. The more work there is, the more certain the kill.
//! On camelot-eks 2026-07-30 that ran 54 times and held a workspace in
//! `Applying` for 28 hours with `Healthy=True` throughout.
//!
//! So the invariant is not about logging at all:
//!
//! > **Observing the system must never consume unbounded resources from the
//! > system being observed.**
//!
//! ## The construction
//!
//! [`BoundedMakeWriter`] hands out a fresh [`BoundedWriter`] per record with a
//! byte budget. Past the budget there is no code path that reaches the inner
//! writer — the bytes are counted and dropped, not queued, not buffered. A
//! record therefore *cannot* exceed its budget plus one fixed-size marker,
//! regardless of what any call site or dependency tries to emit.
//!
//! This is borrowed straight from `hanabi`'s `federation/load_shedding.rs`,
//! which is the same problem one layer up: work arriving faster than it can be
//! served is shed against a bound and *counted*, never allowed to queue until
//! the server dies. The counters here are that module's `RejectionReason`
//! made specific — shedding silently would trade a visible outage for an
//! invisible one.
//!
//! ## Tier, honestly
//!
//! * "A record exceeds the budget" is **truly unrepresentable**: [`Bound`] is
//!   a parse-time-validated newtype, and `write` has no branch that forwards
//!   past `remaining`.
//! * "The operator is killed by its own logging" is **only-mitigated**. The
//!   ceiling is C3, an irreducibly shared resource: this bounds each record,
//!   but a high enough *rate* of in-budget records can still saturate stdout.
//!   Closing that needs a non-blocking writer with a bounded queue
//!   (`tracing_appender::non_blocking`), which is a dependency change and the
//!   named next rung — not something claimed here.
//! * Clipping a JSON record mid-object yields a line a log parser will reject.
//!   That is deliberate and stated: this path only runs when something is
//!   already wrong, the marker says so explicitly, and an unparseable line is
//!   strictly better than a restarted operator.

use std::io;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use tracing_subscriber::fmt::MakeWriter;

/// Appended, once, in place of everything clipped. Fixed size, so the true
/// per-record ceiling is `bound + MARKER.len() + 1` for the newline.
const MARKER: &[u8] = b"\"...CLIPPED (pangea bounded log writer)\"\n";

/// The default per-record budget: generous for any legitimate structured
/// event, far below the ~49 KB that caused the outage. Sized so the marker is
/// noise rather than a meaningful fraction of the record.
pub const DEFAULT_BOUND: usize = 8 * 1024;

/// Records refused past the bound, and the byte volume they would have cost.
///
/// Kept as counters rather than a log line for a reason that matters here: a
/// writer that logged its own overflow would recurse into the exact pressure
/// it exists to relieve.
#[derive(Debug, Default)]
pub struct ClipStats {
    records_clipped: AtomicU64,
    bytes_shed: AtomicU64,
}

impl ClipStats {
    /// Number of records that hit the bound.
    pub fn records_clipped(&self) -> u64 {
        self.records_clipped.load(Ordering::Relaxed)
    }

    /// Bytes that were never written. A nonzero value means some call site is
    /// emitting records it should not be — this is the number to alert on.
    pub fn bytes_shed(&self) -> u64 {
        self.bytes_shed.load(Ordering::Relaxed)
    }
}

/// A validated per-record byte budget.
///
/// Parse-don't-validate: a `Bound` cannot hold a value too small to fit the
/// marker, so the writer never has to handle a budget that cannot even
/// describe its own truncation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Bound(usize);

impl Bound {
    /// The smallest coherent budget — enough that a clipped record can still
    /// say it was clipped.
    pub const MIN: usize = 512;

    /// `None` when `bytes` is below [`Bound::MIN`].
    pub fn new(bytes: usize) -> Option<Self> {
        (bytes >= Self::MIN).then_some(Bound(bytes))
    }

    /// Read from `PANGEA_LOG_MAX_RECORD_BYTES`, falling back to
    /// [`DEFAULT_BOUND`]. An unparseable or too-small value falls back rather
    /// than failing startup: a misconfigured bound must not be able to stop
    /// the operator from booting.
    pub fn from_env() -> Self {
        std::env::var("PANGEA_LOG_MAX_RECORD_BYTES")
            .ok()
            .and_then(|v| v.trim().parse::<usize>().ok())
            .and_then(Bound::new)
            .unwrap_or(Bound(DEFAULT_BOUND))
    }

    pub fn get(&self) -> usize {
        self.0
    }
}

impl Default for Bound {
    fn default() -> Self {
        Bound(DEFAULT_BOUND)
    }
}

/// Wraps any [`MakeWriter`] so every record it produces is bounded.
#[derive(Clone, Debug)]
pub struct BoundedMakeWriter<M> {
    inner: M,
    bound: Bound,
    stats: Arc<ClipStats>,
}

impl<M> BoundedMakeWriter<M> {
    pub fn new(inner: M, bound: Bound) -> Self {
        Self {
            inner,
            bound,
            stats: Arc::new(ClipStats::default()),
        }
    }

    /// Share the counters — wire into metrics, or assert on them in tests.
    pub fn stats(&self) -> Arc<ClipStats> {
        self.stats.clone()
    }
}

impl<'a, M: MakeWriter<'a>> MakeWriter<'a> for BoundedMakeWriter<M> {
    type Writer = BoundedWriter<M::Writer>;

    /// A fresh budget per record. `tracing`'s fmt layer calls this once per
    /// event, which is exactly the granularity the bound should apply at —
    /// no cross-record state, so a clipped record cannot penalise the next.
    fn make_writer(&'a self) -> Self::Writer {
        BoundedWriter {
            inner: self.inner.make_writer(),
            remaining: self.bound.get(),
            stats: self.stats.clone(),
            clipped: false,
        }
    }
}

/// One record's worth of bounded writing.
pub struct BoundedWriter<W: io::Write> {
    inner: W,
    remaining: usize,
    stats: Arc<ClipStats>,
    clipped: bool,
}

impl<W: io::Write> io::Write for BoundedWriter<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if self.clipped {
            // Already over. Report the bytes as accepted so the formatter
            // finishes normally instead of erroring — the record is being
            // discarded on purpose, which is not a write failure.
            self.stats
                .bytes_shed
                .fetch_add(buf.len() as u64, Ordering::Relaxed);
            return Ok(buf.len());
        }

        if buf.len() <= self.remaining {
            let n = self.inner.write(buf)?;
            self.remaining -= n;
            return Ok(n);
        }

        // The clip. Everything past `remaining` has no path to `inner`.
        let head = self.remaining;
        self.inner.write_all(&buf[..head])?;
        self.inner.write_all(MARKER)?;
        self.remaining = 0;
        self.clipped = true;
        self.stats.records_clipped.fetch_add(1, Ordering::Relaxed);
        self.stats
            .bytes_shed
            .fetch_add((buf.len() - head) as u64, Ordering::Relaxed);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

impl<W: io::Write> Drop for BoundedWriter<W> {
    fn drop(&mut self) {
        // MARKER ends in a newline, so a clipped record is already terminated.
        // Nothing to do — but the impl is kept explicit because "does the
        // clipped line still end in \n" is the first question anyone asks, and
        // silently relying on MARKER's last byte is how that invariant gets
        // broken by a later edit to MARKER.
        debug_assert!(MARKER.ends_with(b"\n"));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use std::sync::Mutex;

    /// Collects everything that actually reached the inner writer.
    #[derive(Clone, Default)]
    struct Sink(Arc<Mutex<Vec<u8>>>);

    impl Write for Sink {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    impl<'a> MakeWriter<'a> for Sink {
        type Writer = Sink;
        fn make_writer(&'a self) -> Sink {
            self.clone()
        }
    }

    fn written(sink: &Sink) -> Vec<u8> {
        sink.0.lock().unwrap().clone()
    }

    #[test]
    fn a_record_within_budget_passes_through_byte_for_byte() {
        let sink = Sink::default();
        let mw = BoundedMakeWriter::new(sink.clone(), Bound::new(1024).unwrap());
        let payload = b"{\"message\":\"ordinary reconcile event\"}\n";

        mw.make_writer().write_all(payload).unwrap();

        assert_eq!(written(&sink), payload);
        assert_eq!(mw.stats().records_clipped(), 0);
        assert_eq!(mw.stats().bytes_shed(), 0);
    }

    #[test]
    fn the_outage_payload_cannot_reach_the_inner_writer() {
        // The real shape: a ~49KB record, the size that starved the runtime.
        let sink = Sink::default();
        let bound = Bound::new(1024).unwrap();
        let mw = BoundedMakeWriter::new(sink.clone(), bound);
        let huge = vec![b'x'; 49 * 1024];

        mw.make_writer().write_all(&huge).unwrap();

        let out = written(&sink);
        assert_eq!(
            out.len(),
            bound.get() + MARKER.len(),
            "a record must be physically incapable of exceeding its bound"
        );
        assert!(out.ends_with(MARKER), "a clipped record must say so");
        assert!(out.ends_with(b"\n"), "and must stay line-delimited");
        assert_eq!(mw.stats().records_clipped(), 1);
        assert_eq!(mw.stats().bytes_shed(), (49 * 1024 - 1024) as u64);
    }

    #[test]
    fn clipping_holds_when_the_formatter_writes_in_many_small_pieces() {
        // tracing's fmt layer does not hand over one buffer; it writes field
        // by field. A bound that only worked for single-shot writes would be
        // useless in production and green in a naive test.
        let sink = Sink::default();
        let bound = Bound::new(512).unwrap();
        let mw = BoundedMakeWriter::new(sink.clone(), bound);
        let mut w = mw.make_writer();

        for _ in 0..200 {
            w.write_all(&[b'a'; 64]).unwrap();
        }
        drop(w);

        let out = written(&sink);
        assert_eq!(out.len(), bound.get() + MARKER.len());
        assert_eq!(mw.stats().records_clipped(), 1);
    }

    #[test]
    fn each_record_gets_its_own_budget() {
        // A clipped record must not penalise the next one; the budget is
        // per-record, and `make_writer` is what resets it.
        let sink = Sink::default();
        let mw = BoundedMakeWriter::new(sink.clone(), Bound::new(512).unwrap());

        mw.make_writer().write_all(&vec![b'x'; 5000]).unwrap();
        let after_clip = written(&sink).len();

        let small = b"{\"message\":\"next event is fine\"}\n";
        mw.make_writer().write_all(small).unwrap();

        assert_eq!(
            written(&sink).len(),
            after_clip + small.len(),
            "the second record must pass through in full"
        );
        assert_eq!(mw.stats().records_clipped(), 1, "only the first clipped");
    }

    #[test]
    fn a_budget_too_small_to_describe_its_own_truncation_is_unconstructable() {
        assert!(Bound::new(Bound::MIN - 1).is_none());
        assert!(Bound::new(0).is_none());
        assert_eq!(Bound::new(Bound::MIN).map(|b| b.get()), Some(Bound::MIN));
    }

    #[test]
    fn a_misconfigured_bound_falls_back_instead_of_failing_startup() {
        // Deliberate: an operator that refuses to boot over a bad log knob has
        // turned an observability setting into an availability risk.
        for bad in ["", "not-a-number", "1", "-5"] {
            std::env::set_var("PANGEA_LOG_MAX_RECORD_BYTES", bad);
            assert_eq!(Bound::from_env().get(), DEFAULT_BOUND, "input {bad:?}");
        }
        std::env::set_var("PANGEA_LOG_MAX_RECORD_BYTES", "4096");
        assert_eq!(Bound::from_env().get(), 4096);
        std::env::remove_var("PANGEA_LOG_MAX_RECORD_BYTES");
    }

    #[test]
    fn shed_volume_is_counted_across_records_so_it_can_be_alerted_on() {
        let sink = Sink::default();
        let mw = BoundedMakeWriter::new(sink, Bound::new(512).unwrap());

        for _ in 0..3 {
            mw.make_writer().write_all(&vec![b'x'; 1512]).unwrap();
        }

        assert_eq!(mw.stats().records_clipped(), 3);
        assert_eq!(mw.stats().bytes_shed(), 3 * 1000);
    }
}
