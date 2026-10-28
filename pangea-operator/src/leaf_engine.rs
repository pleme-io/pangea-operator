use pangea_leaf::admit::{Candidate, Caps, InFlight};
use pangea_leaf::drift::DriftLeaf;

pub trait LeafEngine: Send + Sync {
    fn name(&self) -> &'static str;

    fn admit(&self, candidates: &[Candidate], caps: &Caps, in_flight: &InFlight) -> Vec<String>;

    fn drift_due(&self, leaves: &[DriftLeaf], now: u64, interval_secs: u64) -> Vec<String>;

    fn drift_wait(
        &self,
        leaf: &DriftLeaf,
        siblings: &[String],
        now: u64,
        interval_secs: u64,
    ) -> u64;
}

pub struct RustEngine;

impl LeafEngine for RustEngine {
    fn name(&self) -> &'static str {
        "rust"
    }

    fn admit(&self, candidates: &[Candidate], caps: &Caps, in_flight: &InFlight) -> Vec<String> {
        pangea_leaf::admit::admit(candidates, caps, in_flight)
    }

    fn drift_due(&self, leaves: &[DriftLeaf], now: u64, interval_secs: u64) -> Vec<String> {
        pangea_leaf::drift::drift_due(leaves, now, interval_secs)
    }

    fn drift_wait(
        &self,
        leaf: &DriftLeaf,
        siblings: &[String],
        now: u64,
        interval_secs: u64,
    ) -> u64 {
        let placed = pangea_leaf::drift::offsets(
            siblings
                .iter()
                .map(String::as_str)
                .chain(std::iter::once(leaf.name.as_str())),
            interval_secs,
        );
        placed.get(&leaf.name).map_or(0, |offset| {
            pangea_leaf::drift::next_due_in(*offset, now, interval_secs, leaf.last_check)
        })
    }
}

static RUST: RustEngine = RustEngine;

#[must_use]
pub fn engine() -> &'static dyn LeafEngine {
    &RUST
}
