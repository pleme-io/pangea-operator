use pangea_leaf::admit::{Candidate, Caps, InFlight};

pub trait LeafEngine: Send + Sync {
    fn name(&self) -> &'static str;

    fn admit(&self, candidates: &[Candidate], caps: &Caps, in_flight: &InFlight) -> Vec<String>;
}

pub struct RustEngine;

impl LeafEngine for RustEngine {
    fn name(&self) -> &'static str {
        "rust"
    }

    fn admit(&self, candidates: &[Candidate], caps: &Caps, in_flight: &InFlight) -> Vec<String> {
        pangea_leaf::admit::admit(candidates, caps, in_flight)
    }
}

static RUST: RustEngine = RustEngine;

#[must_use]
pub fn engine() -> &'static dyn LeafEngine {
    &RUST
}
