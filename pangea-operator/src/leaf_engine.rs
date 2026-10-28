use std::collections::BTreeMap;
use std::sync::Mutex;

use pangea_leaf::admit::{Candidate, Caps, InFlight};
use pangea_leaf::drift::DriftLeaf;
use pangea_leaf::memory::{MemoryInputs, WorkerMemory, WorkerMemoryStatus};
use pangea_leaf::rollup::{Held, LeafView, LeavesRollup, Transition};
use pangea_leaf::series::{Selection, SeriesMode, SeriesRow};
use pangea_leaf::work::{LeafWorkStatus, PendingLeaf};
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum EngineKind {
    #[default]
    Rust,
    Blue,
}

impl EngineKind {
    #[must_use]
    pub fn parse(raw: &str) -> Option<Self> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "rust" => Some(Self::Rust),
            "blue" => Some(Self::Blue),
            _ => None,
        }
    }
}

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

    fn judge(&self, leaves: &[LeafView], cap: usize) -> LeavesRollup;

    fn transitions(&self, prev: &BTreeMap<String, Held>, leaves: &[LeafView]) -> Vec<Transition>;

    fn series(&self, rows: &[SeriesRow], mode: SeriesMode, max: usize) -> Selection;

    fn work(
        &self,
        pending: &[PendingLeaf],
        rates: &BTreeMap<String, f64>,
        horizon_secs: u64,
        target: u64,
        bounds: (u32, u32),
    ) -> LeafWorkStatus;

    fn memory(&self, setting: WorkerMemory, inputs: &MemoryInputs) -> WorkerMemoryStatus;
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

    fn judge(&self, leaves: &[LeafView], cap: usize) -> LeavesRollup {
        pangea_leaf::rollup::judge(leaves, cap)
    }

    fn transitions(&self, prev: &BTreeMap<String, Held>, leaves: &[LeafView]) -> Vec<Transition> {
        pangea_leaf::rollup::transitions(prev, leaves)
    }

    fn series(&self, rows: &[SeriesRow], mode: SeriesMode, max: usize) -> Selection {
        pangea_leaf::series::select(rows, mode, max)
    }

    fn work(
        &self,
        pending: &[PendingLeaf],
        rates: &BTreeMap<String, f64>,
        horizon_secs: u64,
        target: u64,
        bounds: (u32, u32),
    ) -> LeafWorkStatus {
        pangea_leaf::work::status(pending, rates, horizon_secs, target, bounds.0, bounds.1)
    }

    fn memory(&self, setting: WorkerMemory, inputs: &MemoryInputs) -> WorkerMemoryStatus {
        pangea_leaf::memory::derive(setting, inputs)
    }
}

static RUST: RustEngine = RustEngine;

static SELECTED: Mutex<EngineKind> = Mutex::new(EngineKind::Rust);

#[must_use]
pub fn choose(kind: EngineKind) -> (EngineKind, Option<String>) {
    match kind {
        EngineKind::Rust => (EngineKind::Rust, None),
        EngineKind::Blue => (
            EngineKind::Rust,
            Some("leaf.engine blue needs the happa bidama embedded in this build; the entry is refused and the rust engine judges".into()),
        ),
    }
}

pub fn configure(kind: EngineKind) -> Option<String> {
    let (chosen, refusal) = choose(kind);
    *SELECTED.lock().expect("leaf engine mutex poisoned") = chosen;
    refusal
}

#[must_use]
pub fn engine() -> &'static dyn LeafEngine {
    match *SELECTED.lock().expect("leaf engine mutex poisoned") {
        EngineKind::Rust | EngineKind::Blue => &RUST,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn blue_is_refused_alone_until_happa_is_embedded_and_rust_judges() {
        assert_eq!(choose(EngineKind::Rust), (EngineKind::Rust, None));
        let (chosen, refusal) = choose(EngineKind::Blue);
        assert_eq!(chosen, EngineKind::Rust);
        assert!(refusal.unwrap().contains("happa"));
        assert_eq!(engine().name(), "rust");
        assert_eq!(EngineKind::parse("BLUE"), Some(EngineKind::Blue));
        assert_eq!(EngineKind::parse("lisp"), None);
    }

    #[test]
    fn every_decision_reaches_the_engine_through_the_trait() {
        let e: &dyn LeafEngine = &RustEngine;
        let views = vec![LeafView {
            name: "a".into(),
            state: pangea_leaf::rollup::LeafState::Ready,
            upstreams: vec![],
        }];
        assert_eq!(e.judge(&views, 50).counts.ready, 1);
        assert!(e.transitions(&BTreeMap::new(), &views).is_empty());
        assert_eq!(
            e.series(&[], SeriesMode::Unhealthy, 256),
            Selection::default()
        );
        assert_eq!(e.work(&[], &BTreeMap::new(), 300, 600, (0, 4)).metric, 0);
    }
}
