use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

pub const WINDOW: usize = 32;
pub const DEFAULT_TOLERANCE_PERCENT: u32 = 10;

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct CycleWindow {
    #[serde(default)]
    pub samples_ms: Vec<u64>,
}

impl CycleWindow {
    pub fn push(&mut self, ms: u64) {
        self.samples_ms.push(ms);
        if self.samples_ms.len() > WINDOW {
            let drop = self.samples_ms.len() - WINDOW;
            self.samples_ms.drain(..drop);
        }
    }

    #[must_use]
    pub fn p95_ms(&self) -> Option<u64> {
        if self.samples_ms.is_empty() {
            return None;
        }
        let mut s = self.samples_ms.clone();
        s.sort_unstable();
        let rank = (s.len() * 95).div_ceil(100).max(1);
        Some(s[rank - 1])
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Floor {
    pub leaf_keys: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cycle_p95_ms: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum Metric {
    LeafKeys,
    CycleP95Ms,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Regression {
    pub metric: Metric,
    pub floor: u64,
    pub now: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tolerance_percent: Option<u32>,
}

impl Regression {
    #[must_use]
    pub fn describe(&self) -> String {
        match self.metric {
            Metric::LeafKeys => {
                format!("leaf key sets rose from {} to {}", self.floor, self.now)
            }
            Metric::CycleP95Ms => format!(
                "cycle p95 rose from {} ms to {} ms (tolerance {}%)",
                self.floor,
                self.now,
                self.tolerance_percent.unwrap_or(0)
            ),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Verdict {
    pub floor: Floor,
    pub regressions: Vec<Regression>,
}

#[must_use]
pub fn evaluate(
    floor: Option<&Floor>,
    leaf_keys: u32,
    cycle_p95_ms: Option<u64>,
    tolerance_percent: u32,
) -> Verdict {
    let Some(prev) = floor else {
        return Verdict {
            floor: Floor {
                leaf_keys,
                cycle_p95_ms,
            },
            regressions: Vec::new(),
        };
    };
    let mut regressions = Vec::new();
    if leaf_keys > prev.leaf_keys {
        regressions.push(Regression {
            metric: Metric::LeafKeys,
            floor: u64::from(prev.leaf_keys),
            now: u64::from(leaf_keys),
            tolerance_percent: None,
        });
    }
    if let (Some(f), Some(n)) = (prev.cycle_p95_ms, cycle_p95_ms) {
        let allowed = f.saturating_add(f.saturating_mul(u64::from(tolerance_percent)) / 100);
        if n > allowed {
            regressions.push(Regression {
                metric: Metric::CycleP95Ms,
                floor: f,
                now: n,
                tolerance_percent: Some(tolerance_percent),
            });
        }
    }
    let next_p95 = match (prev.cycle_p95_ms, cycle_p95_ms) {
        (Some(f), Some(n)) => Some(f.min(n)),
        (None, n) => n,
        (f, None) => f,
    };
    Verdict {
        floor: Floor {
            leaf_keys: prev.leaf_keys.min(leaf_keys),
            cycle_p95_ms: next_p95,
        },
        regressions,
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct LeafStatus {
    pub leaf_keys: u32,
    pub fat: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cycle_p95_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub floor: Option<Floor>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub regressions: Vec<Regression>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub skip_reason: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inputs_digest: Option<String>,
}

pub const P95_BUCKET_MS: u64 = 1_000;

#[must_use]
pub fn bucket_up(ms: u64) -> u64 {
    ms.div_ceil(P95_BUCKET_MS).saturating_mul(P95_BUCKET_MS)
}

#[must_use]
pub fn next_status(
    prev: Option<&LeafStatus>,
    leaf_keys: u32,
    fat: bool,
    p95_ms: Option<u64>,
    tolerance_percent: u32,
    skip_reason: Option<String>,
    inputs_digest: Option<String>,
) -> LeafStatus {
    let p95 = p95_ms.map(bucket_up);
    let v = evaluate(
        prev.and_then(|p| p.floor.as_ref()),
        leaf_keys,
        p95,
        tolerance_percent,
    );
    LeafStatus {
        leaf_keys,
        fat,
        cycle_p95_ms: p95,
        floor: Some(v.floor),
        regressions: v.regressions,
        skip_reason,
        inputs_digest,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn p95_is_nearest_rank() {
        let mut w = CycleWindow::default();
        assert_eq!(w.p95_ms(), None);
        for ms in 1..=20 {
            w.push(ms);
        }
        assert_eq!(w.p95_ms(), Some(19));
    }

    #[test]
    fn a_single_sample_is_its_own_p95() {
        let mut w = CycleWindow::default();
        w.push(7);
        assert_eq!(w.p95_ms(), Some(7));
    }

    #[test]
    fn the_window_keeps_only_the_newest_samples() {
        let mut w = CycleWindow::default();
        for ms in 0..100 {
            w.push(ms);
        }
        assert_eq!(w.samples_ms.len(), WINDOW);
        assert_eq!(w.samples_ms[0], 100 - WINDOW as u64);
    }

    #[test]
    fn the_first_observation_sets_the_floor() {
        let v = evaluate(None, 5, Some(1000), 10);
        assert_eq!(
            v.floor,
            Floor {
                leaf_keys: 5,
                cycle_p95_ms: Some(1000)
            }
        );
        assert!(v.regressions.is_empty());
    }

    #[test]
    fn the_floor_only_falls() {
        let f = Floor {
            leaf_keys: 5,
            cycle_p95_ms: Some(1000),
        };
        let v = evaluate(Some(&f), 3, Some(400), 10);
        assert_eq!(
            v.floor,
            Floor {
                leaf_keys: 3,
                cycle_p95_ms: Some(400)
            }
        );
        let up = evaluate(Some(&v.floor), 4, Some(500), 10);
        assert_eq!(up.floor, v.floor);
    }

    #[test]
    fn more_leaf_keys_than_the_floor_regresses() {
        let f = Floor {
            leaf_keys: 2,
            cycle_p95_ms: None,
        };
        let v = evaluate(Some(&f), 3, None, 10);
        assert_eq!(
            v.regressions,
            vec![Regression {
                metric: Metric::LeafKeys,
                floor: 2,
                now: 3,
                tolerance_percent: None
            }]
        );
    }

    #[test]
    fn p95_within_tolerance_does_not_regress() {
        let f = Floor {
            leaf_keys: 1,
            cycle_p95_ms: Some(1000),
        };
        assert!(evaluate(Some(&f), 1, Some(1100), 10).regressions.is_empty());
        assert_eq!(evaluate(Some(&f), 1, Some(1101), 10).regressions.len(), 1);
    }

    #[test]
    fn zero_tolerance_is_strict() {
        let f = Floor {
            leaf_keys: 1,
            cycle_p95_ms: Some(1000),
        };
        assert_eq!(evaluate(Some(&f), 1, Some(1001), 0).regressions.len(), 1);
    }

    #[test]
    fn a_missing_p95_never_regresses_and_keeps_the_floor() {
        let f = Floor {
            leaf_keys: 1,
            cycle_p95_ms: Some(1000),
        };
        let v = evaluate(Some(&f), 1, None, 10);
        assert!(v.regressions.is_empty());
        assert_eq!(v.floor.cycle_p95_ms, Some(1000));
    }

    #[test]
    fn next_status_buckets_the_p95_up_to_a_second() {
        let st = next_status(None, 2, true, Some(1_001), 10, None, None);
        assert_eq!(st.cycle_p95_ms, Some(2_000));
        assert_eq!(
            st.floor,
            Some(Floor {
                leaf_keys: 2,
                cycle_p95_ms: Some(2_000)
            })
        );
    }

    #[test]
    fn next_status_carries_regressions_and_a_falling_floor() {
        let first = next_status(None, 3, true, Some(5_000), 10, None, None);
        let split = next_status(Some(&first), 1, false, Some(900), 10, None, None);
        assert_eq!(
            split.floor,
            Some(Floor {
                leaf_keys: 1,
                cycle_p95_ms: Some(1_000)
            })
        );
        assert!(split.regressions.is_empty());
        let back = next_status(Some(&split), 3, true, Some(5_000), 10, None, None);
        assert_eq!(back.regressions.len(), 2);
        assert_eq!(back.floor, split.floor);
    }

    #[test]
    fn the_same_observation_yields_an_identical_status() {
        let a = next_status(None, 2, true, Some(1_500), 10, None, None);
        let b = next_status(Some(&a), 2, true, Some(1_500), 10, None, None);
        assert_eq!(a, b);
    }
}
