use std::collections::BTreeMap;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct Weights {
    pub refresh: u64,
    pub create: u64,
    pub update: u64,
    pub delete: u64,
    pub import: u64,
    pub replace: u64,
}

impl Default for Weights {
    fn default() -> Self {
        Self {
            refresh: 1,
            create: 3,
            update: 2,
            delete: 2,
            import: 2,
            replace: 5,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ActionCounts {
    pub refresh: u64,
    pub create: u64,
    pub update: u64,
    pub delete: u64,
    pub import: u64,
    pub replace: u64,
}

#[must_use]
pub fn work(n: &ActionCounts, w: &Weights) -> u64 {
    n.refresh * w.refresh
        + n.create * w.create
        + n.update * w.update
        + n.delete * w.delete
        + n.import * w.import
        + n.replace * w.replace
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingLeaf {
    pub credential: Option<String>,
    pub work: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct CredentialWork {
    pub credential: String,
    pub pending: u64,
    pub cap: u64,
    pub counted: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct LeafWorkStatus {
    pub metric: u64,
    pub desired: u32,
    pub unpaced: u64,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub credentials: Vec<CredentialWork>,
}

#[must_use]
pub fn metric(
    pending: &[PendingLeaf],
    rates: &BTreeMap<String, f64>,
    horizon_secs: u64,
) -> (u64, u64, Vec<CredentialWork>) {
    let mut by_credential: BTreeMap<String, u64> = BTreeMap::new();
    let mut unpaced = 0u64;
    for p in pending {
        match p.credential.as_ref().filter(|c| rates.contains_key(*c)) {
            Some(c) => *by_credential.entry(c.clone()).or_default() += p.work,
            None => unpaced += p.work,
        }
    }
    let mut total = unpaced;
    let mut rows = Vec::with_capacity(by_credential.len());
    for (credential, pending) in by_credential {
        let rate = rates.get(&credential).copied().unwrap_or(0.0).max(0.0);
        let cap = (rate * horizon_secs as f64).floor() as u64;
        let counted = pending.min(cap);
        total += counted;
        rows.push(CredentialWork {
            credential,
            pending,
            cap,
            counted,
        });
    }
    (total, unpaced, rows)
}

#[must_use]
pub fn desired(metric: u64, target: u64, min: u32, max: u32) -> u32 {
    let target = target.max(1);
    let want = metric.div_ceil(target);
    u32::try_from(want)
        .unwrap_or(u32::MAX)
        .clamp(min, max.max(min))
}

#[must_use]
pub fn status(
    pending: &[PendingLeaf],
    rates: &BTreeMap<String, f64>,
    horizon_secs: u64,
    target: u64,
    min: u32,
    max: u32,
) -> LeafWorkStatus {
    let (metric, unpaced, credentials) = metric(pending, rates, horizon_secs);
    LeafWorkStatus {
        metric,
        desired: desired(metric, target, min, max),
        unpaced,
        credentials,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn gh(work: u64) -> PendingLeaf {
        PendingLeaf {
            credential: Some("gh".into()),
            work,
        }
    }

    #[test]
    fn a_fixed_queue_gives_the_hand_computed_value() {
        let w = Weights::default();
        let a = work(
            &ActionCounts {
                refresh: 2,
                create: 1,
                ..ActionCounts::default()
            },
            &w,
        );
        let b = work(
            &ActionCounts {
                refresh: 3,
                update: 1,
                replace: 1,
                ..ActionCounts::default()
            },
            &w,
        );
        assert_eq!(a, 2 + 3);
        assert_eq!(b, 3 + 2 + 5);
        let pending = vec![
            gh(a),
            gh(b),
            PendingLeaf {
                credential: None,
                work: 7,
            },
        ];
        let rates = BTreeMap::from([("gh".to_string(), 3.125)]);
        let s = status(&pending, &rates, 300, 600, 0, 4);
        assert_eq!(s.metric, 5 + 10 + 7);
        assert_eq!(s.unpaced, 7);
        assert_eq!(s.desired, 1);
    }

    #[test]
    fn a_starved_credential_caps_at_its_rate_times_the_horizon() {
        let pending: Vec<PendingLeaf> = (0..1023).map(|_| gh(5)).collect();
        let rates = BTreeMap::from([("gh".to_string(), 3.125)]);
        let s = status(&pending, &rates, 300, 600, 0, 4);
        assert_eq!(s.credentials[0].pending, 5115);
        assert_eq!(s.credentials[0].cap, 937);
        assert_eq!(s.metric, 937);
        assert_eq!(s.desired, 2);
    }

    #[test]
    fn desired_replicas_clamp_between_min_and_max() {
        assert_eq!(desired(0, 600, 0, 4), 0);
        assert_eq!(desired(601, 600, 0, 4), 2);
        assert_eq!(desired(100_000, 600, 0, 4), 4);
        assert_eq!(desired(0, 600, 1, 4), 1);
    }
}
