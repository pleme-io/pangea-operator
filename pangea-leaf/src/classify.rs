use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

use crate::address::{leaf_key, AddressError, LeafKey};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "reason", rename_all = "camelCase")]
pub enum FatReason {
    #[serde(rename_all = "camelCase")]
    MultipleLeafKeys { leaf_keys: usize },
    #[serde(rename_all = "camelCase")]
    CycleOverBudget { p95_ms: u64, budget_ms: u64 },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Classification {
    pub leaf_keys: usize,
    pub cycle_p95_ms: Option<u64>,
    pub budget_ms: Option<u64>,
    pub fat: bool,
    pub reasons: Vec<FatReason>,
}

pub fn distinct_leaf_keys<'a>(
    addresses: impl IntoIterator<Item = &'a str>,
) -> Result<BTreeSet<LeafKey>, AddressError> {
    addresses
        .into_iter()
        .filter(|a| !crate::address::is_data(a))
        .map(leaf_key)
        .collect()
}

pub fn classify<'a>(
    addresses: impl IntoIterator<Item = &'a str>,
    cycle_p95_ms: Option<u64>,
    budget_ms: Option<u64>,
) -> Result<Classification, AddressError> {
    let leaf_keys = distinct_leaf_keys(addresses)?.len();
    Ok(classify_counts(leaf_keys, cycle_p95_ms, budget_ms))
}

#[must_use]
pub fn classify_counts(
    leaf_keys: usize,
    cycle_p95_ms: Option<u64>,
    budget_ms: Option<u64>,
) -> Classification {
    let mut reasons = Vec::new();
    if leaf_keys > 1 {
        reasons.push(FatReason::MultipleLeafKeys { leaf_keys });
    }
    if let (Some(p95), Some(budget)) = (cycle_p95_ms, budget_ms) {
        if p95 > budget {
            reasons.push(FatReason::CycleOverBudget {
                p95_ms: p95,
                budget_ms: budget,
            });
        }
    }
    Classification {
        leaf_keys,
        cycle_p95_ms,
        budget_ms,
        fat: !reasons.is_empty(),
        reasons,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn one_keyed_set_is_a_leaf() {
        let c = classify(["a.b[\"x\"]", "c.d[\"x\"]"], None, None).unwrap();
        assert_eq!(c.leaf_keys, 1);
        assert!(!c.fat);
    }

    #[test]
    fn two_key_sets_are_fat() {
        let c = classify(["a.b[\"x\"]", "a.b[\"y\"]"], None, None).unwrap();
        assert_eq!(c.leaf_keys, 2);
        assert!(c.fat);
        assert_eq!(
            c.reasons,
            vec![FatReason::MultipleLeafKeys { leaf_keys: 2 }]
        );
    }

    #[test]
    fn a_slow_cycle_makes_a_single_leaf_fat() {
        let c = classify(["a.b"], Some(900_000), Some(600_000)).unwrap();
        assert!(c.fat);
        assert_eq!(
            c.reasons,
            vec![FatReason::CycleOverBudget {
                p95_ms: 900_000,
                budget_ms: 600_000
            }]
        );
    }

    #[test]
    fn a_cycle_at_budget_is_not_fat() {
        assert!(!classify(["a.b"], Some(600_000), Some(600_000)).unwrap().fat);
    }

    #[test]
    fn data_sources_never_count() {
        let c = classify(["a.b", "data.x.y", "module.m.data.x.y"], None, None).unwrap();
        assert_eq!(c.leaf_keys, 1);
        assert!(!c.fat);
    }

    #[test]
    fn a_malformed_address_stops_classification() {
        assert!(classify(["a..b"], None, None).is_err());
    }

    #[test]
    fn empty_state_is_not_fat() {
        assert!(
            !classify(std::iter::empty::<&str>(), None, None)
                .unwrap()
                .fat
        );
    }
}
