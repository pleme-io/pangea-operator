use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use crate::address::{is_data, leaf_key, AddressError, LeafKey};

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum PartitionError {
    #[error(transparent)]
    Address(#[from] AddressError),
    #[error("address `{0}` appears more than once in the state")]
    Duplicate(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Leaf {
    pub key: LeafKey,
    pub merged_from: Vec<LeafKey>,
    pub addresses: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Bijection {
    pub count_before: usize,
    pub count_after: usize,
    pub hash_before: String,
    pub hash_after: String,
    pub disjoint: bool,
    pub ok: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Partition {
    leaves: BTreeMap<LeafKey, Leaf>,
}

#[must_use]
pub fn multiset_hash(addresses: &[String]) -> String {
    let mut sorted: Vec<&str> = addresses.iter().map(String::as_str).collect();
    sorted.sort_unstable();
    let mut h = blake3::Hasher::new();
    h.update(&(sorted.len() as u64).to_le_bytes());
    for a in sorted {
        h.update(&(a.len() as u64).to_le_bytes());
        h.update(a.as_bytes());
    }
    h.finalize().to_hex().to_string()
}

impl Partition {
    pub fn build(addresses: &[String]) -> Result<Self, PartitionError> {
        let mut seen = BTreeSet::new();
        let mut leaves: BTreeMap<LeafKey, Leaf> = BTreeMap::new();
        for a in addresses.iter().filter(|a| !is_data(a)) {
            if !seen.insert(a.as_str()) {
                return Err(PartitionError::Duplicate(a.clone()));
            }
            let key = leaf_key(a)?;
            leaves
                .entry(key.clone())
                .or_insert_with(|| Leaf {
                    key,
                    merged_from: Vec::new(),
                    addresses: Vec::new(),
                })
                .addresses
                .push(a.clone());
        }
        for leaf in leaves.values_mut() {
            leaf.addresses.sort_unstable();
        }
        Ok(Self { leaves })
    }

    pub fn merge(&mut self, edges: &[(LeafKey, LeafKey)]) {
        let mut parent: BTreeMap<LeafKey, LeafKey> =
            self.leaves.keys().map(|k| (k.clone(), k.clone())).collect();
        fn find(parent: &mut BTreeMap<LeafKey, LeafKey>, k: &LeafKey) -> LeafKey {
            let mut root = k.clone();
            while parent[&root] != root {
                root = parent[&root].clone();
            }
            let mut cur = k.clone();
            while parent[&cur] != root {
                let next = parent[&cur].clone();
                parent.insert(cur, root.clone());
                cur = next;
            }
            root
        }
        for (a, b) in edges {
            if !parent.contains_key(a) || !parent.contains_key(b) {
                continue;
            }
            let ra = find(&mut parent, a);
            let rb = find(&mut parent, b);
            if ra != rb {
                let (lo, hi) = if ra < rb { (ra, rb) } else { (rb, ra) };
                parent.insert(hi, lo);
            }
        }
        let keys: Vec<LeafKey> = self.leaves.keys().cloned().collect();
        let mut merged: BTreeMap<LeafKey, Leaf> = BTreeMap::new();
        for k in keys {
            let root = find(&mut parent, &k);
            let leaf = self.leaves.remove(&k).expect("key listed from the map");
            let slot = merged.entry(root.clone()).or_insert_with(|| Leaf {
                key: root.clone(),
                merged_from: Vec::new(),
                addresses: Vec::new(),
            });
            if k != root {
                slot.merged_from.push(k);
            }
            slot.addresses.extend(leaf.addresses);
        }
        for leaf in merged.values_mut() {
            leaf.addresses.sort_unstable();
            leaf.merged_from.sort();
        }
        self.leaves = merged;
    }

    #[must_use]
    pub fn leaves(&self) -> impl Iterator<Item = &Leaf> {
        self.leaves.values()
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.leaves.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.leaves.is_empty()
    }

    #[must_use]
    pub fn leaf_of(&self, address: &str) -> Option<&LeafKey> {
        self.leaves
            .values()
            .find(|l| {
                l.addresses
                    .binary_search_by(|a| a.as_str().cmp(address))
                    .is_ok()
            })
            .map(|l| &l.key)
    }

    #[must_use]
    pub fn bijection(&self, before: &[String]) -> Bijection {
        let before: Vec<String> = before.iter().filter(|a| !is_data(a)).cloned().collect();
        let after: Vec<String> = self
            .leaves
            .values()
            .flat_map(|l| l.addresses.iter().cloned())
            .collect();
        let unique: BTreeSet<&String> = after.iter().collect();
        let disjoint = unique.len() == after.len();
        let hash_before = multiset_hash(&before);
        let hash_after = multiset_hash(&after);
        Bijection {
            count_before: before.len(),
            count_after: after.len(),
            ok: disjoint && hash_before == hash_after,
            hash_before,
            hash_after,
            disjoint,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn addrs(v: &[&str]) -> Vec<String> {
        v.iter().map(|s| (*s).to_string()).collect()
    }

    #[test]
    fn partition_groups_by_key_and_preserves_the_multiset() {
        let before = addrs(&[
            "r.a[\"x\"]",
            "q.a[\"x\"]",
            "r.a[\"y\"]",
            "module.m.t.u",
            "plain.one",
        ]);
        let p = Partition::build(&before).unwrap();
        assert_eq!(p.len(), 4);
        let b = p.bijection(&before);
        assert!(b.ok);
        assert_eq!(b.count_before, 5);
        assert_eq!(b.hash_before, b.hash_after);
    }

    #[test]
    fn every_address_lands_in_exactly_one_leaf() {
        let before = addrs(&["r.a[\"x\"]", "q.a[\"x\"]", "r.a[\"y\"]"]);
        let p = Partition::build(&before).unwrap();
        for a in &before {
            assert!(p.leaf_of(a).is_some());
        }
        assert_eq!(p.leaves().map(|l| l.addresses.len()).sum::<usize>(), 3);
    }

    #[test]
    fn the_hash_is_order_independent_and_content_sensitive() {
        let a = multiset_hash(&addrs(&["a", "b", "c"]));
        assert_eq!(a, multiset_hash(&addrs(&["c", "a", "b"])));
        assert_ne!(a, multiset_hash(&addrs(&["a", "b", "d"])));
        assert_ne!(a, multiset_hash(&addrs(&["a", "b"])));
    }

    #[test]
    fn the_hash_cannot_be_forged_by_concatenation() {
        assert_ne!(
            multiset_hash(&addrs(&["ab", "c"])),
            multiset_hash(&addrs(&["a", "bc"]))
        );
    }

    #[test]
    fn a_dropped_address_fails_the_bijection() {
        let before = addrs(&["r.a[\"x\"]", "r.a[\"y\"]"]);
        let mut p = Partition::build(&before).unwrap();
        p.leaves
            .get_mut(&LeafKey::Keyed("y".into()))
            .unwrap()
            .addresses
            .clear();
        assert!(!p.bijection(&before).ok);
    }

    #[test]
    fn an_address_in_two_leaves_fails_the_bijection() {
        let before = addrs(&["r.a[\"x\"]", "r.a[\"y\"]"]);
        let mut p = Partition::build(&before).unwrap();
        p.leaves
            .get_mut(&LeafKey::Keyed("y".into()))
            .unwrap()
            .addresses
            .push("r.a[\"x\"]".into());
        let b = p.bijection(&before);
        assert!(!b.disjoint);
        assert!(!b.ok);
    }

    #[test]
    fn a_duplicate_state_address_is_refused() {
        assert_eq!(
            Partition::build(&addrs(&["a.b", "a.b"])),
            Err(PartitionError::Duplicate("a.b".into()))
        );
    }

    #[test]
    fn data_addresses_are_outside_both_sides() {
        let before = addrs(&["a.b", "data.x.y"]);
        let p = Partition::build(&before).unwrap();
        assert_eq!(p.len(), 1);
        assert!(p.bijection(&before).ok);
    }

    #[test]
    fn merge_joins_connected_leaves_and_keeps_the_bijection() {
        let before = addrs(&["a.one", "b.two", "c.three", "d.four"]);
        let mut p = Partition::build(&before).unwrap();
        p.merge(&[
            (
                LeafKey::Address("a.one".into()),
                LeafKey::Address("b.two".into()),
            ),
            (
                LeafKey::Address("b.two".into()),
                LeafKey::Address("c.three".into()),
            ),
        ]);
        assert_eq!(p.len(), 2);
        let big = p.leaves().find(|l| l.addresses.len() == 3).unwrap();
        assert_eq!(big.key, LeafKey::Address("a.one".into()));
        assert_eq!(big.merged_from.len(), 2);
        assert!(p.bijection(&before).ok);
    }

    #[test]
    fn merge_ignores_edges_to_unknown_leaves() {
        let before = addrs(&["a.one"]);
        let mut p = Partition::build(&before).unwrap();
        p.merge(&[(
            LeafKey::Address("a.one".into()),
            LeafKey::Address("zz".into()),
        )]);
        assert_eq!(p.len(), 1);
    }
}
