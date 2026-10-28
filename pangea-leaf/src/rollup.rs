use std::collections::{BTreeMap, BTreeSet};

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

pub const NAME_CAP: usize = 50;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LeafState {
    Ready,
    Planning,
    Applying,
    Failed {
        reason: String,
        since: Option<String>,
    },
    Stalled {
        state: String,
        since: Option<String>,
    },
    RateLimited {
        until: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeafView {
    pub name: String,
    pub state: LeafState,
    pub upstreams: Vec<String>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub enum Verdict {
    #[default]
    Converged,
    Progressing,
    Partial,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Counts {
    pub ready: u32,
    pub planning: u32,
    pub applying: u32,
    pub failed: u32,
    pub stalled: u32,
    pub rate_limited: u32,
    pub blocked: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct FailedLeaf {
    pub leaf: String,
    pub reason: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub since: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct StalledLeaf {
    pub leaf: String,
    pub state: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub since: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct RateLimitedLeaf {
    pub leaf: String,
    pub until: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct BlockedLeaf {
    pub leaf: String,
    pub by: Vec<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct LeavesRollup {
    pub verdict: Verdict,
    pub total: u32,
    pub counts: Counts,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub failed: Vec<FailedLeaf>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub stalled: Vec<StalledLeaf>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub rate_limited: Vec<RateLimitedLeaf>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub blocked: Vec<BlockedLeaf>,
    #[serde(default)]
    pub truncated: u32,
}

fn holding(state: &LeafState) -> bool {
    matches!(
        state,
        LeafState::Failed { .. } | LeafState::Stalled { .. } | LeafState::RateLimited { .. }
    )
}

fn holders_of(
    name: &str,
    by_name: &BTreeMap<&str, &LeafView>,
    memo: &mut BTreeMap<String, BTreeSet<String>>,
    path: &mut BTreeSet<String>,
) -> BTreeSet<String> {
    if let Some(h) = memo.get(name) {
        return h.clone();
    }
    let mut out = BTreeSet::new();
    if !path.insert(name.to_string()) {
        return out;
    }
    if let Some(leaf) = by_name.get(name) {
        for up in &leaf.upstreams {
            if let Some(u) = by_name.get(up.as_str()) {
                if holding(&u.state) {
                    out.insert(up.clone());
                }
                out.extend(holders_of(up, by_name, memo, path));
            }
        }
    }
    path.remove(name);
    memo.insert(name.to_string(), out.clone());
    out
}

#[must_use]
pub fn judge(leaves: &[LeafView], cap: usize) -> LeavesRollup {
    let by_name: BTreeMap<&str, &LeafView> = leaves.iter().map(|l| (l.name.as_str(), l)).collect();
    let mut memo = BTreeMap::new();
    let mut r = LeavesRollup {
        total: u32::try_from(leaves.len()).unwrap_or(u32::MAX),
        ..LeavesRollup::default()
    };
    let mut truncated = 0u32;
    let mut sorted: Vec<&LeafView> = leaves.iter().collect();
    sorted.sort_by(|a, b| a.name.cmp(&b.name));
    for leaf in sorted {
        let held_by = if holding(&leaf.state) {
            BTreeSet::new()
        } else {
            holders_of(&leaf.name, &by_name, &mut memo, &mut BTreeSet::new())
        };
        if !held_by.is_empty() {
            r.counts.blocked += 1;
            if r.blocked.len() < cap {
                r.blocked.push(BlockedLeaf {
                    leaf: leaf.name.clone(),
                    by: held_by.into_iter().collect(),
                });
            } else {
                truncated += 1;
            }
            continue;
        }
        match &leaf.state {
            LeafState::Ready => r.counts.ready += 1,
            LeafState::Planning => r.counts.planning += 1,
            LeafState::Applying => r.counts.applying += 1,
            LeafState::Failed { reason, since } => {
                r.counts.failed += 1;
                if r.failed.len() < cap {
                    r.failed.push(FailedLeaf {
                        leaf: leaf.name.clone(),
                        reason: reason.clone(),
                        since: since.clone(),
                    });
                } else {
                    truncated += 1;
                }
            }
            LeafState::Stalled { state, since } => {
                r.counts.stalled += 1;
                if r.stalled.len() < cap {
                    r.stalled.push(StalledLeaf {
                        leaf: leaf.name.clone(),
                        state: state.clone(),
                        since: since.clone(),
                    });
                } else {
                    truncated += 1;
                }
            }
            LeafState::RateLimited { until } => {
                r.counts.rate_limited += 1;
                if r.rate_limited.len() < cap {
                    r.rate_limited.push(RateLimitedLeaf {
                        leaf: leaf.name.clone(),
                        until: until.clone(),
                    });
                } else {
                    truncated += 1;
                }
            }
        }
    }
    r.truncated = truncated;
    let c = &r.counts;
    r.verdict = if c.failed + c.stalled + c.blocked > 0 {
        Verdict::Partial
    } else if c.ready == r.total {
        Verdict::Converged
    } else {
        Verdict::Progressing
    };
    r
}

#[cfg(test)]
mod tests {
    use super::*;

    fn leaf(name: &str, state: LeafState, ups: &[&str]) -> LeafView {
        LeafView {
            name: name.into(),
            state,
            upstreams: ups.iter().map(|s| (*s).to_string()).collect(),
        }
    }

    fn failed() -> LeafState {
        LeafState::Failed {
            reason: "ImportAbsent".into(),
            since: Some("2026-10-27T18:00:00Z".into()),
        }
    }

    #[test]
    fn one_failed_leaf_makes_the_catalog_partial_names_it_and_blocks_only_its_dependents() {
        let r = judge(
            &[
                leaf("org-estudio", failed(), &[]),
                leaf("org-kensa", LeafState::Ready, &[]),
                leaf("org-tameshi", LeafState::Ready, &[]),
                leaf("org-dep", LeafState::Planning, &["org-estudio"]),
                leaf("org-dep2", LeafState::Ready, &["org-dep"]),
            ],
            NAME_CAP,
        );
        assert_eq!(r.verdict, Verdict::Partial);
        assert_eq!(r.total, 5);
        assert_eq!(r.failed.len(), 1);
        assert_eq!(r.failed[0].leaf, "org-estudio");
        assert_eq!(r.failed[0].reason, "ImportAbsent");
        assert_eq!(r.counts.ready, 2);
        assert_eq!(r.counts.blocked, 2);
        assert_eq!(
            r.blocked,
            vec![
                BlockedLeaf {
                    leaf: "org-dep".into(),
                    by: vec!["org-estudio".into()]
                },
                BlockedLeaf {
                    leaf: "org-dep2".into(),
                    by: vec!["org-estudio".into()]
                },
            ]
        );
    }

    #[test]
    fn all_ready_is_converged_and_work_in_flight_is_progressing() {
        let ready = judge(
            &[
                leaf("a", LeafState::Ready, &[]),
                leaf("b", LeafState::Ready, &[]),
            ],
            NAME_CAP,
        );
        assert_eq!(ready.verdict, Verdict::Converged);
        let busy = judge(
            &[
                leaf("a", LeafState::Ready, &[]),
                leaf("b", LeafState::Applying, &[]),
            ],
            NAME_CAP,
        );
        assert_eq!(busy.verdict, Verdict::Progressing);
        assert_eq!(judge(&[], NAME_CAP).verdict, Verdict::Converged);
    }

    #[test]
    fn a_stalled_leaf_is_partial_and_named_with_its_state() {
        let r = judge(
            &[leaf(
                "a",
                LeafState::Stalled {
                    state: "heartbeatLost".into(),
                    since: None,
                },
                &[],
            )],
            NAME_CAP,
        );
        assert_eq!(r.verdict, Verdict::Partial);
        assert_eq!(r.stalled[0].state, "heartbeatLost");
    }

    #[test]
    fn names_past_the_cap_are_counted_not_listed() {
        let leaves: Vec<LeafView> = (0..5)
            .map(|i| leaf(&format!("l{i}"), failed(), &[]))
            .collect();
        let r = judge(&leaves, 3);
        assert_eq!(r.counts.failed, 5);
        assert_eq!(r.failed.len(), 3);
        assert_eq!(r.truncated, 2);
    }

    #[test]
    fn a_cycle_in_upstreams_terminates() {
        let r = judge(
            &[
                leaf("a", LeafState::Ready, &["b"]),
                leaf("b", LeafState::Ready, &["a"]),
            ],
            NAME_CAP,
        );
        assert_eq!(r.verdict, Verdict::Converged);
    }
}
