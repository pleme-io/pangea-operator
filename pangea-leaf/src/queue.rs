use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

pub const SUBJECT_ROOT: &str = "pangea.leaf";
pub const STREAM: &str = "PANGEA_LEAF";
pub const COST_HEADER: &str = "Pangea-Cost";

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LeafAction {
    Plan,
    Apply,
    Drift,
    Import,
}

impl LeafAction {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Plan => "plan",
            Self::Apply => "apply",
            Self::Drift => "drift",
            Self::Import => "import",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LeafWork {
    pub leaf: String,
    pub credential: String,
    pub action: LeafAction,
    pub digest: String,
    pub cost: u64,
}

fn token(raw: &str) -> String {
    raw.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect()
}

impl LeafWork {
    #[must_use]
    pub fn msg_id(&self) -> String {
        [
            self.leaf.as_str(),
            self.action.as_str(),
            self.digest.as_str(),
        ]
        .join(":")
    }

    #[must_use]
    pub fn subject(&self, root: &str) -> String {
        [root, &token(&self.credential), self.action.as_str()].join(".")
    }
}

#[must_use]
pub fn credential_filter(root: &str, credential: &str) -> String {
    [root, &token(credential), ">"].join(".")
}

#[must_use]
pub fn consumer_name(credential: &str) -> String {
    ["leaf", &token(credential)].join("-")
}

#[must_use]
pub fn shares(rate: f64, held: &BTreeMap<String, u32>) -> BTreeMap<String, f64> {
    let total: u64 = held.values().map(|n| u64::from(*n)).sum();
    if total == 0 || rate <= 0.0 {
        return held.keys().map(|w| (w.clone(), 0.0)).collect();
    }
    held.iter()
        .map(|(w, n)| (w.clone(), rate * f64::from(*n) / total as f64))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn work() -> LeafWork {
        LeafWork {
            leaf: "pleme-io-github-repos-kensa".into(),
            credential: "3f2a9c".into(),
            action: LeafAction::Plan,
            digest: "d41d8c".into(),
            cost: 5,
        }
    }

    #[test]
    fn the_message_id_is_leaf_action_and_input_digest() {
        assert_eq!(work().msg_id(), "pleme-io-github-repos-kensa:plan:d41d8c");
        let mut moved = work();
        moved.digest = "e5".into();
        assert_ne!(work().msg_id(), moved.msg_id());
    }

    #[test]
    fn the_subject_is_per_credential_and_action_and_tokens_are_safe() {
        assert_eq!(work().subject(SUBJECT_ROOT), "pangea.leaf.3f2a9c.plan");
        let mut odd = work();
        odd.credential = "a.b c>".into();
        assert_eq!(odd.subject(SUBJECT_ROOT), "pangea.leaf.a_b_c_.plan");
        assert_eq!(
            credential_filter(SUBJECT_ROOT, "3f2a9c"),
            "pangea.leaf.3f2a9c.>"
        );
        assert_eq!(consumer_name("3f2a9c"), "leaf-3f2a9c");
    }

    #[test]
    fn the_summed_shares_stay_at_the_credentials_rate() {
        let held = BTreeMap::from([
            ("w0".to_string(), 3u32),
            ("w1".to_string(), 1),
            ("w2".to_string(), 4),
        ]);
        let s = shares(3.125, &held);
        let sum: f64 = s.values().sum();
        assert!((sum - 3.125).abs() < 1e-9);
        assert!((s["w2"] - 3.125 * 4.0 / 8.0).abs() < 1e-9);
        let idle = shares(3.125, &BTreeMap::from([("w0".to_string(), 0u32)]));
        assert_eq!(idle["w0"], 0.0);
    }
}
