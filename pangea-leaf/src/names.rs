use std::collections::BTreeMap;

use crate::address::LeafKey;

pub const MAX_NAME_LEN: usize = 63;

fn slug(raw: &str) -> String {
    let mut out = String::new();
    let mut dash = true;
    for c in raw.chars() {
        if c.is_ascii_alphanumeric() {
            out.push(c.to_ascii_lowercase());
            dash = false;
        } else if !dash {
            out.push('-');
            dash = true;
        }
    }
    let trimmed = out.trim_matches('-').to_string();
    if trimmed.is_empty() {
        "leaf".to_string()
    } else {
        trimmed
    }
}

fn suffix(key: &LeafKey) -> String {
    let mut h = blake3::Hasher::new();
    h.update(key.tag().as_bytes());
    h.update(&[0]);
    h.update(key.raw().as_bytes());
    h.finalize().to_hex()[..8].to_string()
}

fn clip(s: &str, max: usize) -> String {
    s.chars()
        .take(max)
        .collect::<String>()
        .trim_end_matches('-')
        .to_string()
}

#[must_use]
pub fn leaf_names(base: &str, keys: &[LeafKey]) -> BTreeMap<LeafKey, String> {
    let plain: Vec<(LeafKey, String)> = keys
        .iter()
        .map(|k| (k.clone(), format!("{base}-{}", slug(k.raw()))))
        .collect();
    let mut counts: BTreeMap<&str, usize> = BTreeMap::new();
    for (_, n) in &plain {
        *counts.entry(n.as_str()).or_default() += 1;
    }
    plain
        .iter()
        .map(|(k, n)| {
            let name = if counts[n.as_str()] > 1 || n.len() > MAX_NAME_LEN {
                format!("{}-{}", clip(n, MAX_NAME_LEN - 9), suffix(k))
            } else {
                n.clone()
            };
            (k.clone(), name)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn k(s: &str) -> LeafKey {
        LeafKey::Keyed(s.into())
    }

    #[test]
    fn names_are_dns_labels_prefixed_by_the_fat_name() {
        let n = leaf_names(
            "repos",
            &[k("Akeyless_Cursor"), LeafKey::Module("module.vpc".into())],
        );
        assert_eq!(n[&k("Akeyless_Cursor")], "repos-akeyless-cursor");
        assert_eq!(n[&LeafKey::Module("module.vpc".into())], "repos-module-vpc");
    }

    #[test]
    fn colliding_slugs_get_distinct_hash_suffixes() {
        let n = leaf_names("r", &[k("a_b"), k("a-b")]);
        assert_ne!(n[&k("a_b")], n[&k("a-b")]);
        assert!(n[&k("a_b")].starts_with("r-a-b-"));
    }

    #[test]
    fn a_lone_key_keeps_its_plain_name() {
        let n = leaf_names("r", &[k("a_b")]);
        assert_eq!(n[&k("a_b")], "r-a-b");
    }

    #[test]
    fn long_names_are_clipped_and_stay_unique() {
        let a = k(&"x".repeat(100));
        let b = k(&format!("{}y", "x".repeat(100)));
        let n = leaf_names("r", &[a.clone(), b.clone()]);
        assert!(n[&a].len() <= MAX_NAME_LEN);
        assert!(n[&b].len() <= MAX_NAME_LEN);
        assert_ne!(n[&a], n[&b]);
    }

    #[test]
    fn naming_is_deterministic() {
        let keys = [k("a_b"), k("a-b"), k("c")];
        assert_eq!(leaf_names("r", &keys), leaf_names("r", &keys));
    }

    #[test]
    fn symbols_only_keys_still_name() {
        assert_eq!(leaf_names("r", &[k("///")])[&k("///")], "r-leaf");
    }
}
