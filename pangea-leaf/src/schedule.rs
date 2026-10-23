use std::collections::BTreeSet;

use serde_json::Value;

use crate::dag::TemplateDag;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Upstream {
    pub template: String,
    pub has_mock: bool,
}

#[must_use]
pub fn blocked_by(
    template: &str,
    upstreams: &[Upstream],
    ready: &BTreeSet<String>,
    applying: bool,
) -> Vec<String> {
    let names: Vec<String> = upstreams.iter().map(|u| u.template.clone()).collect();
    let mut dag = TemplateDag::new();
    dag.add(template, &names);
    let mut effective = ready.clone();
    if !applying {
        effective.extend(
            upstreams
                .iter()
                .filter(|u| u.has_mock)
                .map(|u| u.template.clone()),
        );
    }
    if dag.deps_satisfied(template, &effective) {
        return Vec::new();
    }
    let mut blocked: Vec<String> = names
        .into_iter()
        .filter(|n| !effective.contains(n))
        .collect();
    blocked.sort();
    blocked.dedup();
    blocked
}

#[must_use]
pub fn scope_cap(max_concurrent_leaves: Option<u32>, default_per_scope: usize) -> usize {
    match max_concurrent_leaves {
        Some(n) => usize::try_from(n.max(1)).unwrap_or(usize::MAX),
        None => default_per_scope,
    }
}

fn canonical(v: &Value, out: &mut String) {
    match v {
        Value::Object(m) => {
            let mut keys: Vec<&String> = m.keys().collect();
            keys.sort();
            out.push('{');
            for k in keys {
                out.push_str(&Value::String(k.clone()).to_string());
                out.push(':');
                canonical(&m[k], out);
                out.push(',');
            }
            out.push('}');
        }
        Value::Array(a) => {
            out.push('[');
            for x in a {
                canonical(x, out);
                out.push(',');
            }
            out.push(']');
        }
        other => out.push_str(&other.to_string()),
    }
}

#[must_use]
pub fn inputs_digest(
    catalog_variables: Option<&Value>,
    template_variables: Option<&Value>,
    refs: Option<&Value>,
) -> String {
    let mut s = String::new();
    for part in [catalog_variables, template_variables, refs] {
        match part {
            Some(v) => canonical(v, &mut s),
            None => s.push('~'),
        }
        s.push('|');
    }
    blake3::hash(s.as_bytes()).to_hex()[..32].to_string()
}

#[must_use]
pub fn stale_leaves<'a>(
    leaves: impl IntoIterator<Item = (&'a str, Option<&'a str>, &'a str)>,
) -> Vec<String> {
    let mut out: Vec<String> = leaves
        .into_iter()
        .filter(|(_, recorded, current)| recorded.is_some_and(|r| r != *current))
        .map(|(name, _, _)| name.to_string())
        .collect();
    out.sort();
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn up(n: &str, mock: bool) -> Upstream {
        Upstream {
            template: n.into(),
            has_mock: mock,
        }
    }

    fn ready(v: &[&str]) -> BTreeSet<String> {
        v.iter().map(|s| (*s).to_string()).collect()
    }

    #[test]
    fn a_root_is_never_blocked() {
        assert!(blocked_by("a", &[], &ready(&[]), true).is_empty());
    }

    #[test]
    fn an_unready_upstream_blocks_and_is_named() {
        let b = blocked_by(
            "a",
            &[up("u1", false), up("u2", false)],
            &ready(&["u1"]),
            false,
        );
        assert_eq!(b, vec!["u2".to_string()]);
    }

    #[test]
    fn all_upstreams_ready_unblocks() {
        assert!(blocked_by("a", &[up("u1", false)], &ready(&["u1"]), true).is_empty());
    }

    #[test]
    fn a_mock_lets_a_plan_proceed_but_never_an_apply() {
        let u = [up("u1", true)];
        assert!(blocked_by("a", &u, &ready(&[]), false).is_empty());
        assert_eq!(
            blocked_by("a", &u, &ready(&[]), true),
            vec!["u1".to_string()]
        );
    }

    #[test]
    fn the_cap_is_the_catalog_value_floored_at_one() {
        assert_eq!(scope_cap(Some(6), 2), 6);
        assert_eq!(scope_cap(Some(0), 2), 1);
        assert_eq!(scope_cap(None, 2), 2);
    }

    #[test]
    fn the_digest_ignores_key_order_and_sees_values() {
        let a = json!({"x": 1, "y": {"b": 2, "a": 3}});
        let b = json!({"y": {"a": 3, "b": 2}, "x": 1});
        let c = json!({"y": {"a": 3, "b": 2}, "x": 2});
        assert_eq!(
            inputs_digest(Some(&a), None, None),
            inputs_digest(Some(&b), None, None)
        );
        assert_ne!(
            inputs_digest(Some(&a), None, None),
            inputs_digest(Some(&c), None, None)
        );
    }

    #[test]
    fn the_digest_keeps_the_three_inputs_apart() {
        let v = json!({"k": 1});
        assert_ne!(
            inputs_digest(Some(&v), None, None),
            inputs_digest(None, Some(&v), None)
        );
        assert_ne!(
            inputs_digest(None, None, None),
            inputs_digest(Some(&json!(null)), None, None)
        );
    }

    #[test]
    fn a_leaf_with_no_recorded_digest_is_not_stale_and_a_differing_one_is() {
        let stale = stale_leaves([
            ("new", None, "d1"),
            ("same", Some("d1"), "d1"),
            ("moved", Some("d0"), "d1"),
        ]);
        assert_eq!(stale, vec!["moved".to_string()]);
    }
}
