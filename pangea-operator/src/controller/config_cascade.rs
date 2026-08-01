//! Config inheritance cascade — Terragrunt `include` / `root.hcl` parity (P1).
//!
//! Terragrunt's signature ergonomic is that a leaf unit `include`s a root config
//! and **deep-merges** it: the leaf inherits fleet-wide `inputs`/provider/backend
//! defaults and overrides only the keys it needs. pangea's existing cascade
//! carried *policy* fields only; this module extends the same innermost-wins
//! discipline to arbitrary **config** (`variables`, and any future inherited
//! map), so the authoring chain
//!
//! ```text
//! PangeaNamespace.defaultVariables   (outermost — fleet/namespace defaults)
//!   → WorkspaceCatalog.variables     (workspace defaults, root.hcl analogue)
//!     → InfrastructureTemplate.spec.variables   (innermost — the leaf, wins)
//! ```
//!
//! deep-merges into one effective variable map. A template inherits everything
//! the outer scopes declare and overrides per key — the "easy inherit" semantics
//! the operator asked for. Pure (`serde_json` + `std`), fully unit-testable.

use std::collections::BTreeMap;

use serde_json::Value;

/// The authored config map shape — exactly `InfrastructureTemplate.spec.variables`.
pub type VariableMap = BTreeMap<String, Value>;

/// Deep-merge `over` onto `base`, **innermost (`over`) wins**. Nested JSON
/// objects merge **recursively** — so a leaf can override one key of an inherited
/// object without restating the whole object (Terragrunt's include deep-merge
/// default). Scalars, arrays, and type-mismatches are **replaced** by `over`
/// (lists are not concatenated — innermost-wins, matching the default merge).
#[must_use]
pub fn deep_merge_value(base: &Value, over: &Value) -> Value {
    match (base, over) {
        (Value::Object(b), Value::Object(o)) => {
            let mut merged = b.clone();
            for (k, ov) in o {
                let next = match merged.get(k) {
                    Some(bv) => deep_merge_value(bv, ov),
                    None => ov.clone(),
                };
                merged.insert(k.clone(), next);
            }
            Value::Object(merged)
        }
        // scalar / array / mismatched type ⇒ the inner layer replaces.
        _ => over.clone(),
    }
}

/// Resolve the effective variable map by deep-merging the cascade `layers` in
/// order from **outermost (lowest precedence) to innermost (highest)**. Each
/// layer's keys merge onto the accumulator; the last layer wins per key. Pass
/// `[namespace_defaults, workspace_defaults, template_variables]`.
#[must_use]
pub fn resolve_variables(layers: &[&VariableMap]) -> VariableMap {
    let mut acc: VariableMap = BTreeMap::new();
    for layer in layers {
        for (k, v) in *layer {
            let next = match acc.get(k) {
                Some(existing) => deep_merge_value(existing, v),
                None => v.clone(),
            };
            acc.insert(k.clone(), next);
        }
    }
    acc
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn m(pairs: &[(&str, Value)]) -> VariableMap {
        pairs
            .iter()
            .map(|(k, v)| ((*k).to_string(), v.clone()))
            .collect()
    }

    #[test]
    fn template_inherits_outer_defaults_and_overrides_per_key() {
        let ns = m(&[("region", json!("us-east-2")), ("retics", json!(3))]);
        let ws = m(&[("region", json!("us-west-1")), ("tenant", json!("acme"))]);
        let tpl = m(&[("retics", json!(5))]);
        let eff = resolve_variables(&[&ns, &ws, &tpl]);
        // region: workspace overrides namespace; retics: template overrides namespace;
        // tenant: inherited from workspace; nothing dropped.
        assert_eq!(
            eff["region"],
            json!("us-west-1"),
            "workspace overrides namespace"
        );
        assert_eq!(eff["retics"], json!(5), "template (innermost) wins");
        assert_eq!(eff["tenant"], json!("acme"), "inherited from workspace");
        assert_eq!(eff.len(), 3);
    }

    #[test]
    fn nested_objects_deep_merge_keeping_siblings() {
        // The Terragrunt include behavior: override ONE key of an inherited object.
        let ws = m(&[("tags", json!({"env": "prod", "owner": "platform"}))]);
        let tpl = m(&[("tags", json!({"owner": "team-x"}))]);
        let eff = resolve_variables(&[&ws, &tpl]);
        assert_eq!(
            eff["tags"],
            json!({"env": "prod", "owner": "team-x"}),
            "owner overridden, env inherited (deep merge, not replace)"
        );
    }

    #[test]
    fn arrays_and_scalars_are_replaced_innermost_wins() {
        let ws = m(&[("subnets", json!([1, 2, 3])), ("count", json!(1))]);
        let tpl = m(&[("subnets", json!([9])), ("count", json!(7))]);
        let eff = resolve_variables(&[&ws, &tpl]);
        assert_eq!(eff["subnets"], json!([9]), "arrays replace, not concat");
        assert_eq!(eff["count"], json!(7), "scalar innermost wins");
    }

    #[test]
    fn precedence_is_outermost_to_innermost() {
        let a = m(&[("k", json!("a"))]);
        let b = m(&[("k", json!("b"))]);
        let c = m(&[("k", json!("c"))]);
        assert_eq!(
            resolve_variables(&[&a, &b, &c])["k"],
            json!("c"),
            "last layer wins"
        );
        assert_eq!(
            resolve_variables(&[&c, &b, &a])["k"],
            json!("a"),
            "order matters"
        );
    }

    #[test]
    fn empty_layers_are_identity() {
        let tpl = m(&[("k", json!(1))]);
        let empty: VariableMap = BTreeMap::new();
        assert_eq!(resolve_variables(&[&empty, &tpl, &empty]), tpl);
        assert!(resolve_variables(&[]).is_empty());
    }

    #[test]
    fn type_mismatch_lets_inner_replace() {
        // object inherited, scalar override (or vice-versa) — inner wins cleanly.
        let ws = m(&[("x", json!({"a": 1}))]);
        let tpl = m(&[("x", json!("scalar"))]);
        assert_eq!(resolve_variables(&[&ws, &tpl])["x"], json!("scalar"));
    }
}
