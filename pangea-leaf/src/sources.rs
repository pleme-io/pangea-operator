use std::collections::BTreeSet;

use serde_json::Value;

use crate::address::{strip_index, AddressError};

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SourceError {
    #[error("not a terraform state v4 document: {0}")]
    State(String),
    #[error("not a recognised plan document: {0}")]
    Plan(String),
    #[error(transparent)]
    Address(#[from] AddressError),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum PlanAction {
    NoOp,
    Read,
    Create,
    Update,
    Delete,
    Replace,
}

impl PlanAction {
    fn parse_one(s: &str) -> Option<Self> {
        match s {
            "no_op" | "no-op" => Some(Self::NoOp),
            "read" => Some(Self::Read),
            "create" => Some(Self::Create),
            "update" => Some(Self::Update),
            "delete" => Some(Self::Delete),
            "replace" => Some(Self::Replace),
            _ => None,
        }
    }

    fn from_actions(list: &[String]) -> Option<Self> {
        match list {
            [one] => Self::parse_one(one),
            [a, b] if (a == "delete" && b == "create") || (a == "create" && b == "delete") => {
                Some(Self::Replace)
            }
            _ => None,
        }
    }

    #[must_use]
    pub fn adds(self) -> bool {
        matches!(self, Self::Create | Self::Replace)
    }

    #[must_use]
    pub fn changes(self) -> bool {
        matches!(self, Self::Update)
    }

    #[must_use]
    pub fn destroys(self) -> bool {
        matches!(self, Self::Delete | Self::Replace)
    }
}

fn index_text(key: &Value) -> Option<String> {
    match key {
        Value::String(s) => Some(format!(
            "[\"{}\"]",
            s.replace('\\', "\\\\").replace('"', "\\\"")
        )),
        Value::Number(n) => Some(format!("[{n}]")),
        _ => None,
    }
}

pub fn resource_base(r: &Value) -> Result<String, SourceError> {
    let ty = r
        .get("type")
        .and_then(Value::as_str)
        .ok_or_else(|| SourceError::State("resource without `type`".into()))?;
    let name = r
        .get("name")
        .and_then(Value::as_str)
        .ok_or_else(|| SourceError::State("resource without `name`".into()))?;
    let module = r.get("module").and_then(Value::as_str);
    Ok(match module {
        Some(m) if !m.is_empty() => format!("{m}.{ty}.{name}"),
        _ => format!("{ty}.{name}"),
    })
}

#[must_use]
pub fn instance_address(base: &str, instance: &Value) -> String {
    match instance.get("index_key").and_then(index_text) {
        Some(ix) => format!("{base}{ix}"),
        None => base.to_string(),
    }
}

#[must_use]
pub fn is_managed(r: &Value) -> bool {
    r.get("mode").and_then(Value::as_str) == Some("managed")
}

pub fn addresses_from_state(state: &Value) -> Result<Vec<String>, SourceError> {
    let resources = state
        .get("resources")
        .and_then(Value::as_array)
        .ok_or_else(|| SourceError::State("no `resources` array".into()))?;
    let mut out = Vec::new();
    for r in resources {
        if !is_managed(r) {
            continue;
        }
        let base = resource_base(r)?;
        let instances = r
            .get("instances")
            .and_then(Value::as_array)
            .ok_or_else(|| SourceError::State(format!("`{base}` has no `instances`")))?;
        out.extend(instances.iter().map(|inst| instance_address(&base, inst)));
    }
    Ok(out)
}

pub fn addresses_from_lines(text: &str) -> Vec<String> {
    text.lines()
        .map(str::trim)
        .filter(|l| !l.is_empty() && !l.starts_with('#'))
        .map(str::to_string)
        .collect()
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PlanView {
    pub changes: Vec<(String, PlanAction)>,
    pub references: BTreeSet<(String, String)>,
}

pub fn normalize_address(raw: &str) -> Result<String, SourceError> {
    if raw.starts_with('{') {
        let v: Value = serde_json::from_str(raw)
            .map_err(|e| SourceError::Plan(format!("object address is not JSON: {e}")))?;
        magma_address(&v)
    } else {
        Ok(raw.to_string())
    }
}

fn magma_address(a: &Value) -> Result<String, SourceError> {
    let ty = a
        .get("type_id")
        .and_then(Value::as_str)
        .ok_or_else(|| SourceError::Plan("address without `type_id`".into()))?;
    let name = a
        .get("name")
        .and_then(Value::as_str)
        .ok_or_else(|| SourceError::Plan("address without `name`".into()))?;
    let mut s = String::new();
    if let Some(mods) = a.get("module").and_then(Value::as_array) {
        for m in mods {
            match m {
                Value::String(x) if x.starts_with("module.") => {
                    s.push_str(x);
                    s.push('.');
                }
                Value::String(x) => {
                    s.push_str("module.");
                    s.push_str(x);
                    s.push('.');
                }
                other => {
                    return Err(SourceError::Plan(format!(
                        "unsupported module step {other}"
                    )))
                }
            }
        }
    }
    if a.get("kind").and_then(Value::as_str) == Some("data") {
        s.push_str("data.");
    }
    s.push_str(ty);
    s.push('.');
    s.push_str(name);
    if let Some(ix) = a.get("key").and_then(index_text) {
        s.push_str(&ix);
    }
    Ok(s)
}

fn interpolated_targets(v: &Value, out: &mut BTreeSet<String>) {
    match v {
        Value::String(s) => {
            let mut rest = s.as_str();
            while let Some(pos) = rest.find("${") {
                rest = &rest[pos + 2..];
                let end = rest
                    .find(|c: char| {
                        !(c.is_ascii_alphanumeric() || c == '_' || c == '-' || c == '.')
                    })
                    .unwrap_or(rest.len());
                let mut parts = rest[..end].split('.');
                if let (Some(t), Some(n)) = (parts.next(), parts.next()) {
                    if !t.is_empty() && !n.is_empty() && t != "var" && t != "local" && t != "each" {
                        out.insert(format!("{t}.{n}"));
                    }
                }
            }
        }
        Value::Array(a) => a.iter().for_each(|x| interpolated_targets(x, out)),
        Value::Object(o) => o.values().for_each(|x| interpolated_targets(x, out)),
        _ => {}
    }
}

fn config_references(v: &Value, out: &mut BTreeSet<String>) {
    match v {
        Value::Object(o) => {
            for (k, x) in o {
                if k == "references" {
                    if let Some(a) = x.as_array() {
                        for r in a.iter().filter_map(Value::as_str) {
                            out.insert(r.to_string());
                        }
                    }
                } else {
                    config_references(x, out);
                }
            }
        }
        Value::Array(a) => a.iter().for_each(|x| config_references(x, out)),
        _ => {}
    }
}

fn resource_ref(raw: &str) -> Option<String> {
    let mut parts = raw.split('.');
    let first = parts.next()?;
    let second = parts.next()?;
    match first {
        "var" | "local" | "each" | "count" | "path" | "terraform" | "self" | "module" => None,
        "data" => None,
        _ => Some(format!("{first}.{second}")),
    }
}

pub fn plan_view(plan: &Value) -> Result<PlanView, SourceError> {
    let changes = plan
        .get("resource_changes")
        .and_then(Value::as_array)
        .ok_or_else(|| SourceError::Plan("no `resource_changes` array".into()))?;
    let mut view = PlanView::default();
    for c in changes {
        let (address, action, after) = match c.get("address") {
            Some(Value::String(s)) => {
                let list: Vec<String> = c
                    .pointer("/change/actions")
                    .and_then(Value::as_array)
                    .map(|a| {
                        a.iter()
                            .filter_map(Value::as_str)
                            .map(str::to_string)
                            .collect()
                    })
                    .unwrap_or_default();
                let action = PlanAction::from_actions(&list)
                    .ok_or_else(|| SourceError::Plan(format!("`{s}` has actions {list:?}")))?;
                (s.clone(), action, None)
            }
            Some(obj @ Value::Object(_)) => {
                let a = c
                    .get("action")
                    .and_then(Value::as_str)
                    .and_then(PlanAction::parse_one)
                    .ok_or_else(|| SourceError::Plan("change without a known `action`".into()))?;
                (magma_address(obj)?, a, c.get("after"))
            }
            _ => return Err(SourceError::Plan("change without `address`".into())),
        };
        if let Some(after) = after {
            let mut targets = BTreeSet::new();
            interpolated_targets(after, &mut targets);
            let from = strip_index(&address)?;
            for t in targets {
                if t != from {
                    view.references.insert((from.clone(), t));
                }
            }
        }
        view.changes.push((address, action));
    }
    if let Some(cfg) = plan.pointer("/configuration/root_module") {
        collect_config(cfg, &mut view)?;
    }
    Ok(view)
}

fn collect_config(module: &Value, view: &mut PlanView) -> Result<(), SourceError> {
    if let Some(rs) = module.get("resources").and_then(Value::as_array) {
        for r in rs {
            let Some(addr) = r.get("address").and_then(Value::as_str) else {
                continue;
            };
            let mut refs = BTreeSet::new();
            if let Some(e) = r.get("expressions") {
                config_references(e, &mut refs);
            }
            let from = strip_index(addr)?;
            for raw in refs {
                if let Some(t) = resource_ref(&raw) {
                    if t != from {
                        view.references.insert((from.clone(), t));
                    }
                }
            }
        }
    }
    if let Some(calls) = module.get("module_calls").and_then(Value::as_object) {
        for call in calls.values() {
            if let Some(m) = call.get("module") {
                collect_config(m, view)?;
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn state_addresses_include_modules_and_index_keys() {
        let st = json!({"version":4,"resources":[
            {"mode":"managed","type":"t","name":"a","instances":[{"index_key":"x\"y"},{"index_key":2}]},
            {"mode":"managed","module":"module.m[\"k\"]","type":"t","name":"b","instances":[{}]},
            {"mode":"data","type":"d","name":"c","instances":[{}]}
        ]});
        assert_eq!(
            addresses_from_state(&st).unwrap(),
            vec!["t.a[\"x\\\"y\"]", "t.a[2]", "module.m[\"k\"].t.b"]
        );
    }

    #[test]
    fn state_without_resources_is_refused() {
        assert!(addresses_from_state(&json!({})).is_err());
    }

    #[test]
    fn state_addresses_round_trip_through_the_key_function() {
        let st = json!({"resources":[
            {"mode":"managed","type":"t","name":"a","instances":[{"index_key":"x\"y"}]}
        ]});
        let a = addresses_from_state(&st).unwrap();
        assert_eq!(
            crate::address::leaf_key(&a[0]).unwrap(),
            crate::address::LeafKey::Keyed("x\"y".into())
        );
    }

    #[test]
    fn lines_skip_blanks_and_comments() {
        assert_eq!(
            addresses_from_lines("a.b\n\n# c\n  d.e  \n"),
            vec!["a.b", "d.e"]
        );
    }

    #[test]
    fn terraform_plan_actions_map_including_replace() {
        let p = json!({"resource_changes":[
            {"address":"a.b","change":{"actions":["no-op"]}},
            {"address":"a.c","change":{"actions":["delete","create"]}},
            {"address":"a.d","change":{"actions":["update"]}}
        ]});
        let v = plan_view(&p).unwrap();
        assert_eq!(v.changes[0].1, PlanAction::NoOp);
        assert_eq!(v.changes[1].1, PlanAction::Replace);
        assert!(v.changes[1].1.adds() && v.changes[1].1.destroys());
        assert!(v.changes[2].1.changes());
    }

    #[test]
    fn terraform_plan_config_references_become_edges() {
        let p = json!({"resource_changes":[],"configuration":{"root_module":{"resources":[
            {"address":"a.perm","expressions":{"repository":{"references":["a.repo.name","a.repo","var.x"]}}}
        ]}}});
        let v = plan_view(&p).unwrap();
        assert!(v.references.contains(&("a.perm".into(), "a.repo".into())));
        assert_eq!(v.references.len(), 1);
    }

    #[test]
    fn magma_plan_addresses_and_interpolations() {
        let p = json!({"resource_changes":[
            {"address":{"module":[],"kind":"managed","type_id":"gh_perm","name":"p","key":null},
             "action":"no_op","after":{"repository":"${gh_repo.r.name}","n":"${var.x}"}},
            {"address":{"module":["m"],"kind":"managed","type_id":"gh_repo","name":"r","key":"k"},
             "action":"create"}
        ]});
        let v = plan_view(&p).unwrap();
        assert_eq!(v.changes[0].0, "gh_perm.p");
        assert_eq!(v.changes[1].0, "module.m.gh_repo.r[\"k\"]");
        assert!(v
            .references
            .contains(&("gh_perm.p".into(), "gh_repo.r".into())));
        assert_eq!(v.references.len(), 1);
    }

    #[test]
    fn object_addresses_rendered_as_text_normalize_to_flat_addresses() {
        let raw = r#"{"module":[],"kind":"managed","type_id":"t","name":"n","key":"k"}"#;
        assert_eq!(normalize_address(raw).unwrap(), "t.n[\"k\"]");
        assert_eq!(normalize_address("t.n").unwrap(), "t.n");
        assert!(normalize_address("{broken").is_err());
    }

    #[test]
    fn an_unknown_action_is_refused_not_rounded() {
        let p = json!({"resource_changes":[{"address":"a.b","change":{"actions":["frob"]}}]});
        assert!(plan_view(&p).is_err());
    }
}
