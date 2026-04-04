//! Variable reference resolver for InfrastructureFlow steps.
//!
//! Resolves `{{ steps.NAME.outputs.KEY }}` references in step variables
//! by looking up completed step outputs.

use crate::error::{Error, Result};
use std::collections::BTreeMap;

/// Resolve `{{ steps.NAME.outputs.KEY }}` references in variable values.
///
/// Walks all values in the variables map. For string values containing
/// `{{ steps.X.outputs.Y }}`, looks up the output in `step_outputs`.
/// Non-string values and strings without references pass through unchanged.
pub fn resolve_step_references(
    variables: &BTreeMap<String, serde_json::Value>,
    step_outputs: &BTreeMap<String, BTreeMap<String, serde_json::Value>>,
) -> Result<BTreeMap<String, serde_json::Value>> {
    let mut resolved = BTreeMap::new();

    for (key, value) in variables {
        let resolved_value = resolve_value(value, step_outputs)?;
        resolved.insert(key.clone(), resolved_value);
    }

    Ok(resolved)
}

/// Check if all references in variables can be resolved.
/// Returns the names of steps whose outputs are needed but not available.
pub fn unresolved_dependencies(
    variables: &BTreeMap<String, serde_json::Value>,
    step_outputs: &BTreeMap<String, BTreeMap<String, serde_json::Value>>,
) -> Vec<String> {
    let mut missing = Vec::new();

    for value in variables.values() {
        if let Some(s) = value.as_str() {
            for reference in extract_references(s) {
                if !step_outputs.contains_key(&reference.step_name) {
                    if !missing.contains(&reference.step_name) {
                        missing.push(reference.step_name);
                    }
                }
            }
        }
    }

    missing
}

fn resolve_value(
    value: &serde_json::Value,
    step_outputs: &BTreeMap<String, BTreeMap<String, serde_json::Value>>,
) -> Result<serde_json::Value> {
    match value {
        serde_json::Value::String(s) => {
            let resolved = resolve_string(s, step_outputs)?;
            // If the entire string was a single reference, preserve the original JSON type
            let refs = extract_references(s);
            if refs.len() == 1 && s.trim() == format!("{{{{ steps.{}.outputs.{} }}}}", refs[0].step_name, refs[0].output_key) {
                // Single reference — return the raw value (preserves arrays, objects, numbers)
                if let Some(outputs) = step_outputs.get(&refs[0].step_name) {
                    if let Some(val) = outputs.get(&refs[0].output_key) {
                        return Ok(val.clone());
                    }
                }
            }
            Ok(serde_json::Value::String(resolved))
        }
        serde_json::Value::Array(arr) => {
            let resolved: Result<Vec<_>> = arr
                .iter()
                .map(|v| resolve_value(v, step_outputs))
                .collect();
            Ok(serde_json::Value::Array(resolved?))
        }
        serde_json::Value::Object(obj) => {
            let mut resolved = serde_json::Map::new();
            for (k, v) in obj {
                resolved.insert(k.clone(), resolve_value(v, step_outputs)?);
            }
            Ok(serde_json::Value::Object(resolved))
        }
        // Numbers, bools, null pass through
        other => Ok(other.clone()),
    }
}

fn resolve_string(
    s: &str,
    step_outputs: &BTreeMap<String, BTreeMap<String, serde_json::Value>>,
) -> Result<String> {
    let mut result = s.to_string();

    for reference in extract_references(s) {
        let outputs = step_outputs.get(&reference.step_name).ok_or_else(|| {
            Error::Config(format!(
                "Step '{}' not found in flow outputs",
                reference.step_name
            ))
        })?;

        let value = outputs.get(&reference.output_key).ok_or_else(|| {
            Error::Config(format!(
                "Output '{}' not found in step '{}'",
                reference.output_key, reference.step_name
            ))
        })?;

        let replacement = match value {
            serde_json::Value::String(s) => s.clone(),
            other => other.to_string(),
        };

        let pattern = format!("{{{{ steps.{}.outputs.{} }}}}", reference.step_name, reference.output_key);
        result = result.replace(&pattern, &replacement);
    }

    Ok(result)
}

#[derive(Debug, Clone)]
struct Reference {
    step_name: String,
    output_key: String,
}

fn extract_references(s: &str) -> Vec<Reference> {
    let mut refs = Vec::new();
    let mut remaining = s;

    while let Some(start) = remaining.find("{{ steps.") {
        let after_start = &remaining[start + 9..]; // skip "{{ steps."
        if let Some(end) = after_start.find(" }}") {
            let inner = &after_start[..end]; // "NAME.outputs.KEY"
            if let Some(dot_pos) = inner.find(".outputs.") {
                let step_name = inner[..dot_pos].to_string();
                let output_key = inner[dot_pos + 9..].to_string();
                refs.push(Reference {
                    step_name,
                    output_key,
                });
            }
            remaining = &after_start[end + 3..];
        } else {
            break;
        }
    }

    refs
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_outputs() -> BTreeMap<String, BTreeMap<String, serde_json::Value>> {
        let mut step_a = BTreeMap::new();
        step_a.insert("vpc_id".into(), serde_json::json!("vpc-abc123"));
        step_a.insert("subnet_ids".into(), serde_json::json!(["subnet-1", "subnet-2"]));

        let mut outputs = BTreeMap::new();
        outputs.insert("step-a".into(), step_a);
        outputs
    }

    #[test]
    fn test_resolve_simple_reference() {
        let outputs = make_outputs();
        let mut vars = BTreeMap::new();
        vars.insert("vpc".into(), serde_json::json!("{{ steps.step-a.outputs.vpc_id }}"));

        let resolved = resolve_step_references(&vars, &outputs).unwrap();
        assert_eq!(resolved["vpc"], serde_json::json!("vpc-abc123"));
    }

    #[test]
    fn test_resolve_missing_step() {
        let outputs = make_outputs();
        let mut vars = BTreeMap::new();
        vars.insert("x".into(), serde_json::json!("{{ steps.missing.outputs.foo }}"));

        assert!(resolve_step_references(&vars, &outputs).is_err());
    }

    #[test]
    fn test_resolve_missing_output() {
        let outputs = make_outputs();
        let mut vars = BTreeMap::new();
        vars.insert("x".into(), serde_json::json!("{{ steps.step-a.outputs.missing }}"));

        assert!(resolve_step_references(&vars, &outputs).is_err());
    }

    #[test]
    fn test_resolve_passthrough_static() {
        let outputs = make_outputs();
        let mut vars = BTreeMap::new();
        vars.insert("region".into(), serde_json::json!("us-east-1"));
        vars.insert("count".into(), serde_json::json!(3));

        let resolved = resolve_step_references(&vars, &outputs).unwrap();
        assert_eq!(resolved["region"], serde_json::json!("us-east-1"));
        assert_eq!(resolved["count"], serde_json::json!(3));
    }

    #[test]
    fn test_unresolved_dependencies() {
        let outputs = make_outputs();
        let mut vars = BTreeMap::new();
        vars.insert("a".into(), serde_json::json!("{{ steps.step-a.outputs.vpc_id }}"));
        vars.insert("b".into(), serde_json::json!("{{ steps.step-b.outputs.endpoint }}"));
        vars.insert("c".into(), serde_json::json!("{{ steps.step-c.outputs.arn }}"));

        let missing = unresolved_dependencies(&vars, &outputs);
        assert_eq!(missing.len(), 2);
        assert!(missing.contains(&"step-b".to_string()));
        assert!(missing.contains(&"step-c".to_string()));
    }

    #[test]
    fn test_extract_multiple_references() {
        let refs = extract_references("prefix {{ steps.a.outputs.x }} middle {{ steps.b.outputs.y }} suffix");
        assert_eq!(refs.len(), 2);
        assert_eq!(refs[0].step_name, "a");
        assert_eq!(refs[0].output_key, "x");
        assert_eq!(refs[1].step_name, "b");
        assert_eq!(refs[1].output_key, "y");
    }
}
