//! Variable reference resolver for InfrastructureFlow steps.
//!
//! Resolves `{{ steps.NAME.outputs.KEY }}` references in step variables
//! by looking up completed step outputs.

use crate::error::{Error, Result};
use std::collections::BTreeMap;

/// Context for resolving step references, including both outputs and full state.
#[derive(Default)]
pub struct ResolutionContext {
    /// Step outputs: step_name -> { key: value }
    pub outputs: BTreeMap<String, BTreeMap<String, serde_json::Value>>,
    /// Step state snapshots: step_name -> full terraform state JSON
    pub states: BTreeMap<String, serde_json::Value>,
}

/// Resolve `{{ steps.NAME.outputs.KEY }}` and `{{ steps.NAME.state.TYPE.NAME.ATTR }}`
/// references in variable values.
///
/// Walks all values in the variables map. For string values containing
/// references, looks up the value from the resolution context.
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
            if refs.len() == 1
                && s.trim()
                    == format!(
                        "{{{{ steps.{}.outputs.{} }}}}",
                        refs[0].step_name, refs[0].output_key
                    )
            {
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
            let resolved: Result<Vec<_>> =
                arr.iter().map(|v| resolve_value(v, step_outputs)).collect();
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

        let pattern = format!(
            "{{{{ steps.{}.outputs.{} }}}}",
            reference.step_name, reference.output_key
        );
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

/// Resolve references using the full resolution context (outputs + state).
pub fn resolve_with_context(
    variables: &BTreeMap<String, serde_json::Value>,
    ctx: &ResolutionContext,
) -> Result<BTreeMap<String, serde_json::Value>> {
    let mut resolved = BTreeMap::new();

    for (key, value) in variables {
        let resolved_value = resolve_value_with_context(value, ctx)?;
        resolved.insert(key.clone(), resolved_value);
    }

    Ok(resolved)
}

fn resolve_value_with_context(
    value: &serde_json::Value,
    ctx: &ResolutionContext,
) -> Result<serde_json::Value> {
    match value {
        serde_json::Value::String(s) => {
            let resolved = resolve_string_with_context(s, ctx)?;
            Ok(serde_json::Value::String(resolved))
        }
        serde_json::Value::Array(arr) => {
            let resolved: Result<Vec<_>> = arr
                .iter()
                .map(|v| resolve_value_with_context(v, ctx))
                .collect();
            Ok(serde_json::Value::Array(resolved?))
        }
        serde_json::Value::Object(obj) => {
            let mut resolved = serde_json::Map::new();
            for (k, v) in obj {
                resolved.insert(k.clone(), resolve_value_with_context(v, ctx)?);
            }
            Ok(serde_json::Value::Object(resolved))
        }
        other => Ok(other.clone()),
    }
}

fn resolve_string_with_context(s: &str, ctx: &ResolutionContext) -> Result<String> {
    let mut result = s.to_string();

    // Resolve output references: {{ steps.NAME.outputs.KEY }}
    for reference in extract_references(s) {
        let outputs = ctx.outputs.get(&reference.step_name).ok_or_else(|| {
            Error::Config(format!(
                "Step '{}' not found in context",
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
        let pattern = format!(
            "{{{{ steps.{}.outputs.{} }}}}",
            reference.step_name, reference.output_key
        );
        result = result.replace(&pattern, &replacement);
    }

    // Resolve state references: {{ steps.NAME.state.TYPE.RESNAME.ATTR }}
    for state_ref in extract_state_references(s) {
        let state = ctx.states.get(&state_ref.step_name).ok_or_else(|| {
            Error::Config(format!(
                "State for step '{}' not available",
                state_ref.step_name
            ))
        })?;

        // Navigate: values.root_module.resources[type=TYPE, name=RESNAME].values.ATTR
        let value = resolve_state_attribute(
            state,
            &state_ref.resource_type,
            &state_ref.resource_name,
            &state_ref.attribute,
        )
        .ok_or_else(|| {
            Error::Config(format!(
                "State attribute {}.{}.{} not found in step '{}'",
                state_ref.resource_type,
                state_ref.resource_name,
                state_ref.attribute,
                state_ref.step_name
            ))
        })?;

        let replacement = match value {
            serde_json::Value::String(s) => s.clone(),
            other => other.to_string(),
        };
        let pattern = format!(
            "{{{{ steps.{}.state.{}.{}.{} }}}}",
            state_ref.step_name,
            state_ref.resource_type,
            state_ref.resource_name,
            state_ref.attribute
        );
        result = result.replace(&pattern, &replacement);
    }

    Ok(result)
}

#[derive(Debug)]
struct StateReference {
    step_name: String,
    resource_type: String,
    resource_name: String,
    attribute: String,
}

fn extract_state_references(s: &str) -> Vec<StateReference> {
    let mut refs = Vec::new();
    let mut remaining = s;

    while let Some(start) = remaining.find("{{ steps.") {
        let after_start = &remaining[start + 9..];
        if let Some(end) = after_start.find(" }}") {
            let inner = &after_start[..end];
            // Check for state pattern: NAME.state.TYPE.RESNAME.ATTR
            if let Some(state_pos) = inner.find(".state.") {
                let step_name = inner[..state_pos].to_string();
                let after_state = &inner[state_pos + 7..];
                let parts: Vec<&str> = after_state.splitn(3, '.').collect();
                if parts.len() == 3 {
                    refs.push(StateReference {
                        step_name,
                        resource_type: parts[0].to_string(),
                        resource_name: parts[1].to_string(),
                        attribute: parts[2].to_string(),
                    });
                }
            }
            remaining = &after_start[end + 3..];
        } else {
            break;
        }
    }

    refs
}

/// Navigate terraform state JSON to find a resource attribute.
fn resolve_state_attribute(
    state: &serde_json::Value,
    resource_type: &str,
    resource_name: &str,
    attribute: &str,
) -> Option<serde_json::Value> {
    // Terraform state JSON structure:
    // { "values": { "root_module": { "resources": [ { "type": "...", "name": "...", "values": { ... } } ] } } }
    let resources = state
        .get("values")?
        .get("root_module")?
        .get("resources")?
        .as_array()?;

    for resource in resources {
        let rtype = resource.get("type")?.as_str()?;
        let rname = resource.get("name")?.as_str()?;
        if rtype == resource_type && rname == resource_name {
            return resource.get("values")?.get(attribute).cloned();
        }
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_outputs() -> BTreeMap<String, BTreeMap<String, serde_json::Value>> {
        let mut step_a = BTreeMap::new();
        step_a.insert("vpc_id".into(), serde_json::json!("vpc-abc123"));
        step_a.insert(
            "subnet_ids".into(),
            serde_json::json!(["subnet-1", "subnet-2"]),
        );

        let mut outputs = BTreeMap::new();
        outputs.insert("step-a".into(), step_a);
        outputs
    }

    #[test]
    fn test_resolve_simple_reference() {
        let outputs = make_outputs();
        let mut vars = BTreeMap::new();
        vars.insert(
            "vpc".into(),
            serde_json::json!("{{ steps.step-a.outputs.vpc_id }}"),
        );

        let resolved = resolve_step_references(&vars, &outputs).unwrap();
        assert_eq!(resolved["vpc"], serde_json::json!("vpc-abc123"));
    }

    #[test]
    fn test_resolve_missing_step() {
        let outputs = make_outputs();
        let mut vars = BTreeMap::new();
        vars.insert(
            "x".into(),
            serde_json::json!("{{ steps.missing.outputs.foo }}"),
        );

        assert!(resolve_step_references(&vars, &outputs).is_err());
    }

    #[test]
    fn test_resolve_missing_output() {
        let outputs = make_outputs();
        let mut vars = BTreeMap::new();
        vars.insert(
            "x".into(),
            serde_json::json!("{{ steps.step-a.outputs.missing }}"),
        );

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
        vars.insert(
            "a".into(),
            serde_json::json!("{{ steps.step-a.outputs.vpc_id }}"),
        );
        vars.insert(
            "b".into(),
            serde_json::json!("{{ steps.step-b.outputs.endpoint }}"),
        );
        vars.insert(
            "c".into(),
            serde_json::json!("{{ steps.step-c.outputs.arn }}"),
        );

        let missing = unresolved_dependencies(&vars, &outputs);
        assert_eq!(missing.len(), 2);
        assert!(missing.contains(&"step-b".to_string()));
        assert!(missing.contains(&"step-c".to_string()));
    }

    #[test]
    fn test_resolve_state_reference() {
        let state = serde_json::json!({
            "values": {
                "root_module": {
                    "resources": [
                        {
                            "type": "aws_vpc",
                            "name": "main",
                            "values": {
                                "id": "vpc-12345",
                                "cidr_block": "10.0.0.0/16"
                            }
                        }
                    ]
                }
            }
        });

        let mut ctx = ResolutionContext::default();
        ctx.states.insert("vpc-step".into(), state);

        let mut vars = BTreeMap::new();
        vars.insert(
            "vpc_id".into(),
            serde_json::json!("{{ steps.vpc-step.state.aws_vpc.main.id }}"),
        );
        vars.insert(
            "cidr".into(),
            serde_json::json!("{{ steps.vpc-step.state.aws_vpc.main.cidr_block }}"),
        );

        let resolved = resolve_with_context(&vars, &ctx).unwrap();
        assert_eq!(resolved["vpc_id"], serde_json::json!("vpc-12345"));
        assert_eq!(resolved["cidr"], serde_json::json!("10.0.0.0/16"));
    }

    #[test]
    fn test_extract_state_references() {
        let refs = extract_state_references("{{ steps.net.state.aws_vpc.main.id }}");
        assert_eq!(refs.len(), 1);
        assert_eq!(refs[0].step_name, "net");
        assert_eq!(refs[0].resource_type, "aws_vpc");
        assert_eq!(refs[0].resource_name, "main");
        assert_eq!(refs[0].attribute, "id");
    }

    #[test]
    fn test_extract_multiple_references() {
        let refs = extract_references(
            "prefix {{ steps.a.outputs.x }} middle {{ steps.b.outputs.y }} suffix",
        );
        assert_eq!(refs.len(), 2);
        assert_eq!(refs[0].step_name, "a");
        assert_eq!(refs[0].output_key, "x");
        assert_eq!(refs[1].step_name, "b");
        assert_eq!(refs[1].output_key, "y");
    }

    #[test]
    fn test_extract_references_no_refs() {
        let refs = extract_references("plain string with no references");
        assert!(refs.is_empty());
    }

    #[test]
    fn test_extract_references_incomplete_ref() {
        let refs = extract_references("{{ steps.a.outputs.x");
        assert!(refs.is_empty());
    }

    #[test]
    fn test_extract_references_missing_outputs_keyword() {
        let refs = extract_references("{{ steps.a.something.x }}");
        assert!(refs.is_empty());
    }

    #[test]
    fn test_resolve_nested_json_objects() {
        let outputs = make_outputs();
        let mut vars = BTreeMap::new();
        vars.insert(
            "config".into(),
            serde_json::json!({
                "vpc": "{{ steps.step-a.outputs.vpc_id }}",
                "static": "value"
            }),
        );

        let resolved = resolve_step_references(&vars, &outputs).unwrap();
        let config = &resolved["config"];
        assert_eq!(config["vpc"], "vpc-abc123");
        assert_eq!(config["static"], "value");
    }

    #[test]
    fn test_resolve_array_values() {
        let outputs = make_outputs();
        let mut vars = BTreeMap::new();
        vars.insert(
            "list".into(),
            serde_json::json!(["{{ steps.step-a.outputs.vpc_id }}", "static"]),
        );

        let resolved = resolve_step_references(&vars, &outputs).unwrap();
        let list = resolved["list"].as_array().unwrap();
        assert_eq!(list[0], "vpc-abc123");
        assert_eq!(list[1], "static");
    }

    #[test]
    fn test_resolve_preserves_non_string_types() {
        let outputs = make_outputs();
        let mut vars = BTreeMap::new();
        vars.insert("num".into(), serde_json::json!(42));
        vars.insert("bool".into(), serde_json::json!(true));
        vars.insert("null".into(), serde_json::Value::Null);

        let resolved = resolve_step_references(&vars, &outputs).unwrap();
        assert_eq!(resolved["num"], 42);
        assert_eq!(resolved["bool"], true);
        assert!(resolved["null"].is_null());
    }

    #[test]
    fn test_unresolved_dependencies_empty_when_all_resolved() {
        let outputs = make_outputs();
        let mut vars = BTreeMap::new();
        vars.insert(
            "vpc".into(),
            serde_json::json!("{{ steps.step-a.outputs.vpc_id }}"),
        );

        let missing = unresolved_dependencies(&vars, &outputs);
        assert!(missing.is_empty());
    }

    #[test]
    fn test_unresolved_dependencies_deduplicates() {
        let outputs = BTreeMap::new();
        let mut vars = BTreeMap::new();
        vars.insert(
            "a".into(),
            serde_json::json!("{{ steps.missing.outputs.x }}"),
        );
        vars.insert(
            "b".into(),
            serde_json::json!("{{ steps.missing.outputs.y }}"),
        );

        let missing = unresolved_dependencies(&vars, &outputs);
        assert_eq!(missing.len(), 1);
        assert_eq!(missing[0], "missing");
    }

    #[test]
    fn test_resolve_with_context_missing_state() {
        let ctx = ResolutionContext::default();
        let mut vars = BTreeMap::new();
        vars.insert(
            "x".into(),
            serde_json::json!("{{ steps.net.state.aws_vpc.main.id }}"),
        );

        let result = resolve_with_context(&vars, &ctx);
        assert!(result.is_err());
    }

    #[test]
    fn test_resolve_with_context_missing_resource_in_state() {
        let state = serde_json::json!({
            "values": {
                "root_module": {
                    "resources": []
                }
            }
        });
        let mut ctx = ResolutionContext::default();
        ctx.states.insert("net".into(), state);

        let mut vars = BTreeMap::new();
        vars.insert(
            "x".into(),
            serde_json::json!("{{ steps.net.state.aws_vpc.nonexistent.id }}"),
        );

        let result = resolve_with_context(&vars, &ctx);
        assert!(result.is_err());
    }

    #[test]
    fn test_resolve_with_context_mixed_output_and_state() {
        let state = serde_json::json!({
            "values": {
                "root_module": {
                    "resources": [{
                        "type": "aws_vpc",
                        "name": "main",
                        "values": { "id": "vpc-state-123" }
                    }]
                }
            }
        });

        let mut outputs = BTreeMap::new();
        outputs.insert("vpc_id".into(), serde_json::json!("vpc-output-456"));
        let mut step_outputs = BTreeMap::new();
        step_outputs.insert("net".into(), outputs);

        let mut ctx = ResolutionContext::default();
        ctx.states.insert("net".into(), state);
        ctx.outputs = step_outputs;

        let mut vars = BTreeMap::new();
        vars.insert(
            "from_output".into(),
            serde_json::json!("{{ steps.net.outputs.vpc_id }}"),
        );
        vars.insert(
            "from_state".into(),
            serde_json::json!("{{ steps.net.state.aws_vpc.main.id }}"),
        );

        let resolved = resolve_with_context(&vars, &ctx).unwrap();
        assert_eq!(resolved["from_output"], "vpc-output-456");
        assert_eq!(resolved["from_state"], "vpc-state-123");
    }

    #[test]
    fn test_extract_state_references_multiple() {
        let refs = extract_state_references(
            "a={{ steps.net.state.aws_vpc.main.id }} b={{ steps.db.state.aws_rds.primary.endpoint }}"
        );
        assert_eq!(refs.len(), 2);
        assert_eq!(refs[0].step_name, "net");
        assert_eq!(refs[1].step_name, "db");
        assert_eq!(refs[1].resource_type, "aws_rds");
        assert_eq!(refs[1].resource_name, "primary");
        assert_eq!(refs[1].attribute, "endpoint");
    }

    #[test]
    fn test_extract_state_references_none() {
        let refs = extract_state_references("no state refs here");
        assert!(refs.is_empty());
    }

    #[test]
    fn test_resolve_string_interpolation_embedded_in_text() {
        let outputs = make_outputs();
        let mut vars = BTreeMap::new();
        vars.insert(
            "url".into(),
            serde_json::json!("https://{{ steps.step-a.outputs.vpc_id }}.example.com"),
        );

        let resolved = resolve_step_references(&vars, &outputs).unwrap();
        assert_eq!(resolved["url"], "https://vpc-abc123.example.com");
    }
}
