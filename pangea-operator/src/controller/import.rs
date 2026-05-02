//! Auto-import support for the `tofu import` pre-apply pass.
//!
//! Three resolution layers when figuring out a tofu-import ID for a
//! `create`-action:
//!
//!   1. `spec.importHints` — per-address override (highest priority,
//!      handled in `template_controller::run_import_prepass` directly
//!      via `substitute_import_id`).
//!   2. `spec.importPolicy.naturalIds` — per-resource-type templates
//!      (handled here via `resolve_natural_id`).
//!   3. Operator-bundled defaults for common providers (handled here
//!      via `bundled_natural_ids`).
//!
//! Substitution uses the same `{{ .var }}` / `{{ var }}` syntax as
//! `importHints`, with one extension: `{{ .planned.<attr> }}` reads
//! from the plan's `change.after` block for the resource (i.e. the
//! attributes the apply WOULD have written if the resource didn't
//! already exist).

use std::collections::BTreeMap;

/// Operator-bundled per-resource-type natural-ID templates.
///
/// Keys are terraform resource types; values are substitution
/// templates over `{{ .planned.<attr> }}` (and optionally
/// `{{ var }}` from `spec.variables`).
///
/// Sources (verified against each provider's `terraform import` docs):
///   - github: <https://registry.terraform.io/providers/integrations/github/latest/docs>
///   - aws:    <https://registry.terraform.io/providers/hashicorp/aws/latest/docs>
///   - cloudflare: <https://registry.terraform.io/providers/cloudflare/cloudflare/latest/docs>
///
/// This is a starter set covering the use cases pleme-io's
/// pleme-io-opensource workspace exercises today
/// (github_repository, github_branch_protection, github_issue_label).
/// Extend by appending here when a new provider's resources need
/// auto-import support across multiple templates; one-off cases
/// should use `spec.importPolicy.naturalIds` per-template instead.
pub fn bundled_natural_ids() -> BTreeMap<&'static str, &'static str> {
    let mut m = BTreeMap::new();

    // GitHub provider
    m.insert("github_repository", "{{ .planned.name }}");
    m.insert(
        "github_branch_protection",
        "{{ .planned.repository_id }}:{{ .planned.pattern }}",
    );
    m.insert(
        "github_issue_label",
        "{{ .planned.repository }}:{{ .planned.name }}",
    );
    m.insert(
        "github_team",
        "{{ .planned.slug }}",
    );
    m.insert(
        "github_team_membership",
        "{{ .planned.team_id }}:{{ .planned.username }}",
    );
    m.insert(
        "github_actions_secret",
        "{{ .planned.repository }}:{{ .planned.secret_name }}",
    );
    m.insert(
        "github_actions_variable",
        "{{ .planned.repository }}:{{ .planned.variable_name }}",
    );
    m.insert(
        "github_repository_environment",
        "{{ .planned.repository }}:{{ .planned.environment }}",
    );

    // AWS provider — a starter selection
    m.insert("aws_iam_role", "{{ .planned.name }}");
    m.insert("aws_iam_policy", "{{ .planned.arn }}");
    m.insert("aws_iam_user", "{{ .planned.name }}");
    m.insert("aws_s3_bucket", "{{ .planned.bucket }}");

    // Cloudflare provider
    m.insert(
        "cloudflare_dns_record",
        "{{ .planned.zone_id }}/{{ .planned.id }}",
    );
    m.insert(
        "cloudflare_zero_trust_tunnel_cloudflared",
        "{{ .planned.account_id }}/{{ .planned.id }}",
    );

    m
}

/// Resolve a natural-ID template for a given resource address using
/// the three-layer cascade. Returns `None` if no rule matches.
///
/// `address` is the full tofu address (e.g. `github_repository.foo`).
/// We strip the dotted-instance suffix to get the resource-type
/// (`github_repository`).
pub fn resolve_natural_id<'a>(
    address: &str,
    user_natural_ids: &'a BTreeMap<String, String>,
) -> Option<String> {
    let resource_type = address.split('.').next()?;

    // Layer 2: user's per-template naturalIds.
    if let Some(template) = user_natural_ids.get(resource_type) {
        return Some(template.clone());
    }

    // Layer 3: bundled defaults.
    bundled_natural_ids()
        .get(resource_type)
        .map(|s| s.to_string())
}

/// Parse `tofu show -json plan` output into a per-address map of
/// planned attribute values (`change.after`). Returns an empty map
/// on parse failure (best-effort — the caller proceeds without
/// auto-import rather than blocking the apply).
///
/// The shape we care about:
/// ```json
/// {
///   "resource_changes": [
///     {
///       "address": "github_repository.foo",
///       "change": { "after": { "name": "foo", ... } }
///     }
///   ]
/// }
/// ```
pub fn parse_planned_attrs(
    plan_json: &str,
) -> BTreeMap<String, serde_json::Value> {
    let mut out = BTreeMap::new();
    let parsed: serde_json::Value = match serde_json::from_str(plan_json) {
        Ok(v) => v,
        Err(_) => return out,
    };
    let changes = match parsed.get("resource_changes").and_then(|v| v.as_array()) {
        Some(a) => a,
        None => return out,
    };
    for change in changes {
        let address = match change.get("address").and_then(|v| v.as_str()) {
            Some(s) => s.to_string(),
            None => continue,
        };
        if let Some(after) = change.pointer("/change/after") {
            out.insert(address, after.clone());
        }
    }
    out
}

/// Substitute `{{ .planned.<attr> }}` and `{{ .var }}` (or
/// `{{ var }}`) tokens in `template` against the per-address planned
/// attrs and the spec's variables.
///
/// Returns `Err(missing)` on the first unresolved token so the
/// caller can surface it as a typed event.
///
/// Token forms:
///   - `{{ .planned.name }}`   → `planned_attrs["name"]`
///   - `{{ .name }}`           → `variables["name"]`
///   - `{{ name }}`            → `variables["name"]` (no-dot form)
pub fn substitute_with_planned(
    template: &str,
    planned_attrs: &serde_json::Value,
    variables: &BTreeMap<String, serde_json::Value>,
) -> Result<String, String> {
    let mut out = String::with_capacity(template.len());
    let bytes = template.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if i + 1 < bytes.len() && bytes[i] == b'{' && bytes[i + 1] == b'{' {
            let close = match template[i + 2..].find("}}") {
                Some(p) => i + 2 + p,
                None => {
                    out.push_str(&template[i..]);
                    break;
                }
            };
            let inner = template[i + 2..close].trim();
            let inner = inner.strip_prefix('.').unwrap_or(inner).trim();
            let value = if let Some(attr) = inner.strip_prefix("planned.") {
                planned_attrs.get(attr.trim()).cloned()
            } else {
                variables.get(inner).cloned()
            };
            match value {
                Some(serde_json::Value::String(s)) => out.push_str(&s),
                Some(serde_json::Value::Null) => return Err(inner.to_string()),
                Some(v) => out.push_str(&v.to_string().trim_matches('"').to_string()),
                None => return Err(inner.to_string()),
            }
            i = close + 2;
        } else {
            out.push(bytes[i] as char);
            i += 1;
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bundled_includes_github_repository() {
        let b = bundled_natural_ids();
        assert_eq!(b.get("github_repository").copied(), Some("{{ .planned.name }}"));
    }

    #[test]
    fn bundled_includes_branch_protection() {
        let b = bundled_natural_ids();
        let t = b.get("github_branch_protection").copied().unwrap();
        assert!(t.contains("repository_id"));
        assert!(t.contains("pattern"));
    }

    #[test]
    fn resolve_user_natural_id_overrides_bundled() {
        let mut user = BTreeMap::new();
        user.insert("github_repository".to_string(), "custom-{{ .planned.name }}".to_string());
        let r = resolve_natural_id("github_repository.foo", &user);
        assert_eq!(r.as_deref(), Some("custom-{{ .planned.name }}"));
    }

    #[test]
    fn resolve_falls_back_to_bundled() {
        let user = BTreeMap::new();
        let r = resolve_natural_id("github_repository.foo", &user);
        assert_eq!(r.as_deref(), Some("{{ .planned.name }}"));
    }

    #[test]
    fn resolve_returns_none_for_unknown_type() {
        let user = BTreeMap::new();
        let r = resolve_natural_id("nonexistent_provider_resource.foo", &user);
        assert!(r.is_none());
    }

    #[test]
    fn parse_planned_attrs_extracts_after() {
        let json = r#"{
            "resource_changes": [
                {
                    "address": "github_repository.foo",
                    "change": { "after": { "name": "foo", "private": false } }
                },
                {
                    "address": "github_repository.bar",
                    "change": { "after": { "name": "bar" } }
                }
            ]
        }"#;
        let m = parse_planned_attrs(json);
        assert_eq!(m.len(), 2);
        assert_eq!(m["github_repository.foo"]["name"].as_str(), Some("foo"));
        assert_eq!(m["github_repository.bar"]["name"].as_str(), Some("bar"));
    }

    #[test]
    fn parse_planned_attrs_returns_empty_on_garbage() {
        let m = parse_planned_attrs("not json at all");
        assert!(m.is_empty());
    }

    #[test]
    fn parse_planned_attrs_returns_empty_when_no_resource_changes() {
        let m = parse_planned_attrs(r#"{"format_version":"1.2"}"#);
        assert!(m.is_empty());
    }

    #[test]
    fn substitute_planned_attr() {
        let planned = serde_json::json!({"name": "my-repo", "private": false});
        let vars = BTreeMap::new();
        let out = substitute_with_planned(
            "{{ .planned.name }}",
            &planned,
            &vars,
        )
        .unwrap();
        assert_eq!(out, "my-repo");
    }

    #[test]
    fn substitute_combines_planned_and_var() {
        let planned = serde_json::json!({"id": "abc123"});
        let mut vars = BTreeMap::new();
        vars.insert("zone_id".to_string(), serde_json::Value::String("zone1".into()));
        let out = substitute_with_planned(
            "{{ .zone_id }}/{{ .planned.id }}",
            &planned,
            &vars,
        )
        .unwrap();
        assert_eq!(out, "zone1/abc123");
    }

    #[test]
    fn substitute_returns_err_for_missing_planned() {
        let planned = serde_json::json!({});
        let vars = BTreeMap::new();
        let err = substitute_with_planned("{{ .planned.nope }}", &planned, &vars).unwrap_err();
        assert_eq!(err, "planned.nope");
    }

    #[test]
    fn substitute_handles_repository_id_pattern() {
        let planned = serde_json::json!({
            "repository_id": "my-repo",
            "pattern": "main"
        });
        let vars = BTreeMap::new();
        let out = substitute_with_planned(
            "{{ .planned.repository_id }}:{{ .planned.pattern }}",
            &planned,
            &vars,
        )
        .unwrap();
        assert_eq!(out, "my-repo:main");
    }

    #[test]
    fn substitute_string_coerces_numeric() {
        let planned = serde_json::json!({"id": 42});
        let vars = BTreeMap::new();
        let out = substitute_with_planned("{{ .planned.id }}", &planned, &vars).unwrap();
        assert_eq!(out, "42");
    }
}
