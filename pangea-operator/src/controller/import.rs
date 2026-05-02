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
/// **Contract**: the only attributes available for substitution are
/// those present in the plan's `change.after` block — i.e. attributes
/// the user (or workspace DSL) declares. Attributes that are
/// **server-assigned** by the cloud provider (e.g. cloudflare's
/// `record.id`, AWS IAM policy `arn`, GCP `self_link`, etc.) are NOT
/// available on a `create`-action plan because the resource doesn't
/// exist yet. Templates that reference such attributes are
/// fundamentally unworkable as bundled defaults; they need explicit
/// `spec.importHints` per-address with the actually-known ID, OR a
/// future API-lookup mechanism.
///
/// What MUST NOT go in this map:
///   - `{{ .planned.id }}` — server-assigned for most providers
///   - `{{ .planned.arn }}` — server-assigned for AWS resources
///   - any attribute the provider creates on POST
///
/// What CAN go in this map:
///   - User-declared natural keys: `name`, `slug`, `bucket`, etc.
///   - Composite keys built from user-declared attributes:
///     `{{ .planned.repository }}:{{ .planned.name }}`
///   - Attributes that are upstream-known via `spec.variables` (e.g.
///     the user passes a known zone_id; combine via importHints
///     rather than bundled defaults).
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
/// auto-import support across multiple templates AND have a
/// user-declared natural key. One-off cases should use
/// `spec.importPolicy.naturalIds` per-template, or `spec.importHints`
/// per-address with the known cloud-side ID.
pub fn bundled_natural_ids() -> BTreeMap<&'static str, &'static str> {
    let mut m = BTreeMap::new();

    // GitHub provider — every entry below uses user-declared keys
    // (name, slug, repository, etc.) that ARE present on the plan's
    // change.after block.
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

    // AWS provider — name/bucket are user-declared. aws_iam_policy
    // intentionally omitted: import requires the ARN, which is
    // server-assigned. Use `spec.importHints` with the known ARN.
    m.insert("aws_iam_role", "{{ .planned.name }}");
    m.insert("aws_iam_user", "{{ .planned.name }}");
    m.insert("aws_s3_bucket", "{{ .planned.bucket }}");

    // Cloudflare provider — intentionally has zero entries today.
    //
    // Every cloudflare resource that is import-shaped uses
    // `<account_id>/<resource_id>` or `<zone_id>/<resource_id>` where
    // the resource_id is **server-assigned**. There's no way to
    // recover that ID from the plan's change.after block — the value
    // is null until the cloud-side resource is created.
    //
    // Workaround for cloudflare resources that already exist in the
    // cloud and need adopting: declare per-address importHints with
    // the actual ID, e.g.
    //
    //   spec:
    //     importHints:
    //       cloudflare_dns_record.foo: "{{ zone_id }}/abc123def456"
    //       cloudflare_zero_trust_tunnel_cloudflared.rio:
    //         "{{ account_id }}/9876fedcba00"
    //
    // where the resource_id portion is looked up via the cloudflare
    // API or `tofu import` once and recorded.
    //
    // This was the source of the cycle-166 fail=20 incident on rio
    // (2026-05-02): the bundled defaults claimed cloudflare auto-
    // import worked, but every substitution failed at runtime
    // because planned.id was always null on create-actions. The
    // entries are removed; the contract is now honest.

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

    // ── 2026-05 contract tests for the bundled_natural_ids surface ──
    //
    // These assertions encode the rule "no bundled default may
    // reference a server-assigned attribute". They lock in the fix
    // for the cycle-166 fail=20 incident on rio (cloudflare entries
    // referenced planned.id, which is null on create-actions, so
    // every substitution failed at runtime).

    #[test]
    fn bundled_excludes_server_assigned_cloudflare_resources() {
        // Cloudflare resources whose import requires `<scope>/<id>`
        // where `id` is server-assigned. The ONLY way to import them
        // is via spec.importHints with the known cloud-side ID.
        let b = bundled_natural_ids();
        assert!(
            !b.contains_key("cloudflare_dns_record"),
            "cloudflare_dns_record must NOT be in bundled_natural_ids — \
             planned.id is null on create-actions. Use spec.importHints."
        );
        assert!(
            !b.contains_key("cloudflare_zero_trust_tunnel_cloudflared"),
            "cloudflare_zero_trust_tunnel_cloudflared must NOT be in \
             bundled_natural_ids — planned.id is null on create-actions. \
             Use spec.importHints."
        );
    }

    #[test]
    fn bundled_excludes_server_assigned_aws_resources() {
        let b = bundled_natural_ids();
        assert!(
            !b.contains_key("aws_iam_policy"),
            "aws_iam_policy must NOT be in bundled_natural_ids — \
             planned.arn is null on create-actions. Use spec.importHints."
        );
    }

    /// No bundled default may textually reference `.planned.id`,
    /// `.planned.arn`, or `.planned.self_link` — those are the canonical
    /// server-assigned attributes that are ALWAYS null on create-action
    /// plans. Adding such a template is a substrate-level error.
    #[test]
    fn bundled_natural_ids_have_no_server_assigned_references() {
        let forbidden_tokens = [".planned.id", ".planned.arn", ".planned.self_link"];
        for (resource_type, template) in bundled_natural_ids().iter() {
            for forbidden in &forbidden_tokens {
                assert!(
                    !template.contains(forbidden),
                    "bundled_natural_ids[{}] = {:?} references server-assigned \
                     attribute {:?}. This is unworkable for auto-import: \
                     server-assigned attrs are null on create-action plans. \
                     Either pick a user-declared natural key, or remove the \
                     entry and document spec.importHints as the workaround.",
                    resource_type,
                    template,
                    forbidden,
                );
            }
        }
    }

    #[test]
    fn substitute_returns_planned_id_marker_for_server_assigned() {
        // Reproducer for the cycle-166 incident shape: a bundled
        // template references planned.id, the plan's change.after
        // doesn't have an id field (because it's a create-action),
        // substitute returns Err("planned.id"). Caller (template_
        // controller) reads that string and produces the
        // server-assigned-attribute warning.
        let planned = serde_json::json!({"account_id": "acct-1"});
        let vars = BTreeMap::new();
        let err = substitute_with_planned(
            "{{ .planned.account_id }}/{{ .planned.id }}",
            &planned,
            &vars,
        )
        .unwrap_err();
        assert_eq!(
            err, "planned.id",
            "expected the substitution failure to name 'planned.id' so \
             the controller can detect it as server-assigned"
        );
    }

    #[test]
    fn user_natural_id_overrides_for_cloudflare_resource() {
        // Even though cloudflare resources are NOT in bundled, a
        // user can still supply a per-template naturalIds entry
        // that uses `{{ .var }}` from spec.variables — which CAN
        // resolve at plan-time because variables are upstream-known.
        let mut user = BTreeMap::new();
        user.insert(
            "cloudflare_dns_record".to_string(),
            "{{ .zone_id }}/{{ .record_id }}".to_string(),
        );
        let resolved = resolve_natural_id("cloudflare_dns_record.foo", &user);
        assert_eq!(
            resolved.as_deref(),
            Some("{{ .zone_id }}/{{ .record_id }}"),
            "user naturalIds must override the (now-missing) bundled default"
        );

        // And the substitution succeeds when both vars are provided.
        let planned = serde_json::json!({});
        let mut vars = BTreeMap::new();
        vars.insert("zone_id".to_string(), serde_json::Value::String("zone1".into()));
        vars.insert("record_id".to_string(), serde_json::Value::String("rec123".into()));
        let id = substitute_with_planned(
            &resolved.unwrap(),
            &planned,
            &vars,
        )
        .unwrap();
        assert_eq!(id, "zone1/rec123");
    }

    #[test]
    fn parse_planned_attrs_omits_server_assigned_id_on_create_action() {
        // Reproducer of the actual rio plan shape: cloudflare resource
        // create-action with id=null in change.after.
        let plan_json = r#"{
            "resource_changes": [
                {
                    "address": "cloudflare_zero_trust_tunnel_cloudflared.rio-tunnel",
                    "change": {
                        "actions": ["create"],
                        "after": {
                            "account_id": "acct-1",
                            "name": "rio-tunnel",
                            "id": null
                        }
                    }
                }
            ]
        }"#;
        let m = parse_planned_attrs(plan_json);
        let attrs = &m["cloudflare_zero_trust_tunnel_cloudflared.rio-tunnel"];

        // The ID field IS present in JSON but holds null. substitute_with_planned
        // returns Err for null values (treats them as missing). Verify this
        // is the actual observed behavior.
        let vars = BTreeMap::new();
        let err = substitute_with_planned(
            "{{ .planned.account_id }}/{{ .planned.id }}",
            attrs,
            &vars,
        )
        .unwrap_err();
        assert_eq!(
            err, "planned.id",
            "null value in plan must be treated identically to missing \
             attribute — both indicate server-assigned-ness on create"
        );
    }
}
