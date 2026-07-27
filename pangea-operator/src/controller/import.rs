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

use crate::executor::plan_change::{PlanAction, PlannedChange, ResourceKindClass};

/// A resolved import target: an address to adopt via `import`, the
/// substituted import id, and the resolution source (`"hint"` for a
/// per-address `importHints` entry, `"auto"` for a `naturalIds` / bundled
/// default). Consumed by the prepass to dispatch `try_import`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ImportTarget {
    pub address: String,
    pub id: String,
    pub source: String,
}

/// A create-action that could NOT be resolved to an import id. Carried
/// out of the pure resolver so the async caller emits the matching typed
/// event / warning — keeping [`resolve_import_targets`] ControllerState-
/// free (the "pure core + injectable seam" delivery method).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ImportSkip {
    /// A per-address `importHints` entry referenced an unset variable.
    Hint { address: String, missing: String },
    /// An auto-import `naturalIds` substitution failed. `server_assigned`
    /// flags the canonical null-on-create attributes (`planned.id` /
    /// `planned.arn` / `planned.self_link`) so the caller can emit the
    /// actionable "use spec.importHints" suggestion.
    Auto {
        address: String,
        template: String,
        missing: String,
        server_assigned: bool,
    },
}

/// Split an executor's typed `planned_changes()` output into the two
/// inputs import discovery needs: the create-action addresses (`Managed`
/// resources only — never a Data source / Output / Local value) and the
/// per-address planned `after` attrs.
///
/// This is the **magma-native, disk-free** replacement for
/// `extract_create_addresses_from_plan` + `parse_planned_attrs` — no
/// stringly tofu-`show -json` channel, no `tokio::fs::read`. A `None`
/// planned `after` maps to `serde_json::Value::Null`, which
/// [`substitute_with_planned`] treats identically to a missing attribute
/// (preserving the server-assigned-null semantics).
pub fn discovery_from_planned_changes(
    changes: &[PlannedChange],
) -> (Vec<String>, BTreeMap<String, serde_json::Value>) {
    let mut creates = Vec::new();
    let mut planned = BTreeMap::new();
    for c in changes {
        if c.action == PlanAction::Create && c.kind == ResourceKindClass::Managed {
            creates.push(c.address.clone());
            planned.insert(
                c.address.clone(),
                c.after.clone().unwrap_or(serde_json::Value::Null),
            );
        }
    }
    (creates, planned)
}

/// The pure core of the import prepass.
///
/// Given the create-action addresses + their planned `after` attrs
/// (produced EITHER from an executor's typed `planned_changes()` on the
/// magma path via [`discovery_from_planned_changes`], OR from the legacy
/// tofu `show -json` parse via `extract_create_addresses_from_plan` +
/// `parse_planned_attrs`), resolve every address to an [`ImportTarget`]
/// via the three-layer cascade:
///
///   1. `spec.importHints` — per-address override (highest priority).
///   2. `spec.importPolicy.naturalIds` — per-resource-type templates.
///   3. Operator-bundled defaults ([`bundled_natural_ids`]).
///
/// Layers 2/3 only fire when `import_policy.auto_on_conflict` is `true`.
/// ControllerState-free, executor-free, synchronous — the testable core.
/// Returns the resolved targets + the unresolved skips (for the caller to
/// surface as typed events).
pub fn resolve_import_targets(
    create_addresses: &[String],
    planned_by_addr: &BTreeMap<String, serde_json::Value>,
    import_hints: &BTreeMap<String, String>,
    import_policy: Option<&crate::crd::ImportPolicy>,
    variables: &BTreeMap<String, serde_json::Value>,
) -> (Vec<ImportTarget>, Vec<ImportSkip>) {
    use crate::controller::template_controller::substitute_import_id;
    use std::collections::HashSet;

    let auto_import = import_policy.map(|p| p.auto_on_conflict).unwrap_or(false);
    let create_set: HashSet<&str> = create_addresses.iter().map(String::as_str).collect();

    let mut targets: Vec<ImportTarget> = Vec::new();
    let mut skips: Vec<ImportSkip> = Vec::new();
    let mut covered: HashSet<String> = HashSet::new();

    // Layer 1: per-address importHints (highest priority — the user
    // explicitly named these resources).
    for (addr, id_template) in import_hints {
        if !create_set.contains(addr.as_str()) {
            continue;
        }
        match substitute_import_id(id_template, variables) {
            Ok(id) => {
                targets.push(ImportTarget {
                    address: addr.clone(),
                    id,
                    source: "hint".to_string(),
                });
                covered.insert(addr.clone());
            }
            Err(missing) => skips.push(ImportSkip::Hint {
                address: addr.clone(),
                missing,
            }),
        }
    }

    // Layers 2 + 3: naturalIds / bundled defaults for every create-action
    // not already covered by an explicit hint. Only when autoOnConflict.
    if auto_import {
        let user_natural_ids = import_policy
            .map(|p| p.natural_ids.clone())
            .unwrap_or_default();
        for addr in create_addresses {
            if covered.contains(addr) {
                continue;
            }
            let id_template = match resolve_natural_id(addr, &user_natural_ids) {
                Some(t) => t,
                None => continue,
            };
            let planned_attrs = planned_by_addr
                .get(addr)
                .cloned()
                .unwrap_or(serde_json::Value::Null);
            match substitute_with_planned(&id_template, &planned_attrs, variables) {
                Ok(id) => targets.push(ImportTarget {
                    address: addr.clone(),
                    id,
                    source: "auto".to_string(),
                }),
                Err(missing) => {
                    let server_assigned = matches!(
                        missing.as_str(),
                        "planned.id" | "planned.arn" | "planned.self_link"
                    );
                    skips.push(ImportSkip::Auto {
                        address: addr.clone(),
                        template: id_template,
                        missing,
                        server_assigned,
                    });
                }
            }
        }
    }

    (targets, skips)
}

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

    // ── P1a: magma-native, disk-free import discovery ───────────────
    //
    // These lock in the fix for the breathe class: under the disk-free
    // magma path, a create-that-exists must be IMPORT-discovered (adopted)
    // rather than blindly re-created (422 → cycle Failed). The proof runs
    // WITHOUT ControllerState / PgPool / network — the pure resolver + the
    // typed `planned_changes()` seam are the testable core.

    use crate::crd::ImportPolicy;
    use crate::executor::plan_change::{PlanAction, PlannedChange, ResourceKindClass};

    fn auto_policy() -> ImportPolicy {
        ImportPolicy {
            auto_on_conflict: true,
            ..Default::default()
        }
    }

    #[test]
    fn discovery_from_planned_changes_filters_managed_creates() {
        let changes = vec![
            PlannedChange {
                address: "github_repository.breathe".into(),
                action: PlanAction::Create,
                kind: ResourceKindClass::Managed,
                after: Some(serde_json::json!({ "name": "breathe" })),
            },
            // A managed NON-create — excluded.
            PlannedChange {
                address: "github_repository.existing".into(),
                action: PlanAction::NoOp,
                kind: ResourceKindClass::Managed,
                after: Some(serde_json::json!({ "name": "existing" })),
            },
            // A data source create-ish read — excluded (not Managed).
            PlannedChange {
                address: "github_user.me".into(),
                action: PlanAction::Read,
                kind: ResourceKindClass::Data,
                after: Some(serde_json::json!({ "login": "me" })),
            },
        ];
        let (creates, planned) = discovery_from_planned_changes(&changes);
        assert_eq!(creates, vec!["github_repository.breathe".to_string()]);
        assert_eq!(
            planned["github_repository.breathe"]["name"].as_str(),
            Some("breathe")
        );
        assert!(!planned.contains_key("github_user.me"));
    }

    #[test]
    fn create_that_exists_github_repo_resolves_to_import_target() {
        // The core assertion: a `github_repository.breathe` Create whose
        // planned `after.name == "breathe"` resolves to an import target
        // with id "breathe" (bundled `{{ .planned.name }}`), source "auto".
        let creates = vec!["github_repository.breathe".to_string()];
        let mut planned = BTreeMap::new();
        planned.insert(
            "github_repository.breathe".to_string(),
            serde_json::json!({ "name": "breathe" }),
        );
        let policy = auto_policy();
        let (targets, skips) = resolve_import_targets(
            &creates,
            &planned,
            &BTreeMap::new(),
            Some(&policy),
            &BTreeMap::new(),
        );
        assert!(skips.is_empty(), "no unresolved skips expected: {skips:?}");
        assert_eq!(
            targets,
            vec![ImportTarget {
                address: "github_repository.breathe".into(),
                id: "breathe".into(),
                source: "auto".into(),
            }]
        );
    }

    #[test]
    fn no_auto_import_resolves_nothing_without_hints() {
        // autoOnConflict=false + no hints → zero targets (today's
        // importHints-only behavior preserved).
        let creates = vec!["github_repository.breathe".to_string()];
        let mut planned = BTreeMap::new();
        planned.insert(
            "github_repository.breathe".to_string(),
            serde_json::json!({ "name": "breathe" }),
        );
        let (targets, _skips) = resolve_import_targets(
            &creates,
            &planned,
            &BTreeMap::new(),
            Some(&ImportPolicy::default()),
            &BTreeMap::new(),
        );
        assert!(targets.is_empty());
    }

    #[tokio::test]
    async fn magma_disk_free_planned_changes_import_discovers_create_that_exists() {
        // End-to-end over the injectable executor seam: the RecordingExecutor
        // returns a typed plan via `planned_changes()` (the magma DB-backed,
        // disk-free path) — NO show_plan, NO plan file, NO tofu-format parse.
        // discovery + resolution then IMPORT-discover the create-that-exists.
        use crate::executor::iac_executor::IacExecutor;
        use crate::executor::recording::RecordingExecutor;

        let exec = RecordingExecutor::new();
        exec.set_planned_changes(vec![PlannedChange {
            address: "github_repository.breathe".into(),
            action: PlanAction::Create,
            kind: ResourceKindClass::Managed,
            after: Some(serde_json::json!({ "name": "breathe" })),
        }]);

        // The prepass's first move: the typed, disk-free readback.
        let changes = exec
            .planned_changes()
            .await
            .unwrap()
            .expect("recording executor returns Some typed changes");

        // No `show_plan` was consulted — the disk-free path.
        assert!(
            !exec.recorded_calls().iter().any(|c| c.command == "show"),
            "planned_changes must not touch show_plan (disk-free)"
        );

        let (creates, planned) = discovery_from_planned_changes(&changes);
        let policy = auto_policy();
        let (targets, _skips) = resolve_import_targets(
            &creates,
            &planned,
            &BTreeMap::new(),
            Some(&policy),
            &BTreeMap::new(),
        );

        // The create-that-exists is discovered FOR IMPORT (id "breathe"),
        // not left to be blindly create-failed.
        assert_eq!(
            targets,
            vec![ImportTarget {
                address: "github_repository.breathe".into(),
                id: "breathe".into(),
                source: "auto".into(),
            }]
        );
    }
}
