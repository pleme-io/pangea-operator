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
//!   3. The EXECUTOR's own catalog-derived id, carried in on
//!      `PlannedChange::import_id` — magma's single
//!      `natural_id::CATALOG` evaluated against the workspace state.
//!      The operator no longer authors an import-id table of its own.
//!
//! Substitution uses the same `{{ .var }}` / `{{ var }}` syntax as
//! `importHints`, with one extension: `{{ .planned.<attr> }}` reads
//! from the plan's `change.after` block for the resource (i.e. the
//! attributes the apply WOULD have written if the resource didn't
//! already exist).

use std::collections::BTreeMap;

use crate::executor::plan_change::{DerivedImportId, PlanAction, PlannedChange, ResourceKindClass};

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
    /// The executor derived an import id it does NOT stand behind — a
    /// convention-guessed parent name, or a last-resort address name for a
    /// type with no catalog rule and no `name` attribute.
    ///
    /// Refused rather than dispatched. A successful import against a wrong
    /// id does not fail: it adopts a DIFFERENT real resource under the
    /// planned address, and the next cycle diffs config against it. Losing
    /// an adoption costs a cycle; a wrong one costs the resource.
    Inexact { address: String, id: String },
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
/// Also carries out the executor's own per-address [`DerivedImportId`],
/// which is the ONLY id in this pipeline that can name a parent by its real
/// name — see [`PlannedChange::import_id`].
pub fn discovery_from_planned_changes(
    changes: &[PlannedChange],
) -> (
    Vec<String>,
    BTreeMap<String, serde_json::Value>,
    BTreeMap<String, DerivedImportId>,
) {
    let mut creates = Vec::new();
    let mut planned = BTreeMap::new();
    let mut derived = BTreeMap::new();
    for c in changes {
        if c.action == PlanAction::Create && c.kind == ResourceKindClass::Managed {
            creates.push(c.address.clone());
            planned.insert(
                c.address.clone(),
                c.after.clone().unwrap_or(serde_json::Value::Null),
            );
            if let Some(d) = &c.import_id {
                derived.insert(c.address.clone(), d.clone());
            }
        }
    }
    (creates, planned, derived)
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
///   3. The executor's own [`DerivedImportId`] — magma's one import-id
///      catalog, evaluated against the workspace's state.
///
/// Layers 2/3 only fire when `import_policy.auto_on_conflict` is `true`.
/// ControllerState-free, executor-free, synchronous — the testable core.
/// Returns the resolved targets + the unresolved skips (for the caller to
/// surface as typed events).
///
/// # Why layer 3 is a value and not a template
///
/// It used to be `bundled_natural_ids()` — an operator-local copy of
/// magma's import-id table rendered through `{{ .planned.<attr> }}`
/// substitution over the plan's `after`. Both halves were wrong. The table
/// was a SECOND authored copy of knowledge magma already owns
/// (`magma_apply::natural_id::CATALOG`), so the two could and did disagree.
/// And the substitution was a raw passthrough: `.planned.repository` on a
/// `github_issue_label` create is the literal string
/// `"${github_repository.banken.name}"`, so the rendered id was
/// `${github_repository.banken.name}:bug`, which URL-encodes into a GET
/// that 404s for every single label. Nothing was ever adopted, so the
/// creates collided every cycle, forever.
///
/// The template layer cannot be fixed in place, because the missing datum
/// is not in `after` at all — it is in the workspace's STATE, which this
/// pure function has no access to and should not acquire. So layer 3 now
/// receives an already-derived id from the one component that holds both
/// the plan and the state: the executor.
pub fn resolve_import_targets(
    create_addresses: &[String],
    planned_by_addr: &BTreeMap<String, serde_json::Value>,
    derived_by_addr: &BTreeMap<String, DerivedImportId>,
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

    // Layers 2 + 3: the user's declared naturalIds, else the executor's
    // catalog-derived id, for every create-action not already covered by an
    // explicit hint. Only when autoOnConflict.
    //
    // The user layer stays FIRST: an operator who authored a naturalIds
    // template for a type said something the catalog does not know, and a
    // derived default must not silently outrank it.
    if auto_import {
        let user_natural_ids = import_policy
            .map(|p| p.natural_ids.clone())
            .unwrap_or_default();
        for addr in create_addresses {
            if covered.contains(addr) {
                continue;
            }
            let Some(id_template) = resolve_natural_id(addr, &user_natural_ids) else {
                // Layer 3: no user template — take the executor's derived id,
                // and take it ONLY if the executor stands behind it.
                match derived_by_addr.get(addr) {
                    Some(d) if d.exact => targets.push(ImportTarget {
                        address: addr.clone(),
                        id: d.id.clone(),
                        source: "auto".to_string(),
                    }),
                    Some(d) => skips.push(ImportSkip::Inexact {
                        address: addr.clone(),
                        id: d.id.clone(),
                    }),
                    None => {}
                }
                continue;
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

/// **DELETED — the operator no longer carries an import-id table.**
///
/// `bundled_natural_ids()` lived here and was a second authored copy of
/// `magma_apply::natural_id::CATALOG`. Two copies of one table drift, and
/// these two did: magma's rule for `github_branch_protection` keys on the
/// parent repository's NAME, while this copy emitted the raw
/// `${github_repository.<n>.node_id}` reference. Worse, every rule here was
/// rendered by `substitute_with_planned`, whose `{{ .planned.<attr> }}`
/// lookup is a raw passthrough over the plan's `after` — so a rule naming a
/// parent produced the literal `${…}` text as part of the import id.
///
/// The knowledge now lives in exactly one place (magma's catalog), is
/// evaluated where the state that completes it lives (the executor), and
/// arrives here as a value: [`PlannedChange::import_id`].
///
/// A per-resource-type template a consumer authors themselves is unaffected
/// — that is `spec.importPolicy.naturalIds`, still resolved by
/// [`resolve_natural_id`].
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

    // No layer 3 here any more: the catalog-derived default is a VALUE the
    // executor computes (`PlannedChange::import_id`), not a template this
    // function can render, because completing it needs the workspace state.
    let _ = resource_type;
    None
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
pub fn parse_planned_attrs(plan_json: &str) -> BTreeMap<String, serde_json::Value> {
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

    /// THE structural invariant this module now holds: the operator owns NO
    /// import-id table. Every type the old `bundled_natural_ids()` covered
    /// must resolve to nothing here, so the only possible source of a
    /// catalog-shaped id is the executor's `PlannedChange::import_id`
    /// (magma's `natural_id::CATALOG`). Re-introducing a local table — for
    /// any type, however innocuous — re-opens the drift this replaced.
    #[test]
    fn the_operator_carries_no_import_id_table_of_its_own() {
        let none = BTreeMap::new();
        for addr in [
            "github_repository.x",
            "github_branch_protection.x",
            "github_issue_label.x",
            "github_actions_secret.x",
            "github_team.x",
            "aws_iam_role.x",
            "aws_s3_bucket.x",
        ] {
            assert_eq!(
                resolve_natural_id(addr, &none),
                None,
                "{addr} resolved to an operator-local template; the table must live \
                 only in magma_apply::natural_id::CATALOG"
            );
        }
    }

    #[test]
    fn resolve_user_natural_id_is_the_only_template_layer() {
        let mut user = BTreeMap::new();
        user.insert(
            "github_repository".to_string(),
            "custom-{{ .planned.name }}".to_string(),
        );
        let r = resolve_natural_id("github_repository.foo", &user);
        assert_eq!(r.as_deref(), Some("custom-{{ .planned.name }}"));
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
        let out = substitute_with_planned("{{ .planned.name }}", &planned, &vars).unwrap();
        assert_eq!(out, "my-repo");
    }

    #[test]
    fn substitute_combines_planned_and_var() {
        let planned = serde_json::json!({"id": "abc123"});
        let mut vars = BTreeMap::new();
        vars.insert(
            "zone_id".to_string(),
            serde_json::Value::String("zone1".into()),
        );
        let out =
            substitute_with_planned("{{ .zone_id }}/{{ .planned.id }}", &planned, &vars).unwrap();
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

    // ── 2026-05 contract, re-homed ──────────────────────────────────
    //
    // The rule "no bundled default may reference a server-assigned
    // attribute" (the cycle-166 fail=20 incident on rio: cloudflare rows
    // referenced `planned.id`, null on every create-action) is no longer
    // enforceable here, because there is no bundled table here. It moved
    // WITH the table: `magma_apply::natural_id::derive` returns `None` for
    // a catalog rule whose components are absent, and `Confidence` marks
    // anything it had to guess. The operator-side half of that contract is
    // the refusal below — an id the executor does not stand behind is
    // never dispatched.

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
        vars.insert(
            "zone_id".to_string(),
            serde_json::Value::String("zone1".into()),
        );
        vars.insert(
            "record_id".to_string(),
            serde_json::Value::String("rec123".into()),
        );
        let id = substitute_with_planned(&resolved.unwrap(), &planned, &vars).unwrap();
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
        let err =
            substitute_with_planned("{{ .planned.account_id }}/{{ .planned.id }}", attrs, &vars)
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

    /// The magma executor derives this from the catalog + state. In these
    /// pure-core tests it is supplied directly — the border is a value.
    fn exact(id: &str) -> DerivedImportId {
        DerivedImportId {
            id: id.into(),
            exact: true,
        }
    }
    fn inexact(id: &str) -> DerivedImportId {
        DerivedImportId {
            id: id.into(),
            exact: false,
        }
    }
    fn derived(pairs: &[(&str, DerivedImportId)]) -> BTreeMap<String, DerivedImportId> {
        pairs
            .iter()
            .map(|(a, d)| ((*a).to_string(), d.clone()))
            .collect()
    }

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
                import_id: Some(exact("breathe")),
            },
            // A managed NON-create — excluded.
            PlannedChange {
                address: "github_repository.existing".into(),
                action: PlanAction::NoOp,
                kind: ResourceKindClass::Managed,
                after: Some(serde_json::json!({ "name": "existing" })),
                import_id: None,
            },
            // A data source create-ish read — excluded (not Managed).
            PlannedChange {
                address: "github_user.me".into(),
                action: PlanAction::Read,
                kind: ResourceKindClass::Data,
                after: Some(serde_json::json!({ "login": "me" })),
                import_id: None,
            },
        ];
        let (creates, planned, derived) = discovery_from_planned_changes(&changes);
        assert_eq!(creates, vec!["github_repository.breathe".to_string()]);
        assert_eq!(derived["github_repository.breathe"], exact("breathe"));
        assert!(!derived.contains_key("github_repository.existing"));
        assert_eq!(
            planned["github_repository.breathe"]["name"].as_str(),
            Some("breathe")
        );
        assert!(!planned.contains_key("github_user.me"));
    }

    #[test]
    fn create_that_exists_github_repo_resolves_to_import_target() {
        // The core assertion: a `github_repository.breathe` Create resolves
        // to an import target with id "breathe", source "auto". The id now
        // arrives as a VALUE the executor derived (magma's catalog: a
        // name-keyed type imports by its `name`), not as a template this
        // module renders.
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
            &derived(&[("github_repository.breathe", exact("breathe"))]),
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

    /// The end-to-end shape of the 49 failing labels, through the pure
    /// resolver: the executor hands in `banken:bug`, and the prepass
    /// dispatches exactly that — no template, no `${…}` residue.
    #[test]
    fn a_derived_repo_scoped_id_is_dispatched_verbatim() {
        let addr = "github_issue_label.banken-label-bug";
        let creates = vec![addr.to_string()];
        let mut planned = BTreeMap::new();
        planned.insert(
            addr.to_string(),
            serde_json::json!({
                "repository": "${github_repository.banken.name}",
                "name": "bug"
            }),
        );
        let policy = auto_policy();
        let (targets, skips) = resolve_import_targets(
            &creates,
            &planned,
            &derived(&[(addr, exact("banken:bug"))]),
            &BTreeMap::new(),
            Some(&policy),
            &BTreeMap::new(),
        );
        assert!(skips.is_empty(), "{skips:?}");
        assert_eq!(
            targets,
            vec![ImportTarget {
                address: addr.into(),
                id: "banken:bug".into(),
                source: "auto".into(),
            }]
        );
    }

    /// THE refusal gate. An id the executor does not stand behind is never
    /// dispatched — a successful import against a wrong id adopts a
    /// different real resource under the planned address.
    #[test]
    fn an_inexact_derived_id_is_refused_never_dispatched() {
        let addr = "github_issue_label.tag-forge-label-bug";
        let creates = vec![addr.to_string()];
        let policy = auto_policy();
        let (targets, skips) = resolve_import_targets(
            &creates,
            &BTreeMap::new(),
            // `tag_forge` is the RESOURCE name; the repo is `tag-forge`.
            &derived(&[(addr, inexact("tag_forge:bug"))]),
            &BTreeMap::new(),
            Some(&policy),
            &BTreeMap::new(),
        );
        assert!(
            targets.is_empty(),
            "an inexact id must not become an import target: {targets:?}"
        );
        assert_eq!(
            skips,
            vec![ImportSkip::Inexact {
                address: addr.into(),
                id: "tag_forge:bug".into(),
            }]
        );
    }

    /// A user-authored `naturalIds` template still outranks the derived
    /// default — the operator said something the catalog does not know.
    #[test]
    fn a_user_natural_id_template_outranks_the_derived_default() {
        let addr = "github_repository.breathe";
        let creates = vec![addr.to_string()];
        let mut planned = BTreeMap::new();
        planned.insert(addr.to_string(), serde_json::json!({ "name": "breathe" }));
        let mut policy = auto_policy();
        policy.natural_ids.insert(
            "github_repository".to_string(),
            "custom-{{ .planned.name }}".to_string(),
        );
        let (targets, _) = resolve_import_targets(
            &creates,
            &planned,
            &derived(&[(addr, exact("breathe"))]),
            &BTreeMap::new(),
            Some(&policy),
            &BTreeMap::new(),
        );
        assert_eq!(targets[0].id, "custom-breathe");
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
            &derived(&[("github_repository.breathe", exact("breathe"))]),
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
            import_id: Some(exact("breathe")),
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

        let (creates, planned, derived) = discovery_from_planned_changes(&changes);
        let policy = auto_policy();
        let (targets, _skips) = resolve_import_targets(
            &creates,
            &planned,
            &derived,
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
