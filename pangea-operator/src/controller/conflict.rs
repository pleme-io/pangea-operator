//! Post-apply conflict resolution — the runtime half of the typed,
//! cascading `ConflictResolutionPolicy` (the policy *types* live in
//! `crd::infrastructure_template`; this module is the *logic* that
//! consumes them against live `tofu apply` output).
//!
//! ## Why this exists
//!
//! The pre-apply import sweep (`template_controller::run_import_prepass`)
//! adopts out-of-band resources *before* the apply, so a `create`-action
//! whose target already exists in the provider becomes an `import` +
//! no-op instead of a `422 already exists`. But the prepass is only as
//! good as the plan JSON it reads — if `tofu show -json` comes back
//! empty (historically: a huge single-line plan tripping the executor's
//! line-buffered read), the prepass resolves zero targets and the apply
//! 422-storms. That is exactly what stuck the `pleme-io-opensource`
//! GitHub posture for ~17 days (2026-05-08 → 2026-05-25): ~375
//! out-of-band `caixa-*` repos created behind the operator's back, the
//! prepass silently producing 0 imports, every apply dying on the first
//! `name already exists`.
//!
//! This module is the **convergence guarantee** that does not depend on
//! the prepass succeeding: when an apply fails, classify each provider
//! conflict, and for `import`-resolution conflicts adopt the resource
//! via `tofu import` then re-apply — up to `maxRounds`. It is the
//! "layers and layers of automated policy" answer: a sensible *general*
//! default (adopt `alreadyExists` / `alreadyProtected`) that fires with
//! no extra config, plus *specific* per-(resource-type, kind) override
//! rules authored on `spec.conflictPolicy`.
//!
//! The import-ID derivation reuses the same three-layer cascade as the
//! prepass (`importHints` → `importPolicy.naturalIds` → bundled
//! defaults) via `controller::import`, and sources the planned
//! attributes from a fresh `tofu show -json` when available, falling
//! back to the rendered `main.tf.json` config (literal values — robust
//! even when `show` is the broken path).

use std::collections::{BTreeMap, HashSet};
use std::path::Path;

use tracing::{info, warn};

use crate::controller::import::{parse_planned_attrs, resolve_natural_id, substitute_with_planned};
use crate::controller::ControllerState;
use crate::crd::{
    ConflictKind, ConflictResolution, ConflictResolutionPolicy, InfrastructureTemplate,
};
use crate::executor::TofuResult;

/// Outcome of the post-apply conflict-resolution pass.
pub struct ConflictResolutionOutcome {
    /// The apply result after the import + re-apply loop. If the loop
    /// converged this is a success; otherwise it carries the final
    /// (still-failing) apply result so the caller surfaces a real error.
    pub result: TofuResult,
    /// Addresses adopted into state via `tofu import` during the pass —
    /// merged into the cycle receipt's `imported` set by the caller.
    pub imported: Vec<String>,
    /// How many import+re-apply rounds ran.
    pub rounds: u32,
}

/// The effective conflict-resolution policy for a template: its
/// explicit `spec.conflictPolicy` if set, else the bundled general-case
/// default (adopt `alreadyExists` / `alreadyProtected`, fail otherwise).
pub fn effective_policy(template: &InfrastructureTemplate) -> ConflictResolutionPolicy {
    template
        .spec
        .conflict_policy
        .clone()
        .unwrap_or_else(ConflictResolutionPolicy::bundled_default)
}

/// Is post-apply conflict resolution active for this template?
///
/// Active when the policy's `enabled` is explicitly `Some(true)`, OR
/// when `enabled` is unset and `importPolicy.autoOnConflict` is true —
/// so the existing fleet templates (which already opt into auto-import)
/// get the convergence guarantee with **no spec change**.
pub fn is_enabled(template: &InfrastructureTemplate, policy: &ConflictResolutionPolicy) -> bool {
    let auto_on_conflict = template
        .spec
        .import_policy
        .as_ref()
        .map(|p| p.auto_on_conflict)
        .unwrap_or(false);
    policy.enabled.unwrap_or(auto_on_conflict)
}

/// Tofu resource type for an address (`github_repository.foo` →
/// `github_repository`). Mirrors `import::resolve_natural_id`.
fn resource_type_of(address: &str) -> &str {
    address.split('.').next().unwrap_or(address)
}

/// Classify a single tofu diagnostic block's text into a [`ConflictKind`].
fn classify_kind(error_text: &str) -> ConflictKind {
    let t = error_text.to_ascii_lowercase();
    if t.contains("already protected") || t.contains("protection already exists") {
        ConflictKind::AlreadyProtected
    } else if t.contains("already exists") || t.contains("name already")
        || t.contains("already been taken")
    {
        ConflictKind::AlreadyExists
    } else {
        ConflictKind::Other
    }
}

/// Strip OpenTofu's diagnostic frame from a line. Even under
/// `-no-color`, OpenTofu frames diagnostics with box-drawing chars
/// (`╷` open cap, `│` body, `╵` close cap), so a resource-address line
/// is rendered `│   with github_repository.foo,`. We strip a leading
/// frame char + surrounding whitespace so structural matching works on
/// both the framed (real CLI) and bare (test-string) forms. No-op when
/// no frame char is present.
fn strip_diag_frame(line: &str) -> &str {
    let l = line.trim_start();
    let l = l
        .strip_prefix('│')
        .or_else(|| l.strip_prefix('╷'))
        .or_else(|| l.strip_prefix('╵'))
        .unwrap_or(l);
    l.trim_start()
}

/// Parse a `tofu apply` failure's combined stdout+stderr into per-address
/// conflicts. Best-effort textual scan of OpenTofu's `-no-color`
/// diagnostic blocks: each block opens with `Error:` and (for a
/// resource-scoped error) carries a `  with <address>,` line. The error
/// message lines between the two are accumulated and classified. The
/// box-drawing diagnostic frame (`│`/`╷`/`╵`) is stripped per-line.
///
/// Returns `(address, kind)` for every resource-scoped error. Addresses
/// with no recognized conflict kind are returned as
/// [`ConflictKind::Other`] so the policy — not this parser — decides
/// whether to act.
pub fn classify_apply_conflicts(output: &str) -> Vec<(String, ConflictKind)> {
    let mut out = Vec::new();
    let mut in_error = false;
    let mut current = String::new();

    for raw in output.lines() {
        let line = strip_diag_frame(raw);
        if line.starts_with("Error:") {
            in_error = true;
            current.clear();
            current.push_str(line);
        } else if in_error && line.starts_with("with ") {
            // `with github_repository.caixa_crossbeam,` — strip the
            // keyword + trailing comma to recover the tofu address.
            let addr = line
                .trim_start_matches("with ")
                .trim_end_matches(',')
                .trim()
                .to_string();
            if !addr.is_empty() {
                out.push((addr, classify_kind(&current)));
            }
            in_error = false;
            current.clear();
        } else if in_error {
            current.push(' ');
            current.push_str(line);
        }
    }
    out
}

/// Parse the rendered `main.tf.json` config into per-address attribute
/// maps, shaped identically to `import::parse_planned_attrs` so the same
/// `substitute_with_planned` cascade consumes it.
///
/// Terraform JSON allows a resource block to be either an object
/// (`{"name": …}`) or a single-element array (`[{"name": …}]`); both are
/// handled. This is the fallback attribute source for import-ID
/// derivation when a fresh `tofu show -json` is unavailable — literal
/// config values (e.g. a repo `name`) resolve here even if the plan read
/// is broken. Note: attributes that are TF interpolations
/// (`${github_repository.foo.node_id}`) come through verbatim and won't
/// form a usable import ID — those need the plan path (resolved values).
pub fn config_attrs_from_main_tf(json: &str) -> BTreeMap<String, serde_json::Value> {
    let mut out = BTreeMap::new();
    let parsed: serde_json::Value = match serde_json::from_str(json) {
        Ok(v) => v,
        Err(_) => return out,
    };
    let resources = match parsed.get("resource").and_then(|v| v.as_object()) {
        Some(o) => o,
        None => return out,
    };
    for (rtype, by_label) in resources {
        let labels = match by_label.as_object() {
            Some(o) => o,
            None => continue,
        };
        for (label, block) in labels {
            let attrs = match block {
                serde_json::Value::Array(a) => a.first().cloned().unwrap_or(serde_json::Value::Null),
                other => other.clone(),
            };
            out.insert(format!("{rtype}.{label}"), attrs);
        }
    }
    out
}

/// Combine an apply result's stdout + stderr — OpenTofu writes most
/// diagnostics to stdout, so both must be scanned for conflicts.
fn combined_output(result: &TofuResult) -> String {
    if result.stdout.is_empty() {
        result.stderr.clone()
    } else if result.stderr.is_empty() {
        result.stdout.clone()
    } else {
        format!("{}\n{}", result.stdout, result.stderr)
    }
}

/// Gather per-address planned attributes for import-ID derivation.
/// Prefers a fresh `tofu plan` + `tofu show -json` (resolved values,
/// reliable now that the executor reads the full plan), and falls back
/// to the rendered `main.tf.json` config (literal values) when `show`
/// yields nothing.
async fn gather_attrs(
    template: &InfrastructureTemplate,
    state: &ControllerState,
    work_dir: &Path,
    plan_path: &Path,
    main_tf_path: &Path,
) -> BTreeMap<String, serde_json::Value> {
    let executor = state.executor_for(template);
    let _ = executor.plan(work_dir, Some(plan_path), &[]).await;
    let plan_json = match executor.show_plan(work_dir, plan_path).await {
        Ok(r) if r.success && !r.stdout.is_empty() => r.stdout,
        _ => String::new(),
    };
    if !plan_json.is_empty() {
        let m = parse_planned_attrs(&plan_json);
        if !m.is_empty() {
            return m;
        }
    }
    match tokio::fs::read_to_string(main_tf_path).await {
        Ok(content) => config_attrs_from_main_tf(&content),
        Err(_) => BTreeMap::new(),
    }
}

/// Run the post-apply conflict-resolution loop after a failed `tofu
/// apply`. Returns `None` when conflict resolution is disabled for this
/// template (caller proceeds with the original failure unchanged), or
/// `Some(outcome)` after running the import + re-apply loop.
///
/// `failed_result` is the apply result that just failed; the caller
/// passes the three workspace paths so this module needs no `Workspace`
/// dependency.
pub async fn resolve_conflicts_post_apply(
    template: &InfrastructureTemplate,
    state: &ControllerState,
    work_dir: &Path,
    plan_path: &Path,
    main_tf_path: &Path,
    failed_result: TofuResult,
) -> Option<ConflictResolutionOutcome> {
    let policy = effective_policy(template);
    if !is_enabled(template, &policy) {
        return None;
    }

    // Per-CR executor selection — a CR with `spec.executor: magma` runs
    // the conflict-resolution import + re-apply loop through MagmaExecutor;
    // everything else stays on the shared tofu executor. Mirrors the
    // selection every phase handler in template_controller now makes.
    let executor = state.executor_for(template);

    let max_rounds = policy.rounds();
    let variables = template.spec.variables.clone().unwrap_or_default();
    let user_natural_ids = template
        .spec
        .import_policy
        .as_ref()
        .map(|p| p.natural_ids.clone())
        .unwrap_or_default();

    let mut result = failed_result;
    let mut imported_all: Vec<String> = Vec::new();
    let mut tried: HashSet<String> = HashSet::new();
    let mut rounds_run = 0u32;

    for round in 0..max_rounds {
        let combined = combined_output(&result);
        let conflicts = classify_apply_conflicts(&combined);
        if conflicts.is_empty() {
            break;
        }

        // Select conflicts the policy resolves via Import that we haven't
        // already attempted (avoid re-importing on every round).
        let want: Vec<(String, ConflictKind)> = conflicts
            .into_iter()
            .filter(|(addr, kind)| {
                !tried.contains(addr)
                    && matches!(
                        policy.resolution_for(resource_type_of(addr), *kind),
                        ConflictResolution::Import
                    )
            })
            .collect();

        if want.is_empty() {
            // Conflicts exist but none are import-resolvable under the
            // policy (e.g. defaultResolution=Fail for Other). Stop and let
            // the caller surface the real failure — never silently succeed.
            break;
        }

        rounds_run = round + 1;
        info!(
            round = round + 1,
            candidates = want.len(),
            "conflict-resolution: adopting out-of-band resources via tofu import, then re-applying"
        );

        let attrs_by_addr = gather_attrs(template, state, work_dir, plan_path, main_tf_path).await;

        let mut imported_this_round = 0usize;
        for (addr, _kind) in &want {
            tried.insert(addr.clone());
            let id_template = match resolve_natural_id(addr, &user_natural_ids) {
                Some(t) => t,
                None => {
                    warn!(
                        address = %addr,
                        "conflict-resolution: no natural-id rule for resource type — cannot auto-import; \
                         add spec.importPolicy.naturalIds or spec.importHints"
                    );
                    continue;
                }
            };
            let attrs = attrs_by_addr
                .get(addr)
                .cloned()
                .unwrap_or(serde_json::Value::Null);
            let import_id = match substitute_with_planned(&id_template, &attrs, &variables) {
                Ok(id) => id,
                Err(missing) => {
                    warn!(
                        address = %addr,
                        template = %id_template,
                        missing = %missing,
                        "conflict-resolution: cannot derive import id (attribute unavailable) — skipping"
                    );
                    continue;
                }
            };
            match executor.import(work_dir, addr, &import_id).await {
                Ok(r) if r.success => {
                    imported_all.push(addr.clone());
                    imported_this_round += 1;
                    info!(address = %addr, import_id = %import_id, "conflict-resolution: adopted into state");
                }
                Ok(r) => {
                    warn!(address = %addr, stderr = %r.stderr, "conflict-resolution: tofu import failed");
                }
                Err(e) => {
                    warn!(address = %addr, error = %e, "conflict-resolution: tofu import errored");
                }
            }
        }

        if imported_this_round == 0 {
            // Every candidate failed to import — re-applying would just
            // re-fail identically. Stop.
            break;
        }

        // Re-apply with a fresh plan (plan_file=None) — the imports
        // mutated state, so any cached plan is stale.
        match executor.apply(work_dir, None, true).await {
            Ok(r) => result = r,
            Err(e) => {
                warn!(error = %e, "conflict-resolution: re-apply errored");
                break;
            }
        }

        if result.success {
            info!(
                rounds = round + 1,
                imported = imported_all.len(),
                "conflict-resolution: posture converged after import + re-apply"
            );
            break;
        }
    }

    Some(ConflictResolutionOutcome {
        result,
        imported: imported_all,
        rounds: rounds_run,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classifies_github_repo_already_exists() {
        // Realistic OpenTofu -no-color apply diagnostic for a github_repository 422.
        let output = "\
Error: POST https://api.github.com/orgs/pleme-io/repos: 422 Validation Failed [{Resource:Repository Field:name Code:custom Message:name already exists on this account}]

  with github_repository.caixa_crossbeam,
  on main.tf.json line 0, in resource.github_repository.caixa_crossbeam:

Error: POST https://api.github.com/orgs/pleme-io/repos: 422 Validation Failed [{Resource:Repository Field:name Code:custom Message:name already exists on this account}]

  with github_repository.maquina_engine,
  on main.tf.json line 0, in resource.github_repository.maquina_engine:
";
        let conflicts = classify_apply_conflicts(output);
        assert_eq!(conflicts.len(), 2);
        assert_eq!(conflicts[0].0, "github_repository.caixa_crossbeam");
        assert_eq!(conflicts[0].1, ConflictKind::AlreadyExists);
        assert_eq!(conflicts[1].0, "github_repository.maquina_engine");
        assert_eq!(conflicts[1].1, ConflictKind::AlreadyExists);
    }

    #[test]
    fn classifies_branch_protection_already_protected() {
        let output = "\
Error: PUT https://api.github.com/repos/pleme-io/foo/branches/main/protection: 422 [{Message:branch is already protected}]

  with github_branch_protection.foo_main,
  on main.tf.json line 0:
";
        let conflicts = classify_apply_conflicts(output);
        assert_eq!(conflicts.len(), 1);
        assert_eq!(conflicts[0].0, "github_branch_protection.foo_main");
        assert_eq!(conflicts[0].1, ConflictKind::AlreadyProtected);
    }

    #[test]
    fn unrecognized_error_is_other_not_dropped() {
        let output = "\
Error: Provider produced inconsistent result after apply

  with aws_instance.web,
  on main.tf.json line 0:
";
        let conflicts = classify_apply_conflicts(output);
        assert_eq!(conflicts.len(), 1);
        assert_eq!(conflicts[0].1, ConflictKind::Other);
    }

    #[test]
    fn no_conflicts_for_non_resource_errors() {
        let output = "Error: Backend initialization required\n\nPlease run tofu init.";
        assert!(classify_apply_conflicts(output).is_empty());
    }

    #[test]
    fn config_attrs_object_block() {
        let json = r#"{
            "resource": {
                "github_repository": {
                    "caixa_crossbeam": { "name": "caixa-crossbeam", "visibility": "public" }
                }
            }
        }"#;
        let m = config_attrs_from_main_tf(json);
        assert_eq!(
            m["github_repository.caixa_crossbeam"]["name"].as_str(),
            Some("caixa-crossbeam")
        );
    }

    #[test]
    fn config_attrs_array_block() {
        // Terraform JSON also permits the single-element-array form.
        let json = r#"{
            "resource": {
                "github_repository": {
                    "foo": [{ "name": "foo-repo" }]
                }
            }
        }"#;
        let m = config_attrs_from_main_tf(json);
        assert_eq!(m["github_repository.foo"]["name"].as_str(), Some("foo-repo"));
    }

    #[test]
    fn config_attrs_empty_on_garbage() {
        assert!(config_attrs_from_main_tf("not json").is_empty());
        assert!(config_attrs_from_main_tf(r#"{"no_resource":true}"#).is_empty());
    }

    #[test]
    fn bundled_policy_imports_conflicts_fails_other() {
        let p = ConflictResolutionPolicy::bundled_default();
        assert_eq!(
            p.resolution_for("github_repository", ConflictKind::AlreadyExists),
            ConflictResolution::Import
        );
        assert_eq!(
            p.resolution_for("github_branch_protection", ConflictKind::AlreadyProtected),
            ConflictResolution::Import
        );
        assert_eq!(
            p.resolution_for("aws_instance", ConflictKind::Other),
            ConflictResolution::Fail
        );
    }

    #[test]
    fn specific_rule_overrides_general_default() {
        // "skip already-exists for cloudflare_record, but the general
        //  github rule still imports."
        let p = ConflictResolutionPolicy {
            enabled: Some(true),
            rules: vec![
                ConflictRule {
                    resource_types: vec!["cloudflare_record".into()],
                    kinds: vec![ConflictKind::AlreadyExists],
                    resolution: ConflictResolution::Skip,
                },
                ConflictRule {
                    resource_types: vec![],
                    kinds: vec![ConflictKind::AlreadyExists, ConflictKind::AlreadyProtected],
                    resolution: ConflictResolution::Import,
                },
            ],
            default_resolution: Some(ConflictResolution::Fail),
            max_rounds: Some(2),
        };
        assert_eq!(
            p.resolution_for("cloudflare_record", ConflictKind::AlreadyExists),
            ConflictResolution::Skip
        );
        assert_eq!(
            p.resolution_for("github_repository", ConflictKind::AlreadyExists),
            ConflictResolution::Import
        );
        assert_eq!(p.rounds(), 2);
    }

    use crate::crd::ConflictRule;
}
