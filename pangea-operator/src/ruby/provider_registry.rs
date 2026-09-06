//! `terraform.required_providers`, derived from the resources a synthesis emits.
//!
//! ── WHY THIS EXISTS ────────────────────────────────────────────────────────
//! magma's preflight enforces architecture laws A2/A3 — every provider a
//! workspace USES must be DECLARED. The Ruby compiler path has always satisfied
//! that with a finalize step (`Pangea::ProviderRegistry.inject_into_synthesis`,
//! called from `ruby/owner.rs`), which derives the block from the emitted
//! resource types.
//!
//! The lava path had no equivalent, so every lava-compiled workspace failed:
//!
//!   Magma execution failed: substrate preflight violations:
//!   architecture::assert_all_laws: Architecture law violated:
//!   resources declared but no terraform.required_providers entry
//!
//! Measured 2026-09-06 on plo. This is not specific to one architecture:
//! **0 of the 39 architectures in lava-architectures declare
//! `required_providers`**, because they were never meant to — the finalize
//! step is the designed home for it, and only the Ruby half had one.
//!
//! ── AUTHORITY ──
//! This mirrors `pangea-core/lib/pangea/provider_registry.rb`. Kept as a
//! projection with `sources_match_the_ruby_registry` pinning the table, for the
//! same reason the org-resolver tables are: reading the gem at runtime would
//! reintroduce the Ruby dependency this path exists to remove.

use serde_json::{Map, Value};

/// Canonical Terraform Registry sources, mirroring `ProviderRegistry::SOURCES`
/// (`provider_registry.rb:55`).
const SOURCES: &[(&str, &str)] = &[
    ("aws", "hashicorp/aws"),
    ("azurerm", "hashicorp/azurerm"),
    ("google", "hashicorp/google"),
    ("gcp", "hashicorp/google"),
    ("kubernetes", "hashicorp/kubernetes"),
    ("helm", "hashicorp/helm"),
    ("null", "hashicorp/null"),
    ("random", "hashicorp/random"),
    ("tls", "hashicorp/tls"),
    ("local", "hashicorp/local"),
    ("external", "hashicorp/external"),
    ("archive", "hashicorp/archive"),
    ("time", "hashicorp/time"),
    ("cloudflare", "cloudflare/cloudflare"),
    ("github", "integrations/github"),
    ("datadog", "DataDog/datadog"),
    ("akeyless", "akeyless-community/akeyless"),
    ("hcloud", "hetznercloud/hcloud"),
    ("splunk", "splunk/splunk"),
    ("porkbun", "marcfrederick/porkbun"),
];

/// The provider local name a resource type belongs to — everything before the
/// first `_`. `github_repository` → `github`.
#[must_use]
pub fn provider_name_for(resource_type: &str) -> &str {
    resource_type
        .split_once('_')
        .map_or(resource_type, |(head, _)| head)
}

/// The registry source for a provider local name, falling back to
/// `hashicorp/<name>` — OpenTofu's implicit default, so an unknown provider
/// resolves exactly as it did before anything was declared.
#[must_use]
pub fn source_for(name: &str) -> String {
    SOURCES
        .iter()
        .find(|(k, _)| *k == name)
        .map_or_else(|| format!("hashicorp/{name}"), |(_, v)| (*v).to_string())
}

/// Finalize a synthesis IN PLACE: ensure `terraform.required_providers` names
/// every provider the `resource` / `data` blocks use.
///
/// Three behaviours copied deliberately from the Ruby:
/// - **Existing entries win.** A hand-declared `source`/`version` is never
///   overwritten; only missing providers are added.
/// - **Source-only, no `version`.** tofu then resolves the same versions it
///   previously inferred, so rendered output stays a byte-compatible superset.
/// - **A resource-free synthesis is left untouched** — vacuously magma-clean,
///   and adding an empty block would be a diff for nothing.
pub fn inject_into_synthesis(synthesis: &mut Value) {
    let Some(obj) = synthesis.as_object_mut() else {
        return;
    };

    let mut types: Vec<String> = Vec::new();
    for block in ["resource", "data"] {
        if let Some(Value::Object(m)) = obj.get(block) {
            types.extend(m.keys().cloned());
        }
    }
    if types.is_empty() {
        return;
    }

    let terraform = obj
        .entry("terraform")
        .or_insert_with(|| Value::Object(Map::new()));
    let Some(terraform) = terraform.as_object_mut() else {
        return;
    };
    let required = terraform
        .entry("required_providers")
        .or_insert_with(|| Value::Object(Map::new()));
    let Some(required) = required.as_object_mut() else {
        return;
    };

    for ty in types {
        let name = provider_name_for(&ty);
        if name.is_empty() || required.contains_key(name) {
            continue;
        }
        let mut spec = Map::new();
        spec.insert("source".into(), Value::String(source_for(name)));
        required.insert(name.to_string(), Value::Object(spec));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// Pins the table against `provider_registry.rb:55`.
    ///
    /// Same honest limit as the org-resolver tables: this pins values rather
    /// than reading the gem, which would reintroduce the dependency. If it
    /// fails, read the gem and change whichever side is wrong.
    #[test]
    fn sources_match_the_ruby_registry() {
        assert_eq!(source_for("github"), "integrations/github");
        assert_eq!(source_for("aws"), "hashicorp/aws");
        assert_eq!(source_for("gcp"), "hashicorp/google", "gcp aliases google");
        assert_eq!(source_for("cloudflare"), "cloudflare/cloudflare");
        assert_eq!(source_for("datadog"), "DataDog/datadog");
        assert_eq!(source_for("akeyless"), "akeyless-community/akeyless");
        assert_eq!(source_for("porkbun"), "marcfrederick/porkbun");
        // The fallback IS the contract, not a guess: hashicorp/<name> is
        // OpenTofu's implicit default, so an unlisted provider resolves
        // exactly as it did when nothing was declared.
        assert_eq!(source_for("somethingnew"), "hashicorp/somethingnew");
    }

    #[test]
    fn provider_name_is_everything_before_the_first_underscore() {
        assert_eq!(provider_name_for("github_repository"), "github");
        assert_eq!(provider_name_for("github_branch_protection"), "github");
        assert_eq!(provider_name_for("aws_iam_role"), "aws");
        // No underscore at all — the whole string is the name.
        assert_eq!(provider_name_for("random"), "random");
    }

    /// The failure this module exists to fix, end to end.
    #[test]
    fn resources_get_a_required_providers_block() {
        let mut synth = json!({
            "resource": {
                "github_repository": { "tend": { "name": "tend" } },
                "github_branch_protection": { "tear": { "pattern": "main" } }
            }
        });
        inject_into_synthesis(&mut synth);
        let rp = &synth["terraform"]["required_providers"];
        assert_eq!(rp["github"]["source"], "integrations/github");
        // Two github_* types collapse to ONE provider entry.
        assert_eq!(rp.as_object().unwrap().len(), 1);
        // Source-only — no version, so tofu resolves as before.
        assert!(rp["github"].get("version").is_none());
    }

    #[test]
    fn data_blocks_count_too() {
        let mut synth = json!({ "data": { "aws_ami": { "x": {} } } });
        inject_into_synthesis(&mut synth);
        assert_eq!(
            synth["terraform"]["required_providers"]["aws"]["source"],
            "hashicorp/aws"
        );
    }

    /// Hand-declared entries win — including a pinned version.
    #[test]
    fn existing_entries_are_never_overwritten() {
        let mut synth = json!({
            "terraform": { "required_providers": {
                "github": { "source": "integrations/github", "version": "6.13.0" }
            }},
            "resource": { "github_repository": { "r": {} } }
        });
        inject_into_synthesis(&mut synth);
        assert_eq!(
            synth["terraform"]["required_providers"]["github"]["version"],
            "6.13.0",
            "an explicit pin must survive the finalize"
        );
    }

    /// ANTI-VACUITY: a resource-free synthesis gets NO block.
    ///
    /// Without this, an implementation that always wrote an empty
    /// `required_providers` would pass every test above while adding a diff to
    /// every workspace that renders nothing.
    #[test]
    fn a_resource_free_synthesis_is_left_untouched() {
        let mut synth = json!({ "output": { "x": { "value": 1 } } });
        let before = synth.clone();
        inject_into_synthesis(&mut synth);
        assert_eq!(synth, before, "nothing to declare, nothing to add");
    }
}
