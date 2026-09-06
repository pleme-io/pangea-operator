//! Resolve an `org.yaml` catalogue into the records
//! `lava-architectures/architectures/github-org-repos.tlisp` consumes.
//!
//! ── ★ WHY THIS EXISTS ───────────────────────────────────────────────────────
//! The Ruby renderer this replaces does two separable jobs, and the tatara-lisp
//! architecture deliberately implements only the second:
//!
//!   1. RESOLVE — read `org.yaml`, then CALL THE GITHUB API to decide
//!      adopt-vs-create and learn each repo's live visibility.
//!   2. RENDER — turn each resolved row into terraform resources.
//!
//! lava is a pure evaluator with no I/O, and that is a property worth keeping:
//! *"an architecture that can reach the network is one whose output depends on
//! when you rendered it, which is the opposite of what a plan is for."* So the
//! resolution stays with the caller, and its ANSWERS arrive at the architecture
//! as ordinary record fields.
//!
//! This module is that caller. It lives in the operator because the
//! architecture's own header names the caller as the one *"which already holds
//! a credential and already does API work"* — which is this process.
//!
//! ── ★ THE TWO RESOLVED FIELDS ARE THE WHOLE POINT ──────────────────────────
//! Seventeen of the nineteen record fields are direct projections of `org.yaml`
//! and could be computed offline. Two cannot, and they are the ones that decide
//! whether a plan CREATES or ADOPTS:
//!
//!   repo_exists_on_github  -> gates `(import "github_repository.{name}" …)`.
//!                             Wrong-false plans a CREATE against a repo that
//!                             exists, which the provider rejects; wrong-true
//!                             plans an import of nothing.
//!   repo_live_visibility   -> what the repo IS right now, as opposed to what
//!                             the catalogue says it should be. Rendering the
//!                             catalogue value here would make the plan a no-op
//!                             on exactly the repos whose visibility drifted.
//!
//! ── ★ READ CREDENTIALS, NOT THE APP KEY ────────────────────────────────────
//! Resolution only READS. It needs `Metadata: read` and nothing more, so it
//! takes an ordinary token (or none — a 404 for an absent repo needs no auth).
//! The App identity with `administration: write` is used by the terraform
//! github provider at APPLY time via `app_auth`, which mints its own JWT. Those
//! are deliberately different credentials with different blast radii, and this
//! module never sees the more powerful one.

use serde::Deserialize;
use std::collections::BTreeMap;

/// One row of the `repos:` list in `org.yaml`, narrowed to the fields the
/// architecture actually consumes.
///
/// `#[serde(default)]` throughout: the catalogue is hand-maintained and rows
/// legitimately omit most keys. A missing key is the documented default, never
/// a parse failure that would take the whole org down for one incomplete row.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct OrgRepoRow {
    pub name: String,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub visibility: Option<String>,
    #[serde(default)]
    pub archived: Option<bool>,
    #[serde(default)]
    pub branch_protection: Option<String>,
    #[serde(default)]
    pub standard_labels: Option<bool>,
    // ── THESE WERE HARDCODED, AND 847 ROWS DISAGREED ────────────────────
    // `has_issues` and `delete_branch_on_merge` used to be unconditional
    // `true` in `record_for` and were not fields here at all, so a row's
    // declared value could not reach the record. Measured on the live
    // catalogue 2026-09-06: 98 rows declare `has_issues: false` and 847
    // declare `delete_branch_on_merge: false`. Every one of them would have
    // been rendered with the opposite setting.
    #[serde(default)]
    pub has_issues: Option<bool>,
    #[serde(default)]
    pub delete_branch_on_merge: Option<bool>,
    /// Tri-state in the gem: `None` means "derive from visibility", NOT
    /// "default to on". See `record_for`.
    #[serde(default)]
    pub actions_enabled: Option<bool>,
}

/// The `org.yaml` document, narrowed to `repos:`.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct OrgCatalogue {
    #[serde(default)]
    pub repos: Vec<OrgRepoRow>,
}

/// What the GitHub API said about one repo. Absent means 404 — the repo does
/// not exist, which is a FINDING rather than an error.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LiveRepo {
    pub visibility: String,
}

/// A resolved record, keyed exactly as the architecture interpolates it.
///
/// Every value is a String because the architecture interpolates `{field}` into
/// string positions — including the `:when` gates, where tatara-lisp reads
/// `"true"`/`"false"`. Emitting a JSON boolean there would render the literal
/// `true` into a position expecting a string and silently disable the gate.
pub type RepoRecord = BTreeMap<String, String>;

fn b(v: bool) -> String {
    // One spelling of a boolean, in one place. The `:when` gates compare against
    // this text, so "True"/"1"/"yes" would each silently mean false.
    if v { "true".to_string() } else { "false".to_string() }
}

/// The branch-protection posture that ACTUALLY reaches terraform.
///
/// ── THE AUTHORITY, AND A CORRECTION ──────────────────────────────────────
/// This mirrors `Pangea::Helpers::Github::BRANCH_PROTECTION_PROFILES`
/// (`pangea-github/lib/pangea/helpers/github_presets.rb:75`).
///
/// It is NOT `OpenSourceRepo::PROFILES`, which an earlier version of this file
/// pinned. `bin/lava-resolve-org` says why in its own words: that table's
/// `required_reviews` / `dismiss_stale_reviews` keys "are read nowhere — that
/// table is a validity whitelist". Pinning it produced a confident-looking
/// distinction between `pilot` and `standard` that does not exist in the
/// emitted output: in the real table the two are BYTE-IDENTICAL, and only
/// `hardened` differs.
///
/// The lesson is worth keeping next to the code: two tables with the same key
/// names, one of them dead, and the dead one is the one whose fields read like
/// policy. Read what the emitter fetches, not what looks authoritative.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BranchProtectionPreset {
    pub enforce_admins: bool,
    pub require_signed_commits: bool,
    pub required_linear_history: bool,
}

impl BranchProtectionPreset {
    /// `pilot` and `standard` are deliberately identical — see the type doc.
    pub const PILOT: Self = Self {
        enforce_admins: false,
        require_signed_commits: false,
        required_linear_history: false,
    };
    pub const STANDARD: Self = Self::PILOT;
    pub const HARDENED: Self = Self {
        enforce_admins: true,
        require_signed_commits: true,
        required_linear_history: true,
    };

    /// `None` for `"none"` and for an unknown name.
    ///
    /// The Ruby RAISES on an unknown profile (`fetch` with a block). Returning
    /// `None` here is the safe direction for a resolver — it under-claims
    /// protection the plan then proposes adding, rather than asserting a
    /// posture nobody defined — but it IS a deliberate divergence, so it is
    /// named rather than left to be discovered.
    #[must_use]
    pub fn parse(name: &str) -> Option<Self> {
        match name {
            "pilot" => Some(Self::PILOT),
            "standard" => Some(Self::STANDARD),
            "hardened" => Some(Self::HARDENED),
            _ => None,
        }
    }
}

/// Project one catalogue row plus its live observation into a record.
///
/// `live` is `None` when the repo does not exist on GitHub.
#[must_use]
pub fn record_for(row: &OrgRepoRow, live: Option<&LiveRepo>) -> RepoRecord {
    let declared_visibility = row.visibility.clone().unwrap_or_else(|| "private".to_string());
    let bp = row.branch_protection.clone().unwrap_or_else(|| "none".to_string());
    let archived = row.archived.unwrap_or(false);

    let mut r = RepoRecord::new();
    r.insert("repo_name".into(), row.name.clone());
    r.insert("repo_description".into(), row.description.clone().unwrap_or_default());
    r.insert("repo_visibility".into(), declared_visibility.clone());
    r.insert("repo_archived".into(), b(archived));

    // ── DEFAULTS COME FROM THE GEM, NOT FROM GUESSES ────────────────────
    // `Pangea::Architectures::Types::OpenSourceRepoConfig`
    // (pangea-architectures/lib/pangea/architectures/types.rb):
    //   has_issues             Types::Bool.default(true)    :356
    //   delete_branch_on_merge Types::Bool.default(true)    :352
    //   standard_labels        Types::Bool.default(false)   :363  <- NOT true
    //   archived               Types::Bool.default(false)   :369
    //   default_branch         Types::String.default('main'):351
    r.insert("repo_has_issues".into(), b(row.has_issues.unwrap_or(true)));
    r.insert(
        "repo_delete_branch_on_merge".into(),
        b(row.delete_branch_on_merge.unwrap_or(true)),
    );
    r.insert("repo_standard_labels".into(), b(row.standard_labels.unwrap_or(false)));
    r.insert("repo_default_branch".into(), "main".into());

    // ── actions_enabled IS TRI-STATE ────────────────────────────────────
    // `None` means "derive from visibility", not "default to on". Mirrors
    // lava-resolve-org:113 — `cfg[:actions_enabled].nil? ? cfg[:visibility]
    // != :internal : cfg[:actions_enabled]`. Its own comment states the
    // stake: "a wrong answer flips Actions on or off for a whole shard", and
    // 974 of 1005 rows rely on this derivation.
    let actions_on = row
        .actions_enabled
        .unwrap_or(declared_visibility != "internal");
    r.insert("repo_actions_enabled".into(), b(actions_on));

    // ── AN ARCHIVED REPO IS NOT PROTECTED ───────────────────────────────
    // `protected = !cfg[:archived] && profile != :none` (lava-resolve-org:108).
    // The archived half was missing here, so an archived repo carrying a
    // preset would have been rendered with live branch protection.
    let preset = if archived {
        None
    } else {
        BranchProtectionPreset::parse(&bp)
    };
    r.insert("repo_has_branch_protection".into(), b(preset.is_some()));

    // ── bp_strict IS A CONSTANT, AND THAT IS THE CORRECT VALUE ──────────
    // The Ruby never sets required_status_checks_strict, and `false` is its
    // NO-CHANGE value. An earlier version of this file derived it from the
    // preset, which invents a status-check policy the Ruby path never emits —
    // a divergence in the more dangerous direction, since it would have
    // shown up as a live plan diff against 5 real repos.
    r.insert("repo_bp_strict".into(), b(false));
    r.insert(
        "repo_bp_enforce_admins".into(),
        b(preset.is_some_and(|p| p.enforce_admins)),
    );

    // The CI shim is not modelled in org.yaml, so it is OFF and its three
    // companion fields are empty. They are still emitted: the architecture
    // interpolates them unconditionally into the resource NAME
    // (`"{repo_name}__{repo_ci_shim_slug}"`), and an absent key would render
    // the placeholder text rather than fail.
    r.insert("repo_has_ci_shim".into(), b(false));
    r.insert("repo_ci_shim_slug".into(), String::new());
    r.insert("repo_ci_shim_path".into(), String::new());
    r.insert("repo_ci_shim_content".into(), String::new());

    // ── the two resolved fields ──
    r.insert("repo_exists_on_github".into(), b(live.is_some()));
    r.insert(
        "repo_live_visibility".into(),
        live.map_or_else(|| declared_visibility, |l| l.visibility.clone()),
    );
    r
}

/// Ask GitHub whether one repo exists, and what it looks like right now.
///
/// `Ok(None)` is a 404 — a FINDING, not an error. That distinction is the whole
/// contract: a resolver that mapped 404 to `Err` would abort the run on the
/// very repos the plan is meant to create.
///
/// # Errors
///
/// Transport failures and non-404 error statuses. A 403 is surfaced rather than
/// swallowed: it usually means rate-limiting or a token without `Metadata:
/// read`, and treating it as "absent" would plan a CREATE against a repo that
/// exists — the one outcome the provider rejects late and loudly.
pub async fn look_up(
    client: &reqwest::Client,
    owner: &str,
    repo: &str,
    token: Option<&str>,
) -> Result<Option<LiveRepo>, String> {
    let url = format!("https://api.github.com/repos/{owner}/{repo}");
    let mut req = client
        .get(&url)
        // GitHub rejects requests with no User-Agent. Naming the caller means a
        // rate-limit investigation can find us.
        .header("User-Agent", "pangea-operator-org-resolve")
        .header("Accept", "application/vnd.github+json");
    if let Some(t) = token {
        req = req.bearer_auth(t);
    }

    let resp = req.send().await.map_err(|e| format!("GET {url}: {e}"))?;
    match resp.status().as_u16() {
        200 => {
            let body: serde_json::Value = resp
                .json()
                .await
                .map_err(|e| format!("GET {url}: decoding body: {e}"))?;
            Ok(Some(LiveRepo {
                // `visibility` is the modern field; `private` is the legacy
                // boolean every token can see. Falling back keeps a repo whose
                // visibility field is withheld from being reported as public.
                visibility: body
                    .get("visibility")
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_string)
                    .unwrap_or_else(|| {
                        if body.get("private").and_then(serde_json::Value::as_bool) == Some(false) {
                            "public".to_string()
                        } else {
                            "private".to_string()
                        }
                    }),
            }))
        }
        404 => Ok(None),
        other => Err(format!(
            "GET {url}: HTTP {other} — refusing to treat this as 'absent', which \
             would plan a CREATE against a repo that may exist"
        )),
    }
}

/// Resolve a catalogue into records, optionally narrowed to `only`.
///
/// `only` exists because the catalogue is ~1000 rows and each is one API call.
/// A first run against a handful of repos is both faster and far easier to
/// review than one that touches the whole org, and the architecture is
/// row-oriented so a subset is a legitimate plan rather than a partial one.
///
/// # Errors
///
/// The first lookup failure, with the repo named. Fail-fast rather than
/// resolving the rest: a partial record set renders a plan that silently omits
/// repos, which looks like a successful smaller run.
pub async fn resolve(
    catalogue: &OrgCatalogue,
    owner: &str,
    token: Option<&str>,
    only: Option<&[String]>,
) -> Result<Vec<RepoRecord>, String> {
    let client = reqwest::Client::new();
    let mut out = Vec::new();
    for row in &catalogue.repos {
        if let Some(filter) = only {
            if !filter.iter().any(|n| n == &row.name) {
                continue;
            }
        }
        let live = look_up(&client, owner, &row.name, token)
            .await
            .map_err(|e| format!("resolving {}/{}: {e}", owner, row.name))?;
        out.push(record_for(row, live.as_ref()));
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The preset table must match `BRANCH_PROTECTION_PROFILES` — the table the
    /// emitter actually fetches.
    ///
    /// ── WHY A LITERAL TEST, AND ITS HONEST LIMIT ──
    /// `bin/lava-resolve-org` reads the live Ruby constant precisely so it
    /// "cannot drift from what the Ruby path emits". This cannot do that
    /// without tying the suite to a sibling checkout, where a missing clone
    /// becomes a GREEN run. So it pins the values and names the file:line to
    /// reconcile against — weaker than reading the constant, and stated as
    /// such rather than implied.
    ///
    /// If this fails: read the gem, decide which side is right, change the one
    /// that is wrong. Do not edit these numbers to match.
    #[test]
    fn presets_match_the_emitting_ruby_table() {
        // github_presets.rb:75 — pilot and standard are IDENTICAL there.
        assert_eq!(BranchProtectionPreset::PILOT, BranchProtectionPreset::STANDARD);
        assert!(!BranchProtectionPreset::PILOT.enforce_admins);
        assert!(!BranchProtectionPreset::PILOT.require_signed_commits);
        assert!(!BranchProtectionPreset::PILOT.required_linear_history);

        // Only hardened differs.
        assert!(BranchProtectionPreset::HARDENED.enforce_admins);
        assert!(BranchProtectionPreset::HARDENED.require_signed_commits);
        assert!(BranchProtectionPreset::HARDENED.required_linear_history);
    }

    /// The gem's defaults, each pinned against the row count that relies on it.
    ///
    /// These are the divergences that mattered: `delete_branch_on_merge` was
    /// hardcoded `true` while 847 of 1005 rows declare `false`.
    #[test]
    fn declared_values_reach_the_record() {
        let declared = OrgRepoRow {
            name: "r".into(),
            has_issues: Some(false),
            delete_branch_on_merge: Some(false),
            standard_labels: Some(true),
            ..Default::default()
        };
        let rec = record_for(&declared, None);
        assert_eq!(rec["repo_has_issues"], "false", "98 rows declare this false");
        assert_eq!(
            rec["repo_delete_branch_on_merge"], "false",
            "847 rows declare this false — the largest divergence found"
        );
        assert_eq!(rec["repo_standard_labels"], "true");
    }

    /// Absent keys take the GEM's default, not a convenient one.
    #[test]
    fn absent_keys_take_the_gem_defaults() {
        let bare = OrgRepoRow { name: "r".into(), ..Default::default() };
        let rec = record_for(&bare, None);
        // types.rb:356 / :352 — both default true.
        assert_eq!(rec["repo_has_issues"], "true");
        assert_eq!(rec["repo_delete_branch_on_merge"], "true");
        // types.rb:363 — default FALSE. This was `unwrap_or(true)`.
        assert_eq!(rec["repo_standard_labels"], "false");
        // types.rb:351
        assert_eq!(rec["repo_default_branch"], "main");
    }

    /// `actions_enabled` is tri-state: absent derives from visibility.
    #[test]
    fn actions_enabled_is_tri_state_not_defaulted() {
        let with_vis = |v: &str| OrgRepoRow {
            name: "r".into(),
            visibility: Some(v.to_string()),
            ..Default::default()
        };
        // lava-resolve-org:113 — nil ? visibility != :internal : value
        assert_eq!(record_for(&with_vis("public"), None)["repo_actions_enabled"], "true");
        assert_eq!(record_for(&with_vis("private"), None)["repo_actions_enabled"], "true");
        assert_eq!(record_for(&with_vis("internal"), None)["repo_actions_enabled"], "false");

        // An explicit value wins over the derivation, in both directions.
        let explicit = |on: bool| OrgRepoRow {
            name: "r".into(),
            visibility: Some("public".into()),
            actions_enabled: Some(on),
            ..Default::default()
        };
        assert_eq!(record_for(&explicit(false), None)["repo_actions_enabled"], "false");
        assert_eq!(record_for(&explicit(true), None)["repo_actions_enabled"], "true");
    }

    /// An archived repo is never protected, whatever preset it declares.
    #[test]
    fn archived_repos_are_not_protected() {
        let archived_protected = OrgRepoRow {
            name: "r".into(),
            archived: Some(true),
            branch_protection: Some("hardened".into()),
            ..Default::default()
        };
        let rec = record_for(&archived_protected, None);
        // lava-resolve-org:108 — !cfg[:archived] && profile != :none
        assert_eq!(rec["repo_has_branch_protection"], "false");
        assert_eq!(rec["repo_bp_enforce_admins"], "false");

        // ANTI-VACUITY: the same preset on a LIVE repo must protect, or this
        // test would pass against a record_for that never protects anything.
        let live_protected = OrgRepoRow {
            branch_protection: Some("hardened".into()),
            ..archived_protected.clone()
        };
        let live = record_for(
            &OrgRepoRow { archived: Some(false), ..live_protected },
            None,
        );
        assert_eq!(live["repo_has_branch_protection"], "true");
        assert_eq!(live["repo_bp_enforce_admins"], "true");
    }

    /// `bp_strict` is a CONSTANT false — the Ruby's no-change value.
    #[test]
    fn bp_strict_is_always_the_no_change_value() {
        for bp in ["none", "pilot", "standard", "hardened"] {
            let row = OrgRepoRow {
                name: "r".into(),
                branch_protection: Some(bp.to_string()),
                ..Default::default()
            };
            assert_eq!(
                record_for(&row, None)["repo_bp_strict"], "false",
                "the Ruby never sets required_status_checks_strict; deriving it \
                 from the preset invents a status-check policy it never emits"
            );
        }
    }

    #[test]
    fn none_and_unknown_are_unprotected() {
        assert_eq!(BranchProtectionPreset::parse("none"), None);
        assert_eq!(BranchProtectionPreset::parse("typo"), None);
        assert_eq!(BranchProtectionPreset::parse(""), None);
        assert_eq!(
            BranchProtectionPreset::parse("standard"),
            Some(BranchProtectionPreset::STANDARD)
        );
    }

    fn row(name: &str) -> OrgRepoRow {
        OrgRepoRow { name: name.into(), ..Default::default() }
    }

    /// The adopt-vs-create switch, which is the reason this module exists.
    #[test]
    fn existence_drives_the_import_gate() {
        let absent = record_for(&row("openwrt-uci"), None);
        assert_eq!(absent["repo_exists_on_github"], "false");

        let present = record_for(&row("openwrt-uci"), Some(&LiveRepo { visibility: "public".into() }));
        assert_eq!(present["repo_exists_on_github"], "true");
    }

    /// Live visibility must reflect the WORLD, not the catalogue — otherwise the
    /// plan is a no-op on exactly the repos that drifted.
    #[test]
    fn live_visibility_comes_from_github_when_the_repo_exists() {
        let r = OrgRepoRow { visibility: Some("private".into()), ..row("drifted") };
        let rec = record_for(&r, Some(&LiveRepo { visibility: "public".into() }));
        assert_eq!(rec["repo_visibility"], "private", "declared intent is preserved");
        assert_eq!(rec["repo_live_visibility"], "public", "live state must not echo the catalogue");
    }

    /// With no repo on GitHub there is no live state; falling back to the
    /// declared value keeps the field total rather than empty.
    #[test]
    fn live_visibility_falls_back_to_declared_when_absent() {
        let r = OrgRepoRow { visibility: Some("public".into()), ..row("new") };
        assert_eq!(record_for(&r, None)["repo_live_visibility"], "public");
    }

    /// Every field the architecture interpolates must be present. A missing key
    /// renders the placeholder text into a resource name rather than failing,
    /// which is the worst outcome available.
    #[test]
    fn every_field_the_architecture_interpolates_is_emitted() {
        let rec = record_for(&row("x"), None);
        for field in [
            "repo_name", "repo_description", "repo_visibility", "repo_has_issues",
            "repo_archived", "repo_delete_branch_on_merge", "repo_standard_labels",
            "repo_actions_enabled", "repo_has_ci_shim", "repo_ci_shim_slug",
            "repo_ci_shim_path", "repo_ci_shim_content", "repo_has_branch_protection",
            "repo_default_branch", "repo_bp_strict", "repo_bp_enforce_admins",
            "repo_exists_on_github", "repo_live_visibility",
        ] {
            assert!(rec.contains_key(field), "record is missing {field}");
        }
    }

    /// Booleans are the STRINGS the `:when` gates compare against.
    #[test]
    fn booleans_render_as_when_gate_text() {
        let r = OrgRepoRow { branch_protection: Some("none".into()), ..row("p") };
        assert_eq!(record_for(&r, None)["repo_has_branch_protection"], "false");
        let r2 = OrgRepoRow { branch_protection: Some("standard".into()), ..row("p") };
        assert_eq!(record_for(&r2, None)["repo_has_branch_protection"], "true");
    }

    /// A row with only a name must parse — the catalogue is hand-maintained and
    /// most keys are optional.
    #[test]
    fn a_minimal_row_parses_and_projects() {
        let cat: OrgCatalogue = serde_yaml::from_str("repos:\n  - name: solo\n").expect("parses");
        assert_eq!(cat.repos.len(), 1);
        let rec = record_for(&cat.repos[0], None);
        assert_eq!(rec["repo_name"], "solo");
        assert_eq!(rec["repo_visibility"], "private", "unstated visibility defaults closed");
    }
}

#[cfg(test)]
mod live {
    use super::*;

    /// Resolve the real catalogue against the real API.
    ///
    /// `#[ignore]` because it needs the network and a checked-out
    /// `pangea-architectures`. Run explicitly:
    ///
    ///   cargo test -p pangea-operator --lib live -- --ignored --nocapture
    #[tokio::test]
    #[ignore]
    async fn resolves_the_declared_but_uncreated_repos() {
        let path = std::env::var("ORG_YAML").expect("set ORG_YAML");
        let text = std::fs::read_to_string(&path).expect("read org.yaml");
        let cat: OrgCatalogue = serde_yaml::from_str(&text).expect("parse org.yaml");
        eprintln!("catalogue rows: {}", cat.repos.len());

        let only: Vec<String> = [
            "openwrt-uci", "ancora", "annai", "jikoku",
            "nanori", "roji", "camelot-incept", "lava-discord",
        ]
        .iter()
        .map(|s| (*s).to_string())
        .collect();

        let token = std::env::var("GITHUB_TOKEN").ok();
        let records = resolve(&cat, "pleme-io", token.as_deref(), Some(&only))
            .await
            .expect("resolve");

        eprintln!("resolved {} records", records.len());
        for r in &records {
            eprintln!(
                "  {:<16} exists={:<5} live_vis={:<7} declared_vis={}",
                r["repo_name"], r["repo_exists_on_github"],
                r["repo_live_visibility"], r["repo_visibility"]
            );
        }
        assert_eq!(records.len(), only.len(), "every requested repo must resolve");
    }
}
