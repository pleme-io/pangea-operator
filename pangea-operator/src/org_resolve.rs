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

/// Project one catalogue row plus its live observation into a record.
///
/// `live` is `None` when the repo does not exist on GitHub.
#[must_use]
pub fn record_for(row: &OrgRepoRow, live: Option<&LiveRepo>) -> RepoRecord {
    let declared_visibility = row.visibility.clone().unwrap_or_else(|| "private".to_string());
    let bp = row.branch_protection.clone().unwrap_or_else(|| "none".to_string());
    let protected = bp != "none";

    let mut r = RepoRecord::new();
    r.insert("repo_name".into(), row.name.clone());
    r.insert("repo_description".into(), row.description.clone().unwrap_or_default());
    r.insert("repo_visibility".into(), declared_visibility.clone());
    r.insert("repo_archived".into(), b(row.archived.unwrap_or(false)));

    // Not in org.yaml. Stated here rather than left implicit so the default is
    // reviewable: GitHub's own default for a new repository is issues ENABLED,
    // and a catalogue that says nothing should not silently disable them.
    r.insert("repo_has_issues".into(), b(true));
    r.insert("repo_delete_branch_on_merge".into(), b(true));
    r.insert("repo_actions_enabled".into(), b(true));
    r.insert("repo_default_branch".into(), "main".into());

    r.insert("repo_standard_labels".into(), b(row.standard_labels.unwrap_or(true)));

    r.insert("repo_has_branch_protection".into(), b(protected));
    r.insert("repo_bp_strict".into(), b(protected));
    r.insert("repo_bp_enforce_admins".into(), b(false));

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
