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

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// One row of the `repos:` list in `org.yaml`, narrowed to the fields the
/// architecture actually consumes.
///
/// `#[serde(default)]` throughout: the catalogue is hand-maintained and rows
/// legitimately omit most keys. A missing key is the documented default, never
/// a parse failure that would take the whole org down for one incomplete row.
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct OrgRepoRow {
    pub name: String,
    /// Named entries from the catalogue's `repo_profiles`, applied in the
    /// order given (later wins), between the workspace defaults and this
    /// row's own fields.
    #[serde(default)]
    pub inherits: Vec<String>,
    /// The row's own settings. Flattened, so the YAML shape is unchanged — a
    /// row still writes `has_issues: false` at the top level.
    #[serde(flatten)]
    pub overlay: RepoOverlay,
}

/// The mergeable half of a repository declaration: every field is `Option`,
/// so "this tier does not speak to that setting" is a representable state
/// distinct from "this tier sets it to the default value".
///
/// That distinction is the whole point. Before this type existed there was no
/// tier between the Ruby gem's defaults and an individual row, so a setting a
/// whole workspace agreed on had to be restated on every row — and any row
/// that forgot inherited the GEM's answer, which is not necessarily the
/// workspace's. Measured on `pleme-io-opensource` 2026-09-07:
///
///   - 80% of the file's 22,880 key-value pairs restate the most-common value
///   - 7 keys have exactly ONE distinct value across ~1000 rows
///     (`actions_secrets`, `actions_variables`, `environments`,
///     `allow_squash_merge`, `allow_rebase_merge`, `has_downloads`, `pages`)
///   - 43 rows omit `delete_branch_on_merge`, so they resolve to the gem's
///     `true` while 847 of their siblings declare `false`
///
/// That last line is the defect, not the verbosity: the inherited value
/// DISAGREES with the corpus consensus, and nothing says so because a row
/// that omits a key reads exactly like a row that agrees with the default.
///
/// ── ★★ DO NOT MIGRATE A LIVE `org.yaml` ONTO THESE TIERS YET ─────────────
/// `pleme-io-opensource/org.yaml` is read by TWO implementations: this
/// resolver (via `dialect: lava`) and the Ruby `GithubOrgWorkspace` (via
/// `pleme_io_opensource_org.rb`'s `org_yaml:`). Only this one knows about
/// `repo_defaults` / `repo_profiles`.
///
/// Measured 2026-09-07: the Ruby loader is `YAML.safe_load_file` followed by
/// `data.dig('organization', …)` / `data['repos']`, so an unknown top-level
/// key is **silently ignored** — no error, no warning. And Ruby's
/// `Types::OpenSourceRepoConfig` defaults `delete_branch_on_merge` to `true`
/// (`types.rb:352`).
///
/// So hoisting `delete_branch_on_merge: false` into `repo_defaults` and
/// deleting it from the 847 rows that restate it would render:
///
///   this resolver -> false  (from the workspace tier)
///   the Ruby path -> true   (the key is gone; the gem default applies)
///
/// An **847-row silent divergence between two renderers of one file** — far
/// worse than the 43-row bug it set out to fix. The differential caught it
/// only because the fold was dumped and compared; nothing in either
/// implementation would have said a word.
///
/// The tiers are therefore SAFE TO USE on a catalogue only this resolver
/// reads, and the pleme-io-opensource migration is gated on the Ruby side
/// implementing the same fold — ideally with a cross-implementation parity
/// spec beside `spec/parity/koritsu_parity_spec.rb`, which is exactly the
/// shape that gate wants.
///
/// `pending-org-tier-migration: Ruby GithubOrgWorkspace must fold
///  repo_defaults/repo_profiles before any live catalogue is reduced.`
///
/// Use `--resolve-org --emit overlays` to dump the fold offline (no network,
/// no credential) and diff before/after. Diff the `record` field, not
/// `resolved`: two catalogues can differ in their overlays and render
/// identical records, which is the common case when hoisting a value that
/// already matched the gem.
///
/// ── ★ `deny_unknown_fields` HERE, TOLERANT ON A ROW — DELIBERATELY ────────
/// A row is tolerant because `org.yaml` rows carry keys this Rust does not
/// model and other consumers do (`exposure`, `license`, `topics`, `pages`,
/// `environments`, …); rejecting those would take the whole org down. The
/// defaults and profile tiers are STRICT because they are new and nothing
/// else reads them, so an unknown key there is certainly a typo — and a typo
/// in a tier that 1005 rows inherit from is silent and total. `has_issue`
/// would simply never be read, and every row would keep the gem's answer
/// while the file read as though it had been configured.
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RepoOverlay {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub visibility: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub archived: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub branch_protection: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub standard_labels: Option<bool>,
    // ── THESE WERE HARDCODED, AND 847 ROWS DISAGREED ────────────────────
    // `has_issues` and `delete_branch_on_merge` used to be unconditional
    // `true` in `record_for` and were not fields here at all, so a row's
    // declared value could not reach the record. Measured on the live
    // catalogue 2026-09-06: 98 rows declare `has_issues: false` and 847
    // declare `delete_branch_on_merge: false`. Every one of them would have
    // been rendered with the opposite setting.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub has_issues: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delete_branch_on_merge: Option<bool>,
    /// Tri-state in the gem: `None` means "derive from visibility", NOT
    /// "default to on". See `record_for`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub actions_enabled: Option<bool>,
}

/// The `org.yaml` document, narrowed to what this resolver consumes.
///
/// ── ★ FOUR TIERS, NARROWEST WINS ──────────────────────────────────────────
/// A repository's effective settings are a fold, not a lookup:
///
/// | # | tier | lives in | speaks for |
/// |---|---|---|---|
/// | 1 | gem defaults | `record_for`'s `unwrap_or` | every workspace, everywhere |
/// | 2 | workspace defaults | `repo_defaults:` | one `org.yaml` |
/// | 3 | profiles | `repo_profiles:` named by a row's `inherits:` | a reusable class of repo |
/// | 4 | the row | the row's own keys | one repository |
///
/// Each tier overlays the one above it: a field a tier SETS wins, a field it
/// leaves unset inherits. So a workspace states its house style once, a class
/// of repository (say `rust-library`) states its shape once, and a row
/// declares only what makes it different.
///
/// Tier 1 is the one that was missing a floor under it, and its absence is
/// what made this necessary rather than merely tidy — see [`RepoOverlay`] for
/// the measurement. Tier 1 remains the gem's, deliberately: forking it would
/// break parity with the Ruby side. Tier 2 exists so a workspace can DISAGREE
/// with the gem in one place instead of 1005.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct OrgCatalogue {
    /// Tier 2 — applies to every row in this workspace.
    #[serde(default)]
    pub repo_defaults: RepoOverlay,

    /// Tier 3 — reusable named sets, applied by a row's `inherits:`.
    #[serde(default)]
    pub repo_profiles: BTreeMap<String, RepoOverlay>,

    /// Tier 4 — the rows themselves, as written.
    #[serde(default)]
    pub repos: Vec<OrgRepoRow>,
}

/// Which tier supplied a value. Returned alongside a resolved row so a reader
/// can answer "where did this setting come from?" — the question that was
/// unanswerable before, and the reason a wrong inherited default stayed
/// invisible.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Tier {
    /// Tier 2 — the workspace's `repo_defaults`.
    WorkspaceDefaults,
    /// Tier 3 — a named profile from `repo_profiles`.
    Profile(String),
    /// Tier 4 — the row itself.
    Row,
}

/// Overlay `over` onto `base`, in place: a key `over` sets wins; a key it
/// leaves NULL is inherited.
///
/// Same rule as `shikumi::discovered::deep_merge` — two objects at one key
/// recurse so siblings survive, anything else is replaced wholesale. Written
/// against `serde_json` rather than converted into shikumi's `Dict` and back,
/// because the conversion would buy no behavioural difference on this data and
/// the recursion is four lines. If a nested field is ever added here, the
/// recursion is already correct for it.
///
/// ── ★ WHY THIS IS GENERIC AND NOT A LIST OF FIELDS ───────────────────────
/// A per-field merge (`over.has_issues.or(base.has_issues)`, eight times) is
/// the obvious implementation and it goes stale the first time a field is
/// added: the new field silently stops inheriting, which presents as a
/// workspace default that "doesn't work" for exactly one setting. Merging in
/// value space means a new field on [`RepoOverlay`] participates with no edit
/// here at all — there is no list to forget.
fn overlay_onto(base: &mut serde_json::Map<String, serde_json::Value>, over: &serde_json::Value) {
    let Some(over) = over.as_object() else { return };
    for (k, v) in over {
        if v.is_null() {
            // Unset at this tier: inherit whatever is already there. This is
            // the line that makes `Option` mean "does not speak to it" rather
            // than "sets it to null".
            continue;
        }
        match (base.get_mut(k), v) {
            (Some(serde_json::Value::Object(b)), serde_json::Value::Object(_)) => {
                overlay_onto(b, v);
            }
            _ => {
                base.insert(k.clone(), v.clone());
            }
        }
    }
}

impl OrgRepoRow {
    /// This row with `over` overlaid onto its own settings — the same rule the
    /// tier fold uses, so a caller composing settings exercises the production
    /// merge rather than a second one.
    ///
    /// ── ★ WHY THIS EXISTS RATHER THAN STRUCT-UPDATE SYNTAX ────────────────
    /// `OrgRepoRow { overlay: RepoOverlay { a: Some(x), ..Default::default() },
    /// ..base }` REPLACES the whole overlay, silently discarding every setting
    /// `base` had — because the overlay is one field, so `..base` has nothing
    /// per-setting to carry through. That reads exactly like the flat version
    /// it replaced and behaves differently. It cost a red test the moment the
    /// nesting landed (`archived_repos_are_not_protected`, which lost its
    /// `branch_protection: hardened` and so measured nothing).
    ///
    /// Use this instead of struct-update whenever the base has real settings.
    #[must_use]
    pub fn merging(&self, over: RepoOverlay) -> Self {
        let mut acc = serde_json::to_value(&self.overlay)
            .ok()
            .and_then(|v| v.as_object().cloned())
            .unwrap_or_default();
        if let Ok(v) = serde_json::to_value(&over) {
            overlay_onto(&mut acc, &v);
        }
        Self {
            name: self.name.clone(),
            inherits: self.inherits.clone(),
            overlay: serde_json::from_value(serde_json::Value::Object(acc))
                .unwrap_or_else(|_| self.overlay.clone()),
        }
    }
}

/// The folded rows, discarding provenance — the shape `resolve` needs.
///
/// Exists so `resolve` and `--emit overlays` cannot drift apart on WHICH rows
/// they fold: there is one fold, reached two ways. That drift is precisely the
/// bug this function was extracted to close.
fn resolved_rows_of(catalogue: &OrgCatalogue) -> Result<Vec<OrgRepoRow>, String> {
    Ok(catalogue
        .resolved_rows()?
        .into_iter()
        .map(|(row, _prov)| row)
        .collect())
}

impl OrgCatalogue {
    /// The rows with tiers 2 and 3 folded in, plus the provenance of every
    /// key each row ended up with.
    ///
    /// Errors when a row names a profile the catalogue does not define. That
    /// is a refusal rather than a skip on purpose: silently ignoring an
    /// unknown `inherits:` entry would leave the row on the gem defaults while
    /// the file read as though a profile had been applied — the same silent
    /// class this whole tier structure exists to close.
    pub fn resolved_rows(&self) -> Result<Vec<(OrgRepoRow, BTreeMap<String, Tier>)>, String> {
        let mut out = Vec::with_capacity(self.repos.len());
        for row in &self.repos {
            // Start from tier 2.
            let defaults = serde_json::to_value(&self.repo_defaults)
                .map_err(|e| format!("serializing repo_defaults: {e}"))?;
            let mut acc = defaults.as_object().cloned().unwrap_or_default();
            let mut prov: BTreeMap<String, Tier> = acc
                .keys()
                .map(|k| (k.clone(), Tier::WorkspaceDefaults))
                .collect();

            // Tier 3, in the order the row names them — later wins, so a row
            // listing `[base, override]` gets `override`'s answer.
            for name in &row.inherits {
                let profile = self.repo_profiles.get(name).ok_or_else(|| {
                    format!(
                        "repo {:?} inherits {:?}, which is not defined in repo_profiles. \
                         Known profiles: {}. Refusing rather than ignoring it — a skipped \
                         inherit leaves the row on the gem defaults while the file reads \
                         as though the profile applied.",
                        row.name,
                        name,
                        if self.repo_profiles.is_empty() {
                            "(none declared)".to_string()
                        } else {
                            self.repo_profiles
                                .keys()
                                .map(String::as_str)
                                .collect::<Vec<_>>()
                                .join(", ")
                        }
                    )
                })?;
                let v = serde_json::to_value(profile)
                    .map_err(|e| format!("serializing repo_profiles.{name}: {e}"))?;
                if let Some(o) = v.as_object() {
                    for k in o.keys().filter(|k| !o[*k].is_null()) {
                        prov.insert(k.clone(), Tier::Profile(name.clone()));
                    }
                }
                overlay_onto(&mut acc, &v);
            }

            // Tier 4 — the row's own keys, which always win.
            let v = serde_json::to_value(&row.overlay)
                .map_err(|e| format!("serializing row {:?}: {e}", row.name))?;
            if let Some(o) = v.as_object() {
                for k in o.keys().filter(|k| !o[*k].is_null()) {
                    prov.insert(k.clone(), Tier::Row);
                }
            }
            overlay_onto(&mut acc, &v);

            let overlay: RepoOverlay = serde_json::from_value(serde_json::Value::Object(acc))
                .map_err(|e| format!("folding tiers for row {:?}: {e}", row.name))?;

            out.push((
                OrgRepoRow {
                    name: row.name.clone(),
                    inherits: row.inherits.clone(),
                    overlay,
                },
                prov,
            ));
        }
        Ok(out)
    }
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
    if v {
        "true".to_string()
    } else {
        "false".to_string()
    }
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

/// A repository name sanitized into a Terraform resource identifier.
///
/// ── WHY AN ADDRESS IS NOT A NAME ───────────────────────────────────────────
/// `github-org-repos.tlisp` interpolates the repo name straight into the
/// resource ADDRESS, and lava-core inserts it verbatim (no string transforms
/// exist at the lava layer). A `.` in an address is a traversal separator, so
/// `fastboot.js` renders `${github_repository.fastboot.js.name}` — a reference
/// to a resource `fastboot` that does not exist — and `.github` renders
/// `${github_repository..github.name}`, whose empty name segment makes the
/// reference unresolvable. Both render with NO ERROR.
///
/// Measured over the live catalogue 2026-09-06: 3 of 1005 names carry a dot
/// (`fastboot.js`, `compass.nvim`, `.github`) and are the entire broken set.
/// 690 names carry a HYPHEN, which is a legal Terraform identifier character —
/// those are addressable as-is and are only interesting if adopting the Ruby
/// path's existing state, which slugs them to `_`.
///
/// This mirrors `Pangea::Helpers::Github`'s `tf_slug`
/// (`pangea-github/lib/pangea/helpers/github_presets.rb:59-60`) EXACTLY,
/// including the `r_` prefix for a name that cannot start an identifier, so
/// the two paths address the same resource and either could adopt the other's
/// state.
#[must_use]
pub fn tf_slug(name: &str) -> String {
    let mut s: String = name
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect();
    // `\A[a-zA-Z_]` in the Ruby — a leading digit is as illegal as a leading
    // separator, and `.github` slugs to `_github`, which already satisfies it.
    if !s
        .chars()
        .next()
        .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
    {
        s = format!("r_{s}");
    }
    s
}

/// Project one catalogue row plus its live observation into a record.
///
/// `live` is `None` when the repo does not exist on GitHub.
#[must_use]
pub fn record_for(row: &OrgRepoRow, live: Option<&LiveRepo>) -> RepoRecord {
    let declared_visibility = row
        .overlay
        .visibility
        .clone()
        .unwrap_or_else(|| "private".to_string());
    let bp = row
        .overlay
        .branch_protection
        .clone()
        .unwrap_or_else(|| "none".to_string());
    let archived = row.overlay.archived.unwrap_or(false);

    let mut r = RepoRecord::new();
    r.insert("name".into(), row.name.clone());
    // The ADDRESS component. Separate from `name` on purpose: the repository
    // is still created as `row.name`; only the Terraform identifier is slugged.
    r.insert("slug".into(), tf_slug(&row.name));
    r.insert(
        "description".into(),
        row.overlay.description.clone().unwrap_or_default(),
    );
    r.insert("visibility".into(), declared_visibility.clone());
    r.insert("archived".into(), b(archived));

    // ── DEFAULTS COME FROM THE GEM, NOT FROM GUESSES ────────────────────
    // `Pangea::Architectures::Types::OpenSourceRepoConfig`
    // (pangea-architectures/lib/pangea/architectures/types.rb):
    //   has_issues             Types::Bool.default(true)    :356
    //   delete_branch_on_merge Types::Bool.default(true)    :352
    //   standard_labels        Types::Bool.default(false)   :363  <- NOT true
    //   archived               Types::Bool.default(false)   :369
    //   default_branch         Types::String.default('main'):351
    r.insert(
        "has_issues".into(),
        b(row.overlay.has_issues.unwrap_or(true)),
    );
    r.insert(
        "delete_branch_on_merge".into(),
        b(row.overlay.delete_branch_on_merge.unwrap_or(true)),
    );
    r.insert(
        "standard_labels".into(),
        b(row.overlay.standard_labels.unwrap_or(false)),
    );
    r.insert("default_branch".into(), "main".into());

    // ── actions_enabled IS TRI-STATE ────────────────────────────────────
    // `None` means "derive from visibility", not "default to on". Mirrors
    // lava-resolve-org:113 — `cfg[:actions_enabled].nil? ? cfg[:visibility]
    // != :internal : cfg[:actions_enabled]`. Its own comment states the
    // stake: "a wrong answer flips Actions on or off for a whole shard", and
    // 974 of 1005 rows rely on this derivation.
    let actions_on = row
        .overlay
        .actions_enabled
        .unwrap_or(declared_visibility != "internal");
    r.insert("actions_enabled".into(), b(actions_on));

    // ── AN ARCHIVED REPO IS NOT PROTECTED ───────────────────────────────
    // `protected = !cfg[:archived] && profile != :none` (lava-resolve-org:108).
    // The archived half was missing here, so an archived repo carrying a
    // preset would have been rendered with live branch protection.
    let preset = if archived {
        None
    } else {
        BranchProtectionPreset::parse(&bp)
    };
    r.insert("has_branch_protection".into(), b(preset.is_some()));

    // ── bp_strict IS A CONSTANT, AND THAT IS THE CORRECT VALUE ──────────
    // The Ruby never sets required_status_checks_strict, and `false` is its
    // NO-CHANGE value. An earlier version of this file derived it from the
    // preset, which invents a status-check policy the Ruby path never emits —
    // a divergence in the more dangerous direction, since it would have
    // shown up as a live plan diff against 5 real repos.
    r.insert("bp_strict".into(), b(false));
    r.insert(
        "bp_enforce_admins".into(),
        b(preset.is_some_and(|p| p.enforce_admins)),
    );

    // The CI shim is not modelled in org.yaml, so it is OFF and its three
    // companion fields are empty. They are still emitted: the architecture
    // interpolates them unconditionally into the resource NAME
    // (`"{repo_name}__{repo_ci_shim_slug}"`), and an absent key would render
    // the placeholder text rather than fail.
    r.insert("has_ci_shim".into(), b(false));
    r.insert("ci_shim_slug".into(), String::new());
    r.insert("ci_shim_path".into(), String::new());
    r.insert("ci_shim_content".into(), String::new());

    // ── the two resolved fields ──
    r.insert("exists_on_github".into(), b(live.is_some()));
    r.insert(
        "live_visibility".into(),
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
/// A single catalogue row that GitHub will refuse, and why.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatalogueViolation {
    /// The repository the row names.
    pub repo: String,
    /// The offending field.
    pub field: &'static str,
    /// What is wrong, in the operator's terms.
    pub detail: String,
}

impl std::fmt::Display for CatalogueViolation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {} — {}", self.repo, self.field, self.detail)
    }
}

/// GitHub's own limit on a repository `description`, in characters.
///
/// Measured, not read from documentation: `jikou` carried a 365-character
/// description and the API answered **422** on it, which surfaced as a failed
/// apply cycle after 1005 lookups had already been spent. GitHub documents no
/// number here.
pub const DESCRIPTION_MAX_CHARS: usize = 350;

/// Refuse a catalogue GitHub will refuse, BEFORE spending an API call on it.
///
/// ── ★ WHY THIS IS A PARSE-BOUNDARY CHECK AND NOT AN APPLY-TIME ERROR ──────
/// `resolve` makes one API call per row — 1005 of them for pleme-io — and the
/// fields checked here are not consulted until the *apply* that follows. So a
/// single over-long description meant: 1005 lookups, a compile, a plan, an
/// approval, and then a 422 from deep inside a provider, naming an address
/// rather than a catalogue row. Measured 2026-09-07, and the diagnosis cost
/// far more than the fix.
///
/// Every rule here is a fact about **GitHub**, not about us — which is the
/// MIRAGEM test for a limit that should be typed rather than dissolved. We
/// cannot make GitHub accept a 400-character description, so the honest move
/// is to reject it at the boundary where the author can still see their own
/// row.
///
/// Returns every violation rather than the first, because an author fixing a
/// catalogue wants the whole list in one pass, not one per apply cycle.
///
/// TIER: parse-time-rejected, not truly-unrepresentable — `OrgRepoRow` can
/// still *hold* a 400-character description. Making it unrepresentable wants a
/// refinement-typed field (`Refined<String, MaxChars<350>>`), which is the
/// destination; this is the honest middle tier and it is stated as such.
#[must_use]
pub fn validate_catalogue(catalogue: &OrgCatalogue) -> Vec<CatalogueViolation> {
    let mut out = Vec::new();
    for row in &catalogue.repos {
        // ── description length ────────────────────────────────────────────
        // CHARACTERS, not bytes: GitHub counts characters, and a description
        // with an em-dash or an accented word is longer in bytes than in
        // characters. `len()` would reject rows GitHub accepts — a false
        // refusal is still a defect.
        if let Some(desc) = &row.overlay.description {
            let n = desc.chars().count();
            if n > DESCRIPTION_MAX_CHARS {
                out.push(CatalogueViolation {
                    repo: row.name.clone(),
                    field: "description",
                    detail: format!(
                        "{n} characters exceeds GitHub's limit of {DESCRIPTION_MAX_CHARS}; \
                         the API answers 422 and the failure surfaces at apply time, \
                         naming a provider address rather than this row"
                    ),
                });
            }
        }

        // ── name charset ──────────────────────────────────────────────────
        // GitHub accepts ASCII alphanumerics, `-`, `_` and `.`. Anything else
        // is a 422 on create — and for an EXISTING repo the lookup 404s, which
        // the resolver correctly reads as absent, so a typo'd name presents as
        // a repo that needs creating rather than as a bad row.
        if row.name.is_empty() {
            out.push(CatalogueViolation {
                repo: "<empty>".to_string(),
                field: "name",
                detail: "a row with an empty name resolves to a lookup of the org root \
                         and cannot be created"
                    .to_string(),
            });
        } else if let Some(bad) = row
            .name
            .chars()
            .find(|c| !(c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.')))
        {
            out.push(CatalogueViolation {
                repo: row.name.clone(),
                field: "name",
                detail: format!(
                    "contains {bad:?}; GitHub accepts ASCII alphanumerics, '-', '_' and '.'. \
                     An unacceptable name 404s on lookup, which reads as ABSENT — so this \
                     presents as a repository needing creation rather than as a bad row"
                ),
            });
        }

        // ── visibility enum ───────────────────────────────────────────────
        // `visibility` is a free-form Option<String> in the row, so a typo is
        // not a parse error. `record_for` passes it through to the
        // architecture, which emits it into the provider — where `publi` is a
        // 422, and `Public` is too (GitHub is case-sensitive here).
        if let Some(v) = &row.overlay.visibility {
            if !matches!(v.as_str(), "public" | "private" | "internal") {
                out.push(CatalogueViolation {
                    repo: row.name.clone(),
                    field: "visibility",
                    detail: format!(
                        "{v:?} is not one of \"public\", \"private\", \"internal\" \
                         (case-sensitive). The row parses, the plan compiles, and the \
                         provider answers 422"
                    ),
                });
            }
        }
    }
    out
}

pub async fn resolve(
    catalogue: &OrgCatalogue,
    owner: &str,
    token: Option<&str>,
    only: Option<&[String]>,
) -> Result<Vec<RepoRecord>, String> {
    // ── ★ REFUSE BEFORE SPENDING 1005 API CALLS ──────────────────────────
    // These fields are not consulted until the apply that follows resolution,
    // so without this a single bad row costs a full resolve + compile + plan
    // + approval before a 422 arrives naming a provider address instead of
    // the row. The check is free and the whole list comes back at once.
    let violations = validate_catalogue(catalogue);
    if !violations.is_empty() {
        return Err(format!(
            "the catalogue declares {} row(s) GitHub will refuse:\n  {}",
            violations.len(),
            violations
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join("\n  ")
        ));
    }

    // ── ★ RESOLVE THE FOLDED ROWS, NOT THE RAW ONES ──────────────────────
    // This line is the whole tier feature. Reading `catalogue.repos` directly
    // — as this did until 2026-09-07 — means `repo_defaults` and
    // `repo_profiles` are parsed, validated, and then IGNORED by the only
    // path the operator actually runs.
    //
    // It failed exactly that way in production and the failure was SILENT in
    // the worst possible shape: `--emit overlays` folds (it calls
    // `resolved_rows`), so the migration oracle reported the tier working and
    // the differential was clean, while `--emit cr-patch` — the path the
    // node's seed unit uses — emitted unfolded records. The seed logged
    // `infrastructuretemplate … patched` and changed nothing, because the
    // patch it computed was byte-identical to what was already there.
    //
    // Every layer said yes: the catalogue parsed, the fold was tested
    // (1444 tests), both renderers agreed 1005/1005, the seed ran, kubectl
    // reported a successful patch. The only thing that said no was counting
    // the values in the live CR afterwards.
    //
    // A no-op reported as success — the same class as everything else this
    // resolver guards against, this time in the wiring rather than the logic.
    let rows: Vec<OrgRepoRow> = resolved_rows_of(catalogue)?;

    let client = reqwest::Client::new();
    let mut out = Vec::new();
    for row in &rows {
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

/// The `spec.variables` merge patch for an `InfrastructureTemplate`, from a
/// resolved record list.
///
/// ── ★ THE KEYS ARE A CONTRACT, NOT A CHOICE ────────────────────────────────
/// `github-org-repos.tlisp` reads `{owner}` and `{repo_count}` and loops over
/// `repos`; these names are that architecture's interpolation contract. Two
/// details are load-bearing and neither is guessable from the names:
///
/// - `repo_count` is a **string**. lava interpolates strings, so an integer
///   here renders differently from what the architecture expects while every
///   type in Rust and every schema in Kubernetes stays happy.
/// - `labels` is present **and empty**. The architecture indexes it, and an
///   ABSENT key is not an empty list to the interpolator.
///
/// Lives here rather than in `main` so the shape is testable and sits beside
/// the records it describes. `main`'s `--emit cr-patch` is a thin caller.
#[must_use]
pub fn cr_patch(owner: &str, records: &[RepoRecord]) -> serde_json::Value {
    serde_json::json!({
        "spec": {
            "variables": {
                "owner": owner,
                "repo_count": records.len().to_string(),
                "repos": records,
                "labels": [],
            }
        }
    })
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
        assert_eq!(
            BranchProtectionPreset::PILOT,
            BranchProtectionPreset::STANDARD
        );
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
            overlay: RepoOverlay {
                has_issues: Some(false),
                delete_branch_on_merge: Some(false),
                standard_labels: Some(true),
                ..Default::default()
            },
            ..Default::default()
        };
        let rec = record_for(&declared, None);
        assert_eq!(rec["has_issues"], "false", "98 rows declare this false");
        assert_eq!(
            rec["delete_branch_on_merge"], "false",
            "847 rows declare this false — the largest divergence found"
        );
        assert_eq!(rec["standard_labels"], "true");
    }

    /// Absent keys take the GEM's default, not a convenient one.
    #[test]
    fn absent_keys_take_the_gem_defaults() {
        let bare = OrgRepoRow {
            name: "r".into(),
            ..Default::default()
        };
        let rec = record_for(&bare, None);
        // types.rb:356 / :352 — both default true.
        assert_eq!(rec["has_issues"], "true");
        assert_eq!(rec["delete_branch_on_merge"], "true");
        // types.rb:363 — default FALSE. This was `unwrap_or(true)`.
        assert_eq!(rec["standard_labels"], "false");
        // types.rb:351
        assert_eq!(rec["default_branch"], "main");
    }

    /// `actions_enabled` is tri-state: absent derives from visibility.
    #[test]
    fn actions_enabled_is_tri_state_not_defaulted() {
        let with_vis = |v: &str| OrgRepoRow {
            name: "r".into(),
            overlay: RepoOverlay {
                visibility: Some(v.to_string()),
                ..Default::default()
            },
            ..Default::default()
        };
        // lava-resolve-org:113 — nil ? visibility != :internal : value
        assert_eq!(
            record_for(&with_vis("public"), None)["actions_enabled"],
            "true"
        );
        assert_eq!(
            record_for(&with_vis("private"), None)["actions_enabled"],
            "true"
        );
        assert_eq!(
            record_for(&with_vis("internal"), None)["actions_enabled"],
            "false"
        );

        // An explicit value wins over the derivation, in both directions.
        let explicit = |on: bool| OrgRepoRow {
            name: "r".into(),
            overlay: RepoOverlay {
                visibility: Some("public".into()),
                actions_enabled: Some(on),
                ..Default::default()
            },
            ..Default::default()
        };
        assert_eq!(
            record_for(&explicit(false), None)["actions_enabled"],
            "false"
        );
        assert_eq!(record_for(&explicit(true), None)["actions_enabled"], "true");
    }

    /// An archived repo is never protected, whatever preset it declares.
    #[test]
    fn archived_repos_are_not_protected() {
        let archived_protected = OrgRepoRow {
            name: "r".into(),
            overlay: RepoOverlay {
                archived: Some(true),
                branch_protection: Some("hardened".into()),
                ..Default::default()
            },
            ..Default::default()
        };
        let rec = record_for(&archived_protected, None);
        // lava-resolve-org:108 — !cfg[:archived] && profile != :none
        assert_eq!(rec["has_branch_protection"], "false");
        assert_eq!(rec["bp_enforce_admins"], "false");

        // ANTI-VACUITY: the same preset on a LIVE repo must protect, or this
        // test would pass against a record_for that never protects anything.
        // `merging`, not struct-update: `OrgRepoRow { overlay: .., ..base }`
        // would REPLACE the overlay and drop `branch_protection: hardened`,
        // leaving this anti-vacuity leg measuring nothing. That is exactly
        // what happened when the nesting first landed.
        let live = record_for(
            &archived_protected.merging(RepoOverlay {
                branch_protection: Some("hardened".into()),
                archived: Some(false),
                ..Default::default()
            }),
            None,
        );
        assert_eq!(live["has_branch_protection"], "true");
        assert_eq!(live["bp_enforce_admins"], "true");
    }

    /// `bp_strict` is a CONSTANT false — the Ruby's no-change value.
    #[test]
    fn bp_strict_is_always_the_no_change_value() {
        for bp in ["none", "pilot", "standard", "hardened"] {
            let row = OrgRepoRow {
                name: "r".into(),
                overlay: RepoOverlay {
                    branch_protection: Some(bp.to_string()),
                    ..Default::default()
                },
                ..Default::default()
            };
            assert_eq!(
                record_for(&row, None)["bp_strict"],
                "false",
                "the Ruby never sets required_status_checks_strict; deriving it \
                 from the preset invents a status-check policy it never emits"
            );
        }
    }

    /// The three names that render a BROKEN address without the slug.
    ///
    /// Measured over the live 1005-row catalogue: these are the entire broken
    /// set. Pinned by name because the failure is silent — lava emits the bad
    /// reference and the provider receives a literal `${...}` as a value.
    #[test]
    fn dotted_names_are_slugged_into_valid_identifiers() {
        assert_eq!(tf_slug("fastboot.js"), "fastboot_js");
        assert_eq!(tf_slug("compass.nvim"), "compass_nvim");
        // Leading dot slugs to a leading underscore, which is already a legal
        // identifier start — so NO `r_` prefix. Mirrors the Ruby's regex.
        assert_eq!(tf_slug(".github"), "_github");
    }

    /// Hyphens are LEGAL terraform identifiers, but the Ruby slugs them, and
    /// matching it is what lets either path adopt the other's state.
    #[test]
    fn hyphens_match_the_ruby_slug() {
        assert_eq!(tf_slug("pangea-operator"), "pangea_operator");
        assert_eq!(tf_slug("lava-architectures"), "lava_architectures");
    }

    /// A name that cannot START an identifier gets the Ruby's `r_` prefix.
    #[test]
    fn a_leading_digit_gets_the_r_prefix() {
        assert_eq!(tf_slug("2fa-tools"), "r_2fa_tools");
        // ANTI-VACUITY: an ordinary name must NOT be prefixed, or this test
        // would pass against a tf_slug that prefixed everything.
        assert_eq!(tf_slug("tend"), "tend");
    }

    /// `slug` is an ADDRESS, `name` is the repository. Conflating them would
    /// create a repo literally called `fastboot_js`.
    #[test]
    fn the_slug_never_replaces_the_real_name() {
        let r = OrgRepoRow {
            name: "fastboot.js".into(),
            ..Default::default()
        };
        let rec = record_for(&r, None);
        assert_eq!(rec["name"], "fastboot.js", "the repo keeps its real name");
        assert_eq!(rec["slug"], "fastboot_js", "only the address is slugged");
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
        OrgRepoRow {
            name: name.into(),
            ..Default::default()
        }
    }

    /// The adopt-vs-create switch, which is the reason this module exists.
    #[test]
    fn existence_drives_the_import_gate() {
        let absent = record_for(&row("openwrt-uci"), None);
        assert_eq!(absent["exists_on_github"], "false");

        let present = record_for(
            &row("openwrt-uci"),
            Some(&LiveRepo {
                visibility: "public".into(),
            }),
        );
        assert_eq!(present["exists_on_github"], "true");
    }

    /// Live visibility must reflect the WORLD, not the catalogue — otherwise the
    /// plan is a no-op on exactly the repos that drifted.
    #[test]
    fn live_visibility_comes_from_github_when_the_repo_exists() {
        let r = OrgRepoRow {
            overlay: RepoOverlay {
                visibility: Some("private".into()),
                ..Default::default()
            },
            ..row("drifted")
        };
        let rec = record_for(
            &r,
            Some(&LiveRepo {
                visibility: "public".into(),
            }),
        );
        assert_eq!(rec["visibility"], "private", "declared intent is preserved");
        assert_eq!(
            rec["live_visibility"], "public",
            "live state must not echo the catalogue"
        );
    }

    /// With no repo on GitHub there is no live state; falling back to the
    /// declared value keeps the field total rather than empty.
    #[test]
    fn live_visibility_falls_back_to_declared_when_absent() {
        let r = OrgRepoRow {
            overlay: RepoOverlay {
                visibility: Some("public".into()),
                ..Default::default()
            },
            ..row("new")
        };
        assert_eq!(record_for(&r, None)["live_visibility"], "public");
    }

    /// Every field the architecture interpolates must be present — UNPREFIXED.
    ///
    /// ── THE CONTRACT, AND THE MISTAKE THIS TEST USED TO MAKE ──────────────
    /// lava composes an interpolation name from the LOOP VARIABLE and the
    /// record KEY. `github-org-repos.tlisp` iterates
    /// `(for-each ((i repo) (enumerate repos)) ...)`, so the loop variable is
    /// `repo` and the placeholder `{repo_name}` resolves against the record
    /// key `name` — NOT `repo_name`.
    ///
    /// This test previously asserted the record contained `repo_name`,
    /// `repo_description` and so on. It passed, and it was checking the wrong
    /// thing: the resolver emitted keys already carrying the prefix, so lava
    /// computed `repo_repo_name` and every interpolation failed with
    ///
    ///   lava evaluation failed: interpolation: unknown var `repo_name`
    ///
    /// measured on plo 2026-09-06, the first time the architecture was ever
    /// evaluated with real records. The authority is
    /// `lava-architectures/tests/empty_resolve_is_silent.rs`, whose
    /// `set_records` call uses bare keys, and `bin/lava-resolve-org`'s FIELDS
    /// list, which is bare too.
    ///
    /// Reading an interpolation name as if it were a key is an easy mistake to
    /// make twice, so the list below is written as the PAIR — what lava writes,
    /// and what the record must hold — rather than as one column.
    #[test]
    fn every_interpolated_placeholder_maps_to_an_unprefixed_key() {
        let rec = record_for(&row("x"), None);
        // (placeholder the architecture writes, key the record must hold)
        for (placeholder, key) in [
            ("{repo_name}", "name"),
            ("{repo_description}", "description"),
            ("{repo_visibility}", "visibility"),
            ("{repo_has_issues}", "has_issues"),
            ("{repo_archived}", "archived"),
            ("{repo_delete_branch_on_merge}", "delete_branch_on_merge"),
            ("{repo_standard_labels}", "standard_labels"),
            ("{repo_actions_enabled}", "actions_enabled"),
            ("{repo_has_ci_shim}", "has_ci_shim"),
            ("{repo_ci_shim_slug}", "ci_shim_slug"),
            ("{repo_ci_shim_path}", "ci_shim_path"),
            ("{repo_ci_shim_content}", "ci_shim_content"),
            ("{repo_has_branch_protection}", "has_branch_protection"),
            ("{repo_default_branch}", "default_branch"),
            ("{repo_bp_strict}", "bp_strict"),
            ("{repo_bp_enforce_admins}", "bp_enforce_admins"),
            ("{repo_exists_on_github}", "exists_on_github"),
        ] {
            assert!(
                rec.contains_key(key),
                "architecture writes {placeholder}, so the record needs key `{key}` \
                 (loop var `repo` + `_` + key); record has: {:?}",
                rec.keys().collect::<Vec<_>>()
            );
            // The inverse, which is what actually broke: a key must NOT already
            // carry the prefix, or lava resolves `repo_<prefixed>` and fails.
            let prefixed = format!("repo_{key}");
            assert!(
                !rec.contains_key(prefixed.as_str()),
                "record key `{prefixed}` is pre-prefixed; lava would look for \
                 `repo_{prefixed}` and never find it"
            );
        }
    }

    /// Booleans are the STRINGS the `:when` gates compare against.
    #[test]
    fn booleans_render_as_when_gate_text() {
        let r = OrgRepoRow {
            overlay: RepoOverlay {
                branch_protection: Some("none".into()),
                ..Default::default()
            },
            ..row("p")
        };
        assert_eq!(record_for(&r, None)["has_branch_protection"], "false");
        let r2 = OrgRepoRow {
            overlay: RepoOverlay {
                branch_protection: Some("standard".into()),
                ..Default::default()
            },
            ..row("p")
        };
        assert_eq!(record_for(&r2, None)["has_branch_protection"], "true");
    }

    /// A row with only a name must parse — the catalogue is hand-maintained and
    /// most keys are optional.
    #[test]
    fn a_minimal_row_parses_and_projects() {
        let cat: OrgCatalogue = serde_yaml::from_str("repos:\n  - name: solo\n").expect("parses");
        assert_eq!(cat.repos.len(), 1);
        let rec = record_for(&cat.repos[0], None);
        assert_eq!(rec["name"], "solo");
        assert_eq!(
            rec["visibility"], "private",
            "unstated visibility defaults closed"
        );
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
            "openwrt-uci",
            "ancora",
            "annai",
            "jikoku",
            "nanori",
            "roji",
            "camelot-incept",
            "lava-discord",
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
                r["name"], r["exists_on_github"], r["live_visibility"], r["visibility"]
            );
        }
        assert_eq!(
            records.len(),
            only.len(),
            "every requested repo must resolve"
        );
    }

    // ── the cr-patch shape, pinned against what is LIVE ────────────────────
    // Measured 2026-09-07 against the InfrastructureTemplate on plo: 4
    // variable keys, and `repo_count` a string. These assertions exist because
    // both facts are invisible from the type system — an integer repo_count
    // type-checks in Rust, validates in Kubernetes, and renders wrong in lava.

    fn rec(name: &str) -> RepoRecord {
        record_for(
            &OrgRepoRow {
                name: name.into(),
                ..Default::default()
            },
            None,
        )
    }

    #[test]
    fn cr_patch_has_exactly_the_four_live_variable_keys() {
        let patch = cr_patch("pleme-io", &[rec("tend")]);
        let vars = patch["spec"]["variables"]
            .as_object()
            .expect("variables is an object");
        let mut keys: Vec<&str> = vars.keys().map(String::as_str).collect();
        keys.sort_unstable();
        assert_eq!(
            keys,
            ["labels", "owner", "repo_count", "repos"],
            "the live spec.variables carries exactly these four; a fifth key or \
             a missing one is a divergence from the lava architecture's contract"
        );
    }

    #[test]
    fn repo_count_is_a_string_because_lava_interpolates_strings() {
        let patch = cr_patch("pleme-io", &[rec("tend"), rec("codesearch")]);
        assert_eq!(
            patch["spec"]["variables"]["repo_count"],
            serde_json::json!("2"),
            "an integer type-checks in Rust, validates in Kubernetes, and \
             renders differently in lava — which is exactly why this is asserted"
        );
    }

    #[test]
    fn labels_is_present_and_empty_not_absent() {
        let patch = cr_patch("pleme-io", &[rec("tend")]);
        let vars = &patch["spec"]["variables"];
        assert!(
            vars.get("labels").is_some(),
            "the architecture indexes `labels`; an absent key is not an empty \
             list to the interpolator"
        );
        assert_eq!(vars["labels"], serde_json::json!([]));
    }

    #[test]
    fn repo_count_tracks_the_record_list_length() {
        // ANTI-VACUITY: without this, a `cr_patch` that hardcoded a count
        // would pass the string-type test above. The count and the list must
        // agree, because the architecture loops `repo_count` times.
        for n in [1_usize, 3, 7] {
            let records: Vec<RepoRecord> = (0..n).map(|i| rec(&format!("r{i}"))).collect();
            let patch = cr_patch("pleme-io", &records);
            assert_eq!(
                patch["spec"]["variables"]["repo_count"],
                serde_json::json!(n.to_string()),
                "repo_count must equal repos.len()"
            );
            assert_eq!(
                patch["spec"]["variables"]["repos"]
                    .as_array()
                    .expect("repos is an array")
                    .len(),
                n
            );
        }
    }

    // ── the catalogue validator ────────────────────────────────────────────
    // Every rule here cost a real diagnosis. The point of the tests is not
    // that the checks fire, but that they fire on the shapes that actually
    // reached production and stayed invisible until apply time.

    #[test]
    fn an_over_long_description_is_refused_at_the_boundary() {
        // jikou's real shape: 365 characters, a 422 from GitHub, surfaced only
        // after 1005 lookups and a full plan.
        let cat = OrgCatalogue {
            repos: vec![OrgRepoRow {
                name: "jikou".into(),
                overlay: RepoOverlay {
                    description: Some("x".repeat(365)),
                    ..Default::default()
                },
                ..Default::default()
            }],
            ..Default::default()
        };
        let v = validate_catalogue(&cat);
        assert_eq!(v.len(), 1, "one row, one violation");
        assert_eq!(v[0].repo, "jikou");
        assert_eq!(v[0].field, "description");
        assert!(v[0].detail.contains("365"), "the count must name itself");
    }

    #[test]
    fn a_description_exactly_at_the_limit_is_accepted() {
        // ANTI-VACUITY, and the direction that matters: an off-by-one here
        // rejects rows GitHub accepts, which is its own defect. 348 is the
        // length jikou was corrected to and which applied cleanly.
        for n in [1_usize, 348, DESCRIPTION_MAX_CHARS] {
            let cat = OrgCatalogue {
                repos: vec![OrgRepoRow {
                    name: "r".into(),
                    overlay: RepoOverlay {
                        description: Some("x".repeat(n)),
                        ..Default::default()
                    },
                    ..Default::default()
                }],
                ..Default::default()
            };
            assert!(
                validate_catalogue(&cat).is_empty(),
                "{n} characters is within GitHub's limit and must be accepted"
            );
        }
    }

    #[test]
    fn the_description_limit_counts_characters_not_bytes() {
        // 350 em-dashes is 350 characters and 1050 bytes. `len()` would refuse
        // it; GitHub does not. A false refusal is still a defect.
        let cat = OrgCatalogue {
            repos: vec![OrgRepoRow {
                name: "r".into(),
                overlay: RepoOverlay {
                    description: Some("—".repeat(DESCRIPTION_MAX_CHARS)),
                    ..Default::default()
                },
                ..Default::default()
            }],
            ..Default::default()
        };
        assert!(
            validate_catalogue(&cat).is_empty(),
            "GitHub counts characters; a byte-length check would reject this"
        );
    }

    #[test]
    fn a_bad_name_is_refused_rather_than_read_as_absent() {
        // The insidious one: an unacceptable name 404s on lookup, which
        // `look_up` correctly maps to ABSENT — so without this check a typo'd
        // name presents as a repository needing CREATION.
        let cat = OrgCatalogue {
            repos: vec![OrgRepoRow {
                name: "bad name!".into(),
                ..Default::default()
            }],
            ..Default::default()
        };
        let v = validate_catalogue(&cat);
        assert_eq!(v.len(), 1);
        assert_eq!(v[0].field, "name");
    }

    #[test]
    fn legal_name_characters_are_accepted() {
        for n in ["tend", "repo-forge", "blackmatter_pleme", "a.b", "x0"] {
            let cat = OrgCatalogue {
                repos: vec![OrgRepoRow {
                    name: n.into(),
                    ..Default::default()
                }],
                ..Default::default()
            };
            assert!(
                validate_catalogue(&cat).is_empty(),
                "{n} is a legal GitHub repository name"
            );
        }
    }

    #[test]
    fn a_visibility_typo_is_refused_and_case_matters() {
        for bad in ["publi", "Public", "PRIVATE", "open"] {
            let cat = OrgCatalogue {
                repos: vec![OrgRepoRow {
                    name: "r".into(),
                    overlay: RepoOverlay {
                        visibility: Some(bad.into()),
                        ..Default::default()
                    },
                    ..Default::default()
                }],
                ..Default::default()
            };
            let v = validate_catalogue(&cat);
            assert_eq!(v.len(), 1, "{bad:?} must be refused");
            assert_eq!(v[0].field, "visibility");
        }
        for good in ["public", "private", "internal"] {
            let cat = OrgCatalogue {
                repos: vec![OrgRepoRow {
                    name: "r".into(),
                    overlay: RepoOverlay {
                        visibility: Some(good.into()),
                        ..Default::default()
                    },
                    ..Default::default()
                }],
                ..Default::default()
            };
            assert!(validate_catalogue(&cat).is_empty(), "{good:?} is valid");
        }
    }

    #[test]
    fn every_violation_is_reported_not_just_the_first() {
        // An author fixing a catalogue wants one pass, not one apply cycle per
        // defect.
        let cat = OrgCatalogue {
            repos: vec![
                OrgRepoRow {
                    name: "a".into(),
                    overlay: RepoOverlay {
                        description: Some("x".repeat(400)),
                        ..Default::default()
                    },
                    ..Default::default()
                },
                OrgRepoRow {
                    name: "b!".into(),
                    ..Default::default()
                },
                OrgRepoRow {
                    name: "c".into(),
                    overlay: RepoOverlay {
                        visibility: Some("publi".into()),
                        ..Default::default()
                    },
                    ..Default::default()
                },
            ],
            ..Default::default()
        };
        let v = validate_catalogue(&cat);
        assert_eq!(v.len(), 3, "all three, in one pass");
        let fields: Vec<&str> = v.iter().map(|x| x.field).collect();
        assert_eq!(fields, ["description", "name", "visibility"]);
    }

    #[test]
    fn the_real_catalogue_shape_passes() {
        // ANTI-VACUITY for the whole validator: a check that refuses
        // everything is as useless as one that refuses nothing. This is the
        // shape of a real row from org.yaml.
        let cat = OrgCatalogue {
            repos: vec![OrgRepoRow {
                name: "codesearch".into(),
                overlay: RepoOverlay {
                    description: Some(
                        "Fast, local semantic code search as MCP server for OpenCode and \
                     Claude Code. Rust-powered, fully offline."
                            .into(),
                    ),
                    visibility: Some("private".into()),
                    ..Default::default()
                },
                ..Default::default()
            }],
            ..Default::default()
        };
        assert!(validate_catalogue(&cat).is_empty());
    }


    /// ★ THE GATE FOR THE BUG THAT SHIPPED: two row sources, one folded.
    ///
    /// `resolve` read `catalogue.repos` directly while `--emit overlays` read
    /// the FOLDED rows, so `repo_defaults` was parsed, validated, tested, and
    /// then ignored by the only path the operator runs. The seed unit logged
    /// `infrastructuretemplate … patched` and changed nothing, because the
    /// patch was byte-identical to what was already there.
    ///
    /// 1444 unit tests did not catch it, and could not have: every one of
    /// them exercised the fold directly, and `resolve` needs a network. So
    /// the gate is SOURCE-LEVEL, the same shape as the nix repo's
    /// `noBespokeNixosSystemCalls` — count the call sites and fail on a new
    /// one, rather than try to test a behaviour that needs the world.
    ///
    /// Exactly two places may read RAW rows:
    ///   - `resolved_rows`, which is the fold
    ///   - `validate_catalogue`, which SHOULD see raw rows: validation is
    ///     about what the author actually wrote, so folding first would let a
    ///     row inherit its way past a refusal
    ///
    /// A third is a consumer that silently skips the tiers.
    #[test]
    fn only_the_fold_and_the_validator_may_read_raw_rows() {
        // ── ★ COUNT THE PRODUCTION HALF ONLY ─────────────────────────────
        // The first version of this gate counted the WHOLE file and found 3
        // instead of 1 — because this test's own doc comment and assertion
        // messages quote the pattern. A source-level gate is part of the
        // source it inspects.
        //
        // Truncating at `#[cfg(test)]` is also the semantically right scope: a
        // test that iterates rows is not a production consumer of them, so it
        // should not be able to trip this. Worth noting the gate DID fire on
        // itself, which is at least proof it is not vacuous.
        let whole = include_str!("org_resolve.rs");
        let src = whole
            .split_once("#[cfg(test)]")
            .map_or(whole, |(production, _tests)| production);

        let self_repos = src.matches("for row in &self.repos").count();
        assert_eq!(
            self_repos, 1,
            "`for row in &self.repos` must appear exactly once (in `resolved_rows`, \
             which IS the fold); found {self_repos}"
        );

        let catalogue_repos = src.matches("for row in &catalogue.repos").count();
        assert_eq!(
            catalogue_repos, 1,
            "`for row in &catalogue.repos` must appear exactly once (in \
             `validate_catalogue`, which validates what the author WROTE). Found \
             {catalogue_repos} — a second one is a consumer that skips \
             repo_defaults/repo_profiles entirely, which is exactly the bug this \
             gate exists to catch. Route it through `resolved_rows_of` instead."
        );

        // ANTI-VACUITY: the strings must actually be present, or the two
        // assertions above would pass on a file that had been renamed out
        // from under them and this gate would guard nothing.
        assert!(
            src.contains("fn resolved_rows_of"),
            "the single fold entry point must exist for the counts above to mean anything"
        );
        assert!(
            src.contains("let rows: Vec<OrgRepoRow> = resolved_rows_of(catalogue)?;"),
            "`resolve` must source its rows from the fold; if this line moved, the \
             counts above no longer prove resolve folds"
        );
    }
    // ── THE TIER FOLD ─────────────────────────────────────────────────────
    // What the user asked for and what was missing: config for all
    // workspaces, for one workspace, and for each element within it, in a
    // reusable stackable mergeable way. These tests are the contract.

    fn cat(yaml: &str) -> OrgCatalogue {
        serde_yaml::from_str(yaml).expect("catalogue parses")
    }

    #[test]
    fn a_workspace_default_reaches_every_row() {
        // The whole point: state it once instead of on 1005 rows.
        let c = cat("repo_defaults:\n  delete_branch_on_merge: false\n\
             repos:\n  - name: a\n  - name: b\n");
        let rows = c.resolved_rows().expect("folds");
        assert_eq!(rows.len(), 2);
        for (row, _) in &rows {
            assert_eq!(
                row.overlay.delete_branch_on_merge,
                Some(false),
                "row {:?} must inherit the workspace default",
                row.name
            );
        }
    }

    #[test]
    fn this_is_the_bug_that_motivated_it() {
        // Measured on pleme-io-opensource 2026-09-07: 43 rows omit
        // `delete_branch_on_merge`, so they resolve to the GEM's `true` while
        // 847 of their siblings declare `false`. The inherited value
        // DISAGREES with the corpus, and nothing says so, because a row that
        // omits a key reads exactly like a row that agrees with the default.
        //
        // Without a workspace tier there is nowhere to fix that except by
        // editing 43 rows and hoping the 44th never appears.
        let without = cat("repos:\n  - name: forgot\n");
        let (row, _) = &without.resolved_rows().expect("folds")[0];
        assert_eq!(
            row.overlay.delete_branch_on_merge, None,
            "unset at every tier, so record_for falls to the gem's true"
        );
        assert_eq!(
            record_for(row, None)["delete_branch_on_merge"],
            "true",
            "the gem default — which 847 rows contradict"
        );

        let with = cat("repo_defaults:\n  delete_branch_on_merge: false\n\
             repos:\n  - name: forgot\n");
        let (row, prov) = &with.resolved_rows().expect("folds")[0];
        assert_eq!(
            record_for(row, None)["delete_branch_on_merge"],
            "false",
            "one line in one place fixes the row that forgot, and every future one"
        );
        assert_eq!(
            prov.get("delete_branch_on_merge"),
            Some(&Tier::WorkspaceDefaults),
            "and the provenance says WHERE it came from — the question that \
             was unanswerable, which is why the wrong default stayed invisible"
        );
    }

    #[test]
    fn a_row_overrides_the_workspace_default() {
        let c = cat("repo_defaults:\n  has_issues: false\n\
             repos:\n  - name: keeps\n  - name: wants\n    has_issues: true\n");
        let rows = c.resolved_rows().expect("folds");
        let by = |n: &str| {
            rows.iter()
                .find(|(r, _)| r.name == n)
                .expect("row present")
                .clone()
        };
        assert_eq!(by("keeps").0.overlay.has_issues, Some(false));
        assert_eq!(by("wants").0.overlay.has_issues, Some(true));
        assert_eq!(by("wants").1.get("has_issues"), Some(&Tier::Row));
        assert_eq!(
            by("keeps").1.get("has_issues"),
            Some(&Tier::WorkspaceDefaults)
        );
    }

    #[test]
    fn a_profile_sits_between_the_workspace_and_the_row() {
        // The reusable middle tier: a CLASS of repository states its shape
        // once. `rust-library` is the obvious real one — 139 rows carry a
        // `ci_shim`, 77% of them the same value.
        let c = cat("repo_defaults:\n  has_issues: false\n  visibility: private\n\
             repo_profiles:\n\
             \u{20} rust-library:\n    has_issues: true\n    standard_labels: true\n\
             repos:\n\
             \u{20} - name: plain\n\
             \u{20} - name: lib\n    inherits: [rust-library]\n\
             \u{20} - name: lib-quiet\n    inherits: [rust-library]\n    has_issues: false\n");
        let rows = c.resolved_rows().expect("folds");
        let by = |n: &str| {
            rows.iter()
                .find(|(r, _)| r.name == n)
                .expect("row present")
                .clone()
        };

        // no profile -> workspace answer
        assert_eq!(by("plain").0.overlay.has_issues, Some(false));
        assert_eq!(by("plain").0.overlay.standard_labels, None);

        // profile beats workspace
        assert_eq!(by("lib").0.overlay.has_issues, Some(true));
        assert_eq!(by("lib").0.overlay.standard_labels, Some(true));
        assert_eq!(
            by("lib").1.get("has_issues"),
            Some(&Tier::Profile("rust-library".into()))
        );
        // and a setting the profile does NOT speak to still inherits
        assert_eq!(
            by("lib").0.overlay.visibility.as_deref(),
            Some("private"),
            "the profile is an overlay, not a replacement — siblings survive"
        );

        // row beats profile
        assert_eq!(by("lib-quiet").0.overlay.has_issues, Some(false));
        assert_eq!(by("lib-quiet").1.get("has_issues"), Some(&Tier::Row));
        assert_eq!(
            by("lib-quiet").0.overlay.standard_labels,
            Some(true),
            "overriding one setting must not drop the rest of the profile"
        );
    }

    #[test]
    fn profiles_stack_in_the_order_named() {
        let c = cat("repo_profiles:\n\
             \u{20} base:\n    has_issues: true\n    standard_labels: true\n\
             \u{20} quiet:\n    has_issues: false\n\
             repos:\n\
             \u{20} - name: a\n    inherits: [base, quiet]\n\
             \u{20} - name: b\n    inherits: [quiet, base]\n");
        let rows = c.resolved_rows().expect("folds");
        let by = |n: &str| rows.iter().find(|(r, _)| r.name == n).expect("row").clone();
        assert_eq!(by("a").0.overlay.has_issues, Some(false), "quiet last wins");
        assert_eq!(by("b").0.overlay.has_issues, Some(true), "base last wins");
        // ANTI-VACUITY: a setting only one profile speaks to survives either order
        assert_eq!(by("a").0.overlay.standard_labels, Some(true));
        assert_eq!(by("b").0.overlay.standard_labels, Some(true));
    }

    #[test]
    fn an_unknown_inherit_is_refused_not_ignored() {
        // Ignoring it would leave the row on the gem defaults while the file
        // read as though a profile applied — the same silent class the whole
        // tier structure exists to close.
        let c = cat("repos:\n  - name: a\n    inherits: [nope]\n");
        let err = c.resolved_rows().expect_err("must refuse");
        assert!(err.contains("nope"), "the error names the profile: {err}");
        assert!(err.contains('a'), "and the row: {err}");
    }

    #[test]
    fn a_typo_in_a_shared_tier_is_refused_at_parse() {
        // `deny_unknown_fields` on RepoOverlay. A typo in a tier 1005 rows
        // inherit from is silent and total: `has_issue` would never be read,
        // every row would keep the gem's answer, and the file would read as
        // though it had been configured.
        assert!(
            serde_yaml::from_str::<OrgCatalogue>("repo_defaults:\n  has_issue: true\n").is_err(),
            "an unknown key in repo_defaults must be refused"
        );
        assert!(
            serde_yaml::from_str::<OrgCatalogue>(
                "repo_profiles:\n  p:\n    delete_branch_on_merg: true\n"
            )
            .is_err(),
            "an unknown key in a profile must be refused"
        );
        // ...while a ROW stays tolerant, deliberately: org.yaml rows carry
        // keys other consumers read and this Rust does not model.
        let c = cat("repos:\n  - name: a\n    license: MIT\n    topics: [x]\n");
        assert_eq!(
            c.repos.len(),
            1,
            "a row's extra keys must not break the org"
        );
    }

    #[test]
    fn the_fold_needs_no_field_list_and_so_cannot_go_stale() {
        // ★ THE STALENESS GATE. A per-field merge goes stale the first time a
        // field is added — the new field silently stops inheriting, which
        // presents as a workspace default that works for every setting but
        // one. `overlay_onto` merges in value space, so this asserts that
        // EVERY serializable field of RepoOverlay participates, by counting
        // rather than by listing.
        //
        // Add a field to RepoOverlay and this test covers it with no edit. If
        // it ever fails, the fold has acquired a field list somewhere.
        let full = RepoOverlay {
            description: Some("d".into()),
            visibility: Some("public".into()),
            archived: Some(true),
            branch_protection: Some("hardened".into()),
            standard_labels: Some(true),
            has_issues: Some(true),
            delete_branch_on_merge: Some(true),
            actions_enabled: Some(true),
        };
        let as_map = serde_json::to_value(&full)
            .expect("serializes")
            .as_object()
            .expect("object")
            .clone();
        let n = as_map.len();
        assert!(
            n >= 8,
            "expected every RepoOverlay field to serialize, got {n}"
        );

        // Every one of those keys must reach a bare row through the fold.
        let mut yaml = String::from("repo_defaults:\n");
        for (k, v) in &as_map {
            yaml.push_str(&format!("  {k}: {v}\n"));
        }
        yaml.push_str("repos:\n  - name: bare\n");
        let c = cat(&yaml);
        let (row, prov) = &c.resolved_rows().expect("folds")[0];
        let got = serde_json::to_value(&row.overlay)
            .expect("serializes")
            .as_object()
            .expect("object")
            .clone();
        assert_eq!(
            got.len(),
            n,
            "every field set in repo_defaults must reach the row; \
             {} of {n} arrived — the fold has a field list that went stale",
            got.len()
        );
        for k in as_map.keys() {
            assert_eq!(
                prov.get(k),
                Some(&Tier::WorkspaceDefaults),
                "field {k:?} did not inherit from the workspace tier"
            );
        }
    }

    #[test]
    fn an_empty_catalogue_folds_to_nothing_rather_than_erroring() {
        // The tiers are additive: a catalogue with no repo_defaults and no
        // profiles must behave exactly as before they existed. This is the
        // backwards-compatibility leg — 1005 live rows depend on it.
        let c = cat("repos:\n  - name: a\n    has_issues: false\n");
        let (row, prov) = &c.resolved_rows().expect("folds")[0];
        assert_eq!(row.overlay.has_issues, Some(false));
        assert_eq!(prov.get("has_issues"), Some(&Tier::Row));
        assert_eq!(prov.len(), 1, "only the key the row actually set");
    }
}
