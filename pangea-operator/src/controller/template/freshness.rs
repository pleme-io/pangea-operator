//! Source-freshness model for `InfrastructureTemplate` reconcile —
//! the typed gate that makes "Ready/Settled against a stale compile"
//! mechanically unutterable.
//!
//! Shape mirrors `evaluate_compile_failure_escalation`: the decision
//! fns are pure (kube-free, I/O-free) so the law "a stale compile can
//! never produce Settled" is a unit test, not an operational hope.
//! The one I/O fn here is [`observe_head`] (`git ls-remote`, 1 RTT,
//! no clone); its compile-side twin `git_rev_parse_head` lives next
//! to the clone block in `template_controller`.
//!
//! Tier-honest: freshness is a **C2 external-world observation** —
//! "Ready is true" is renewed per check (bounded by the refresh
//! interval + the [`Freshness::Unknown`] arm), never proven at
//! compile time. No compile error can prove a remote's HEAD.

use crate::error::{Error, Result};
use gix::bstr::ByteSlice;

/// The freshness verdict for one template's git source.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Freshness {
    /// The compiled revision IS the observed remote HEAD.
    Fresh,
    /// The remote moved past the compiled revision — or no revision
    /// was ever recorded (`compiled: None`, the legacy-CR case, which
    /// converges by exactly one recompile).
    Stale {
        compiled: Option<String>,
        head: String,
    },
    /// The remote could not be observed (ls-remote unreachable).
    /// Constructed by the caller — never by
    /// [`evaluate_source_freshness`], which requires an observation.
    /// The drift loop proceeds on Unknown (never wedges), but the
    /// Settled condition message says "HEAD: unverified" and the
    /// `pangea_source_freshness_check_failures_total` counter ticks.
    Unknown,
}

/// Pure freshness comparison. `None` compiled ⇒ `Stale` — a legacy CR
/// that predates `status.compiledRevision` converges by one
/// recompile rather than being silently grandfathered as fresh.
pub fn evaluate_source_freshness(compiled: Option<&str>, observed_head: &str) -> Freshness {
    match compiled {
        Some(rev) if rev == observed_head => Freshness::Fresh,
        other => Freshness::Stale {
            compiled: other.map(str::to_string),
            head: observed_head.to_string(),
        },
    }
}

/// What the Ready-phase drift check is allowed to conclude.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadyAction {
    /// The compile is stale — bounce to Compiling before trusting any
    /// plan result. Drift correction must never apply a stale compile.
    RecompileStale,
    /// Fresh (or unverifiable) compile + no plan changes — settled.
    Settled,
    /// Fresh (or unverifiable) compile + plan changes — drift.
    Drifted,
}

/// The pure Ready-phase decision: **"no changes" structurally cannot
/// be uttered against a stale compile.** `Unknown` proceeds (the
/// drift loop must not wedge on a flaky remote) — the honesty lives
/// in the Settled condition message, which names the unverified HEAD.
pub fn ready_drift_decision(freshness: &Freshness, plan_has_changes: bool) -> ReadyAction {
    match (freshness, plan_has_changes) {
        (Freshness::Stale { .. }, _) => ReadyAction::RecompileStale,
        (Freshness::Fresh | Freshness::Unknown, false) => ReadyAction::Settled,
        (Freshness::Fresh | Freshness::Unknown, true) => ReadyAction::Drifted,
    }
}

/// An HTTPS git credential, held **in memory only**.
///
/// ★ This type exists to make the old mechanism unwritable, not merely
/// unused. The subprocess path authenticated by writing `_git_user`,
/// `_git_pass` (0600, the `ghs_` installation token) and a `#!/bin/sh`
/// `_git_askpass.sh` into the workspace on every reconcile — a token on
/// disk, and the operator's ONLY remaining `/bin/sh` dependency. There is
/// deliberately no `to_env()`, no `Display`, and no `AsRef<str>` here: the
/// credential can reach a transport and nothing else.
///
/// Tier-honest: parse-time-rejected WITHIN this module, not
/// truly-unrepresentable — a caller holding the token string can still
/// format it. That is bounded by TYPED EMISSION, which is CI-tier.
#[derive(Clone, Default)]
pub enum GitCredential {
    /// No `secretRef` — a public repo, fetched anonymously.
    #[default]
    Anonymous,
    /// HTTPS basic auth: username + token.
    Basic { username: String, token: String },
}

impl std::fmt::Debug for GitCredential {
    /// Redacting by construction — a token cannot reach a log through
    /// `{:?}`, which is how most secrets actually escape.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Anonymous => f.write_str("GitCredential::Anonymous"),
            Self::Basic { username, .. } => {
                write!(
                    f,
                    "GitCredential::Basic {{ username: {username:?}, token: <redacted> }}"
                )
            }
        }
    }
}

/// `HEAD`'s commit id in `repo_dir` — the commit a just-landed
/// clone/fetch checked out. The freshness model's compile-side
/// anchor; pairs with [`observe_head`] (the remote-side observation).
///
/// In-process (`gix::open` + `head_id`). The previous `git rev-parse
/// HEAD` subprocess was a SECOND, independent production break from the
/// same missing binary: its error propagates through a bare `?` in
/// `handle_compiling`, so it failed the whole compile rather than
/// degrading to `Freshness::Unknown` the way `observe_head` does.
///
/// The returned id is hex BY CONSTRUCTION — `gix::ObjectId` cannot hold
/// a non-hex value, closing the old path's habit of returning the entire
/// trimmed stdout as "the SHA" with no validation.
pub async fn git_rev_parse_head(repo_dir: &std::path::Path) -> Result<String> {
    let dir = repo_dir.to_path_buf();
    tokio::task::spawn_blocking(move || {
        let repo = gix::open(&dir).map_err(|e| {
            Error::Compilation(format!("rev-parse HEAD in {} failed: {e}", dir.display()))
        })?;
        let id = repo.head_id().map_err(|e| {
            Error::Compilation(format!("rev-parse HEAD in {} failed: {e}", dir.display()))
        })?;
        Ok(id.to_string())
    })
    .await
    .map_err(|e| Error::Io(std::io::Error::other(e)))?
}

/// Observe the remote's HEAD for `git_ref` — one round trip, no clone,
/// entirely in-process.
///
/// A 40-hex SHA ref is returned as-is (a SHA-pinned source is
/// definitionally at its pinned revision and must not acquire a network
/// call). A ref that resolves to nothing is an error — the caller maps
/// any error here to [`Freshness::Unknown`].
///
/// ── TWO DELIBERATE BEHAVIOUR CHANGES ────────────────────────────────
///
/// **1. Ref selection is now EXPLICIT.** `git ls-remote <url> <ref>`
/// matches every namespace whose last component equals `<ref>`, and the
/// old code took the FIRST LINE — i.e. the winner was an alphabetical
/// accident. With `refs/changes/main` present alongside `refs/heads/main`,
/// `refs/changes/main` sorts first and freshness compared against the
/// wrong commit, while `clone --branch main` checked out `refs/heads/main`
/// — a permanent `Stale` → `RecompileStale` loop. Precedence is now
/// heads → tags → exact full name.
///
/// **2. Annotated tags now resolve to the COMMIT.** `ls-remote v1`
/// returns the tag OBJECT id while `clone --branch v1` + rev-parse HEAD
/// returns the peeled commit, so every template pinned to an annotated
/// tag was permanently Stale and recompiling forever. `Peeled { object }`
/// is the commit, and that is what is returned. In the field this will
/// read as "freshness suddenly changed" — it is the loop turning OFF.
pub async fn observe_head(url: &str, git_ref: &str, cred: &GitCredential) -> Result<String> {
    if git_ref.len() == 40 && git_ref.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Ok(git_ref.to_string());
    }
    let (u, r, c) = (url.to_string(), git_ref.to_string(), cred.clone());
    let handle = tokio::task::spawn_blocking(move || ls_remote_blocking(&u, &r, &c));
    // The outer bound is retained ONLY to preserve the `Error::Timeout(30)`
    // arm callers already render. Honest limit: `timeout` ABANDONS a
    // `spawn_blocking` handle, it cannot cancel it — so a stalled response
    // body leaks the thread. `connect_timeout` below bounds the handshake;
    // nothing bounds a stalled body. That is a real, named gap.
    tokio::time::timeout(std::time::Duration::from_secs(30), handle)
        .await
        .map_err(|_| Error::Timeout(30))?
        .map_err(|e| Error::Io(std::io::Error::other(e)))?
}

/// The blocking core of [`observe_head`]. gitoxide has no async HTTP
/// transport, so this runs on a blocking thread by necessity — the
/// arrangement P0 measured to be panic-free on upstream gix 0.83 (the
/// "connect panics on background threads" claim in sui belongs to a
/// retired fork).
fn ls_remote_blocking(url: &str, git_ref: &str, cred: &GitCredential) -> Result<String> {
    let io_err = |e: String| Error::Io(std::io::Error::other(e));
    let not_found = || {
        Error::Compilation(format!(
            "ls-remote {url} {git_ref}: ref not found on remote"
        ))
    };

    // An empty in-memory repo: this is a listing, so nothing is written
    // and no worktree exists. `gix` needs a repo handle to hang a remote
    // off; the directory is discarded when `tmp` drops.
    let tmp = tempfile::tempdir().map_err(Error::Io)?;
    let repo = gix::init_bare(tmp.path()).map_err(|e| io_err(format!("init: {e}")))?;

    let remote = repo
        .remote_at(url)
        .map_err(|e| io_err(format!("remote_at {url}: {e}")))?
        // Without refspecs the listing comes back EMPTY — measured against
        // a private remote, where a bare `remote_at` returned 0 refs while
        // the same call with these refspecs returned refs/heads/main.
        .with_refspecs(
            [
                "+refs/heads/*:refs/remotes/origin/*",
                "+refs/tags/*:refs/tags/*",
            ],
            gix::remote::Direction::Fetch,
        )
        .map_err(|e| io_err(format!("refspecs: {e}")))?;

    let mut conn = remote
        .connect(gix::remote::Direction::Fetch)
        .map_err(|e| io_err(format!("connect {url}: {e}")))?;

    // ★ Credentials are supplied here and NOWHERE else. Passing an explicit
    // helper also SUPPRESSES gix's default, which consults the ambient
    // credential helper — measured: with no helper set, an unauthenticated
    // probe against a PRIVATE repo succeeded on a workstation (the OS
    // keychain answered) and would fail in a distroless pod. Binding the
    // credential explicitly removes that dev/prod split.
    if let GitCredential::Basic { username, token } = cred {
        let account = gix::sec::identity::Account {
            username: username.clone(),
            password: token.clone(),
            oauth_refresh_token: None,
        };
        conn = conn.with_credentials(move |action| match action {
            gix::credentials::helper::Action::Get(ctx) => {
                Ok(Some(gix::credentials::protocol::Outcome {
                    identity: account.clone(),
                    next: ctx.into(),
                }))
            }
            // Store/Erase are helper bookkeeping; there is no helper.
            _ => Ok(None),
        });
    }

    let (map, _handshake) = conn
        .ref_map(gix::progress::Discard, Default::default())
        .map_err(|e| io_err(format!("ls-remote {url} {git_ref}: {e}")))?;

    select_ref(&map.remote_refs, git_ref).ok_or_else(not_found)
}

/// Pure ref selection — the half of `observe_head` that is worth a unit
/// test. Precedence: `refs/heads/<r>` → `refs/tags/<r>` → exact full name.
///
/// Kept free of I/O so the "which ref wins" law is a test rather than an
/// operational hope; see the two behaviour changes on [`observe_head`].
fn select_ref(refs: &[gix::protocol::handshake::Ref], git_ref: &str) -> Option<String> {
    let candidates = [
        format!("refs/heads/{git_ref}"),
        format!("refs/tags/{git_ref}"),
        git_ref.to_string(),
    ];
    for want in &candidates {
        for r in refs {
            let (name, oid, peeled) = r.unpack();
            if name.to_str_lossy().as_ref() != want.as_str() {
                continue;
            }
            // An annotated tag advertises the TAG object; `peeled` is the
            // commit it points at, and the commit is what a checkout of
            // that ref produces. Prefer it.
            if let Some(commit) = peeled {
                return Some(commit.to_string());
            }
            if let Some(direct) = oid {
                return Some(direct.to_string());
            }
            // `Unborn` — advertised but pointing at nothing yet.
            return None;
        }
    }
    None
}

/// Augment a git auth env with the NON-INTERACTIVE guardrails that make
/// a misconfigured credential helper FAIL FAST instead of hanging to the
/// timeout (the concrete cause of the persistent `Unknown`/frozen-HEAD
/// wedge on rio: a misfiring `GIT_ASKPASS` against a private repo blocks
/// for the full 30s, errors, and the caller proceeds against a stale
/// compile). Pure + testable: no subprocess, no I/O.
///
/// - `GIT_TERMINAL_PROMPT=0` — git never prompts on a TTY/askpass miss.
/// - `GIT_CONFIG_NOSYSTEM=1` + `GIT_CONFIG_GLOBAL=/dev/null` — ignore any
///   ambient/system git config (a stray `credential.helper` can't hang us).
#[must_use]
pub fn non_interactive_git_env(base: &[(String, String)]) -> Vec<(String, String)> {
    let mut env: Vec<(String, String)> = base.to_vec();
    for (k, v) in [
        ("GIT_TERMINAL_PROMPT", "0"),
        ("GIT_CONFIG_NOSYSTEM", "1"),
        ("GIT_CONFIG_GLOBAL", "/dev/null"),
    ] {
        if !env.iter().any(|(ek, _)| ek == k) {
            env.push((k.to_string(), v.to_string()));
        }
    }
    env
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── non_interactive_git_env (pure) ───────────────────────────

    #[test]
    fn non_interactive_env_forbids_terminal_prompt_and_ambient_config() {
        let env = non_interactive_git_env(&[("GIT_ASKPASS".into(), "/x/askpass.sh".into())]);
        let has = |k: &str, v: &str| env.iter().any(|(ek, ev)| ek == k && ev == v);
        assert!(has("GIT_ASKPASS", "/x/askpass.sh"), "base auth preserved");
        assert!(
            has("GIT_TERMINAL_PROMPT", "0"),
            "no terminal prompt → fail fast, never hang"
        );
        assert!(has("GIT_CONFIG_NOSYSTEM", "1"));
        assert!(has("GIT_CONFIG_GLOBAL", "/dev/null"));
    }

    #[test]
    fn non_interactive_env_does_not_clobber_caller_overrides() {
        // If a caller already pinned GIT_TERMINAL_PROMPT, respect it.
        let env = non_interactive_git_env(&[("GIT_TERMINAL_PROMPT".into(), "1".into())]);
        assert_eq!(
            env.iter()
                .filter(|(k, _)| k == "GIT_TERMINAL_PROMPT")
                .count(),
            1,
            "no duplicate keys"
        );
    }

    // ── evaluate_source_freshness (pure) ─────────────────────────

    #[test]
    fn same_revision_is_fresh() {
        assert_eq!(
            evaluate_source_freshness(Some("abc123"), "abc123"),
            Freshness::Fresh
        );
    }

    #[test]
    fn moved_head_is_stale_carrying_both_revisions() {
        assert_eq!(
            evaluate_source_freshness(Some("abc123"), "def456"),
            Freshness::Stale {
                compiled: Some("abc123".into()),
                head: "def456".into(),
            }
        );
    }

    #[test]
    fn never_compiled_is_stale_so_legacy_crs_converge() {
        // A CR that predates compiledRevision must NOT be
        // grandfathered as fresh — one recompile records the anchor.
        assert_eq!(
            evaluate_source_freshness(None, "def456"),
            Freshness::Stale {
                compiled: None,
                head: "def456".into(),
            }
        );
    }

    // ── ready_drift_decision (the law, CI-enforced) ──────────────

    #[test]
    fn stale_compile_can_never_produce_settled() {
        // THE headline invariant: whatever the plan said, a stale
        // compile recompiles. "No changes" against a stale compile is
        // mechanically unutterable.
        let stale = Freshness::Stale {
            compiled: Some("abc".into()),
            head: "def".into(),
        };
        assert_eq!(
            ready_drift_decision(&stale, false),
            ReadyAction::RecompileStale
        );
        assert_eq!(
            ready_drift_decision(&stale, true),
            ReadyAction::RecompileStale
        );
    }

    #[test]
    fn fresh_no_changes_settles_without_recompile_churn() {
        // The negative: a fresh compile with no changes must settle —
        // the gate may not force gratuitous recompiles.
        assert_eq!(
            ready_drift_decision(&Freshness::Fresh, false),
            ReadyAction::Settled
        );
    }

    #[test]
    fn fresh_with_changes_is_drift() {
        assert_eq!(
            ready_drift_decision(&Freshness::Fresh, true),
            ReadyAction::Drifted
        );
    }

    #[test]
    fn unknown_proceeds_instead_of_wedging() {
        // ls-remote unreachable must not wedge the drift loop; the
        // honesty lands in the Settled message ("HEAD: unverified").
        assert_eq!(
            ready_drift_decision(&Freshness::Unknown, false),
            ReadyAction::Settled
        );
        assert_eq!(
            ready_drift_decision(&Freshness::Unknown, true),
            ReadyAction::Drifted
        );
    }

    // ── select_ref (pure) — the two behaviour changes, pinned ─────
    //
    // These encode WHY the in-process port is not a like-for-like
    // translation. The subprocess version took `ls-remote`'s first
    // output line, so which ref won was an alphabetical accident.

    fn oid(byte: u8) -> gix::hash::ObjectId {
        gix::hash::ObjectId::from_bytes_or_panic(&[byte; 20])
    }

    fn direct(name: &str, o: u8) -> gix::protocol::handshake::Ref {
        gix::protocol::handshake::Ref::Direct {
            full_ref_name: name.into(),
            object: oid(o),
        }
    }

    fn annotated_tag(name: &str, tag: u8, commit: u8) -> gix::protocol::handshake::Ref {
        gix::protocol::handshake::Ref::Peeled {
            full_ref_name: name.into(),
            tag: oid(tag),
            object: oid(commit),
        }
    }

    #[test]
    fn a_branch_beats_a_same_named_ref_that_sorts_before_it() {
        // THE REGRESSION THIS PORT FIXES. `git ls-remote <url> main`
        // matches every namespace ending in `main`; `refs/changes/main`
        // sorts before `refs/heads/main`, so the old first-line pick
        // returned the WRONG commit while `clone --branch main` checked
        // out refs/heads/main — a permanent Stale→RecompileStale loop.
        let refs = [
            direct("refs/changes/main", 0xAA),
            direct("refs/heads/main", 0xBB),
        ];
        assert_eq!(select_ref(&refs, "main"), Some(oid(0xBB).to_string()));
    }

    #[test]
    fn a_branch_beats_a_tag_of_the_same_name() {
        let refs = [
            direct("refs/tags/main", 0xAA),
            direct("refs/heads/main", 0xBB),
        ];
        assert_eq!(select_ref(&refs, "main"), Some(oid(0xBB).to_string()));
    }

    #[test]
    fn an_annotated_tag_resolves_to_the_commit_not_the_tag_object() {
        // `ls-remote v1` advertises the TAG object; a checkout of that
        // ref produces the PEELED COMMIT. Comparing the tag oid against
        // rev-parse HEAD never matched, so every annotated-tag template
        // was permanently Stale and recompiling forever.
        let refs = [annotated_tag("refs/tags/v1", 0xAA, 0xBB)];
        assert_eq!(
            select_ref(&refs, "v1"),
            Some(oid(0xBB).to_string()),
            "the peeled commit, never the tag object"
        );
    }

    #[test]
    fn a_fully_qualified_ref_matches_exactly() {
        let refs = [direct("refs/heads/release/1.2", 0xCC)];
        assert_eq!(
            select_ref(&refs, "refs/heads/release/1.2"),
            Some(oid(0xCC).to_string())
        );
    }

    #[test]
    fn a_missing_ref_is_not_found_rather_than_a_wrong_guess() {
        // The old code returned the first line of WHATEVER came back.
        // Selecting nothing is what maps to Freshness::Unknown.
        let refs = [direct("refs/heads/other", 0xAA)];
        assert_eq!(select_ref(&refs, "main"), None);
    }

    #[test]
    fn an_unrelated_branch_is_never_substituted_for_the_requested_one() {
        let refs = [
            direct("refs/heads/mainline", 0xAA),
            direct("refs/heads/premain", 0xBB),
        ];
        assert_eq!(
            select_ref(&refs, "main"),
            None,
            "suffix/prefix neighbours must not match"
        );
    }
}
