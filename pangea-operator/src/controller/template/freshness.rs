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

/// `git rev-parse HEAD` in `repo_dir` — the commit a just-landed
/// clone/fetch checked out. The freshness model's compile-side
/// anchor; pairs with [`observe_head`] (the remote-side observation).
pub async fn git_rev_parse_head(repo_dir: &std::path::Path) -> Result<String> {
    let out = tokio::process::Command::new("git")
        .args(["rev-parse", "HEAD"])
        .current_dir(repo_dir)
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .output()
        .await
        .map_err(Error::Io)?;
    if !out.status.success() {
        return Err(Error::Compilation(format!(
            "git rev-parse HEAD in {} failed: {}",
            repo_dir.display(),
            String::from_utf8_lossy(&out.stderr).trim()
        )));
    }
    Ok(String::from_utf8_lossy(&out.stdout).trim().to_string())
}

/// Observe the remote's HEAD for `git_ref` via `git ls-remote <url>
/// <ref>` — one round trip, no clone, authenticated with the same
/// askpass env the clone path uses (`git_auth_env`).
///
/// A 40-hex SHA ref is returned as-is (ls-remote can't list a bare
/// commit; a SHA-pinned source is definitionally at its pinned
/// revision). An empty listing (ref deleted upstream) is an error —
/// the caller maps any error here to [`Freshness::Unknown`].
pub async fn observe_head(url: &str, git_ref: &str, env: &[(String, String)]) -> Result<String> {
    if git_ref.len() == 40 && git_ref.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Ok(git_ref.to_string());
    }
    let mut cmd = tokio::process::Command::new("git");
    cmd.args(["ls-remote", url, git_ref])
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped());
    for (k, v) in non_interactive_git_env(env) {
        cmd.env(k, v);
    }
    let out = tokio::time::timeout(std::time::Duration::from_secs(30), cmd.output())
        .await
        .map_err(|_| Error::Timeout(30))?
        .map_err(Error::Io)?;
    if !out.status.success() {
        return Err(Error::Compilation(format!(
            "git ls-remote {url} {git_ref} failed: {}",
            String::from_utf8_lossy(&out.stderr).trim()
        )));
    }
    let stdout = String::from_utf8_lossy(&out.stdout);
    // First line is the best match (`<sha>\t<refname>`); ls-remote
    // with an explicit <ref> pattern lists only matching refs.
    stdout
        .lines()
        .find_map(|line| line.split_whitespace().next())
        .map(str::to_string)
        .ok_or_else(|| {
            Error::Compilation(format!(
                "git ls-remote {url} {git_ref}: ref not found on remote"
            ))
        })
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
}
