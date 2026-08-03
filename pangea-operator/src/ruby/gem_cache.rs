//! Per-ArchitectureGem git-clone cache.
//!
//! Replaces "bundle every architecture gem at compiler-image build
//! time" with "clone the gem on first reference; cache by `(name, ref)`."
//! Adding a new architecture gem becomes one `ArchitectureGem` CR + a
//! git push; the operator picks it up at the next reconcile.
//!
//! See `theory/PANGEA-WORKSPACE-RECONCILIATION.md` § M8.4.
//!
//! Cache layout:
//!
//! ```text
//! /var/pangea/gems/
//!   pangea-architectures-7fb2fcc/        # name + short-ref dir
//!     .git/
//!     lib/pangea/architectures/...
//!     spec/fixtures/...
//!   pangea-architectures-main/           # also keyed by branch name
//!   pangea-cloudflare-v0.1.0/
//! ```
//!
//! Cache directories are immutable once populated; a new ref triggers
//! a fresh clone into a new directory. Old refs aren't auto-evicted —
//! disk reclamation is M8.5+ remit.

use std::path::{Path, PathBuf};
use std::process::Stdio;
use thiserror::Error;
use tokio::process::Command;
use tracing::{info, warn};

#[derive(Debug, Error)]
pub enum GemCacheError {
    #[error("git clone failed: {0}")]
    Clone(String),
    #[error("filesystem: {0}")]
    Filesystem(String),
    #[error("invalid name (path-traversal attempt?): {0}")]
    InvalidName(String),
}

/// One cache entry. Returns paths suitable for $LOAD_PATH-prepending
/// + fixture-path-resolving.
#[derive(Debug, Clone)]
pub struct GemEntry {
    /// Where the gem tree lives on disk.
    pub gem_path: PathBuf,
    /// `gem_path/lib`. Convention: every Ruby gem ships its public
    /// API under `lib/`. Prepend this to `$LOAD_PATH` for `require`.
    pub lib_path: PathBuf,
}

#[derive(Clone)]
pub struct GemCache {
    base_dir: PathBuf,
}

impl GemCache {
    /// `base_dir` defaults to `/var/pangea/gems`. Override via
    /// `PANGEA_GEM_CACHE_DIR` for tests + dev shells where the
    /// operator can't write to the canonical path.
    pub fn from_env() -> Self {
        let base = std::env::var("PANGEA_GEM_CACHE_DIR")
            .unwrap_or_else(|_| "/var/pangea/gems".to_string());
        Self {
            base_dir: PathBuf::from(base),
        }
    }

    pub fn new(base_dir: impl Into<PathBuf>) -> Self {
        Self {
            base_dir: base_dir.into(),
        }
    }

    pub fn base_dir(&self) -> &Path {
        &self.base_dir
    }

    /// Compute the cache directory for a given (name, ref). Pure —
    /// no I/O. Used by tests + by `ensure()`.
    pub fn entry_dir(&self, name: &str, git_ref: &str) -> Result<PathBuf, GemCacheError> {
        validate_name_component(name)?;
        validate_name_component(git_ref)?;
        // Truncate the ref to keep the dir name short. Full SHAs
        // collide on the prefix only at astronomically low rates,
        // but we keep enough characters to be obvious in `ls`.
        let short_ref: String = git_ref.chars().take(40).collect();
        Ok(self.base_dir.join(format!("{name}-{short_ref}")))
    }

    /// Idempotent: clone the gem if not cached, otherwise return
    /// the existing entry. `git_url` may be HTTPS or SSH; whatever
    /// the operator's git config can handle.
    ///
    /// Cache semantics differ by ref shape:
    ///   - **SHA refs** (`/[0-9a-f]{7,40}/`) are content-addressed and
    ///     immutable — a cache hit returns immediately, no network.
    ///   - **Branch / tag / symbolic refs** (`main`, `v1.2.3`, `HEAD`)
    ///     are mutable — we `git fetch` + `git reset --hard origin/<ref>`
    ///     on every `ensure()` so the operator picks up new commits
    ///     pushed to the ref. Without this, mutable-ref users (most
    ///     "track main" CRs) would clone once and never see new commits,
    ///     which is the bug this comment ships the fix for.
    pub async fn ensure(
        &self,
        name: &str,
        git_url: &str,
        git_ref: &str,
    ) -> Result<GemEntry, GemCacheError> {
        let dir = self.entry_dir(name, git_ref)?;
        let lib_path = dir.join("lib");
        let auth = GitAuth::github_from_env();

        if dir.is_dir() && dir.join(".git").exists() {
            // Remediation for caches written by the previous code, which
            // cloned from a URL carrying the token. Git persists a clone
            // URL verbatim, so every such cache dir still holds a live PAT
            // in `.git/config` — and the SHA-ref branch below returns
            // without ever running git, so nothing else would clear it.
            // Resetting origin to the declared URL is idempotent and costs
            // one local git process.
            scrub_persisted_credential(&dir, git_url).await;

            // Cache exists with a real .git. SHA refs are immutable —
            // return as-is. Mutable refs (branch/tag/etc.) re-fetch
            // from origin so the working tree tracks upstream HEAD.
            if is_sha_ref(git_ref) {
                info!(
                    name,
                    git_ref,
                    path = %dir.display(),
                    "gem cache hit (immutable SHA ref)"
                );
                return Ok(GemEntry {
                    gem_path: dir,
                    lib_path,
                });
            }
            // Mutable ref — refresh from origin. Failures here fall
            // through to a fresh re-clone (defensive: better to
            // re-clone slowly than serve a stale gem).
            match refresh_mutable_ref(&dir, git_ref, auth.as_ref()).await {
                Ok(()) => {
                    info!(
                        name,
                        git_ref,
                        path = %dir.display(),
                        "gem cache hit (mutable ref re-fetched)"
                    );
                    return Ok(GemEntry {
                        gem_path: dir,
                        lib_path,
                    });
                }
                Err(e) => {
                    warn!(
                        name,
                        git_ref,
                        error = %e,
                        "mutable-ref refresh failed; re-cloning from scratch"
                    );
                    let _ = tokio::fs::remove_dir_all(&dir).await;
                }
            }
        } else if dir.is_dir() {
            warn!(
                name,
                git_ref,
                path = %dir.display(),
                "stale gem cache dir without .git — re-cloning"
            );
            tokio::fs::remove_dir_all(&dir).await.map_err(|e| {
                GemCacheError::Filesystem(format!("remove stale {}: {e}", dir.display()))
            })?;
        }

        // Cache miss. Make sure base_dir exists.
        tokio::fs::create_dir_all(&self.base_dir)
            .await
            .map_err(|e| {
                GemCacheError::Filesystem(format!(
                    "create base_dir {}: {e}",
                    self.base_dir.display()
                ))
            })?;

        info!(
            name,
            git_ref,
            url = git_url,
            path = %dir.display(),
            "cloning gem"
        );

        // The URL is the declared one, unmodified. Authentication rides
        // the environment (see `GitAuth`) precisely so it does NOT end up
        // in this argv, and so git has nothing credential-shaped to
        // persist into the clone's `.git/config`.
        let mut cmd = Command::new("git");
        cmd.arg("clone")
            .arg("--depth")
            .arg("1")
            .arg("--branch")
            .arg(git_ref)
            .arg(git_url)
            .arg(&dir)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        GitAuth::apply(auth.as_ref(), &mut cmd);
        let output = cmd
            .output()
            .await
            .map_err(|e| GemCacheError::Clone(format!("spawn git clone: {e}")))?;

        if !output.status.success() {
            // git clone --branch fails if `ref` is a SHA; retry without
            // --branch then `git checkout <ref>` against the default
            // branch. This is how the existing compiler image's
            // workspace clone path handles SHA refs too.
            let stderr = String::from_utf8_lossy(&output.stderr);
            warn!(
                name,
                git_ref,
                stderr = %stderr,
                "shallow branch clone failed; retrying with full clone + checkout"
            );
            // Clean partial clone.
            let _ = tokio::fs::remove_dir_all(&dir).await;
            let mut cmd = Command::new("git");
            cmd.arg("clone")
                .arg(git_url)
                .arg(&dir)
                .stdin(Stdio::null())
                .stdout(Stdio::piped())
                .stderr(Stdio::piped());
            GitAuth::apply(auth.as_ref(), &mut cmd);
            let clone = cmd
                .output()
                .await
                .map_err(|e| GemCacheError::Clone(format!("spawn git clone (full): {e}")))?;
            if !clone.status.success() {
                let err = String::from_utf8_lossy(&clone.stderr);
                return Err(GemCacheError::Clone(format!("full clone failed: {err}")));
            }
            let checkout = Command::new("git")
                .arg("-C")
                .arg(&dir)
                .arg("checkout")
                .arg(git_ref)
                .stdin(Stdio::null())
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .output()
                .await
                .map_err(|e| GemCacheError::Clone(format!("spawn git checkout: {e}")))?;
            if !checkout.status.success() {
                let err = String::from_utf8_lossy(&checkout.stderr);
                return Err(GemCacheError::Clone(format!(
                    "checkout {git_ref} failed: {err}"
                )));
            }
        }

        Ok(GemEntry {
            gem_path: dir,
            lib_path,
        })
    }
}

/// Git credentials delivered to a child `git` through the environment,
/// via git's `GIT_CONFIG_COUNT` / `GIT_CONFIG_KEY_<n>` /
/// `GIT_CONFIG_VALUE_<n>` protocol (git >= 2.31).
///
/// # Why not the URL any more
///
/// This module used to authenticate by rewriting
/// `https://github.com/org/repo` into
/// `https://x-access-token:$PANGEA_GEM_AUTH_TOKEN@github.com/org/repo`
/// and passing that to `git clone`. That shape leaks the token twice:
///
///   - **argv.** `/proc/<pid>/cmdline` is world-readable, so the PAT was
///     visible to every process in the container for the clone's
///     lifetime.
///   - **at rest, and this is the worse one.** Git persists a clone URL
///     verbatim into `.git/config`. Every gem cache directory the
///     operator ever populated therefore holds whatever token was live
///     at clone time, indefinitely, on a volume nobody thinks of as
///     credential storage. `pleme-io/tend` found exactly this shape
///     fossilized in 25 repositories across two orgs on 2026-07-29 —
///     three distinct tokens, one still valid.
///
/// Environment-scoped git config has neither property: it exists for
/// the lifetime of one process and is written to no file.
///
/// # Why `basic`, not `bearer`
///
/// Bearer authenticates the GitHub REST API but not git-over-HTTPS —
/// git ignores the rejected header and falls through to prompting for a
/// username, which in a non-interactive container is a hang. This is
/// documented from a measurement in `tend/src/secret.rs`, whose
/// `GitConfigEnv` this is the local adaptation of. `x-access-token` is
/// the conventional non-secret username for a token-as-password; GitHub
/// ignores the username field.
struct GitAuth {
    entries: Vec<(String, String)>,
}

impl GitAuth {
    /// Auth config for `https://github.com/`, carrying `secret` as a
    /// Basic `Authorization` header.
    fn github(secret: &cofre_secret::Secret) -> Self {
        use base64::Engine as _;
        // `expose()` at exactly one boundary, which is the point of the
        // type: every other path to the plaintext is a compile error.
        let encoded = base64::engine::general_purpose::STANDARD
            .encode(format!("x-access-token:{}", secret.expose()));
        Self {
            entries: vec![(
                "http.https://github.com/.extraheader".to_string(),
                format!("AUTHORIZATION: basic {encoded}"),
            )],
        }
    }

    /// Read `PANGEA_GEM_AUTH_TOKEN`. `None` when unset or empty — an
    /// empty env var is not a credential, and treating it as one turns a
    /// clean unauthenticated clone of a public gem into a 401.
    fn github_from_env() -> Option<Self> {
        cofre_secret::Secret::from_env("PANGEA_GEM_AUTH_TOKEN")
            .ok()
            .map(|s| Self::github(&s))
    }

    /// The environment pairs git expects. Materialized separately from
    /// [`Self::apply`] so tests can assert on the exact pairs.
    fn env_pairs(&self) -> Vec<(String, String)> {
        let mut pairs = Vec::with_capacity(self.entries.len() * 2 + 1);
        pairs.push((
            "GIT_CONFIG_COUNT".to_string(),
            self.entries.len().to_string(),
        ));
        for (i, (key, value)) in self.entries.iter().enumerate() {
            pairs.push((format!("GIT_CONFIG_KEY_{i}"), key.clone()));
            pairs.push((format!("GIT_CONFIG_VALUE_{i}"), value.clone()));
        }
        pairs
    }

    /// Apply to a command, or leave it untouched when there is no
    /// credential. Takes the `Option` so no call site can forget the
    /// no-token case, and so an unauthenticated invocation is
    /// byte-identical to one that never called this.
    fn apply(auth: Option<&Self>, cmd: &mut Command) {
        let Some(auth) = auth else { return };
        for (k, v) in auth.env_pairs() {
            cmd.env(k, v);
        }
    }
}

/// Reset a cached clone's `origin` to `git_url`, discarding any
/// credential a previous code path embedded in it.
///
/// Best-effort and deliberately silent on failure: this is remediation
/// of an old defect, not a precondition for serving the gem. A cache
/// directory that cannot be rewritten still works — it is just still
/// carrying the fossil, and the warn line says so.
async fn scrub_persisted_credential(dir: &Path, git_url: &str) {
    let out = Command::new("git")
        .arg("-C")
        .arg(dir)
        .arg("remote")
        .arg("set-url")
        .arg("origin")
        .arg(git_url)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .await;
    match out {
        Ok(o) if o.status.success() => {}
        Ok(o) => warn!(
            path = %dir.display(),
            stderr = %String::from_utf8_lossy(&o.stderr),
            "could not reset cached remote URL; a credential embedded by an \
             earlier clone may still be present in .git/config"
        ),
        Err(e) => warn!(
            path = %dir.display(),
            error = %e,
            "could not spawn git to reset cached remote URL"
        ),
    }
}

/// Heuristic: does `r` look like a git SHA? Accepts 7–40 lower-case
/// hex chars. SHA refs are content-addressed and cache-immutable;
/// every other ref (branch, tag, HEAD, FETCH_HEAD, …) is mutable
/// and requires re-fetching on each `ensure()`.
fn is_sha_ref(r: &str) -> bool {
    let len = r.len();
    (7..=40).contains(&len)
        && r.bytes()
            .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
}

/// Refresh a cached shallow clone to track upstream `<ref>`. Runs
/// `git fetch --depth 1 origin <ref>` then `git reset --hard FETCH_HEAD`.
///
/// `auth` is now a parameter rather than something inherited from the
/// cached remote URL. It has to be: the fetch used to authenticate
/// because the URL in `.git/config` still carried the token from clone
/// time, which is the leak this module stopped producing. With the URL
/// clean, an authenticated refresh only happens if the credential is
/// handed to it here.
async fn refresh_mutable_ref(
    dir: &Path,
    git_ref: &str,
    auth: Option<&GitAuth>,
) -> Result<(), GemCacheError> {
    let mut cmd = Command::new("git");
    cmd.arg("-C")
        .arg(dir)
        .arg("fetch")
        .arg("--depth")
        .arg("1")
        .arg("origin")
        .arg(git_ref)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    GitAuth::apply(auth, &mut cmd);
    let fetch = cmd
        .output()
        .await
        .map_err(|e| GemCacheError::Clone(format!("spawn git fetch: {e}")))?;
    if !fetch.status.success() {
        return Err(GemCacheError::Clone(format!(
            "git fetch origin {git_ref}: {}",
            String::from_utf8_lossy(&fetch.stderr)
        )));
    }
    let reset = Command::new("git")
        .arg("-C")
        .arg(dir)
        .arg("reset")
        .arg("--hard")
        .arg("FETCH_HEAD")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .await
        .map_err(|e| GemCacheError::Clone(format!("spawn git reset: {e}")))?;
    if !reset.status.success() {
        return Err(GemCacheError::Clone(format!(
            "git reset --hard FETCH_HEAD: {}",
            String::from_utf8_lossy(&reset.stderr)
        )));
    }
    Ok(())
}

/// Validate a name component is safe to use as a filesystem path
/// segment. Rejects path traversal, NUL, leading dots, slashes.
fn validate_name_component(s: &str) -> Result<(), GemCacheError> {
    if s.is_empty()
        || s.starts_with('.')
        || s.contains('/')
        || s.contains('\\')
        || s.contains('\0')
        || s == "."
        || s == ".."
    {
        return Err(GemCacheError::InvalidName(s.to_string()));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn entry_dir_uses_name_and_ref() {
        let cache = GemCache::new("/tmp/gems");
        let p = cache.entry_dir("pangea-architectures", "7fb2fcc").unwrap();
        assert_eq!(p, Path::new("/tmp/gems/pangea-architectures-7fb2fcc"));
    }

    #[test]
    fn entry_dir_rejects_path_traversal() {
        let cache = GemCache::new("/tmp/gems");
        assert!(cache.entry_dir("../etc", "main").is_err());
        assert!(cache.entry_dir("ok", "../sneak").is_err());
        assert!(cache.entry_dir("a/b", "main").is_err());
        assert!(cache.entry_dir(".hidden", "main").is_err());
    }

    // ── Credential delivery ──────────────────────────────────────
    //
    // These replace the old `inject_token` suite, which asserted that a
    // token was correctly interpolated into a clone URL — i.e. it pinned
    // the defect in place. What matters now is the opposite property:
    // the credential reaches git through the environment and appears in
    // neither argv nor anything git will persist.
    //
    // Credential-SHAPED so the assertions exercise real matching, with
    // an explicit marker so a scanner can tell it is a fixture. Same
    // convention cofre-secret's own tests use.
    const TOKEN: &str = "ghp_EXAMPLENOTAREALTOKENxxxxxxxxxxxxxxxx";

    fn test_auth() -> GitAuth {
        GitAuth::github(&cofre_secret::Secret::new(TOKEN).unwrap())
    }

    /// Pins the exact config git needs. `basic` is NOT interchangeable
    /// with `bearer` here — bearer does not authenticate git-over-HTTPS,
    /// and git falls through to prompting, which in the operator's
    /// container is a hang rather than an error.
    #[test]
    fn github_auth_carries_a_basic_extraheader() {
        use base64::Engine as _;
        let pairs = test_auth().env_pairs();
        assert_eq!(pairs[0], ("GIT_CONFIG_COUNT".into(), "1".into()));
        assert_eq!(
            pairs[1],
            (
                "GIT_CONFIG_KEY_0".into(),
                "http.https://github.com/.extraheader".into()
            )
        );
        let expected =
            base64::engine::general_purpose::STANDARD.encode(format!("x-access-token:{TOKEN}"));
        assert_eq!(
            pairs[2],
            (
                "GIT_CONFIG_VALUE_0".into(),
                format!("AUTHORIZATION: basic {expected}")
            )
        );
    }

    /// The load-bearing property. Base64 is encoding, not concealment,
    /// so neither the raw token nor its encoded form may appear in argv
    /// — `/proc/<pid>/cmdline` is world-readable. And the URL git is
    /// handed must be the declared one, because git copies a clone URL
    /// verbatim into `.git/config` and keeps it forever.
    #[test]
    fn applying_auth_leaves_argv_and_the_clone_url_clean() {
        use base64::Engine as _;
        let url = "https://github.com/pleme-io/pangea-architectures";
        let mut cmd = Command::new("git");
        cmd.arg("clone").arg(url).arg("/tmp/dest");
        GitAuth::apply(Some(&test_auth()), &mut cmd);

        let encoded =
            base64::engine::general_purpose::STANDARD.encode(format!("x-access-token:{TOKEN}"));
        let argv = format!("{:?}", cmd.as_std().get_args().collect::<Vec<_>>());
        assert!(!argv.contains(TOKEN), "raw token reached argv: {argv}");
        assert!(
            !argv.contains(&encoded),
            "encoded token reached argv: {argv}"
        );
        assert!(
            !argv.contains("x-access-token:"),
            "credential-bearing URL reached argv: {argv}"
        );
        assert!(argv.contains(url), "declared URL missing from argv: {argv}");

        let in_env = cmd
            .as_std()
            .get_envs()
            .any(|(_, v)| v.is_some_and(|v| v.to_string_lossy().contains(&encoded)));
        assert!(in_env, "credential did not reach the environment");
    }

    /// No credential must leave the command byte-identical to one that
    /// never called `apply` — not `GIT_CONFIG_COUNT=0`, which git
    /// accepts but which makes the unauthenticated path a different
    /// path.
    #[test]
    fn no_auth_leaves_the_command_untouched() {
        let mut cmd = Command::new("git");
        GitAuth::apply(None, &mut cmd);
        assert_eq!(cmd.as_std().get_envs().count(), 0);
    }

    /// Guards the ONE test that mutates process env. Matches the
    /// manual-mutex pattern already used for this reason in `config.rs`
    /// and `controller/reconciler.rs` (no `serial_test` dependency for a
    /// single test). A mutex is the right tool here because the thing
    /// under test is precisely "does the wrapper read that env key", so
    /// the global cannot be injected away — it is the subject.
    static ENV_VAR_TEST_GUARD: std::sync::Mutex<()> = std::sync::Mutex::new(());

    #[test]
    fn github_from_env_reads_the_real_env_var() {
        let _guard = ENV_VAR_TEST_GUARD
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);

        std::env::remove_var("PANGEA_GEM_AUTH_TOKEN");
        assert!(GitAuth::github_from_env().is_none());

        // An empty var is not a credential: authenticating with it turns
        // a clean public-gem clone into a 401.
        std::env::set_var("PANGEA_GEM_AUTH_TOKEN", "");
        assert!(GitAuth::github_from_env().is_none());

        std::env::set_var("PANGEA_GEM_AUTH_TOKEN", TOKEN);
        assert_eq!(
            GitAuth::github_from_env().unwrap().env_pairs(),
            test_auth().env_pairs()
        );

        std::env::remove_var("PANGEA_GEM_AUTH_TOKEN");
        assert!(GitAuth::github_from_env().is_none());
    }

    #[test]
    fn is_sha_ref_classification() {
        // SHA-like (immutable in cache)
        assert!(is_sha_ref("7fb2fcc")); // short
        assert!(is_sha_ref("7fb2fccdeadbeef")); // 15 chars
        assert!(is_sha_ref("0123456789abcdef0123456789abcdef01234567")); // 40 chars
                                                                         // Not SHA-like (re-fetch every ensure)
        assert!(!is_sha_ref("main"));
        assert!(!is_sha_ref("HEAD"));
        assert!(!is_sha_ref("v0.1.0")); // tag — also mutable in practice
        assert!(!is_sha_ref("feature/foo"));
        assert!(!is_sha_ref("ABCDEF0123456789ABCDEF0123456789ABCDEF01")); // uppercase rejected
        assert!(!is_sha_ref(""));
        assert!(!is_sha_ref("abc")); // < 7 chars
        assert!(!is_sha_ref("g")); // non-hex char
        assert!(!is_sha_ref(&"a".repeat(41))); // > 40 chars
    }

    // ── Integration tests guarding the load-bearing invariant ──
    //
    // These spin up a temp "remote" git repo, point a GemCache at a
    // separate cache root, and verify the contract end-to-end. If
    // any future refactor regresses the cache to "immutable per ref"
    // semantics, these tests fail loud.

    use tempfile::TempDir;
    use tokio::process::Command as TokioCommand;

    async fn git_run(dir: &Path, args: &[&str]) {
        let out = TokioCommand::new("git")
            .arg("-C")
            .arg(dir)
            .args(args)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .output()
            .await
            .expect("git spawn");
        assert!(
            out.status.success(),
            "git {:?} failed: {}",
            args,
            String::from_utf8_lossy(&out.stderr)
        );
    }

    /// Build a "remote" git repo with one initial commit on `main`
    /// touching `lib/test_gem/marker.txt`. Returns the dir + the
    /// initial commit SHA.
    async fn make_remote_repo(content: &str) -> (TempDir, String) {
        let dir = TempDir::new().expect("tempdir");
        let path = dir.path();
        git_run(path, &["init", "-q", "-b", "main"]).await;
        git_run(path, &["config", "user.email", "test@example.com"]).await;
        git_run(path, &["config", "user.name", "test"]).await;
        git_run(path, &["config", "commit.gpgsign", "false"]).await;
        // Subjects below are descriptive, not "initial"/"update", because a
        // pleme-io workstation carries a GLOBAL commit-msg hook (core.hooksPath,
        // blackmatter.components.gitconfig.hooks.rejectSubjects) that refuses
        // placeholder subjects. A hook the developer installed for their own
        // repositories does not know this is a throwaway fixture, so it failed
        // both of these tests on every such machine while passing in CI.
        let lib_dir = path.join("lib").join("test_gem");
        tokio::fs::create_dir_all(&lib_dir).await.unwrap();
        tokio::fs::write(lib_dir.join("marker.txt"), content)
            .await
            .unwrap();
        git_run(path, &["add", "-A"]).await;
        git_run(path, &["commit", "-q", "-m", "fixture: seed the gem tree"]).await;
        let sha_out = TokioCommand::new("git")
            .arg("-C")
            .arg(path)
            .args(["rev-parse", "HEAD"])
            .output()
            .await
            .expect("rev-parse");
        let sha = String::from_utf8_lossy(&sha_out.stdout).trim().to_string();
        (dir, sha)
    }

    /// Push a new commit overwriting `marker.txt`. Returns the new SHA.
    async fn add_commit(remote: &Path, content: &str) -> String {
        tokio::fs::write(
            remote.join("lib").join("test_gem").join("marker.txt"),
            content,
        )
        .await
        .unwrap();
        git_run(remote, &["add", "-A"]).await;
        git_run(remote, &["commit", "-q", "-m", "fixture: move the marker"]).await;
        let sha_out = TokioCommand::new("git")
            .arg("-C")
            .arg(remote)
            .args(["rev-parse", "HEAD"])
            .output()
            .await
            .expect("rev-parse");
        String::from_utf8_lossy(&sha_out.stdout).trim().to_string()
    }

    async fn read_marker(entry: &GemEntry) -> String {
        let path = entry.lib_path.join("test_gem").join("marker.txt");
        tokio::fs::read_to_string(&path).await.unwrap_or_default()
    }

    /// THE bug-fix invariant: cache hit on a branch ref re-fetches.
    /// Without the fix in this commit, the second ensure() returns
    /// the stale "v1" contents. With the fix, it returns "v2".
    #[tokio::test]
    async fn mutable_branch_ref_re_fetches_on_subsequent_ensure() {
        let (remote, _) = make_remote_repo("v1").await;
        let cache_dir = TempDir::new().expect("cache tempdir");
        let cache = GemCache::new(cache_dir.path());

        let url = format!("file://{}", remote.path().display());
        let entry1 = cache
            .ensure("test-gem", &url, "main")
            .await
            .expect("first ensure");
        assert_eq!(read_marker(&entry1).await, "v1");

        // Mutate the remote.
        add_commit(remote.path(), "v2").await;

        // Same call — must observe the new content.
        let entry2 = cache
            .ensure("test-gem", &url, "main")
            .await
            .expect("second ensure");
        assert_eq!(
            read_marker(&entry2).await,
            "v2",
            "mutable-ref cache hit should refresh from origin"
        );
    }

    /// SHA refs remain immutable — second ensure() should NOT re-fetch
    /// even if remote moves. (Verified by checking we still see the
    /// original content after a remote commit.)
    #[tokio::test]
    async fn sha_ref_is_immutable_no_refresh() {
        let (remote, sha) = make_remote_repo("v1").await;
        let cache_dir = TempDir::new().expect("cache tempdir");
        let cache = GemCache::new(cache_dir.path());
        let url = format!("file://{}", remote.path().display());

        let entry1 = cache
            .ensure("test-gem", &url, &sha)
            .await
            .expect("first ensure");
        assert_eq!(read_marker(&entry1).await, "v1");

        add_commit(remote.path(), "v2").await;

        let entry2 = cache
            .ensure("test-gem", &url, &sha)
            .await
            .expect("second ensure");
        assert_eq!(
            read_marker(&entry2).await,
            "v1",
            "SHA-pinned cache hit must stay frozen at the original commit"
        );
    }
}
