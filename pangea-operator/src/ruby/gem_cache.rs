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

        if dir.is_dir() && dir.join(".git").exists() {
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
            match refresh_mutable_ref(&dir, git_ref).await {
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

        // Apply auth token to HTTPS GitHub URLs if PANGEA_GEM_AUTH_TOKEN
        // is set in the env. Rewrites `https://github.com/...` to
        // `https://x-access-token:$TOKEN@github.com/...` so private
        // repo clones succeed. SSH URLs + non-GitHub URLs pass through.
        let effective_url = inject_github_token(git_url);

        info!(
            name,
            git_ref,
            url = git_url,  // log original (no token leak)
            path = %dir.display(),
            "cloning gem"
        );

        let output = Command::new("git")
            .arg("clone")
            .arg("--depth")
            .arg("1")
            .arg("--branch")
            .arg(git_ref)
            .arg(&effective_url)
            .arg(&dir)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
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
            let clone = Command::new("git")
                .arg("clone")
                .arg(&effective_url)
                .arg(&dir)
                .stdin(Stdio::null())
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
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

/// Rewrite `https://github.com/...` URLs to embed
/// `x-access-token:$PANGEA_GEM_AUTH_TOKEN@github.com/...` when the
/// env var is set. Other URL shapes (SSH, non-GitHub HTTPS) pass
/// through unchanged.
///
/// The token is the standard GitHub-Apps pattern accepted by
/// github.com — works for fine-grained PATs, classic PATs, and
/// installation tokens. Rewriting at clone time avoids needing to
/// configure git credential helpers in the container image.
fn inject_github_token(url: &str) -> String {
    let token = match std::env::var("PANGEA_GEM_AUTH_TOKEN") {
        Ok(t) if !t.is_empty() => t,
        _ => return url.to_string(),
    };
    let prefix = "https://github.com/";
    if let Some(rest) = url.strip_prefix(prefix) {
        format!("https://x-access-token:{token}@github.com/{rest}")
    } else {
        url.to_string()
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
/// Authenticates via the same PANGEA_GEM_AUTH_TOKEN injection the
/// initial clone uses (the cached remote URL already carries it if
/// the original clone was authed).
async fn refresh_mutable_ref(dir: &Path, git_ref: &str) -> Result<(), GemCacheError> {
    let fetch = Command::new("git")
        .arg("-C")
        .arg(dir)
        .arg("fetch")
        .arg("--depth")
        .arg("1")
        .arg("origin")
        .arg(git_ref)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
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

    #[test]
    fn inject_github_token_passthrough_when_unset() {
        std::env::remove_var("PANGEA_GEM_AUTH_TOKEN");
        let url = "https://github.com/pleme-io/pangea-architectures";
        assert_eq!(inject_github_token(url), url);
    }

    #[test]
    fn inject_github_token_rewrites_when_set() {
        std::env::set_var("PANGEA_GEM_AUTH_TOKEN", "ghp_test123");
        let url = "https://github.com/pleme-io/pangea-architectures";
        let out = inject_github_token(url);
        assert_eq!(
            out,
            "https://x-access-token:ghp_test123@github.com/pleme-io/pangea-architectures"
        );
        std::env::remove_var("PANGEA_GEM_AUTH_TOKEN");
    }

    #[test]
    fn inject_github_token_passes_through_ssh_and_non_github() {
        std::env::set_var("PANGEA_GEM_AUTH_TOKEN", "ghp_test123");
        let ssh = "git@github.com:pleme-io/pangea-architectures.git";
        assert_eq!(inject_github_token(ssh), ssh, "ssh URL should pass through");
        let other = "https://gitlab.com/foo/bar";
        assert_eq!(
            inject_github_token(other),
            other,
            "non-github should pass through"
        );
        std::env::remove_var("PANGEA_GEM_AUTH_TOKEN");
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
        let lib_dir = path.join("lib").join("test_gem");
        tokio::fs::create_dir_all(&lib_dir).await.unwrap();
        tokio::fs::write(lib_dir.join("marker.txt"), content)
            .await
            .unwrap();
        git_run(path, &["add", "-A"]).await;
        git_run(path, &["commit", "-q", "-m", "initial"]).await;
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
        git_run(remote, &["commit", "-q", "-m", "update"]).await;
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
