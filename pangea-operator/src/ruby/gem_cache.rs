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
    pub async fn ensure(
        &self,
        name: &str,
        git_url: &str,
        git_ref: &str,
    ) -> Result<GemEntry, GemCacheError> {
        let dir = self.entry_dir(name, git_ref)?;
        let lib_path = dir.join("lib");

        if dir.is_dir() {
            // Cache hit. Check it has a .git so we know it's a real
            // clone (not a stray dir); rebuild if not.
            if dir.join(".git").exists() || dir.join("lib").exists() {
                info!(
                    name,
                    git_ref,
                    path = %dir.display(),
                    "gem cache hit"
                );
                return Ok(GemEntry {
                    gem_path: dir,
                    lib_path,
                });
            }
            warn!(
                name,
                git_ref,
                path = %dir.display(),
                "stale gem cache dir without .git — re-cloning"
            );
            tokio::fs::remove_dir_all(&dir)
                .await
                .map_err(|e| GemCacheError::Filesystem(format!("remove stale {}: {e}", dir.display())))?;
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

        let output = Command::new("git")
            .arg("clone")
            .arg("--depth")
            .arg("1")
            .arg("--branch")
            .arg(git_ref)
            .arg(git_url)
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
                .arg(git_url)
                .arg(&dir)
                .stdin(Stdio::null())
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .output()
                .await
                .map_err(|e| GemCacheError::Clone(format!("spawn git clone (full): {e}")))?;
            if !clone.status.success() {
                let err = String::from_utf8_lossy(&clone.stderr);
                return Err(GemCacheError::Clone(format!(
                    "full clone failed: {err}"
                )));
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
        let p = cache
            .entry_dir("pangea-architectures", "7fb2fcc")
            .unwrap();
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
}
