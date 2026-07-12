//! Workspace management for template execution.
//!
//! Each InfrastructureTemplate gets an isolated workspace directory
//! containing the compiled Terraform JSON, backend configuration,
//! and execution artifacts.

use crate::crd::InfrastructureTemplate;
use crate::error::{Error, Result};
use std::path::PathBuf;
use tokio::fs;
use tracing::{debug, info};

/// Canonical filename for the on-disk Terraform/OpenTofu state file
/// under the tofu-local-backend path (no `backend.tf.json` declared —
/// tofu's own default convention: `terraform.tfstate` in the working
/// directory `tofu` is invoked from, i.e. the workspace directory).
/// Workspaces using a real remote backend (the tofu `pg` backend, or
/// magma's DB-backed state) never populate this file on disk — that's
/// expected, not a gap; see `Workspace::read_state_bytes`.
const STATE_FILE_NAME: &str = "terraform.tfstate";

/// tofu writes this as a point-in-time backup of the PREVIOUS state
/// immediately before overwriting `terraform.tfstate`. It is the one
/// generation of rollback safety tofu itself provides — `clean()` must
/// preserve it for the same reason it preserves the state file itself.
const STATE_BACKUP_FILE_NAME: &str = "terraform.tfstate.backup";

/// Manages workspaces for template execution.
#[derive(Clone)]
pub struct WorkspaceManager {
    /// Base directory for all workspaces.
    base_dir: PathBuf,
}

/// A workspace for a single template.
#[derive(Debug)]
pub struct Workspace {
    /// Workspace directory path.
    pub path: PathBuf,

    /// Namespace of the template.
    pub namespace: String,

    /// Name of the template.
    pub name: String,

    /// Whether this workspace should be cleaned up.
    cleanup_on_drop: bool,
}

impl WorkspaceManager {
    /// Create a new workspace manager.
    pub fn new(base_dir: PathBuf) -> Self {
        Self { base_dir }
    }

    /// Ensure the base directory exists.
    pub async fn init(&self) -> Result<()> {
        fs::create_dir_all(&self.base_dir)
            .await
            .map_err(|e| Error::Io(e))?;
        Ok(())
    }

    /// Create or get a workspace for a template.
    pub async fn get_workspace(&self, template: &InfrastructureTemplate) -> Result<Workspace> {
        let namespace = template.metadata.namespace.clone().unwrap_or_default();
        let name = template.metadata.name.clone().unwrap_or_default();

        let workspace_path = self.base_dir.join(&namespace).join(&name);

        fs::create_dir_all(&workspace_path)
            .await
            .map_err(|e| Error::Io(e))?;

        debug!(
            namespace = %namespace,
            name = %name,
            path = ?workspace_path,
            "Workspace ready"
        );

        Ok(Workspace {
            path: workspace_path,
            namespace,
            name,
            cleanup_on_drop: false,
        })
    }

    /// Get or create a workspace by namespace and name strings.
    /// Used by controllers that don't have an InfrastructureTemplate reference
    /// (e.g., PackerBuild, AmiTest).
    pub async fn get_or_create(&self, namespace: &str, name: &str) -> Result<Workspace> {
        let workspace_path = self.base_dir.join(namespace).join(name);

        fs::create_dir_all(&workspace_path)
            .await
            .map_err(|e| Error::Io(e))?;

        debug!(
            namespace = %namespace,
            name = %name,
            path = ?workspace_path,
            "Workspace ready"
        );

        Ok(Workspace {
            path: workspace_path,
            namespace: namespace.to_string(),
            name: name.to_string(),
            cleanup_on_drop: false,
        })
    }

    /// Create a temporary workspace.
    pub async fn create_temp_workspace(&self, prefix: &str) -> Result<Workspace> {
        let temp_name = format!("{}_{}", prefix, uuid::Uuid::new_v4());
        let workspace_path = self.base_dir.join("_temp").join(&temp_name);

        fs::create_dir_all(&workspace_path)
            .await
            .map_err(|e| Error::Io(e))?;

        Ok(Workspace {
            path: workspace_path,
            namespace: "_temp".to_string(),
            name: temp_name,
            cleanup_on_drop: true,
        })
    }

    /// Delete a workspace — the WHOLE directory, state included.
    ///
    /// This is the CR-lifecycle full-wipe primitive: called from the
    /// `Destroying` phase handler AFTER a real `tofu destroy` has
    /// already run against that state (`template_controller.rs`'s
    /// `handle_destroying`). It is NOT a cache-invalidation primitive
    /// — do not call this to drop a stale `_repo` clone or any other
    /// "make the next reconcile start fresh" need. Use
    /// [`Self::invalidate_repo_cache`] for that; it is scoped to the
    /// `_repo` subdirectory only and can never touch
    /// `terraform.tfstate` / `.backup`. Reusing this method for cache
    /// invalidation is exactly the bug documented in
    /// `docs/postmortems/2026-07-12-camelot-eks-state-wipe-duplicate-vpc.md`'s
    /// second code path: a still-Ready template whose compile starts
    /// failing (e.g. a bad commit on `gitRepository`) would have its
    /// live-applied state silently deleted by the escalation ladder's
    /// `RefreshSource` handler, five minutes into the failure.
    pub async fn delete_workspace(&self, namespace: &str, name: &str) -> Result<()> {
        let workspace_path = self.base_dir.join(namespace).join(name);

        if workspace_path.exists() {
            fs::remove_dir_all(&workspace_path)
                .await
                .map_err(|e| Error::Io(e))?;

            info!(
                namespace = %namespace,
                name = %name,
                "Workspace deleted"
            );
        }

        Ok(())
    }

    /// Invalidate the cached `_repo` git clone for a workspace,
    /// leaving every other file — `terraform.tfstate`,
    /// `terraform.tfstate.backup`, `.terraform`, rendered config —
    /// untouched.
    ///
    /// This is the ONLY primitive `WorkspaceCacheInvalidator`'s real
    /// (`WorkspaceManager`) implementation may call. Unlike
    /// [`Self::delete_workspace`], the deletion target here is not a
    /// parameter — it is always `base_dir/{namespace}/{name}/_repo` —
    /// so there is no path through this function that can reach
    /// `terraform.tfstate` or any sibling file, regardless of what a
    /// future caller passes in. Idempotent: a missing `_repo` is a
    /// no-op, matching `WorkspaceCacheInvalidator::invalidate`'s
    /// contract.
    pub async fn invalidate_repo_cache(&self, namespace: &str, name: &str) -> Result<()> {
        let repo_dir = self.base_dir.join(namespace).join(name).join("_repo");

        if repo_dir.exists() {
            fs::remove_dir_all(&repo_dir)
                .await
                .map_err(|e| Error::Io(e))?;

            info!(
                namespace = %namespace,
                name = %name,
                "Workspace _repo cache invalidated (state preserved)"
            );
        }

        Ok(())
    }

    /// List all workspaces.
    pub async fn list_workspaces(&self) -> Result<Vec<(String, String)>> {
        let mut workspaces = Vec::new();

        let mut ns_entries = fs::read_dir(&self.base_dir)
            .await
            .map_err(|e| Error::Io(e))?;

        while let Some(ns_entry) = ns_entries.next_entry().await.map_err(|e| Error::Io(e))? {
            let ns_path = ns_entry.path();
            if ns_path.is_dir() {
                let namespace = ns_entry.file_name().to_string_lossy().to_string();

                // Skip temp directory
                if namespace.starts_with('_') {
                    continue;
                }

                let mut name_entries = fs::read_dir(&ns_path)
                    .await
                    .map_err(|e| Error::Io(e))?;

                while let Some(name_entry) = name_entries.next_entry().await.map_err(|e| Error::Io(e))? {
                    if name_entry.path().is_dir() {
                        let name = name_entry.file_name().to_string_lossy().to_string();
                        workspaces.push((namespace.clone(), name));
                    }
                }
            }
        }

        Ok(workspaces)
    }
}

impl Workspace {
    /// Get the path to the main Terraform file.
    pub fn main_tf_path(&self) -> PathBuf {
        self.path.join("main.tf.json")
    }

    /// Get the path to the backend configuration.
    pub fn backend_path(&self) -> PathBuf {
        self.path.join("backend.tf.json")
    }

    /// Get the path to the providers configuration.
    pub fn providers_path(&self) -> PathBuf {
        self.path.join("providers.tf.json")
    }

    /// Get the path to the plan file.
    pub fn plan_path(&self) -> PathBuf {
        self.path.join("tfplan")
    }

    /// Get the path to the plan JSON output.
    pub fn plan_json_path(&self) -> PathBuf {
        self.path.join("tfplan.json")
    }

    /// Path to the on-disk Terraform/OpenTofu state file. See
    /// `STATE_FILE_NAME` for which backends actually populate this.
    pub fn state_path(&self) -> PathBuf {
        self.path.join(STATE_FILE_NAME)
    }

    /// Path to tofu's automatic pre-overwrite state backup.
    pub fn state_backup_path(&self) -> PathBuf {
        self.path.join(STATE_BACKUP_FILE_NAME)
    }

    /// Best-effort read of the on-disk state file, used to fingerprint
    /// plan-approval hashes (`template_controller::plan_approval_hash`)
    /// — never consulted for actual planning/apply logic, which always
    /// goes through the executor. Returns `None` when no state file
    /// exists — a legitimate, distinct value (first-ever apply, or a
    /// workspace whose backend never writes state to disk) — never an
    /// error a caller needs to handle specially.
    pub async fn read_state_bytes(&self) -> Option<Vec<u8>> {
        fs::read(self.state_path()).await.ok()
    }

    /// Write content to a file in the workspace.
    pub async fn write_file(&self, filename: &str, content: &str) -> Result<PathBuf> {
        let file_path = self.path.join(filename);
        fs::write(&file_path, content)
            .await
            .map_err(|e| Error::Io(e))?;
        Ok(file_path)
    }

    /// Write JSON content to a file in the workspace.
    pub async fn write_json(&self, filename: &str, value: &serde_json::Value) -> Result<PathBuf> {
        let content = serde_json::to_string_pretty(value)
            .map_err(|e| Error::Serialization(e))?;
        self.write_file(filename, &content).await
    }

    /// Read a file from the workspace.
    pub async fn read_file(&self, filename: &str) -> Result<String> {
        let file_path = self.path.join(filename);
        fs::read_to_string(&file_path)
            .await
            .map_err(|e| Error::Io(e))
    }

    /// Check if a file exists in the workspace.
    pub fn file_exists(&self, filename: &str) -> bool {
        self.path.join(filename).exists()
    }

    /// Clean the workspace: remove rendered config + plan artifacts so
    /// the next cycle recompiles/replans from scratch, but NEVER touch
    /// Terraform/OpenTofu state.
    ///
    /// Every existing caller (a spec-generation bump, a self-heal after
    /// a stale-plan/empty-workspace apply anomaly, a Failed-phase
    /// retry) wants "force a fresh render + plan," never "discard the
    /// state of infrastructure that may already be partially or fully
    /// applied." Before this fix, `clean()` deleted `terraform.tfstate`
    /// unconditionally on every call — including the one that fires on
    /// ANY `metadata.generation` bump — which turned a benign spec edit
    /// into full state loss: the next planning cycle replanned against
    /// an empty state (every resource `create`), and a stale
    /// plan-approval hash let the resulting apply run without a human
    /// ever reviewing it. See
    /// `docs/postmortems/2026-07-12-camelot-eks-state-wipe-duplicate-vpc.md`.
    ///
    /// The one place that genuinely disposes of a workspace's state is
    /// `WorkspaceManager::delete_workspace` — called from the
    /// Destroying phase handler AFTER a real `tofu destroy` has already
    /// run against that state, and it removes the whole workspace
    /// directory. That is a distinct, explicit code path; it does not
    /// go through `clean()` and is unaffected by this change.
    pub async fn clean(&self) -> Result<()> {
        let mut entries = fs::read_dir(&self.path)
            .await
            .map_err(|e| Error::Io(e))?;

        while let Some(entry) = entries.next_entry().await.map_err(|e| Error::Io(e))? {
            let path = entry.path();
            let name = entry.file_name().to_string_lossy().to_string();

            // Keep .terraform (cached providers) and every state-bearing
            // file. State survives every clean() call unconditionally.
            if name == ".terraform"
                || name == ".terraform.lock.hcl"
                || name == STATE_FILE_NAME
                || name == STATE_BACKUP_FILE_NAME
            {
                continue;
            }

            if path.is_dir() {
                fs::remove_dir_all(&path).await.map_err(|e| Error::Io(e))?;
            } else {
                fs::remove_file(&path).await.map_err(|e| Error::Io(e))?;
            }
        }

        Ok(())
    }
}

impl Drop for Workspace {
    fn drop(&mut self) {
        if self.cleanup_on_drop {
            let path = self.path.clone();
            tokio::spawn(async move {
                let _ = fs::remove_dir_all(&path).await;
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a `Workspace` rooted at a fresh temp dir. The returned
    /// `TempDir` must stay alive for the workspace's lifetime — it
    /// deletes itself on drop.
    fn make_workspace() -> (tempfile::TempDir, Workspace) {
        let dir = tempfile::tempdir().expect("create temp dir");
        let ws = Workspace {
            path: dir.path().to_path_buf(),
            namespace: "test-ns".to_string(),
            name: "test-template".to_string(),
            cleanup_on_drop: false,
        };
        (dir, ws)
    }

    // ── Bug 1 regression: clean() must never delete state ──────────

    #[tokio::test]
    async fn clean_preserves_existing_terraform_tfstate() {
        let (_dir, ws) = make_workspace();
        fs::write(ws.state_path(), r#"{"version":4,"serial":3}"#)
            .await
            .expect("seed state file");
        fs::write(ws.main_tf_path(), "{}").await.expect("seed main.tf.json");
        fs::write(ws.plan_path(), b"binary-plan-data").await.expect("seed plan file");

        ws.clean().await.expect("clean must succeed");

        assert!(
            ws.state_path().exists(),
            "clean() must never delete terraform.tfstate (the 2026-07-12 \
             camelot-eks duplicate-VPC bug)"
        );
        assert_eq!(
            fs::read_to_string(ws.state_path()).await.unwrap(),
            r#"{"version":4,"serial":3}"#,
            "state CONTENT must be untouched, not just the filename surviving"
        );
        assert!(!ws.main_tf_path().exists(), "rendered config must still be cleaned");
        assert!(!ws.plan_path().exists(), "plan artifacts must still be cleaned");
    }

    #[tokio::test]
    async fn clean_preserves_state_backup_file() {
        let (_dir, ws) = make_workspace();
        fs::write(ws.state_backup_path(), r#"{"version":4,"serial":2}"#)
            .await
            .expect("seed backup file");

        ws.clean().await.expect("clean must succeed");

        assert!(
            ws.state_backup_path().exists(),
            "clean() must preserve terraform.tfstate.backup (tofu's own \
             one-generation rollback safety net)"
        );
    }

    #[tokio::test]
    async fn clean_still_removes_non_state_files_and_keeps_terraform_dir() {
        let (_dir, ws) = make_workspace();
        fs::write(ws.path.join(".terraform.lock.hcl"), "lock")
            .await
            .expect("seed lock file");
        fs::create_dir(ws.path.join(".terraform")).await.expect("seed .terraform dir");
        fs::write(ws.path.join(".terraform").join("providers.json"), "{}")
            .await
            .expect("seed cached provider file");
        fs::write(ws.backend_path(), "{}").await.expect("seed backend.tf.json");
        fs::write(ws.providers_path(), "{}").await.expect("seed providers.tf.json");

        ws.clean().await.expect("clean must succeed");

        assert!(ws.path.join(".terraform.lock.hcl").exists(), "lock file must survive");
        assert!(ws.path.join(".terraform").exists(), "cached provider dir must survive");
        assert!(!ws.backend_path().exists(), "backend.tf.json must still be cleaned");
        assert!(!ws.providers_path().exists(), "providers.tf.json must still be cleaned");
    }

    #[tokio::test]
    async fn clean_with_no_state_file_yet_is_not_an_error() {
        // First-ever apply: no terraform.tfstate exists. This is NOT
        // the bug class (state that existed, then got wiped) — it's
        // the ordinary bootstrap case, and clean() must handle it
        // gracefully rather than erroring on a missing file.
        let (_dir, ws) = make_workspace();
        fs::write(ws.main_tf_path(), "{}").await.expect("seed main.tf.json");

        let result = ws.clean().await;

        assert!(result.is_ok(), "clean() on a never-applied workspace must succeed");
        assert!(!ws.state_path().exists());
        assert!(!ws.main_tf_path().exists());
    }

    #[tokio::test]
    async fn clean_is_idempotent_across_repeated_generation_bumps() {
        // Simulates the exact incident sequence: state exists from a
        // real apply, then N spec-edit-triggered clean() calls fire in
        // a row (each a generation bump) before the next apply. State
        // must survive every single one, unchanged.
        let (_dir, ws) = make_workspace();
        fs::write(ws.state_path(), "real-state-bytes").await.unwrap();

        for _ in 0..5 {
            fs::write(ws.main_tf_path(), "{}").await.unwrap();
            ws.clean().await.expect("clean must succeed");
            assert!(ws.state_path().exists());
        }

        assert_eq!(
            fs::read_to_string(ws.state_path()).await.unwrap(),
            "real-state-bytes"
        );
    }

    // ── Bug 2 regression: invalidate_repo_cache must never delete state ──
    //
    // See `docs/postmortems/2026-07-12-camelot-eks-state-wipe-duplicate-vpc.md`.
    // `WorkspaceCacheInvalidator for WorkspaceManager` used to call
    // `delete_workspace`, which removes the WHOLE workspace directory.
    // `invalidate_repo_cache` is the fix: its deletion target is not a
    // parameter, it is always `.../_repo`, so state can never be in its
    // reach. These tests exercise `WorkspaceManager` directly (not just
    // `Workspace`) since the bug lived in the manager-level method the
    // real invalidator calls — the end-to-end version through the trait
    // object lives in `controller::escalation_handlers::tests`.

    #[tokio::test]
    async fn invalidate_repo_cache_removes_repo_but_preserves_state() {
        let base = tempfile::tempdir().expect("create temp base dir");
        let wm = WorkspaceManager::new(base.path().to_path_buf());
        let ws = wm
            .get_or_create("ns", "tmpl")
            .await
            .expect("create workspace");

        fs::write(ws.state_path(), r#"{"version":4,"serial":9}"#)
            .await
            .expect("seed state file");
        fs::write(ws.state_backup_path(), r#"{"version":4,"serial":8}"#)
            .await
            .expect("seed backup file");
        let repo_dir = ws.path.join("_repo");
        fs::create_dir_all(&repo_dir).await.expect("seed _repo dir");
        fs::write(repo_dir.join("cloned.tf"), "source").await.expect("seed repo file");

        wm.invalidate_repo_cache("ns", "tmpl")
            .await
            .expect("invalidate_repo_cache must succeed");

        assert!(!repo_dir.exists(), "_repo must be removed");
        assert!(
            ws.state_path().exists(),
            "terraform.tfstate must survive — this method must be structurally \
             incapable of reaching it"
        );
        assert_eq!(
            fs::read_to_string(ws.state_path()).await.unwrap(),
            r#"{"version":4,"serial":9}"#
        );
        assert!(ws.state_backup_path().exists(), "terraform.tfstate.backup must survive");
    }

    #[tokio::test]
    async fn invalidate_repo_cache_with_no_repo_dir_yet_is_not_an_error() {
        let base = tempfile::tempdir().expect("create temp base dir");
        let wm = WorkspaceManager::new(base.path().to_path_buf());
        wm.get_or_create("ns", "tmpl").await.expect("create workspace");

        let result = wm.invalidate_repo_cache("ns", "tmpl").await;

        assert!(result.is_ok(), "invalidating a never-cloned workspace must be a no-op, not an error");
    }

    // ── read_state_bytes ────────────────────────────────────────────

    #[tokio::test]
    async fn read_state_bytes_returns_none_when_no_state_file_exists() {
        let (_dir, ws) = make_workspace();
        assert_eq!(ws.read_state_bytes().await, None);
    }

    #[tokio::test]
    async fn read_state_bytes_returns_the_file_contents() {
        let (_dir, ws) = make_workspace();
        fs::write(ws.state_path(), b"some-state-bytes").await.unwrap();
        assert_eq!(
            ws.read_state_bytes().await,
            Some(b"some-state-bytes".to_vec())
        );
    }

    #[tokio::test]
    async fn read_state_bytes_reflects_a_wipe() {
        // After a legitimate full wipe (WorkspaceManager::delete_workspace
        // recreating the dir, or a fresh workspace), read_state_bytes
        // must observe the absence — this is what lets the plan-approval
        // hash (template_controller::plan_approval_hash) tell "state
        // existed" apart from "state is gone."
        let (_dir, ws) = make_workspace();
        fs::write(ws.state_path(), b"before").await.unwrap();
        assert!(ws.read_state_bytes().await.is_some());

        fs::remove_file(ws.state_path()).await.unwrap();
        assert_eq!(ws.read_state_bytes().await, None);
    }
}
