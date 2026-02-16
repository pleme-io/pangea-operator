//! Workspace management for template execution.
//!
//! Each InfrastructureTemplate gets an isolated workspace directory
//! containing the compiled Terraform JSON, backend configuration,
//! and execution artifacts.

use crate::crd::InfrastructureTemplate;
use crate::error::{Error, Result};
use std::path::{Path, PathBuf};
use tokio::fs;
use tracing::{debug, info};

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

    /// Delete a workspace.
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

    /// Clean the workspace (remove all files except .terraform).
    pub async fn clean(&self) -> Result<()> {
        let mut entries = fs::read_dir(&self.path)
            .await
            .map_err(|e| Error::Io(e))?;

        while let Some(entry) = entries.next_entry().await.map_err(|e| Error::Io(e))? {
            let path = entry.path();
            let name = entry.file_name().to_string_lossy().to_string();

            // Keep .terraform directory (cached providers)
            if name == ".terraform" || name == ".terraform.lock.hcl" {
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
