//! CLI configuration management.

use anyhow::{Context, Result};
use directories::ProjectDirs;
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::PathBuf;

/// CLI configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    /// GraphQL endpoint URL.
    #[serde(default = "default_endpoint")]
    pub endpoint: String,

    /// API authentication token.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token: Option<String>,

    /// Default Kubernetes namespace.
    #[serde(default)]
    pub default_namespace: Option<String>,

    /// Output format (table, json, yaml).
    #[serde(default = "default_output")]
    pub output: String,
}

fn default_endpoint() -> String {
    // Default to PLO cluster internal address
    "http://pangea-api.quero.local/graphql".to_string()
}

fn default_output() -> String {
    "table".to_string()
}

impl Default for Config {
    fn default() -> Self {
        Self {
            endpoint: default_endpoint(),
            token: None,
            default_namespace: None,
            output: default_output(),
        }
    }
}

impl Config {
    /// Get the config directory path.
    pub fn config_dir() -> Result<PathBuf> {
        if let Some(proj_dirs) = ProjectDirs::from("io", "pleme", "pangea") {
            Ok(proj_dirs.config_dir().to_path_buf())
        } else {
            // Fallback to ~/.pangea
            let home = std::env::var("HOME").context("HOME not set")?;
            Ok(PathBuf::from(home).join(".pangea"))
        }
    }

    /// Get the config file path.
    pub fn config_path() -> Result<PathBuf> {
        Ok(Self::config_dir()?.join("config.yaml"))
    }

    /// Load configuration from file.
    pub fn load() -> Result<Self> {
        let path = Self::config_path()?;

        let mut config = if path.exists() {
            let content = fs::read_to_string(&path)
                .with_context(|| format!("Failed to read config from {:?}", path))?;
            serde_yaml::from_str(&content)
                .with_context(|| format!("Failed to parse config from {:?}", path))?
        } else {
            Config::default()
        };

        // Environment variables override config file
        if let Ok(endpoint) = std::env::var("PANGEA_ENDPOINT") {
            config.endpoint = endpoint;
        }

        if let Ok(token) = std::env::var("PANGEA_TOKEN") {
            config.token = Some(token);
        }

        if let Ok(namespace) = std::env::var("PANGEA_NAMESPACE") {
            config.default_namespace = Some(namespace);
        }

        Ok(config)
    }

    /// Save configuration to file.
    pub fn save(&self) -> Result<()> {
        let path = Self::config_path()?;
        let dir = Self::config_dir()?;

        // Create config directory if it doesn't exist
        if !dir.exists() {
            fs::create_dir_all(&dir)
                .with_context(|| format!("Failed to create config directory {:?}", dir))?;
        }

        let content = serde_yaml::to_string(self).context("Failed to serialize config")?;
        fs::write(&path, content)
            .with_context(|| format!("Failed to write config to {:?}", path))?;

        Ok(())
    }

    /// Set a configuration value.
    pub fn set(&mut self, key: &str, value: &str) -> Result<()> {
        match key {
            "endpoint" => self.endpoint = value.to_string(),
            "token" => {
                self.token = if value.is_empty() {
                    None
                } else {
                    Some(value.to_string())
                }
            }
            "default_namespace" | "namespace" => {
                self.default_namespace = if value.is_empty() {
                    None
                } else {
                    Some(value.to_string())
                }
            }
            "output" => {
                if !["table", "json", "yaml"].contains(&value) {
                    anyhow::bail!("Invalid output format: {}. Must be table, json, or yaml", value);
                }
                self.output = value.to_string();
            }
            _ => anyhow::bail!("Unknown configuration key: {}", key),
        }
        Ok(())
    }

    /// Get a configuration value.
    pub fn get(&self, key: &str) -> Option<String> {
        match key {
            "endpoint" => Some(self.endpoint.clone()),
            "token" => self.token.clone(),
            "default_namespace" | "namespace" => self.default_namespace.clone(),
            "output" => Some(self.output.clone()),
            _ => None,
        }
    }
}
