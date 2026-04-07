//! Packer command execution and manifest parsing.

use crate::error::{Error, Result};
use std::collections::HashMap;
use std::path::Path;
use std::process::Stdio;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Command;
use tracing::{debug, error, info, warn};

/// Packer command executor.
pub struct PackerExecutor {
    /// Path to the packer binary.
    binary: std::path::PathBuf,

    /// Command timeout.
    timeout: Duration,

    /// Whether to enable verbose output.
    verbose: bool,
}

/// Packer command types.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PackerCommand {
    Init,
    Validate,
    Build,
    Inspect,
}

impl PackerCommand {
    fn as_str(&self) -> &'static str {
        match self {
            PackerCommand::Init => "init",
            PackerCommand::Validate => "validate",
            PackerCommand::Build => "build",
            PackerCommand::Inspect => "inspect",
        }
    }
}

/// Result of a packer command execution.
#[derive(Debug, Clone)]
pub struct PackerResult {
    /// Exit code.
    pub exit_code: i32,

    /// Standard output.
    pub stdout: String,

    /// Standard error.
    pub stderr: String,

    /// Whether the command succeeded.
    pub success: bool,

    /// Execution duration.
    pub duration: Duration,
}

impl PackerExecutor {
    /// Create a new executor.
    pub fn new(binary: std::path::PathBuf, timeout: Duration, verbose: bool) -> Self {
        Self {
            binary,
            timeout,
            verbose,
        }
    }

    /// Run `packer init`.
    pub async fn init(&self, work_dir: &Path, extra_args: &[&str]) -> Result<PackerResult> {
        let mut args: Vec<&str> = Vec::new();
        args.extend(extra_args);
        // Point at the working directory itself (contains *.pkr.json)
        args.push(".");
        self.execute(PackerCommand::Init, work_dir, &args, &HashMap::new())
            .await
    }

    /// Run `packer validate`.
    pub async fn validate(&self, work_dir: &Path) -> Result<PackerResult> {
        self.execute(
            PackerCommand::Validate,
            work_dir,
            &["."],
            &HashMap::new(),
        )
        .await
    }

    /// Run `packer build` with variables and var-files.
    pub async fn build(
        &self,
        work_dir: &Path,
        vars: &HashMap<String, String>,
        var_files: &[&Path],
        on_error: &str,
    ) -> Result<PackerResult> {
        let mut args: Vec<String> = Vec::new();

        args.push(format!("-on-error={on_error}"));

        for (key, value) in vars {
            args.push("-var".to_string());
            args.push(format!("{key}={value}"));
        }

        for vf in var_files {
            args.push("-var-file".to_string());
            args.push(vf.to_string_lossy().to_string());
        }

        args.push(".".to_string());

        let arg_refs: Vec<&str> = args.iter().map(|s| s.as_str()).collect();
        self.execute(PackerCommand::Build, work_dir, &arg_refs, &HashMap::new())
            .await
    }

    /// Run `packer inspect`.
    pub async fn inspect_config(&self, work_dir: &Path) -> Result<PackerResult> {
        self.execute(
            PackerCommand::Inspect,
            work_dir,
            &["."],
            &HashMap::new(),
        )
        .await
    }

    /// Execute a packer command.
    async fn execute(
        &self,
        command: PackerCommand,
        work_dir: &Path,
        args: &[&str],
        env: &HashMap<String, String>,
    ) -> Result<PackerResult> {
        let start = std::time::Instant::now();
        let cmd_str = command.as_str();

        info!(
            command = cmd_str,
            work_dir = ?work_dir,
            args = ?args,
            "Executing packer command"
        );

        let mut cmd = Command::new(&self.binary);
        cmd.arg(cmd_str)
            .args(args)
            .current_dir(work_dir)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .env("PACKER_NO_COLOR", "1");

        if self.verbose {
            cmd.env("PACKER_LOG", "1");
        }

        // Add custom environment variables
        for (key, value) in env {
            cmd.env(key, value);
        }

        let mut child = cmd.spawn().map_err(|e| {
            Error::PackerExecution(format!("Failed to spawn packer: {}", e))
        })?;

        let stdout_handle = child.stdout.take().unwrap();
        let stderr_handle = child.stderr.take().unwrap();

        // Read output streams
        let stdout_task = tokio::spawn(async move {
            let mut lines = Vec::new();
            let mut reader = BufReader::new(stdout_handle).lines();
            while let Ok(Some(line)) = reader.next_line().await {
                lines.push(line);
            }
            lines.join("\n")
        });

        let stderr_task = tokio::spawn(async move {
            let mut lines = Vec::new();
            let mut reader = BufReader::new(stderr_handle).lines();
            while let Ok(Some(line)) = reader.next_line().await {
                lines.push(line);
            }
            lines.join("\n")
        });

        // Wait with timeout
        let wait_result = tokio::time::timeout(self.timeout, child.wait()).await;

        let status = match wait_result {
            Ok(Ok(status)) => status,
            Ok(Err(e)) => {
                return Err(Error::PackerExecution(format!(
                    "Failed to wait for packer: {}",
                    e
                )));
            }
            Err(_) => {
                error!(command = cmd_str, "Packer command timed out");
                let _ = child.kill().await;
                return Err(Error::Timeout(self.timeout.as_secs()));
            }
        };

        let stdout = stdout_task.await.unwrap_or_default();
        let stderr = stderr_task.await.unwrap_or_default();

        let exit_code = status.code().unwrap_or(-1);
        let success = status.success();
        let duration = start.elapsed();

        if self.verbose {
            debug!(stdout = %stdout, "Packer stdout");
            if !stderr.is_empty() {
                debug!(stderr = %stderr, "Packer stderr");
            }
        }

        if success {
            info!(
                command = cmd_str,
                exit_code,
                duration_secs = duration.as_secs_f64(),
                "Packer command completed"
            );
        } else {
            warn!(
                command = cmd_str,
                exit_code,
                stderr = %stderr,
                "Packer command failed"
            );
        }

        Ok(PackerResult {
            exit_code,
            stdout,
            stderr,
            success,
            duration,
        })
    }
}

/// Parse a `packer-manifest.json` file and extract the AMI ID from the last build.
///
/// Expected format:
/// ```json
/// {
///   "builds": [
///     { "artifact_id": "us-east-1:ami-0123456789abcdef0" }
///   ]
/// }
/// ```
pub fn parse_packer_manifest(manifest_path: &Path) -> Result<String> {
    let content = std::fs::read_to_string(manifest_path).map_err(|e| {
        Error::PackerManifest(format!("Failed to read {}: {}", manifest_path.display(), e))
    })?;

    let json: serde_json::Value = serde_json::from_str(&content).map_err(|e| {
        Error::PackerManifest(format!("Failed to parse {}: {}", manifest_path.display(), e))
    })?;

    let ami_id = json["builds"]
        .as_array()
        .and_then(|builds| builds.last())
        .and_then(|build| build["artifact_id"].as_str())
        .and_then(|artifact| artifact.split(':').nth(1))
        .ok_or_else(|| {
            let builds = json["builds"].as_array().map(|b| b.len()).unwrap_or(0);
            Error::PackerManifest(format!(
                "Could not extract AMI ID from {} ({} builds found). \
                 Expected format: {{\"builds\": [{{\"artifact_id\": \"region:ami-xxx\"}}]}}",
                manifest_path.display(),
                builds
            ))
        })?
        .to_string();

    info!(ami = %ami_id, "Parsed AMI ID from packer manifest");
    Ok(ami_id)
}

/// Extract the region from a packer manifest artifact_id (e.g., "us-east-1" from "us-east-1:ami-xxx").
pub fn parse_packer_manifest_region(manifest_path: &Path) -> Result<Option<String>> {
    let content = std::fs::read_to_string(manifest_path).map_err(|e| {
        Error::PackerManifest(format!("Failed to read {}: {}", manifest_path.display(), e))
    })?;

    let json: serde_json::Value = serde_json::from_str(&content).map_err(|e| {
        Error::PackerManifest(format!("Failed to parse {}: {}", manifest_path.display(), e))
    })?;

    let region = json["builds"]
        .as_array()
        .and_then(|builds| builds.last())
        .and_then(|build| build["artifact_id"].as_str())
        .and_then(|artifact| artifact.split(':').next())
        .map(|s| s.to_string());

    Ok(region)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[test]
    fn test_parse_packer_manifest() {
        let mut f = NamedTempFile::new().unwrap();
        writeln!(
            f,
            r#"{{"builds": [{{"artifact_id": "us-east-1:ami-0abc123def456"}}]}}"#
        )
        .unwrap();

        let ami = parse_packer_manifest(f.path()).unwrap();
        assert_eq!(ami, "ami-0abc123def456");
    }

    #[test]
    fn test_parse_packer_manifest_region() {
        let mut f = NamedTempFile::new().unwrap();
        writeln!(
            f,
            r#"{{"builds": [{{"artifact_id": "us-west-2:ami-0abc123def456"}}]}}"#
        )
        .unwrap();

        let region = parse_packer_manifest_region(f.path()).unwrap();
        assert_eq!(region, Some("us-west-2".to_string()));
    }

    #[test]
    fn test_parse_packer_manifest_multiple_builds() {
        let mut f = NamedTempFile::new().unwrap();
        writeln!(
            f,
            r#"{{"builds": [
                {{"artifact_id": "us-east-1:ami-old"}},
                {{"artifact_id": "us-east-1:ami-new"}}
            ]}}"#
        )
        .unwrap();

        let ami = parse_packer_manifest(f.path()).unwrap();
        assert_eq!(ami, "ami-new");
    }

    #[test]
    fn test_parse_packer_manifest_empty_builds() {
        let mut f = NamedTempFile::new().unwrap();
        writeln!(f, r#"{{"builds": []}}"#).unwrap();

        let result = parse_packer_manifest(f.path());
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_packer_manifest_missing_artifact_id() {
        let mut f = NamedTempFile::new().unwrap();
        writeln!(f, r#"{{"builds": [{{"name": "test"}}]}}"#).unwrap();

        let result = parse_packer_manifest(f.path());
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_packer_manifest_invalid_json() {
        let mut f = NamedTempFile::new().unwrap();
        writeln!(f, "not valid json").unwrap();

        let result = parse_packer_manifest(f.path());
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_packer_manifest_nonexistent_file() {
        let result = parse_packer_manifest(Path::new("/tmp/nonexistent_packer_manifest.json"));
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_packer_manifest_region_empty_builds() {
        let mut f = NamedTempFile::new().unwrap();
        writeln!(f, r#"{{"builds": []}}"#).unwrap();

        let region = parse_packer_manifest_region(f.path()).unwrap();
        assert!(region.is_none());
    }

    #[test]
    fn test_parse_packer_manifest_region_nonexistent_file() {
        let result = parse_packer_manifest_region(Path::new("/tmp/nonexistent_file.json"));
        assert!(result.is_err());
    }

    #[test]
    fn test_packer_command_as_str() {
        assert_eq!(PackerCommand::Init.as_str(), "init");
        assert_eq!(PackerCommand::Validate.as_str(), "validate");
        assert_eq!(PackerCommand::Build.as_str(), "build");
        assert_eq!(PackerCommand::Inspect.as_str(), "inspect");
    }

    #[test]
    fn test_packer_executor_new() {
        let exec = PackerExecutor::new(
            std::path::PathBuf::from("/usr/bin/packer"),
            Duration::from_secs(300),
            true,
        );
        assert!(exec.verbose);
        assert_eq!(exec.timeout, Duration::from_secs(300));
    }

    #[test]
    fn test_parse_packer_manifest_artifact_without_colon() {
        let mut f = NamedTempFile::new().unwrap();
        writeln!(f, r#"{{"builds": [{{"artifact_id": "ami-no-region"}}]}}"#).unwrap();

        let result = parse_packer_manifest(f.path());
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_packer_manifest_picks_last_build() {
        let mut f = NamedTempFile::new().unwrap();
        writeln!(f, r#"{{
            "builds": [
                {{"artifact_id": "us-east-1:ami-first"}},
                {{"artifact_id": "us-east-1:ami-second"}},
                {{"artifact_id": "us-east-1:ami-third"}}
            ]
        }}"#).unwrap();

        let ami = parse_packer_manifest(f.path()).unwrap();
        assert_eq!(ami, "ami-third");
    }
}
