//! Import existing Terraform state into pangea-operator.
//!
//! Reads a local Terraform state directory and generates an
//! InfrastructureTemplate CR manifest for creating in the cluster.
//! After applying the manifest and importing state into the PostgreSQL
//! backend, the operator takes over management — the local bootstrap
//! layer is no longer needed.

use crate::client::PangeaClient;
use crate::output::OutputFormat;
use anyhow::{bail, Context, Result};
use std::path::Path;

/// Import local Terraform state into pangea-operator.
///
/// This command:
/// 1. Reads the main.tf.json from the state directory
/// 2. Generates an InfrastructureTemplate manifest
/// 3. Prints instructions for state migration
///
/// With `--destroy-protection`, the imported template cannot be destroyed
/// via the operator — critical for bootstrap infrastructure (VPC, EKS, RDS)
/// that the operator itself runs on.
pub async fn import_state(
    _client: &PangeaClient,
    state_dir: &str,
    template_name: &str,
    namespace: &str,
    pangea_namespace: &str,
    auto_approve: bool,
    destroy_protection: bool,
    _output: OutputFormat,
) -> Result<()> {
    let state_path = Path::new(state_dir);

    if !state_path.exists() {
        bail!("State directory does not exist: {}", state_dir);
    }

    let tf_config =
        read_terraform_config(state_path).context("Failed to read Terraform configuration")?;

    let state_file = state_path.join("terraform.tfstate");
    let has_local_state = state_file.exists();

    println!(
        "Importing state from {} into InfrastructureTemplate {}/{}",
        state_dir, namespace, template_name
    );
    if destroy_protection {
        println!("  Destroy protection: ENABLED (recommended for bootstrap infra)");
    }
    println!();

    // Step 1: Print the manifest
    println!("Step 1 — Apply this manifest:");
    println!("---");
    print_kubectl_manifest(
        template_name,
        namespace,
        pangea_namespace,
        auto_approve,
        destroy_protection,
        &tf_config,
    );

    // Step 2: State migration
    if has_local_state {
        println!();
        println!("Step 2 — Import existing state into PostgreSQL backend:");
        println!("  cd {}", state_dir);
        println!("  tofu init \\");
        println!("    -backend-config='conn_str=<pangea_pg_url>' \\");
        println!(
            "    -backend-config='schema_name=pangea_{}_{}_states'",
            pangea_namespace, template_name
        );
        println!("  tofu state push terraform.tfstate");
    } else {
        println!();
        println!("Step 2 — No local state file found. If state exists in a remote backend,");
        println!("  migrate it to the PostgreSQL backend before the operator reconciles.");
    }

    // Step 3: Verify
    println!();
    println!("Step 3 — Verify the operator has taken over:");
    println!("  kubectl get infra {} -n {} -w", template_name, namespace);
    println!("  # Should show: Pending -> Compiling -> Initializing -> Planning -> Ready");

    // Step 4: Disconnect
    println!();
    println!("Step 4 — Disconnect from the bootstrap layer:");
    println!("  The operator now manages this infrastructure via CRD.");
    if destroy_protection {
        println!("  Destroy protection is ON — the operator will refuse to destroy this infra.");
        println!("  Plan/apply for drift correction continues normally.");
    }
    println!("  The local pangea bootstrap is no longer needed for management.");

    Ok(())
}

/// Read Terraform configuration from a directory.
fn read_terraform_config(state_path: &Path) -> Result<String> {
    // Try main.tf.json first
    let json_path = state_path.join("main.tf.json");
    if json_path.exists() {
        return std::fs::read_to_string(&json_path).context("Failed to read main.tf.json");
    }

    // Try to find any .tf.json files and merge them
    let mut found = Vec::new();
    for entry in std::fs::read_dir(state_path)? {
        let entry = entry?;
        let path = entry.path();
        let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
        if name.ends_with(".tf.json") {
            found.push(path);
        }
    }

    if found.is_empty() {
        bail!(
            "No Terraform JSON configuration found in {}. Expected main.tf.json or *.tf.json files.",
            state_path.display()
        );
    }

    if found.len() == 1 {
        return std::fs::read_to_string(&found[0])
            .with_context(|| format!("Failed to read {}", found[0].display()));
    }

    // Multiple files — merge them into one JSON object
    let mut merged = serde_json::Map::new();
    for path in &found {
        let content = std::fs::read_to_string(path)
            .with_context(|| format!("Failed to read {}", path.display()))?;
        let value: serde_json::Value = serde_json::from_str(&content)
            .with_context(|| format!("Invalid JSON in {}", path.display()))?;
        if let serde_json::Value::Object(map) = value {
            for (k, v) in map {
                merged.insert(k, v);
            }
        }
    }

    serde_json::to_string_pretty(&serde_json::Value::Object(merged))
        .context("Failed to serialize merged configuration")
}

/// Print a kubectl-compatible YAML manifest for the InfrastructureTemplate.
fn print_kubectl_manifest(
    template_name: &str,
    namespace: &str,
    pangea_namespace: &str,
    auto_approve: bool,
    destroy_protection: bool,
    tf_config: &str,
) {
    let indented: String = tf_config
        .lines()
        .map(|l| format!("      {}", l))
        .collect::<Vec<_>>()
        .join("\n");

    println!(
        r#"apiVersion: pangea.pleme.io/v1alpha1
kind: InfrastructureTemplate
metadata:
  name: {template_name}
  namespace: {namespace}
spec:
  pangeaNamespace: {pangea_namespace}
  autoApprove: {auto_approve}
  destroyProtection: {destroy_protection}
  refreshInterval: "10m"
  source:
    inline: |
{indented}"#
    );
}
