//! Namespace management commands.

use anyhow::Result;
use clap::Subcommand;

use crate::client::PangeaClient;
use crate::output::{self, OutputFormat};

#[derive(Subcommand)]
pub enum NamespaceCommands {
    /// List Pangea namespaces
    List,

    /// Get details for a namespace
    Get {
        /// Namespace name
        name: String,
    },
}

/// Execute a namespace command.
pub async fn execute(
    command: NamespaceCommands,
    client: &PangeaClient,
    format: OutputFormat,
) -> Result<()> {
    match command {
        NamespaceCommands::List => {
            let namespaces = client.list_namespaces().await?;

            if namespaces.is_empty() {
                println!("No namespaces found");
            } else {
                output::print_namespaces(&namespaces, format)?;
            }
        }

        NamespaceCommands::Get { name } => {
            let namespace = client.get_namespace(&name).await?;

            match namespace {
                Some(ns) => match format {
                    OutputFormat::Table => {
                        println!("\nNamespace: {}", ns.name.clone().unwrap_or_default());
                        println!("  Backend Type:    {}", ns.backend_type);
                        println!(
                            "  Database Host:   {}",
                            ns.database_host.clone().unwrap_or_default()
                        );
                        println!(
                            "  Database Name:   {}",
                            ns.database_name.clone().unwrap_or_default()
                        );
                        println!(
                            "  Schema Name:     {}",
                            ns.schema_name.clone().unwrap_or_default()
                        );
                        println!(
                            "  Ready:           {}",
                            if ns.is_ready { "Yes" } else { "No" }
                        );
                        println!("  Template Count:  {}", ns.template_count);
                    }
                    OutputFormat::Json => {
                        println!("{}", serde_json::to_string_pretty(&ns)?);
                    }
                    OutputFormat::Yaml => {
                        println!("{}", serde_yaml::to_string(&ns)?);
                    }
                },
                None => {
                    eprintln!("Namespace not found: {}", name);
                    std::process::exit(1);
                }
            }
        }
    }

    Ok(())
}
