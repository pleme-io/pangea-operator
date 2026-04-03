//! Pangea CLI - Command-line interface for Pangea infrastructure management.

mod client;
mod commands;
mod config;
mod output;
mod schema;

use anyhow::Result;
use clap::{Parser, Subcommand};

use commands::{logs, namespace, plan, template};
use config::Config;

/// Pangea CLI - Manage infrastructure templates via GraphQL API.
#[derive(Parser)]
#[command(name = "pangea")]
#[command(author, version, about, long_about = None)]
#[command(propagate_version = true)]
struct Cli {
    /// GraphQL endpoint URL
    #[arg(long, env = "PANGEA_ENDPOINT", global = true)]
    endpoint: Option<String>,

    /// API authentication token
    #[arg(long, env = "PANGEA_TOKEN", global = true)]
    token: Option<String>,

    /// Output format (table, json, yaml)
    #[arg(long, short, default_value = "table", global = true)]
    output: output::OutputFormat,

    /// Enable verbose output
    #[arg(long, short, global = true)]
    verbose: bool,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Manage infrastructure templates
    Template {
        #[command(subcommand)]
        command: template::TemplateCommands,
    },

    /// Manage Pangea namespaces
    Namespace {
        #[command(subcommand)]
        command: namespace::NamespaceCommands,
    },

    /// Show plan for a template
    Plan {
        /// Template reference (namespace/name or just name)
        template: String,

        /// Kubernetes namespace (if not specified in template ref)
        #[arg(long, short)]
        namespace: Option<String>,
    },

    /// Apply changes to a template
    Apply {
        /// Template reference (namespace/name or just name)
        template: String,

        /// Kubernetes namespace (if not specified in template ref)
        #[arg(long, short)]
        namespace: Option<String>,
    },

    /// Approve pending changes for a template
    Approve {
        /// Template reference (namespace/name or just name)
        template: String,

        /// Kubernetes namespace (if not specified in template ref)
        #[arg(long, short)]
        namespace: Option<String>,
    },

    /// Import existing Terraform state into pangea-operator
    Import {
        /// Path to the local Terraform state directory
        #[arg(long)]
        state_dir: String,

        /// Target template name to create
        #[arg(long)]
        template_name: String,

        /// Target Kubernetes namespace for the template
        #[arg(long, short, default_value = "default")]
        namespace: String,

        /// PangeaNamespace to use for state storage
        #[arg(long, default_value = "development")]
        pangea_namespace: String,

        /// Enable auto-approve on the imported template
        #[arg(long)]
        auto_approve: bool,

        /// Enable destroy protection (recommended for bootstrap infrastructure)
        #[arg(long)]
        destroy_protection: bool,
    },

    /// Stream logs for a template
    Logs {
        /// Template reference (namespace/name or just name)
        template: String,

        /// Kubernetes namespace (if not specified in template ref)
        #[arg(long, short)]
        namespace: Option<String>,

        /// Follow logs (stream continuously)
        #[arg(long, short)]
        follow: bool,
    },

    /// Show CLI configuration
    Config {
        #[command(subcommand)]
        command: Option<ConfigCommands>,
    },
}

#[derive(Subcommand)]
enum ConfigCommands {
    /// Show current configuration
    Show,
    /// Set configuration value
    Set {
        /// Configuration key
        key: String,
        /// Configuration value
        value: String,
    },
    /// Get configuration value
    Get {
        /// Configuration key
        key: String,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    // Load configuration
    let mut config = Config::load()?;

    // Override from CLI if provided
    if let Some(endpoint) = cli.endpoint {
        config.endpoint = endpoint;
    }
    if let Some(token) = cli.token {
        config.token = Some(token);
    }

    // Create client with token
    let client = client::PangeaClient::new(&config.endpoint, config.token.clone())?;

    match cli.command {
        Commands::Template { command } => {
            template::execute(command, &client, cli.output).await?;
        }

        Commands::Namespace { command } => {
            namespace::execute(command, &client, cli.output).await?;
        }

        Commands::Plan { template, namespace } => {
            plan::show_plan(&client, &template, namespace.as_deref(), cli.output).await?;
        }

        Commands::Apply { template, namespace } => {
            template::apply(&client, &template, namespace.as_deref(), cli.output).await?;
        }

        Commands::Approve { template, namespace } => {
            template::approve(&client, &template, namespace.as_deref(), cli.output).await?;
        }

        Commands::Import {
            state_dir,
            template_name,
            namespace,
            pangea_namespace,
            auto_approve,
            destroy_protection,
        } => {
            commands::import::import_state(
                &client,
                &state_dir,
                &template_name,
                &namespace,
                &pangea_namespace,
                auto_approve,
                destroy_protection,
                cli.output,
            )
            .await?;
        }

        Commands::Logs {
            template,
            namespace,
            follow,
        } => {
            logs::stream_logs(&client, &template, namespace.as_deref(), follow).await?;
        }

        Commands::Config { command } => match command {
            Some(ConfigCommands::Show) | None => {
                println!("{}", serde_yaml::to_string(&config)?);
            }
            Some(ConfigCommands::Set { key, value }) => {
                config.set(&key, &value)?;
                config.save()?;
                println!("Set {} = {}", key, value);
            }
            Some(ConfigCommands::Get { key }) => {
                if let Some(value) = config.get(&key) {
                    println!("{}", value);
                } else {
                    eprintln!("Key not found: {}", key);
                    std::process::exit(1);
                }
            }
        },
    }

    Ok(())
}
