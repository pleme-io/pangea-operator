//! Template management commands.

use anyhow::Result;
use clap::Subcommand;
use colored::Colorize;

use crate::client::{parse_template_ref, PangeaClient};
use crate::output::{self, OutputFormat};
use crate::schema::Phase;

#[derive(Subcommand)]
pub enum TemplateCommands {
    /// List infrastructure templates
    List {
        /// Filter by Kubernetes namespace
        #[arg(long, short)]
        namespace: Option<String>,

        /// Filter by phase
        #[arg(long, short)]
        phase: Option<String>,
    },

    /// Get details for a template
    Get {
        /// Template reference (namespace/name)
        template: String,

        /// Kubernetes namespace (if not in template ref)
        #[arg(long, short)]
        namespace: Option<String>,
    },

    /// Trigger apply for a template
    Apply {
        /// Template reference (namespace/name)
        template: String,

        /// Kubernetes namespace (if not in template ref)
        #[arg(long, short)]
        namespace: Option<String>,
    },

    /// Approve pending changes
    Approve {
        /// Template reference (namespace/name)
        template: String,

        /// Kubernetes namespace (if not in template ref)
        #[arg(long, short)]
        namespace: Option<String>,
    },

    /// Destroy infrastructure
    Destroy {
        /// Template reference (namespace/name)
        template: String,

        /// Kubernetes namespace (if not in template ref)
        #[arg(long, short)]
        namespace: Option<String>,

        /// Skip confirmation prompt
        #[arg(long)]
        yes: bool,
    },

    /// Suspend reconciliation
    Suspend {
        /// Template reference (namespace/name)
        template: String,

        /// Kubernetes namespace (if not in template ref)
        #[arg(long, short)]
        namespace: Option<String>,
    },

    /// Resume reconciliation
    Resume {
        /// Template reference (namespace/name)
        template: String,

        /// Kubernetes namespace (if not in template ref)
        #[arg(long, short)]
        namespace: Option<String>,
    },

    /// Force unlock state
    Unlock {
        /// Template reference (namespace/name)
        template: String,

        /// Kubernetes namespace (if not in template ref)
        #[arg(long, short)]
        namespace: Option<String>,
    },
}

/// Execute a template command.
pub async fn execute(
    command: TemplateCommands,
    client: &PangeaClient,
    format: OutputFormat,
) -> Result<()> {
    match command {
        TemplateCommands::List { namespace, phase } => {
            let phase = phase.map(|p| parse_phase(&p)).transpose()?;
            let templates = client.list_templates(namespace.as_deref(), phase).await?;

            if templates.is_empty() {
                println!("No templates found");
            } else {
                output::print_templates(&templates, format)?;
            }
        }

        TemplateCommands::Get {
            template,
            namespace,
        } => {
            let (ns, name) = parse_template_ref(&template, namespace.as_deref())?;
            let template = client.get_template(&ns, &name).await?;

            match template {
                Some(t) => output::print_template_detail(&t, format)?,
                None => {
                    eprintln!("Template not found: {}/{}", ns, name);
                    std::process::exit(1);
                }
            }
        }

        TemplateCommands::Apply {
            template,
            namespace,
        } => {
            apply(client, &template, namespace.as_deref(), format).await?;
        }

        TemplateCommands::Approve {
            template,
            namespace,
        } => {
            approve(client, &template, namespace.as_deref(), format).await?;
        }

        TemplateCommands::Destroy {
            template,
            namespace,
            yes,
        } => {
            let (ns, name) = parse_template_ref(&template, namespace.as_deref())?;

            if !yes {
                eprintln!(
                    "{}",
                    format!(
                        "WARNING: This will destroy all infrastructure managed by {}/{}",
                        ns, name
                    )
                    .red()
                    .bold()
                );
                eprintln!("Use --yes to confirm");
                std::process::exit(1);
            }

            let result = client.destroy(&ns, &name).await?;
            println!(
                "{}",
                format!("Triggered destroy for {}/{}", ns, name).yellow()
            );
            output::print_template_detail(&result, format)?;
        }

        TemplateCommands::Suspend {
            template,
            namespace,
        } => {
            let (ns, name) = parse_template_ref(&template, namespace.as_deref())?;
            let result = client.suspend(&ns, &name, true).await?;
            println!("{}", format!("Suspended {}/{}", ns, name).yellow());
            output::print_template_detail(&result, format)?;
        }

        TemplateCommands::Resume {
            template,
            namespace,
        } => {
            let (ns, name) = parse_template_ref(&template, namespace.as_deref())?;
            let result = client.suspend(&ns, &name, false).await?;
            println!("{}", format!("Resumed {}/{}", ns, name).green());
            output::print_template_detail(&result, format)?;
        }

        TemplateCommands::Unlock {
            template,
            namespace,
        } => {
            let (ns, name) = parse_template_ref(&template, namespace.as_deref())?;
            let success = client.unlock(&ns, &name).await?;
            if success {
                println!("{}", format!("Unlocked state for {}/{}", ns, name).green());
            } else {
                eprintln!("Failed to unlock state");
                std::process::exit(1);
            }
        }
    }

    Ok(())
}

/// Apply changes to a template.
pub async fn apply(
    client: &PangeaClient,
    template: &str,
    namespace: Option<&str>,
    format: OutputFormat,
) -> Result<()> {
    let (ns, name) = parse_template_ref(template, namespace)?;
    let result = client.apply(&ns, &name).await?;
    println!("{}", format!("Triggered apply for {}/{}", ns, name).green());
    output::print_template_detail(&result, format)?;
    Ok(())
}

/// Approve pending changes for a template.
pub async fn approve(
    client: &PangeaClient,
    template: &str,
    namespace: Option<&str>,
    format: OutputFormat,
) -> Result<()> {
    let (ns, name) = parse_template_ref(template, namespace)?;
    let result = client.approve(&ns, &name).await?;
    println!(
        "{}",
        format!("Approved changes for {}/{}", ns, name).green()
    );
    output::print_template_detail(&result, format)?;
    Ok(())
}

fn parse_phase(s: &str) -> Result<Phase> {
    match s.to_lowercase().as_str() {
        "pending" => Ok(Phase::Pending),
        "compiling" => Ok(Phase::Compiling),
        "initializing" => Ok(Phase::Initializing),
        "planning" => Ok(Phase::Planning),
        "applying" => Ok(Phase::Applying),
        "ready" => Ok(Phase::Ready),
        "drifted" => Ok(Phase::Drifted),
        "failed" => Ok(Phase::Failed),
        "destroying" => Ok(Phase::Destroying),
        _ => anyhow::bail!("Invalid phase: {}", s),
    }
}
