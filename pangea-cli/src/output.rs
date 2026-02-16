//! Output formatting for CLI commands.

use anyhow::Result;
use colored::Colorize;
use serde::Serialize;
use std::fmt::Display;
use tabled::{settings::Style, Table, Tabled};

use crate::schema::{InfrastructureTemplate, PangeaNamespace, Phase, PlanResult};

/// Output format for CLI commands.
#[derive(Debug, Clone, Copy, clap::ValueEnum)]
pub enum OutputFormat {
    /// Table format (default).
    Table,
    /// JSON format.
    Json,
    /// YAML format.
    Yaml,
}

impl Default for OutputFormat {
    fn default() -> Self {
        OutputFormat::Table
    }
}

/// Print data in the specified format.
pub fn print<T: Serialize + Tabled>(data: &[T], format: OutputFormat) -> Result<()> {
    match format {
        OutputFormat::Table => {
            let table = Table::new(data).with(Style::rounded()).to_string();
            println!("{}", table);
        }
        OutputFormat::Json => {
            println!("{}", serde_json::to_string_pretty(data)?);
        }
        OutputFormat::Yaml => {
            println!("{}", serde_yaml::to_string(data)?);
        }
    }
    Ok(())
}

/// Print a single item in the specified format.
pub fn print_one<T: Serialize + Display>(item: &T, format: OutputFormat) -> Result<()> {
    match format {
        OutputFormat::Table => {
            println!("{}", item);
        }
        OutputFormat::Json => {
            println!("{}", serde_json::to_string_pretty(item)?);
        }
        OutputFormat::Yaml => {
            println!("{}", serde_yaml::to_string(item)?);
        }
    }
    Ok(())
}

/// Row for template list table.
#[derive(Tabled)]
pub struct TemplateRow {
    #[tabled(rename = "NAMESPACE")]
    pub namespace: String,
    #[tabled(rename = "NAME")]
    pub name: String,
    #[tabled(rename = "PHASE")]
    pub phase: String,
    #[tabled(rename = "PANGEA NS")]
    pub pangea_namespace: String,
    #[tabled(rename = "RESOURCES")]
    pub resources: String,
    #[tabled(rename = "SOURCE")]
    pub source: String,
}

impl From<&InfrastructureTemplate> for TemplateRow {
    fn from(t: &InfrastructureTemplate) -> Self {
        let phase_colored = match t.phase {
            Phase::Ready => "Ready".green().to_string(),
            Phase::Failed => "Failed".red().to_string(),
            Phase::Drifted => "Drifted".yellow().to_string(),
            Phase::Applying | Phase::Planning | Phase::Compiling | Phase::Initializing => {
                format!("{}", t.phase).cyan().to_string()
            }
            Phase::Pending => "Pending".dimmed().to_string(),
            Phase::Destroying => "Destroying".red().to_string(),
        };

        Self {
            namespace: t.namespace.clone().unwrap_or_default(),
            name: t.name.clone().unwrap_or_default(),
            phase: phase_colored,
            pangea_namespace: t.pangea_namespace.clone(),
            resources: format!(
                "{} (+{} ~{} -{})",
                t.resource_counts.total,
                t.resource_counts.added,
                t.resource_counts.changed,
                t.resource_counts.destroyed
            ),
            source: t.source.source_type.clone(),
        }
    }
}

/// Print templates as table.
pub fn print_templates(templates: &[InfrastructureTemplate], format: OutputFormat) -> Result<()> {
    match format {
        OutputFormat::Table => {
            let rows: Vec<TemplateRow> = templates.iter().map(TemplateRow::from).collect();
            let table = Table::new(&rows).with(Style::rounded()).to_string();
            println!("{}", table);
        }
        OutputFormat::Json => {
            println!("{}", serde_json::to_string_pretty(templates)?);
        }
        OutputFormat::Yaml => {
            println!("{}", serde_yaml::to_string(templates)?);
        }
    }
    Ok(())
}

/// Row for namespace list table.
#[derive(Tabled)]
pub struct NamespaceRow {
    #[tabled(rename = "NAME")]
    pub name: String,
    #[tabled(rename = "BACKEND")]
    pub backend: String,
    #[tabled(rename = "DATABASE")]
    pub database: String,
    #[tabled(rename = "READY")]
    pub ready: String,
    #[tabled(rename = "TEMPLATES")]
    pub templates: String,
}

impl From<&PangeaNamespace> for NamespaceRow {
    fn from(ns: &PangeaNamespace) -> Self {
        let ready_colored = if ns.is_ready {
            "Yes".green().to_string()
        } else {
            "No".red().to_string()
        };

        Self {
            name: ns.name.clone().unwrap_or_default(),
            backend: ns.backend_type.clone(),
            database: format!(
                "{}/{}",
                ns.database_host.clone().unwrap_or_default(),
                ns.database_name.clone().unwrap_or_default()
            ),
            ready: ready_colored,
            templates: ns.template_count.to_string(),
        }
    }
}

/// Print namespaces as table.
pub fn print_namespaces(namespaces: &[PangeaNamespace], format: OutputFormat) -> Result<()> {
    match format {
        OutputFormat::Table => {
            let rows: Vec<NamespaceRow> = namespaces.iter().map(NamespaceRow::from).collect();
            let table = Table::new(&rows).with(Style::rounded()).to_string();
            println!("{}", table);
        }
        OutputFormat::Json => {
            println!("{}", serde_json::to_string_pretty(namespaces)?);
        }
        OutputFormat::Yaml => {
            println!("{}", serde_yaml::to_string(namespaces)?);
        }
    }
    Ok(())
}

/// Print plan result.
pub fn print_plan(plan: &PlanResult, format: OutputFormat) -> Result<()> {
    match format {
        OutputFormat::Table => {
            if plan.has_changes {
                println!(
                    "\n{}\n",
                    "Plan: Changes detected".yellow().bold()
                );
                println!(
                    "  {} {} to add",
                    format!("+{}", plan.added).green(),
                    "resource(s)"
                );
                println!(
                    "  {} {} to change",
                    format!("~{}", plan.changed).yellow(),
                    "resource(s)"
                );
                println!(
                    "  {} {} to destroy",
                    format!("-{}", plan.destroyed).red(),
                    "resource(s)"
                );
                println!("\nSummary: {}", plan.summary);
            } else {
                println!(
                    "\n{}\n",
                    "No changes. Infrastructure is up-to-date.".green()
                );
            }
        }
        OutputFormat::Json => {
            println!("{}", serde_json::to_string_pretty(plan)?);
        }
        OutputFormat::Yaml => {
            println!("{}", serde_yaml::to_string(plan)?);
        }
    }
    Ok(())
}

/// Print a template detail.
pub fn print_template_detail(template: &InfrastructureTemplate, format: OutputFormat) -> Result<()> {
    match format {
        OutputFormat::Table => {
            println!("\n{}", "Template Details".bold());
            println!("  Namespace:       {}", template.namespace.clone().unwrap_or_default());
            println!("  Name:            {}", template.name.clone().unwrap_or_default());
            println!("  Phase:           {}", format_phase(template.phase));
            println!("  Pangea NS:       {}", template.pangea_namespace);
            println!("  Auto Approve:    {}", template.auto_approve);
            println!("  Suspended:       {}", template.suspended);
            println!();
            println!("{}", "Source".bold());
            println!("  Type:            {}", template.source.source_type);
            println!("  Reference:       {}", template.source.reference);
            println!();
            println!("{}", "Resources".bold());
            println!("  Total:           {}", template.resource_counts.total);
            println!("  Added:           {}", template.resource_counts.added);
            println!("  Changed:         {}", template.resource_counts.changed);
            println!("  Destroyed:       {}", template.resource_counts.destroyed);

            if let Some(ref summary) = template.plan_summary {
                println!();
                println!("{}", "Plan Summary".bold());
                println!("  {}", summary);
            }

            if let Some(ref error) = template.last_error {
                println!();
                println!("{}", "Last Error".red().bold());
                println!("  {}", error);
            }
        }
        OutputFormat::Json => {
            println!("{}", serde_json::to_string_pretty(template)?);
        }
        OutputFormat::Yaml => {
            println!("{}", serde_yaml::to_string(template)?);
        }
    }
    Ok(())
}

fn format_phase(phase: Phase) -> String {
    match phase {
        Phase::Ready => "Ready".green().to_string(),
        Phase::Failed => "Failed".red().to_string(),
        Phase::Drifted => "Drifted".yellow().to_string(),
        Phase::Applying | Phase::Planning | Phase::Compiling | Phase::Initializing => {
            format!("{}", phase).cyan().to_string()
        }
        Phase::Pending => "Pending".dimmed().to_string(),
        Phase::Destroying => "Destroying".red().to_string(),
    }
}
