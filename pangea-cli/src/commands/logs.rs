//! Log streaming command.

use anyhow::Result;
use colored::Colorize;

use crate::client::{parse_template_ref, PangeaClient};

/// Stream logs for a template.
pub async fn stream_logs(
    client: &PangeaClient,
    template: &str,
    namespace: Option<&str>,
    follow: bool,
) -> Result<()> {
    let (ns, name) = parse_template_ref(template, namespace)?;

    println!(
        "{}",
        format!("Streaming logs for {}/{}...", ns, name).dimmed()
    );
    println!(
        "{}",
        "(Log streaming via WebSocket subscription - press Ctrl+C to stop)".dimmed()
    );
    println!();

    // TODO: Implement WebSocket subscription for real-time logs
    // For now, this is a placeholder that shows how it would work
    if follow {
        println!(
            "{}",
            "WebSocket subscriptions not yet implemented in CLI.".yellow()
        );
        println!(
            "{}",
            "Use the web UI at pangea.quero.local for real-time log streaming.".dimmed()
        );
    } else {
        // Get template to show recent status
        let template = client.get_template(&ns, &name).await?;
        match template {
            Some(t) => {
                if let Some(error) = t.last_error {
                    println!("{} {}", "[ERROR]".red(), error);
                }
                if let Some(summary) = t.plan_summary {
                    println!("{} Plan: {}", "[INFO]".blue(), summary);
                }
                println!(
                    "{} Current phase: {}",
                    "[INFO]".blue(),
                    format!("{}", t.phase)
                );
            }
            None => {
                eprintln!("Template not found: {}/{}", ns, name);
                std::process::exit(1);
            }
        }
    }

    Ok(())
}
