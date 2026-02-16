//! Plan command implementation.

use anyhow::Result;

use crate::client::{parse_template_ref, PangeaClient};
use crate::output::{self, OutputFormat};

/// Show plan for a template.
pub async fn show_plan(
    client: &PangeaClient,
    template: &str,
    namespace: Option<&str>,
    format: OutputFormat,
) -> Result<()> {
    let (ns, name) = parse_template_ref(template, namespace)?;
    let plan = client.get_plan(&ns, &name).await?;

    match plan {
        Some(p) => output::print_plan(&p, format)?,
        None => {
            eprintln!("No plan available for {}/{}. Template may not exist or has no pending changes.", ns, name);
            std::process::exit(1);
        }
    }

    Ok(())
}
