use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use clap::{Args, Subcommand};
use pangea_leaf::plan::{plan_migration, MigrationPlan, PlannerInput, Verdict};
use pangea_leaf::sources::{addresses_from_lines, addresses_from_state, plan_view};
use serde_json::{json, Value};

#[derive(Subcommand)]
pub enum LeafCommands {
    Plan(PlanArgs),
    Migrate(MigrateArgs),
}

#[derive(Args)]
pub struct MigrateArgs {
    #[arg(long)]
    fat_template: String,
    #[arg(long)]
    state: PathBuf,
    #[arg(long, required_unless_present = "reverse")]
    moves: Option<PathBuf>,
    #[arg(long)]
    emit_dir: PathBuf,
    #[arg(long, requires = "receipt")]
    reverse: bool,
    #[arg(long)]
    receipt: Option<PathBuf>,
}

fn migrate(a: &MigrateArgs) -> Result<()> {
    let fat = std::fs::read(&a.state).with_context(|| format!("reading {}", a.state.display()))?;
    std::fs::create_dir_all(&a.emit_dir)?;
    if a.reverse {
        let receipt_path = a.receipt.as_ref().context("--reverse needs --receipt")?;
        let receipt: pangea_leaf::migrate::Receipt =
            serde_json::from_slice(&std::fs::read(receipt_path)?)?;
        let mut leaves = Vec::new();
        for l in &receipt.leaves {
            let p = a.emit_dir.join(format!("{}.tfstate.json", l.template));
            leaves.push(pangea_leaf::migrate::LeafState {
                template: l.template.clone(),
                bytes: std::fs::read(&p).with_context(|| format!("reading {}", p.display()))?,
            });
        }
        let merged = pangea_leaf::migrate::merge(&fat, &receipt, &leaves)?;
        let out = a
            .emit_dir
            .join(format!("{}.merged.tfstate.json", a.fat_template));
        std::fs::write(&out, merged)?;
        println!("{}", out.display());
        return Ok(());
    }
    let moves_path = a.moves.as_ref().context("--moves is required")?;
    let moves: Vec<pangea_leaf::plan::StateMove> =
        serde_json::from_slice(&std::fs::read(moves_path)?)?;
    let split = match pangea_leaf::migrate::split(&a.fat_template, &fat, &moves) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("refused: {e}");
            std::process::exit(2);
        }
    };
    for l in &split.leaves {
        std::fs::write(
            a.emit_dir.join(format!("{}.tfstate.json", l.template)),
            &l.bytes,
        )?;
    }
    let receipt = serde_json::to_string_pretty(&split.receipt)?;
    std::fs::write(a.emit_dir.join("receipt.json"), &receipt)?;
    println!("{receipt}");
    Ok(())
}

#[derive(Args)]
pub struct PlanArgs {
    #[arg(long)]
    template: PathBuf,
    #[arg(
        long,
        conflicts_with = "addresses",
        required_unless_present = "addresses"
    )]
    state: Option<PathBuf>,
    #[arg(long)]
    addresses: Option<PathBuf>,
    #[arg(long)]
    plan: Option<PathBuf>,
    #[arg(long)]
    cycle_p95_ms: Option<u64>,
    #[arg(long)]
    budget_ms: Option<u64>,
    #[arg(long)]
    group_references: bool,
    #[arg(long)]
    max_concurrent_leaves: Option<u32>,
    #[arg(long)]
    emit_dir: Option<PathBuf>,
    #[arg(long)]
    summary: bool,
}

fn read_value(path: &Path) -> Result<Value> {
    let text =
        std::fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
    serde_yaml::from_str(&text).with_context(|| format!("parsing {}", path.display()))
}

fn summary(plan: &MigrationPlan) -> Value {
    let mut sizes: BTreeMap<String, usize> = BTreeMap::new();
    for (size, count) in &plan.partition_sizes {
        sizes.insert(size.to_string(), *count);
    }
    json!({
        "fatTemplate": plan.fat_template,
        "verdict": plan.verdict,
        "classification": plan.classification,
        "checks": plan.checks,
        "bijection": plan.bijection,
        "projected": plan.projected,
        "leafCount": plan.leaf_count,
        "partitionSizes": sizes,
    })
}

fn write_yaml(path: &Path, v: &Value) -> Result<()> {
    std::fs::write(path, serde_yaml::to_string(v)?)
        .with_context(|| format!("writing {}", path.display()))
}

fn emit(dir: &Path, plan: &MigrationPlan) -> Result<()> {
    std::fs::create_dir_all(dir.join("leaves"))?;
    std::fs::write(
        dir.join("plan.json"),
        serde_json::to_string_pretty(&summary(plan))?,
    )?;
    std::fs::write(
        dir.join("state-moves.json"),
        serde_json::to_string_pretty(&plan.state_moves)?,
    )?;
    if let Some(c) = &plan.catalog {
        write_yaml(&dir.join("catalog.yaml"), c)?;
    }
    if let Some(p) = &plan.fat_patch {
        write_yaml(&dir.join("fat-patch.yaml"), p)?;
    }
    for m in &plan.leaf_templates {
        let name = m
            .pointer("/metadata/name")
            .and_then(Value::as_str)
            .context("leaf manifest without a name")?;
        write_yaml(&dir.join("leaves").join(format!("{name}.yaml")), m)?;
    }
    Ok(())
}

pub fn execute(command: LeafCommands) -> Result<()> {
    let a = match command {
        LeafCommands::Plan(a) => a,
        LeafCommands::Migrate(m) => return migrate(&m),
    };
    let template = read_value(&a.template)?;
    let addresses = match (&a.state, &a.addresses) {
        (Some(p), None) => addresses_from_state(&read_value(p)?)?,
        (None, Some(p)) => addresses_from_lines(&std::fs::read_to_string(p)?),
        _ => bail!("exactly one of --state and --addresses is required"),
    };
    let view = match &a.plan {
        Some(p) => Some(plan_view(&read_value(p)?)?),
        None => None,
    };
    let plan = plan_migration(&PlannerInput {
        template: &template,
        addresses: &addresses,
        plan: view.as_ref(),
        cycle_p95_ms: a.cycle_p95_ms,
        budget_ms: a.budget_ms,
        group_references: a.group_references,
        max_concurrent_leaves: a.max_concurrent_leaves,
    })?;
    if let Some(dir) = &a.emit_dir {
        emit(dir, &plan)?;
    }
    if a.summary {
        println!("{}", serde_json::to_string_pretty(&summary(&plan))?);
    } else {
        println!("{}", serde_json::to_string_pretty(&plan)?);
    }
    if plan.verdict == Verdict::Refused {
        std::process::exit(2);
    }
    Ok(())
}
