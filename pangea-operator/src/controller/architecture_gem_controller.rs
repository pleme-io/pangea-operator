//! ArchitectureGem reconciler — M1 of
//! `theory/PANGEA-WORKSPACE-RECONCILIATION.md`.
//!
//! For each ArchitectureGem CR:
//!   1. Phase=Loading: query compiler sidecar
//!      `GET /v1/architectures` for the gem's loaded class list.
//!   2. Compute missing = expected - loaded. If missing != [], set
//!      Phase=Failed with a typed condition naming the missing classes.
//!   3. Phase=SmokeTesting: for each fixture, POST
//!      /v1/architectures/smoke-test. Aggregate results.
//!   4. Phase=Loaded if every expected class loaded AND every fixture
//!      passed.
//!
//! No retry-loops on LoadError — every failure surfaces as a typed
//! condition the operator-human can read with `kubectl get archgem`.

use chrono::Utc;
use kube::{
    api::{Api, Patch, PatchParams},
    runtime::controller::{Action, Controller},
    Client,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;
use tracing::{error, info, warn};

use crate::crd::architecture_gem::{
    ArchitectureGem, ArchitectureGemStatus, Condition, FixtureResult, Phase, SmokeStatus,
};
use futures::StreamExt;

/// Wire ArchitectureGem reconciliation into the operator's runtime.
///
/// Returns a future that owns the controller's run loop; spawn
/// alongside the existing controllers in `main.rs`.
pub fn run(client: Client, compiler_endpoint: String) -> impl std::future::Future<Output = ()> {
    let api: Api<ArchitectureGem> = Api::all(client.clone());
    let context = Arc::new(Context {
        client,
        compiler_endpoint,
        http: reqwest::Client::builder()
            .timeout(Duration::from_secs(30))
            .build()
            .expect("reqwest client builds"),
    });

    Controller::new(api, kube::runtime::watcher::Config::default())
        .run(reconcile, error_policy, context)
        .for_each(|res| async move {
            match res {
                Ok((obj, _)) => info!("ArchitectureGem reconciled: {:?}", obj.name),
                Err(e) => error!("ArchitectureGem reconcile error: {:?}", e),
            }
        })
}

struct Context {
    client: Client,
    compiler_endpoint: String,
    http: reqwest::Client,
}

#[derive(Debug, thiserror::Error)]
enum Error {
    #[error("kube API error: {0}")]
    Kube(#[from] kube::Error),
    #[error("compiler RPC error: {0}")]
    Rpc(#[from] reqwest::Error),
    #[error("missing metadata.name on ArchitectureGem")]
    MissingName,
}

async fn reconcile(gem: Arc<ArchitectureGem>, ctx: Arc<Context>) -> Result<Action, Error> {
    let name = gem.metadata.name.clone().ok_or(Error::MissingName)?;

    // Suspended? Skip the whole loop; preserve last known status.
    if gem.spec.suspend {
        info!(gem = %name, "ArchitectureGem suspended; skipping reconcile");
        return Ok(Action::requeue(Duration::from_secs(300)));
    }

    info!(
        gem = %name,
        version = %gem.spec.version,
        "reconciling ArchitectureGem"
    );

    // Phase 1 — Loading: ask compiler what's available.
    let listing = match query_compiler_listing(&ctx, &gem.spec.gem_name).await {
        Ok(l) => l,
        Err(e) => {
            warn!(gem = %name, error = %e, "compiler listing RPC failed");
            patch_status_failed(
                &ctx.client,
                &name,
                "CompilerUnreachable",
                &format!("compiler listing RPC failed: {}", e),
            )
            .await?;
            return Ok(Action::requeue(parse_interval(&gem.spec.refresh_interval)));
        }
    };

    let expected: Vec<String> = gem.spec.expected_classes.clone();
    let missing: Vec<String> = expected
        .iter()
        .filter(|c| !listing.classes.contains(c))
        .cloned()
        .collect();

    if !missing.is_empty() {
        warn!(
            gem = %name,
            missing = ?missing,
            loaded = ?listing.classes,
            "ArchitectureGem has missing expected classes"
        );
        let status = ArchitectureGemStatus {
            phase: Some(Phase::Failed),
            smoke_status: Some(SmokeStatus::NotRun),
            loaded_classes: listing.classes.clone(),
            loaded_class_count: listing.classes.len() as u32,
            missing_classes: missing.clone(),
            fixture_results: vec![],
            last_reconcile_time: Some(Utc::now()),
            observed_version: listing.version.clone(),
            conditions: vec![Condition {
                condition_type: "Loaded".to_string(),
                status: "False".to_string(),
                reason: "ExpectedClassesMissing".to_string(),
                message: format!(
                    "{} expected class(es) not loaded by the compiler: {}",
                    missing.len(),
                    missing.join(", ")
                ),
                last_transition_time: Utc::now(),
            }],
        };
        patch_status(&ctx.client, &name, &status).await?;
        return Ok(Action::requeue(parse_interval(&gem.spec.refresh_interval)));
    }

    // Phase 2 — SmokeTesting: run each fixture.
    let mut fixture_results: Vec<FixtureResult> = Vec::with_capacity(gem.spec.fixtures.len());
    let mut all_passed = true;
    for fixture in &gem.spec.fixtures {
        match run_smoke_fixture(
            &ctx,
            &gem.spec.gem_name,
            &fixture.class_name,
            &fixture.fixture_path,
        )
        .await
        {
            Ok(result) => {
                if !result.passed {
                    all_passed = false;
                }
                fixture_results.push(result);
            }
            Err(e) => {
                all_passed = false;
                fixture_results.push(FixtureResult {
                    class_name: fixture.class_name.clone(),
                    passed: false,
                    last_run: Utc::now(),
                    error: Some(format!("RPC error: {}", e)),
                    input_hash: None,
                });
            }
        }
    }

    let phase = if all_passed {
        Phase::Loaded
    } else {
        Phase::Failed
    };
    let smoke_status = if gem.spec.fixtures.is_empty() {
        SmokeStatus::NotRun
    } else if all_passed {
        SmokeStatus::Passed
    } else {
        SmokeStatus::Failed
    };

    let status = ArchitectureGemStatus {
        phase: Some(phase.clone()),
        smoke_status: Some(smoke_status),
        loaded_classes: listing.classes.clone(),
        loaded_class_count: listing.classes.len() as u32,
        missing_classes: vec![],
        fixture_results,
        last_reconcile_time: Some(Utc::now()),
        observed_version: listing.version,
        conditions: vec![
            Condition {
                condition_type: "Loaded".to_string(),
                status: "True".to_string(),
                reason: "ExpectedClassesLoaded".to_string(),
                message: format!(
                    "all {} expected class(es) loaded",
                    gem.spec.expected_classes.len()
                ),
                last_transition_time: Utc::now(),
            },
            Condition {
                condition_type: "SmokeTested".to_string(),
                status: if all_passed { "True" } else { "False" }.to_string(),
                reason: if all_passed {
                    "AllFixturesPassed"
                } else {
                    "FixtureFailures"
                }
                .to_string(),
                message: if all_passed {
                    format!("{} fixtures passed", gem.spec.fixtures.len())
                } else {
                    "one or more smoke-test fixtures failed; see status.fixtureResults".to_string()
                },
                last_transition_time: Utc::now(),
            },
            Condition {
                condition_type: "Ready".to_string(),
                status: if matches!(phase, Phase::Loaded) {
                    "True"
                } else {
                    "False"
                }
                .to_string(),
                reason: if matches!(phase, Phase::Loaded) {
                    "Loaded"
                } else {
                    "FixtureFailures"
                }
                .to_string(),
                message: format!("phase: {}", phase),
                last_transition_time: Utc::now(),
            },
        ],
    };
    patch_status(&ctx.client, &name, &status).await?;

    info!(
        gem = %name,
        phase = ?phase,
        loaded = listing.classes.len(),
        "ArchitectureGem reconcile complete"
    );

    Ok(Action::requeue(parse_interval(&gem.spec.refresh_interval)))
}

#[derive(Deserialize)]
struct CompilerListing {
    classes: Vec<String>,
    #[serde(default)]
    version: Option<String>,
}

async fn query_compiler_listing(ctx: &Context, gem_name: &str) -> Result<CompilerListing, Error> {
    let url = format!("{}/v1/architectures?gem={}", ctx.compiler_endpoint, gem_name);
    let resp = ctx.http.get(&url).send().await?.error_for_status()?;
    let listing: CompilerListing = resp.json().await?;
    Ok(listing)
}

#[derive(Serialize)]
struct SmokeRequest<'a> {
    gem: &'a str,
    class_name: &'a str,
    fixture_path: &'a str,
}

#[derive(Deserialize)]
struct SmokeResponse {
    passed: bool,
    #[serde(default)]
    error: Option<String>,
    #[serde(default)]
    input_hash: Option<String>,
}

async fn run_smoke_fixture(
    ctx: &Context,
    gem_name: &str,
    class_name: &str,
    fixture_path: &str,
) -> Result<FixtureResult, Error> {
    let url = format!("{}/v1/architectures/smoke-test", ctx.compiler_endpoint);
    let body = SmokeRequest {
        gem: gem_name,
        class_name,
        fixture_path,
    };
    let resp = ctx
        .http
        .post(&url)
        .json(&body)
        .send()
        .await?
        .error_for_status()?;
    let parsed: SmokeResponse = resp.json().await?;
    Ok(FixtureResult {
        class_name: class_name.to_string(),
        passed: parsed.passed,
        last_run: Utc::now(),
        error: parsed.error,
        input_hash: parsed.input_hash,
    })
}

async fn patch_status(
    client: &Client,
    name: &str,
    status: &ArchitectureGemStatus,
) -> Result<(), Error> {
    let api: Api<ArchitectureGem> = Api::all(client.clone());
    let pp = PatchParams::default();
    let patch = serde_json::json!({ "status": status });
    api.patch_status(name, &pp, &Patch::Merge(&patch)).await?;
    Ok(())
}

async fn patch_status_failed(
    client: &Client,
    name: &str,
    reason: &str,
    message: &str,
) -> Result<(), Error> {
    let status = ArchitectureGemStatus {
        phase: Some(Phase::Failed),
        smoke_status: Some(SmokeStatus::NotRun),
        loaded_classes: vec![],
        loaded_class_count: 0,
        missing_classes: vec![],
        fixture_results: vec![],
        last_reconcile_time: Some(Utc::now()),
        observed_version: None,
        conditions: vec![Condition {
            condition_type: "Loaded".to_string(),
            status: "False".to_string(),
            reason: reason.to_string(),
            message: message.to_string(),
            last_transition_time: Utc::now(),
        }],
    };
    patch_status(client, name, &status).await
}

fn error_policy(_obj: Arc<ArchitectureGem>, err: &Error, _ctx: Arc<Context>) -> Action {
    error!("ArchitectureGem error policy: {}", err);
    Action::requeue(Duration::from_secs(60))
}

fn parse_interval(s: &str) -> Duration {
    // Tiny duration parser: `10s`, `5m`, `1h`. Falls back to 5min.
    let trimmed = s.trim();
    if let Some(num) = trimmed.strip_suffix('s') {
        if let Ok(n) = num.parse::<u64>() {
            return Duration::from_secs(n);
        }
    }
    if let Some(num) = trimmed.strip_suffix('m') {
        if let Ok(n) = num.parse::<u64>() {
            return Duration::from_secs(n * 60);
        }
    }
    if let Some(num) = trimmed.strip_suffix('h') {
        if let Ok(n) = num.parse::<u64>() {
            return Duration::from_secs(n * 3600);
        }
    }
    Duration::from_secs(300)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_interval_seconds() {
        assert_eq!(parse_interval("30s"), Duration::from_secs(30));
    }

    #[test]
    fn parse_interval_minutes() {
        assert_eq!(parse_interval("5m"), Duration::from_secs(300));
    }

    #[test]
    fn parse_interval_hours() {
        assert_eq!(parse_interval("2h"), Duration::from_secs(7200));
    }

    #[test]
    fn parse_interval_invalid_falls_back_to_5m() {
        assert_eq!(parse_interval("garbage"), Duration::from_secs(300));
        assert_eq!(parse_interval(""), Duration::from_secs(300));
        assert_eq!(parse_interval("10x"), Duration::from_secs(300));
    }

    #[test]
    fn parse_interval_with_whitespace() {
        assert_eq!(parse_interval("  5m  "), Duration::from_secs(300));
    }
}
