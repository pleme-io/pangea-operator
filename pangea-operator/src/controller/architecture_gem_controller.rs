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
use std::sync::Arc;
use std::time::Duration;
use tracing::{error, info, warn};

use crate::crd::architecture_gem::{
    ArchitectureGem, ArchitectureGemStatus, Condition, FixtureResult, Phase, SmokeStatus,
};
use crate::ruby::{
    BackendError, CompilerBackend, HttpCompilerBackend, SmokeRequest,
};
use futures::StreamExt;

/// Wire ArchitectureGem reconciliation into the operator's runtime.
///
/// Returns a future that owns the controller's run loop; spawn
/// alongside the existing controllers in `main.rs`.
///
/// `backend` is dyn so callers can choose HTTP-to-sidecar or embedded
/// magnus depending on `PANGEA_COMPILER_BACKEND` / helm flag (see M8.2
/// design in `theory/PANGEA-WORKSPACE-RECONCILIATION.md`).
pub fn run(
    client: Client,
    backend: Arc<dyn CompilerBackend>,
) -> impl std::future::Future<Output = ()> {
    let api: Api<ArchitectureGem> = Api::all(client.clone());
    let context = Arc::new(Context { client, backend });

    Controller::new(api, kube::runtime::watcher::Config::default())
        .run(reconcile, error_policy, context)
        .for_each(|res| async move {
            match res {
                Ok((obj, _)) => info!("ArchitectureGem reconciled: {:?}", obj.name),
                Err(e) => error!("ArchitectureGem reconcile error: {:?}", e),
            }
        })
}

/// Convenience constructor for the default HTTP-to-sidecar backend.
/// Used by `main.rs` when `PANGEA_COMPILER_BACKEND` is unset/`http`.
pub fn http_backend(compiler_endpoint: String) -> Arc<dyn CompilerBackend> {
    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .expect("reqwest client builds");
    Arc::new(HttpCompilerBackend::new(http, compiler_endpoint))
}

struct Context {
    client: Client,
    backend: Arc<dyn CompilerBackend>,
}

#[derive(Debug, thiserror::Error)]
enum Error {
    #[error("kube API error: {0}")]
    Kube(#[from] kube::Error),
    #[error("compiler backend: {0}")]
    Backend(#[from] BackendError),
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

    // Phase 0 — Prepare: if the CR has a gitRepository source, ask
    // the backend to prepare it (HTTP no-op; embedded does
    // gem-cache.ensure + $LOAD_PATH prepend). Idempotent across
    // reconcile cycles. Failure here is "CompilerUnreachable"-shaped
    // because the gem isn't usable until prepare succeeds.
    if let Some(gr) = gem.spec.source.git_repository.as_ref() {
        // Translate the CRD-side SourceKind to the backend-side mirror.
        // Default Ruby preserves backward compat: any pre-M2 CR
        // without explicit kind continues to land on the magnus path.
        let kind = match gem.spec.source.kind {
            crate::crd::architecture_gem::SourceKind::Ruby => crate::ruby::SourceKind::Ruby,
            crate::crd::architecture_gem::SourceKind::Lisp => crate::ruby::SourceKind::Lisp,
            crate::crd::architecture_gem::SourceKind::Wasm => crate::ruby::SourceKind::Wasm,
        };
        let prep = ctx
            .backend
            .prepare_gem(&crate::ruby::GemSource {
                name: gem.spec.gem_name.clone(),
                git_url: gr.url.clone(),
                git_ref: gr.r#ref.clone(),
                kind,
            })
            .await;
        if let Err(e) = prep {
            warn!(gem = %name, error = %e, "gem prepare failed");
            patch_status_failed(
                &ctx.client,
                &name,
                "GemPrepareFailed",
                &format!("prepare_gem failed: {e}"),
            )
            .await?;
            return Ok(Action::requeue(parse_interval(&gem.spec.refresh_interval)));
        }
    }

    // Phase 1 — Loading: ask compiler what's available. Backend
    // dispatch is HTTP-to-sidecar or embedded-magnus depending on
    // operator config (M8.2).
    let listing = match ctx.backend.list_architectures(&gem.spec.gem_name).await {
        Ok(l) => l,
        Err(e) => {
            warn!(gem = %name, error = %e, "compiler listing failed");
            patch_status_failed(
                &ctx.client,
                &name,
                "CompilerUnreachable",
                &format!("compiler listing failed: {}", e),
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

    // Phase 2 — SmokeTesting: run each fixture via the backend.
    let mut fixture_results: Vec<FixtureResult> = Vec::with_capacity(gem.spec.fixtures.len());
    let mut all_passed = true;
    for fixture in &gem.spec.fixtures {
        let req = SmokeRequest {
            gem: gem.spec.gem_name.clone(),
            class_name: fixture.class_name.clone(),
            fixture_path: fixture.fixture_path.clone(),
        };
        match ctx.backend.smoke_test(req).await {
            Ok(outcome) => {
                if !outcome.passed {
                    all_passed = false;
                }
                fixture_results.push(FixtureResult {
                    class_name: fixture.class_name.clone(),
                    passed: outcome.passed,
                    last_run: Utc::now(),
                    error: outcome.error,
                    input_hash: outcome.input_hash,
                });
            }
            Err(e) => {
                all_passed = false;
                fixture_results.push(FixtureResult {
                    class_name: fixture.class_name.clone(),
                    passed: false,
                    last_run: Utc::now(),
                    error: Some(format!("backend error: {}", e)),
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
