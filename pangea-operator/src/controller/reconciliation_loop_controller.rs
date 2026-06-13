//! ReconciliationLoop (`roda`) reconciler — the loop-granularity axis of
//! `theory/RECONCILIATION-TOPOLOGY.md` §II.
//!
//! For each `ReconciliationLoop` CR this controller:
//!   1. Resolves `spec.selector.matchLabels` against every
//!      `InfrastructureTemplate` in the fleet → the loop's MEMBERSHIP.
//!   2. Writes `status.matchedWorkspaces` / `matchedCount` / `phase`
//!      (Active | Suspended | Empty) — the observability half of `roda`:
//!      operators declare a wheel by label, and see exactly which workspaces
//!      it turns.
//!   3. Requeues at `spec.cadence` — the wheel's own tick. One `roda` may
//!      bind 1 workspace or 100; the operator hosts many wheels concurrently.
//!
//! **First increment.** This delivers the loop CRD + membership resolution +
//! cadence tick + Ready/Members conditions. Two pieces are the NEXT increment
//! (explicitly deferred, not silently missing):
//!   - **cadence ownership** — members reconcile ON the loop's cadence,
//!     suppressing their own `refreshInterval` (template_controller consults
//!     its loop assignment). Today members keep their own interval; the loop
//!     observes + reports membership.
//!   - the **`malha`** one-resource-per-workspace axis (§III).
//!
//! Status-write discipline per pangea-operator/CLAUDE.md: a diff-gate skips
//! the patch when nothing observable changed, and `last_tick_at` (a
//! `Utc::now()` field) is EXCLUDED from the gate so it can't self-trigger the
//! watch hot-loop. Cluster-scoped (a wheel spans namespaces).

use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use futures::StreamExt;
use kube::{
    api::{Api, ListParams},
    runtime::controller::Action,
    Client,
};
use tracing::{debug, error, info, warn};

use crate::crd::reconciliation_loop::{LoopPhase, ReconciliationLoop, ReconciliationLoopStatus};
use crate::crd::{Condition, InfrastructureTemplate};

/// Wire ReconciliationLoop reconciliation into the operator runtime.
pub fn run(
    client: Client,
    metrics: Arc<crate::observability::Metrics>,
) -> impl std::future::Future<Output = ()> {
    let context = Arc::new(Context {
        client: client.clone(),
        metrics,
    });

    crate::controller::generation_filter::filtered_controller::<ReconciliationLoop>(client)
        .run(reconcile, error_policy, context)
        .for_each(|res| async move {
            match res {
                Ok((obj, _)) => debug!("ReconciliationLoop reconciled: {:?}", obj.name),
                Err(e) => error!("ReconciliationLoop reconcile error: {:?}", e),
            }
        })
}

struct Context {
    client: Client,
    metrics: Arc<crate::observability::Metrics>,
}

#[derive(Debug, thiserror::Error)]
enum Error {
    #[error("kube API error: {0}")]
    Kube(#[from] kube::Error),
    #[error("missing metadata.name on ReconciliationLoop")]
    MissingName,
}

async fn reconcile(roda: Arc<ReconciliationLoop>, ctx: Arc<Context>) -> Result<Action, Error> {
    let name = roda.metadata.name.clone().ok_or(Error::MissingName)?;
    ctx.metrics.record_reconcile_named("reconciliationloop", "ok");

    let cadence = parse_cadence(&roda.spec.cadence);
    let observed_generation = roda.metadata.generation.unwrap_or(0);

    if roda.spec.suspend {
        info!(loop_name = %name, "ReconciliationLoop suspended; not ticking");
        let status = ReconciliationLoopStatus {
            phase: Some(LoopPhase::Suspended),
            matched_workspaces: roda
                .status
                .as_ref()
                .map(|s| s.matched_workspaces.clone())
                .unwrap_or_default(),
            matched_count: roda.status.as_ref().map(|s| s.matched_count).unwrap_or(0),
            last_tick_at: roda.status.as_ref().and_then(|s| s.last_tick_at),
            observed_generation,
            conditions: vec![ready_condition("False", "Suspended", "spec.suspend = true")],
        };
        patch_status_if_changed(&ctx.client, &name, roda.status.as_ref(), status).await?;
        return Ok(Action::requeue(cadence));
    }

    // Resolve membership: every InfrastructureTemplate whose labels satisfy
    // the selector. Empty selector matches nothing (no accidental fleet loop).
    let tmpl_api: Api<InfrastructureTemplate> = Api::all(ctx.client.clone());
    let templates = tmpl_api.list(&ListParams::default()).await?;
    let mut matched_workspaces: Vec<String> = templates
        .items
        .iter()
        .filter(|t| {
            let labels = t.metadata.labels.clone().unwrap_or_default();
            roda.spec.selector.matches(&labels)
        })
        .map(|t| {
            let ns = t.metadata.namespace.as_deref().unwrap_or("default");
            let n = t.metadata.name.as_deref().unwrap_or("?");
            format!("{ns}/{n}")
        })
        .collect();
    matched_workspaces.sort();
    let matched_count = matched_workspaces.len() as u32;

    let phase = if matched_count == 0 {
        LoopPhase::Empty
    } else {
        LoopPhase::Active
    };
    let (ready_status, ready_reason, ready_msg) = match phase {
        LoopPhase::Empty => (
            "True",
            "EmptyButHealthy",
            format!("Loop active; selector matched 0 workspaces (cadence {})", roda.spec.cadence),
        ),
        _ => (
            "True",
            "Ticking",
            format!(
                "Loop driving {matched_count} workspace(s) at cadence {} (concurrency {})",
                roda.spec.cadence, roda.spec.concurrency
            ),
        ),
    };

    let status = ReconciliationLoopStatus {
        phase: Some(phase),
        matched_workspaces,
        matched_count,
        last_tick_at: Some(Utc::now()),
        observed_generation,
        conditions: vec![
            ready_condition(ready_status, ready_reason, &ready_msg),
            members_condition(matched_count),
        ],
    };

    patch_status_if_changed(&ctx.client, &name, roda.status.as_ref(), status).await?;

    info!(
        loop_name = %name,
        members = matched_count,
        cadence = %roda.spec.cadence,
        "ReconciliationLoop tick: membership resolved"
    );
    Ok(Action::requeue(cadence))
}

fn ready_condition(status: &str, reason: &str, message: &str) -> Condition {
    Condition {
        r#type: "Ready".to_string(),
        status: status.to_string(),
        reason: reason.to_string(),
        message: message.to_string(),
        last_transition_time: Utc::now(),
    }
}

fn members_condition(count: u32) -> Condition {
    Condition {
        r#type: "MembersResolved".to_string(),
        status: "True".to_string(),
        reason: "SelectorEvaluated".to_string(),
        message: format!("{count} workspace(s) bound to this loop"),
        last_transition_time: Utc::now(),
    }
}

/// Status diff-gate: patch only when observable content changed. EXCLUDES
/// `last_tick_at` (a `Utc::now()` field) — including it would make every
/// reconcile "differ" and self-trigger the watch hot-loop (the bug class the
/// CLAUDE.md status-loop rule defends against). Conditions compare by content
/// (type/status/reason/message), ignoring transition timestamps.
async fn patch_status_if_changed(
    client: &Client,
    name: &str,
    old: Option<&ReconciliationLoopStatus>,
    mut new_status: ReconciliationLoopStatus,
) -> Result<(), Error> {
    if let Some(prev) = old {
        new_status.conditions = crate::controller::status::merge_condition_transitions(
            &prev.conditions,
            new_status.conditions,
        );
        if status_content_equal(prev, &new_status) {
            debug!(loop_name = %name, "ReconciliationLoop status unchanged; skipping patch");
            return Ok(());
        }
    }
    crate::controller::status::patch_status::<ReconciliationLoop, _>(client, name, &new_status)
        .await?;
    Ok(())
}

/// Observable-content equality — `last_tick_at` deliberately excluded.
fn status_content_equal(a: &ReconciliationLoopStatus, b: &ReconciliationLoopStatus) -> bool {
    a.phase == b.phase
        && a.matched_count == b.matched_count
        && a.matched_workspaces == b.matched_workspaces
        && a.observed_generation == b.observed_generation
        && crate::controller::status::conditions_observably_equal(&a.conditions, &b.conditions)
}

fn error_policy(_obj: Arc<ReconciliationLoop>, err: &Error, ctx: Arc<Context>) -> Action {
    ctx.metrics.record_reconcile_named("reconciliationloop", "error");
    warn!(error = %err, "ReconciliationLoop reconcile error; requeue in 60s");
    Action::requeue(Duration::from_secs(60))
}

/// Parse a cadence string ("5m", "30s", "1h", "90") into a Duration. Bare
/// integers are seconds. Falls back to 300s on any parse failure (never panics
/// — a malformed cadence must not wedge the loop).
fn parse_cadence(s: &str) -> Duration {
    const FALLBACK: Duration = Duration::from_secs(300);
    let s = s.trim();
    if s.is_empty() {
        return FALLBACK;
    }
    let (num_part, mult) = match s.chars().last() {
        Some('s') => (&s[..s.len() - 1], 1u64),
        Some('m') => (&s[..s.len() - 1], 60),
        Some('h') => (&s[..s.len() - 1], 3600),
        Some(c) if c.is_ascii_digit() => (s, 1),
        _ => return FALLBACK,
    };
    match num_part.trim().parse::<u64>() {
        Ok(n) if n > 0 => Duration::from_secs(n.saturating_mul(mult)),
        _ => FALLBACK,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_cadence_units() {
        assert_eq!(parse_cadence("30s"), Duration::from_secs(30));
        assert_eq!(parse_cadence("5m"), Duration::from_secs(300));
        assert_eq!(parse_cadence("1h"), Duration::from_secs(3600));
        assert_eq!(parse_cadence("90"), Duration::from_secs(90));
    }

    #[test]
    fn parse_cadence_falls_back_never_panics() {
        // Malformed / zero / empty must not wedge the loop → 300s default.
        assert_eq!(parse_cadence("garbage"), Duration::from_secs(300));
        assert_eq!(parse_cadence(""), Duration::from_secs(300));
        assert_eq!(parse_cadence("0s"), Duration::from_secs(300));
        assert_eq!(parse_cadence("xm"), Duration::from_secs(300));
    }

    #[test]
    fn status_content_equal_ignores_last_tick_at() {
        // The anti-hot-loop invariant: two statuses identical except
        // last_tick_at are "equal" so the timestamp can't refire the watch.
        let base = ReconciliationLoopStatus {
            phase: Some(LoopPhase::Active),
            matched_workspaces: vec!["ns/a".to_string()],
            matched_count: 1,
            last_tick_at: Some(Utc::now()),
            observed_generation: 3,
            conditions: vec![],
        };
        let mut later = base.clone();
        later.last_tick_at = Some(Utc::now() + chrono::Duration::seconds(60));
        assert!(status_content_equal(&base, &later));
        // A real membership change is NOT equal.
        let mut changed = base.clone();
        changed.matched_count = 2;
        assert!(!status_content_equal(&base, &changed));
    }
}
