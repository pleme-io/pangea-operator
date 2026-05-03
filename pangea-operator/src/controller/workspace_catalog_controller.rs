//! WorkspaceCatalog reconciler.
//!
//! Workspace-level reconciler. For each `WorkspaceCatalog`:
//!   1. Resolve `spec.requiredGems` to `ArchitectureGem` CRs and check
//!      that every one has `status.phase == Loaded`. Aggregate result
//!      becomes the `GemsLoaded` condition.
//!   2. Count `InfrastructureTemplate` CRs labeled
//!      `pangea.pleme.io/workspace=<catalog name>` across every
//!      namespace. That count is `status.templateCount`.
//!   3. Mark `Verified=True` iff every required gem is loaded AND the
//!      catalog itself is reachable (we treat reachability as "the
//!      reconciler has run at least once" — Phase 1 honest semantics;
//!      M2 will add a real source-fetch reachability probe).
//!
//! Same content-equality patch gating as `architecture_gem_controller`
//! to avoid the watch hot-loop: we only patch status when the
//! observable content actually changed. Conditions preserve their
//! transition timestamps when their content didn't transition.
//!
//! See `pleme-io/theory/PANGEA-WORKSPACE-RECONCILIATION.md` §
//! "Hierarchical policy cascade" for the four-level policy story; this
//! controller owns the workspace level (cascade level 2).

use chrono::Utc;
use kube::{
    api::{Api, ListParams},
    runtime::controller::{Action, Controller},
    Client,
};
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, error, info, warn};

use crate::crd::architecture_gem::{
    ArchitectureGem, Condition, Phase as GemPhase,
};
use crate::crd::workspace_catalog::{WorkspaceCatalog, WorkspaceCatalogStatus};
use crate::crd::InfrastructureTemplate;
use futures::StreamExt;

/// Label key used by `InfrastructureTemplate` resources to declare
/// their parent workspace. The catalog controller counts templates
/// matching this key against its own `metadata.name`.
pub const WORKSPACE_LABEL: &str = "pangea.pleme.io/workspace";

/// Wire WorkspaceCatalog reconciliation into the operator's runtime.
pub fn run(
    client: Client,
    operator_policy: Arc<crate::controller::operator_policy_cache::OperatorPolicyCache>,
    metrics: Arc<crate::observability::Metrics>,
) -> impl std::future::Future<Output = ()> {
    let api: Api<WorkspaceCatalog> = Api::all(client.clone());
    let context = Arc::new(Context {
        client,
        operator_policy,
        metrics,
    });

    Controller::new(api, kube::runtime::watcher::Config::default())
        .run(reconcile, error_policy, context)
        .for_each(|res| async move {
            match res {
                Ok((obj, _)) => debug!("WorkspaceCatalog reconciled: {:?}", obj.name),
                Err(e) => error!("WorkspaceCatalog reconcile error: {:?}", e),
            }
        })
}

struct Context {
    client: Client,
    operator_policy: Arc<crate::controller::operator_policy_cache::OperatorPolicyCache>,
    metrics: Arc<crate::observability::Metrics>,
}

#[derive(Debug, thiserror::Error)]
enum Error {
    #[error("kube API error: {0}")]
    Kube(#[from] kube::Error),
    #[error("missing metadata.name on WorkspaceCatalog")]
    MissingName,
}

async fn reconcile(wsc: Arc<WorkspaceCatalog>, ctx: Arc<Context>) -> Result<Action, Error> {
    let name = wsc.metadata.name.clone().ok_or(Error::MissingName)?;

    ctx.metrics
        .record_reconcile(crate::crd::ControllerKind::WorkspaceCatalog, "ok");

    // Cluster-wide kill-switch — honor `OperatorPolicy/default`.
    // Routed through policy_pipeline (cache-only variant) so future
    // fleet-wide gates added to the pipeline apply uniformly.
    if let Some(action) = crate::controller::policy_pipeline::run_for_controller_with_cache(
        &ctx.operator_policy,
        crate::crd::ControllerKind::WorkspaceCatalog,
    )
    .into_skip_action()
    {
        return Ok(action);
    }

    // Per-workspace pause: `OperatorPolicy.spec.workspaceSuspend.catalogs[<name>]`.
    // Tri-state precedence ladder (catalog entry → controllerSuspend
    // → globalSuspend) — `Active` carves this catalog out of a more-
    // general pause; `Paused` freezes it regardless. Cache-only
    // variant (no metrics handle in this context).
    if let Some(action) = crate::controller::policy_pipeline::run_for_catalog_with_cache(
        &ctx.operator_policy,
        &name,
    )
    .into_skip_action()
    {
        return Ok(action);
    }

    if wsc.spec.suspend {
        info!(workspace = %name, "WorkspaceCatalog suspended; skipping reconcile");
        return Ok(Action::requeue(Duration::from_secs(300)));
    }

    info!(workspace = %name, "reconciling WorkspaceCatalog");

    // Resolve required gems → check phase == Loaded for each.
    let gem_api: Api<ArchitectureGem> = Api::all(ctx.client.clone());
    let mut missing_gems: Vec<String> = Vec::new();
    let mut not_loaded_gems: Vec<String> = Vec::new();
    for gem_name in &wsc.spec.required_gems {
        match gem_api.get_opt(gem_name).await? {
            None => missing_gems.push(gem_name.clone()),
            Some(g) => {
                let loaded = g
                    .status
                    .as_ref()
                    .and_then(|s| s.phase.as_ref())
                    .map(|p| matches!(p, GemPhase::Loaded))
                    .unwrap_or(false);
                if !loaded {
                    not_loaded_gems.push(gem_name.clone());
                }
            }
        }
    }
    let gems_loaded = missing_gems.is_empty() && not_loaded_gems.is_empty();

    // Count templates labeled with this workspace.
    let tmpl_api: Api<InfrastructureTemplate> = Api::all(ctx.client.clone());
    let label_selector = format!("{}={}", WORKSPACE_LABEL, name);
    let lp = ListParams::default().labels(&label_selector);
    let templates = tmpl_api.list(&lp).await?;
    let template_count = templates.items.len() as u32;

    let now = Utc::now();
    let conditions = vec![
        Condition {
            condition_type: "Reachable".to_string(),
            status: "True".to_string(),
            reason: "ReconciledOnce".to_string(),
            message: "Catalog reconciler has run; source reachability probe lands in M2."
                .to_string(),
            last_transition_time: now,
        },
        Condition {
            condition_type: "GemsLoaded".to_string(),
            status: if gems_loaded { "True" } else { "False" }.to_string(),
            reason: gems_loaded_reason(&missing_gems, &not_loaded_gems).to_string(),
            message: gems_loaded_message(&wsc.spec.required_gems, &missing_gems, &not_loaded_gems),
            last_transition_time: now,
        },
        Condition {
            condition_type: "Verified".to_string(),
            status: if gems_loaded { "True" } else { "False" }.to_string(),
            reason: if gems_loaded {
                "AllRequiredGemsLoaded"
            } else {
                "GemsNotReady"
            }
            .to_string(),
            message: if gems_loaded {
                format!(
                    "{} required gem(s) loaded; {} template(s) under reconciliation",
                    wsc.spec.required_gems.len(),
                    template_count
                )
            } else {
                "Verified gate blocked — see GemsLoaded condition".to_string()
            },
            last_transition_time: now,
        },
    ];

    let new_status = WorkspaceCatalogStatus {
        template_count,
        verified: gems_loaded,
        last_reconcile_time: Some(now),
        conditions,
    };

    patch_status_if_changed(&ctx.client, &name, wsc.status.as_ref(), new_status).await?;

    if !gems_loaded {
        warn!(
            workspace = %name,
            missing = ?missing_gems,
            not_loaded = ?not_loaded_gems,
            "WorkspaceCatalog blocked: required gems not ready"
        );
    } else {
        info!(
            workspace = %name,
            template_count,
            "WorkspaceCatalog reconcile complete"
        );
    }

    // 5-min default cadence — same shape as ArchitectureGem reconciler.
    Ok(Action::requeue(Duration::from_secs(300)))
}

fn gems_loaded_reason(missing: &[String], not_loaded: &[String]) -> &'static str {
    if !missing.is_empty() {
        "GemNotFound"
    } else if !not_loaded.is_empty() {
        "GemNotLoaded"
    } else {
        "AllGemsLoaded"
    }
}

fn gems_loaded_message(
    required: &[String],
    missing: &[String],
    not_loaded: &[String],
) -> String {
    if !missing.is_empty() {
        format!(
            "{} required gem(s) have no ArchitectureGem CR: {}",
            missing.len(),
            missing.join(", ")
        )
    } else if !not_loaded.is_empty() {
        format!(
            "{} gem(s) present but not phase=Loaded: {}",
            not_loaded.len(),
            not_loaded.join(", ")
        )
    } else {
        format!("all {} required gem(s) loaded", required.len())
    }
}

/// Patch status only when the computed content differs from what's
/// already on the resource. Same shape as
/// `architecture_gem_controller::patch_status_if_changed` — every
/// WorkspaceCatalog reconcile would otherwise bump
/// `lastReconcileTime` + every condition's `lastTransitionTime`,
/// trigger our own watch, and tighten into a hot loop.
async fn patch_status_if_changed(
    client: &Client,
    name: &str,
    old: Option<&WorkspaceCatalogStatus>,
    mut new_status: WorkspaceCatalogStatus,
) -> Result<(), Error> {
    if let Some(prev) = old {
        new_status.conditions = crate::controller::status::merge_condition_transitions(
            &prev.conditions,
            new_status.conditions,
        );
        if status_content_equal(prev, &new_status) {
            debug!(workspace = %name, "WorkspaceCatalog status unchanged; skipping patch");
            return Ok(());
        }
    }
    crate::controller::status::patch_status::<WorkspaceCatalog, _>(client, name, &new_status).await?;
    Ok(())
}

fn status_content_equal(a: &WorkspaceCatalogStatus, b: &WorkspaceCatalogStatus) -> bool {
    if a.template_count != b.template_count || a.verified != b.verified {
        return false;
    }
    if a.conditions.len() != b.conditions.len() {
        return false;
    }
    let _: HashSet<_> = a.conditions.iter().map(|c| &c.condition_type).collect();
    for (ac, bc) in a.conditions.iter().zip(b.conditions.iter()) {
        if ac.condition_type != bc.condition_type
            || ac.status != bc.status
            || ac.reason != bc.reason
            || ac.message != bc.message
        {
            return false;
        }
    }
    true
}

fn error_policy(_obj: Arc<WorkspaceCatalog>, err: &Error, ctx: Arc<Context>) -> Action {
    crate::controller::error_policy::run_error_policy(
        &ctx.metrics,
        crate::crd::ControllerKind::WorkspaceCatalog,
        err,
        Duration::from_secs(60),
    )
}

/// Look up the parent `WorkspaceCatalog` for an
/// `InfrastructureTemplate` via its `pangea.pleme.io/workspace` label.
/// Returns `Ok(None)` if the template carries no workspace label OR
/// the named catalog doesn't exist (templates without a parent
/// catalog continue to reconcile with no workspace-level cascade).
pub async fn parent_catalog_for_template(
    client: &Client,
    template: &InfrastructureTemplate,
) -> Result<Option<WorkspaceCatalog>, kube::Error> {
    let labels = match template.metadata.labels.as_ref() {
        Some(l) => l,
        None => return Ok(None),
    };
    let workspace_name = match labels.get(WORKSPACE_LABEL) {
        Some(n) => n.clone(),
        None => return Ok(None),
    };
    let api: Api<WorkspaceCatalog> = Api::all(client.clone());
    api.get_opt(&workspace_name).await
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cond(t: &str, s: &str, r: &str, m: &str, ts: chrono::DateTime<Utc>) -> Condition {
        Condition {
            condition_type: t.to_string(),
            status: s.to_string(),
            reason: r.to_string(),
            message: m.to_string(),
            last_transition_time: ts,
        }
    }

    #[test]
    fn merge_keeps_old_timestamp_when_condition_unchanged() {
        let old_ts = Utc::now() - chrono::Duration::hours(2);
        let new_ts = Utc::now();
        let prev = vec![cond("Verified", "True", "AllGemsLoaded", "msg", old_ts)];
        let new = vec![cond("Verified", "True", "AllGemsLoaded", "msg", new_ts)];
        let merged = crate::controller::status::merge_condition_transitions(&prev, new);
        assert_eq!(merged[0].last_transition_time, old_ts);
    }

    #[test]
    fn merge_uses_new_timestamp_on_transition() {
        let old_ts = Utc::now() - chrono::Duration::hours(2);
        let new_ts = Utc::now();
        let prev = vec![cond("Verified", "False", "GemsNotReady", "blocked", old_ts)];
        let new = vec![cond("Verified", "True", "AllGemsLoaded", "ok", new_ts)];
        let merged = crate::controller::status::merge_condition_transitions(&prev, new);
        assert_eq!(merged[0].last_transition_time, new_ts);
    }

    fn mk_status(template_count: u32, verified: bool, conds: Vec<Condition>) -> WorkspaceCatalogStatus {
        WorkspaceCatalogStatus {
            template_count,
            verified,
            last_reconcile_time: Some(Utc::now()),
            conditions: conds,
        }
    }

    #[test]
    fn equal_when_only_last_reconcile_time_differs() {
        let now = Utc::now();
        let cs = vec![cond("Verified", "True", "AllGemsLoaded", "ok", now)];
        let mut a = mk_status(3, true, cs.clone());
        let mut b = mk_status(3, true, cs);
        a.last_reconcile_time = Some(now - chrono::Duration::hours(1));
        b.last_reconcile_time = Some(now);
        assert!(status_content_equal(&a, &b));
    }

    #[test]
    fn unequal_when_template_count_differs() {
        let cs = vec![cond("Verified", "True", "AllGemsLoaded", "ok", Utc::now())];
        let a = mk_status(3, true, cs.clone());
        let b = mk_status(4, true, cs);
        assert!(!status_content_equal(&a, &b));
    }

    #[test]
    fn unequal_when_verified_flips() {
        let cs = vec![cond("Verified", "True", "AllGemsLoaded", "ok", Utc::now())];
        let a = mk_status(3, true, cs.clone());
        let b = mk_status(3, false, cs);
        assert!(!status_content_equal(&a, &b));
    }

    #[test]
    fn unequal_when_condition_message_differs() {
        let now = Utc::now();
        let a = mk_status(3, true, vec![cond("Verified", "True", "OK", "msg-a", now)]);
        let b = mk_status(3, true, vec![cond("Verified", "True", "OK", "msg-b", now)]);
        assert!(!status_content_equal(&a, &b));
    }

    #[test]
    fn gems_loaded_reason_picks_missing_first() {
        assert_eq!(
            gems_loaded_reason(&["x".into()], &["y".into()]),
            "GemNotFound"
        );
        assert_eq!(gems_loaded_reason(&[], &["y".into()]), "GemNotLoaded");
        assert_eq!(gems_loaded_reason(&[], &[]), "AllGemsLoaded");
    }

    #[test]
    fn gems_loaded_message_lists_missing() {
        let m = gems_loaded_message(
            &["a".into(), "b".into()],
            &["a".into()],
            &[],
        );
        assert!(m.contains("a"));
        assert!(m.contains("1 required gem"));
    }

    #[test]
    fn gems_loaded_message_lists_not_loaded() {
        let m = gems_loaded_message(
            &["a".into(), "b".into()],
            &[],
            &["b".into()],
        );
        assert!(m.contains("b"));
        assert!(m.contains("not phase=Loaded"));
    }

    #[test]
    fn gems_loaded_message_all_ok() {
        let m = gems_loaded_message(&["a".into(), "b".into()], &[], &[]);
        assert!(m.contains("all 2 required gem"));
    }
}
