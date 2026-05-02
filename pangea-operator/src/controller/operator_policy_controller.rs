//! Controller for `OperatorPolicy/default`.
//!
//! Two responsibilities:
//!
//!   1. **Cache propagation** — keep `state.operator_policy` in sync
//!      with the cluster's current `OperatorPolicy/default` spec, so
//!      every other controller's `policy_gate` read sees fresh data.
//!
//!   2. **Status mirror** — copy spec → `status.effective`, set
//!      `status.lastChangedAt`, and surface `status.reconcilesSkipped`
//!      from the in-memory counter.
//!
//! Singleton enforcement: only the resource named `default` is
//! honored. Any other name is logged at WARN and otherwise ignored —
//! the controller does not modify or fight non-default resources.
//!
//! Self-pause exception: this controller intentionally does NOT
//! consult `policy_gate`. If it did, `globalSuspend=true` would freeze
//! the very controller that surfaces the pause status — pause would
//! be invisible. The kill switch is one-way reversible by intent.

use crate::controller::{operator_policy_cache::OperatorPolicyCache, ControllerState};
use crate::crd::{OperatorPolicy, OperatorPolicyStatus, OPERATOR_POLICY_SINGLETON};
use crate::error::{Error, Result};

use futures::StreamExt;
use kube::{
    api::{Api, Patch, PatchParams},
    runtime::{
        controller::{Action, Controller},
        watcher::Config,
    },
    Client, ResourceExt,
};
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, error, info, instrument, warn};

const REQUEUE_INTERVAL: Duration = Duration::from_secs(60);
const ERROR_REQUEUE_INTERVAL: Duration = Duration::from_secs(15);

/// Top-level controller for `OperatorPolicy`.
pub struct OperatorPolicyController {
    state: ControllerState,
}

impl OperatorPolicyController {
    pub fn new(state: ControllerState) -> Self {
        Self { state }
    }

    /// Drive reconciliation. Call once at operator startup; runs until
    /// the controller stream terminates (operator shutdown).
    pub async fn run(self) -> Result<()> {
        let client = self.state.client.clone();
        let api: Api<OperatorPolicy> = Api::all(client.clone());
        let state = Arc::new(self.state);

        info!("Starting OperatorPolicy controller");

        Controller::new(api, Config::default())
            .run(
                move |policy, ctx| {
                    let state = Arc::clone(&ctx);
                    async move { reconcile(policy, state).await }
                },
                error_policy,
                state,
            )
            .for_each(|result| async move {
                match result {
                    Ok((obj, action)) => {
                        debug!(name = %obj.name, ?action, "OperatorPolicy reconcile completed");
                    }
                    Err(e) => {
                        warn!(error = %e, "OperatorPolicy reconcile error");
                    }
                }
            })
            .await;

        Ok(())
    }
}

#[instrument(skip(state), fields(name = %policy.name_any()))]
async fn reconcile(
    policy: Arc<OperatorPolicy>,
    state: Arc<ControllerState>,
) -> std::result::Result<Action, Error> {
    let name = policy.name_any();

    // Singleton enforcement. Non-default OperatorPolicies are no-ops.
    if !OperatorPolicyCache::is_singleton_name(&name) {
        warn!(
            name = %name,
            singleton = OPERATOR_POLICY_SINGLETON,
            "Ignoring OperatorPolicy: only the singleton named '{}' is honored",
            OPERATOR_POLICY_SINGLETON,
        );
        return Ok(Action::requeue(REQUEUE_INTERVAL));
    }

    info!("Reconciling OperatorPolicy/default");

    // Layer 1: propagate spec into the in-memory cache so every
    // controller's `policy_gate` sees the new value on its next read.
    state.operator_policy.store(policy.spec.clone());

    if policy.spec.global_suspend {
        let reason = policy
            .spec
            .global_suspend_reason
            .as_deref()
            .unwrap_or("(no reason given)");
        info!(reason = %reason, "Global suspend ACTIVE — every controller is paused");
    }

    // Layer 2: surface the resolved view + counter into status.
    let status = OperatorPolicyStatus {
        observed_generation: policy.metadata.generation.unwrap_or(0),
        last_changed_at: Some(chrono::Utc::now()),
        effective: Some(policy.spec.clone()),
        reconciles_skipped: state.operator_policy.skipped(),
    };

    let api: Api<OperatorPolicy> = Api::all(state.client.clone());
    let patch = serde_json::json!({
        "status": status,
    });
    let pp = PatchParams::apply("pangea-operator");

    // Use Merge (RFC 7396): no apiVersion/kind required, no field
    // ownership conflicts with kubectl-driven user patches against
    // spec. Status is exclusively operator-owned so merge is fine.
    if let Err(e) = api.patch_status(&name, &pp, &Patch::Merge(&patch)).await {
        error!(error = %e, "Failed to patch OperatorPolicy status");
        return Ok(Action::requeue(ERROR_REQUEUE_INTERVAL));
    }

    Ok(Action::requeue(REQUEUE_INTERVAL))
}

fn error_policy(
    _obj: Arc<OperatorPolicy>,
    error: &Error,
    _ctx: Arc<ControllerState>,
) -> Action {
    error!(%error, "OperatorPolicy reconcile error");
    Action::requeue(ERROR_REQUEUE_INTERVAL)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crd::{ControllerSuspend, OperatorPolicySpec};

    #[test]
    fn singleton_name_match() {
        assert!(OperatorPolicyCache::is_singleton_name("default"));
        assert!(!OperatorPolicyCache::is_singleton_name("other"));
    }

    #[test]
    fn cache_store_round_trip_via_spec() {
        // Simulates what the reconciler does: take a spec from a CR
        // and store it; subsequent reads reflect it.
        let cache = OperatorPolicyCache::new_permissive();
        let mut cs = ControllerSuspend::default();
        cs.dashboard = true;
        let spec = OperatorPolicySpec {
            global_suspend: false,
            global_suspend_reason: None,
            controller_suspend: cs,
        };
        cache.store(spec.clone());
        let read = cache.read();
        assert_eq!(*read, spec);
    }

    #[test]
    fn status_struct_serializes_with_camelcase() {
        let status = OperatorPolicyStatus {
            observed_generation: 5,
            last_changed_at: None,
            effective: None,
            reconciles_skipped: 42,
        };
        let json = serde_json::to_value(&status).unwrap();
        assert!(json.get("observedGeneration").is_some());
        assert!(json.get("reconcilesSkipped").is_some());
        assert_eq!(json.get("reconcilesSkipped").unwrap(), 42);
    }
}
