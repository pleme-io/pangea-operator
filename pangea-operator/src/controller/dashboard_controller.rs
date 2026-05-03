//! PangeaDashboard controller — synthesizes Grafana dashboards from Ruby DSL.
//!
//! Reconciliation loop:
//! 1. Read PangeaDashboard spec (inline Ruby or ConfigMap ref)
//! 2. POST Ruby to compiler sidecar → receive Grafana JSON
//! 3. Create/update ConfigMap with dashboard JSON (ownerReference)
//! 4. Create/update GrafanaDashboard CRD (configMapRef + instanceSelector)
//! 5. Update status: phase, configMapName, grafanaDashboardName, dashboardUid

use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use kube::{
    api::Api,
    runtime::{
        controller::{Action, Controller},
        watcher::Config,
    },
    ResourceExt,
};
use tracing::{debug, error, info, instrument, warn};

use crate::crd::{DashboardSource, PangeaDashboard};
use crate::error::Error;

use super::{ControllerState, DEFAULT_REQUEUE_INTERVAL};

/// Controller for PangeaDashboard resources.
pub struct DashboardController {
    state: ControllerState,
}

impl DashboardController {
    pub fn new(state: ControllerState) -> Self {
        Self { state }
    }

    /// Start the PangeaDashboard controller.
    pub async fn run(self) -> crate::error::Result<()> {
        let client = self.state.client.clone();
        let api: Api<PangeaDashboard> = Api::all(client.clone());
        let state = Arc::new(self.state);

        info!("Starting PangeaDashboard controller");

        Controller::new(api, Config::default())
            .run(
                move |pd, ctx| {
                    let state = Arc::clone(&ctx);
                    async move { reconcile(pd, state).await }
                },
                error_policy,
                state,
            )
            .for_each(|result| async move {
                match result {
                    Ok((obj, action)) => {
                        debug!(name = %obj.name, ?action, "PangeaDashboard reconciliation completed");
                    }
                    Err(e) => {
                        error!(error = %e, "PangeaDashboard reconciliation failed");
                    }
                }
            })
            .await;

        Ok(())
    }
}

fn error_policy(
    _pd: Arc<PangeaDashboard>,
    _error: &Error,
    _ctx: Arc<ControllerState>,
) -> Action {
    _ctx
        .metrics
        .record_reconcile(crate::crd::ControllerKind::Dashboard, "error");
    Action::requeue(Duration::from_secs(30))
}

#[instrument(skip(state), fields(name = %pd.name_any()))]
async fn reconcile(
    pd: Arc<PangeaDashboard>,
    state: Arc<ControllerState>,
) -> Result<Action, Error> {
    state
        .metrics
        .record_reconcile(crate::crd::ControllerKind::Dashboard, "ok");
    let name = pd.name_any();
    let namespace = pd.namespace().unwrap_or_else(|| "default".to_string());

    info!(%namespace, "Reconciling PangeaDashboard");

    // Cluster-wide kill-switch — honor `OperatorPolicy/default`.
    if let Some(action) = crate::controller::policy_gate::check_operator_policy(
        &state,
        crate::crd::ControllerKind::Dashboard,
    ) {
        return Ok(action);
    }

    // Per-CR suspend gate (the cluster-scoped policy is checked above).
    if pd.spec.suspend {
        info!(%name, %namespace, "PangeaDashboard suspended; skipping reconcile");
        return Ok(Action::requeue(Duration::from_secs(30)));
    }

    // Extract Ruby source
    let ruby_source = match &pd.spec.source {
        DashboardSource::Inline { ruby } => ruby.clone(),
        DashboardSource::ConfigMapRef {
            name: cm_name, key, ..
        } => {
            let cm_api: Api<k8s_openapi::api::core::v1::ConfigMap> =
                Api::namespaced(state.client.clone(), &namespace);
            match cm_api.get(cm_name).await {
                Ok(cm) => cm
                    .data
                    .and_then(|d| d.get(key).cloned())
                    .unwrap_or_default(),
                Err(e) => {
                    warn!(%name, error = %e, "Failed to read source ConfigMap");
                    return Ok(Action::requeue(Duration::from_secs(30)));
                }
            }
        }
    };

    // TODO: POST ruby_source to compiler sidecar at /compile
    // TODO: Create ConfigMap with synthesized JSON
    // TODO: Create GrafanaDashboard CRD with configMapRef
    // TODO: Update PangeaDashboard status

    info!(
        %name,
        source_len = ruby_source.len(),
        "PangeaDashboard source extracted, synthesis pending"
    );

    Ok(Action::requeue(DEFAULT_REQUEUE_INTERVAL))
}
