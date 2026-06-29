//! The typed kube facade behind the pangea-operator MCP — the CRDs ARE the
//! state (a typed facade, not a second store), exactly like `breathe-facade`.
//!
//! Reads/patches the pangea-operator control surface via kube's DYNAMIC api
//! (group `pangea.pleme.io`, version `v1alpha1`) so this crate stays decoupled
//! from the operator's internal CRD structs — it only needs the GVK + the
//! handful of status/spec fields it surfaces. A `reconcile`/`restart` is the
//! same metadata patch a `kubectl annotate` / `kubectl rollout restart`
//! performs, so it never contends with the controller (which co-writes
//! `status`). Behind the [`PangeaStore`] trait so the tools are mock-tested
//! without a cluster.

use async_trait::async_trait;
use k8s_openapi::api::apps::v1::Deployment;
use kube::{
    api::{Api, ApiResource, DynamicObject, GroupVersionKind, ListParams, Patch, PatchParams},
    Client,
};
use serde_json::{json, Value};
use thiserror::Error;

pub const GROUP: &str = "pangea.pleme.io";
pub const VERSION: &str = "v1alpha1";

/// Default location of the operator Deployment on a pleme-io cluster.
pub const DEFAULT_OPERATOR_NS: &str = "pangea-system";
pub const DEFAULT_OPERATOR_DEPLOYMENT: &str = "pangea-operator";

#[derive(Debug, Error)]
pub enum StoreError {
    #[error("kube: {0}")]
    Kube(#[from] kube::Error),
    #[error("serialize: {0}")]
    Serde(#[from] serde_json::Error),
}

/// The control + observe surface a model drives over the pangea-operator space.
/// One method per operator verb; every mutation is an idempotent metadata/spec
/// patch (never a destructive op).
#[async_trait]
pub trait PangeaStore: Send + Sync {
    /// List InfrastructureTemplates (optionally namespace-scoped) as compact
    /// summaries: phase, suspended, lastError, planSummary, resourceCounts.
    async fn list_templates(&self, namespace: Option<&str>) -> Result<Vec<Value>, StoreError>;
    /// The full InfrastructureTemplate CR (spec + status.lastCycle) — the
    /// "why is this stuck / what did it last do" surface.
    async fn get_template(&self, namespace: &str, name: &str) -> Result<Value, StoreError>;
    /// Force a reconcile by stamping a reconcile-request annotation (a metadata
    /// change the controller's watch fires on) — the `flux reconcile` analog.
    async fn reconcile_template(&self, namespace: &str, name: &str, nonce: &str) -> Result<(), StoreError>;
    /// Patch `spec.suspend` to pause/resume reconciliation of one template.
    async fn set_template_suspend(&self, namespace: &str, name: &str, suspend: bool) -> Result<(), StoreError>;
    /// List WorkspaceCatalogs (cluster-scoped) with their verify status.
    async fn list_workspaces(&self) -> Result<Vec<Value>, StoreError>;
    /// List ArchitectureGems with their load phase (a not-Loaded gem blocks
    /// every template under its workspace from advancing past Verified).
    async fn list_gems(&self) -> Result<Vec<Value>, StoreError>;
    /// Roll-restart the operator Deployment (stamp the standard
    /// `kubectl.kubernetes.io/restartedAt` pod-template annotation).
    async fn restart_operator(&self, namespace: &str, deployment: &str, nonce: &str) -> Result<(), StoreError>;
    /// The operator Deployment status — replicas/ready/available + conditions
    /// (is the controller even up?).
    async fn operator_status(&self, namespace: &str, deployment: &str) -> Result<Value, StoreError>;
}

/// Live implementation over a kube [`Client`] (in-cluster or kubeconfig).
pub struct KubeStore {
    client: Client,
}

impl KubeStore {
    /// In-cluster (sidecar) or local-kubeconfig client — same `from_env`
    /// contract breathe-mcp uses.
    pub async fn from_env() -> Result<Self, StoreError> {
        Ok(Self { client: Client::try_default().await? })
    }

    pub fn new(client: Client) -> Self {
        Self { client }
    }

    fn ar(kind: &str, plural: &str) -> ApiResource {
        let gvk = GroupVersionKind::gvk(GROUP, VERSION, kind);
        ApiResource::from_gvk_with_plural(&gvk, plural)
    }

    fn templates_api(&self, namespace: Option<&str>) -> Api<DynamicObject> {
        let ar = Self::ar("InfrastructureTemplate", "infrastructuretemplates");
        match namespace {
            Some(ns) => Api::namespaced_with(self.client.clone(), ns, &ar),
            None => Api::all_with(self.client.clone(), &ar),
        }
    }

    fn cluster_api(&self, kind: &str, plural: &str) -> Api<DynamicObject> {
        Api::all_with(self.client.clone(), &Self::ar(kind, plural))
    }
}

/// Extract a compact, robust summary from a template CR (tolerant of
/// camelCase / snake_case status field naming).
fn summarize(obj: &DynamicObject) -> Value {
    let status = obj.data.get("status").cloned().unwrap_or(Value::Null);
    let spec = obj.data.get("spec").cloned().unwrap_or(Value::Null);
    let pick = |k_camel: &str, k_snake: &str| -> Value {
        status
            .get(k_camel)
            .or_else(|| status.get(k_snake))
            .cloned()
            .unwrap_or(Value::Null)
    };
    json!({
        "name": obj.metadata.name,
        "namespace": obj.metadata.namespace,
        "phase": pick("phase", "phase"),
        "suspended": spec.get("suspend").cloned()
            .or_else(|| pick("suspended", "suspended").into()),
        "lastError": pick("lastError", "last_error"),
        "planSummary": pick("planSummary", "plan_summary"),
        "resourceCounts": pick("resourceCounts", "resource_counts"),
        "lastCycle": pick("lastCycle", "last_cycle"),
    })
}

#[async_trait]
impl PangeaStore for KubeStore {
    async fn list_templates(&self, namespace: Option<&str>) -> Result<Vec<Value>, StoreError> {
        let list = self.templates_api(namespace).list(&ListParams::default()).await?;
        Ok(list.items.iter().map(summarize).collect())
    }

    async fn get_template(&self, namespace: &str, name: &str) -> Result<Value, StoreError> {
        let obj = self.templates_api(Some(namespace)).get(name).await?;
        Ok(serde_json::to_value(obj)?)
    }

    async fn reconcile_template(&self, namespace: &str, name: &str, nonce: &str) -> Result<(), StoreError> {
        let patch = json!({
            "metadata": { "annotations": { "pangea.pleme.io/reconcile-requested-at": nonce } }
        });
        self.templates_api(Some(namespace))
            .patch(name, &PatchParams::default(), &Patch::Merge(&patch))
            .await?;
        Ok(())
    }

    async fn set_template_suspend(&self, namespace: &str, name: &str, suspend: bool) -> Result<(), StoreError> {
        let patch = json!({ "spec": { "suspend": suspend } });
        self.templates_api(Some(namespace))
            .patch(name, &PatchParams::default(), &Patch::Merge(&patch))
            .await?;
        Ok(())
    }

    async fn list_workspaces(&self) -> Result<Vec<Value>, StoreError> {
        let list = self
            .cluster_api("WorkspaceCatalog", "workspacecatalogs")
            .list(&ListParams::default())
            .await?;
        Ok(list
            .items
            .iter()
            .map(|o| {
                json!({
                    "name": o.metadata.name,
                    "status": o.data.get("status").cloned().unwrap_or(Value::Null),
                    "requiredGems": o.data.pointer("/spec/requiredGems").cloned().unwrap_or(Value::Null),
                })
            })
            .collect())
    }

    async fn list_gems(&self) -> Result<Vec<Value>, StoreError> {
        let list = self
            .cluster_api("ArchitectureGem", "architecturegems")
            .list(&ListParams::default())
            .await?;
        Ok(list
            .items
            .iter()
            .map(|o| {
                json!({
                    "name": o.metadata.name,
                    "phase": o.data.pointer("/status/phase").cloned().unwrap_or(Value::Null),
                    "status": o.data.get("status").cloned().unwrap_or(Value::Null),
                })
            })
            .collect())
    }

    async fn restart_operator(&self, namespace: &str, deployment: &str, nonce: &str) -> Result<(), StoreError> {
        let api: Api<Deployment> = Api::namespaced(self.client.clone(), namespace);
        let patch = json!({
            "spec": { "template": { "metadata": { "annotations": {
                "kubectl.kubernetes.io/restartedAt": nonce
            } } } }
        });
        api.patch(deployment, &PatchParams::default(), &Patch::Merge(&patch)).await?;
        Ok(())
    }

    async fn operator_status(&self, namespace: &str, deployment: &str) -> Result<Value, StoreError> {
        let api: Api<Deployment> = Api::namespaced(self.client.clone(), namespace);
        let d = api.get(deployment).await?;
        let status = d.status.unwrap_or_default();
        Ok(json!({
            "name": deployment,
            "namespace": namespace,
            "replicas": status.replicas,
            "readyReplicas": status.ready_replicas,
            "availableReplicas": status.available_replicas,
            "updatedReplicas": status.updated_replicas,
            "unavailableReplicas": status.unavailable_replicas,
            "conditions": status.conditions,
        }))
    }
}
