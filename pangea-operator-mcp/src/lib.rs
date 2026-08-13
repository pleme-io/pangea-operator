//! `pangea-operator-mcp` — the MCP surface a model uses to OBSERVE and CONTROL
//! a running pangea-operator. The CRDs ARE the state (a typed facade, not a
//! second store — same shape as `breathe-mcp`); this crate is the rmcp tool
//! wrapping: one `#[tool]` per operator verb, over stdio.
//!
//! It answers the "I'm blind to the operator" failure mode directly: list +
//! get InfrastructureTemplates (with `status.lastCycle` — phase / lastError /
//! plan), force a reconcile, suspend/resume, see the WorkspaceCatalog verify
//! state + ArchitectureGem load phases (a not-Loaded gem blocks every template
//! under its workspace), and restart / status-check the operator Deployment.
//! A tool's patch is the same mutation `kubectl annotate` / `kubectl rollout
//! restart` performs, so it never contends with the controller's `status`
//! co-write.

mod facade;

use std::sync::Arc;

use rmcp::{
    handler::server::wrapper::Parameters,
    model::{ServerCapabilities, ServerInfo},
    schemars, tool, tool_handler, tool_router, ServerHandler,
};
use serde::Deserialize;
use serde_json::{json, Value};

pub use facade::{
    KubeStore, PangeaStore, StoreError, DEFAULT_OPERATOR_DEPLOYMENT, DEFAULT_OPERATOR_NS,
};

// ─────────────────────────── tool inputs ───────────────────────────

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct ListTemplatesInput {
    #[serde(default)]
    #[schemars(
        description = "Namespace to scope to (e.g. rio-architectures). Omitted = all namespaces."
    )]
    pub namespace: Option<String>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct TemplateRef {
    #[schemars(description = "Template namespace (e.g. rio-architectures)")]
    pub namespace: String,
    #[schemars(description = "Template name (e.g. rio-tendril-cloudflare-tunnel)")]
    pub name: String,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct SuspendInput {
    pub namespace: String,
    pub name: String,
    #[schemars(description = "true = pause reconciliation; false = resume")]
    pub suspend: bool,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct OperatorRef {
    #[serde(default)]
    #[schemars(description = "Operator Deployment namespace (default pangea-system)")]
    pub namespace: Option<String>,
    #[serde(default)]
    #[schemars(description = "Operator Deployment name (default pangea-operator)")]
    pub deployment: Option<String>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct Empty {}

// ─────────────────────────── the server ────────────────────────────

// No `tool_router` field, and its absence is the correct shape under rmcp 3
// rather than an omission. In 0.15 `#[tool_handler]` defaulted to the FIELD
// `self.tool_router`; in 3.x it defaults to the associated function
// `Self::tool_router()` that `#[tool_router]` generates. Carrying the field
// too would compile, wire the tools correctly via the associated function,
// and leave a never-read clone of the router on every instance — which is
// exactly what `dead_code` reported at the bump. The field is only earned
// back by a per-instance router (`#[tool_handler(router = self.tool_router)]`),
// and this server builds the same router every time.
#[derive(Clone)]
pub struct PangeaOperatorMcp {
    store: Arc<dyn PangeaStore>,
}

/// A successful read. `outcome: "found"` for a payload; a list that came back
/// empty is a FINDING and says so.
fn ok(v: &Value) -> String {
    // No let-chain: this crate is not on edition 2024.
    if let Value::Array(items) = v {
        if items.is_empty() {
            return kotae::Answer::empty("matching resources").render();
        }
    }
    kotae::Answer::found_value(v.clone()).render()
}

/// **The classification, and it is the whole value of adopting kotae here.**
///
/// This returned `{"error": <display>}` for every failure, so a template that
/// does not exist, a request the apiserver rejected, and an unreachable cluster
/// were one shape. An agent cannot act on that: the remedies are *stop looking*,
/// *change the request*, and *fix the environment*, and it had no way to tell
/// which it was facing.
///
/// `kube::Error::Api` carries the HTTP status, so the distinction is available
/// and was simply being discarded:
///
/// - **404** → `empty`. The resource is not there. That is an answer, not a
///   failure — and it is the one that most needs separating, because "the
///   template is gone" and "I could not reach the cluster" lead an operator to
///   opposite conclusions.
/// - **400 / 403 / 405 / 409 / 422** → `refused`. The apiserver understood and
///   declined: a malformed name, an RBAC denial, a conflicting write. The
///   caller must change something.
/// - **anything else** (5xx, connect, TLS, auth, timeout, serde) → `blind`.
///   We could not look. Conclude nothing about the resource.
///
/// Deliberately NOT collapsing 403 into `blind`: a permission denial is a fact
/// about the request, not about reachability, and telling an agent to retry it
/// wastes calls against an apiserver that will keep saying no.
fn err(e: &StoreError) -> String {
    let msg = e.to_string();
    let k = match e {
        StoreError::Kube(k) => k,
        // Serde: we could not render what we found. Not a statement about the
        // resource.
        StoreError::Serde(_) => return kotae::Answer::blind(msg).render(),
    };
    match k {
        kube::Error::Api(response) => match response.code {
            404 => kotae::Answer::empty(msg).render(),
            400 | 403 | 405 | 409 | 422 => kotae::Answer::refused(
                msg,
                ["a request the apiserver accepts (check name, namespace, and RBAC)"],
            )
            .render(),
            _ => kotae::Answer::blind(msg).render(),
        },
        _ => kotae::Answer::blind(msg).render(),
    }
}
fn nonce() -> String {
    chrono::Utc::now().to_rfc3339()
}

#[tool_router]
impl PangeaOperatorMcp {
    #[must_use]
    pub fn new(store: Arc<dyn PangeaStore>) -> Self {
        Self { store }
    }

    #[tool(
        description = "List InfrastructureTemplates (optionally namespace-scoped) as compact summaries: phase, suspended, lastError, planSummary, resourceCounts, lastCycle. The 'what is the operator doing / what is stuck' overview."
    )]
    pub async fn pangea_template_list(
        &self,
        Parameters(p): Parameters<ListTemplatesInput>,
    ) -> String {
        match self.store.list_templates(p.namespace.as_deref()).await {
            Ok(v) => ok(&json!(v)),
            Err(e) => err(&e),
        }
    }

    #[tool(
        description = "Get the full InfrastructureTemplate CR (spec + status.lastCycle) — the 'why is this template stuck / what did it last apply / what is the reconcile error' surface."
    )]
    pub async fn pangea_template_get(&self, Parameters(p): Parameters<TemplateRef>) -> String {
        match self.store.get_template(&p.namespace, &p.name).await {
            Ok(v) => ok(&v),
            Err(e) => err(&e),
        }
    }

    #[tool(
        description = "Force a reconcile of one InfrastructureTemplate by stamping a reconcile-request annotation (the `flux reconcile` analog) — kick a stuck/slow template without touching its desired state."
    )]
    pub async fn pangea_template_reconcile(
        &self,
        Parameters(p): Parameters<TemplateRef>,
    ) -> String {
        let n = nonce();
        match self
            .store
            .reconcile_template(&p.namespace, &p.name, &n)
            .await
        {
            Ok(()) => ok(
                &json!({ "reconcileRequestedAt": n, "template": format!("{}/{}", p.namespace, p.name) }),
            ),
            Err(e) => err(&e),
        }
    }

    #[tool(
        description = "Suspend (pause) or resume reconciliation of one InfrastructureTemplate via spec.suspend."
    )]
    pub async fn pangea_template_suspend(&self, Parameters(p): Parameters<SuspendInput>) -> String {
        match self
            .store
            .set_template_suspend(&p.namespace, &p.name, p.suspend)
            .await
        {
            Ok(()) => ok(
                &json!({ "suspended": p.suspend, "template": format!("{}/{}", p.namespace, p.name) }),
            ),
            Err(e) => err(&e),
        }
    }

    #[tool(
        description = "List WorkspaceCatalogs (cluster-scoped) with their verify status — a workspace whose required ArchitectureGems are not all Loaded blocks every template under it from advancing past Verified."
    )]
    pub async fn pangea_workspace_list(&self, Parameters(_): Parameters<Empty>) -> String {
        match self.store.list_workspaces().await {
            Ok(v) => ok(&json!(v)),
            Err(e) => err(&e),
        }
    }

    #[tool(
        description = "List ArchitectureGems with their load phase — a not-Loaded gem is a common root cause of templates stuck at Verified."
    )]
    pub async fn pangea_gem_list(&self, Parameters(_): Parameters<Empty>) -> String {
        match self.store.list_gems().await {
            Ok(v) => ok(&json!(v)),
            Err(e) => err(&e),
        }
    }

    #[tool(
        description = "Roll-restart the pangea-operator Deployment (default pangea-system/pangea-operator) — the break-glass for a wedged controller."
    )]
    pub async fn pangea_operator_restart(&self, Parameters(p): Parameters<OperatorRef>) -> String {
        let ns = p.namespace.as_deref().unwrap_or(DEFAULT_OPERATOR_NS);
        let dep = p
            .deployment
            .as_deref()
            .unwrap_or(DEFAULT_OPERATOR_DEPLOYMENT);
        let n = nonce();
        match self.store.restart_operator(ns, dep, &n).await {
            Ok(()) => ok(&json!({ "restartedAt": n, "deployment": format!("{ns}/{dep}") })),
            Err(e) => err(&e),
        }
    }

    #[tool(
        description = "Operator Deployment status — replicas / ready / available + conditions. Answers 'is the controller even up?' before chasing a template."
    )]
    pub async fn pangea_operator_status(&self, Parameters(p): Parameters<OperatorRef>) -> String {
        let ns = p.namespace.as_deref().unwrap_or(DEFAULT_OPERATOR_NS);
        let dep = p
            .deployment
            .as_deref()
            .unwrap_or(DEFAULT_OPERATOR_DEPLOYMENT);
        match self.store.operator_status(ns, dep).await {
            Ok(v) => ok(&v),
            Err(e) => err(&e),
        }
    }
}

#[tool_handler]
impl ServerHandler for PangeaOperatorMcp {
    fn get_info(&self) -> ServerInfo {
        // rmcp 3 marks `ServerInfo` #[non_exhaustive], so it cannot be built
        // with a struct expression — not even with `..Default::default()`,
        // which is the part that surprises people. The attribute forbids the
        // SYNTAX from outside the defining crate, independently of whether
        // every field is named. Start from the default and assign.
        let mut info = ServerInfo::default();
        info.capabilities = ServerCapabilities::builder().enable_tools().build();
        info.instructions = Some(
            "Observe + control a running pangea-operator. The CRDs are the state. List/get \
             InfrastructureTemplates with status.lastCycle (phase/lastError/plan), force a \
             reconcile, suspend/resume; read WorkspaceCatalog verify state + ArchitectureGem \
             load phases (a not-Loaded gem blocks templates at Verified); restart + status-check \
             the operator Deployment. Declare-and-observe: mutations are idempotent metadata/spec \
             patches (the kubectl annotate / rollout-restart analogs), never destructive."
                .into(),
        );
        info
    }
}

#[cfg(test)]
mod classification_tests {
    use super::*;

    fn api_error(code: u16) -> StoreError {
        StoreError::Kube(kube::Error::Api(kube::error::ErrorResponse {
            status: "Failure".into(),
            message: format!("status {code}"),
            reason: "Test".into(),
            code,
        }))
    }

    fn outcome_of(s: &str) -> String {
        let v: Value = serde_json::from_str(s).expect("every answer is valid JSON");
        v["outcome"].as_str().expect("carries a discriminant").to_owned()
    }

    /// **THE WHOLE VALUE OF THIS ADOPTION.** Before, every failure was
    /// `{"error": <display>}` — a 404, an RBAC denial and an unreachable
    /// cluster were ONE shape with three different remedies (stop looking,
    /// change the request, fix the environment). The status code was available
    /// on `kube::Error::Api` and simply discarded.
    #[test]
    fn a_status_code_decides_the_outcome() {
        assert_eq!(outcome_of(&err(&api_error(404))), "empty");
        for refused in [400, 403, 405, 409, 422] {
            assert_eq!(
                outcome_of(&err(&api_error(refused))),
                "refused",
                "status {refused} is the apiserver declining, not us being blind",
            );
        }
        for blind in [500, 502, 503] {
            assert_eq!(outcome_of(&err(&api_error(blind))), "blind", "status {blind}");
        }
    }

    /// 404 must NOT read as an error. "The template is not there" and "I could
    /// not reach the cluster" lead an operator to opposite conclusions, and
    /// collapsing them is the single most expensive confusion this type exists
    /// to prevent.
    #[test]
    fn a_missing_resource_is_empty_and_never_blind() {
        assert_ne!(outcome_of(&err(&api_error(404))), "blind");
    }

    /// 403 stays a REFUSAL rather than folding into blind. A permission denial
    /// is a fact about the request, not about reachability — telling an agent
    /// to retry it wastes calls against an apiserver that will keep saying no.
    #[test]
    fn a_permission_denial_is_refused_not_blind() {
        assert_eq!(outcome_of(&err(&api_error(403))), "refused");
    }

    /// A transport failure carries no information about the resource.
    #[test]
    fn a_non_api_kube_error_is_blind() {
        let e = StoreError::Kube(kube::Error::LinesCodecMaxLineLengthExceeded);
        assert_eq!(outcome_of(&err(&e)), "blind");
    }

    /// An empty list is a finding, not an empty success.
    #[test]
    fn an_empty_list_says_so() {
        assert_eq!(outcome_of(&ok(&json!([]))), "empty");
        assert_eq!(outcome_of(&ok(&json!([{"name": "x"}]))), "found");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use std::sync::Mutex;

    #[derive(Default)]
    struct MockStore {
        reconciled: Mutex<Vec<String>>,
        restarted: Mutex<Vec<String>>,
        suspended: Mutex<Vec<(String, bool)>>,
    }

    #[async_trait]
    impl PangeaStore for MockStore {
        async fn list_templates(&self, _ns: Option<&str>) -> Result<Vec<Value>, StoreError> {
            Ok(vec![
                json!({ "name": "rio-tendril-cloudflare-tunnel", "phase": "Pending" }),
            ])
        }
        async fn get_template(&self, ns: &str, name: &str) -> Result<Value, StoreError> {
            Ok(
                json!({ "metadata": { "namespace": ns, "name": name }, "status": { "phase": "Pending" } }),
            )
        }
        async fn reconcile_template(
            &self,
            ns: &str,
            name: &str,
            _n: &str,
        ) -> Result<(), StoreError> {
            self.reconciled.lock().unwrap().push(format!("{ns}/{name}"));
            Ok(())
        }
        async fn set_template_suspend(
            &self,
            _ns: &str,
            name: &str,
            s: bool,
        ) -> Result<(), StoreError> {
            self.suspended.lock().unwrap().push((name.to_string(), s));
            Ok(())
        }
        async fn list_workspaces(&self) -> Result<Vec<Value>, StoreError> {
            Ok(vec![
                json!({ "name": "rio-architectures", "status": { "verified": false } }),
            ])
        }
        async fn list_gems(&self) -> Result<Vec<Value>, StoreError> {
            Ok(vec![
                json!({ "name": "pangea-architectures", "phase": "Loaded" }),
            ])
        }
        async fn restart_operator(&self, ns: &str, dep: &str, _n: &str) -> Result<(), StoreError> {
            self.restarted.lock().unwrap().push(format!("{ns}/{dep}"));
            Ok(())
        }
        async fn operator_status(&self, _ns: &str, dep: &str) -> Result<Value, StoreError> {
            Ok(json!({ "name": dep, "readyReplicas": 1 }))
        }
    }

    #[tokio::test]
    async fn template_list_returns_summaries() {
        let mcp = PangeaOperatorMcp::new(Arc::new(MockStore::default()));
        let out = mcp
            .pangea_template_list(Parameters(ListTemplatesInput { namespace: None }))
            .await;
        assert!(out.contains("rio-tendril-cloudflare-tunnel"));
        assert!(out.contains("Pending"));
    }

    #[tokio::test]
    async fn reconcile_stamps_and_records_the_template() {
        let mock = Arc::new(MockStore::default());
        let mcp = PangeaOperatorMcp::new(mock.clone());
        let out = mcp
            .pangea_template_reconcile(Parameters(TemplateRef {
                namespace: "rio-architectures".into(),
                name: "rio-tendril-cloudflare-tunnel".into(),
            }))
            .await;
        assert!(out.contains("reconcileRequestedAt"));
        assert_eq!(
            mock.reconciled.lock().unwrap().as_slice(),
            ["rio-architectures/rio-tendril-cloudflare-tunnel"]
        );
    }

    #[tokio::test]
    async fn operator_restart_defaults_to_pangea_system() {
        let mock = Arc::new(MockStore::default());
        let mcp = PangeaOperatorMcp::new(mock.clone());
        let out = mcp
            .pangea_operator_restart(Parameters(OperatorRef {
                namespace: None,
                deployment: None,
            }))
            .await;
        assert!(out.contains("restartedAt"));
        assert_eq!(
            mock.restarted.lock().unwrap().as_slice(),
            ["pangea-system/pangea-operator"]
        );
    }

    #[tokio::test]
    async fn suspend_records_the_flag() {
        let mock = Arc::new(MockStore::default());
        let mcp = PangeaOperatorMcp::new(mock.clone());
        mcp.pangea_template_suspend(Parameters(SuspendInput {
            namespace: "rio-architectures".into(),
            name: "x".into(),
            suspend: true,
        }))
        .await;
        assert_eq!(
            mock.suspended.lock().unwrap().as_slice(),
            [("x".to_string(), true)]
        );
    }
}
