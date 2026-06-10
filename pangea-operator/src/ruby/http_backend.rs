//! HTTP backend — wraps reqwest calls to the pangea-compiler sidecar.
//!
//! This is the existing path. Lifted out of `architecture_gem_controller.rs`
//! into a `CompilerBackend` impl so the reconciler doesn't have to know
//! whether it's hitting a sidecar or an embedded interpreter.

use async_trait::async_trait;

use super::backend::{
    ArchListing, BackendError, CompileAnyRequest, CompileAnyResult, CompileRequest,
    CompileResult, CompilerBackend, FixtureOutcome, SmokeRequest,
};

#[derive(Clone)]
pub struct HttpCompilerBackend {
    http: reqwest::Client,
    base_url: String,
}

impl HttpCompilerBackend {
    pub fn new(http: reqwest::Client, base_url: impl Into<String>) -> Self {
        Self {
            http,
            base_url: base_url.into(),
        }
    }
}

#[async_trait]
impl CompilerBackend for HttpCompilerBackend {
    async fn list_architectures(&self, gem: &str) -> Result<ArchListing, BackendError> {
        let url = format!("{}/v1/architectures?gem={}", self.base_url, gem);
        let resp = self
            .http
            .get(&url)
            .send()
            .await
            .map_err(|e| BackendError::Transport(format!("GET {url}: {e}")))?
            .error_for_status()
            .map_err(|e| BackendError::Compiler(format!("compiler returned: {e}")))?;
        resp.json::<ArchListing>()
            .await
            .map_err(|e| BackendError::Compiler(format!("decode listing: {e}")))
    }

    async fn compile(&self, req: CompileRequest) -> Result<CompileResult, BackendError> {
        let url = format!("{}/compile", self.base_url);
        let resp = self
            .http
            .post(&url)
            .json(&req)
            .timeout(std::time::Duration::from_secs(120))
            .send()
            .await
            .map_err(|e| BackendError::Transport(format!("POST {url}: {e}")))?;
        if !resp.status().is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(BackendError::Compiler(format!(
                "compile returned non-2xx: {body}"
            )));
        }
        // Compiler responds with { terraform_json, template_count, errors }.
        // Pull just terraform_json — the controller doesn't consume the rest.
        let raw: serde_json::Value = resp
            .json()
            .await
            .map_err(|e| BackendError::Compiler(format!("decode compile response: {e}")))?;
        let terraform_json = raw["terraform_json"]
            .as_str()
            .ok_or_else(|| {
                BackendError::Compiler("compile response missing terraform_json".into())
            })?
            .to_string();
        // HTTP-backend path: the legacy compiler sidecar returned
        // pretty-serialized JSON only. Re-parse it to populate
        // synthesis_value so embedded + HTTP backends present a
        // consistent CompileResult shape; magma + preview consumers
        // can stay backend-agnostic.
        let synthesis_value: Option<serde_json::Value> =
            serde_json::from_str(&terraform_json).ok();
        // The legacy sidecar has no conflict detector — honest absence.
        Ok(CompileResult { terraform_json, synthesis_value, conflicts: Vec::new() })
    }

    async fn compile_any(
        &self,
        req: CompileAnyRequest,
    ) -> Result<CompileAnyResult, BackendError> {
        let url = format!("{}/compile-any", self.base_url);
        let resp = self
            .http
            .post(&url)
            .json(&req)
            .timeout(std::time::Duration::from_secs(120))
            .send()
            .await
            .map_err(|e| BackendError::Transport(format!("POST {url}: {e}")))?;
        if !resp.status().is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(BackendError::Compiler(format!(
                "compile-any returned non-2xx: {body}"
            )));
        }
        resp.json::<CompileAnyResult>()
            .await
            .map_err(|e| BackendError::Compiler(format!("decode compile-any response: {e}")))
    }

    async fn smoke_test(&self, req: SmokeRequest) -> Result<FixtureOutcome, BackendError> {
        #[derive(serde::Serialize)]
        struct Wire<'a> {
            gem: &'a str,
            class_name: &'a str,
            fixture_path: &'a str,
        }
        let url = format!("{}/v1/architectures/smoke-test", self.base_url);
        let body = Wire {
            gem: &req.gem,
            class_name: &req.class_name,
            fixture_path: &req.fixture_path,
        };
        let resp = self
            .http
            .post(&url)
            .json(&body)
            .send()
            .await
            .map_err(|e| BackendError::Transport(format!("POST {url}: {e}")))?
            .error_for_status()
            .map_err(|e| BackendError::Compiler(format!("compiler returned: {e}")))?;
        resp.json::<FixtureOutcome>()
            .await
            .map_err(|e| BackendError::Compiler(format!("decode smoke result: {e}")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_stores_base_url_verbatim() {
        // base_url is concatenated with endpoint paths in every
        // method — drift here (e.g. trimming a trailing slash) would
        // break URL construction across all four endpoints.
        let client = reqwest::Client::new();
        let backend = HttpCompilerBackend::new(client.clone(), "http://compiler:3000");
        assert_eq!(backend.base_url, "http://compiler:3000");

        // Trailing slash NOT trimmed — the format!() call paths each
        // start with their own slash, so a trailing-slash base_url
        // produces a double-slash URL. The contract is "no trailing
        // slash"; this test locks that in.
        let backend2 =
            HttpCompilerBackend::new(client, "http://compiler:3000/");
        assert!(
            backend2.base_url.ends_with('/'),
            "constructor preserves caller-provided trailing slash"
        );
    }

    #[test]
    fn new_accepts_string_or_str() {
        // The Into<String> bound is convenient at call sites — verify
        // both common shapes work without compile-time errors.
        let client = reqwest::Client::new();
        let _ = HttpCompilerBackend::new(client.clone(), "http://x");
        let _ = HttpCompilerBackend::new(client, String::from("http://x"));
    }

    #[tokio::test]
    async fn list_architectures_against_unreachable_returns_transport_error() {
        // Hit a port no one is listening on; the reqwest layer must
        // bubble up as BackendError::Transport (not Compiler — that's
        // reserved for non-2xx HTTP responses, not network failures).
        // Using port 1 to guarantee connection refused.
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(2))
            .build()
            .unwrap();
        let backend = HttpCompilerBackend::new(client, "http://127.0.0.1:1");
        let err = backend.list_architectures("any").await.unwrap_err();
        assert!(
            matches!(err, BackendError::Transport(_)),
            "expected Transport, got {err:?}"
        );
    }
}
