//! Provider-credential resolution from Kubernetes Secrets for
//! `InfrastructureTemplate`.
//!
//! Lifted from `template_controller.rs` during R6. The function reads
//! `spec.providerCredentials.{aws,cloudflare}` and resolves each into
//! a backend-config block via `BackendConfigGenerator`. Tolerant of
//! several legacy + current key names (`api_token` /
//! `CLOUDFLARE_API_TOKEN` / `CF_API_TOKEN`) so operator works with
//! secrets that follow either the pangea-CLI or workspace-template
//! ENV-fetch naming convention.

use crate::backend::BackendConfigGenerator;
use crate::controller::ControllerState;
use crate::crd::InfrastructureTemplate;
use crate::error::{Error, Result};
use k8s_openapi::api::core::v1::Secret;
use kube::api::Api;
use kube::ResourceExt;

/// Resolve a `ProviderCredentials` spec into the JSON provider config
/// that the OpenTofu backend expects. Pulls credentials from the
/// referenced Secret(s); returns Error::SecretNotFound when a
/// referenced Secret doesn't exist.
pub async fn resolve_provider_config(
    provider_creds: &crate::crd::ProviderCredentials,
    template: &InfrastructureTemplate,
    state: &ControllerState,
) -> Result<serde_json::Value> {
    let default_ns = template.namespace().unwrap_or_else(|| "default".to_string());

    let aws_creds = if let Some(aws) = &provider_creds.aws {
        let ns = aws
            .secret_ref
            .namespace
            .as_deref()
            .unwrap_or(&default_ns);
        let secret_api: Api<Secret> = Api::namespaced(state.client.clone(), ns);
        let secret = secret_api.get(&aws.secret_ref.name).await.map_err(|_| {
            Error::SecretNotFound {
                namespace: ns.to_string(),
                name: aws.secret_ref.name.clone(),
            }
        })?;

        let data = secret.data.as_ref().ok_or_else(|| {
            Error::Config("AWS credentials secret has no data".into())
        })?;

        let access_key = data
            .get("access_key")
            .or_else(|| data.get("AWS_ACCESS_KEY_ID"))
            .map(|v| String::from_utf8_lossy(&v.0).to_string())
            .ok_or_else(|| Error::Config("access_key not found in AWS secret".into()))?;

        let secret_key = data
            .get("secret_key")
            .or_else(|| data.get("AWS_SECRET_ACCESS_KEY"))
            .map(|v| String::from_utf8_lossy(&v.0).to_string())
            .ok_or_else(|| Error::Config("secret_key not found in AWS secret".into()))?;

        let session_token = data
            .get("session_token")
            .or_else(|| data.get("AWS_SESSION_TOKEN"))
            .map(|v| String::from_utf8_lossy(&v.0).to_string());

        Some(crate::backend::AwsCredentialsConfig {
            access_key,
            secret_key,
            session_token,
        })
    } else {
        None
    };

    let cf_creds = if let Some(cf) = &provider_creds.cloudflare {
        let ns = cf
            .secret_ref
            .namespace
            .as_deref()
            .unwrap_or(&default_ns);
        let secret_api: Api<Secret> = Api::namespaced(state.client.clone(), ns);
        let secret = secret_api.get(&cf.secret_ref.name).await.map_err(|_| {
            Error::SecretNotFound {
                namespace: ns.to_string(),
                name: cf.secret_ref.name.clone(),
            }
        })?;

        let data = secret.data.as_ref().ok_or_else(|| {
            Error::Config("Cloudflare credentials secret has no data".into())
        })?;

        // Several legacy + current key names — be tolerant so the
        // operator works with secrets that follow either the
        // pangea-CLI naming convention (api_token / CLOUDFLARE_API_TOKEN)
        // or the workspace-template ENV-fetch convention
        // (CF_API_TOKEN). If none are present we skip writing a
        // backend-managed provider block — the template's inline
        // `provider :cloudflare, …` (with ENV.fetch) already covers
        // that case via the new compile-time variables injection.
        let api_token = data
            .get("api_token")
            .or_else(|| data.get("CLOUDFLARE_API_TOKEN"))
            .or_else(|| data.get("CF_API_TOKEN"))
            .map(|v| String::from_utf8_lossy(&v.0).to_string());

        api_token.map(|t| crate::backend::CloudflareCredentialsConfig { api_token: t })
    } else {
        None
    };

    let aws_region = provider_creds
        .aws
        .as_ref()
        .and_then(|a| a.region.as_deref());

    Ok(BackendConfigGenerator::generate_provider_config(
        aws_region,
        aws_creds.as_ref(),
        cf_creds.as_ref(),
    ))
}
