//! Provider-credential resolution from Kubernetes Secrets for
//! `InfrastructureTemplate`.
//!
//! Lifted from `template_controller.rs` during R6. The function reads
//! `spec.providerCredentials.{aws,cloudflare,porkbun}` and resolves
//! each into a backend-config block via `BackendConfigGenerator`.
//! Tolerant of several legacy + current key names (`api_token` /
//! `CLOUDFLARE_API_TOKEN` / `CF_API_TOKEN`, `api_key` /
//! `PORKBUN_API_KEY`, …) so operator works with secrets that follow
//! either the pangea-CLI or workspace-template ENV-fetch naming
//! convention.
//!
//! Two consumers of the same resolved-from-Secret credential values:
//!
//! 1. [`resolve_provider_config`] — the **tofu** path. Returns the
//!    `providers.tf.json`-shaped `{"provider": {"<name>": {...}}}`
//!    JSON value (`BackendConfigGenerator::generate_provider_config`),
//!    written to disk and consumed by `tofu init`.
//! 2. [`resolve_provider_configs`] — the **magma** path. Returns each
//!    provider's bare ConfigureProvider config-object keyed by the
//!    terraform provider local name (`cloudflare` →
//!    `{"api_token": …}`), threaded into magma's `ApplyContext` via
//!    `with_provider_config`. This is the magma analogue of writing
//!    `providers.tf.json` — without it, magma's in-process provider-RPC
//!    apply hands the provider a null config and every real RPC fails
//!    ("Service was not ready: channel closed").
//!
//! Both share the exact same Secret-read + tolerant-key logic; the
//! per-provider attribute shape lives once in
//! [`provider_config_object`] so the two surfaces can never drift.

use crate::backend::BackendConfigGenerator;
use crate::controller::ControllerState;
use crate::crd::{InfrastructureTemplate, ProviderKind};
use crate::error::{Error, Result};
use k8s_openapi::api::core::v1::Secret;
use k8s_openapi::ByteString;
use kube::api::Api;
use kube::ResourceExt;
use std::collections::BTreeMap;

/// Resolve a `ProviderCredentials` spec into the JSON provider config
/// that the OpenTofu backend expects. Pulls credentials from the
/// referenced Secret(s); returns Error::SecretNotFound when a
/// referenced Secret doesn't exist.
pub async fn resolve_provider_config(
    provider_creds: &crate::crd::ProviderCredentials,
    template: &InfrastructureTemplate,
    state: &ControllerState,
) -> Result<Option<serde_json::Value>> {
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

    let pb_creds = if let Some(pb) = &provider_creds.porkbun {
        let ns = pb
            .secret_ref
            .namespace
            .as_deref()
            .unwrap_or(&default_ns);
        let secret_api: Api<Secret> = Api::namespaced(state.client.clone(), ns);
        let secret = secret_api.get(&pb.secret_ref.name).await.map_err(|_| {
            Error::SecretNotFound {
                namespace: ns.to_string(),
                name: pb.secret_ref.name.clone(),
            }
        })?;

        let data = secret.data.as_ref().ok_or_else(|| {
            Error::Config("Porkbun credentials secret has no data".into())
        })?;

        // Tolerant of the terraform-attribute naming (api_key /
        // secret_api_key) and the env-var convention
        // (PORKBUN_API_KEY / PORKBUN_SECRET_API_KEY) — same style as
        // the cloudflare tolerant-key fallback above.
        let api_key = data
            .get("api_key")
            .or_else(|| data.get("PORKBUN_API_KEY"))
            .map(|v| String::from_utf8_lossy(&v.0).to_string())
            .ok_or_else(|| Error::Config("api_key not found in Porkbun secret".into()))?;

        let secret_api_key = data
            .get("secret_api_key")
            .or_else(|| data.get("PORKBUN_SECRET_API_KEY"))
            .map(|v| String::from_utf8_lossy(&v.0).to_string())
            .ok_or_else(|| Error::Config("secret_api_key not found in Porkbun secret".into()))?;

        Some(crate::backend::PorkbunCredentialsConfig {
            api_key,
            secret_api_key,
        })
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
        pb_creds.as_ref(),
    ))
}

/// Read a value from a Secret's data map, trying each candidate key in
/// order. Returns the first present key's value decoded as a UTF-8
/// string (lossy). The tolerant-key fallbacks mirror
/// [`resolve_provider_config`] so the magma path accepts the exact
/// same secret shapes the tofu path does.
fn first_present(
    data: &BTreeMap<String, ByteString>,
    candidates: &[&str],
) -> Option<String> {
    candidates
        .iter()
        .find_map(|k| data.get(*k))
        .map(|v| String::from_utf8_lossy(&v.0).to_string())
}

/// Build the bare ConfigureProvider **config-object** for one provider
/// from a resolved Secret's `data` map (+ optional AWS region from the
/// CRD). This is the single place the per-provider attribute shape is
/// authored; both the magma path here and the tofu path's
/// `generate_provider_config` map the same credentials, so the two
/// surfaces cannot drift.
///
/// Attribute names are the provider's real terraform schema attrs:
///   * cloudflare → `{"api_token": …}`        (scoped API token)
///   * aws        → `{"region"?, "access_key"?, "secret_key"?, "token"?}`
///                  (`token` is the AWS provider's session-token attr)
///   * github     → `{"token": …, "owner"?}`
///   * porkbun    → `{"api_key": …, "secret_api_key": …}` (both required
///                  by the `marcfrederick/porkbun` provider schema)
///
/// Returns `None` when no usable credential attr is present (e.g. a
/// cloudflare secret with no recognizable token key) — the caller then
/// omits that provider, letting the rendered config or the provider's
/// own pod-env fallback supply it, exactly like the tofu path's
/// `None`-returns-no-block behavior.
fn provider_config_object(
    kind: ProviderKind,
    data: &BTreeMap<String, ByteString>,
    aws_region: Option<&str>,
) -> Option<serde_json::Value> {
    match kind {
        ProviderKind::Cloudflare => {
            let api_token = first_present(
                data,
                &["api_token", "CLOUDFLARE_API_TOKEN", "CF_API_TOKEN"],
            )?;
            let mut obj = serde_json::Map::new();
            obj.insert("api_token".to_string(), serde_json::Value::String(api_token));
            Some(serde_json::Value::Object(obj))
        }
        ProviderKind::Aws => {
            let mut obj = serde_json::Map::new();
            if let Some(region) = aws_region {
                obj.insert("region".to_string(), serde_json::Value::String(region.to_string()));
            }
            if let Some(ak) = first_present(data, &["access_key", "AWS_ACCESS_KEY_ID"]) {
                obj.insert("access_key".to_string(), serde_json::Value::String(ak));
            }
            if let Some(sk) = first_present(data, &["secret_key", "AWS_SECRET_ACCESS_KEY"]) {
                obj.insert("secret_key".to_string(), serde_json::Value::String(sk));
            }
            // AWS provider's session-token attr is `token`, NOT `session_token`.
            if let Some(t) = first_present(data, &["session_token", "AWS_SESSION_TOKEN"]) {
                obj.insert("token".to_string(), serde_json::Value::String(t));
            }
            if obj.is_empty() {
                None
            } else {
                Some(serde_json::Value::Object(obj))
            }
        }
        ProviderKind::GitHub => {
            let token = first_present(data, &["token", "password", "GITHUB_TOKEN"])?;
            let mut obj = serde_json::Map::new();
            obj.insert("token".to_string(), serde_json::Value::String(token));
            if let Some(owner) = first_present(data, &["owner", "GITHUB_OWNER"]) {
                obj.insert("owner".to_string(), serde_json::Value::String(owner));
            }
            Some(serde_json::Value::Object(obj))
        }
        ProviderKind::Porkbun => {
            let api_key = first_present(data, &["api_key", "PORKBUN_API_KEY"])?;
            let secret_api_key =
                first_present(data, &["secret_api_key", "PORKBUN_SECRET_API_KEY"])?;
            let mut obj = serde_json::Map::new();
            obj.insert("api_key".to_string(), serde_json::Value::String(api_key));
            obj.insert(
                "secret_api_key".to_string(),
                serde_json::Value::String(secret_api_key),
            );
            Some(serde_json::Value::Object(obj))
        }
    }
}

/// Resolve every populated `spec.providerCredentials` provider into its
/// bare ConfigureProvider **config-object**, keyed by the terraform
/// provider local name (`cloudflare` / `aws` / `github` / `porkbun`).
///
/// This is the magma analogue of [`resolve_provider_config`] (which
/// writes `providers.tf.json` for tofu). The returned map is folded
/// into magma's `ApplyContext` via `with_provider_config`, so the
/// in-process provider-RPC apply/destroy reaches each provider with
/// real credentials instead of a null config.
///
/// Iteration is exhaustive over `ProviderCredentials::iter_secret_refs`
/// (compile-forced when a new `ProviderKind` lands), so the fix closes
/// the whole **class**: any provider whose creds live in
/// `spec.providerCredentials` reaches magma regardless of whether the
/// Ruby renderer emits a `provider "<name>" {}` block.
///
/// A referenced Secret that doesn't exist is a typed
/// `Error::SecretNotFound`. A populated provider whose secret carries
/// no recognizable credential attr is simply omitted (the rendered
/// config or the provider's pod-env fallback covers it) — never a
/// silent wrong answer, never a panic.
pub async fn resolve_provider_configs(
    provider_creds: &crate::crd::ProviderCredentials,
    template: &InfrastructureTemplate,
    state: &ControllerState,
) -> Result<BTreeMap<String, serde_json::Value>> {
    let default_ns = template.namespace().unwrap_or_else(|| "default".to_string());
    let aws_region = provider_creds
        .aws
        .as_ref()
        .and_then(|a| a.region.as_deref());

    let mut out: BTreeMap<String, serde_json::Value> = BTreeMap::new();

    for (kind, sref) in provider_creds.iter_secret_refs() {
        let ns = sref.namespace.as_deref().unwrap_or(&default_ns);
        let secret_api: Api<Secret> = Api::namespaced(state.client.clone(), ns);
        let secret = secret_api.get(&sref.name).await.map_err(|_| {
            Error::SecretNotFound {
                namespace: ns.to_string(),
                name: sref.name.clone(),
            }
        })?;

        let Some(data) = secret.data.as_ref() else {
            // No data block — nothing to forward for this provider.
            // The provider's pod-env fallback still applies.
            continue;
        };

        if let Some(obj) = provider_config_object(kind, data, aws_region) {
            out.insert(kind.name().to_string(), obj);
        }
    }

    Ok(out)
}

/// Merge a base provider-config object (from `spec.providerCredentials`)
/// with a rendered-config provider block, into one config-object.
///
/// **Precedence (documented):** the base — `spec.providerCredentials` —
/// is the authoritative source of *credentials*; the rendered-config
/// block carries non-secret author tuning (account_id, region, …). A
/// rendered key wins on conflict, EXCEPT it never clobbers a present
/// base credential attr with a null/empty value. Because rio renders no
/// `provider "<name>" {}` block today, this yields exactly the base
/// `{"api_token": …}` — the missing credential the magma path dropped.
pub fn merge_provider_config(
    base: &serde_json::Value,
    rendered: Option<&serde_json::Value>,
) -> serde_json::Value {
    let mut merged: serde_json::Map<String, serde_json::Value> = base
        .as_object()
        .cloned()
        .unwrap_or_default();

    if let Some(serde_json::Value::Object(rendered_obj)) = rendered {
        for (k, v) in rendered_obj {
            // Rendered tuning wins — but a null/empty rendered value must
            // not erase a real base credential.
            let is_empty = v.is_null()
                || matches!(v, serde_json::Value::String(s) if s.is_empty());
            if is_empty && merged.contains_key(k) {
                continue;
            }
            merged.insert(k.clone(), v.clone());
        }
    }

    serde_json::Value::Object(merged)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn data(pairs: &[(&str, &str)]) -> BTreeMap<String, ByteString> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), ByteString(v.as_bytes().to_vec())))
            .collect()
    }

    #[test]
    fn cloudflare_resolves_api_token_attr() {
        let d = data(&[("api_token", "cf-secret-token")]);
        let obj = provider_config_object(ProviderKind::Cloudflare, &d, None)
            .expect("cloudflare token present → config object");
        assert_eq!(obj["api_token"], "cf-secret-token");
        // The shape magma's with_provider_config consumes is a bare attr
        // object: exactly one key, the provider's ConfigureProvider attr.
        assert_eq!(obj.as_object().unwrap().len(), 1);
    }

    #[test]
    fn cloudflare_tolerant_key_fallbacks() {
        for key in ["api_token", "CLOUDFLARE_API_TOKEN", "CF_API_TOKEN"] {
            let d = data(&[(key, "t")]);
            let obj = provider_config_object(ProviderKind::Cloudflare, &d, None)
                .unwrap_or_else(|| panic!("key {key} should resolve"));
            assert_eq!(obj["api_token"], "t");
        }
    }

    #[test]
    fn cloudflare_no_token_yields_none() {
        let d = data(&[("unrelated", "x")]);
        assert!(provider_config_object(ProviderKind::Cloudflare, &d, None).is_none());
    }

    #[test]
    fn aws_resolves_region_and_creds_with_session_token_attr() {
        let d = data(&[
            ("access_key", "AKID"),
            ("secret_key", "SK"),
            ("session_token", "STS"),
        ]);
        let obj = provider_config_object(ProviderKind::Aws, &d, Some("us-east-1"))
            .expect("aws creds present → config object");
        assert_eq!(obj["region"], "us-east-1");
        assert_eq!(obj["access_key"], "AKID");
        assert_eq!(obj["secret_key"], "SK");
        // AWS session-token attr is `token`, not `session_token`.
        assert_eq!(obj["token"], "STS");
        assert!(obj.get("session_token").is_none());
    }

    #[test]
    fn aws_region_only_still_emits_block() {
        let d = data(&[]);
        let obj = provider_config_object(ProviderKind::Aws, &d, Some("eu-west-1"))
            .expect("region alone → config object (creds from instance role/env)");
        assert_eq!(obj["region"], "eu-west-1");
        assert!(obj.get("access_key").is_none());
    }

    #[test]
    fn aws_empty_yields_none() {
        let d = data(&[]);
        assert!(provider_config_object(ProviderKind::Aws, &d, None).is_none());
    }

    #[test]
    fn github_resolves_token_and_optional_owner() {
        let d = data(&[("token", "ghp_xxx"), ("owner", "pleme-io")]);
        let obj = provider_config_object(ProviderKind::GitHub, &d, None)
            .expect("github token present → config object");
        assert_eq!(obj["token"], "ghp_xxx");
        assert_eq!(obj["owner"], "pleme-io");
    }

    #[test]
    fn github_token_without_owner() {
        let d = data(&[("GITHUB_TOKEN", "ghp_yyy")]);
        let obj = provider_config_object(ProviderKind::GitHub, &d, None)
            .expect("github token present → config object");
        assert_eq!(obj["token"], "ghp_yyy");
        assert!(obj.get("owner").is_none());
    }

    #[test]
    fn porkbun_resolves_api_key_and_secret_api_key() {
        let d = data(&[("api_key", "pk1_abc"), ("secret_api_key", "sk1_xyz")]);
        let obj = provider_config_object(ProviderKind::Porkbun, &d, None)
            .expect("porkbun creds present → config object");
        assert_eq!(obj["api_key"], "pk1_abc");
        assert_eq!(obj["secret_api_key"], "sk1_xyz");
        assert_eq!(obj.as_object().unwrap().len(), 2);
    }

    #[test]
    fn porkbun_tolerant_env_var_key_fallbacks() {
        let d = data(&[
            ("PORKBUN_API_KEY", "pk1_env"),
            ("PORKBUN_SECRET_API_KEY", "sk1_env"),
        ]);
        let obj = provider_config_object(ProviderKind::Porkbun, &d, None)
            .expect("porkbun creds present via env-var-shaped keys → config object");
        assert_eq!(obj["api_key"], "pk1_env");
        assert_eq!(obj["secret_api_key"], "sk1_env");
    }

    #[test]
    fn porkbun_missing_secret_api_key_yields_none() {
        // Both attrs are required by the provider schema — a partial
        // secret must not produce a half-populated config object.
        let d = data(&[("api_key", "pk1_abc")]);
        assert!(provider_config_object(ProviderKind::Porkbun, &d, None).is_none());
    }

    #[test]
    fn porkbun_empty_yields_none() {
        let d = data(&[]);
        assert!(provider_config_object(ProviderKind::Porkbun, &d, None).is_none());
    }

    /// The load-bearing assertion for this fix: a resolved Porkbun
    /// credential never leaks the literal secret anywhere except this
    /// typed, ephemeral config-object — closing the
    /// `Pangea::Secrets.resolve`-at-synth-time leak the CRD schema gap
    /// used to force `platform_dns.rb` into.
    #[test]
    fn porkbun_config_is_with_provider_config_shaped() {
        let d = data(&[("api_key", "pk1_live"), ("secret_api_key", "sk1_live")]);
        let obj = provider_config_object(ProviderKind::Porkbun, &d, None).unwrap();
        let expected = serde_json::json!({ "api_key": "pk1_live", "secret_api_key": "sk1_live" });
        assert_eq!(obj, expected);
    }

    /// The load-bearing assertion: a resolved Cloudflare credential
    /// produces exactly the `with_provider_config`-shaped
    /// `{api_token: …}` value the magma ApplyContext forwards to the
    /// cloudflare provider's ConfigureProvider RPC — closing the rio
    /// "channel closed" root cause.
    #[test]
    fn cloudflare_config_is_with_provider_config_shaped() {
        let d = data(&[("CF_API_TOKEN", "live-token")]);
        let obj = provider_config_object(ProviderKind::Cloudflare, &d, None).unwrap();
        let expected = serde_json::json!({ "api_token": "live-token" });
        assert_eq!(obj, expected);
    }

    #[test]
    fn merge_base_only_when_no_rendered_block() {
        // rio's case: spec.providerCredentials base, NO rendered provider block.
        let base = serde_json::json!({ "api_token": "live-token" });
        let merged = merge_provider_config(&base, None);
        assert_eq!(merged, serde_json::json!({ "api_token": "live-token" }));
    }

    #[test]
    fn merge_rendered_tuning_augments_base_credential() {
        // Base supplies the credential; rendered supplies non-secret tuning.
        let base = serde_json::json!({ "api_token": "live-token" });
        let rendered = serde_json::json!({ "account_id": "acct-123" });
        let merged = merge_provider_config(&base, Some(&rendered));
        assert_eq!(merged["api_token"], "live-token");
        assert_eq!(merged["account_id"], "acct-123");
    }

    #[test]
    fn merge_rendered_key_wins_on_conflict() {
        let base = serde_json::json!({ "region": "us-east-1", "access_key": "AKID" });
        let rendered = serde_json::json!({ "region": "eu-west-1" });
        let merged = merge_provider_config(&base, Some(&rendered));
        // Rendered author tuning wins for a non-credential field.
        assert_eq!(merged["region"], "eu-west-1");
        assert_eq!(merged["access_key"], "AKID");
    }

    #[test]
    fn merge_empty_rendered_value_never_clobbers_base_credential() {
        let base = serde_json::json!({ "api_token": "live-token" });
        // A rendered block with an empty/null token must NOT erase the
        // real base credential.
        let rendered = serde_json::json!({ "api_token": "" });
        let merged = merge_provider_config(&base, Some(&rendered));
        assert_eq!(merged["api_token"], "live-token");

        let rendered_null = serde_json::json!({ "api_token": null });
        let merged_null = merge_provider_config(&base, Some(&rendered_null));
        assert_eq!(merged_null["api_token"], "live-token");
    }
}
