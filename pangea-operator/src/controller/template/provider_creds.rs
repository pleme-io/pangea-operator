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
    let default_ns = template
        .namespace()
        .unwrap_or_else(|| "default".to_string());

    let aws_creds = if let Some(aws) = &provider_creds.aws {
        let ns = aws.secret_ref.namespace.as_deref().unwrap_or(&default_ns);
        let secret_api: Api<Secret> = Api::namespaced(state.client.clone(), ns);
        let secret =
            secret_api
                .get(&aws.secret_ref.name)
                .await
                .map_err(|_| Error::SecretNotFound {
                    namespace: ns.to_string(),
                    name: aws.secret_ref.name.clone(),
                })?;

        let data = secret
            .data
            .as_ref()
            .ok_or_else(|| Error::Config("AWS credentials secret has no data".into()))?;

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
        let ns = cf.secret_ref.namespace.as_deref().unwrap_or(&default_ns);
        let secret_api: Api<Secret> = Api::namespaced(state.client.clone(), ns);
        let secret =
            secret_api
                .get(&cf.secret_ref.name)
                .await
                .map_err(|_| Error::SecretNotFound {
                    namespace: ns.to_string(),
                    name: cf.secret_ref.name.clone(),
                })?;

        let data = secret
            .data
            .as_ref()
            .ok_or_else(|| Error::Config("Cloudflare credentials secret has no data".into()))?;

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
        let ns = pb.secret_ref.namespace.as_deref().unwrap_or(&default_ns);
        let secret_api: Api<Secret> = Api::namespaced(state.client.clone(), ns);
        let secret =
            secret_api
                .get(&pb.secret_ref.name)
                .await
                .map_err(|_| Error::SecretNotFound {
                    namespace: ns.to_string(),
                    name: pb.secret_ref.name.clone(),
                })?;

        let data = secret
            .data
            .as_ref()
            .ok_or_else(|| Error::Config("Porkbun credentials secret has no data".into()))?;

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
fn first_present(data: &BTreeMap<String, ByteString>, candidates: &[&str]) -> Option<String> {
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
            let api_token =
                first_present(data, &["api_token", "CLOUDFLARE_API_TOKEN", "CF_API_TOKEN"])?;
            let mut obj = serde_json::Map::new();
            obj.insert(
                "api_token".to_string(),
                serde_json::Value::String(api_token),
            );
            Some(serde_json::Value::Object(obj))
        }
        ProviderKind::Aws => {
            let mut obj = serde_json::Map::new();
            if let Some(region) = aws_region {
                obj.insert(
                    "region".to_string(),
                    serde_json::Value::String(region.to_string()),
                );
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
            let mut obj = serde_json::Map::new();

            // ── ★ GITHUB APP AUTH IS PREFERRED, AND THE OPERATOR MINTS NOTHING ──
            // The `github` Terraform provider carries a native `app_auth` block
            // and does the JWT signing + installation-token exchange itself. So
            // App support here is a PASS-THROUGH of three fields, not a token
            // minting loop in the operator — no expiry to track, no refresh, and
            // no bearer at rest in this process.
            //
            // Why it is preferred over a PAT, measured 2026-08-22:
            //   * A PAT is a PERSON. The credential this replaces was one
            //     operator's classic `repo` + `admin:org` token holding
            //     org-owner-equivalent control of ~997 repos — and it was found
            //     DEAD (401 on every endpoint), which is why the reconciler had
            //     been suspended.
            //   * Rate limit: an installation gets 12,500/hr against a user
            //     PAT's 5,000, and it scales with org size. That is the direct
            //     fix for the measured 5000/5000 starvation that queued 18 jobs
            //     for ~40 minutes with zero runners.
            //   * Revocation is uninstalling, not hunting for who minted what.
            //
            // `pem_file` is the provider's attribute name and takes the KEY
            // MATERIAL, not a path — the name is upstream's, and it is a trap
            // worth naming rather than rediscovering.
            let app = (
                first_present(data, &["app_id", "APP_ID", "GITHUB_APP_ID"]),
                first_present(
                    data,
                    &["installation_id", "INSTALLATION_ID", "GITHUB_APP_INSTALLATION_ID"],
                ),
                first_present(
                    data,
                    &["private_key", "pem_file", "PRIVATE_KEY", "GITHUB_APP_PEM_FILE"],
                ),
            );
            if let (Some(id), Some(installation_id), Some(pem)) = app {
                let mut auth = serde_json::Map::new();
                auth.insert("id".to_string(), serde_json::Value::String(id));
                auth.insert(
                    "installation_id".to_string(),
                    serde_json::Value::String(installation_id),
                );
                auth.insert("pem_file".to_string(), serde_json::Value::String(pem));
                // A provider BLOCK renders as an array of objects in provider
                // JSON, even when the schema permits exactly one.
                obj.insert(
                    "app_auth".to_string(),
                    serde_json::Value::Array(vec![serde_json::Value::Object(auth)]),
                );
            } else {
                // ★ PARTIAL APP CREDENTIALS FALL BACK RATHER THAN HALF-CONFIGURE.
                // Two of three fields is not App auth; emitting it would produce
                // a provider that fails at plan time with a schema error instead
                // of an auth error, which sends the reader to the wrong place.
                let token = first_present(data, &["token", "password", "GITHUB_TOKEN"])?;
                obj.insert("token".to_string(), serde_json::Value::String(token));
            }

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
        // Akeyless (2026-07-25): same shape as GitHub -- `operator_emits_
        // provider_block() == false` governs only the TOFU text-render path
        // (resolve_provider_config); it says nothing about this, the MAGMA
        // live-RPC-config path. Confirmed against
        // pleme-io/terraform-provider-akeyless/akeyless/provider.go: the
        // provider's real schema is a top-level `api_gateway_address`
        // string plus a NESTED `api_key_login` block (`access_id`/
        // `access_key`) -- both attrs already carry their own
        // `EnvDefaultFunc` fallback in the provider binary itself, but that
        // only helps if the magma-hosted process happens to inherit those
        // exact env vars; `with_provider_config` is the explicit, load-
        // bearing path this module's own doc comment already warns is
        // required (a null config here fails every real RPC). Per this
        // org's documented cty-typed encoding rule (a `max_items=1` nested
        // block serializes as a 1-element list of objects, theory/MAGMA.md
        // § ApplyRpcContract), `api_key_login` is wrapped in an array.
        // Datadog. The provider's config surface is FLAT -- api_key, app_key
        // and api_url are all top-level optional strings (verified against
        // DataDog/datadog 4.10.0's own schema), so none of the cty
        // nested-block wrapping Akeyless needs applies here.
        //
        // Both keys are required together: a config carrying one of them
        // authenticates nothing, and returning a half-built object would send
        // every RPC out to fail with a credential error rather than falling
        // back to the provider's own env defaults.
        ProviderKind::Datadog => {
            let api_key = first_present(data, &["api_key", "DD_API_KEY", "DATADOG_API_KEY"])?;
            let app_key = first_present(data, &["app_key", "DD_APP_KEY", "DATADOG_APP_KEY"])?;
            let mut obj = serde_json::Map::new();
            obj.insert("api_key".to_string(), serde_json::Value::String(api_key));
            obj.insert("app_key".to_string(), serde_json::Value::String(app_key));
            // Only a full URL. DD_SITE carries a bare hostname
            // (datadoghq.eu), and synthesising a URL from it here would be a
            // guess about a scheme and path the estate never stated.
            if let Some(url) = first_present(data, &["api_url", "DD_API_URL", "DATADOG_HOST"]) {
                obj.insert("api_url".to_string(), serde_json::Value::String(url));
            }
            Some(serde_json::Value::Object(obj))
        }
        ProviderKind::Akeyless => {
            let access_id = first_present(data, &["access_id", "AKEYLESS_ACCESS_ID"])?;
            let access_key = first_present(data, &["access_key", "AKEYLESS_ACCESS_KEY"])?;
            let mut login_obj = serde_json::Map::new();
            login_obj.insert(
                "access_id".to_string(),
                serde_json::Value::String(access_id),
            );
            login_obj.insert(
                "access_key".to_string(),
                serde_json::Value::String(access_key),
            );
            let mut obj = serde_json::Map::new();
            obj.insert(
                "api_key_login".to_string(),
                serde_json::Value::Array(vec![serde_json::Value::Object(login_obj)]),
            );
            // Real provider env fallback is `AKEYLESS_GATEWAY`; also accept
            // `AKEYLESS_API_GATEWAY` (this CRD's own doc-comment convention)
            // and the bare terraform attr name, tolerant-key style.
            if let Some(gw) = first_present(
                data,
                &[
                    "api_gateway_address",
                    "AKEYLESS_GATEWAY",
                    "AKEYLESS_API_GATEWAY",
                ],
            ) {
                obj.insert(
                    "api_gateway_address".to_string(),
                    serde_json::Value::String(gw),
                );
            }
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
    let default_ns = template
        .namespace()
        .unwrap_or_else(|| "default".to_string());
    let aws_region = provider_creds
        .aws
        .as_ref()
        .and_then(|a| a.region.as_deref());

    let mut out: BTreeMap<String, serde_json::Value> = BTreeMap::new();

    for (kind, sref) in provider_creds.iter_secret_refs() {
        let ns = sref.namespace.as_deref().unwrap_or(&default_ns);
        let secret_api: Api<Secret> = Api::namespaced(state.client.clone(), ns);
        let secret = secret_api
            .get(&sref.name)
            .await
            .map_err(|_| Error::SecretNotFound {
                namespace: ns.to_string(),
                name: sref.name.clone(),
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
    let mut merged: serde_json::Map<String, serde_json::Value> =
        base.as_object().cloned().unwrap_or_default();

    if let Some(serde_json::Value::Object(rendered_obj)) = rendered {
        for (k, v) in rendered_obj {
            // Rendered tuning wins — but a null/empty rendered value must
            // not erase a real base credential.
            let is_empty = v.is_null() || matches!(v, serde_json::Value::String(s) if s.is_empty());
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
    fn github_app_auth_is_preferred_over_a_token() {
        // All three App fields present AND a token: App wins. A PAT that
        // lingers in the secret must not silently keep the reconciler on the
        // person-tied credential the App exists to retire.
        let d = data(&[
            ("app_id", "4685620"),
            ("installation_id", "155780635"),
            ("private_key", "-----BEGIN RSA PRIVATE KEY-----\nx\n-----END RSA PRIVATE KEY-----"),
            ("token", "ghp_should_be_ignored"),
        ]);
        let obj = provider_config_object(ProviderKind::GitHub, &d, None).expect("resolves");
        let o = obj.as_object().unwrap();
        assert!(!o.contains_key("token"), "a bearer must not be emitted alongside app_auth");
        let auth = o["app_auth"].as_array().expect("app_auth renders as a block array");
        assert_eq!(auth.len(), 1);
        assert_eq!(auth[0]["id"], "4685620");
        assert_eq!(auth[0]["installation_id"], "155780635");
        assert!(auth[0]["pem_file"].as_str().unwrap().starts_with("-----BEGIN"));
    }

    #[test]
    fn github_partial_app_credentials_fall_back_rather_than_half_configure() {
        // ★ Two of three is NOT App auth. Emitting a partial app_auth block
        // produces a SCHEMA error at plan time instead of an auth error, which
        // sends the reader to entirely the wrong place.
        let d = data(&[
            ("app_id", "4685620"),
            ("installation_id", "155780635"),
            ("token", "ghp_yyy"),
        ]);
        let obj = provider_config_object(ProviderKind::GitHub, &d, None).expect("resolves");
        let o = obj.as_object().unwrap();
        assert!(!o.contains_key("app_auth"), "partial app creds must not emit a block");
        assert_eq!(o["token"], "ghp_yyy");
    }

    #[test]
    fn github_partial_app_credentials_with_no_token_yields_none() {
        // Nothing usable at all: None, not a half-built provider.
        let d = data(&[("app_id", "4685620")]);
        assert!(provider_config_object(ProviderKind::GitHub, &d, None).is_none());
    }

    #[test]
    fn github_app_tolerates_the_env_style_key_names() {
        // ESO projects Akeyless paths into env-shaped keys; both spellings
        // must resolve or the secret works in one delivery path and not the other.
        let d = data(&[
            ("GITHUB_APP_ID", "1"),
            ("GITHUB_APP_INSTALLATION_ID", "2"),
            ("GITHUB_APP_PEM_FILE", "pem"),
        ]);
        let obj = provider_config_object(ProviderKind::GitHub, &d, None).expect("resolves");
        assert_eq!(obj["app_auth"][0]["id"], "1");
    }

    #[test]
    fn github_token_only_still_works_unchanged() {
        // The pre-existing path must be byte-identical — every consumer that
        // has not moved to an App keeps working.
        let d = data(&[("GITHUB_TOKEN", "ghp_yyy"), ("owner", "pleme-io")]);
        let obj = provider_config_object(ProviderKind::GitHub, &d, None).expect("resolves");
        assert_eq!(obj["token"], "ghp_yyy");
        assert_eq!(obj["owner"], "pleme-io");
        assert!(obj.as_object().unwrap().get("app_auth").is_none());
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
    fn datadog_resolves_both_keys_into_a_flat_config() {
        let d = data(&[("api_key", "dd-api"), ("app_key", "dd-app")]);
        let obj = provider_config_object(ProviderKind::Datadog, &d, None)
            .expect("datadog creds present → config object");
        // FLAT, unlike akeyless: api_key/app_key/api_url are top-level
        // strings in the provider's own schema, so no cty list wrapping.
        assert_eq!(obj["api_key"], "dd-api");
        assert_eq!(obj["app_key"], "dd-app");
        assert!(obj.get("api_url").is_none());
    }

    #[test]
    fn datadog_accepts_the_env_style_key_names() {
        let d = data(&[("DD_API_KEY", "dd-api"), ("DD_APP_KEY", "dd-app")]);
        let obj = provider_config_object(ProviderKind::Datadog, &d, None)
            .expect("env-style keys are accepted too");
        assert_eq!(obj["api_key"], "dd-api");
        assert_eq!(obj["app_key"], "dd-app");
    }

    // Half a credential authenticates nothing. Returning a partial object
    // would send every RPC out to fail rather than letting the provider's own
    // env defaults apply.
    #[test]
    fn datadog_needs_both_keys_or_none() {
        let only_api = data(&[("api_key", "dd-api")]);
        assert!(provider_config_object(ProviderKind::Datadog, &only_api, None).is_none());

        let only_app = data(&[("app_key", "dd-app")]);
        assert!(provider_config_object(ProviderKind::Datadog, &only_app, None).is_none());
    }

    // DD_SITE carries a bare hostname; synthesising a URL from it would be a
    // guess about scheme and path the estate never stated. Only a full URL.
    #[test]
    fn datadog_carries_an_explicit_api_url_when_given_one() {
        let d = data(&[
            ("api_key", "k"),
            ("app_key", "a"),
            ("api_url", "https://api.datadoghq.eu/"),
        ]);
        let obj = provider_config_object(ProviderKind::Datadog, &d, None).expect("config object");
        assert_eq!(obj["api_url"], "https://api.datadoghq.eu/");
    }

    // Ruby-side authority: the absorbed workspace's shard entry points already
    // declare `provider :datadog` with ENV.fetch, so a parallel
    // operator-rendered block would collide with it.
    #[test]
    fn datadog_is_ruby_side_and_emits_no_provider_block() {
        assert!(!ProviderKind::Datadog.operator_emits_provider_block());
        assert_eq!(ProviderKind::Datadog.name(), "datadog");
    }

    #[test]
    fn akeyless_resolves_access_id_and_key_into_a_nested_api_key_login_block() {
        let d = data(&[("access_id", "p-abc123"), ("access_key", "sekret")]);
        let obj = provider_config_object(ProviderKind::Akeyless, &d, None)
            .expect("akeyless creds present → config object");
        // Nested-block encoding per this org's cty max_items=1 rule: a
        // 1-element list of objects, not a bare object.
        let expected = serde_json::json!({
            "api_key_login": [{ "access_id": "p-abc123", "access_key": "sekret" }],
        });
        assert_eq!(obj, expected);
    }

    #[test]
    fn akeyless_tolerant_env_var_key_fallbacks() {
        let d = data(&[
            ("AKEYLESS_ACCESS_ID", "p-env"),
            ("AKEYLESS_ACCESS_KEY", "sekret-env"),
        ]);
        let obj = provider_config_object(ProviderKind::Akeyless, &d, None)
            .expect("akeyless creds present via env-var-shaped keys → config object");
        assert_eq!(obj["api_key_login"][0]["access_id"], "p-env");
        assert_eq!(obj["api_key_login"][0]["access_key"], "sekret-env");
    }

    #[test]
    fn akeyless_optional_gateway_address_is_included_when_present() {
        let d = data(&[
            ("access_id", "p-1"),
            ("access_key", "k-1"),
            ("AKEYLESS_GATEWAY", "https://gw.internal:8080"),
        ]);
        let obj = provider_config_object(ProviderKind::Akeyless, &d, None).unwrap();
        assert_eq!(obj["api_gateway_address"], "https://gw.internal:8080");
    }

    #[test]
    fn akeyless_gateway_address_absent_when_not_present() {
        let d = data(&[("access_id", "p-1"), ("access_key", "k-1")]);
        let obj = provider_config_object(ProviderKind::Akeyless, &d, None).unwrap();
        assert!(obj.get("api_gateway_address").is_none());
    }

    #[test]
    fn akeyless_missing_access_key_yields_none() {
        // Both attrs are required by the provider schema — a partial
        // secret must not produce a half-populated login block.
        let d = data(&[("access_id", "p-1")]);
        assert!(provider_config_object(ProviderKind::Akeyless, &d, None).is_none());
    }

    #[test]
    fn akeyless_empty_yields_none() {
        let d = data(&[]);
        assert!(provider_config_object(ProviderKind::Akeyless, &d, None).is_none());
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
