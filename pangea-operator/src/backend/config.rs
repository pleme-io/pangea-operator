//! OpenTofu backend configuration generator.
//!
//! Generates the backend configuration files needed for OpenTofu
//! to use PostgreSQL as its state backend.

use super::Credentials;
use crate::crd::{PangeaNamespace, PostgresBackendConfig};
use crate::error::Result;
use std::path::Path;
use tracing::debug;

/// Generates OpenTofu backend configuration.
pub struct BackendConfigGenerator;

impl BackendConfigGenerator {
    /// Generate backend configuration for a template.
    ///
    /// Creates a backend.tf.json file with PostgreSQL backend settings.
    pub fn generate_backend_config(
        namespace: &PangeaNamespace,
        template_name: &str,
        credentials: &Credentials,
    ) -> Result<serde_json::Value> {
        let pg = namespace.spec.backend.pg.as_ref().ok_or_else(|| {
            crate::error::Error::Config("Missing PostgreSQL configuration".into())
        })?;

        let schema_name = namespace.schema_name();
        let conn_str = build_conn_str(pg, credentials);

        debug!(
            schema_name,
            template_name, "Generating backend configuration"
        );

        // Generate backend configuration matching OpenTofu pg backend
        let config = serde_json::json!({
            "terraform": {
                "backend": {
                    "pg": {
                        "conn_str": conn_str,
                        "schema_name": format!("{}_{}_states", schema_name, template_name)
                    }
                }
            }
        });

        Ok(config)
    }

    /// Write backend configuration to a file.
    pub async fn write_backend_config(
        namespace: &PangeaNamespace,
        template_name: &str,
        credentials: &Credentials,
        work_dir: &Path,
    ) -> Result<()> {
        let config = Self::generate_backend_config(namespace, template_name, credentials)?;

        let config_path = work_dir.join("backend.tf.json");
        let content = serde_json::to_string_pretty(&config)
            .map_err(|e| crate::error::Error::Serialization(e))?;

        tokio::fs::write(&config_path, content)
            .await
            .map_err(|e| crate::error::Error::Io(e))?;

        debug!(?config_path, "Backend configuration written");
        Ok(())
    }

    /// Generate provider configuration with credentials.
    ///
    /// Returns `None` when no operator-side provider block is needed —
    /// e.g. a workspace whose only declared provider is GitHub, where
    /// the Ruby renderer's `provider :github do { token gh_token }`
    /// block already inlines the credential via the env-var injection
    /// path. In that case writing `{"provider": {}}` would be invalid
    /// tofu syntax (rejected at `tofu init` with "Missing block label
    /// — at least one object property is required").
    ///
    /// Returns `Some(json)` when at least one provider block must be
    /// rendered into the operator's `providers.tf.json` (separate from
    /// any provider blocks the Ruby DSL emits in workspace.tf.json).
    ///
    /// **Operator-side vs Ruby-side provider authority** is a typed
    /// per-`ProviderKind` decision — see
    /// `ProviderKind::operator_emits_provider_block()`. Adding a new
    /// `ProviderKind` variant forces a typed answer; "I forgot to
    /// extend the operator emitter" is a compile error rather than a
    /// runtime tofu-init wedge.
    pub fn generate_provider_config(
        aws_region: Option<&str>,
        aws_credentials: Option<&AwsCredentialsConfig>,
        cloudflare_credentials: Option<&CloudflareCredentialsConfig>,
        porkbun_credentials: Option<&PorkbunCredentialsConfig>,
        github_app_credentials: Option<&GitHubAppCredentialsConfig>,
    ) -> Option<serde_json::Value> {
        let mut providers = serde_json::Map::new();

        if let Some(region) = aws_region {
            let mut aws_config = serde_json::Map::new();
            aws_config.insert("region".to_string(), serde_json::json!(region));

            if let Some(creds) = aws_credentials {
                aws_config.insert(
                    "access_key".to_string(),
                    serde_json::json!(creds.access_key),
                );
                aws_config.insert(
                    "secret_key".to_string(),
                    serde_json::json!(creds.secret_key),
                );
                if let Some(token) = &creds.session_token {
                    aws_config.insert("token".to_string(), serde_json::json!(token));
                }
            }

            providers.insert("aws".to_string(), serde_json::Value::Object(aws_config));
        }

        if let Some(creds) = cloudflare_credentials {
            let mut cf_config = serde_json::Map::new();
            cf_config.insert("api_token".to_string(), serde_json::json!(creds.api_token));

            providers.insert(
                "cloudflare".to_string(),
                serde_json::Value::Object(cf_config),
            );
        }

        if let Some(creds) = porkbun_credentials {
            let mut pb_config = serde_json::Map::new();
            pb_config.insert("api_key".to_string(), serde_json::json!(creds.api_key));
            pb_config.insert(
                "secret_api_key".to_string(),
                serde_json::json!(creds.secret_api_key),
            );

            providers.insert("porkbun".to_string(), serde_json::Value::Object(pb_config));
        }

        // ── ★ GITHUB: EMITTED ONLY WHEN APP CREDENTIALS ARE PRESENT ──────────
        // The authority for this kind is CONDITIONAL, and the condition is the
        // credential's SHAPE rather than a static per-kind flag — which is what
        // makes the two authorities mutually exclusive by construction instead
        // of by convention.
        //
        //   App credentials present  -> the operator renders `app_auth` here and
        //                               the Ruby block must be absent, which it
        //                               is: github_org_workspace.rb emits its
        //                               `provider :github` only when GITHUB_TOKEN
        //                               is set, and the App path deletes that env
        //                               var rather than repointing it.
        //   App credentials absent   -> nothing is emitted and Ruby's token block
        //                               remains authoritative. Byte-identical to
        //                               the previous behaviour.
        //
        // Emitting unconditionally is the hazard `operator_emits_provider_block`
        // documents: two provider blocks for one provider is not a merge, it is
        // conflicting definitions, and the failure lands at tofu-init rather than
        // where the mistake was made.
        if let Some(creds) = github_app_credentials {
            let mut auth = serde_json::Map::new();
            auth.insert("id".to_string(), serde_json::json!(creds.app_id));
            auth.insert(
                "installation_id".to_string(),
                serde_json::json!(creds.installation_id),
            );
            auth.insert("pem_file".to_string(), serde_json::json!(creds.pem));

            let mut gh_config = serde_json::Map::new();
            // A provider BLOCK renders as an ARRAY of objects in provider JSON
            // even where the schema permits exactly one — the same shape
            // provider_creds.rs emits, kept identical on purpose so the two
            // cannot drift into producing different documents.
            gh_config.insert(
                "app_auth".to_string(),
                serde_json::Value::Array(vec![serde_json::Value::Object(auth)]),
            );

            providers.insert("github".to_string(), serde_json::Value::Object(gh_config));
        }

        if providers.is_empty() {
            None
        } else {
            Some(serde_json::json!({
                "provider": providers
            }))
        }
    }

    /// Write provider configuration to a file.
    ///
    /// Skips the write entirely when `config` is `None` (no operator-
    /// side provider blocks to emit). This is the load-bearing safety
    /// over the previous unconditional write that produced
    /// `{"provider": {}}` for workspaces with only Ruby-handled
    /// providers (GitHub-only orgs, etc.) — invalid tofu syntax that
    /// wedged reconciliation indefinitely.
    pub async fn write_provider_config(
        config: Option<serde_json::Value>,
        work_dir: &Path,
    ) -> Result<()> {
        let Some(config) = config else {
            debug!("No operator-side provider config to write (Ruby-handled providers only)");
            return Ok(());
        };

        let config_path = work_dir.join("providers.tf.json");
        let content = serde_json::to_string_pretty(&config)
            .map_err(|e| crate::error::Error::Serialization(e))?;

        // This file carries live cloud credentials — the AWS access key,
        // secret key and session token, the Cloudflare API token, the
        // Porkbun key pair. `tokio::fs::write` creates at 0666 & ~umask,
        // which on the operator image (umask 022) is a 0644 file holding
        // an AWS secret key for as long as the workspace lives. cofre-fs
        // sets the mode in the same syscall that creates the file, so
        // there is no interval in which it is readable by anyone else.
        //
        // Synchronous inside an async fn deliberately: this is one small
        // write plus its fsync, and the alternative (spawn_blocking over a
        // borrowed path) buys nothing on a reconcile-latency budget
        // measured in seconds.
        cofre_fs::write_secret(&config_path, content.as_bytes(), 0o600)
            .map_err(|e| crate::error::Error::Io(e))?;

        debug!(?config_path, "Provider configuration written");
        Ok(())
    }
}

/// AWS credentials for provider configuration.
#[derive(Debug, Clone)]
pub struct AwsCredentialsConfig {
    pub access_key: String,
    pub secret_key: String,
    pub session_token: Option<String>,
}

/// Cloudflare credentials for provider configuration.
#[derive(Debug, Clone)]
pub struct CloudflareCredentialsConfig {
    pub api_token: String,
}

/// Porkbun credentials for provider configuration. Both attrs are
/// required by the `marcfrederick/porkbun` terraform provider schema.
#[derive(Debug, Clone)]
pub struct PorkbunCredentialsConfig {
    pub api_key: String,
    pub secret_api_key: String,
}

/// GitHub App credentials for the `github` provider's native `app_auth`.
///
/// ── ★ WHY AN APP AND NOT A TOKEN ──────────────────────────────────────
/// A token is a value with an expiry; an App key is a capability without
/// one. Carrying the App credentials lets the PROVIDER mint and refresh
/// its own installation tokens, so nothing in this system has to hold a
/// credential that dies on a date.
///
/// That is not a preference. The token this replaces reached the provider
/// as a pod ENV VAR, and an env `secretKeyRef` resolves exactly once at
/// container start — measured 2026-08-23, a running operator pod had been
/// carrying a value injected sixteen days earlier, which had since been
/// revoked. No refresh interval anywhere can fix that shape: the only
/// options are a credential that never expires, or a restart on every
/// rotation. This type is the first.
#[derive(Debug, Clone)]
pub struct GitHubAppCredentialsConfig {
    pub app_id: String,
    pub installation_id: String,
    /// Full PEM. Written to the ephemeral pod-local providers.tf.json and
    /// never to the workspace the Ruby renderer sees.
    pub pem: String,
}

/// Build PostgreSQL connection string.
fn build_conn_str(pg: &PostgresBackendConfig, credentials: &Credentials) -> String {
    format!(
        "postgres://{}:{}@{}:{}/{}?sslmode={}",
        urlencoding::encode(&credentials.username),
        urlencoding::encode(&credentials.password),
        pg.host,
        pg.port,
        pg.database,
        pg.ssl_mode
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_pg_config() -> PostgresBackendConfig {
        PostgresBackendConfig {
            host: "localhost".to_string(),
            port: 5432,
            database: "pangea_state".to_string(),
            schema_prefix: "pangea_".to_string(),
            ssl_mode: "require".to_string(),
            secret_ref: crate::crd::PostgresSecretRef {
                name: "test".to_string(),
                namespace: None,
                username_key: "username".to_string(),
                password_key: "password".to_string(),
                ca_cert_key: None,
            },
            pool: None,
        }
    }

    #[test]
    fn test_build_conn_str() {
        let pg = make_pg_config();
        let credentials = Credentials::new("user", "pass@word");
        let conn_str = build_conn_str(&pg, &credentials);

        assert!(conn_str.contains("postgres://"));
        assert!(conn_str.contains("localhost:5432"));
        assert!(conn_str.contains("pangea_state"));
        assert!(conn_str.contains("pass%40word")); // URL encoded
    }

    #[test]
    fn test_build_conn_str_special_chars_in_username() {
        let pg = make_pg_config();
        let credentials = Credentials::new("user@host/db", "password");
        let conn_str = build_conn_str(&pg, &credentials);
        assert!(conn_str.contains("user%40host%2Fdb"));
    }

    #[test]
    fn test_generate_provider_config_aws_only() {
        let config = BackendConfigGenerator::generate_provider_config(
            Some("us-east-1"),
            Some(&AwsCredentialsConfig {
                access_key: "AKID123".to_string(),
                secret_key: "secret456".to_string(),
                session_token: None,
            }),
            None,
            None,
            None,
        )
        .expect("aws config produces a non-empty provider block");

        let aws = &config["provider"]["aws"];
        assert_eq!(aws["region"], "us-east-1");
        assert_eq!(aws["access_key"], "AKID123");
        assert_eq!(aws["secret_key"], "secret456");
        assert!(aws.get("token").is_none());
    }

    #[test]
    fn test_generate_provider_config_aws_with_session_token() {
        let config = BackendConfigGenerator::generate_provider_config(
            Some("us-west-2"),
            Some(&AwsCredentialsConfig {
                access_key: "AKID".to_string(),
                secret_key: "SK".to_string(),
                session_token: Some("TOKEN123".to_string()),
            }),
            None,
            None,
            None,
        )
        .expect("aws config produces a non-empty provider block");

        assert_eq!(config["provider"]["aws"]["token"], "TOKEN123");
    }

    #[test]
    fn test_generate_provider_config_cloudflare_only() {
        let config = BackendConfigGenerator::generate_provider_config(
            None,
            None,
            Some(&CloudflareCredentialsConfig {
                api_token: "cf-token-xyz".to_string(),
            }),
            None,
            None,
        )
        .expect("cloudflare config produces a non-empty provider block");

        assert_eq!(
            config["provider"]["cloudflare"]["api_token"],
            "cf-token-xyz"
        );
        assert!(config["provider"].get("aws").is_none());
    }

    #[test]
    fn test_generate_provider_config_both_providers() {
        let config = BackendConfigGenerator::generate_provider_config(
            Some("eu-west-1"),
            Some(&AwsCredentialsConfig {
                access_key: "AK".to_string(),
                secret_key: "SK".to_string(),
                session_token: None,
            }),
            Some(&CloudflareCredentialsConfig {
                api_token: "cf-token".to_string(),
            }),
            None,
            None,
        )
        .expect("two configs produce a non-empty provider block");

        assert!(config["provider"]["aws"].is_object());
        assert!(config["provider"]["cloudflare"].is_object());
    }

    #[test]
    fn test_generate_provider_config_porkbun_only() {
        let config = BackendConfigGenerator::generate_provider_config(
            None,
            None,
            None,
            Some(&PorkbunCredentialsConfig {
                api_key: "pk1_abc".to_string(),
                secret_api_key: "sk1_xyz".to_string(),
            }),
            None,
        )
        .expect("porkbun config produces a non-empty provider block");

        assert_eq!(config["provider"]["porkbun"]["api_key"], "pk1_abc");
        assert_eq!(config["provider"]["porkbun"]["secret_api_key"], "sk1_xyz");
        assert!(config["provider"].get("aws").is_none());
        assert!(config["provider"].get("cloudflare").is_none());
    }

    #[test]
    fn test_generate_provider_config_all_three_providers() {
        let config = BackendConfigGenerator::generate_provider_config(
            Some("eu-west-1"),
            Some(&AwsCredentialsConfig {
                access_key: "AK".to_string(),
                secret_key: "SK".to_string(),
                session_token: None,
            }),
            Some(&CloudflareCredentialsConfig {
                api_token: "cf-token".to_string(),
            }),
            Some(&PorkbunCredentialsConfig {
                api_key: "pk1_abc".to_string(),
                secret_api_key: "sk1_xyz".to_string(),
            }),
            None,
        )
        .expect("three configs produce a non-empty provider block");

        assert!(config["provider"]["aws"].is_object());
        assert!(config["provider"]["cloudflare"].is_object());
        assert!(config["provider"]["porkbun"].is_object());
    }

    /// Regression for the pleme-io-opensource wedge.
    ///
    /// Workspaces whose only declared providerCredentials is GitHub
    /// (operator-emits-provider-block = false; the Ruby DSL handles it)
    /// must NOT cause the operator to write an empty
    /// `{"provider": {}}` block — that's invalid tofu syntax and was
    /// the root cause of the 2026-05-05 reconciliation wedge. The
    /// function now returns None and `write_provider_config` skips the
    /// file entirely.
    #[test]
    fn test_generate_provider_config_no_providers_returns_none() {
        let config = BackendConfigGenerator::generate_provider_config(None, None, None, None, None);
        assert!(
            config.is_none(),
            "no operator-side providers must produce None, not an empty block"
        );
    }

    #[test]
    fn test_generate_provider_config_region_without_creds() {
        let config = BackendConfigGenerator::generate_provider_config(
            Some("ap-southeast-1"),
            None,
            None,
            None,
            None,
        )
        .expect("region-only AWS still emits a provider block");
        let aws = &config["provider"]["aws"];
        assert_eq!(aws["region"], "ap-southeast-1");
        assert!(aws.get("access_key").is_none());
        assert!(aws.get("secret_key").is_none());
    }
}

#[cfg(test)]
mod github_app_provider_tests {
    use super::*;

    #[test]
    fn absent_app_credentials_emit_no_github_block_at_all() {
        // ★ THE LOAD-BEARING CASE. While Ruby's `provider :github` block is
        // authoritative, the operator emitting ANYTHING for github — even an
        // empty object — is two definitions of one provider, which fails at
        // tofu-init rather than where the mistake was made.
        let config = BackendConfigGenerator::generate_provider_config(
            Some("us-east-2"),
            None,
            None,
            None,
            None,
        )
        .expect("aws region alone still produces a provider block");
        assert!(
            config["provider"].get("github").is_none(),
            "no github key may appear when App credentials are absent, got {config}"
        );
    }

    #[test]
    fn app_credentials_emit_app_auth_as_an_array() {
        // ★ A DELIBERATELY NON-PEM SENTINEL. The property under test is that
        // the key material travels WHOLE and unmodified into the rendered
        // document — not that it looks like a PEM. Using real PEM framing here
        // would put credential-shaped bytes in the source tree for no added
        // coverage, and the repo's pre-commit guard rightly refuses those:
        // a value that reaches a commit is unrecoverable by force-push.
        let opaque = "sentinel-key-material-\n-with-a-newline-in-it";
        let creds = GitHubAppCredentialsConfig {
            app_id: "4683416".to_string(),
            installation_id: "155727405".to_string(),
            pem: opaque.to_string(),
        };
        let config =
            BackendConfigGenerator::generate_provider_config(None, None, None, None, Some(&creds))
                .expect("app credentials alone produce a provider block");
        let auth = &config["provider"]["github"]["app_auth"];
        // ARRAY, not object: a provider block renders as an array of objects in
        // provider JSON even where exactly one is permitted. An object here
        // parses and then silently configures nothing.
        assert!(auth.is_array(), "app_auth must be an array, got {auth}");
        assert_eq!(auth[0]["id"], "4683416");
        assert_eq!(auth[0]["installation_id"], "155727405");
        // Whole and byte-identical — including the embedded newline, which is
        // the part a naive single-line serializer would truncate. The field is
        // named pem_FILE but the provider takes the material itself.
        assert_eq!(
            auth[0]["pem_file"].as_str().unwrap(),
            opaque,
            "the key material must travel whole and unmodified, not a path or a prefix"
        );
    }

    #[test]
    fn github_app_credentials_do_not_disturb_other_providers() {
        let creds = GitHubAppCredentialsConfig {
            app_id: "1".to_string(),
            installation_id: "2".to_string(),
            pem: "pem".to_string(),
        };
        let config = BackendConfigGenerator::generate_provider_config(
            Some("us-east-2"),
            None,
            None,
            None,
            Some(&creds),
        )
        .expect("both present");
        assert_eq!(config["provider"]["aws"]["region"], "us-east-2");
        assert!(config["provider"]["github"]["app_auth"].is_array());
    }
}
