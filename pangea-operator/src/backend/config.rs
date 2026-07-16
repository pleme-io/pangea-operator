//! OpenTofu backend configuration generator.
//!
//! Generates the backend configuration files needed for OpenTofu
//! to use PostgreSQL as its state backend.

use crate::crd::{PangeaNamespace, PostgresBackendConfig};
use crate::error::Result;
use super::Credentials;
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
        let pg = namespace
            .spec
            .backend
            .pg
            .as_ref()
            .ok_or_else(|| crate::error::Error::Config("Missing PostgreSQL configuration".into()))?;

        let schema_name = namespace.schema_name();
        let conn_str = build_conn_str(pg, credentials);

        debug!(
            schema_name,
            template_name,
            "Generating backend configuration"
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
    ) -> Option<serde_json::Value> {
        let mut providers = serde_json::Map::new();

        if let Some(region) = aws_region {
            let mut aws_config = serde_json::Map::new();
            aws_config.insert("region".to_string(), serde_json::json!(region));

            if let Some(creds) = aws_credentials {
                aws_config.insert("access_key".to_string(), serde_json::json!(creds.access_key));
                aws_config.insert("secret_key".to_string(), serde_json::json!(creds.secret_key));
                if let Some(token) = &creds.session_token {
                    aws_config.insert("token".to_string(), serde_json::json!(token));
                }
            }

            providers.insert("aws".to_string(), serde_json::Value::Object(aws_config));
        }

        if let Some(creds) = cloudflare_credentials {
            let mut cf_config = serde_json::Map::new();
            cf_config.insert("api_token".to_string(), serde_json::json!(creds.api_token));

            providers.insert("cloudflare".to_string(), serde_json::Value::Object(cf_config));
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

        // GitHub is intentionally absent: see ProviderKind::operator_emits_provider_block.
        // The Ruby renderer's `provider :github do …` block handles this kind, the
        // operator only needs to inject GITHUB_TOKEN as ENV (done in template_controller.rs).

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

        tokio::fs::write(&config_path, content)
            .await
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
        )
        .expect("cloudflare config produces a non-empty provider block");

        assert_eq!(config["provider"]["cloudflare"]["api_token"], "cf-token-xyz");
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
        let config = BackendConfigGenerator::generate_provider_config(None, None, None, None);
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
        )
        .expect("region-only AWS still emits a provider block");
        let aws = &config["provider"]["aws"];
        assert_eq!(aws["region"], "ap-southeast-1");
        assert!(aws.get("access_key").is_none());
        assert!(aws.get("secret_key").is_none());
    }
}
