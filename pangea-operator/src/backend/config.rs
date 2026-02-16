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
    pub fn generate_provider_config(
        aws_region: Option<&str>,
        aws_credentials: Option<&AwsCredentialsConfig>,
        cloudflare_credentials: Option<&CloudflareCredentialsConfig>,
    ) -> serde_json::Value {
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

        serde_json::json!({
            "provider": providers
        })
    }

    /// Write provider configuration to a file.
    pub async fn write_provider_config(
        config: serde_json::Value,
        work_dir: &Path,
    ) -> Result<()> {
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

    #[test]
    fn test_build_conn_str() {
        let pg = PostgresBackendConfig {
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
        };

        let credentials = Credentials::new("user", "pass@word");

        let conn_str = build_conn_str(&pg, &credentials);

        assert!(conn_str.contains("postgres://"));
        assert!(conn_str.contains("localhost:5432"));
        assert!(conn_str.contains("pangea_state"));
        assert!(conn_str.contains("pass%40word")); // URL encoded
    }
}
