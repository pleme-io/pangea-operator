//! PostgreSQL connection management.

use super::Credentials;
use crate::crd::PostgresBackendConfig;
use crate::error::{Error, Result};

use sqlx::postgres::{PgConnectOptions, PgPoolOptions, PgSslMode};
use sqlx::PgPool;
use std::time::Duration;
use tracing::{debug, info};

/// PostgreSQL backend connection handler.
pub struct PostgresBackend;

impl PostgresBackend {
    /// Connect to PostgreSQL with the given configuration.
    pub async fn connect(
        config: &PostgresBackendConfig,
        credentials: Credentials,
    ) -> Result<PgPool> {
        let ssl_mode = parse_ssl_mode(&config.ssl_mode)?;

        let mut connect_options = PgConnectOptions::new()
            .host(&config.host)
            .port(config.port)
            .database(&config.database)
            .username(&credentials.username)
            .password(&credentials.password)
            .ssl_mode(ssl_mode);

        // Add CA certificate if provided
        if let Some(ca_cert) = &credentials.ca_cert {
            // Write CA cert to temp file for sqlx
            let temp_path = std::env::temp_dir().join("pangea_ca_cert.pem");
            std::fs::write(&temp_path, ca_cert)?;
            connect_options = connect_options.ssl_root_cert(&temp_path);
        }

        // Configure pool settings
        let pool_config = config.pool.as_ref();
        let min_connections = pool_config.map(|p| p.min_connections).unwrap_or(2);
        let max_connections = pool_config.map(|p| p.max_connections).unwrap_or(10);
        let acquire_timeout = pool_config.map(|p| p.acquire_timeout_secs).unwrap_or(30);
        let idle_timeout = pool_config.map(|p| p.idle_timeout_secs).unwrap_or(600);

        debug!(
            host = %config.host,
            port = %config.port,
            database = %config.database,
            min_connections,
            max_connections,
            "Connecting to PostgreSQL"
        );

        let pool = PgPoolOptions::new()
            .min_connections(min_connections)
            .max_connections(max_connections)
            .acquire_timeout(Duration::from_secs(acquire_timeout))
            .idle_timeout(Duration::from_secs(idle_timeout))
            .connect_with(connect_options)
            .await
            .map_err(|e| Error::Database(e))?;

        // Verify connection
        sqlx::query("SELECT 1")
            .execute(&pool)
            .await
            .map_err(|e| Error::Database(e))?;

        info!(
            host = %config.host,
            database = %config.database,
            "Connected to PostgreSQL"
        );

        Ok(pool)
    }

    /// Build a connection URL (for OpenTofu backend config).
    pub fn build_connection_url(
        config: &PostgresBackendConfig,
        credentials: &Credentials,
    ) -> String {
        format!(
            "postgres://{}:{}@{}:{}/{}?sslmode={}",
            urlencoding::encode(&credentials.username),
            urlencoding::encode(&credentials.password),
            config.host,
            config.port,
            config.database,
            config.ssl_mode
        )
    }
}

/// Parse SSL mode string to sqlx PgSslMode.
fn parse_ssl_mode(mode: &str) -> Result<PgSslMode> {
    match mode.to_lowercase().as_str() {
        "disable" => Ok(PgSslMode::Disable),
        "allow" => Ok(PgSslMode::Allow),
        "prefer" => Ok(PgSslMode::Prefer),
        "require" => Ok(PgSslMode::Require),
        "verify-ca" | "verify_ca" => Ok(PgSslMode::VerifyCa),
        "verify-full" | "verify_full" => Ok(PgSslMode::VerifyFull),
        _ => Err(Error::Config(format!("Invalid SSL mode: {}", mode))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_pg_config() -> PostgresBackendConfig {
        PostgresBackendConfig {
            host: "db.example.com".to_string(),
            port: 5432,
            database: "pangea".to_string(),
            schema_prefix: "pangea_".to_string(),
            ssl_mode: "require".to_string(),
            secret_ref: Some(crate::crd::PostgresSecretRef {
                name: "db-secret".to_string(),
                namespace: None,
                username_key: "username".to_string(),
                password_key: "password".to_string(),
                ca_cert_key: None,
            }),
            user: None,
            pool: None,
        }
    }

    #[test]
    fn test_parse_ssl_mode() {
        assert!(matches!(
            parse_ssl_mode("disable").unwrap(),
            PgSslMode::Disable
        ));
        assert!(matches!(
            parse_ssl_mode("require").unwrap(),
            PgSslMode::Require
        ));
        assert!(matches!(
            parse_ssl_mode("verify-full").unwrap(),
            PgSslMode::VerifyFull
        ));
        assert!(parse_ssl_mode("invalid").is_err());
    }

    #[test]
    fn test_parse_ssl_mode_all_variants() {
        assert!(matches!(
            parse_ssl_mode("disable").unwrap(),
            PgSslMode::Disable
        ));
        assert!(matches!(parse_ssl_mode("allow").unwrap(), PgSslMode::Allow));
        assert!(matches!(
            parse_ssl_mode("prefer").unwrap(),
            PgSslMode::Prefer
        ));
        assert!(matches!(
            parse_ssl_mode("require").unwrap(),
            PgSslMode::Require
        ));
        assert!(matches!(
            parse_ssl_mode("verify-ca").unwrap(),
            PgSslMode::VerifyCa
        ));
        assert!(matches!(
            parse_ssl_mode("verify_ca").unwrap(),
            PgSslMode::VerifyCa
        ));
        assert!(matches!(
            parse_ssl_mode("verify-full").unwrap(),
            PgSslMode::VerifyFull
        ));
        assert!(matches!(
            parse_ssl_mode("verify_full").unwrap(),
            PgSslMode::VerifyFull
        ));
    }

    #[test]
    fn test_parse_ssl_mode_case_insensitive() {
        assert!(matches!(
            parse_ssl_mode("REQUIRE").unwrap(),
            PgSslMode::Require
        ));
        assert!(matches!(
            parse_ssl_mode("Disable").unwrap(),
            PgSslMode::Disable
        ));
        assert!(matches!(
            parse_ssl_mode("VERIFY-FULL").unwrap(),
            PgSslMode::VerifyFull
        ));
    }

    #[test]
    fn test_parse_ssl_mode_empty_string() {
        assert!(parse_ssl_mode("").is_err());
    }

    #[test]
    fn test_build_connection_url() {
        let config = make_pg_config();
        let credentials = Credentials::new("admin", "secret");
        let url = PostgresBackend::build_connection_url(&config, &credentials);
        assert!(url.starts_with("postgres://"));
        assert!(url.contains("admin"));
        assert!(url.contains("secret"));
        assert!(url.contains("db.example.com:5432"));
        assert!(url.contains("pangea"));
        assert!(url.contains("sslmode=require"));
    }

    #[test]
    fn test_build_connection_url_encodes_special_chars() {
        let config = make_pg_config();
        let credentials = Credentials::new("user@domain", "p@ss:word/123");
        let url = PostgresBackend::build_connection_url(&config, &credentials);
        assert!(url.contains("user%40domain"));
        assert!(url.contains("p%40ss%3Aword%2F123"));
    }

    #[test]
    fn test_credentials_new() {
        let creds = Credentials::new("user", "pass");
        assert_eq!(creds.username, "user");
        assert_eq!(creds.password, "pass");
        assert!(creds.ca_cert.is_none());
    }

    #[test]
    fn test_credentials_with_ca_cert() {
        let creds = Credentials::new("user", "pass")
            .with_ca_cert("-----BEGIN CERTIFICATE-----\nMIIC...\n-----END CERTIFICATE-----");
        assert!(creds.ca_cert.is_some());
        assert!(creds.ca_cert.unwrap().contains("BEGIN CERTIFICATE"));
    }
}
