//! PostgreSQL connection management.

use crate::crd::PostgresBackendConfig;
use crate::error::{Error, Result};
use super::Credentials;

use sqlx::postgres::{PgConnectOptions, PgPoolOptions, PgSslMode};
use sqlx::PgPool;
use std::str::FromStr;
use std::time::Duration;
use tracing::{debug, info};

/// PostgreSQL backend connection handler.
pub struct PostgresBackend;

impl PostgresBackend {
    /// Connect to PostgreSQL with the given configuration.
    pub async fn connect(config: &PostgresBackendConfig, credentials: Credentials) -> Result<PgPool> {
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

    #[test]
    fn test_parse_ssl_mode() {
        assert!(matches!(parse_ssl_mode("disable").unwrap(), PgSslMode::Disable));
        assert!(matches!(parse_ssl_mode("require").unwrap(), PgSslMode::Require));
        assert!(matches!(parse_ssl_mode("verify-full").unwrap(), PgSslMode::VerifyFull));
        assert!(parse_ssl_mode("invalid").is_err());
    }
}
