//! PangeaNamespace CRD definition.
//!
//! Cluster-scoped resource that configures a Pangea namespace, including
//! the state backend (PostgreSQL) configuration and default settings.

use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// PangeaNamespace is a cluster-scoped resource that configures
/// a Pangea namespace with backend and default settings.
#[derive(CustomResource, Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[kube(
    group = "pangea.pleme.io",
    version = "v1alpha1",
    kind = "PangeaNamespace",
    status = "PangeaNamespaceStatus",
    shortname = "pns",
    category = "pangea",
    printcolumn = r#"{"name":"Backend","type":"string","jsonPath":".spec.backend.type"}"#,
    printcolumn = r#"{"name":"Templates","type":"integer","jsonPath":".status.templateCount"}"#,
    printcolumn = r#"{"name":"Age","type":"date","jsonPath":".metadata.creationTimestamp"}"#
)]
#[serde(rename_all = "camelCase")]
pub struct PangeaNamespaceSpec {
    /// Description of this Pangea namespace.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,

    /// State backend configuration.
    pub backend: BackendConfig,

    /// Default tags to apply to all resources in this namespace.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub default_tags: BTreeMap<String, String>,

    /// Namespace-default VARIABLES inherited by every template using this
    /// namespace — the OUTERMOST layer of the config-inheritance cascade (P1,
    /// Terragrunt `root.hcl` parity). Deep-merged under the workspace's and the
    /// template's variables (`controller::config_cascade`); a template inherits
    /// these fleet-wide defaults and overrides per key.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_variables: Option<BTreeMap<String, serde_json::Value>>,

    /// Default provider configuration for templates in this namespace.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_providers: Option<DefaultProviders>,

    /// Default compliance profiles for templates in this namespace.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub default_compliance_profiles: Vec<String>,

    /// When true, the namespace controller skips reconciliation entirely
    /// — no schema provisioning, no backend health verification, no
    /// status updates. Use to temporarily quiesce a single namespace
    /// without affecting siblings. Compose with the cluster-scoped
    /// `OperatorPolicy/default` for fleet-wide pause.
    #[serde(default)]
    pub suspend: bool,
}

/// State backend configuration.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct BackendConfig {
    /// Type of backend (pg, s3, local).
    pub r#type: BackendType,

    /// PostgreSQL backend configuration.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pg: Option<PostgresBackendConfig>,

    /// S3 backend configuration (legacy, for migration).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub s3: Option<S3BackendConfig>,
}

/// Type of state backend.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum BackendType {
    /// PostgreSQL backend (recommended).
    Pg,
    /// S3 + DynamoDB backend (legacy).
    S3,
    /// Local filesystem (development only).
    Local,
}

impl Default for BackendType {
    fn default() -> Self {
        BackendType::Pg
    }
}

/// PostgreSQL backend configuration.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct PostgresBackendConfig {
    /// PostgreSQL host.
    pub host: String,

    /// PostgreSQL port.
    #[serde(default = "default_pg_port")]
    pub port: u16,

    /// Database name.
    pub database: String,

    /// Schema name prefix (default: "pangea_").
    #[serde(default = "default_schema_prefix")]
    pub schema_prefix: String,

    /// SSL mode for connections.
    #[serde(default = "default_ssl_mode")]
    pub ssl_mode: String,

    /// Reference to a Secret containing credentials.
    ///
    /// ── ★ OPTIONAL, AND ONLY BECAUSE A SOCKET NEEDS NO PASSWORD ──────────
    /// Required for a TCP `host`. Optional when `host` is an absolute path,
    /// i.e. a unix-socket directory, because PostgreSQL then authenticates by
    /// PEER — the kernel vouches for the connecting uid and no password is
    /// ever read.
    ///
    /// This was mandatory, and the consequence was a credential invented to
    /// satisfy a type. Measured on plo 2026-09-06: a node whose whole design
    /// is peer auth ("a removed credential, not a withheld one") had to be
    /// given a Secret carrying the literal string `peer-auth-unused`, because
    /// the field could not be omitted. A required field that forces a
    /// meaningless secret into existence is worse than optional — it teaches
    /// readers that a password matters here, and invites someone to
    /// "rotate" a value that authenticates nothing.
    ///
    /// Absence is not a hole: [`Self::credential_source`] resolves it to a
    /// typed decision, and a TCP host with no `secretRef` is a config error
    /// rather than a silent connection attempt.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub secret_ref: Option<PostgresSecretRef>,

    /// Role to connect as when `secretRef` is absent (socket + peer auth).
    ///
    /// Only consulted on the peer path. libpq would otherwise default to the
    /// process uid's name, which happens to be right today and would break
    /// silently the moment the daemon's `User=` changed — so the role is
    /// stated rather than inherited.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub user: Option<String>,

    /// Connection pool settings.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pool: Option<PoolConfig>,
}

/// How this backend proves who it is — the typed resolution of
/// `secretRef` / `user` / `host`, so no caller re-derives the rule.
///
/// `Peer` carries the role explicitly rather than leaving it implicit in the
/// process uid; `Secret` carries the reference. There is deliberately no
/// third arm for "neither", because that is the error case and an enum arm
/// would invite someone to handle it by connecting anyway.
///
/// Deliberately no `PartialEq`: comparing two credential sources is not a
/// question any caller has, and deriving it would force `PostgresSecretRef` to
/// gain equality it does not otherwise want.
#[derive(Debug, Clone)]
pub enum PgCredentialSource<'a> {
    /// Unix socket + PEER auth. No password exists to supply.
    Peer { user: &'a str },
    /// TCP (or an explicit secret over a socket) — credentials from a Secret.
    Secret(&'a PostgresSecretRef),
}

impl PostgresBackendConfig {
    /// True when `host` names a unix-socket DIRECTORY rather than a TCP host.
    ///
    /// libpq's own rule: a leading `/` selects the socket transport. Matching
    /// libpq here rather than inventing a flag means the CR cannot disagree
    /// with what the client library will actually do.
    #[must_use]
    pub fn is_socket_host(&self) -> bool {
        self.host.starts_with('/')
    }

    /// Resolve the credential source, or say why it cannot be resolved.
    ///
    /// # Errors
    ///
    /// A message naming the missing field when a TCP host has no `secretRef`,
    /// or a socket host has neither `secretRef` nor `user`.
    pub fn credential_source(&self) -> std::result::Result<PgCredentialSource<'_>, String> {
        match (&self.secret_ref, self.is_socket_host()) {
            // An explicit secret always wins, even over a socket: an operator
            // who supplied one may genuinely be using scram over the socket.
            (Some(r), _) => Ok(PgCredentialSource::Secret(r)),
            (None, true) => self.user.as_deref().map(|user| PgCredentialSource::Peer { user }).ok_or_else(|| {
                format!(
                    "backend.pg.host {:?} is a unix socket, so secretRef may be omitted, \
                     but then `user` must name the role to connect as",
                    self.host
                )
            }),
            (None, false) => Err(format!(
                "backend.pg.secretRef is required for TCP host {:?}; it may only be \
                 omitted when host is an absolute path (unix socket + peer auth)",
                self.host
            )),
        }
    }
}

fn default_pg_port() -> u16 {
    5432
}

fn default_schema_prefix() -> String {
    crate::crd::schema_identity::DEFAULT_SCHEMA_PREFIX.to_string()
}

fn default_ssl_mode() -> String {
    "verify-full".to_string()
}

/// Reference to a Secret containing PostgreSQL credentials.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct PostgresSecretRef {
    /// Name of the Secret.
    pub name: String,

    /// Namespace of the Secret.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,

    /// Key containing the username (default: "username").
    #[serde(default = "default_username_key")]
    pub username_key: String,

    /// Key containing the password (default: "password").
    #[serde(default = "default_password_key")]
    pub password_key: String,

    /// Key containing the CA certificate (optional).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ca_cert_key: Option<String>,
}

fn default_username_key() -> String {
    "username".to_string()
}

fn default_password_key() -> String {
    "password".to_string()
}

/// Connection pool configuration.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct PoolConfig {
    /// Minimum number of connections.
    #[serde(default = "default_min_connections")]
    pub min_connections: u32,

    /// Maximum number of connections.
    #[serde(default = "default_max_connections")]
    pub max_connections: u32,

    /// Connection acquire timeout in seconds.
    #[serde(default = "default_acquire_timeout")]
    pub acquire_timeout_secs: u64,

    /// Idle connection timeout in seconds.
    #[serde(default = "default_idle_timeout")]
    pub idle_timeout_secs: u64,
}

fn default_min_connections() -> u32 {
    2
}

fn default_max_connections() -> u32 {
    10
}

fn default_acquire_timeout() -> u64 {
    30
}

fn default_idle_timeout() -> u64 {
    600
}

/// S3 backend configuration (legacy).
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct S3BackendConfig {
    /// S3 bucket name.
    pub bucket: String,

    /// AWS region.
    pub region: String,

    /// Key prefix for state files.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key_prefix: Option<String>,

    /// DynamoDB table for state locking.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dynamodb_table: Option<String>,

    /// Reference to a Secret containing AWS credentials.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub secret_ref: Option<S3SecretRef>,
}

/// Reference to a Secret containing S3/AWS credentials.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct S3SecretRef {
    /// Name of the Secret.
    pub name: String,

    /// Namespace of the Secret.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
}

/// Default provider configurations.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct DefaultProviders {
    /// Default AWS region.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub aws_region: Option<String>,

    /// Default AWS credentials secret.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub aws_secret_ref: Option<SecretRef>,

    /// Default Cloudflare credentials secret.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cloudflare_secret_ref: Option<SecretRef>,
}

/// Reference to a Kubernetes Secret.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SecretRef {
    /// Name of the Secret.
    pub name: String,

    /// Namespace of the Secret.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
}

/// Status of a PangeaNamespace.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct PangeaNamespaceStatus {
    /// Number of templates using this namespace.
    #[serde(default)]
    pub template_count: u32,

    /// Whether the backend is ready.
    #[serde(default)]
    pub backend_ready: bool,

    /// Last time the backend was verified.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_verified_at: Option<chrono::DateTime<chrono::Utc>>,

    /// PostgreSQL schema name for this namespace.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema_name: Option<String>,

    /// Error message if backend is not ready.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,

    /// Resource statistics.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resource_stats: Option<ResourceStats>,
}

/// Resource statistics for a namespace.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ResourceStats {
    /// Total managed resources across all templates.
    pub total_resources: u32,

    /// Total templates in Ready state.
    pub ready_templates: u32,

    /// Total templates in Failed state.
    pub failed_templates: u32,

    /// Total templates with drift detected.
    pub drifted_templates: u32,
}

impl PangeaNamespace {
    /// Get the PostgreSQL schema name for this namespace.
    ///
    /// Routes through [`crate::crd::schema_identity`] — the single
    /// derivation. This is the only entry point that resolves the CR's
    /// real `spec.backend.pg.schemaPrefix`; the template-side derivation
    /// still assumes the default (see that module's Phase 2 note). Both
    /// agree today because every live `PangeaNamespace` uses the default.
    pub fn schema_name(&self) -> String {
        crate::crd::schema_identity::schema_name(
            crate::crd::schema_identity::namespace_schema_prefix(self),
            self.metadata.name.as_deref().unwrap_or("default"),
        )
    }

    /// Build the PostgreSQL connection string (without credentials).
    pub fn connection_url(&self) -> Option<String> {
        let pg = self.spec.backend.pg.as_ref()?;

        Some(format!(
            "postgres://{}:{}/{}?sslmode={}",
            pg.host, pg.port, pg.database, pg.ssl_mode
        ))
    }

    /// Check if this namespace uses PostgreSQL backend.
    pub fn uses_postgres(&self) -> bool {
        matches!(self.spec.backend.r#type, BackendType::Pg)
    }
}

#[cfg(test)]
mod credential_source_tests {
    use super::*;

    fn cfg(host: &str, secret: bool, user: Option<&str>) -> PostgresBackendConfig {
        PostgresBackendConfig {
            host: host.to_string(),
            port: 5432,
            database: "pangea".to_string(),
            schema_prefix: "pangea_".to_string(),
            ssl_mode: "disable".to_string(),
            secret_ref: secret.then(|| PostgresSecretRef {
                name: "s".to_string(),
                namespace: None,
                username_key: "username".to_string(),
                password_key: "password".to_string(),
                ca_cert_key: None,
            }),
            user: user.map(str::to_string),
            pool: None,
        }
    }

    /// The case this whole change exists for: a socket host with a declared
    /// role needs no Secret. Before `secretRef` became optional, plo had to
    /// store the literal string `peer-auth-unused` to satisfy the field.
    #[test]
    fn socket_host_with_a_user_needs_no_secret() {
        let c = cfg("/run/postgresql", false, Some("pangea"));
        assert!(c.is_socket_host());
        match c.credential_source().expect("resolves") {
            PgCredentialSource::Peer { user } => assert_eq!(user, "pangea"),
            PgCredentialSource::Secret(_) => panic!("a socket host must resolve to Peer"),
        }
    }

    /// A TCP host still REQUIRES a Secret. Making the field optional must not
    /// become "credentials are optional" — that would be a real loosening, and
    /// the failure would be a connection attempt with no password rather than a
    /// config error.
    #[test]
    fn tcp_host_without_a_secret_is_a_config_error() {
        let err = cfg("db.internal", false, Some("pangea"))
            .credential_source()
            .expect_err("TCP with no secretRef must not resolve");
        assert!(err.contains("secretRef is required"), "unhelpful message: {err}");
        assert!(err.contains("db.internal"), "message must name the host: {err}");
    }

    /// Omitting BOTH is an error rather than a silent libpq default. The uid's
    /// name happens to be right today and would break silently the moment the
    /// daemon's `User=` changed.
    #[test]
    fn socket_host_without_a_user_is_a_config_error() {
        let err = cfg("/run/postgresql", false, None)
            .credential_source()
            .expect_err("socket with neither secretRef nor user must not resolve");
        assert!(err.contains("`user` must name the role"), "unhelpful message: {err}");
    }

    /// An explicit Secret wins even over a socket — an operator may genuinely be
    /// using scram there, and silently ignoring a supplied credential would be
    /// the worst of the three behaviours.
    #[test]
    fn an_explicit_secret_wins_over_the_peer_path() {
        let c = cfg("/run/postgresql", true, Some("pangea"));
        match c.credential_source().expect("resolves") {
            PgCredentialSource::Secret(r) => assert_eq!(r.name, "s"),
            PgCredentialSource::Peer { .. } => {
                panic!("a supplied secretRef must not be ignored")
            }
        }
    }

    /// A relative path is NOT a socket. libpq's rule is a LEADING slash, and
    /// matching it exactly is what keeps the CR from disagreeing with what the
    /// client library will do.
    #[test]
    fn only_an_absolute_path_counts_as_a_socket() {
        assert!(!cfg("run/postgresql", true, None).is_socket_host());
        assert!(cfg("/var/run/postgresql", true, None).is_socket_host());
    }
}
