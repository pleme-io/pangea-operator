use std::collections::{BTreeMap, HashMap};
use std::sync::{Mutex, OnceLock, PoisonError};
use std::time::Duration;

use base64::Engine;
use chrono::{DateTime, Utc};

use crate::github_read::{GetFailure, GitHubReader, GITHUB_API};

pub const API_URL_ENV: &str = "PANGEA_GITHUB_API_URL";
pub const MINT_MODE_ENV: &str = "PANGEA_GITHUB_APP_TOKEN_MINT";
pub const REFRESH_MARGIN: Duration = Duration::from_secs(10 * 60);
pub const REFUSAL_COOLDOWN: Duration = Duration::from_secs(30 * 60);
const USER_AGENT: &str = "pangea-operator-github-app";
const PKCS1_LABEL: &str = concat!("BEGIN RSA ", "PRIVATE", " KEY");
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MintMode {
    Operator,
    Provider,
}

impl MintMode {
    #[must_use]
    pub fn parse(raw: Option<&str>) -> Self {
        match raw.map(|v| v.trim().to_ascii_lowercase()).as_deref() {
            Some("provider") | Some("app_auth") | Some("off") => Self::Provider,
            _ => Self::Operator,
        }
    }

    #[must_use]
    pub fn from_env() -> Self {
        Self::parse(std::env::var(MINT_MODE_ENV).ok().as_deref())
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct AppCredentials {
    pub app_id: String,
    pub installation_id: String,
    pub private_key_pem: String,
}

impl std::fmt::Debug for AppCredentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AppCredentials")
            .field("app_id", &self.app_id)
            .field("installation_id", &self.installation_id)
            .finish_non_exhaustive()
    }
}

impl AppCredentials {
    #[must_use]
    pub fn identity(&self) -> String {
        let key = blake3::hash(self.private_key_pem.trim().as_bytes()).to_hex();
        format!("{}/{}/{}", self.app_id, self.installation_id, &key[..16])
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum AppAuthError {
    #[error("GitHub App private key: {0}")]
    Key(String),
    #[error("{method} {url}: HTTP {status}: {body}")]
    Http {
        method: &'static str,
        url: String,
        status: u16,
        body: String,
    },
    #[error("{method} {url}: {detail}")]
    Transport {
        method: &'static str,
        url: String,
        detail: String,
    },
    #[error("{0}")]
    Read(String),
    #[error("decoding {what}: {detail}")]
    Decode { what: &'static str, detail: String },
}

fn b64url(bytes: &[u8]) -> String {
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes)
}

fn pem_body(pem: &str) -> Result<Vec<u8>, AppAuthError> {
    let body: String = pem
        .lines()
        .map(str::trim)
        .filter(|l| !l.is_empty() && !l.starts_with("-----"))
        .collect();
    base64::engine::general_purpose::STANDARD
        .decode(body.as_bytes())
        .map_err(|e| AppAuthError::Key(format!("not PEM: {e}")))
}

pub fn sign_app_jwt(
    app_id: &str,
    private_key_pem: &str,
    now: DateTime<Utc>,
) -> Result<String, AppAuthError> {
    let der = pem_body(private_key_pem)?;
    let key = if private_key_pem.contains(PKCS1_LABEL) {
        ring::signature::RsaKeyPair::from_der(&der)
    } else {
        ring::signature::RsaKeyPair::from_pkcs8(&der)
    }
    .map_err(|e| AppAuthError::Key(format!("unusable RSA key: {e}")))?;
    let header = b64url(br#"{"alg":"RS256","typ":"JWT"}"#);
    let iat = now.timestamp() - 60;
    let claims = serde_json::json!({ "iat": iat, "exp": iat + 600, "iss": app_id });
    let signing_input = format!("{header}.{}", b64url(claims.to_string().as_bytes()));
    let mut signature = vec![0u8; key.public().modulus_len()];
    key.sign(
        &ring::signature::RSA_PKCS1_SHA256,
        &ring::rand::SystemRandom::new(),
        signing_input.as_bytes(),
        &mut signature,
    )
    .map_err(|e| AppAuthError::Key(format!("signing the App JWT: {e}")))?;
    Ok(format!("{signing_input}.{}", b64url(&signature)))
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MintedToken {
    pub token: String,
    pub expires_at: DateTime<Utc>,
    pub permissions: BTreeMap<String, String>,
}

impl MintedToken {
    #[must_use]
    pub fn usable_at(&self, now: DateTime<Utc>, margin: Duration) -> bool {
        let margin = chrono::Duration::from_std(margin).unwrap_or(chrono::Duration::zero());
        self.expires_at - now > margin
    }
}

fn permissions_of(v: &serde_json::Value) -> BTreeMap<String, String> {
    v.get("permissions")
        .and_then(serde_json::Value::as_object)
        .map(|m| {
            m.iter()
                .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                .collect()
        })
        .unwrap_or_default()
}

fn level_rank(level: &str) -> u8 {
    match level {
        "read" => 1,
        "write" => 2,
        "admin" => 3,
        _ => 0,
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PermissionGap {
    pub permission: String,
    pub requested: String,
    pub granted: Option<String>,
}

impl std::fmt::Display for PermissionGap {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} (App requests {}, installation grants {})",
            self.permission,
            self.requested,
            self.granted.as_deref().unwrap_or("nothing")
        )
    }
}

#[must_use]
pub fn permission_gaps(
    requested: &BTreeMap<String, String>,
    granted: &BTreeMap<String, String>,
) -> Vec<PermissionGap> {
    requested
        .iter()
        .filter_map(|(name, want)| {
            let have = granted.get(name);
            let short = have.map_or(true, |h| level_rank(h) < level_rank(want));
            short.then(|| PermissionGap {
                permission: name.clone(),
                requested: want.clone(),
                granted: have.cloned(),
            })
        })
        .collect()
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InstallationInfo {
    pub account: Option<String>,
    pub account_is_org: bool,
    pub html_url: Option<String>,
    pub app_slug: Option<String>,
    pub permissions: BTreeMap<String, String>,
}

impl InstallationInfo {
    #[must_use]
    pub fn settings_url(&self, installation_id: &str) -> String {
        if let Some(u) = &self.html_url {
            return u.clone();
        }
        match (&self.account, self.account_is_org) {
            (Some(org), true) => {
                format!("https://github.com/organizations/{org}/settings/installations/{installation_id}")
            }
            _ => format!("https://github.com/settings/installations/{installation_id}"),
        }
    }

    #[must_use]
    pub fn app_settings_url(&self) -> Option<String> {
        let slug = self.app_slug.as_ref()?;
        Some(match (&self.account, self.account_is_org) {
            (Some(org), true) => {
                format!("https://github.com/organizations/{org}/settings/apps/{slug}/permissions")
            }
            _ => format!("https://github.com/settings/apps/{slug}/permissions"),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PermissionVerdict {
    PendingAcceptance {
        gaps: Vec<PermissionGap>,
        settings_url: String,
    },
    NotRequested {
        settings_url: String,
        app_settings_url: Option<String>,
    },
}

#[derive(Debug, Clone)]
pub struct GitHubAppClient {
    http: reqwest::Client,
    base: String,
}

impl GitHubAppClient {
    pub fn new(base: &str) -> Result<Self, AppAuthError> {
        let http = reqwest::Client::builder()
            .connect_timeout(CONNECT_TIMEOUT)
            .timeout(REQUEST_TIMEOUT)
            .build()
            .map_err(|e| AppAuthError::Transport {
                method: "BUILD",
                url: base.to_string(),
                detail: e.to_string(),
            })?;
        Ok(Self {
            http,
            base: base.trim_end_matches('/').to_string(),
        })
    }

    pub fn from_env() -> Result<Self, AppAuthError> {
        let base = std::env::var(API_URL_ENV).unwrap_or_else(|_| GITHUB_API.to_string());
        Self::new(&base)
    }

    pub async fn mint(&self, creds: &AppCredentials) -> Result<MintedToken, AppAuthError> {
        let jwt = sign_app_jwt(&creds.app_id, &creds.private_key_pem, Utc::now())?;
        let url = format!(
            "{}/app/installations/{}/access_tokens",
            self.base, creds.installation_id
        );
        let resp = self
            .http
            .post(&url)
            .header("User-Agent", USER_AGENT)
            .header("Accept", "application/vnd.github+json")
            .bearer_auth(&jwt)
            .send()
            .await
            .map_err(|e| AppAuthError::Transport {
                method: "POST",
                url: url.clone(),
                detail: e.to_string(),
            })?;
        let status = resp.status().as_u16();
        let body = resp.text().await.unwrap_or_default();
        if !(200..300).contains(&status) {
            return Err(AppAuthError::Http {
                method: "POST",
                url,
                status,
                body,
            });
        }
        let v: serde_json::Value =
            serde_json::from_str(&body).map_err(|e| AppAuthError::Decode {
                what: "installation token",
                detail: e.to_string(),
            })?;
        let token = v
            .get("token")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| AppAuthError::Decode {
                what: "installation token",
                detail: "no token field".into(),
            })?
            .to_string();
        let expires_at = v
            .get("expires_at")
            .and_then(serde_json::Value::as_str)
            .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
            .map(|d| d.with_timezone(&Utc))
            .ok_or_else(|| AppAuthError::Decode {
                what: "installation token",
                detail: "no parseable expires_at".into(),
            })?;
        Ok(MintedToken {
            token,
            expires_at,
            permissions: permissions_of(&v),
        })
    }

    async fn get_as_app(
        &self,
        creds: &AppCredentials,
        path: &str,
    ) -> Result<serde_json::Value, AppAuthError> {
        let jwt = sign_app_jwt(&creds.app_id, &creds.private_key_pem, Utc::now())?;
        let reader = GitHubReader::from_client(self.http.clone(), Some(&jwt)).base(&self.base);
        let url = format!("{}{path}", self.base);
        match reader.get(&url).await {
            Ok(Some(page)) => Ok(page.body),
            Ok(None) => Err(AppAuthError::Read(format!("GET {url}: 404"))),
            Err(e) if matches!(e.cause, GetFailure::Unauthorized) => Err(AppAuthError::Http {
                method: "GET",
                url,
                status: 401,
                body: e.to_string(),
            }),
            Err(e) => Err(AppAuthError::Read(e.to_string())),
        }
    }

    pub async fn requested_permissions(
        &self,
        creds: &AppCredentials,
    ) -> Result<BTreeMap<String, String>, AppAuthError> {
        Ok(permissions_of(&self.get_as_app(creds, "/app").await?))
    }

    pub async fn installation(
        &self,
        creds: &AppCredentials,
    ) -> Result<InstallationInfo, AppAuthError> {
        let path = format!("/app/installations/{}", creds.installation_id);
        let v = self.get_as_app(creds, &path).await?;
        let account = v.get("account");
        Ok(InstallationInfo {
            account: account
                .and_then(|a| a.get("login"))
                .and_then(serde_json::Value::as_str)
                .map(str::to_string),
            account_is_org: account
                .and_then(|a| a.get("type"))
                .and_then(serde_json::Value::as_str)
                == Some("Organization")
                || v.get("target_type").and_then(serde_json::Value::as_str)
                    == Some("Organization"),
            html_url: v
                .get("html_url")
                .and_then(serde_json::Value::as_str)
                .map(str::to_string),
            app_slug: v
                .get("app_slug")
                .and_then(serde_json::Value::as_str)
                .map(str::to_string),
            permissions: permissions_of(&v),
        })
    }

    pub async fn diagnose(
        &self,
        creds: &AppCredentials,
    ) -> Result<PermissionVerdict, AppAuthError> {
        let requested = self.requested_permissions(creds).await?;
        let installation = self.installation(creds).await?;
        let settings_url = installation.settings_url(&creds.installation_id);
        let gaps = permission_gaps(&requested, &installation.permissions);
        Ok(if gaps.is_empty() {
            PermissionVerdict::NotRequested {
                settings_url,
                app_settings_url: installation.app_settings_url(),
            }
        } else {
            PermissionVerdict::PendingAcceptance { gaps, settings_url }
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CredentialFailure {
    Rejected,
    Forbidden,
}

#[must_use]
pub fn classify_failure(text: &str) -> Option<CredentialFailure> {
    let lower = text.to_ascii_lowercase();
    if lower.contains("bad credentials") || lower.contains("401 unauthorized") {
        Some(CredentialFailure::Rejected)
    } else if lower.contains("resource not accessible by integration") {
        Some(CredentialFailure::Forbidden)
    } else {
        None
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum CredentialRefusal {
    #[error(
        "GitHub App installation {installation_id} rejected a freshly minted token \
         (401 Bad credentials); check the App's private key and installation id in the \
         credential Secret"
    )]
    RejectedAfterRemint { installation_id: String },
    #[error(
        "GitHub App permissions are pending acceptance on installation {installation_id}: \
         {gaps}; an owner must accept them at {settings_url}"
    )]
    PermissionsPendingAcceptance {
        installation_id: String,
        gaps: String,
        settings_url: String,
    },
    #[error(
        "GitHub answered 403 Resource not accessible by integration with a freshly minted \
         token, and installation {installation_id} already grants everything the App \
         requests, so the App itself lacks a permission this workspace needs; add it to \
         the App{} and accept it at {settings_url}",
        app_settings_url.as_ref().map(|u| format!(" ({u})")).unwrap_or_default()
    )]
    InsufficientPermissions {
        installation_id: String,
        settings_url: String,
        app_settings_url: Option<String>,
    },
}

impl CredentialRefusal {
    #[must_use]
    pub fn reason(&self) -> &'static str {
        match self {
            Self::RejectedAfterRemint { .. } => "BadCredentials",
            Self::PermissionsPendingAcceptance { .. } => "PermissionsPendingAcceptance",
            Self::InsufficientPermissions { .. } => "InsufficientPermissions",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum TokenError {
    #[error("minting a GitHub App installation token: {0}")]
    Mint(#[from] AppAuthError),
    #[error(transparent)]
    Refused(#[from] CredentialRefusal),
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Standing {
    Healthy,
    RemintDue,
    Reminted,
    DiagnoseDue,
    Refused {
        refusal: CredentialRefusal,
        at: DateTime<Utc>,
    },
}

#[derive(Debug, Clone)]
struct Entry {
    token: Option<MintedToken>,
    standing: Standing,
}

impl Default for Entry {
    fn default() -> Self {
        Self {
            token: None,
            standing: Standing::Healthy,
        }
    }
}

pub struct TokenBroker {
    client: GitHubAppClient,
    margin: Duration,
    cooldown: Duration,
    entries: Mutex<HashMap<String, Entry>>,
    owners: Mutex<HashMap<String, String>>,
}

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

fn token_key(token: &str) -> String {
    blake3::hash(token.as_bytes()).to_hex().to_string()
}

impl TokenBroker {
    #[must_use]
    pub fn new(client: GitHubAppClient, margin: Duration, cooldown: Duration) -> Self {
        Self {
            client,
            margin,
            cooldown,
            entries: Mutex::new(HashMap::new()),
            owners: Mutex::new(HashMap::new()),
        }
    }

    pub fn global() -> Option<&'static TokenBroker> {
        static BROKER: OnceLock<Option<TokenBroker>> = OnceLock::new();
        BROKER
            .get_or_init(|| match GitHubAppClient::from_env() {
                Ok(c) => Some(TokenBroker::new(c, REFRESH_MARGIN, REFUSAL_COOLDOWN)),
                Err(e) => {
                    tracing::error!(error = %e, "github app token broker unavailable");
                    None
                }
            })
            .as_ref()
    }

    fn entry(&self, id: &str) -> Entry {
        lock(&self.entries).get(id).cloned().unwrap_or_default()
    }

    fn store(&self, id: &str, entry: Entry) {
        if let Some(t) = &entry.token {
            lock(&self.owners).insert(token_key(&t.token), id.to_string());
        }
        lock(&self.entries).insert(id.to_string(), entry);
    }

    async fn mint_into(
        &self,
        id: &str,
        creds: &AppCredentials,
        standing: Standing,
    ) -> Result<MintedToken, TokenError> {
        let minted = self.client.mint(creds).await?;
        tracing::info!(
            identity = %id,
            expires_at = %minted.expires_at,
            "minted a GitHub App installation token"
        );
        self.store(
            id,
            Entry {
                token: Some(minted.clone()),
                standing,
            },
        );
        Ok(minted)
    }

    pub async fn token(&self, creds: &AppCredentials) -> Result<MintedToken, TokenError> {
        self.token_at(creds, Utc::now()).await
    }

    pub async fn token_at(
        &self,
        creds: &AppCredentials,
        now: DateTime<Utc>,
    ) -> Result<MintedToken, TokenError> {
        let id = creds.identity();
        let entry = self.entry(&id);
        match entry.standing {
            Standing::Refused { refusal, at } => {
                let cooled = chrono::Duration::from_std(self.cooldown)
                    .map(|c| now - at >= c)
                    .unwrap_or(true);
                match refusal {
                    CredentialRefusal::PermissionsPendingAcceptance { .. } => {
                        self.diagnose_into(&id, creds, now).await
                    }
                    other if !cooled => Err(TokenError::Refused(other)),
                    _ => self.mint_into(&id, creds, Standing::Reminted).await,
                }
            }
            Standing::DiagnoseDue => self.diagnose_into(&id, creds, now).await,
            Standing::RemintDue => self.mint_into(&id, creds, Standing::Reminted).await,
            standing @ (Standing::Healthy | Standing::Reminted) => match entry.token {
                Some(t) if t.usable_at(now, self.margin) => Ok(t),
                _ => self.mint_into(&id, creds, standing).await,
            },
        }
    }

    async fn diagnose_into(
        &self,
        id: &str,
        creds: &AppCredentials,
        now: DateTime<Utc>,
    ) -> Result<MintedToken, TokenError> {
        let refusal = match self.client.diagnose(creds).await? {
            PermissionVerdict::PendingAcceptance { gaps, settings_url } => {
                CredentialRefusal::PermissionsPendingAcceptance {
                    installation_id: creds.installation_id.clone(),
                    gaps: gaps
                        .iter()
                        .map(ToString::to_string)
                        .collect::<Vec<_>>()
                        .join(", "),
                    settings_url,
                }
            }
            PermissionVerdict::NotRequested {
                settings_url,
                app_settings_url,
            } => {
                let previous = self.entry(id).standing;
                if matches!(
                    previous,
                    Standing::Refused {
                        refusal: CredentialRefusal::PermissionsPendingAcceptance { .. },
                        ..
                    }
                ) {
                    return self.mint_into(id, creds, Standing::Reminted).await;
                }
                CredentialRefusal::InsufficientPermissions {
                    installation_id: creds.installation_id.clone(),
                    settings_url,
                    app_settings_url,
                }
            }
        };
        tracing::warn!(identity = %id, refusal = %refusal, "github app credential refused");
        let token = self.entry(id).token;
        self.store(
            id,
            Entry {
                token,
                standing: Standing::Refused {
                    refusal: refusal.clone(),
                    at: now,
                },
            },
        );
        Err(TokenError::Refused(refusal))
    }

    fn identity_of_token(&self, token: &str) -> Option<String> {
        lock(&self.owners).get(&token_key(token)).cloned()
    }

    pub fn observe_failure(
        &self,
        token: &str,
        failure: CredentialFailure,
        now: DateTime<Utc>,
    ) -> bool {
        let Some(id) = self.identity_of_token(token) else {
            return false;
        };
        let mut entries = lock(&self.entries);
        let entry = entries.entry(id.clone()).or_default();
        entry.standing = match (&entry.standing, failure) {
            (Standing::Reminted, CredentialFailure::Rejected) => Standing::Refused {
                refusal: CredentialRefusal::RejectedAfterRemint {
                    installation_id: id.split('/').nth(1).unwrap_or_default().to_string(),
                },
                at: now,
            },
            (Standing::Reminted, CredentialFailure::Forbidden) => Standing::DiagnoseDue,
            (Standing::Refused { .. } | Standing::DiagnoseDue, _) => entry.standing.clone(),
            _ => Standing::RemintDue,
        };
        tracing::warn!(
            identity = %id,
            ?failure,
            standing = ?entry.standing,
            "provider reported a credential failure for an operator-minted token"
        );
        true
    }

    pub fn observe_success(&self, token: &str) {
        let Some(id) = self.identity_of_token(token) else {
            return;
        };
        if let Some(entry) = lock(&self.entries).get_mut(&id) {
            if entry.standing == Standing::Reminted {
                entry.standing = Standing::Healthy;
            }
        }
    }
}

#[must_use]
pub fn github_token_of(configs: &BTreeMap<String, serde_json::Value>) -> Option<&str> {
    configs
        .get("github")
        .and_then(|c| c.get("token"))
        .and_then(serde_json::Value::as_str)
}

pub fn observe_cycle<'a>(
    configs: &BTreeMap<String, serde_json::Value>,
    texts: impl IntoIterator<Item = &'a str>,
) -> Option<(CredentialFailure, String)> {
    let token = github_token_of(configs)?;
    let broker = TokenBroker::global()?;
    let found = texts
        .into_iter()
        .find_map(|t| classify_failure(t).map(|f| (f, t.to_string())));
    match &found {
        Some((failure, _)) => {
            broker.observe_failure(token, *failure, Utc::now());
        }
        None => broker.observe_success(token),
    }
    found
}

#[cfg(test)]
mod tests {
    use super::*;
    use wiremock::matchers::{header_regex, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    const PKCS1: &str = include_str!("../tests/fixtures/github-app-test-pkcs1.pem");
    const PKCS8: &str = include_str!("../tests/fixtures/github-app-test-pkcs8.pem");

    fn creds() -> AppCredentials {
        AppCredentials {
            app_id: "4242".into(),
            installation_id: "155727405".into(),
            private_key_pem: PKCS1.into(),
        }
    }

    fn token_body(token: &str, expires_at: DateTime<Utc>) -> serde_json::Value {
        serde_json::json!({
            "token": token,
            "expires_at": expires_at.to_rfc3339(),
            "permissions": { "administration": "write", "metadata": "read" },
        })
    }

    fn broker(server: &MockServer) -> TokenBroker {
        TokenBroker::new(
            GitHubAppClient::new(&server.uri()).unwrap(),
            REFRESH_MARGIN,
            REFUSAL_COOLDOWN,
        )
    }

    async fn mount_mint(server: &MockServer, token: &str, expires_at: DateTime<Utc>, times: u64) {
        Mock::given(method("POST"))
            .and(path("/app/installations/155727405/access_tokens"))
            .and(header_regex("authorization", r"^Bearer [\w-]+\.[\w-]+\.[\w-]+$"))
            .respond_with(ResponseTemplate::new(201).set_body_json(token_body(token, expires_at)))
            .up_to_n_times(times)
            .expect(times)
            .mount(server)
            .await;
    }

    fn decode_segment(seg: &str) -> serde_json::Value {
        let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(seg)
            .unwrap();
        serde_json::from_slice(&bytes).unwrap()
    }

    #[test]
    fn the_app_jwt_is_rs256_with_github_claims_from_either_key_encoding() {
        let now = Utc::now();
        for pem in [PKCS1, PKCS8] {
            let jwt = sign_app_jwt("4242", pem, now).unwrap();
            let parts: Vec<&str> = jwt.split('.').collect();
            assert_eq!(parts.len(), 3);
            assert_eq!(decode_segment(parts[0])["alg"], "RS256");
            let claims = decode_segment(parts[1]);
            assert_eq!(claims["iss"], "4242");
            let iat = claims["iat"].as_i64().unwrap();
            let exp = claims["exp"].as_i64().unwrap();
            assert!(iat <= now.timestamp() - 30);
            assert!(exp - iat <= 600 && exp > now.timestamp());
        }
    }

    #[test]
    fn a_garbage_key_is_a_typed_key_error() {
        assert!(matches!(
            sign_app_jwt("1", &format!("-----{PKCS1_LABEL}-----\nAAAA\n-----END-----"), Utc::now()),
            Err(AppAuthError::Key(_))
        ));
    }

    #[test]
    fn identity_changes_when_the_secret_rotates() {
        let a = creds();
        let mut b = creds();
        b.private_key_pem = PKCS8.into();
        let mut c = creds();
        c.installation_id = "1".into();
        assert_ne!(a.identity(), b.identity());
        assert_ne!(a.identity(), c.identity());
        assert_eq!(a.identity(), creds().identity());
    }

    #[test]
    fn classifier_names_the_two_github_credential_failures() {
        assert_eq!(
            classify_failure("GET https://api.github.com/repos/x: 401 Bad credentials []"),
            Some(CredentialFailure::Rejected)
        );
        assert_eq!(
            classify_failure("POST https://api.github.com/orgs/pleme-io/repos: 403 Resource not accessible by integration []"),
            Some(CredentialFailure::Forbidden)
        );
        assert_eq!(classify_failure("403 API rate limit exceeded"), None);
    }

    #[test]
    fn gaps_are_what_the_app_requests_beyond_the_installation() {
        let requested: BTreeMap<String, String> = [
            ("administration", "write"),
            ("metadata", "read"),
            ("actions", "write"),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
        let granted: BTreeMap<String, String> = [("administration", "read"), ("metadata", "read")]
            .into_iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        let gaps = permission_gaps(&requested, &granted);
        assert_eq!(gaps.len(), 2);
        assert_eq!(gaps[0].permission, "actions");
        assert_eq!(gaps[0].granted, None);
        assert_eq!(gaps[1].permission, "administration");
        assert_eq!(gaps[1].granted.as_deref(), Some("read"));
    }

    #[tokio::test]
    async fn a_token_is_reused_until_ten_minutes_before_expiry_then_reminted() {
        let server = MockServer::start().await;
        let now = Utc::now();
        mount_mint(&server, "ghs_first", now + chrono::Duration::minutes(60), 1).await;
        let b = broker(&server);
        let t1 = b.token_at(&creds(), now).await.unwrap();
        assert_eq!(t1.token, "ghs_first");
        let t2 = b
            .token_at(&creds(), now + chrono::Duration::minutes(45))
            .await
            .unwrap();
        assert_eq!(t2.token, "ghs_first");
        server.verify().await;
        server.reset().await;
        mount_mint(&server, "ghs_second", now + chrono::Duration::minutes(120), 1).await;
        let t3 = b
            .token_at(&creds(), now + chrono::Duration::minutes(51))
            .await
            .unwrap();
        assert_eq!(t3.token, "ghs_second");
        server.verify().await;
    }

    #[tokio::test]
    async fn a_rotated_secret_mints_for_the_new_identity_without_a_restart() {
        let server = MockServer::start().await;
        let now = Utc::now();
        mount_mint(&server, "ghs_a", now + chrono::Duration::minutes(60), 2).await;
        let b = broker(&server);
        b.token_at(&creds(), now).await.unwrap();
        let mut rotated = creds();
        rotated.private_key_pem = PKCS8.into();
        b.token_at(&rotated, now).await.unwrap();
        server.verify().await;
    }

    #[tokio::test]
    async fn bad_credentials_remints_once_then_refuses_with_a_typed_reason() {
        let server = MockServer::start().await;
        let now = Utc::now();
        mount_mint(&server, "ghs_1", now + chrono::Duration::minutes(60), 1).await;
        let b = broker(&server);
        let t = b.token_at(&creds(), now).await.unwrap();
        assert!(b.observe_failure(&t.token, CredentialFailure::Rejected, now));
        server.verify().await;
        server.reset().await;
        mount_mint(&server, "ghs_2", now + chrono::Duration::minutes(60), 1).await;
        let t2 = b.token_at(&creds(), now).await.unwrap();
        assert_eq!(t2.token, "ghs_2");
        server.verify().await;
        assert!(b.observe_failure(&t2.token, CredentialFailure::Rejected, now));
        let refused = b.token_at(&creds(), now).await.unwrap_err();
        match refused {
            TokenError::Refused(r) => assert_eq!(r.reason(), "BadCredentials"),
            other => panic!("expected a refusal, got {other:?}"),
        }
        server.reset().await;
        mount_mint(&server, "ghs_3", now + chrono::Duration::minutes(60), 1).await;
        let after = b
            .token_at(&creds(), now + chrono::Duration::minutes(31))
            .await
            .unwrap();
        assert_eq!(after.token, "ghs_3");
    }

    #[tokio::test]
    async fn a_success_after_a_remint_returns_the_credential_to_healthy() {
        let server = MockServer::start().await;
        let now = Utc::now();
        mount_mint(&server, "ghs_1", now + chrono::Duration::minutes(60), 3).await;
        let b = broker(&server);
        let t = b.token_at(&creds(), now).await.unwrap();
        b.observe_failure(&t.token, CredentialFailure::Rejected, now);
        let t2 = b.token_at(&creds(), now).await.unwrap();
        b.observe_success(&t2.token);
        b.observe_failure(&t2.token, CredentialFailure::Rejected, now);
        assert!(b.token_at(&creds(), now).await.is_ok());
    }

    async fn mount_permissions(server: &MockServer, requested: serde_json::Value, granted: serde_json::Value) {
        Mock::given(method("GET"))
            .and(path("/app"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "slug": "pleme-io-reconciler",
                "permissions": requested,
            })))
            .mount(server)
            .await;
        Mock::given(method("GET"))
            .and(path("/app/installations/155727405"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "account": { "login": "pleme-io", "type": "Organization" },
                "app_slug": "pleme-io-reconciler",
                "target_type": "Organization",
                "permissions": granted,
            })))
            .mount(server)
            .await;
    }

    #[tokio::test]
    async fn a_new_privilege_is_picked_up_by_a_remint_without_any_report() {
        let server = MockServer::start().await;
        let now = Utc::now();
        mount_mint(&server, "ghs_1", now + chrono::Duration::minutes(60), 2).await;
        let b = broker(&server);
        let t = b.token_at(&creds(), now).await.unwrap();
        b.observe_failure(&t.token, CredentialFailure::Forbidden, now);
        let t2 = b.token_at(&creds(), now).await.unwrap();
        b.observe_success(&t2.token);
        server.verify().await;
    }

    #[tokio::test]
    async fn forbidden_after_a_remint_reports_the_pending_permissions_and_url() {
        let server = MockServer::start().await;
        let now = Utc::now();
        mount_mint(&server, "ghs_1", now + chrono::Duration::minutes(60), 2).await;
        mount_permissions(
            &server,
            serde_json::json!({ "administration": "write", "actions": "write", "metadata": "read" }),
            serde_json::json!({ "administration": "write", "metadata": "read" }),
        )
        .await;
        let b = broker(&server);
        let t = b.token_at(&creds(), now).await.unwrap();
        b.observe_failure(&t.token, CredentialFailure::Forbidden, now);
        let t2 = b.token_at(&creds(), now).await.unwrap();
        b.observe_failure(&t2.token, CredentialFailure::Forbidden, now);
        let err = b.token_at(&creds(), now).await.unwrap_err();
        let TokenError::Refused(refusal) = err else {
            panic!("expected a refusal, got {err:?}");
        };
        assert_eq!(refusal.reason(), "PermissionsPendingAcceptance");
        let msg = refusal.to_string();
        assert!(msg.contains("actions (App requests write, installation grants nothing)"), "{msg}");
        assert!(
            msg.contains("https://github.com/organizations/pleme-io/settings/installations/155727405"),
            "{msg}"
        );
        server.verify().await;
    }

    #[tokio::test]
    async fn accepted_permissions_clear_the_refusal_on_the_next_cycle() {
        let server = MockServer::start().await;
        let now = Utc::now();
        mount_mint(&server, "ghs_1", now + chrono::Duration::minutes(60), 2).await;
        mount_permissions(
            &server,
            serde_json::json!({ "actions": "write" }),
            serde_json::json!({}),
        )
        .await;
        let b = broker(&server);
        let t = b.token_at(&creds(), now).await.unwrap();
        b.observe_failure(&t.token, CredentialFailure::Forbidden, now);
        let t2 = b.token_at(&creds(), now).await.unwrap();
        b.observe_failure(&t2.token, CredentialFailure::Forbidden, now);
        assert!(b.token_at(&creds(), now).await.is_err());
        server.reset().await;
        mount_permissions(
            &server,
            serde_json::json!({ "actions": "write" }),
            serde_json::json!({ "actions": "write" }),
        )
        .await;
        mount_mint(&server, "ghs_accepted", now + chrono::Duration::minutes(60), 1).await;
        let t3 = b.token_at(&creds(), now).await.unwrap();
        assert_eq!(t3.token, "ghs_accepted");
    }

    #[tokio::test]
    async fn forbidden_with_no_gap_names_the_app_settings_and_backs_off() {
        let server = MockServer::start().await;
        let now = Utc::now();
        mount_mint(&server, "ghs_1", now + chrono::Duration::minutes(60), 2).await;
        mount_permissions(
            &server,
            serde_json::json!({ "metadata": "read" }),
            serde_json::json!({ "metadata": "read" }),
        )
        .await;
        let b = broker(&server);
        let t = b.token_at(&creds(), now).await.unwrap();
        b.observe_failure(&t.token, CredentialFailure::Forbidden, now);
        let t2 = b.token_at(&creds(), now).await.unwrap();
        b.observe_failure(&t2.token, CredentialFailure::Forbidden, now);
        let TokenError::Refused(refusal) = b.token_at(&creds(), now).await.unwrap_err() else {
            panic!("expected a refusal");
        };
        assert_eq!(refusal.reason(), "InsufficientPermissions");
        assert!(refusal
            .to_string()
            .contains("https://github.com/organizations/pleme-io/settings/apps/pleme-io-reconciler/permissions"));
        assert!(matches!(
            b.token_at(&creds(), now + chrono::Duration::minutes(5)).await,
            Err(TokenError::Refused(_))
        ));
        server.verify().await;
    }

    #[tokio::test]
    async fn a_mint_http_error_is_typed_and_never_hangs() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(404).set_body_string("Not Found"))
            .mount(&server)
            .await;
        let b = broker(&server);
        match b.token(&creds()).await.unwrap_err() {
            TokenError::Mint(AppAuthError::Http { status, .. }) => assert_eq!(status, 404),
            other => panic!("expected an HTTP mint error, got {other:?}"),
        }
    }

    #[test]
    fn mint_mode_defaults_to_the_operator_and_can_be_configured_back() {
        assert_eq!(MintMode::parse(None), MintMode::Operator);
        assert_eq!(MintMode::parse(Some("operator")), MintMode::Operator);
        assert_eq!(MintMode::parse(Some("provider")), MintMode::Provider);
        assert_eq!(MintMode::parse(Some("app_auth")), MintMode::Provider);
    }
}
