//! GitHub credential resolution for the org resolver and settings reconciler.
//!
//! One place decides which credential a run authenticates with:
//!
//! | source | flag | lifetime |
//! |---|---|---|
//! | GitHub App | `--app-dir <dir>` | installation token minted per run, one hour |
//! | PAT file | `--token-file <path>` | static |
//! | PAT env | `GITHUB_TOKEN` | static |
//!
//! The App wins when named. A PAT is behaviour, not a defect, so it is
//! configured off rather than deleted: `--pat-enabled false` refuses both PAT
//! sources with a message naming the knob, and the code and tests stay.

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use rsa::pkcs1::DecodeRsaPrivateKey;
use rsa::pkcs1v15::SigningKey;
use rsa::pkcs8::DecodePrivateKey;
use rsa::signature::{SignatureEncoding, Signer};
use rsa::RsaPrivateKey;
use sha2::Sha256;

use crate::github_read::{GITHUB_API, USER_AGENT};

const APP_ID_FILE: &str = "app-id";
const INSTALL_ID_FILE: &str = "install-id";
const PRIVATE_KEY_FILE: &str = "private_key";
const JWT_BACKDATE_SECS: u64 = 60;
const JWT_LIFETIME_SECS: u64 = 540;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CredentialPolicy {
    pub pat_enabled: bool,
}

impl Default for CredentialPolicy {
    fn default() -> Self {
        Self { pat_enabled: true }
    }
}

impl CredentialPolicy {
    pub fn parse(raw: Option<&str>) -> Result<Self, String> {
        match raw {
            None => Ok(Self::default()),
            Some("true") => Ok(Self { pat_enabled: true }),
            Some("false") => Ok(Self { pat_enabled: false }),
            Some(other) => Err(format!(
                "--pat-enabled takes true or false, got {other:?}"
            )),
        }
    }
}

#[derive(Clone)]
pub struct AppCredentials {
    app_id: String,
    installation_id: String,
    pem: String,
}

impl std::fmt::Debug for AppCredentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AppCredentials")
            .field("app_id", &self.app_id)
            .field("installation_id", &self.installation_id)
            .field("pem", &"<redacted>")
            .finish()
    }
}

impl AppCredentials {
    pub fn new(app_id: &str, installation_id: &str, pem: &str) -> Result<Self, String> {
        let trim = |label: &str, v: &str| -> Result<String, String> {
            let v = v.trim();
            if v.is_empty() {
                Err(format!("GitHub App {label} is empty"))
            } else {
                Ok(v.to_string())
            }
        };
        Ok(Self {
            app_id: trim("app id", app_id)?,
            installation_id: trim("installation id", installation_id)?,
            pem: trim("private key", pem)?,
        })
    }

    pub fn from_dir(dir: &str) -> Result<Self, String> {
        let read = |name: &str| -> Result<String, String> {
            std::fs::read_to_string(std::path::Path::new(dir).join(name))
                .map_err(|e| format!("reading {name} in --app-dir {dir}: {e}"))
        };
        Self::new(&read(APP_ID_FILE)?, &read(INSTALL_ID_FILE)?, &read(PRIVATE_KEY_FILE)?)
    }

    fn signing_key(&self) -> Result<SigningKey<Sha256>, String> {
        let key = RsaPrivateKey::from_pkcs1_pem(&self.pem)
            .or_else(|_| RsaPrivateKey::from_pkcs8_pem(&self.pem))
            .map_err(|e| format!("GitHub App private key is neither PKCS#1 nor PKCS#8 PEM: {e}"))?;
        Ok(SigningKey::<Sha256>::new(key))
    }

    pub fn app_jwt(&self, now_unix: u64) -> Result<String, String> {
        let header = URL_SAFE_NO_PAD.encode(br#"{"alg":"RS256","typ":"JWT"}"#);
        let claims = serde_json::json!({
            "iat": now_unix.saturating_sub(JWT_BACKDATE_SECS),
            "exp": now_unix + JWT_LIFETIME_SECS,
            "iss": self.app_id,
        });
        let payload = URL_SAFE_NO_PAD.encode(claims.to_string());
        let signing_input = format!("{header}.{payload}");
        let signature = self.signing_key()?.sign(signing_input.as_bytes());
        Ok(format!(
            "{signing_input}.{}",
            URL_SAFE_NO_PAD.encode(signature.to_bytes())
        ))
    }

    pub async fn installation_token(&self, api_base: &str) -> Result<String, String> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|e| format!("system clock before the epoch: {e}"))?
            .as_secs();
        let jwt = self.app_jwt(now)?;
        let url = format!(
            "{api_base}/app/installations/{}/access_tokens",
            self.installation_id
        );
        let response = reqwest::Client::new()
            .post(&url)
            .bearer_auth(jwt)
            .header("Accept", "application/vnd.github+json")
            .header("User-Agent", USER_AGENT)
            .send()
            .await
            .map_err(|e| format!("minting installation token: {e}"))?;
        let status = response.status();
        let body: serde_json::Value = response
            .json()
            .await
            .map_err(|e| format!("minting installation token: unreadable response ({status}): {e}"))?;
        if !status.is_success() {
            return Err(format!(
                "minting installation token: {status}: {}",
                body.get("message").and_then(|m| m.as_str()).unwrap_or("no message")
            ));
        }
        body.get("token")
            .and_then(|t| t.as_str())
            .filter(|t| !t.is_empty())
            .map(str::to_string)
            .ok_or_else(|| "minting installation token: response carried no token".to_string())
    }
}

pub struct CredentialRequest {
    pub app_dir: Option<String>,
    pub token_file: Option<String>,
    pub env_token: Option<String>,
    pub policy: CredentialPolicy,
}

pub async fn resolve(request: CredentialRequest) -> Result<Option<String>, String> {
    if let Some(dir) = &request.app_dir {
        return AppCredentials::from_dir(dir)?
            .installation_token(GITHUB_API)
            .await
            .map(Some);
    }
    let pat_requested = request.token_file.is_some() || request.env_token.is_some();
    if pat_requested && !request.policy.pat_enabled {
        return Err(
            "a PAT credential was supplied but PATs are configured off (--pat-enabled false); \
             pass --app-dir <dir> to authenticate as the GitHub App"
                .to_string(),
        );
    }
    match request.token_file {
        Some(path) => crate::org_resolve::read_token_file(&path).map(Some),
        None => Ok(request.env_token),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rsa::pkcs1::EncodeRsaPrivateKey;
    use rsa::pkcs1v15::VerifyingKey;
    use rsa::signature::Verifier;

    fn test_credentials() -> (AppCredentials, RsaPrivateKey) {
        let mut rng = rsa::rand_core::OsRng;
        let key = RsaPrivateKey::new(&mut rng, 2048).expect("generate test key");
        let pem = key.to_pkcs1_pem(rsa::pkcs8::LineEnding::LF).expect("pem");
        (AppCredentials::new("4683416", "155780635", &pem).expect("creds"), key)
    }

    #[test]
    fn the_jwt_verifies_under_the_public_key_and_carries_the_app_id() {
        let (creds, key) = test_credentials();
        let jwt = creds.app_jwt(1_800_000_000).expect("jwt");
        let parts: Vec<&str> = jwt.split('.').collect();
        assert_eq!(parts.len(), 3);
        let claims: serde_json::Value =
            serde_json::from_slice(&URL_SAFE_NO_PAD.decode(parts[1]).unwrap()).unwrap();
        assert_eq!(claims["iss"], "4683416");
        assert_eq!(claims["iat"], 1_800_000_000u64 - JWT_BACKDATE_SECS);
        assert_eq!(claims["exp"], 1_800_000_000u64 + JWT_LIFETIME_SECS);
        let verifying = VerifyingKey::<Sha256>::new(key.to_public_key());
        let signature = rsa::pkcs1v15::Signature::try_from(
            URL_SAFE_NO_PAD.decode(parts[2]).unwrap().as_slice(),
        )
        .unwrap();
        verifying
            .verify(format!("{}.{}", parts[0], parts[1]).as_bytes(), &signature)
            .expect("signature verifies");
    }

    #[test]
    fn debug_never_prints_the_key() {
        let (creds, _) = test_credentials();
        let shown = format!("{creds:?}");
        assert!(shown.contains("<redacted>"));
        assert!(!shown.contains("BEGIN RSA PRIVATE KEY"));
    }

    #[test]
    fn an_empty_field_is_refused_not_signed_with() {
        assert!(AppCredentials::new("", "1", "k").is_err());
        assert!(AppCredentials::new("1", " ", "k").is_err());
        assert!(AppCredentials::new("1", "1", "").is_err());
    }

    #[test]
    fn a_bad_pem_names_the_problem() {
        let creds = AppCredentials::new("1", "2", "not a pem").unwrap();
        assert!(creds.app_jwt(0).unwrap_err().contains("neither PKCS#1 nor PKCS#8"));
    }

    #[test]
    fn the_policy_parses_true_false_and_absent_and_refuses_the_rest() {
        assert!(CredentialPolicy::parse(None).unwrap().pat_enabled);
        assert!(CredentialPolicy::parse(Some("true")).unwrap().pat_enabled);
        assert!(!CredentialPolicy::parse(Some("false")).unwrap().pat_enabled);
        assert!(CredentialPolicy::parse(Some("off")).is_err());
    }

    #[tokio::test]
    async fn a_pat_is_refused_when_configured_off_and_the_message_names_the_knob() {
        let err = resolve(CredentialRequest {
            app_dir: None,
            token_file: None,
            env_token: Some("ghp_x".into()),
            policy: CredentialPolicy { pat_enabled: false },
        })
        .await
        .unwrap_err();
        assert!(err.contains("--pat-enabled false"));
    }

    #[tokio::test]
    async fn a_pat_still_resolves_when_enabled() {
        let got = resolve(CredentialRequest {
            app_dir: None,
            token_file: None,
            env_token: Some("ghp_x".into()),
            policy: CredentialPolicy::default(),
        })
        .await
        .unwrap();
        assert_eq!(got.as_deref(), Some("ghp_x"));
    }

    #[tokio::test]
    async fn no_credential_at_all_stays_anonymous() {
        let got = resolve(CredentialRequest {
            app_dir: None,
            token_file: None,
            env_token: None,
            policy: CredentialPolicy { pat_enabled: false },
        })
        .await
        .unwrap();
        assert_eq!(got, None);
    }
}
