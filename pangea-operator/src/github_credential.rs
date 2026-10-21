//! GitHub credential resolution for the org resolver and settings reconciler.
//!
//! One place decides which credential a run authenticates with:
//!
//! | source | flag | lifetime |
//! |---|---|---|
//! | GitHub App installation token | `--app-token-file <path>` | one hour, minted per run |
//! | PAT file | `--token-file <path>` | static |
//! | PAT env | `GITHUB_TOKEN` | static |
//!
//! The App token is minted outside this binary by `github-installation-token-file`,
//! which can narrow its permissions per run; this module only reads the file it
//! wrote. The App wins when named. A PAT is behaviour, not a defect, so it is
//! configured off rather than deleted: `--pat-enabled false` refuses both PAT
//! sources with a message naming the knob, and the code and tests stay.

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
            Some(other) => Err(format!("--pat-enabled takes true or false, got {other:?}")),
        }
    }
}

pub struct CredentialRequest {
    pub app_token_file: Option<String>,
    pub token_file: Option<String>,
    pub env_token: Option<String>,
    pub policy: CredentialPolicy,
}

pub fn resolve(request: CredentialRequest) -> Result<Option<String>, String> {
    if let Some(path) = &request.app_token_file {
        return crate::org_resolve::read_token_file(path).map(Some);
    }
    let pat_requested = request.token_file.is_some() || request.env_token.is_some();
    if pat_requested && !request.policy.pat_enabled {
        return Err(
            "a PAT credential was supplied but PATs are configured off (--pat-enabled false); \
             pass --app-token-file <path> to authenticate as the GitHub App"
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

    fn request(
        app: Option<&str>,
        file: Option<&str>,
        env: Option<&str>,
        pat: bool,
    ) -> CredentialRequest {
        CredentialRequest {
            app_token_file: app.map(str::to_string),
            token_file: file.map(str::to_string),
            env_token: env.map(str::to_string),
            policy: CredentialPolicy { pat_enabled: pat },
        }
    }

    fn token_file(name: &str, body: &str) -> String {
        let path = std::env::temp_dir().join(format!("github-credential-test-{name}"));
        std::fs::write(&path, body).unwrap();
        path.to_string_lossy().into_owned()
    }

    #[test]
    fn the_policy_parses_true_false_and_absent_and_refuses_the_rest() {
        assert!(CredentialPolicy::parse(None).unwrap().pat_enabled);
        assert!(CredentialPolicy::parse(Some("true")).unwrap().pat_enabled);
        assert!(!CredentialPolicy::parse(Some("false")).unwrap().pat_enabled);
        assert!(CredentialPolicy::parse(Some("off")).is_err());
    }

    #[test]
    fn a_pat_is_refused_when_configured_off_and_the_message_names_the_knob() {
        let err = resolve(request(None, None, Some("ghp_x"), false)).unwrap_err();
        assert!(err.contains("--pat-enabled false"));
        let path = token_file("pat-off", "ghp_y\n");
        let err = resolve(request(None, Some(&path), None, false)).unwrap_err();
        assert!(err.contains("--app-token-file"));
    }

    #[test]
    fn a_pat_still_resolves_when_enabled() {
        let got = resolve(request(None, None, Some("ghp_x"), true)).unwrap();
        assert_eq!(got.as_deref(), Some("ghp_x"));
    }

    #[test]
    fn the_app_token_resolves_even_when_pats_are_off() {
        let path = token_file("app-on", "ghs_app\n");
        let got = resolve(request(Some(&path), None, Some("ghp_x"), false)).unwrap();
        assert_eq!(got.as_deref(), Some("ghs_app"));
    }

    #[test]
    fn an_empty_app_token_file_is_refused_not_read_as_anonymous() {
        let path = token_file("app-empty", "  \n");
        assert!(resolve(request(Some(&path), None, None, true)).is_err());
    }

    #[test]
    fn no_credential_at_all_stays_anonymous() {
        assert_eq!(resolve(request(None, None, None, false)).unwrap(), None);
    }
}
