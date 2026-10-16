use crate::backend::AwsCredentialsConfig;
use crate::crd::AwsRolesAnywhere;
use crate::error::{Error, Result};

pub fn session_request(ra: &AwsRolesAnywhere) -> roles_anywhere::SessionRequest {
    roles_anywhere::SessionRequest {
        trust_anchor_arn: ra.trust_anchor_arn.clone(),
        profile_arn: ra.profile_arn.clone(),
        role_arn: ra.role_arn.clone(),
        duration_seconds: ra.session_duration_seconds,
    }
}

pub fn into_provider_credentials(c: roles_anywhere::SessionCredentials) -> AwsCredentialsConfig {
    let roles_anywhere::SessionCredentials {
        access_key_id,
        secret_access_key: key,
        session_token,
        ..
    } = c;
    AwsCredentialsConfig {
        access_key: access_key_id,
        secret_key: key,
        session_token: Some(session_token),
    }
}

pub async fn issue_session(
    ra: &AwsRolesAnywhere,
    tls_crt: &[u8],
    tls_key: &[u8],
) -> Result<AwsCredentialsConfig> {
    roles_anywhere::issue_session(&session_request(ra), tls_crt, tls_key)
        .await
        .map(into_provider_credentials)
        .map_err(|e| Error::Config(format!("rolesAnywhere: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crd::SecretRef;

    #[test]
    fn crd_source_maps_onto_the_shared_session_request() {
        let ra = AwsRolesAnywhere {
            trust_anchor_arn: "arn:aws:rolesanywhere:us-east-2:376129857990:trust-anchor/ta".into(),
            profile_arn: "arn:aws:rolesanywhere:us-east-2:376129857990:profile/p".into(),
            role_arn: "arn:aws:iam::376129857990:role/pangea-operator-ryn".into(),
            cert_secret_ref: SecretRef {
                name: "c".into(),
                namespace: None,
            },
            session_duration_seconds: Some(1800),
        };
        let req = session_request(&ra);
        assert_eq!(req.endpoint_region(), Some("us-east-2"));
        assert_eq!(req.duration_seconds, Some(1800));
        let c = into_provider_credentials(roles_anywhere::SessionCredentials {
            access_key_id: "ASIA".into(),
            secret_access_key: String::new(),
            session_token: "t".into(),
            expiration: None,
        });
        assert_eq!(c.session_token.as_deref(), Some("t"));
    }
}
