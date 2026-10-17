use crate::error::{Error, Result};
use k8s_openapi::api::core::v1::Secret;
use kube::api::Api;

pub fn key_from_secret(
    secret: &Secret,
    namespace: &str,
    name: &str,
    key: &str,
    context: &str,
) -> Result<String> {
    let data = secret.data.as_ref().ok_or_else(|| {
        Error::Config(format!("{context}: secret {namespace}/{name} has no data"))
    })?;
    data.get(key)
        .map(|v| String::from_utf8_lossy(&v.0).to_string())
        .ok_or_else(|| {
            Error::Config(format!(
                "{context}: key '{key}' not found in secret {namespace}/{name}"
            ))
        })
}

pub async fn read_secret_key(
    client: &kube::Client,
    namespace: &str,
    name: &str,
    key: &str,
    context: &str,
) -> Result<String> {
    let api: Api<Secret> = Api::namespaced(client.clone(), namespace);
    let found = api.get(name).await.map_err(|_| Error::SecretNotFound {
        namespace: namespace.to_string(),
        name: name.to_string(),
    })?;
    key_from_secret(&found, namespace, name, key, context)
}

#[cfg(test)]
mod tests {
    use super::*;
    use k8s_openapi::ByteString;
    use std::collections::BTreeMap;

    fn secret(pairs: &[(&str, &str)]) -> Secret {
        Secret {
            data: Some(
                pairs
                    .iter()
                    .map(|(k, v)| ((*k).to_string(), ByteString(v.as_bytes().to_vec())))
                    .collect::<BTreeMap<_, _>>(),
            ),
            ..Default::default()
        }
    }

    #[test]
    fn reads_the_named_key() {
        let s = secret(&[("k", "v")]);
        assert_eq!(key_from_secret(&s, "ns", "s", "k", "ctx").unwrap(), "v");
    }

    #[test]
    fn a_missing_key_names_its_coordinates_and_not_any_value() {
        let s = secret(&[("other", "fixture-value-123")]);
        let e = key_from_secret(&s, "ns", "s", "k", "secretVariables[X]")
            .unwrap_err()
            .to_string();
        assert!(
            e.contains("secretVariables[X]") && e.contains("ns/s") && e.contains("'k'"),
            "{e}"
        );
        assert!(!e.contains("fixture-value-123"), "{e}");
    }

    #[test]
    fn a_secret_without_data_is_refused() {
        let s = Secret::default();
        assert!(key_from_secret(&s, "ns", "s", "k", "ctx")
            .unwrap_err()
            .to_string()
            .contains("has no data"));
    }
}
