//! Resolves `InfrastructureTemplateSpec.secretFiles` into ephemeral,
//! pod-local files under a template's tofu workspace.
//!
//! Generalizes the shape `template_controller.rs::git_auth_env` already
//! proves for git credentials (resolve a Secret → write it to a file in
//! the workspace → reference the file from rendered HCL) to ANY named
//! secret a workspace's Ruby needs as a file on disk — not just git
//! auth. First consumer: a self-managed cluster's `flux` provider
//! `kubernetes { token = ... }` block
//! (`Pangea::Architectures::FluxBootstrapGeneric`, `:host_token` mode),
//! which has no Ruby `provider :xyz do` surface to inline an
//! `ENV.fetch` into (unlike the `:deploy_key`/`:https_pat` git-auth
//! path) and isn't provider-block-shaped credential data either
//! (`ProviderCredentials` models region/api_token/owner-shaped
//! attributes the operator renders into `providers.tf.json` — a raw
//! bearer token referenced via `file(...)` is neither).
//!
//! Deliberately outside the `ProviderCredentials`/`ProviderKind`
//! exhaustiveness chain (see that type's own doc comment in
//! `crd::infrastructure_template`) — adding a `ProviderKind::Kubernetes`
//! variant would force every consumer of that enum (the tofu
//! `providers.tf.json` emitter, the magma `ConfigureProvider` mapper) to
//! grow a case for a "provider" that doesn't correspond to any
//! `provider "<name>" {}` block at all. `secret_files` sits as a
//! sibling field on `InfrastructureTemplateSpec` instead — the correct
//! shape for "an arbitrary named file the rendered HCL reads via
//! `file(...)`", not a provider credential.

use crate::controller::ControllerState;
use crate::crd::InfrastructureTemplate;
use crate::error::{Error, Result};
use crate::executor::Workspace;
use k8s_openapi::api::core::v1::Secret;
use kube::api::Api;
use kube::ResourceExt;

/// Write every `spec.secretFiles` entry into the workspace. Called once
/// per Initializing-phase reconcile, immediately before `executor.init`
/// — the same call site `write_provider_config` already occupies (see
/// `template_controller.rs::handle_initializing`), and gated the same
/// way (skipped on the magma path, which reads providers from the
/// resolved config rather than workspace files).
///
/// A no-op (zero Secret reads, zero files written) when
/// `spec.secretFiles` is empty — existing templates pay nothing for
/// this.
///
/// A referenced Secret that doesn't exist, or a `key` absent from its
/// `data` map, is a typed `Error` that fails the Initializing phase
/// loudly — never a silently-skipped file. Unlike
/// `resolve_provider_configs`' credential-omission path (where an
/// absent attr just means "let the pod-env fallback cover it"), a
/// missing `secretFiles` entry has no fallback: the rendered HCL's
/// `file(...)` call would fail at plan time with an opaque "no such
/// file" error, so failing here — at the true cause, with the Secret
/// coordinates named — is strictly better for the operator reading
/// `status.lastError`.
pub async fn write_secret_files(
    template: &InfrastructureTemplate,
    state: &ControllerState,
    workspace: &Workspace,
) -> Result<()> {
    let default_ns = template
        .namespace()
        .unwrap_or_else(|| "default".to_string());

    for sf in &template.spec.secret_files {
        validate_file_name(&sf.name)?;

        let ns = sf.secret_ref.namespace.as_deref().unwrap_or(&default_ns);
        let secret_api: Api<Secret> = Api::namespaced(state.client.clone(), ns);
        let secret =
            secret_api
                .get(&sf.secret_ref.name)
                .await
                .map_err(|_| Error::SecretNotFound {
                    namespace: ns.to_string(),
                    name: sf.secret_ref.name.clone(),
                })?;

        let data = secret.data.as_ref().ok_or_else(|| {
            Error::Config(format!(
                "secretFiles[{}]: secret {}/{} has no data",
                sf.name, ns, sf.secret_ref.name
            ))
        })?;

        let value = data
            .get(&sf.key)
            .map(|v| String::from_utf8_lossy(&v.0).to_string())
            .ok_or_else(|| {
                Error::Config(format!(
                    "secretFiles[{}]: key '{}' not found in secret {}/{}",
                    sf.name, sf.key, ns, sf.secret_ref.name
                ))
            })?;

        let path = workspace.write_file(&sf.name, &value).await?;

        // Best-effort tighten to owner-read/write only — matches the
        // sensitivity of a bearer token / PAT living on disk, even
        // though the pod-local emptyDir this lives on is already
        // single-tenant (this operator's own container). Failure to
        // chmod is not fatal (e.g. a filesystem that doesn't support
        // unix perms) — the file is still written and usable by tofu.
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            if let Err(e) =
                tokio::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).await
            {
                tracing::warn!(
                    file = %sf.name,
                    error = %e,
                    "secretFiles: failed to chmod 0600 (non-fatal)"
                );
            }
        }
    }

    Ok(())
}

/// Reject a `name` that could escape the workspace directory via
/// `PathBuf::join` (e.g. `../../etc/passwd`, an absolute path) or that
/// collides with the operator's own reserved dotfile control surface.
/// Bare filenames only — the same shape the operator's own `_git_user`
/// / `_git_pass` / `_git_askpass.sh` control files use (a leading
/// underscore is fine and idiomatic here; a leading dot is reserved).
fn validate_file_name(name: &str) -> Result<()> {
    if name.is_empty()
        || name.contains('/')
        || name.contains('\\')
        || name == ".."
        || name.starts_with('.')
    {
        return Err(Error::Config(format!(
            "secretFiles: invalid file name '{name}' — must be a bare filename with no path separators or leading dot"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_path_traversal_and_absolute_paths() {
        assert!(validate_file_name("../etc/passwd").is_err());
        assert!(validate_file_name("../../etc/passwd").is_err());
        assert!(validate_file_name("a/b").is_err());
        assert!(validate_file_name("a\\b").is_err());
        assert!(validate_file_name("..").is_err());
    }

    #[test]
    fn rejects_leading_dot_and_empty() {
        assert!(validate_file_name(".hidden").is_err());
        assert!(validate_file_name("").is_err());
    }

    #[test]
    fn accepts_bare_filenames_including_leading_underscore() {
        assert!(validate_file_name("_flux_cluster_token").is_ok());
        assert!(validate_file_name("_flux_git_pat").is_ok());
        assert!(validate_file_name("token.txt").is_ok());
        assert!(validate_file_name("plain-name").is_ok());
    }
}
