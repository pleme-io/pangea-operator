//! Finalizer helpers for `InfrastructureTemplate` reconciliation.
//!
//! Lifted from `template_controller.rs` during the 2026-05-03 review
//! pass (R6). The 3329-line monolith was a barrier to navigation;
//! splitting cleanly-separable helper groups into sibling modules
//! under `controller/template/` reduces the main file to its actual
//! responsibility (the reconcile state machine).
//!
//! Shape mirrors the per-CRD finalizer helpers in image_pipeline,
//! packer_build, ami_test, compliance_schedule. If a fourth controller
//! ever needs the exact same `add` / `remove` / `has` triple, lift
//! again into a generic `finalizer<T>(client, name, ns, finalizer_str)`
//! helper. Until then, the duplication is honest — each controller's
//! Finalizer string is different and the API surface is tiny.

use crate::crd::InfrastructureTemplate;
use crate::error::Result;
use kube::api::{Api, Patch, PatchParams};
use kube::ResourceExt;
use tracing::{debug, info};

use crate::controller::ControllerState;

/// Canonical finalizer string for `InfrastructureTemplate`. Re-exported
/// at module-public scope so the reconciler + helpers share one
/// constant — drift here would orphan templates on delete.
pub const FINALIZER_NAME: &str = "pangea.pleme.io/cleanup";

/// Returns true iff the canonical finalizer is currently attached.
pub fn has_finalizer(template: &InfrastructureTemplate) -> bool {
    template
        .metadata
        .finalizers
        .as_ref()
        .map(|f| f.contains(&FINALIZER_NAME.to_string()))
        .unwrap_or(false)
}

/// Add the canonical finalizer via a server-side merge patch.
pub async fn add_finalizer(
    template: &InfrastructureTemplate,
    state: &ControllerState,
) -> Result<()> {
    let name = template.name_any();
    let namespace = template.namespace().unwrap_or_default();
    let api: Api<InfrastructureTemplate> = Api::namespaced(state.client.clone(), &namespace);

    let patch = serde_json::json!({
        "metadata": { "finalizers": [FINALIZER_NAME] }
    });

    api.patch(
        &name,
        &PatchParams::apply("pangea-operator"),
        &Patch::Merge(&patch),
    )
    .await?;

    debug!("Finalizer added");
    Ok(())
}

/// Remove the canonical finalizer via a server-side merge patch.
/// The new finalizers list is the previous list minus our entry —
/// other operators' finalizers are preserved.
pub async fn remove_finalizer(
    template: &InfrastructureTemplate,
    state: &ControllerState,
) -> Result<()> {
    let name = template.name_any();
    let namespace = template.namespace().unwrap_or_default();
    let api: Api<InfrastructureTemplate> = Api::namespaced(state.client.clone(), &namespace);

    let finalizers: Vec<String> = template
        .metadata
        .finalizers
        .as_ref()
        .map(|f| f.iter().filter(|s| s.as_str() != FINALIZER_NAME).cloned().collect())
        .unwrap_or_default();

    let patch = serde_json::json!({
        "metadata": { "finalizers": finalizers }
    });

    api.patch(
        &name,
        &PatchParams::apply("pangea-operator"),
        &Patch::Merge(&patch),
    )
    .await?;

    info!("Finalizer removed");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fake(finalizers: Option<Vec<String>>) -> InfrastructureTemplate {
        let mut t: InfrastructureTemplate = serde_json::from_value(serde_json::json!({
            "apiVersion": "pangea.pleme.io/v1alpha1",
            "kind": "InfrastructureTemplate",
            "metadata": { "name": "x", "namespace": "y" },
            "spec": {
                "source": { "raw": "" },
                "pangeaNamespace": "default"
            }
        })).expect("fake template parses");
        t.metadata.finalizers = finalizers;
        t
    }

    #[test]
    fn has_finalizer_detects_canonical_name() {
        assert!(!has_finalizer(&fake(None)));
        assert!(has_finalizer(&fake(Some(vec![FINALIZER_NAME.to_string()]))));
        assert!(!has_finalizer(&fake(Some(vec!["other.io/finalizer".to_string()]))));
    }

    #[test]
    fn has_finalizer_handles_finalizer_among_several() {
        let mixed = vec![
            "other.io/cleanup".to_string(),
            FINALIZER_NAME.to_string(),
            "yet-another/finalizer".to_string(),
        ];
        assert!(has_finalizer(&fake(Some(mixed))));
    }
}
