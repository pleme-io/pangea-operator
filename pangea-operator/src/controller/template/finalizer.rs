//! Finalizer plumbing for `InfrastructureTemplate`.
//!
//! Lifted from `template_controller.rs` during R6, then collapsed to
//! a thin wrapper over `crate::controller::finalizer::*` during T5
//! when the same shape was identified across 4 other controllers.
//!
//! The thin per-CRD wrappers exist only to bind the canonical
//! finalizer string to the type. The body of has/add/remove is the
//! generic helper.

use crate::controller::ControllerState;
use crate::crd::InfrastructureTemplate;
use crate::error::Result;

/// Canonical finalizer string for `InfrastructureTemplate`. Drift
/// here would orphan templates on delete.
pub const FINALIZER_NAME: &str = "pangea.pleme.io/cleanup";

/// Returns true iff the canonical finalizer is currently attached.
pub fn has_finalizer(template: &InfrastructureTemplate) -> bool {
    crate::controller::finalizer::has(template, FINALIZER_NAME)
}

/// Add the canonical finalizer via a server-side merge patch.
pub async fn add_finalizer(
    template: &InfrastructureTemplate,
    state: &ControllerState,
) -> Result<()> {
    crate::controller::finalizer::add(template, FINALIZER_NAME, &state.client)
        .await
        .map_err(Into::into)
}

/// Remove the canonical finalizer via a server-side merge patch.
pub async fn remove_finalizer(
    template: &InfrastructureTemplate,
    state: &ControllerState,
) -> Result<()> {
    crate::controller::finalizer::remove(template, FINALIZER_NAME, &state.client)
        .await
        .map_err(Into::into)
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
        }))
        .expect("fake template parses");
        t.metadata.finalizers = finalizers;
        t
    }

    #[test]
    fn has_finalizer_detects_canonical_name() {
        assert!(!has_finalizer(&fake(None)));
        assert!(has_finalizer(&fake(Some(vec![FINALIZER_NAME.to_string()]))));
        assert!(!has_finalizer(&fake(Some(vec![
            "other.io/finalizer".to_string()
        ]))));
    }
}
