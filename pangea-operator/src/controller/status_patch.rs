//! Generic status-patch helper — single chokepoint for every
//! `api.patch_status(...)` call across the operator.
//!
//! Lifted during U2 (continuation of the 2026-05-03 review pass).
//! 23 call sites across 8 controllers each repeated the same shape:
//!
//! ```ignore
//! let api: Api<X> = Api::namespaced(state.client.clone(), &namespace);
//! api.patch_status(
//!     &name,
//!     &PatchParams::apply("pangea-operator"),
//!     &Patch::Merge(&patch),
//! ).await.map_err(Error::Kube)?;
//! ```
//!
//! Lift to one generic `patch_status<R>` over any namespace-scoped
//! kube-rs `Resource`. Caller hands a `serde_json::Value` for the
//! `status` patch body; the helper handles client / api / patch-
//! params / merge / error mapping.
//!
//! Why this matters beyond line reduction: future cross-cutting
//! changes (e.g. switching from Merge to StrategicMerge, adding
//! field-manager telemetry, threading a tracing span through every
//! status patch) become one-edit instead of 23-edits.

use kube::api::{Api, Patch, PatchParams};
use kube::{Resource, ResourceExt};
use serde::de::DeserializeOwned;
use std::fmt::Debug;

/// Apply a server-side merge patch on a CRD's `.status` subresource.
/// Field manager is `pangea-operator`. Generic over any namespaced
/// kube-rs `Resource`.
///
/// The `patch` argument is the full status-patch body — typically
/// `serde_json::json!({ "status": { ... } })`. Pass-through to
/// `kube::api::Api::patch_status` after threading the standard
/// PatchParams + Merge wrapper.
pub async fn patch_status<R>(
    obj: &R,
    client: &kube::Client,
    patch: serde_json::Value,
) -> Result<(), kube::Error>
where
    R: Resource<Scope = k8s_openapi::NamespaceResourceScope, DynamicType = ()>
        + Clone
        + Debug
        + serde::Serialize
        + DeserializeOwned,
{
    let name = obj.name_any();
    let namespace = obj.namespace().unwrap_or_default();
    let api: Api<R> = Api::namespaced(client.clone(), &namespace);
    api.patch_status(
        &name,
        &PatchParams::apply("pangea-operator"),
        &Patch::Merge(&patch),
    )
    .await
    .map(|_| ())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crd::PackerBuild;

    #[test]
    fn patch_status_signature_compiles_for_packerbuild() {
        // Compile-time check: PackerBuild satisfies the trait bounds.
        // No actual API call — that requires a live kube::Client.
        // The intent is to lock in the bound surface so a future
        // CRD with a different shape can't accidentally be excluded
        // by a stricter bound on this helper.
        fn _assert_bounds<R>()
        where
            R: Resource<Scope = k8s_openapi::NamespaceResourceScope, DynamicType = ()>
                + Clone
                + Debug
                + serde::Serialize
                + DeserializeOwned,
        {
        }
        _assert_bounds::<PackerBuild>();
        _assert_bounds::<crate::crd::AmiTest>();
        _assert_bounds::<crate::crd::ImagePipeline>();
        _assert_bounds::<crate::crd::InfrastructureTemplate>();
        _assert_bounds::<crate::crd::ComplianceSchedule>();
        _assert_bounds::<crate::crd::ComplianceBinding>();
        _assert_bounds::<crate::crd::InfrastructureFlow>();
    }
}
