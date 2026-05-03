//! Generic finalizer plumbing parameterized over a CRD type +
//! finalizer string.
//!
//! Lifted during T5 (continuation of the 2026-05-03 review pass).
//! Five controllers (template, image_pipeline, packer_build, ami_test,
//! compliance_schedule) each had a hand-rolled `add_finalizer` /
//! `remove_finalizer` / `has_finalizer` triple. Identical body modulo:
//!
//!   - the CRD type T
//!   - the finalizer string (each controller's "pangea.pleme.io/<crd>-cleanup")
//!
//! Lift to one generic `has<R>` / `add<R>` / `remove<R>` over any
//! kube-rs `Resource` whose metadata carries a finalizers vec.
//! Per-controller wrappers shrink to 3-line passthroughs.
//!
//! Drift between the original 5 implementations was minimal but real
//! (some used `info!("Finalizer added")`, some had no log; some did
//! `.map_err(Error::Kube)?`, some used `?` directly with a different
//! From bound). The lift collapses the surface to one canonical
//! shape.

use kube::api::{Api, Patch, PatchParams};
use kube::{Resource, ResourceExt};
use serde::de::DeserializeOwned;
use std::fmt::Debug;
use tracing::debug;

/// Returns true iff the named finalizer is currently attached to the
/// resource. Generic over any kube-rs `Resource` (any CRD).
pub fn has<R: ResourceExt>(obj: &R, finalizer_name: &str) -> bool {
    obj.meta()
        .finalizers
        .as_ref()
        .map(|f| f.iter().any(|s| s == finalizer_name))
        .unwrap_or(false)
}

/// Add the named finalizer via a server-side merge patch. Idempotent
/// from the API server's perspective; if the finalizer already
/// exists the patch is a no-op for that field.
pub async fn add<R>(
    obj: &R,
    finalizer_name: &str,
    client: &kube::Client,
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

    let patch = serde_json::json!({
        "metadata": { "finalizers": [finalizer_name] }
    });

    api.patch(
        &name,
        &PatchParams::apply("pangea-operator"),
        &Patch::Merge(&patch),
    )
    .await?;

    debug!(finalizer = finalizer_name, "Finalizer added");
    Ok(())
}

/// Remove the named finalizer via a server-side merge patch. Other
/// finalizers from third parties are preserved — only ours is
/// filtered out.
pub async fn remove<R>(
    obj: &R,
    finalizer_name: &str,
    client: &kube::Client,
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

    let finalizers: Vec<String> = obj
        .meta()
        .finalizers
        .as_ref()
        .map(|f| {
            f.iter()
                .filter(|s| s.as_str() != finalizer_name)
                .cloned()
                .collect()
        })
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

    debug!(finalizer = finalizer_name, "Finalizer removed");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crd::PackerBuild;

    fn fake_pb(finalizers: Option<Vec<String>>) -> PackerBuild {
        let mut pb: PackerBuild = serde_json::from_value(serde_json::json!({
            "apiVersion": "pangea.pleme.io/v1alpha1",
            "kind": "PackerBuild",
            "metadata": { "name": "x", "namespace": "y" },
            "spec": { "source": { "raw": "" } }
        })).expect("fake PackerBuild parses");
        pb.metadata.finalizers = finalizers;
        pb
    }

    const TEST_FINALIZER: &str = "pangea.pleme.io/test-cleanup";

    #[test]
    fn has_returns_false_when_no_finalizers() {
        assert!(!has(&fake_pb(None), TEST_FINALIZER));
    }

    #[test]
    fn has_returns_true_when_canonical_present() {
        assert!(has(
            &fake_pb(Some(vec![TEST_FINALIZER.to_string()])),
            TEST_FINALIZER
        ));
    }

    #[test]
    fn has_returns_false_when_only_other_finalizers_present() {
        assert!(!has(
            &fake_pb(Some(vec!["other.io/finalizer".to_string()])),
            TEST_FINALIZER
        ));
    }

    #[test]
    fn has_returns_true_for_canonical_among_several() {
        // Mixed list — generic helper finds the requested name.
        let mixed = vec![
            "other.io/cleanup".to_string(),
            TEST_FINALIZER.to_string(),
            "yet-another/finalizer".to_string(),
        ];
        assert!(has(&fake_pb(Some(mixed)), TEST_FINALIZER));
    }

    #[test]
    fn has_distinguishes_between_different_finalizer_names() {
        // The whole point of parameterizing on the name: two
        // different finalizers on the same CRD type don't collide.
        let pb = fake_pb(Some(vec![
            "pangea.pleme.io/cleanup".to_string(),
            "pangea.pleme.io/audit".to_string(),
        ]));
        assert!(has(&pb, "pangea.pleme.io/cleanup"));
        assert!(has(&pb, "pangea.pleme.io/audit"));
        assert!(!has(&pb, "pangea.pleme.io/missing"));
    }
}
