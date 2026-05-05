//! Publish tofu outputs to user-defined K8s Secrets.
//!
//! X2 of the Crossplane-absorb plan — analog of Crossplane's
//! `writeConnectionSecretToRef`, but per-binding (not all outputs to
//! one secret) and with a typed `sensitive` flag.
//!
//! Called from `template_controller::handle_applying` after the apply
//! succeeds and `update_apply_status` has populated `status.outputs`.
//! Each binding picks one output address and writes its value to a
//! named Secret/key in any namespace the operator has reach to. The
//! write is idempotent via server-side apply: re-publishes only when
//! the value actually changes.
//!
//! Failure mode is "log and continue" — a single binding's failure
//! doesn't fail the reconcile (apply already succeeded by the time
//! bindings publish). The cycle receipt records the per-binding
//! result so audits can see what was published vs. skipped.

use std::collections::BTreeMap;

use k8s_openapi::api::core::v1::Secret;
use k8s_openapi::ByteString;
use kube::api::{Patch, PatchParams};
use kube::{Api, Client};
use serde_json::Value as JsonValue;
use tracing::{info, warn};

use crate::crd::{InfrastructureTemplate, OutputBinding};

/// Per-binding result, intended for the cycle receipt and metrics.
#[derive(Debug, Clone, PartialEq)]
pub enum PublishStatus {
    /// Wrote the binding's value to the Secret. `created` distinguishes
    /// "new Secret" from "existing Secret updated."
    Published { created: bool },
    /// The binding's output address didn't appear in `status.outputs`.
    /// Typically means the user added a binding before the next apply
    /// produced the output, or the address has a typo.
    OutputMissing,
    /// API error writing the Secret. Logged with the error string.
    /// Skipping leaves the previous Secret value intact.
    Errored(String),
}

/// Per-binding outcome record. Carries the binding's identity so
/// future cycle-receipt JSON wiring can serialize a per-binding audit
/// trail without re-deriving target metadata. Today only `status` is
/// read by `summarize`, but the surface is part of the typed contract
/// for downstream consumers.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct PublishResult {
    pub output: String,
    pub secret_namespace: String,
    pub secret_name: String,
    pub key: String,
    pub status: PublishStatus,
}

/// For each binding on the template, look up its output value in
/// `outputs`, build a Secret manifest, and server-side-apply it.
/// Returns one [`PublishResult`] per binding (in input order).
///
/// Idempotent: re-running with identical outputs produces no Secret
/// mutation (kube-apiserver's SSA dedupes when content matches).
///
/// Cross-namespace: the operator's ClusterRole grants secrets
/// create/update/patch cluster-wide (chart 0.8.12+); attempting to
/// write into a namespace the operator doesn't have reach to will
/// surface as `Errored`.
pub async fn apply_output_bindings(
    template: &InfrastructureTemplate,
    outputs: &BTreeMap<String, JsonValue>,
    client: &Client,
) -> Vec<PublishResult> {
    let mut results = Vec::with_capacity(template.spec.output_bindings.len());
    for binding in &template.spec.output_bindings {
        results.push(apply_single_binding(template, binding, outputs, client).await);
    }
    results
}

/// Pure value-extraction: pick the binding's output from the tofu
/// output map and stringify the inner `value` field. The map shape
/// matches `tofu output -json`: each entry is
/// `{"value": <v>, "type": <typeexp>, "sensitive": <bool>}`. To stay
/// robust against future tofu shape changes, the function also
/// accepts a flat `{name: <value>}` shape — if `.value` isn't present,
/// the entry is treated as the value itself.
///
/// Numbers/booleans serialize to their JSON repr; strings are taken
/// verbatim (no extra quoting). Missing or null → None.
///
/// Separated from the K8s I/O so unit tests can exercise the value
/// shape without a kube client.
pub fn extract_output_value(
    outputs: &BTreeMap<String, JsonValue>,
    output: &str,
) -> Option<String> {
    let entry = outputs.get(output)?;
    // Prefer the tofu-shaped {value, type, sensitive} envelope; fall
    // back to treating the entry itself as the value.
    let v = match entry {
        JsonValue::Object(map) if map.contains_key("value") => map.get("value")?,
        other => other,
    };
    Some(match v {
        JsonValue::String(s) => s.clone(),
        JsonValue::Null => return None,
        // Numbers and booleans get their canonical JSON repr without
        // extra quoting. Objects/arrays serialize as compact JSON.
        other => other.to_string(),
    })
}

async fn apply_single_binding(
    template: &InfrastructureTemplate,
    binding: &OutputBinding,
    outputs: &BTreeMap<String, JsonValue>,
    client: &Client,
) -> PublishResult {
    let result_skel = |status: PublishStatus| PublishResult {
        output: binding.output.clone(),
        secret_namespace: binding.secret_ref.namespace.clone(),
        secret_name: binding.secret_ref.name.clone(),
        key: binding.secret_ref.key.clone(),
        status,
    };

    let value = match extract_output_value(outputs, &binding.output) {
        Some(v) => v,
        None => {
            warn!(
                output = %binding.output,
                target = %format!("{}/{}#{}", binding.secret_ref.namespace, binding.secret_ref.name, binding.secret_ref.key),
                "output_bindings: tofu output missing in status.outputs; skipping"
            );
            return result_skel(PublishStatus::OutputMissing);
        }
    };

    let api: Api<Secret> = Api::namespaced(client.clone(), &binding.secret_ref.namespace);
    let existed_before = api.get_opt(&binding.secret_ref.name).await.ok().flatten().is_some();

    let secret = build_secret_manifest(template, binding, &value);
    match api
        .patch(
            &binding.secret_ref.name,
            &PatchParams::apply("pangea-operator").force(),
            &Patch::Apply(&secret),
        )
        .await
    {
        Ok(_) => {
            info!(
                output = %binding.output,
                target = %format!("{}/{}#{}", binding.secret_ref.namespace, binding.secret_ref.name, binding.secret_ref.key),
                created = !existed_before,
                "output_bindings: published"
            );
            result_skel(PublishStatus::Published {
                created: !existed_before,
            })
        }
        Err(e) => {
            warn!(
                output = %binding.output,
                target = %format!("{}/{}#{}", binding.secret_ref.namespace, binding.secret_ref.name, binding.secret_ref.key),
                error = %e,
                "output_bindings: failed"
            );
            result_skel(PublishStatus::Errored(e.to_string()))
        }
    }
}

/// Build a Secret manifest ready for server-side-apply. Pure function
/// — no I/O — so unit-testable.
///
/// Carries a `pangea.pleme.io/sensitive=true` label when the binding
/// declared `sensitive: true`. Doesn't change K8s storage behavior;
/// surfaces author intent for downstream tooling (mounted-as-env
/// policies, audit dashboards).
pub fn build_secret_manifest(
    template: &InfrastructureTemplate,
    binding: &OutputBinding,
    value: &str,
) -> Secret {
    use k8s_openapi::apimachinery::pkg::apis::meta::v1::{ObjectMeta, OwnerReference};

    let mut labels: std::collections::BTreeMap<String, String> = std::collections::BTreeMap::new();
    labels.insert(
        "app.kubernetes.io/managed-by".into(),
        "pangea-operator".into(),
    );
    labels.insert(
        "pangea.pleme.io/template".into(),
        template
            .metadata
            .name
            .clone()
            .unwrap_or_else(|| "unknown".into()),
    );
    if let Some(ns) = template.metadata.namespace.clone() {
        labels.insert("pangea.pleme.io/template-namespace".into(), ns);
    }
    if binding.secret_ref.sensitive {
        labels.insert("pangea.pleme.io/sensitive".into(), "true".into());
    }

    // Owner reference back to the template — but only when the Secret
    // lives in the SAME namespace as the template, since K8s rejects
    // cross-namespace owner refs. Cross-namespace bindings still get
    // labels for traceability.
    let owner_refs = if template.metadata.namespace.as_deref()
        == Some(&binding.secret_ref.namespace)
    {
        template.metadata.uid.as_ref().map(|uid| {
            vec![OwnerReference {
                api_version: "pangea.pleme.io/v1alpha1".into(),
                kind: "InfrastructureTemplate".into(),
                name: template.metadata.name.clone().unwrap_or_default(),
                uid: uid.clone(),
                controller: Some(false),
                block_owner_deletion: Some(false),
            }]
        })
    } else {
        None
    };

    let mut data: std::collections::BTreeMap<String, ByteString> =
        std::collections::BTreeMap::new();
    data.insert(binding.secret_ref.key.clone(), ByteString(value.as_bytes().to_vec()));

    Secret {
        metadata: ObjectMeta {
            name: Some(binding.secret_ref.name.clone()),
            namespace: Some(binding.secret_ref.namespace.clone()),
            labels: Some(labels),
            owner_references: owner_refs,
            ..Default::default()
        },
        type_: Some("Opaque".into()),
        data: Some(data),
        ..Default::default()
    }
}

/// Helper for callers that want to log a publish summary.
/// Returns `(published_count, missing_count, errored_count)`.
pub fn summarize(results: &[PublishResult]) -> (u32, u32, u32) {
    let mut p = 0;
    let mut m = 0;
    let mut e = 0;
    for r in results {
        match &r.status {
            PublishStatus::Published { .. } => p += 1,
            PublishStatus::OutputMissing => m += 1,
            PublishStatus::Errored(_) => e += 1,
        }
    }
    (p, m, e)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crd::{OutputBinding, OutputSecretRef};
    use serde_json::json;

    fn outputs_with(pairs: &[(&str, JsonValue)]) -> BTreeMap<String, JsonValue> {
        let mut m = BTreeMap::new();
        for (k, v) in pairs {
            m.insert(k.to_string(), v.clone());
        }
        m
    }

    /// Wrap a value in the tofu `output -json` envelope shape.
    fn tofu(value: JsonValue) -> JsonValue {
        json!({"value": value, "type": "string", "sensitive": false})
    }

    #[test]
    fn extract_output_value_string_passes_through() {
        let outs = outputs_with(&[("foo", tofu(json!("bar")))]);
        assert_eq!(extract_output_value(&outs, "foo").as_deref(), Some("bar"));
    }

    #[test]
    fn extract_output_value_number_serializes_to_json_repr() {
        // Crossplane writes raw bytes, not type-aware. Match that —
        // a number becomes its decimal repr.
        let outs = outputs_with(&[("port", tofu(json!(8080)))]);
        assert_eq!(extract_output_value(&outs, "port").as_deref(), Some("8080"));
    }

    #[test]
    fn extract_output_value_boolean_serializes_to_json_repr() {
        let outs = outputs_with(&[("enabled", tofu(json!(true)))]);
        assert_eq!(
            extract_output_value(&outs, "enabled").as_deref(),
            Some("true")
        );
    }

    #[test]
    fn extract_output_value_null_treated_as_missing() {
        // null in tofu state = "this output exists but has no value".
        // For our purposes that's missing — don't write an empty
        // string.
        let outs = outputs_with(&[("foo", tofu(JsonValue::Null))]);
        assert!(extract_output_value(&outs, "foo").is_none());
    }

    #[test]
    fn extract_output_value_missing_key_returns_none() {
        let outs = outputs_with(&[("foo", tofu(json!("bar")))]);
        assert!(extract_output_value(&outs, "absent").is_none());
    }

    #[test]
    fn extract_output_value_object_serializes_compactly() {
        let outs = outputs_with(&[("creds", tofu(json!({"id": "x", "secret": "y"})))]);
        let out = extract_output_value(&outs, "creds").expect("Some");
        // Object stringification is whatever serde_json emits.
        // Order-stable for BTreeMap-backed values; the key set
        // determines content.
        assert!(out.contains("\"id\":\"x\""));
        assert!(out.contains("\"secret\":\"y\""));
    }

    #[test]
    fn extract_output_value_flat_shape_treated_as_value() {
        // Defensive fallback: if some future caller passes already-
        // unwrapped values, we accept the entry-as-value shape too.
        let outs = outputs_with(&[("foo", json!("bar"))]);
        assert_eq!(extract_output_value(&outs, "foo").as_deref(), Some("bar"));
    }

    fn fake_template(name: &str, namespace: &str, uid: &str) -> InfrastructureTemplate {
        let payload = json!({
            "apiVersion": "pangea.pleme.io/v1alpha1",
            "kind": "InfrastructureTemplate",
            "metadata": {
                "name": name,
                "namespace": namespace,
                "uid": uid,
            },
            "spec": {
                "source": { "raw": "" },
                "pangeaNamespace": "default"
            }
        });
        serde_json::from_value(payload).expect("fake template parses")
    }

    fn binding(output: &str, ns: &str, name: &str, key: &str, sensitive: bool) -> OutputBinding {
        OutputBinding {
            output: output.into(),
            secret_ref: OutputSecretRef {
                name: name.into(),
                namespace: ns.into(),
                key: key.into(),
                sensitive,
            },
        }
    }

    #[test]
    fn build_secret_manifest_writes_value_under_requested_key() {
        let t = fake_template("ct", "cf", "uid-1");
        let b = binding("zone.id", "cf", "cf-zone", "id", false);
        let s = build_secret_manifest(&t, &b, "abc123");
        let data = s.data.as_ref().expect("data");
        assert_eq!(data.get("id").map(|v| v.0.clone()), Some(b"abc123".to_vec()));
        assert_eq!(s.metadata.name.as_deref(), Some("cf-zone"));
        assert_eq!(s.metadata.namespace.as_deref(), Some("cf"));
        assert_eq!(s.type_.as_deref(), Some("Opaque"));
    }

    #[test]
    fn build_secret_manifest_labels_carry_managed_by_and_template_provenance() {
        let t = fake_template("ct", "cf", "uid-1");
        let b = binding("zone.id", "cf", "cf-zone", "id", false);
        let s = build_secret_manifest(&t, &b, "abc123");
        let labels = s.metadata.labels.as_ref().expect("labels");
        assert_eq!(
            labels.get("app.kubernetes.io/managed-by").map(|s| s.as_str()),
            Some("pangea-operator")
        );
        assert_eq!(labels.get("pangea.pleme.io/template").map(|s| s.as_str()), Some("ct"));
        assert_eq!(
            labels.get("pangea.pleme.io/template-namespace").map(|s| s.as_str()),
            Some("cf")
        );
    }

    #[test]
    fn build_secret_manifest_sensitive_label_set_when_flag_true() {
        let t = fake_template("ct", "cf", "uid-1");
        let b = binding("creds.token", "cf", "cf-token", "token", true);
        let s = build_secret_manifest(&t, &b, "tok-123");
        let labels = s.metadata.labels.as_ref().expect("labels");
        assert_eq!(labels.get("pangea.pleme.io/sensitive").map(|s| s.as_str()), Some("true"));
    }

    #[test]
    fn build_secret_manifest_sensitive_label_absent_when_flag_false() {
        let t = fake_template("ct", "cf", "uid-1");
        let b = binding("creds.token", "cf", "cf-token", "token", false);
        let s = build_secret_manifest(&t, &b, "tok-123");
        let labels = s.metadata.labels.as_ref().expect("labels");
        assert!(!labels.contains_key("pangea.pleme.io/sensitive"));
    }

    #[test]
    fn build_secret_manifest_owner_ref_set_when_secret_in_same_namespace() {
        // Same namespace → ownerRef back to template, K8s GC chains
        // through.
        let t = fake_template("ct", "cf", "uid-1");
        let b = binding("zone.id", "cf", "cf-zone", "id", false);
        let s = build_secret_manifest(&t, &b, "abc");
        let owners = s.metadata.owner_references.as_ref().expect("owner_refs");
        assert_eq!(owners.len(), 1);
        assert_eq!(owners[0].name, "ct");
        assert_eq!(owners[0].uid, "uid-1");
        assert_eq!(owners[0].controller, Some(false));
        assert_eq!(owners[0].block_owner_deletion, Some(false));
    }

    #[test]
    fn build_secret_manifest_owner_ref_omitted_when_cross_namespace() {
        // K8s rejects cross-namespace owner refs. Template in `cf`,
        // Secret in `varanda` → no owner ref. Labels carry the
        // provenance instead.
        let t = fake_template("ct", "cf", "uid-1");
        let b = binding("zone.id", "varanda", "varanda-zone", "id", false);
        let s = build_secret_manifest(&t, &b, "abc");
        assert!(s.metadata.owner_references.is_none());
        let labels = s.metadata.labels.as_ref().expect("labels");
        assert_eq!(labels.get("pangea.pleme.io/template").map(|s| s.as_str()), Some("ct"));
    }

    #[test]
    fn summarize_counts_three_categories() {
        let r = vec![
            PublishResult {
                output: "a".into(),
                secret_namespace: "ns".into(),
                secret_name: "s".into(),
                key: "k".into(),
                status: PublishStatus::Published { created: true },
            },
            PublishResult {
                output: "b".into(),
                secret_namespace: "ns".into(),
                secret_name: "s".into(),
                key: "k".into(),
                status: PublishStatus::Published { created: false },
            },
            PublishResult {
                output: "c".into(),
                secret_namespace: "ns".into(),
                secret_name: "s".into(),
                key: "k".into(),
                status: PublishStatus::OutputMissing,
            },
            PublishResult {
                output: "d".into(),
                secret_namespace: "ns".into(),
                secret_name: "s".into(),
                key: "k".into(),
                status: PublishStatus::Errored("forbidden".into()),
            },
        ];
        assert_eq!(summarize(&r), (2, 1, 1));
    }

}
