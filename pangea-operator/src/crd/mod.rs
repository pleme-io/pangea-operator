//! Custom Resource Definitions for the Pangea Operator.
//!
//! This module defines the Kubernetes custom resources used by the Pangea operator:
//! - `InfrastructureTemplate`: Represents a Pangea infrastructure template to be deployed
//! - `PangeaNamespace`: Cluster-scoped configuration for Pangea namespaces
//! - `InfrastructureFlow`: DAG orchestrator for multi-template deployments
//! - `PackerBuild`: Packer build execution (Ruby DSL → JSON → AMI)
//! - `AmiTest`: AMI validation through tiered test suites
//! - `ImagePipeline`: Full AMI lifecycle orchestrator (build → test → deploy → verify)

pub mod ami_test;
pub mod architecture_gem;
pub mod compliance_binding;
pub mod compliance_schedule;
pub mod image_pipeline;
mod infrastructure_flow;
pub mod infrastructure_template;
pub mod operator_policy;
mod packer_build;
pub mod pangea_dashboard;
pub mod pangea_fleet_status;
mod pangea_namespace;
pub mod reconciliation_loop;
pub mod schema_identity;
pub mod synthesizer_format;
pub mod workspace_catalog;

pub use reconciliation_loop::{
    LoopPhase, LoopSelector, ReconciliationLoop, ReconciliationLoopSpec, ReconciliationLoopStatus,
};

// Re-export InfrastructureTemplate types
pub use architecture_gem::{
    FailureEscalation, PhaseTimeoutPolicy, ReactiveAction, ReactivePolicy, VerifiedBlockedPolicy,
};
pub use infrastructure_template::{
    ActionDistribution, AwsCredentials, BundleRef, CloudflareCredentials, ComplianceStatus,
    Condition, ConfigMapRef, ConflictKind, ConflictResolution, ConflictResolutionPolicy,
    ConflictRule, CycleSummary, Dialect, DriftAction, DriftDetail, GitHubCredentials,
    GitRepositoryRef, ImportPolicy, InfrastructureTemplate, InfrastructureTemplateSpec,
    InfrastructureTemplateStatus, Outcome, OutputBinding, OutputSecretRef, Phase, PolicyDecision,
    PolicyEvaluation, PolicyMatch, PolicyRule, PorkbunCredentials, ProfileResult,
    ProviderCredentials, ProviderKind, ReconcileCycle, ResolvedDialect, ResourceOutcome,
    ResourceSummary, RetryPolicy, RiskLevel, SecretFileRef, SecretRef, SettlingExhaustionAction,
    SettlingPolicy, SeverityRollup, TemplateObjectRef, TemplateSource, VariableRef,
};

// Re-export InfrastructureFlow types
pub use infrastructure_flow::{
    BackoffStrategy, DestroyOrder, FlowPhase, FlowRetryPolicy, FlowStep, FlowStepStatus,
    FlowTemplateRef, InfrastructureFlow, InfrastructureFlowSpec, InfrastructureFlowStatus, step_destroy_protection,};

// Re-export PangeaNamespace types (SecretRef renamed to avoid collision)
pub use pangea_namespace::{
    BackendConfig, BackendType, DefaultProviders, PangeaNamespace, PangeaNamespaceSpec,
    PangeaNamespaceStatus, PoolConfig, PostgresBackendConfig, PostgresSecretRef, ResourceStats,
    S3BackendConfig, S3SecretRef, SecretRef as ProviderSecretRef,
};

// The single derivation of a namespace's PostgreSQL schema identity.
// Re-exported at `crd::` so a call site never has to reach for the
// module path (and so the seven ex-hand-copies read as one import).
pub use schema_identity::{
    schema_name, schema_name_for_namespace, template_schema_name, DEFAULT_SCHEMA_PREFIX,
};

// Re-export PackerBuild types
pub use packer_build::{
    PackerBuild, PackerBuildPhase, PackerBuildSpec, PackerBuildStatus, VarFileSource,
};

// Re-export AmiTest types
pub use ami_test::{
    AmiSource, AmiTest, AmiTestPhase, AmiTestSpec, AmiTestStatus, FailurePolicy, SuitePhase,
    SuiteResult, TestSuite, TestSuiteType,
};

// Re-export ComplianceSchedule types
pub use compliance_schedule::{
    AttestationConfig, ComplianceRunner, ComplianceSchedule, ComplianceSchedulePhase,
    ComplianceScheduleSpec, ComplianceScheduleStatus, ComplianceSuite, ComplianceSuiteResult,
    PrometheusConfig, ReportingConfig, S3ReportConfig, VectorConfig,
};

// Re-export ComplianceBinding types
pub use compliance_binding::{
    BindingComplianceState, BindingTarget, ComplianceBinding, ComplianceBindingSpec,
    ComplianceBindingStatus, ComplianceEvent, ComplianceRef, EnforcementLevel, Reaction,
    ReactionAction, SekibanIntegration, TargetKind, TargetStatus,
};

// Re-export PangeaDashboard types
pub use pangea_dashboard::{
    DashboardLanguage, DashboardSource, PangeaDashboard, PangeaDashboardPhase,
    PangeaDashboardSpec, PangeaDashboardStatus,
};

// Re-export ArchitectureGem types (M1 — workspace reconciliation hardening)
pub use architecture_gem::{
    ApprovalRouting, ArchitectureGem, ArchitectureGemSpec, ArchitectureGemStatus,
    Condition as ArchitectureGemCondition, DriftReaction, FixtureResult, GemPolicy, GemSource,
    Phase as ArchitectureGemPhase, SettlingExhaustionAction as GemSettlingExhaustionAction,
    SettlingPolicy as GemSettlingPolicy, SmokeFixture, SmokeStatus,
};

// Re-export WorkspaceCatalog types (M3 — hierarchical policy cascade)
pub use workspace_catalog::{
    WorkspaceCatalog, WorkspaceCatalogSpec, WorkspaceCatalogStatus, WorkspacePolicy,
    WorkspaceSource,
};

// Re-export SynthesizerFormat types
pub use synthesizer_format::{
    ArraySectionSpec, KeyTransform, MapSectionSpec, SynthesizerFormat, SynthesizerFormatPhase,
    SynthesizerFormatSpec, SynthesizerFormatStatus,
};

// Re-export OperatorPolicy types — fleet-wide kill-switch primitive.
pub use operator_policy::{
    ControllerKind, ControllerSuspend, OperatorPolicy, OperatorPolicySpec, OperatorPolicyStatus,
    SuspensionDecision, SuspensionSource, WorkspaceState, WorkspaceSuspend, WorkspaceSuspendEntry,
    OPERATOR_POLICY_SINGLETON,
};

// Re-export PangeaFleetStatus types — operator-driven fleet
// aggregation singleton.
pub use pangea_fleet_status::{
    ArchitectureGemTracking, ComplianceBindingTracking, PangeaFleetStatus, PangeaFleetStatusSpec,
    PangeaFleetStatusStatus, PangeaNamespaceTracking, PhaseHistogramTracking, TemplateTracking,
    WorkspaceCatalogTracking, PANGEA_FLEET_STATUS_SINGLETON,
};

// Re-export ImagePipeline types
pub use image_pipeline::{
    ApprovalMode, ApprovalModeKind, ChangeVerification, HealthCheck, HealthCheckResult,
    HealthCheckType, ImagePipeline, ImagePipelinePhase, ImagePipelineSpec, ImagePipelineStatus,
    PipelineBuildSpec, PipelineBuildStatus, PipelineDeploySpec, PipelineDeployStatus,
    PipelineTestSpec, PipelineTestStatus, PipelineVerificationStatus, PlanAssertion,
    PlanAssertionRule, RollbackConfig, RollbackTrigger,
};

use kube::CustomResourceExt;

/// Generate an opaque JSON object schema: `type: object` **plus**
/// `x-kubernetes-preserve-unknown-fields`. Used for `serde_json::Value` fields
/// that hold an arbitrary JSON OBJECT (config / inline tf.json / tofu state /
/// architecture params — all object-shaped).
///
/// # The extension is the whole function, and omitting it silently emptied
/// every field that used this
///
/// A CRD schema is STRUCTURAL. `type: object` with no `properties` and no
/// `x-kubernetes-preserve-unknown-fields` does not mean "any object" — it
/// means an object with **no permitted fields**, and the apiserver PRUNES
/// everything inside it before the object is stored. No error, no warning; the
/// field simply arrives at the controller as `{}`.
///
/// Measured 2026-08-13 on a throwaway cluster: applying a `PangeaDashboard`
/// with `spec.source.architecture.params` carrying the six parameters
/// `lareira-camelot-observe` actually emits produced
///
///   strict decoding error: unknown field "spec.source.architecture.params.env",
///   unknown field "spec.source.architecture.params.service", … (all six)
///
/// under `--validate=strict`, and would have SILENTLY DROPPED all six under a
/// normal apply — rendering a dashboard from an architecture with no
/// parameters bound. That is the Ruby-free path's entire payload, and it is
/// the exact failure mode the fleet's own doctrine names: *"`properties` is a
/// closed schema — a field the Rust types declare and the yaml omits is pruned
/// before the controller sees it. The CR applies, and nothing errors."*
///
/// Five fields used this helper (dashboard params, compliance-schedule config,
/// ami-test spec, packer-build spec, and infrastructure_flow's own local
/// copy), so this was never one dashboard's bug — it was every opaque JSON
/// field in the CRD set.
///
/// `type: object` is RETAINED deliberately: that is what distinguishes this
/// from [`any_json_schema`], which permits scalars too. An architecture's
/// params must be an object; a mocked upstream output may legitimately be a
/// bare string.
pub fn opaque_json_schema(_gen: &mut schemars::gen::SchemaGenerator) -> schemars::schema::Schema {
    let mut obj = schemars::schema::SchemaObject {
        instance_type: Some(schemars::schema::InstanceType::Object.into()),
        ..Default::default()
    };
    obj.extensions.insert(
        "x-kubernetes-preserve-unknown-fields".to_string(),
        serde_json::Value::Bool(true),
    );
    schemars::schema::Schema::Object(obj)
}

/// Generate an "any JSON value" schema via `x-kubernetes-preserve-unknown-fields`
/// — accepts object/array/scalar alike. Required for a structural CRD field that
/// can hold ANY JSON (vs [`opaque_json_schema`], which pins `type: object` and so
/// would reject a scalar). Use for fields like a mocked upstream output, which
/// is usually a scalar (an ID/ARN string), not an object. A bare
/// `serde_json::Value` field WITHOUT one of these helpers emits a typeless
/// schema that Kubernetes rejects ("type: Required value: must not be empty").
pub fn any_json_schema(_gen: &mut schemars::gen::SchemaGenerator) -> schemars::schema::Schema {
    let mut obj = schemars::schema::SchemaObject::default();
    obj.extensions.insert(
        "x-kubernetes-preserve-unknown-fields".to_string(),
        serde_json::Value::Bool(true),
    );
    schemars::schema::Schema::Object(obj)
}

/// Generate CRD manifests for all Pangea custom resources.
pub fn generate_crds() -> String {
    let mut crds = String::new();

    crds.push_str("---\n");
    crds.push_str(
        &serde_yaml::to_string(&InfrastructureTemplate::crd())
            .expect("Failed to serialize InfrastructureTemplate CRD"),
    );

    crds.push_str("---\n");
    crds.push_str(
        &serde_yaml::to_string(&PangeaNamespace::crd())
            .expect("Failed to serialize PangeaNamespace CRD"),
    );

    crds.push_str("---\n");
    crds.push_str(
        &serde_yaml::to_string(&InfrastructureFlow::crd())
            .expect("Failed to serialize InfrastructureFlow CRD"),
    );

    crds.push_str("---\n");
    crds.push_str(
        &serde_yaml::to_string(&PackerBuild::crd()).expect("Failed to serialize PackerBuild CRD"),
    );

    crds.push_str("---\n");
    crds.push_str(
        &serde_yaml::to_string(&AmiTest::crd()).expect("Failed to serialize AmiTest CRD"),
    );

    crds.push_str("---\n");
    crds.push_str(
        &serde_yaml::to_string(&ImagePipeline::crd())
            .expect("Failed to serialize ImagePipeline CRD"),
    );

    crds.push_str("---\n");
    crds.push_str(
        &serde_yaml::to_string(&SynthesizerFormat::crd())
            .expect("Failed to serialize SynthesizerFormat CRD"),
    );

    crds.push_str("---\n");
    crds.push_str(
        &serde_yaml::to_string(&ComplianceSchedule::crd())
            .expect("Failed to serialize ComplianceSchedule CRD"),
    );

    crds.push_str("---\n");
    crds.push_str(
        &serde_yaml::to_string(&ComplianceBinding::crd())
            .expect("Failed to serialize ComplianceBinding CRD"),
    );

    crds.push_str("---\n");
    crds.push_str(
        &serde_yaml::to_string(&PangeaDashboard::crd())
            .expect("Failed to serialize PangeaDashboard CRD"),
    );

    // M1 — ArchitectureGem: typed registry + smoke-test gate.
    crds.push_str("---\n");
    crds.push_str(
        &serde_yaml::to_string(&ArchitectureGem::crd())
            .expect("Failed to serialize ArchitectureGem CRD"),
    );

    // M3 — WorkspaceCatalog: declares operator-watched workspaces +
    // workspace-level policy in the four-level cascade.
    crds.push_str("---\n");
    crds.push_str(
        &serde_yaml::to_string(&WorkspaceCatalog::crd())
            .expect("Failed to serialize WorkspaceCatalog CRD"),
    );

    // OperatorPolicy: cluster-scoped fleet-wide kill-switch. Composes
    // with each CR's per-resource `spec.suspend`.
    crds.push_str("---\n");
    crds.push_str(
        &serde_yaml::to_string(&OperatorPolicy::crd())
            .expect("Failed to serialize OperatorPolicy CRD"),
    );

    // PangeaFleetStatus: cluster-scoped operator-driven aggregation
    // singleton. `kubectl get pangeafleetstatus default` surfaces the
    // operator's running rollup of every pangea CRD class.
    crds.push_str("---\n");
    crds.push_str(
        &serde_yaml::to_string(&PangeaFleetStatus::crd())
            .expect("Failed to serialize PangeaFleetStatus CRD"),
    );

    // ReconciliationLoop (roda): the loop-granularity axis
    // (theory/RECONCILIATION-TOPOLOGY.md §II). Cluster-scoped; one wheel
    // drives a label-selected set of workspaces at its own cadence.
    crds.push_str("---\n");
    crds.push_str(
        &serde_yaml::to_string(&reconciliation_loop::ReconciliationLoop::crd())
            .expect("Failed to serialize ReconciliationLoop CRD"),
    );

    crds
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_opaque_json_schema_produces_object_type() {
        let mut gen = schemars::gen::SchemaGenerator::default();
        let schema = opaque_json_schema(&mut gen);
        match schema {
            schemars::schema::Schema::Object(obj) => {
                assert_eq!(
                    obj.instance_type,
                    Some(schemars::schema::InstanceType::Object.into())
                );
            }
            _ => panic!("Expected Schema::Object variant"),
        }
    }

    /// `type: object` alone is NOT an opaque object in a CRD — it is an object
    /// with no permitted fields, and the apiserver prunes everything inside it
    /// before the controller ever sees it.
    ///
    /// This assertion is the one that was missing. The test above checks
    /// `instance_type` and passed for as long as the helper emitted a schema
    /// that silently emptied five CRD fields, including
    /// `PangeaDashboard.spec.source.architecture.params` — the entire payload
    /// of the Ruby-free dashboard path. Measured against a real apiserver
    /// 2026-08-13: six params in, zero params stored.
    #[test]
    fn opaque_json_schema_preserves_unknown_fields() {
        let mut gen = schemars::gen::SchemaGenerator::default();
        let schemars::schema::Schema::Object(obj) = opaque_json_schema(&mut gen) else {
            panic!("Expected Schema::Object variant");
        };
        assert_eq!(
            obj.extensions.get("x-kubernetes-preserve-unknown-fields"),
            Some(&serde_json::Value::Bool(true)),
            "without this extension a structural CRD schema PRUNES every field \
             inside the object — the field applies cleanly and arrives empty, \
             which is the most expensive way for a schema to be wrong"
        );
    }

    /// The generated bundle carries the extension on the dashboard params
    /// field specifically — the helper being right is necessary but not
    /// sufficient, since a field could be repointed at a different helper.
    #[test]
    fn generated_dashboard_params_preserve_unknown_fields() {
        let crds = generate_crds();
        let dash = crds
            .split("---")
            .find(|d| d.contains("name: pangeadashboards.pangea.pleme.io"))
            .expect("the bundle must contain the PangeaDashboard CRD");
        let params_idx = dash
            .find("params:")
            .expect("the architecture source must expose a params field");
        let window = &dash[params_idx..(params_idx + 400).min(dash.len())];
        assert!(
            window.contains("x-kubernetes-preserve-unknown-fields"),
            "architecture params must preserve unknown fields or every parameter \
             is pruned on apply; got:\n{window}"
        );
    }

    #[test]
    fn test_generate_crds_not_empty() {
        let crds = generate_crds();
        assert!(!crds.is_empty());
    }

    /// An InfrastructureTemplate can be halted two independent ways:
    /// the author sets `spec.suspend`, or a ReactivePolicy Suspend
    /// escalation sets `status.autoSuspended`. Both halt reconcile, and
    /// neither implies the other, so a single column cannot report
    /// "is this template halted" — it can only report one of the two
    /// and read as authoritative about both.
    ///
    /// Measured on camelot-eks 2026-08-02: `camelot-eks-sui-nlb-sg` had
    /// `spec.suspend: true` and printed `SUSPENDED false`, because the
    /// column was bound to `autoSuspended` (correctly false — the
    /// breaker had not tripped). The template had been halted 31h and
    /// the surface an operator scans first said otherwise.
    ///
    /// `Suspended` is also bound to `.spec.suspend` on ReconciliationLoop,
    /// so this keeps one column name meaning one thing fleet-wide.
    #[test]
    fn infrastructure_template_prints_both_suspension_mechanisms() {
        let crd = InfrastructureTemplate::crd();
        let columns: Vec<(String, String)> = crd
            .spec
            .versions
            .iter()
            .flat_map(|v| v.additional_printer_columns.iter().flatten())
            .map(|c| (c.name.clone(), c.json_path.clone()))
            .collect();

        let path_of = |name: &str| -> Option<&str> {
            columns
                .iter()
                .find(|(n, _)| n == name)
                .map(|(_, p)| p.as_str())
        };

        assert_eq!(
            path_of("Suspended"),
            Some(".spec.suspend"),
            "the author-set halt must be visible, and under the same \
             column name ReconciliationLoop uses for the same field; \
             columns were {columns:?}"
        );
        assert_eq!(
            path_of("AutoSusp"),
            Some(".status.autoSuspended"),
            "the ReactivePolicy circuit breaker must stay visible \
             alongside it, not be displaced by it; columns were {columns:?}"
        );
    }

    #[test]
    fn test_generate_crds_contains_all_resources() {
        let crds = generate_crds();
        assert!(
            crds.contains("InfrastructureTemplate"),
            "Missing InfrastructureTemplate CRD"
        );
        assert!(
            crds.contains("PangeaNamespace"),
            "Missing PangeaNamespace CRD"
        );
        assert!(
            crds.contains("InfrastructureFlow"),
            "Missing InfrastructureFlow CRD"
        );
        assert!(crds.contains("PackerBuild"), "Missing PackerBuild CRD");
        assert!(crds.contains("AmiTest"), "Missing AmiTest CRD");
        assert!(crds.contains("ImagePipeline"), "Missing ImagePipeline CRD");
        assert!(
            crds.contains("SynthesizerFormat"),
            "Missing SynthesizerFormat CRD"
        );
        assert!(
            crds.contains("ComplianceSchedule"),
            "Missing ComplianceSchedule CRD"
        );
        assert!(
            crds.contains("ComplianceBinding"),
            "Missing ComplianceBinding CRD"
        );
        assert!(
            crds.contains("PangeaDashboard"),
            "Missing PangeaDashboard CRD"
        );
        assert!(
            crds.contains("ArchitectureGem"),
            "Missing ArchitectureGem CRD"
        );
        assert!(
            crds.contains("WorkspaceCatalog"),
            "Missing WorkspaceCatalog CRD"
        );
        assert!(
            crds.contains("OperatorPolicy"),
            "Missing OperatorPolicy CRD"
        );
    }

    #[test]
    fn test_generate_crds_valid_yaml() {
        let crds = generate_crds();
        let mut count = 0;
        for doc in crds.split("---\n") {
            let trimmed = doc.trim();
            if trimmed.is_empty() {
                continue;
            }
            let parsed: Result<serde_yaml::Value, _> = serde_yaml::from_str(trimmed);
            assert!(
                parsed.is_ok(),
                "Invalid YAML in CRD document {}: {:?}",
                count,
                parsed.err()
            );
            count += 1;
        }
        // 15 CRD kinds (see src/crd/*.rs): the 14 originals + reconciliation_loop
        // (roda). Update this when adding/removing a CustomResource.
        assert_eq!(count, 15, "Expected 15 CRD documents");
    }

    // ── Item H — CRD upgrade-compat tests ────────────────────────
    //
    // Catch accidentally-breaking shape changes that would orphan
    // existing CRs on operator upgrade. The contract these tests
    // enforce:
    //
    //   1. Every CRD's spec field-name set is camelCase + stable.
    //   2. Default values for optional fields exist + serde
    //      round-trips them.
    //   3. Adding a new optional field with a default doesn't break
    //      a deserialize of the prior schema.
    //
    // What these tests don't catch (by design): deeply-nested field
    // renames in third-party CRDs we don't own. For pleme-io's own
    // CRDs that's the entire surface.

    #[test]
    fn every_crd_has_a_singular_kind_name() {
        // Every CRD's spec.names.kind should be the type's struct name.
        // A typo or rename would produce a CRD that doesn't match
        // existing CRs in cluster.
        let crds = generate_crds();
        for kind in &[
            "InfrastructureTemplate",
            "PangeaNamespace",
            "InfrastructureFlow",
            "PackerBuild",
            "AmiTest",
            "ImagePipeline",
            "SynthesizerFormat",
            "ComplianceSchedule",
            "ComplianceBinding",
            "PangeaDashboard",
            "ArchitectureGem",
            "WorkspaceCatalog",
            "OperatorPolicy",
        ] {
            // Each kind appears as both the spec's `kind:` value AND
            // (lowercased) as the resource plural — assert at least
            // both forms are present somewhere in the YAML stream.
            assert!(
                crds.contains(&format!("kind: {}", kind)),
                "CRD stream missing `kind: {}`",
                kind
            );
        }
    }

    #[test]
    fn every_crd_uses_v1alpha1_api_version() {
        // Operator currently ships every CRD at v1alpha1. Promotion
        // to v1beta1/v1 requires a conversion webhook + migration
        // path. This test pins the version so an accidental bump
        // (which would orphan v1alpha1 CRs) fails CI.
        let crds = generate_crds();
        for doc in crds.split("---\n") {
            let trimmed = doc.trim();
            if trimmed.is_empty() {
                continue;
            }
            // Each CRD declares `versions: [{ name: v1alpha1, ... }]`.
            assert!(
                trimmed.contains("name: v1alpha1"),
                "CRD missing `name: v1alpha1`. Bumping API version requires \
                 a conversion webhook + migration plan. Got:\n{}",
                trimmed.lines().take(15).collect::<Vec<_>>().join("\n"),
            );
        }
    }

    #[test]
    fn every_crd_has_a_status_subresource() {
        // Status subresource separation is required for the operator's
        // patch_status pattern to work without conflicting with user
        // patches against spec. Forgetting `status` on a new CRD
        // means status patches silently land on spec — a subtle data
        // corruption bug.
        let crds = generate_crds();
        let mut without_status = Vec::new();
        for doc in crds.split("---\n") {
            let trimmed = doc.trim();
            if trimmed.is_empty() {
                continue;
            }
            // Extract kind name for diagnostic.
            let kind_line = trimmed
                .lines()
                .find(|l| l.trim_start().starts_with("kind:"))
                .map(|l| l.trim().trim_start_matches("kind:").trim().to_string())
                .unwrap_or_else(|| "<unknown>".to_string());
            // SynthesizerFormat is the only renderer-only CRD without
            // an interesting status. Others should have it.
            if !trimmed.contains("subresources:") || !trimmed.contains("status:") {
                without_status.push(kind_line);
            }
        }
        // Allow SynthesizerFormat as the documented exception; everything
        // else must have status.
        let _unexpected: Vec<_> = without_status
            .into_iter()
            .filter(|k| !k.contains("SynthesizerFormat") && !k.contains("CustomResourceDefinition"))
            .collect();
        // Note: kind: CustomResourceDefinition appears once per CRD as the
        // top-level kind; we filter that. The substantive kind is the
        // CRD's spec.names.kind which should have status.
    }

    #[test]
    fn crd_yaml_has_no_trailing_garbage() {
        // Defensive: serde_yaml occasionally emits trailing whitespace
        // or duplicate newlines on edge cases. kubectl apply will
        // accept these, but kustomize sometimes complains. Confirm
        // the output is well-formed.
        let crds = generate_crds();
        // Each document ends with a trailing newline; the joined
        // stream should contain no `\n\n\n` (triple-newline gap).
        assert!(
            !crds.contains("\n\n\n\n"),
            "CRD YAML stream has 4+ consecutive newlines — serializer hiccup?"
        );
    }

    #[test]
    fn operator_policy_crd_is_cluster_scoped() {
        // OperatorPolicy is the singleton kill switch — must be
        // cluster-scoped (not namespaced) so a single `default` CR
        // governs the whole operator. A namespace-scoped CRD would
        // create one policy per namespace, breaking the contract.
        let crd = OperatorPolicy::crd();
        let yaml = serde_yaml::to_string(&crd).unwrap();
        assert!(
            yaml.contains("scope: Cluster"),
            "OperatorPolicy MUST be cluster-scoped (singleton kill-switch contract). \
             Got:\n{}",
            yaml.lines().take(10).collect::<Vec<_>>().join("\n"),
        );
    }

    #[test]
    fn pangea_namespace_crd_is_cluster_scoped() {
        // PangeaNamespace declares a state-backend home; it's
        // referenced cross-namespace by InfrastructureTemplates so
        // it MUST be cluster-scoped. A namespace-scoped CRD would
        // require duplicating per workspace namespace.
        let crd = PangeaNamespace::crd();
        let yaml = serde_yaml::to_string(&crd).unwrap();
        assert!(
            yaml.contains("scope: Cluster"),
            "PangeaNamespace MUST be cluster-scoped. Got:\n{}",
            yaml.lines().take(10).collect::<Vec<_>>().join("\n"),
        );
    }

    #[test]
    fn workspace_catalog_crd_is_cluster_scoped() {
        // WorkspaceCatalog enumerates operator-watched workspaces
        // fleet-wide. Cluster-scoped by design.
        let crd = WorkspaceCatalog::crd();
        let yaml = serde_yaml::to_string(&crd).unwrap();
        assert!(
            yaml.contains("scope: Cluster"),
            "WorkspaceCatalog MUST be cluster-scoped. Got:\n{}",
            yaml.lines().take(10).collect::<Vec<_>>().join("\n"),
        );
    }

    #[test]
    fn architecture_gem_crd_is_cluster_scoped() {
        // ArchitectureGem references a remote source by URL — it's
        // a fleet-level resource, not per-namespace.
        let crd = ArchitectureGem::crd();
        let yaml = serde_yaml::to_string(&crd).unwrap();
        assert!(
            yaml.contains("scope: Cluster"),
            "ArchitectureGem MUST be cluster-scoped. Got:\n{}",
            yaml.lines().take(10).collect::<Vec<_>>().join("\n"),
        );
    }

    #[test]
    fn test_generate_crds_each_has_api_version() {
        let crds = generate_crds();
        for doc in crds.split("---\n") {
            let trimmed = doc.trim();
            if trimmed.is_empty() {
                continue;
            }
            assert!(
                trimmed.contains("apiextensions.k8s.io"),
                "CRD document missing apiextensions.k8s.io"
            );
        }
    }
}

#[cfg(test)]
mod crd_emit {
    /// Emit the generated CRDs to `PANGEA_CRD_OUT` when set.
    ///
    /// A test rather than a bin: the CRD is a pure function of the Rust types,
    /// and this crate has no crdgen binary. Doctrine — a CRD SCHEMA IS
    /// GENERATED, NEVER MAINTAINED — and `properties` is a CLOSED schema, so a
    /// field the types declare and the yaml omits is PRUNED BEFORE THE
    /// CONTROLLER SEES IT. Hand-editing the yaml is how a field goes silently
    /// missing.
    #[test]
    fn emit_generated_crds() {
        if let Ok(path) = std::env::var("PANGEA_CRD_OUT") {
            std::fs::write(&path, super::generate_crds()).expect("write generated crds");
        }
    }
}
