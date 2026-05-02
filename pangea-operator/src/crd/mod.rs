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
mod infrastructure_flow;
mod infrastructure_template;
pub mod image_pipeline;
pub mod operator_policy;
mod packer_build;
mod pangea_namespace;
pub mod synthesizer_format;
pub mod compliance_schedule;
pub mod compliance_binding;
pub mod pangea_dashboard;
pub mod workspace_catalog;

// Re-export InfrastructureTemplate types
pub use architecture_gem::{
    FailureEscalation, PhaseTimeoutPolicy, ReactiveAction, ReactivePolicy, VerifiedBlockedPolicy,
};
pub use infrastructure_template::{
    AwsCredentials, CloudflareCredentials, ComplianceStatus, Condition, ConfigMapRef, CycleSummary,
    DriftDetail, GitHubCredentials, GitRepositoryRef, ImportPolicy, InfrastructureTemplate,
    InfrastructureTemplateSpec, InfrastructureTemplateStatus, Outcome, Phase, PolicyDecision,
    PolicyEvaluation, PolicyMatch, PolicyRule, ProfileResult, ProviderCredentials, ProviderKind,
    ReconcileCycle, ResourceOutcome, ResourceSummary, RetryPolicy, SecretRef,
    SettlingExhaustionAction, SettlingPolicy, TemplateObjectRef, TemplateSource, VariableRef,
};

// Re-export InfrastructureFlow types
pub use infrastructure_flow::{
    BackoffStrategy, DestroyOrder, FlowPhase, FlowRetryPolicy, FlowStep, FlowStepStatus,
    FlowTemplateRef, InfrastructureFlow, InfrastructureFlowSpec, InfrastructureFlowStatus,
};

// Re-export PangeaNamespace types (SecretRef renamed to avoid collision)
pub use pangea_namespace::{
    BackendConfig, BackendType, DefaultProviders, PangeaNamespace, PangeaNamespaceSpec,
    PangeaNamespaceStatus, PoolConfig, PostgresBackendConfig, PostgresSecretRef, ResourceStats,
    S3BackendConfig, S3SecretRef, SecretRef as ProviderSecretRef,
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
    DashboardSource, PangeaDashboard, PangeaDashboardPhase, PangeaDashboardSpec,
    PangeaDashboardStatus,
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
    WorkspaceCatalog, WorkspaceCatalogSpec, WorkspaceCatalogStatus,
    WorkspacePolicy, WorkspaceSource,
};

// Re-export SynthesizerFormat types
pub use synthesizer_format::{
    ArraySectionSpec, KeyTransform, MapSectionSpec, SynthesizerFormat, SynthesizerFormatPhase,
    SynthesizerFormatSpec, SynthesizerFormatStatus,
};

// Re-export OperatorPolicy types — fleet-wide kill-switch primitive.
pub use operator_policy::{
    ControllerKind, ControllerSuspend, OperatorPolicy, OperatorPolicySpec,
    OperatorPolicyStatus, OPERATOR_POLICY_SINGLETON,
};

// Re-export ImagePipeline types
pub use image_pipeline::{
    ApprovalMode, ChangeVerification, HealthCheck, HealthCheckResult, HealthCheckType,
    ImagePipeline, ImagePipelinePhase, ImagePipelineSpec, ImagePipelineStatus, PipelineBuildSpec,
    PipelineBuildStatus, PipelineDeploySpec, PipelineDeployStatus, PipelineTestSpec,
    PipelineTestStatus, PipelineVerificationStatus, PlanAssertion, PlanAssertionRule,
    RollbackConfig, RollbackTrigger,
};

use kube::CustomResourceExt;

/// Generate an opaque JSON object schema (type: object with no properties).
/// Used for `serde_json::Value` fields that hold arbitrary JSON.
pub fn opaque_json_schema(_gen: &mut schemars::gen::SchemaGenerator) -> schemars::schema::Schema {
    schemars::schema::Schema::Object(schemars::schema::SchemaObject {
        instance_type: Some(schemars::schema::InstanceType::Object.into()),
        ..Default::default()
    })
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
        &serde_yaml::to_string(&PackerBuild::crd())
            .expect("Failed to serialize PackerBuild CRD"),
    );

    crds.push_str("---\n");
    crds.push_str(
        &serde_yaml::to_string(&AmiTest::crd())
            .expect("Failed to serialize AmiTest CRD"),
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

    #[test]
    fn test_generate_crds_not_empty() {
        let crds = generate_crds();
        assert!(!crds.is_empty());
    }

    #[test]
    fn test_generate_crds_contains_all_resources() {
        let crds = generate_crds();
        assert!(crds.contains("InfrastructureTemplate"), "Missing InfrastructureTemplate CRD");
        assert!(crds.contains("PangeaNamespace"), "Missing PangeaNamespace CRD");
        assert!(crds.contains("InfrastructureFlow"), "Missing InfrastructureFlow CRD");
        assert!(crds.contains("PackerBuild"), "Missing PackerBuild CRD");
        assert!(crds.contains("AmiTest"), "Missing AmiTest CRD");
        assert!(crds.contains("ImagePipeline"), "Missing ImagePipeline CRD");
        assert!(crds.contains("SynthesizerFormat"), "Missing SynthesizerFormat CRD");
        assert!(crds.contains("ComplianceSchedule"), "Missing ComplianceSchedule CRD");
        assert!(crds.contains("ComplianceBinding"), "Missing ComplianceBinding CRD");
        assert!(crds.contains("PangeaDashboard"), "Missing PangeaDashboard CRD");
        assert!(crds.contains("ArchitectureGem"), "Missing ArchitectureGem CRD");
        assert!(crds.contains("WorkspaceCatalog"), "Missing WorkspaceCatalog CRD");
        assert!(crds.contains("OperatorPolicy"), "Missing OperatorPolicy CRD");
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
            assert!(parsed.is_ok(), "Invalid YAML in CRD document {}: {:?}", count, parsed.err());
            count += 1;
        }
        assert_eq!(count, 13, "Expected 13 CRD documents");
    }

    #[test]
    fn test_generate_crds_each_has_api_version() {
        let crds = generate_crds();
        for doc in crds.split("---\n") {
            let trimmed = doc.trim();
            if trimmed.is_empty() {
                continue;
            }
            assert!(trimmed.contains("apiextensions.k8s.io"),
                "CRD document missing apiextensions.k8s.io");
        }
    }
}
