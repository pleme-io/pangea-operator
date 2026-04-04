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
mod infrastructure_flow;
mod infrastructure_template;
pub mod image_pipeline;
mod packer_build;
mod pangea_namespace;
pub mod synthesizer_format;
pub mod compliance_schedule;
pub mod compliance_binding;

// Re-export InfrastructureTemplate types
pub use infrastructure_template::{
    AwsCredentials, CloudflareCredentials, ComplianceStatus, Condition, ConfigMapRef,
    GitRepositoryRef, InfrastructureTemplate, InfrastructureTemplateSpec,
    InfrastructureTemplateStatus, Phase, ProfileResult, ProviderCredentials, ResourceSummary,
    RetryPolicy, SecretRef, TemplateObjectRef, TemplateSource, VariableRef,
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

// Re-export SynthesizerFormat types
pub use synthesizer_format::{
    ArraySectionSpec, KeyTransform, MapSectionSpec, SynthesizerFormat, SynthesizerFormatPhase,
    SynthesizerFormatSpec, SynthesizerFormatStatus,
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

    crds
}
