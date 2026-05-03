//! Controller for SynthesizerFormat resources.
//!
//! Validates format definitions, probes the compiler sidecar to confirm it can
//! materialize a synthesizer from the spec, and sets phase to Ready or Invalid.
//! Other CRDs (PackerBuild, future build types) reference a SynthesizerFormat
//! by name and block until it reaches Ready.

use crate::crd::synthesizer_format::{
    SynthesizerFormat, SynthesizerFormatPhase,
};
use crate::error::Error;

use futures::StreamExt;
use kube::{
    api::{Api, Patch, PatchParams},
    runtime::{
        controller::{Action, Controller},
        watcher::Config,
    },
    ResourceExt,
};
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, error, info, instrument, warn};

use super::{create_condition, ControllerState, DEFAULT_REQUEUE_INTERVAL};

pub struct SynthesizerFormatController {
    state: ControllerState,
}

impl SynthesizerFormatController {
    pub fn new(state: ControllerState) -> Self {
        Self { state }
    }

    pub async fn run(self) -> crate::error::Result<()> {
        let client = self.state.client.clone();
        let api: Api<SynthesizerFormat> = Api::all(client.clone());
        let state = Arc::new(self.state);

        info!("Starting SynthesizerFormat controller");

        Controller::new(api, Config::default())
            .run(
                move |format, ctx| {
                    let state = Arc::clone(&ctx);
                    async move { reconcile(format, state).await }
                },
                error_policy,
                state,
            )
            .for_each(|result| async move {
                match result {
                    Ok((obj, action)) => {
                        debug!(name = %obj.name, ?action, "SynthesizerFormat reconciliation completed");
                    }
                    Err(e) => {
                        error!(error = %e, "SynthesizerFormat reconciliation failed");
                    }
                }
            })
            .await;

        Ok(())
    }
}

#[instrument(skip(state), fields(name = %format.name_any()))]
async fn reconcile(
    format: Arc<SynthesizerFormat>,
    state: Arc<ControllerState>,
) -> std::result::Result<Action, Error> {
    state
        .metrics
        .record_reconcile(crate::crd::ControllerKind::SynthesizerFormat, "ok");
    let _name = format.name_any();

    info!("Reconciling SynthesizerFormat");

    // Cluster-wide kill-switch — honor `OperatorPolicy/default`.
    if let Some(action) = crate::controller::policy_pipeline::run_for_controller(
        &state,
        crate::crd::ControllerKind::SynthesizerFormat,
    )
    .into_skip_action()
    {
        return Ok(action);
    }

    let current_gen = format.metadata.generation.unwrap_or(0);
    let observed_gen = format
        .status
        .as_ref()
        .map(|s| s.observed_generation)
        .unwrap_or(0);

    // Skip if already reconciled at this generation
    if current_gen == observed_gen
        && format
            .status
            .as_ref()
            .and_then(|s| s.phase)
            .is_some()
    {
        return Ok(Action::requeue(DEFAULT_REQUEUE_INTERVAL));
    }

    // Step 1: Validate the spec locally
    if let Err(err) = validate_spec(&format.spec) {
        update_status(
            &format,
            SynthesizerFormatPhase::Invalid,
            Some(&err),
            &state,
        )
        .await?;
        return Ok(Action::requeue(DEFAULT_REQUEUE_INTERVAL));
    }

    // Step 2: Probe the compiler backend to confirm it can create the synthesizer
    match probe_sidecar(&format.spec, state.compiler_backend.as_ref()).await {
        Ok(()) => {
            info!("Sidecar probe succeeded, format is Ready");
            update_status(&format, SynthesizerFormatPhase::Ready, None, &state).await?;
        }
        Err(err) => {
            warn!(error = %err, "Sidecar probe failed");
            update_status(
                &format,
                SynthesizerFormatPhase::Invalid,
                Some(&format!("Sidecar probe failed: {err}")),
                &state,
            )
            .await?;
        }
    }

    Ok(Action::requeue(DEFAULT_REQUEUE_INTERVAL))
}

// ---------------------------------------------------------------------------
// Validation
// ---------------------------------------------------------------------------

fn validate_spec(
    spec: &crate::crd::synthesizer_format::SynthesizerFormatSpec,
) -> std::result::Result<(), String> {
    // Must have at least one section
    if spec.array_sections.is_empty() && spec.map_sections.is_empty() {
        return Err("SynthesizerFormat must define at least one section".into());
    }

    // No duplicate section names
    let mut names = HashSet::new();
    for s in &spec.array_sections {
        if !names.insert(&s.name) {
            return Err(format!("Duplicate section name: {}", s.name));
        }
        if s.name.is_empty() {
            return Err("Array section name cannot be empty".into());
        }
        if s.type_field.is_empty() {
            return Err(format!(
                "Array section '{}' has empty type_field",
                s.name
            ));
        }
    }
    for s in &spec.map_sections {
        if !names.insert(&s.name) {
            return Err(format!("Duplicate section name: {}", s.name));
        }
        if s.name.is_empty() {
            return Err("Map section name cannot be empty".into());
        }
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Sidecar probe
// ---------------------------------------------------------------------------

/// Send a minimal compile request to the sidecar with the format definition
/// and a no-op source to verify the synthesizer can be created.
async fn probe_sidecar(
    spec: &crate::crd::synthesizer_format::SynthesizerFormatSpec,
    backend: &dyn crate::ruby::CompilerBackend,
) -> std::result::Result<(), String> {
    let format_definition = spec.to_sidecar_definition();
    let req = crate::ruby::CompileAnyRequest {
        source: "# no-op probe".to_string(),
        variables: std::collections::HashMap::new(),
        format: None,
        format_definition: Some(format_definition),
    };
    backend
        .compile_any(req)
        .await
        .map(|_| ())
        .map_err(|e| format!("compile-any probe failed: {e}"))
}

// ---------------------------------------------------------------------------
// Status
// ---------------------------------------------------------------------------

async fn update_status(
    format: &SynthesizerFormat,
    phase: SynthesizerFormatPhase,
    error: Option<&str>,
    state: &ControllerState,
) -> std::result::Result<(), Error> {
    let name = format.name_any();
    let api: Api<SynthesizerFormat> = Api::all(state.client.clone());

    let spec = &format.spec;

    let array_count = spec.array_sections.len();
    let map_count = spec.map_sections.len();
    let summary = format!("{array_count} array, {map_count} map");

    // Compute DSL method names for documentation
    let mut methods: Vec<String> = Vec::new();
    for s in &spec.array_sections {
        let method = s
            .method_alias
            .clone()
            .unwrap_or_else(|| singularize(&s.name));
        methods.push(format!("synth.{}(:type, ...)", method));
    }
    for s in &spec.map_sections {
        let method = s
            .method_alias
            .clone()
            .unwrap_or_else(|| singularize(&s.name));
        methods.push(format!("synth.{}(:key, ...)", method));
    }

    let ready = phase == SynthesizerFormatPhase::Ready;
    let message = error.unwrap_or(if ready { "Format validated and ready" } else { "Validation failed" });

    let conditions = vec![
        create_condition("Ready", ready, &format!("{phase}"), message),
    ];

    let patch = serde_json::json!({
        "status": {
            "phase": phase,
            "sectionSummary": summary,
            "dslMethods": methods,
            "conditions": conditions,
            "observedGeneration": format.metadata.generation.unwrap_or(0),
            "error": error,
        }
    });

    api.patch_status(&name, &PatchParams::apply("pangea-operator"), &Patch::Merge(&patch))
        .await
        .map_err(Error::Kube)?;

    info!(%phase, "SynthesizerFormat status updated");
    Ok(())
}

fn singularize(word: &str) -> String {
    let w = word.to_string();
    if w.ends_with("ors") {
        w.strip_suffix('s').unwrap_or(&w).to_string()
    } else if w.ends_with("ers") {
        w.strip_suffix('s').unwrap_or(&w).to_string()
    } else if w.ends_with('s') {
        w.strip_suffix('s').unwrap_or(&w).to_string()
    } else {
        w
    }
}

fn error_policy(
    _format: Arc<SynthesizerFormat>,
    error: &Error,
    ctx: Arc<ControllerState>,
) -> Action {
    crate::controller::error_policy::run_error_policy(
        &ctx.metrics,
        crate::crd::ControllerKind::SynthesizerFormat,
        error,
        Duration::from_secs(60),
    )
}

#[cfg(test)]
mod tests {
    use crate::crd::synthesizer_format::{
        SynthesizerFormat, SynthesizerFormatPhase, SynthesizerFormatStatus,
    };
    use kube::CustomResourceExt;

    /// Phase enum serializes to its expected variant strings —
    /// guards against silent rename drift during refactors.
    #[test]
    fn phase_serde_round_trip() {
        let phases = [
            SynthesizerFormatPhase::Ready,
            SynthesizerFormatPhase::Invalid,
        ];
        for p in phases {
            let s = serde_json::to_string(&p).unwrap();
            let back: SynthesizerFormatPhase = serde_json::from_str(&s).unwrap();
            assert_eq!(format!("{:?}", p), format!("{:?}", back));
        }
    }

    /// Default Status renders without panic and is serde-compatible.
    #[test]
    fn status_default_round_trips() {
        let s = SynthesizerFormatStatus::default();
        let j = serde_json::to_string(&s).unwrap();
        let back: SynthesizerFormatStatus = serde_json::from_str(&j).unwrap();
        assert_eq!(format!("{:?}", s), format!("{:?}", back));
    }

    /// CR can be derived (kube CustomResource trait wires correctly).
    #[test]
    fn crd_yaml_renders_cleanly() {
        let yaml =
            serde_yaml::to_string(&SynthesizerFormat::crd()).expect("CRD serializes to YAML");
        // Must include the GVK + the singular name.
        assert!(yaml.contains("synthesizerformats.pangea.pleme.io"));
        assert!(yaml.contains("SynthesizerFormat"));
        // Category we just added.
        assert!(yaml.contains("- pangea"));
    }

    // ── D4 deep tests — pure helper coverage ──

    use super::singularize;

    #[test]
    fn singularize_handles_common_endings() {
        // The DSL's plural-to-singular helper drives the generated
        // method names. Drift here breaks consumer-facing API.
        assert_eq!(singularize("providers"), "provider");
        assert_eq!(singularize("workers"), "worker");
        assert_eq!(singularize("things"), "thing");
    }

    #[test]
    fn singularize_passes_through_short_strings() {
        // Words too short to have plural suffix shouldn't crash.
        assert_eq!(singularize(""), "");
        assert_eq!(singularize("a"), "a");
    }
}
