//! Controller for InfrastructureFlow resources.
//!
//! Orchestrates multi-template deployments as a DAG. Each flow step
//! becomes an InfrastructureTemplate, with dependencies controlling
//! execution order and output references resolved between steps.

use crate::crd::{
    FlowPhase, FlowStepStatus, InfrastructureFlow, InfrastructureFlowStatus,
    InfrastructureTemplate, InfrastructureTemplateSpec, Phase,
};
use crate::error::{Error, Result};
use crate::executor::variable_resolver;

use futures::StreamExt;
use kube::{
    api::{Api, PostParams},
    runtime::{
        controller::{Action, Controller},
        events::{Event, EventType, Recorder, Reporter},
        watcher::Config,
    },
    Resource, ResourceExt,
};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, error, info, instrument, warn};

use super::{
    conditions_for_suspended, create_condition, ControllerState,
    DEFAULT_REQUEUE_INTERVAL, SHORT_REQUEUE_INTERVAL,
};

/// Controller for InfrastructureFlow resources.
pub struct FlowController {
    state: ControllerState,
}

impl FlowController {
    pub fn new(state: ControllerState) -> Self {
        Self { state }
    }

    pub async fn run(self) -> Result<()> {
        let client = self.state.client.clone();
        let api: Api<InfrastructureFlow> = Api::all(client.clone());
        let state = Arc::new(self.state);

        info!("Starting InfrastructureFlow controller");

        Controller::new(api, Config::default())
            .run(
                move |flow, ctx| {
                    let state = Arc::clone(&ctx);
                    async move { reconcile_flow(flow, state).await }
                },
                error_policy,
                state,
            )
            .for_each(|result| async move {
                match result {
                    Ok((obj, action)) => {
                        debug!(name = %obj.name, namespace = ?obj.namespace, ?action, "Flow reconciliation completed");
                    }
                    Err(e) => {
                        error!(error = %e, "Flow reconciliation failed");
                    }
                }
            })
            .await;

        Ok(())
    }
}

#[instrument(skip(state), fields(name = %flow.name_any(), namespace = ?flow.namespace()))]
async fn reconcile_flow(
    flow: Arc<InfrastructureFlow>,
    state: Arc<ControllerState>,
) -> std::result::Result<Action, Error> {
    state
        .metrics
        .record_reconcile(crate::crd::ControllerKind::Flow, "ok");

    info!("Reconciling InfrastructureFlow");

    // Cluster-wide kill-switch — honor `OperatorPolicy/default`.
    if let Some(action) = crate::controller::policy_pipeline::run_for_controller(
        &state,
        crate::crd::ControllerKind::Flow,
    )
    .into_skip_action()
    {
        return Ok(action);
    }

    if flow.spec.suspend {
        let conditions = conditions_for_suspended();
        let patch = serde_json::json!({ "status": { "conditions": conditions } });
        let _ = crate::controller::status_patch::patch_status(&*flow, &state.client, patch).await;
        return Ok(Action::requeue(DEFAULT_REQUEUE_INTERVAL));
    }

    let total_steps = flow.spec.steps.len() as u32;

    // Topological sort
    let order = match toposort(&flow.spec.steps) {
        Ok(order) => order,
        Err(e) => {
            update_flow_phase(&flow, FlowPhase::Failed, Some(&e), &state).await?;
            return Ok(Action::requeue(DEFAULT_REQUEUE_INTERVAL));
        }
    };

    // Collect current step statuses from managed templates
    let namespace = flow.namespace().unwrap_or_default();
    let template_api: Api<InfrastructureTemplate> = Api::namespaced(state.client.clone(), &namespace);
    let mut step_statuses: BTreeMap<String, FlowStepStatus> = BTreeMap::new();
    let mut step_outputs: BTreeMap<String, BTreeMap<String, serde_json::Value>> = BTreeMap::new();
    let mut ready_count = 0u32;
    let mut any_failed = false;
    // The flow phase is "Progressing" iff `ready_count != total_steps && !any_failed`,
    // which is computed at the end without needing a per-step accumulator.

    for step_name in &order {
        let template_name = flow.template_name_for_step(step_name);
        let step = flow.spec.steps.iter().find(|s| &s.name == step_name).unwrap();

        // Check dependencies
        let deps_ready = step.depends_on.iter().all(|dep| {
            step_statuses
                .get(dep)
                .and_then(|s| s.phase.as_deref())
                .map(|p| p == "Ready")
                .unwrap_or(false)
        });

        // Check template status
        match template_api.get_opt(&template_name).await {
            Ok(Some(template)) => {
                let phase = template
                    .status
                    .as_ref()
                    .and_then(|s| s.phase)
                    .unwrap_or(Phase::Pending);

                let outputs = template.status.as_ref().and_then(|s| s.outputs.clone());

                if phase == Phase::Ready {
                    ready_count += 1;
                    if let Some(ref out) = outputs {
                        step_outputs.insert(step_name.clone(), out.clone());
                    }
                } else if phase == Phase::Failed {
                    any_failed = true;
                }
                // Progressing is the default — derived from the final
                // ready_count + any_failed at the bottom of this fn.

                step_statuses.insert(step_name.clone(), FlowStepStatus {
                    phase: Some(phase.to_string()),
                    outputs,
                    template_name: Some(template_name.clone()),
                    dependencies_ready: deps_ready,
                    last_error: template.status.as_ref().and_then(|s| s.last_error.clone()),
                    state: None,
                    warmed_up: false,
                });
            }
            Ok(None) => {
                // Template doesn't exist yet — create it if deps are ready
                if deps_ready {
                    // Resolve variable references
                    let variables = if let Some(vars) = &step.variables {
                        match variable_resolver::resolve_step_references(vars, &step_outputs) {
                            Ok(resolved) => Some(resolved),
                            Err(e) => {
                                warn!(step = step_name, error = %e, "Failed to resolve variables");
                                step_statuses.insert(step_name.clone(), FlowStepStatus {
                                    phase: Some("Pending".into()),
                                    outputs: None,
                                    template_name: None,
                                    dependencies_ready: true,
                                    last_error: Some(format!("Variable resolution failed: {}", e)),
                                    state: None,
                                    warmed_up: false,
                                });
                                continue;
                            }
                        }
                    } else {
                        None
                    };

                    // Create the template
                    let source = if let Some(ref src) = step.template_ref.source {
                        src.clone()
                    } else {
                        warn!(step = step_name, "Step has no source and no existing template");
                        continue;
                    };

                    let template = InfrastructureTemplate::new(
                        &template_name,
                        InfrastructureTemplateSpec {
                            source,
                            pangea_namespace: flow.spec.pangea_namespace.clone(),
                            template_name: None,
                            variables,
                            variable_refs: None,
                            auto_approve: step.auto_approve.unwrap_or(true),
                            refresh_interval: step.refresh_interval.clone().unwrap_or_else(|| "10m".into()),
                            suspend: false,
                            destroy_protection: step.destroy_protection.unwrap_or(flow.spec.destroy_protection),
                            retry_policy: None,
                            provider_credentials: None,
                            compliance_profiles: vec![],
                            policies: vec![],
                            default_decision: None,
                            settling_policy: None,
                            reactive_policy: None,
                            import_policy: None,
                            import_hints: Default::default(),
                            output_bindings: Default::default(),
                        },
                    );

                    match template_api.create(&PostParams::default(), &template).await {
                        Ok(_) => {
                            info!(step = step_name, template = template_name, "Created template for flow step");
                            record_flow_event(&flow, &state, EventType::Normal, "StepCreated", &format!("Created template {} for step {}", template_name, step_name)).await;
                        }
                        Err(e) => {
                            warn!(step = step_name, error = %e, "Failed to create template");
                        }
                    }
                }

                step_statuses.insert(step_name.clone(), FlowStepStatus {
                    phase: Some("Pending".into()),
                    outputs: None,
                    template_name: if deps_ready { Some(template_name) } else { None },
                    dependencies_ready: deps_ready,
                    last_error: None,
                    state: None,
                    warmed_up: false,
                });
            }
            Err(e) => {
                warn!(step = step_name, error = %e, "Failed to get template");
            }
        }
    }

    // Determine flow phase
    let flow_phase = if ready_count == total_steps {
        FlowPhase::Ready
    } else if any_failed {
        FlowPhase::Failed
    } else {
        FlowPhase::Progressing
    };

    // Update flow status
    let conditions = flow_conditions(flow_phase);
    let status = InfrastructureFlowStatus {
        phase: Some(flow_phase),
        total_steps,
        ready_steps: ready_count,
        steps: step_statuses,
        conditions,
        observed_generation: flow.metadata.generation.unwrap_or(0),
        last_error: None,
    };

    let patch = serde_json::json!({ "status": status });
    crate::controller::status_patch::patch_status(&*flow, &state.client, patch).await?;

    if flow_phase == FlowPhase::Ready {
        info!("All flow steps Ready");
        Ok(Action::requeue(DEFAULT_REQUEUE_INTERVAL))
    } else {
        Ok(Action::requeue(SHORT_REQUEUE_INTERVAL))
    }
}

/// Topological sort of flow steps by dependencies.
fn toposort(steps: &[crate::crd::FlowStep]) -> std::result::Result<Vec<String>, String> {
    let step_names: Vec<String> = steps.iter().map(|s| s.name.clone()).collect();
    let mut in_degree: BTreeMap<String, usize> = BTreeMap::new();
    let mut adj: BTreeMap<String, Vec<String>> = BTreeMap::new();

    for step in steps {
        in_degree.entry(step.name.clone()).or_insert(0);
        adj.entry(step.name.clone()).or_default();
        for dep in &step.depends_on {
            if !step_names.contains(dep) {
                return Err(format!("Step '{}' depends on unknown step '{}'", step.name, dep));
            }
            adj.entry(dep.clone()).or_default().push(step.name.clone());
            *in_degree.entry(step.name.clone()).or_insert(0) += 1;
        }
    }

    let mut queue: Vec<String> = in_degree
        .iter()
        .filter(|(_, &deg)| deg == 0)
        .map(|(name, _)| name.clone())
        .collect();
    queue.sort(); // deterministic

    let mut order = Vec::new();
    while let Some(node) = queue.pop() {
        order.push(node.clone());
        if let Some(neighbors) = adj.get(&node) {
            for neighbor in neighbors {
                if let Some(deg) = in_degree.get_mut(neighbor) {
                    *deg -= 1;
                    if *deg == 0 {
                        queue.push(neighbor.clone());
                        queue.sort();
                    }
                }
            }
        }
    }

    if order.len() != steps.len() {
        return Err("Cycle detected in flow step dependencies".into());
    }

    Ok(order)
}

fn flow_conditions(phase: FlowPhase) -> Vec<crate::crd::Condition> {
    let ready = phase == FlowPhase::Ready;
    let progressing = phase == FlowPhase::Progressing;
    let reason = format!("{}", phase);
    let message: String = match phase {
        FlowPhase::Ready => "All steps completed successfully".into(),
        FlowPhase::Progressing => "Steps are being deployed".into(),
        FlowPhase::Failed => "One or more steps failed".into(),
        FlowPhase::Pending => "Waiting to start".into(),
        FlowPhase::Destroying => "Flow is being destroyed".into(),
    };

    vec![
        create_condition("Ready", ready, &reason, &message),
        create_condition("Progressing", progressing, &reason, &message),
    ]
}

async fn update_flow_phase(
    flow: &InfrastructureFlow,
    phase: FlowPhase,
    error: Option<&str>,
    state: &ControllerState,
) -> Result<()> {
    let patch = serde_json::json!({
        "status": {
            "phase": phase,
            "conditions": flow_conditions(phase),
            "lastError": error,
        }
    });

    crate::controller::status_patch::patch_status(flow, &state.client, patch).await?;
    Ok(())
}

async fn record_flow_event(
    flow: &InfrastructureFlow,
    state: &ControllerState,
    event_type: EventType,
    reason: &str,
    message: &str,
) {
    let reporter = Reporter {
        controller: "pangea-operator".into(),
        instance: std::env::var("POD_NAME").ok(),
    };
    let recorder = Recorder::new(state.client.clone(), reporter);
    let obj_ref = flow.object_ref(&());
    let event = Event {
        type_: event_type,
        reason: reason.into(),
        note: Some(message.into()),
        action: reason.into(),
        secondary: None,
    };
    if let Err(e) = recorder.publish(&event, &obj_ref).await {
        warn!(error = %e, "Failed to record flow event");
    }
}

fn error_policy(
    _obj: Arc<InfrastructureFlow>,
    error: &Error,
    ctx: Arc<ControllerState>,
) -> Action {
    crate::controller::error_policy::run_error_policy(
        &ctx.metrics,
        crate::crd::ControllerKind::Flow,
        error,
        Duration::from_secs(60),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn step(name: &str, deps: &[&str]) -> crate::crd::FlowStep {
        crate::crd::FlowStep {
            name: name.into(),
            template_ref: crate::crd::FlowTemplateRef { name: None, source: None },
            depends_on: deps.iter().map(|s| s.to_string()).collect(),
            variables: None,
            auto_approve: None,
            destroy_protection: None,
            refresh_interval: None,
            retry: None,
        }
    }

    #[test]
    fn test_toposort_simple() {
        let steps = vec![step("a", &[]), step("b", &["a"]), step("c", &["b"])];
        let order = toposort(&steps).unwrap();
        assert_eq!(order, vec!["a", "b", "c"]);
    }

    #[test]
    fn test_toposort_parallel() {
        let steps = vec![step("a", &[]), step("b", &["a"]), step("c", &["a"]), step("d", &["b", "c"])];
        let order = toposort(&steps).unwrap();
        assert_eq!(order[0], "a");
        assert_eq!(order[3], "d");
        // b and c can be in either order
        assert!(order[1] == "b" || order[1] == "c");
    }

    #[test]
    fn test_toposort_cycle() {
        let steps = vec![step("a", &["b"]), step("b", &["a"])];
        assert!(toposort(&steps).is_err());
    }

    #[test]
    fn test_toposort_unknown_dep() {
        let steps = vec![step("a", &["missing"])];
        assert!(toposort(&steps).is_err());
    }

    #[test]
    fn test_toposort_empty() {
        let steps: Vec<crate::crd::FlowStep> = vec![];
        let order = toposort(&steps).unwrap();
        assert!(order.is_empty());
    }

    #[test]
    fn test_toposort_single_step() {
        let steps = vec![step("only", &[])];
        let order = toposort(&steps).unwrap();
        assert_eq!(order, vec!["only"]);
    }

    #[test]
    fn test_toposort_self_cycle() {
        let steps = vec![step("a", &["a"])];
        assert!(toposort(&steps).is_err());
    }

    #[test]
    fn test_toposort_three_node_cycle() {
        let steps = vec![step("a", &["c"]), step("b", &["a"]), step("c", &["b"])];
        assert!(toposort(&steps).is_err());
    }

    #[test]
    fn test_toposort_wide_fan_out() {
        let steps = vec![
            step("root", &[]),
            step("leaf1", &["root"]),
            step("leaf2", &["root"]),
            step("leaf3", &["root"]),
            step("leaf4", &["root"]),
        ];
        let order = toposort(&steps).unwrap();
        assert_eq!(order[0], "root");
        assert_eq!(order.len(), 5);
    }

    #[test]
    fn test_toposort_diamond_converges() {
        let steps = vec![
            step("a", &[]),
            step("b", &["a"]),
            step("c", &["a"]),
            step("d", &["b", "c"]),
        ];
        let order = toposort(&steps).unwrap();
        assert_eq!(order[0], "a");
        assert_eq!(order[3], "d");
    }

    #[test]
    fn test_toposort_deterministic() {
        let steps = vec![step("c", &[]), step("a", &[]), step("b", &[])];
        let order1 = toposort(&steps).unwrap();
        let order2 = toposort(&steps).unwrap();
        assert_eq!(order1, order2);
    }

    #[test]
    fn test_flow_conditions_all_phases() {
        for phase in [FlowPhase::Pending, FlowPhase::Progressing, FlowPhase::Ready, FlowPhase::Failed, FlowPhase::Destroying] {
            let conditions = flow_conditions(phase);
            assert_eq!(conditions.len(), 2);
            assert_eq!(conditions[0].r#type, "Ready");
            assert_eq!(conditions[1].r#type, "Progressing");
        }
    }

    #[test]
    fn test_flow_conditions_ready_phase() {
        let conditions = flow_conditions(FlowPhase::Ready);
        assert_eq!(conditions[0].status, "True");
        assert_eq!(conditions[1].status, "False");
    }

    #[test]
    fn test_flow_conditions_progressing_phase() {
        let conditions = flow_conditions(FlowPhase::Progressing);
        assert_eq!(conditions[0].status, "False");
        assert_eq!(conditions[1].status, "True");
    }

    #[test]
    fn test_flow_conditions_failed_phase() {
        let conditions = flow_conditions(FlowPhase::Failed);
        assert_eq!(conditions[0].status, "False");
        assert_eq!(conditions[1].status, "False");
    }
}
