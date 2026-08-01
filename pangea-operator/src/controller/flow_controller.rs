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
        controller::Action,
        events::{Event, EventType, Recorder, Reporter},
    },
    Resource, ResourceExt,
};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, error, info, instrument, warn};

use super::{
    conditions_for_suspended, create_condition, ControllerState, DEFAULT_REQUEUE_INTERVAL,
    SHORT_REQUEUE_INTERVAL,
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
        let state = Arc::new(self.state);

        info!("Starting InfrastructureFlow controller");

        crate::controller::generation_filter::filtered_controller::<InfrastructureFlow>(client)
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

// `skip_all`, never an enumerated `skip(...)`. `#[instrument]` Debug-records
// every argument it is not told to skip, and the JSON formatter stamps the
// resulting span fields onto EVERY event inside the span. A large CR (a
// workspace carrying hundreds of DriftDetails) turned each ordinary INFO line
// into ~49KB; tracing's stdout writer is a synchronous mutex, so the reconcile
// path blocked tokio workers on log I/O until the liveness handler could not
// be scheduled inside the kubelet's 5s timeout and the pod was restarted
// mid-cycle, discarding ~22 minutes of plan+apply. See tests/
// instrument_never_dumps_a_cr.rs for the full account.
//
// An enumerated skip-list is not enough: it is a snapshot of today's
// arguments, so the next parameter added here silently reintroduces the bug.
// `skip_all` is total — nothing is ever auto-recorded — and the explicit
// `fields(...)` below carry the only identity anyone wanted.
#[instrument(skip_all, fields(name = %flow.name_any(), namespace = ?flow.namespace()))]
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
        // Diff-gate the suspend-skip status patch — matches the
        // template_controller fix (rio firefighting 2026-05-07).
        // Without this gate, every reconcile re-PATCHes the same
        // (type, status, reason, message) tuple with a fresh
        // `lastTransitionTime` (`create_condition` calls `Utc::now()`),
        // refiring the watch and creating a closed loop. Compare
        // observable fields and skip the PATCH when prev already
        // carries the suspended condition set.
        let new_conditions = conditions_for_suspended();
        let prev_conditions: &[crate::crd::Condition] = flow
            .status
            .as_ref()
            .map(|s| s.conditions.as_slice())
            .unwrap_or(&[]);
        let already_set = crate::controller::status::conditions_observably_equal(
            prev_conditions,
            &new_conditions,
        );
        if !already_set {
            let patch = serde_json::json!({ "status": { "conditions": new_conditions } });
            let _ =
                crate::controller::status_patch::patch_status(&*flow, &state.client, patch).await;
        } else {
            debug!("Suspended flow conditions already set; skipping status patch (avoids self-trigger watch loop)");
        }
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
    let template_api: Api<InfrastructureTemplate> =
        Api::namespaced(state.client.clone(), &namespace);
    let mut step_statuses: BTreeMap<String, FlowStepStatus> = BTreeMap::new();
    let mut step_outputs: BTreeMap<String, BTreeMap<String, serde_json::Value>> = BTreeMap::new();
    let mut ready_count = 0u32;
    let mut any_failed = false;
    // The flow phase is "Progressing" iff `ready_count != total_steps && !any_failed`,
    // which is computed at the end without needing a per-step accumulator.

    for step_name in &order {
        let template_name = flow.template_name_for_step(step_name);
        let step = flow
            .spec
            .steps
            .iter()
            .find(|s| &s.name == step_name)
            .unwrap();

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

                step_statuses.insert(
                    step_name.clone(),
                    FlowStepStatus {
                        phase: Some(phase.to_string()),
                        outputs,
                        template_name: Some(template_name.clone()),
                        dependencies_ready: deps_ready,
                        last_error: template.status.as_ref().and_then(|s| s.last_error.clone()),
                        state: None,
                        warmed_up: false,
                    },
                );
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
                                step_statuses.insert(
                                    step_name.clone(),
                                    FlowStepStatus {
                                        phase: Some("Pending".into()),
                                        outputs: None,
                                        template_name: None,
                                        dependencies_ready: true,
                                        last_error: Some(format!(
                                            "Variable resolution failed: {}",
                                            e
                                        )),
                                        state: None,
                                        warmed_up: false,
                                    },
                                );
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
                        warn!(
                            step = step_name,
                            "Step has no source and no existing template"
                        );
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
                            // Flow-spawned templates default auto_approve to
                            // true (see above) -- the GitOps-native manual
                            // approval field has nothing to do until a step
                            // explicitly opts into requireApproval, same
                            // rationale as the other None-defaulted fields
                            // below.
                            spec_approved_plan_hash: None,
                            refresh_interval: step
                                .refresh_interval
                                .clone()
                                .unwrap_or_else(|| "10m".into()),
                            suspend: false,
                            // Flow-spawned templates inherit the operator-wide
                            // executor default. Per-CR overrides happen on
                            // operator-authored templates only (M0.12).
                            executor: None,
                            destroy_protection: step
                                .destroy_protection
                                .unwrap_or(flow.spec.destroy_protection),
                            retry_policy: None,
                            provider_credentials: None,
                            compliance_profiles: vec![],
                            policies: vec![],
                            default_decision: None,
                            settling_policy: None,
                            reactive_policy: None,
                            import_policy: None,
                            import_hints: Default::default(),
                            conflict_policy: None,
                            output_bindings: Default::default(),
                            // Flow-spawned templates never carry ad hoc
                            // secret-file references — that's an
                            // operator-authored-template-only concern (same
                            // rationale as provider_credentials: None above).
                            secret_files: Default::default(),
                        },
                    );

                    match template_api.create(&PostParams::default(), &template).await {
                        Ok(_) => {
                            info!(
                                step = step_name,
                                template = template_name,
                                "Created template for flow step"
                            );
                            record_flow_event(
                                &flow,
                                &state,
                                EventType::Normal,
                                "StepCreated",
                                &format!(
                                    "Created template {} for step {}",
                                    template_name, step_name
                                ),
                            )
                            .await;
                        }
                        Err(e) => {
                            warn!(step = step_name, error = %e, "Failed to create template");
                        }
                    }
                }

                step_statuses.insert(
                    step_name.clone(),
                    FlowStepStatus {
                        phase: Some("Pending".into()),
                        outputs: None,
                        template_name: if deps_ready {
                            Some(template_name)
                        } else {
                            None
                        },
                        dependencies_ready: deps_ready,
                        last_error: None,
                        state: None,
                        warmed_up: false,
                    },
                );
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

    // Update flow status — but diff-gate the PATCH to avoid the
    // self-trigger watch loop pattern (see operator_policy_controller
    // header for the canonical bug-class description). `flow_conditions`
    // restamps `lastTransitionTime` on every call, so naive PATCHes
    // would refire the watch even when nothing observable changed.
    let new_conditions = flow_conditions(flow_phase);
    let new_observed_gen = flow.metadata.generation.unwrap_or(0);

    // We deliberately don't compare the per-step `steps` map in the
    // gate: its `FlowStepStatus` type does not derive PartialEq
    // (carries `serde_json::Value` for state snapshots), and its
    // content already changes whenever phase/ready_count change.
    // Phase + counts + conditions + observedGeneration are sufficient
    // to gate the self-trigger loop.
    let needs_patch = flow_status_needs_patch(
        flow.status.as_ref(),
        flow_phase,
        total_steps,
        ready_count,
        &new_conditions,
        new_observed_gen,
    );

    if needs_patch {
        let status = InfrastructureFlowStatus {
            phase: Some(flow_phase),
            total_steps,
            ready_steps: ready_count,
            steps: step_statuses,
            conditions: new_conditions,
            observed_generation: new_observed_gen,
            last_error: None,
        };
        let patch = serde_json::json!({ "status": status });
        crate::controller::status_patch::patch_status(&*flow, &state.client, patch).await?;
    } else {
        debug!("Flow status unchanged; skipping patch (avoids self-trigger watch loop)");
    }

    if flow_phase == FlowPhase::Ready {
        info!("All flow steps Ready");
        Ok(Action::requeue(DEFAULT_REQUEUE_INTERVAL))
    } else {
        Ok(Action::requeue(SHORT_REQUEUE_INTERVAL))
    }
}

/// Diff-gate for the InfrastructureFlow main reconcile status PATCH.
///
/// Returns `true` only when at least one observable status field would
/// change. Without this gate, every reconcile would re-PATCH the same
/// content with a fresh `lastTransitionTime` (`flow_conditions` calls
/// `create_condition`, which stamps `Utc::now()`), refiring the watch
/// and creating a closed-loop reconcile cycle. Conditions are compared
/// semantically — `(type, status, reason, message)` tuples — ignoring
/// the timestamp that's the source of churn.
///
/// The per-step `steps` map is intentionally omitted from this check:
/// its `FlowStepStatus` type does not derive `PartialEq` (carries
/// `serde_json::Value`), and its content already covaries with
/// `phase`/`ready_count`, so the omission cannot mask a real change.
fn flow_status_needs_patch(
    prev: Option<&InfrastructureFlowStatus>,
    new_phase: FlowPhase,
    new_total: u32,
    new_ready: u32,
    new_conditions: &[crate::crd::Condition],
    new_observed_gen: i64,
) -> bool {
    let prev_phase = prev.and_then(|s| s.phase);
    let prev_total = prev.map(|s| s.total_steps).unwrap_or(0);
    let prev_ready = prev.map(|s| s.ready_steps).unwrap_or(0);
    let prev_observed_gen = prev.map(|s| s.observed_generation).unwrap_or(0);
    let prev_conditions: &[crate::crd::Condition] =
        prev.map(|s| s.conditions.as_slice()).unwrap_or(&[]);

    let conditions_match =
        crate::controller::status::conditions_observably_equal(prev_conditions, new_conditions);

    !conditions_match
        || prev_phase != Some(new_phase)
        || prev_total != new_total
        || prev_ready != new_ready
        || prev_observed_gen != new_observed_gen
        || prev.is_none()
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
                return Err(format!(
                    "Step '{}' depends on unknown step '{}'",
                    step.name, dep
                ));
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

fn error_policy(_obj: Arc<InfrastructureFlow>, error: &Error, ctx: Arc<ControllerState>) -> Action {
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
            template_ref: crate::crd::FlowTemplateRef {
                name: None,
                source: None,
            },
            depends_on: deps.iter().map(|s| s.to_string()).collect(),
            variables: None,
            auto_approve: None,
            destroy_protection: None,
            refresh_interval: None,
            retry: None,
        }
    }

    // ── Diff-gate tests ─────────────────────────────────────────
    fn cond(typ: &str, status: &str, reason: &str, msg: &str) -> crate::crd::Condition {
        crate::crd::Condition {
            r#type: typ.into(),
            status: status.into(),
            reason: reason.into(),
            message: msg.into(),
            // Stale on purpose — the gate must compare semantically
            // and not be tricked by old timestamps.
            last_transition_time: chrono::TimeZone::with_ymd_and_hms(
                &chrono::Utc,
                2025,
                1,
                1,
                0,
                0,
                0,
            )
            .unwrap(),
        }
    }

    fn ready_status(observed_gen: i64) -> InfrastructureFlowStatus {
        InfrastructureFlowStatus {
            phase: Some(FlowPhase::Ready),
            total_steps: 3,
            ready_steps: 3,
            steps: BTreeMap::new(),
            conditions: flow_conditions(FlowPhase::Ready)
                .into_iter()
                .map(|c| cond(&c.r#type, &c.status, &c.reason, &c.message))
                .collect(),
            observed_generation: observed_gen,
            last_error: None,
        }
    }

    #[test]
    fn flow_status_first_reconcile_must_patch() {
        let new_conditions = flow_conditions(FlowPhase::Progressing);
        assert!(
            flow_status_needs_patch(None, FlowPhase::Progressing, 3, 1, &new_conditions, 1),
            "missing prev status must always force a PATCH"
        );
    }

    #[test]
    fn flow_status_phase_transition_must_patch() {
        let prev = ready_status(5);
        // Same generation, but phase regresses Ready → Progressing.
        let new_conditions = flow_conditions(FlowPhase::Progressing);
        assert!(
            flow_status_needs_patch(
                Some(&prev),
                FlowPhase::Progressing,
                3,
                2,
                &new_conditions,
                5
            ),
            "phase change must force a PATCH even if conditions slot in semantically"
        );
    }

    #[test]
    fn flow_status_observed_gen_advance_must_patch() {
        let prev = ready_status(5);
        let new_conditions = flow_conditions(FlowPhase::Ready);
        assert!(
            flow_status_needs_patch(Some(&prev), FlowPhase::Ready, 3, 3, &new_conditions, 6),
            "observed generation bump must force a PATCH"
        );
    }

    #[test]
    fn flow_status_steady_state_skips_patch() {
        // The rio loop case: nothing changed, only `lastTransitionTime`
        // would be re-stamped fresh. Gate must skip.
        let prev = ready_status(7);
        let new_conditions = flow_conditions(FlowPhase::Ready);
        assert!(
            !flow_status_needs_patch(Some(&prev), FlowPhase::Ready, 3, 3, &new_conditions, 7),
            "must NOT patch on timestamp-only churn — that's the loop bug"
        );
    }

    #[test]
    fn flow_status_ready_count_change_must_patch() {
        let prev = ready_status(5);
        let new_conditions = flow_conditions(FlowPhase::Progressing);
        assert!(
            flow_status_needs_patch(
                Some(&prev),
                FlowPhase::Progressing,
                3,
                2,
                &new_conditions,
                5
            ),
            "ready_count change must force a PATCH"
        );
    }

    #[test]
    fn test_toposort_simple() {
        let steps = vec![step("a", &[]), step("b", &["a"]), step("c", &["b"])];
        let order = toposort(&steps).unwrap();
        assert_eq!(order, vec!["a", "b", "c"]);
    }

    #[test]
    fn test_toposort_parallel() {
        let steps = vec![
            step("a", &[]),
            step("b", &["a"]),
            step("c", &["a"]),
            step("d", &["b", "c"]),
        ];
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
        for phase in [
            FlowPhase::Pending,
            FlowPhase::Progressing,
            FlowPhase::Ready,
            FlowPhase::Failed,
            FlowPhase::Destroying,
        ] {
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
