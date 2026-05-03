//! Kubernetes Event recording helper for `InfrastructureTemplate`.
//!
//! Lifted from `template_controller.rs` during R6. The reconciler
//! emits Events on phase transitions so operators can `kubectl
//! describe template <name>` to see the audit trail. The helper
//! swallows publish errors (Events are best-effort observability,
//! not load-bearing) but logs them at warn so the loss is visible.

use crate::controller::ControllerState;
use crate::crd::InfrastructureTemplate;
use kube::runtime::events::{Event, EventType, Recorder, Reporter};
use kube::Resource;
use tracing::warn;

/// Record a Kubernetes Event on the template. Best-effort: a publish
/// failure logs at `warn` and is dropped (the reconcile continues).
pub async fn record_event(
    template: &InfrastructureTemplate,
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
    let obj_ref = template.object_ref(&());
    let event = Event {
        type_: event_type,
        reason: reason.into(),
        note: Some(message.into()),
        action: reason.into(),
        secondary: None,
    };
    if let Err(e) = recorder.publish(&event, &obj_ref).await {
        warn!(error = %e, "Failed to record event");
    }
}
