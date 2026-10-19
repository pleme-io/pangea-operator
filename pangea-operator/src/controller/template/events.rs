use crate::controller::event_sink::{reference_key, Delivery};
use crate::controller::ControllerState;
use crate::crd::InfrastructureTemplate;
use kube::runtime::events::{Event, EventType};
use kube::Resource;
use tracing::warn;

pub async fn record_event(
    template: &InfrastructureTemplate,
    state: &ControllerState,
    event_type: EventType,
    reason: &str,
    message: &str,
) {
    let obj_ref = template.object_ref(&());
    let event = Event {
        type_: event_type,
        reason: reason.into(),
        note: Some(message.into()),
        action: reason.into(),
        secondary: None,
    };
    let key = reference_key(&obj_ref);
    let prior = template
        .status
        .as_ref()
        .map(|s| s.recent_events.as_slice())
        .unwrap_or(&[]);
    state.events.seed_ring(&key, prior);
    let delivery = state.events.record(&obj_ref, &event).await;
    state
        .metrics
        .live
        .record_event_delivery(delivery.outcome_label());
    if let Delivery::Ring { .. } = delivery {
        let patch = serde_json::json!({
            "status": { "recentEvents": state.events.ring(&key) }
        });
        if let Err(e) =
            crate::controller::status_patch::patch_status(template, &state.client, patch).await
        {
            warn!(error = %e, reason, "event ring could not be written to status.recentEvents");
        }
    }
}
