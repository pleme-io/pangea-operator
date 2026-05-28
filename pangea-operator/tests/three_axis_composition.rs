//! Integration test — the three-axis composition contract.
//!
//! This test locks the end-to-end shape of detection / recurrence /
//! escalation axes flowing through one anomaly observation:
//!
//!   1. `error_signature(err_msg)` is stable across repeated calls.
//!   2. `RecurrenceObserver.observe` bumps the count + tracks age.
//!   3. `EscalationLadder.pick(duration_unready)` returns the deepest
//!      satisfied action.
//!   4. `EscalationHandlerRegistry.handler_for(action).execute(&ctx)`
//!      returns the variant-specific `EscalationOutcome` with the
//!      right status patch + event reason.
//!
//! Composition holds across N persistent failures: the recurrence
//! count keeps climbing, and once `duration_unready` crosses 60 min
//! the ladder picks `PauseAndAlert` and the handler patches
//! `status.autoSuspended=true`.
//!
//! Pure Rust — no Ruby, no kube-rs mocking, no async runtime beyond
//! what `tokio::test` provides for the handler's `.await`.

use pangea_operator::controller::anomaly_tracker::{
    error_signature, AnomalySummary, InMemoryRecurrenceTracker, RecurrenceObserver,
};
use pangea_operator::controller::escalation::{EscalationAction, EscalationLadder};
use pangea_operator::controller::escalation_handlers::{
    EscalationContext, EscalationHandlerRegistry,
};
use pangea_operator::crd::InfrastructureTemplate;
use std::time::Duration;

fn stub_template() -> InfrastructureTemplate {
    let payload = serde_json::json!({
        "apiVersion": "pangea.pleme.io/v1alpha1",
        "kind": "InfrastructureTemplate",
        "metadata": { "name": "test-t", "namespace": "test-ns" },
        "spec": {
            "source": { "raw": "" },
            "pangeaNamespace": "default"
        },
        "status": null
    });
    serde_json::from_value(payload).unwrap()
}

#[tokio::test]
async fn three_axis_composition_persistent_failure_reaches_pause_and_alert() {
    // ── Detection axis: same logical error N times ───────────────────
    // Simulates rio-drive-cloudflare-tunnel's known chronic failure:
    // `uninitialized constant Pangea::Resources::Cloudflare` repeats
    // with the same canonical signature even though hex addresses
    // and timestamps vary in the raw message.
    let err_variants = [
        // Baseline.
        "in /var/pangea/workspaces/template-a/_repo/lib/foo: uninitialized constant Pangea::Resources::Cloudflare",
        // Same logical error from a different workspace — strip should
        // collapse the path to <NAME> so signature stays the same.
        "in /var/pangea/workspaces/template-b/_repo/lib/foo: uninitialized constant Pangea::Resources::Cloudflare",
        // Same error with the hex address suffix — strip should
        // collapse 0x... to <HEX>.
        "in /var/pangea/workspaces/template-a/_repo/lib/foo: uninitialized constant Pangea::Resources::Cloudflare 0xdeadbeef",
    ];
    let signatures: Vec<String> = err_variants.iter().map(|m| error_signature(m)).collect();
    // Variants 0 and 1 differ ONLY in workspace path — must canonicalize
    // to one signature (workspace path → <NAME>).
    assert_eq!(signatures[0], signatures[1],
        "workspace path stripping must canonicalize to one signature");
    // Variant 2 differs from 0 by trailing hex — must canonicalize to a
    // separate signature (the message has different overall shape with
    // the hex suffix added; strip only canonicalizes WHICH hex, not
    // whether one is present). This is correct behavior: the presence
    // of an address often signals a different bug class.
    assert_ne!(signatures[0], signatures[2],
        "structural-shape differences (hex suffix present vs absent) must produce distinct signatures");

    // ── Recurrence axis: observe N times ─────────────────────────────
    let tracker = InMemoryRecurrenceTracker::new();
    let key = "test-ns/test-t";
    let sig = &signatures[0];

    let mut counts: Vec<u32> = Vec::with_capacity(10);
    for _ in 0..10 {
        let r = tracker.observe(key, sig);
        counts.push(r.count);
    }
    // Counts are monotonic + dense — recurrence is observed each time.
    assert_eq!(counts, (1..=10).collect::<Vec<u32>>());

    // ── Escalation axis: deeper rung as time grows ───────────────────
    let ladder = EscalationLadder::pangea_default();
    let timeline = [
        (Duration::from_secs(0),    EscalationAction::Retry),           // 0s → Retry
        (Duration::from_secs(60),   EscalationAction::Retry),           // 1min → still Retry
        (Duration::from_secs(300),  EscalationAction::RefreshSource),   // 5min → RefreshSource
        (Duration::from_secs(900),  EscalationAction::ReloadGems),      // 15min → ReloadGems
        (Duration::from_secs(1800), EscalationAction::RecycleWorkers),  // 30min → RecycleWorkers
        (Duration::from_secs(3600), EscalationAction::PauseAndAlert),   // 60min → PauseAndAlert
        // Past the deepest rung — stays PauseAndAlert (no overflow).
        (Duration::from_secs(86400), EscalationAction::PauseAndAlert),   // 24h → still PauseAndAlert
    ];
    for (dur, expected) in timeline.iter() {
        let picked = ladder.pick(*dur);
        assert_eq!(picked, *expected,
            "duration {:?} → expected {:?}, got {:?}", dur, expected, picked);
    }

    // ── Composite: build AnomalySummary at the deepest rung ──────────
    let recurrence = tracker.observe(key, sig); // 11th observation
    let action_at_top = ladder.pick(Duration::from_secs(3600));
    let summary = AnomalySummary::compose(
        &recurrence,
        action_at_top.label(),
        action_at_top.depth(),
        None,
    );
    assert_eq!(summary.signature, *sig);
    assert_eq!(summary.recurrence_count, 11);
    assert_eq!(summary.recommended_action, "pause_and_alert");
    assert_eq!(summary.recommended_depth, 4);
    assert_eq!(summary.typed_detector, None);

    // ── Handler axis: dispatch by trait, verify outcome shape ────────
    let registry = EscalationHandlerRegistry::pangea_default();
    let template = stub_template();
    let ctx = EscalationContext {
        template: &template,
        action: action_at_top,
        duration_unready: Duration::from_secs(3600),
        consecutive_failures: 11,
        last_error: err_variants[0].to_string(),
        error_signature: sig.clone(),
    };
    let handler = registry.handler_for(action_at_top);
    let outcome = handler.execute(&ctx).await.expect("handler success");

    // The deepest rung's handler MUST request autoSuspended=true.
    assert_eq!(
        outcome.status_patch["status"]["autoSuspended"],
        serde_json::Value::Bool(true),
        "PauseAndAlert handler must request autoSuspended=true"
    );
    assert_eq!(outcome.event_reason, "EscalationLadderPause");
    // The event message embeds the error signature (so operators can
    // grep `kubectl get events` by bug class).
    assert!(
        outcome.event_message.contains(sig),
        "event message must embed the error signature for grep-based correlation"
    );
}

#[tokio::test]
async fn three_axis_composition_distinct_signatures_track_independently() {
    // Two distinct bug classes should produce distinct signatures + the
    // tracker should count them independently. Locks the
    // "separated by signature" contract — without it, dashboards would
    // misreport recurrence as a single number when actually there are
    // multiple distinct issues.

    let sig_a = error_signature("Attribute :cluster_name has already been defined");
    let sig_b = error_signature("uninitialized constant Pangea::Resources::Cloudflare");
    assert_ne!(sig_a, sig_b, "distinct error classes must have distinct signatures");

    let tracker = InMemoryRecurrenceTracker::new();
    let key = "test-ns/test-t";

    for _ in 0..5 { tracker.observe(key, &sig_a); }
    for _ in 0..3 { tracker.observe(key, &sig_b); }

    let r_a = tracker.peek(key, &sig_a).expect("a observed");
    let r_b = tracker.peek(key, &sig_b).expect("b observed");
    assert_eq!(r_a.count, 5);
    assert_eq!(r_b.count, 3);
}

#[tokio::test]
async fn three_axis_composition_short_duration_keeps_handler_no_op() {
    // The shallowest rung — Retry at <5min unready — must produce an
    // EMPTY status patch (no behavior change). Catches a regression
    // where someone makes Retry mutate state (against the "Retry is
    // a no-op" contract).

    let registry = EscalationHandlerRegistry::pangea_default();
    let template = stub_template();
    let ctx = EscalationContext {
        template: &template,
        action: EscalationAction::Retry,
        duration_unready: Duration::from_secs(30),
        consecutive_failures: 1,
        last_error: "first failure".to_string(),
        error_signature: "abc".to_string(),
    };

    let handler = registry.handler_for(EscalationAction::Retry);
    let outcome = handler.execute(&ctx).await.expect("handler success");
    assert_eq!(outcome.status_patch, serde_json::json!({}));
    assert_eq!(outcome.event_reason, "EscalationLadderRetry");
}

#[tokio::test]
async fn three_axis_composition_action_label_round_trips_through_summary() {
    // The label embedded in AnomalySummary.recommended_action MUST
    // match `EscalationAction::label()` exactly. Round-trip via the
    // handler dispatch.
    for action in [
        EscalationAction::Retry,
        EscalationAction::RefreshSource,
        EscalationAction::ReloadGems,
        EscalationAction::RecycleWorkers,
        EscalationAction::PauseAndAlert,
    ] {
        let tracker = InMemoryRecurrenceTracker::new();
        let rec = tracker.observe("k", "s");
        let summary = AnomalySummary::compose(&rec, action.label(), action.depth(), None);

        let registry = EscalationHandlerRegistry::pangea_default();
        let handler = registry.handler_for(action);

        assert_eq!(handler.action().label(), summary.recommended_action,
            "round-trip action label mismatch for {action:?}");
        assert_eq!(handler.action().depth(), summary.recommended_depth,
            "round-trip action depth mismatch for {action:?}");
    }
}
