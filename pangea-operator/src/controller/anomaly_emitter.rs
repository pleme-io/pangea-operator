//! `AnomalyEmitter` trait + per-sink implementations + composite fan-out.
//!
//! ## Why this trait exists (the expose axis abstracted)
//!
//! Today every failure path emits an anomaly to four sinks inline:
//!
//!   1. `tracing::info!` with the `AnomalySummary` shape.
//!   2. `state.metrics.escalation_recommended_depth.set(...)` +
//!      `state.metrics.escalation_actions_total.inc(...)`.
//!   3. `status.lastError` via `patch_status` (the escalation message
//!      with the recommendation embedded).
//!   4. `record_event(... reason, message)` for `kubectl get events`.
//!
//! Adding a new sink (slice-4's `.status.anomalies[]` typed field,
//! or a future GraphQL subscription, or a Prometheus exemplar)
//! would mean editing every failure call site. The trait pulls the
//! fan-out up: one `emitter.emit(&ctx).await` at the call site,
//! N implementations behind the trait. New sink = new impl + added
//! to the composite, no call-site touch.
//!
//! ## The composition pattern
//!
//! `CompositeEmitter` wraps `Vec<Arc<dyn AnomalyEmitter>>` and fans
//! the same `AnomalyContext` to every sink in order. The default
//! production composite is `tracing + metrics + events`; slice-4
//! adds a `StatusFieldEmitter` impl that writes `.status.anomalies[]`.
//! Tests inject a fake emitter to verify the fan-out without setting
//! up Prometheus + kube-rs mocks.
//!
//! Pairs with:
//!   * `anomaly_tracker.rs` — produces the `AnomalySummary` carried
//!     by `AnomalyContext`.
//!   * `escalation.rs` + `escalation_handlers.rs` — the action axis
//!     whose recommendation lands in the summary.
//!   * `project_controller_detection_axis.md` — the named axis this
//!     trait operationalizes.

use super::anomaly_tracker::AnomalySummary;
use crate::crd::InfrastructureTemplate;
use async_trait::async_trait;
use kube::ResourceExt;
use std::sync::Arc;

/// Inputs to one emit call. Owned by the caller; passed by shared
/// reference so emitters don't take ownership.
///
/// Adding a field is a contract-extending change — existing emitters
/// keep compiling, new ones can rely on it. Removing or renaming a
/// field requires touching every emitter, which is the cost of the
/// stable contract.
#[derive(Debug)]
pub struct AnomalyContext<'a> {
    /// The CR currently being handled.
    pub template: &'a InfrastructureTemplate,
    /// The composite anomaly summary — joins the three axes
    /// (detection / recurrence / escalation).
    pub summary: &'a AnomalySummary,
    /// The raw underlying error message.
    pub error_msg: &'a str,
    /// Stable event reason for k8s Events (e.g. `EscalationLadderPause`).
    /// Matches what the `EscalationActionHandler::execute` returned.
    pub event_reason: &'static str,
    /// Free-form human-readable message body for k8s Events +
    /// status fields. Carries the full ladder recommendation + error.
    pub event_message: &'a str,
}

/// One emitter — fans the anomaly to ONE sink (logs / metrics / events
/// / status / GraphQL / …).
///
/// Implementations MUST be infallible (or swallow errors with a
/// tracing warn). Recovery-path emitters that themselves fail would
/// drown the actual error in noise, and the operator must keep
/// reconciling regardless.
#[async_trait]
pub trait AnomalyEmitter: Send + Sync {
    /// Stable label for tracing + debug. Pick once, never change.
    fn name(&self) -> &'static str;

    /// Emit the anomaly. MUST NOT panic. MUST NOT block longer than
    /// is necessary to flush to its sink (Prometheus inc is µs;
    /// k8s Event POST is ms; a slow emitter starves the others if
    /// `CompositeEmitter` runs serially — but the alternative,
    /// parallel emit, makes error correlation harder, so we accept
    /// the serial cost for clarity).
    async fn emit(&self, ctx: &AnomalyContext<'_>);
}

// ── TracingEmitter ──────────────────────────────────────────────────

/// Emits to `tracing::info!` with the structured `AnomalySummary`
/// shape. The canonical first sink — every failure path lands here
/// regardless of what other sinks are configured.
pub struct TracingEmitter;

#[async_trait]
impl AnomalyEmitter for TracingEmitter {
    fn name(&self) -> &'static str {
        "tracing"
    }

    async fn emit(&self, ctx: &AnomalyContext<'_>) {
        tracing::info!(
            template = %ctx.template.name_any(),
            namespace = %ctx.template.namespace().unwrap_or_default(),
            summary = ?ctx.summary,
            "anomaly observed"
        );
    }
}

// ── MetricsEmitter ──────────────────────────────────────────────────

/// Emits to Prometheus — sets the escalation_recommended_depth gauge
/// and bumps the escalation_actions_total counter. Cardinality is
/// bounded (per-template gauge + bounded action labels).
pub struct MetricsEmitter {
    metrics: Arc<crate::observability::Metrics>,
}

impl MetricsEmitter {
    pub fn new(metrics: Arc<crate::observability::Metrics>) -> Self {
        Self { metrics }
    }
}

#[async_trait]
impl AnomalyEmitter for MetricsEmitter {
    fn name(&self) -> &'static str {
        "metrics"
    }

    async fn emit(&self, ctx: &AnomalyContext<'_>) {
        let namespace = ctx.template.namespace().unwrap_or_default();
        let name = ctx.template.name_any();
        self.metrics
            .escalation_recommended_depth
            .with_label_values(&[&namespace, &name])
            .set(ctx.summary.recommended_depth as i64);
        self.metrics
            .escalation_actions_total
            .with_label_values(&[&namespace, &name, &ctx.summary.recommended_action])
            .inc();
    }
}

// ── CompositeEmitter ────────────────────────────────────────────────

/// Fan-out emitter — runs every contained emitter against the same
/// `AnomalyContext`. Order is preserved: tracing first (always
/// observable), metrics second (cheap), then any heavier sinks
/// (events, status patches, GraphQL).
///
/// Slice-4 adds a `StatusFieldEmitter` that writes `.status.anomalies[]`;
/// you append it to the composite, no call-site change.
pub struct CompositeEmitter {
    emitters: Vec<Arc<dyn AnomalyEmitter>>,
}

impl CompositeEmitter {
    pub fn new(emitters: Vec<Arc<dyn AnomalyEmitter>>) -> Self {
        Self { emitters }
    }

    /// Production default — tracing + metrics. Events + status field
    /// stay inline at the handle_compile_failure call site for now
    /// since they're coupled to the escalation handler's outcome;
    /// future refactor adds them as emitters too.
    pub fn pangea_default(metrics: Arc<crate::observability::Metrics>) -> Self {
        Self::new(vec![
            Arc::new(TracingEmitter),
            Arc::new(MetricsEmitter::new(metrics)),
        ])
    }

    /// Iterate the configured emitters — for debug + tests.
    pub fn emitters(&self) -> &[Arc<dyn AnomalyEmitter>] {
        &self.emitters
    }
}

#[async_trait]
impl AnomalyEmitter for CompositeEmitter {
    fn name(&self) -> &'static str {
        "composite"
    }

    async fn emit(&self, ctx: &AnomalyContext<'_>) {
        for emitter in &self.emitters {
            emitter.emit(ctx).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::controller::anomaly_tracker::AnomalySummary;
    use std::sync::Mutex;
    use std::time::Duration;

    /// Fake emitter that records what it sees — for verifying the
    /// composite fan-out without setting up Prometheus + kube-rs.
    struct RecordingEmitter {
        name: &'static str,
        calls: Mutex<Vec<String>>,
    }

    impl RecordingEmitter {
        fn new(name: &'static str) -> Self {
            Self {
                name,
                calls: Mutex::new(vec![]),
            }
        }

        fn call_count(&self) -> usize {
            self.calls.lock().unwrap().len()
        }
    }

    #[async_trait]
    impl AnomalyEmitter for RecordingEmitter {
        fn name(&self) -> &'static str {
            self.name
        }
        async fn emit(&self, ctx: &AnomalyContext<'_>) {
            self.calls
                .lock()
                .unwrap()
                .push(ctx.summary.signature.clone());
        }
    }

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

    fn stub_summary() -> AnomalySummary {
        AnomalySummary {
            signature: "abc123".to_string(),
            recurrence_count: 1,
            recurrence_age_s: 0,
            recommended_action: "retry".to_string(),
            recommended_depth: 0,
            typed_detector: None,
        }
    }

    #[tokio::test]
    async fn composite_fans_out_to_every_emitter_in_order() {
        let a = Arc::new(RecordingEmitter::new("a"));
        let b = Arc::new(RecordingEmitter::new("b"));
        let c = Arc::new(RecordingEmitter::new("c"));

        let composite = CompositeEmitter::new(vec![a.clone(), b.clone(), c.clone()]);

        let template = stub_template();
        let summary = stub_summary();
        let ctx = AnomalyContext {
            template: &template,
            summary: &summary,
            error_msg: "test",
            event_reason: "TestReason",
            event_message: "test message",
        };
        composite.emit(&ctx).await;

        // All three fired exactly once.
        assert_eq!(a.call_count(), 1);
        assert_eq!(b.call_count(), 1);
        assert_eq!(c.call_count(), 1);
        // All saw the same signature.
        assert_eq!(a.calls.lock().unwrap()[0], "abc123");
        assert_eq!(b.calls.lock().unwrap()[0], "abc123");
        assert_eq!(c.calls.lock().unwrap()[0], "abc123");
    }

    #[tokio::test]
    async fn empty_composite_emits_nothing_without_panic() {
        // A bare composite (no emitters configured) MUST be a no-op,
        // not a panic. Useful for test setups that don't care about
        // observability.
        let composite = CompositeEmitter::new(vec![]);
        let template = stub_template();
        let summary = stub_summary();
        let ctx = AnomalyContext {
            template: &template,
            summary: &summary,
            error_msg: "test",
            event_reason: "T",
            event_message: "m",
        };
        composite.emit(&ctx).await;
        assert!(composite.emitters().is_empty());
    }

    #[tokio::test]
    async fn composite_runs_emitters_serially_in_insertion_order() {
        // Run-order matters for tracing context propagation (parent
        // span emits before child consumers). Locking the contract.
        use std::sync::atomic::{AtomicUsize, Ordering};
        let counter = Arc::new(AtomicUsize::new(0));

        struct OrderedEmitter {
            order: Arc<AtomicUsize>,
            mine: Arc<Mutex<Option<usize>>>,
        }
        #[async_trait]
        impl AnomalyEmitter for OrderedEmitter {
            fn name(&self) -> &'static str {
                "ordered"
            }
            async fn emit(&self, _ctx: &AnomalyContext<'_>) {
                let i = self.order.fetch_add(1, Ordering::SeqCst);
                *self.mine.lock().unwrap() = Some(i);
            }
        }

        let a_mark = Arc::new(Mutex::new(None));
        let b_mark = Arc::new(Mutex::new(None));
        let c_mark = Arc::new(Mutex::new(None));
        let composite = CompositeEmitter::new(vec![
            Arc::new(OrderedEmitter {
                order: counter.clone(),
                mine: a_mark.clone(),
            }),
            Arc::new(OrderedEmitter {
                order: counter.clone(),
                mine: b_mark.clone(),
            }),
            Arc::new(OrderedEmitter {
                order: counter.clone(),
                mine: c_mark.clone(),
            }),
        ]);

        let template = stub_template();
        let summary = stub_summary();
        let ctx = AnomalyContext {
            template: &template,
            summary: &summary,
            error_msg: "test",
            event_reason: "T",
            event_message: "m",
        };
        composite.emit(&ctx).await;

        assert_eq!(*a_mark.lock().unwrap(), Some(0));
        assert_eq!(*b_mark.lock().unwrap(), Some(1));
        assert_eq!(*c_mark.lock().unwrap(), Some(2));
    }

    #[test]
    fn emitter_names_are_stable() {
        // Used as labels + log fields — refactor that renames these
        // breaks dashboards. Lock the strings.
        assert_eq!(TracingEmitter.name(), "tracing");
        let dummy_metrics = Arc::new(crate::observability::Metrics::new());
        let me = MetricsEmitter::new(dummy_metrics.clone());
        assert_eq!(me.name(), "metrics");
        let composite = CompositeEmitter::pangea_default(dummy_metrics);
        assert_eq!(composite.name(), "composite");
        assert_eq!(composite.emitters().len(), 2);
        // age suppresses an unused-import warning in #[cfg(test)].
        let _ = Duration::from_secs(0);
    }
}
