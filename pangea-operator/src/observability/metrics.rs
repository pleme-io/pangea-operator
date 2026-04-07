//! Prometheus metrics for the Pangea Operator.

use prometheus::{
    Counter, CounterVec, Gauge, GaugeVec, Histogram, HistogramOpts, HistogramVec,
    IntCounter, IntCounterVec, IntGauge, IntGaugeVec, Opts, Registry,
};

/// Prometheus metrics for the Pangea Operator.
#[derive(Clone)]
pub struct Metrics {
    registry: Registry,

    /// Total number of reconciliations.
    pub reconciliations_total: IntCounter,

    /// Total namespace reconciliations.
    pub namespace_reconciliations_total: IntCounter,

    /// Number of templates by phase.
    pub templates_by_phase: IntGaugeVec,

    /// Reconciliation duration histogram.
    pub reconciliation_duration_seconds: HistogramVec,

    /// Total drift detections.
    pub drift_detected_total: IntCounter,

    /// Total managed resources.
    pub managed_resources_total: IntGaugeVec,

    /// OpenTofu operations counter.
    pub tofu_operations_total: IntCounterVec,

    /// PostgreSQL state operations.
    pub pg_state_operations_total: IntCounterVec,

    /// Active reconciliations gauge.
    pub active_reconciliations: IntGauge,

    /// Template source compilation duration.
    pub compilation_duration_seconds: Histogram,

    /// Failed reconciliations.
    pub reconciliation_errors_total: IntCounterVec,
}

impl Metrics {
    /// Create new metrics instance.
    pub fn new() -> Self {
        let registry = Registry::new();

        let reconciliations_total = IntCounter::with_opts(Opts::new(
            "pangea_reconciliations_total",
            "Total number of InfrastructureTemplate reconciliations",
        ))
        .expect("metric can be created");

        let namespace_reconciliations_total = IntCounter::with_opts(Opts::new(
            "pangea_namespace_reconciliations_total",
            "Total number of PangeaNamespace reconciliations",
        ))
        .expect("metric can be created");

        let templates_by_phase = IntGaugeVec::new(
            Opts::new(
                "pangea_templates_by_phase",
                "Number of InfrastructureTemplates by phase",
            ),
            &["phase"],
        )
        .expect("metric can be created");

        let reconciliation_duration_seconds = HistogramVec::new(
            HistogramOpts::new(
                "pangea_reconciliation_duration_seconds",
                "Duration of reconciliation operations",
            )
            .buckets(vec![0.1, 0.5, 1.0, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0]),
            &["phase"],
        )
        .expect("metric can be created");

        let drift_detected_total = IntCounter::with_opts(Opts::new(
            "pangea_drift_detected_total",
            "Total number of drift detections",
        ))
        .expect("metric can be created");

        let managed_resources_total = IntGaugeVec::new(
            Opts::new(
                "pangea_managed_resources_total",
                "Total managed resources by namespace",
            ),
            &["namespace"],
        )
        .expect("metric can be created");

        let tofu_operations_total = IntCounterVec::new(
            Opts::new(
                "pangea_tofu_operations_total",
                "Total OpenTofu operations by type and result",
            ),
            &["operation", "result"],
        )
        .expect("metric can be created");

        let pg_state_operations_total = IntCounterVec::new(
            Opts::new(
                "pangea_pg_state_operations_total",
                "Total PostgreSQL state backend operations",
            ),
            &["operation"],
        )
        .expect("metric can be created");

        let active_reconciliations = IntGauge::with_opts(Opts::new(
            "pangea_active_reconciliations",
            "Number of currently active reconciliations",
        ))
        .expect("metric can be created");

        let compilation_duration_seconds = Histogram::with_opts(
            HistogramOpts::new(
                "pangea_compilation_duration_seconds",
                "Duration of Ruby DSL compilation",
            )
            .buckets(vec![0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0]),
        )
        .expect("metric can be created");

        let reconciliation_errors_total = IntCounterVec::new(
            Opts::new(
                "pangea_reconciliation_errors_total",
                "Total reconciliation errors by type",
            ),
            &["error_type"],
        )
        .expect("metric can be created");

        // Register all metrics
        registry
            .register(Box::new(reconciliations_total.clone()))
            .expect("metric can be registered");
        registry
            .register(Box::new(namespace_reconciliations_total.clone()))
            .expect("metric can be registered");
        registry
            .register(Box::new(templates_by_phase.clone()))
            .expect("metric can be registered");
        registry
            .register(Box::new(reconciliation_duration_seconds.clone()))
            .expect("metric can be registered");
        registry
            .register(Box::new(drift_detected_total.clone()))
            .expect("metric can be registered");
        registry
            .register(Box::new(managed_resources_total.clone()))
            .expect("metric can be registered");
        registry
            .register(Box::new(tofu_operations_total.clone()))
            .expect("metric can be registered");
        registry
            .register(Box::new(pg_state_operations_total.clone()))
            .expect("metric can be registered");
        registry
            .register(Box::new(active_reconciliations.clone()))
            .expect("metric can be registered");
        registry
            .register(Box::new(compilation_duration_seconds.clone()))
            .expect("metric can be registered");
        registry
            .register(Box::new(reconciliation_errors_total.clone()))
            .expect("metric can be registered");

        Self {
            registry,
            reconciliations_total,
            namespace_reconciliations_total,
            templates_by_phase,
            reconciliation_duration_seconds,
            drift_detected_total,
            managed_resources_total,
            tofu_operations_total,
            pg_state_operations_total,
            active_reconciliations,
            compilation_duration_seconds,
            reconciliation_errors_total,
        }
    }

    /// Gather metrics in Prometheus text format.
    pub fn gather(&self) -> String {
        use prometheus::Encoder;
        let encoder = prometheus::TextEncoder::new();
        let metric_families = self.registry.gather();
        let mut buffer = Vec::new();
        encoder.encode(&metric_families, &mut buffer).unwrap();
        String::from_utf8(buffer).unwrap()
    }
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metrics_new_does_not_panic() {
        let _metrics = Metrics::new();
    }

    #[test]
    fn test_metrics_default_does_not_panic() {
        let _metrics = Metrics::default();
    }

    #[test]
    fn test_gather_returns_valid_prometheus_text() {
        let metrics = Metrics::new();
        let output = metrics.gather();
        assert!(output.is_empty() || output.contains("pangea_"));
    }

    #[test]
    fn test_reconciliation_counter_increments() {
        let metrics = Metrics::new();
        metrics.reconciliations_total.inc();
        metrics.reconciliations_total.inc();

        let output = metrics.gather();
        assert!(output.contains("pangea_reconciliations_total"));
        assert!(output.contains(" 2"));
    }

    #[test]
    fn test_namespace_reconciliation_counter() {
        let metrics = Metrics::new();
        metrics.namespace_reconciliations_total.inc();

        let output = metrics.gather();
        assert!(output.contains("pangea_namespace_reconciliations_total"));
    }

    #[test]
    fn test_templates_by_phase_gauge() {
        let metrics = Metrics::new();
        metrics.templates_by_phase.with_label_values(&["Ready"]).set(5);
        metrics.templates_by_phase.with_label_values(&["Failed"]).set(1);

        let output = metrics.gather();
        assert!(output.contains("pangea_templates_by_phase"));
        assert!(output.contains("Ready"));
        assert!(output.contains("Failed"));
    }

    #[test]
    fn test_active_reconciliations_gauge() {
        let metrics = Metrics::new();
        metrics.active_reconciliations.set(3);
        assert_eq!(metrics.active_reconciliations.get(), 3);

        metrics.active_reconciliations.inc();
        assert_eq!(metrics.active_reconciliations.get(), 4);

        metrics.active_reconciliations.dec();
        assert_eq!(metrics.active_reconciliations.get(), 3);
    }

    #[test]
    fn test_tofu_operations_counter() {
        let metrics = Metrics::new();
        metrics.tofu_operations_total.with_label_values(&["plan", "success"]).inc();
        metrics.tofu_operations_total.with_label_values(&["apply", "failure"]).inc();

        let output = metrics.gather();
        assert!(output.contains("pangea_tofu_operations_total"));
    }

    #[test]
    fn test_reconciliation_errors_counter() {
        let metrics = Metrics::new();
        metrics.reconciliation_errors_total.with_label_values(&["kube_error"]).inc();
        metrics.reconciliation_errors_total.with_label_values(&["timeout"]).inc();
        metrics.reconciliation_errors_total.with_label_values(&["timeout"]).inc();

        let output = metrics.gather();
        assert!(output.contains("pangea_reconciliation_errors_total"));
    }

    #[test]
    fn test_drift_detected_counter() {
        let metrics = Metrics::new();
        metrics.drift_detected_total.inc();

        let output = metrics.gather();
        assert!(output.contains("pangea_drift_detected_total"));
    }

    #[test]
    fn test_histogram_records_observation() {
        let metrics = Metrics::new();
        metrics.reconciliation_duration_seconds.with_label_values(&["Planning"]).observe(1.5);
        metrics.compilation_duration_seconds.observe(0.3);

        let output = metrics.gather();
        assert!(output.contains("pangea_reconciliation_duration_seconds"));
        assert!(output.contains("pangea_compilation_duration_seconds"));
    }

    #[test]
    fn test_metrics_clone() {
        let metrics = Metrics::new();
        metrics.reconciliations_total.inc();
        let cloned = metrics.clone();
        cloned.reconciliations_total.inc();

        let output = metrics.gather();
        assert!(output.contains(" 2"));
    }
}
