use prometheus::{HistogramOpts, HistogramVec, IntCounterVec, IntGaugeVec, Opts, Registry};

use crate::crd::LiveProgress;

#[derive(Clone)]
pub struct LiveMetrics {
    pub events_total: IntCounterVec,
    pub phase_duration_seconds: HistogramVec,
    pub seconds_since_progress: IntGaugeVec,
    pub last_progress_timestamp_seconds: IntGaugeVec,
    pub phase_started_timestamp_seconds: IntGaugeVec,
    pub cycle_age_seconds: IntGaugeVec,
    pub last_cycle_timestamp_seconds: IntGaugeVec,
    pub resources_done: IntGaugeVec,
    pub resources_total: IntGaugeVec,
    pub pending_creates: IntGaugeVec,
    pub generation_lag_seconds: IntGaugeVec,
    pub reactions_total: IntCounterVec,
    pub reaction_notify_total: IntCounterVec,
    pub preflight_failures_total: IntCounterVec,
    pub resumed_after_restart_total: IntCounterVec,
}

fn gauge(registry: &Registry, name: &str, help: &str, labels: &[&str]) -> IntGaugeVec {
    let g = IntGaugeVec::new(Opts::new(name, help), labels).expect("metric can be created");
    registry
        .register(Box::new(g.clone()))
        .expect("metric can be registered");
    g
}

fn counter(registry: &Registry, name: &str, help: &str, labels: &[&str]) -> IntCounterVec {
    let c = IntCounterVec::new(Opts::new(name, help), labels).expect("metric can be created");
    registry
        .register(Box::new(c.clone()))
        .expect("metric can be registered");
    c
}

const TEMPLATE: &[&str] = &["template", "namespace"];

impl LiveMetrics {
    pub fn register(registry: &Registry) -> Self {
        let phase_duration_seconds = HistogramVec::new(
            HistogramOpts::new(
                "pangea_template_phase_duration_seconds",
                "Wall time an InfrastructureTemplate spent in an expensive phase, observed when the phase ends",
            )
            .buckets(vec![
                1.0, 5.0, 15.0, 30.0, 60.0, 120.0, 300.0, 600.0, 1200.0, 1800.0, 3600.0, 7200.0,
            ]),
            &["phase"],
        )
        .expect("metric can be created");
        registry
            .register(Box::new(phase_duration_seconds.clone()))
            .expect("metric can be registered");
        Self {
            events_total: counter(
                registry,
                "pangea_events_total",
                "Kubernetes events by where they landed: events.k8s.io/v1, v1, or ring (status.recentEvents)",
                &["api"],
            ),
            phase_duration_seconds,
            seconds_since_progress: gauge(
                registry,
                "pangea_template_seconds_since_progress",
                "Seconds since the running phase last made progress",
                TEMPLATE,
            ),
            last_progress_timestamp_seconds: gauge(
                registry,
                "pangea_template_last_progress_timestamp_seconds",
                "Unix time of the running phase's last progress",
                TEMPLATE,
            ),
            phase_started_timestamp_seconds: gauge(
                registry,
                "pangea_template_phase_started_timestamp_seconds",
                "Unix time the running phase started",
                &["template", "namespace", "phase"],
            ),
            cycle_age_seconds: gauge(
                registry,
                "pangea_template_cycle_age_seconds",
                "Seconds since status.lastCycle.completedAt",
                TEMPLATE,
            ),
            last_cycle_timestamp_seconds: gauge(
                registry,
                "pangea_template_last_cycle_timestamp_seconds",
                "Unix time of status.lastCycle.completedAt",
                TEMPLATE,
            ),
            resources_done: gauge(
                registry,
                "pangea_template_progress_resources_done",
                "Resources the running phase has completed",
                TEMPLATE,
            ),
            resources_total: gauge(
                registry,
                "pangea_template_progress_resources_total",
                "Resources the running phase has to do, when known",
                TEMPLATE,
            ),
            pending_creates: gauge(
                registry,
                "pangea_template_pending_creates",
                "Declared resources not yet created (status.pendingCreates.count)",
                TEMPLATE,
            ),
            generation_lag_seconds: gauge(
                registry,
                "pangea_template_generation_lag_seconds",
                "Seconds observedGeneration has trailed metadata.generation; 0 when caught up",
                TEMPLATE,
            ),
            reactions_total: counter(
                registry,
                "pangea_template_reactions_total",
                "Reactions fired by trigger (PlanStalled, ProviderExited, GenerationLag, ConvergencePending)",
                &["template", "namespace", "trigger"],
            ),
            reaction_notify_total: counter(
                registry,
                "pangea_reaction_notify_total",
                "Notify-hook deliveries by outcome",
                &["outcome"],
            ),
            preflight_failures_total: counter(
                registry,
                "pangea_template_preflight_failures_total",
                "Cycle-start preflight failures by check and reason",
                &["template", "namespace", "check", "reason"],
            ),
            resumed_after_restart_total: counter(
                registry,
                "pangea_template_resumed_after_restart_total",
                "Phases found running from a previous operator process and restarted cleanly",
                &["template", "namespace", "phase"],
            ),
        }
    }

    pub fn record_event_delivery(&self, api: &str) {
        self.events_total.with_label_values(&[api]).inc();
    }

    pub fn publish_progress(
        &self,
        template: &str,
        namespace: &str,
        progress: &LiveProgress,
        now: chrono::DateTime<chrono::Utc>,
    ) {
        let labels = [template, namespace];
        self.seconds_since_progress
            .with_label_values(&labels)
            .set((now - progress.last_progress_at).num_seconds().max(0));
        self.last_progress_timestamp_seconds
            .with_label_values(&labels)
            .set(progress.last_progress_at.timestamp());
        let phase = progress.phase.to_string();
        self.phase_started_timestamp_seconds
            .with_label_values(&[template, namespace, phase.as_str()])
            .set(progress.phase_started_at.timestamp());
        self.resources_done
            .with_label_values(&labels)
            .set(i64::try_from(progress.resources_done).unwrap_or(i64::MAX));
        self.resources_total.with_label_values(&labels).set(
            progress
                .resources_total
                .and_then(|t| i64::try_from(t).ok())
                .unwrap_or(0),
        );
    }

    pub fn finish_phase(&self, template: &str, namespace: &str, progress: &LiveProgress) {
        let end = progress.finished_at.unwrap_or(progress.last_progress_at);
        let secs = (end - progress.phase_started_at).num_milliseconds().max(0) as f64 / 1000.0;
        let phase = progress.phase.to_string();
        self.phase_duration_seconds
            .with_label_values(&[phase.as_str()])
            .observe(secs);
        self.seconds_since_progress
            .with_label_values(&[template, namespace])
            .set(0);
    }

    pub fn publish_cycle(
        &self,
        template: &str,
        namespace: &str,
        completed_at: Option<chrono::DateTime<chrono::Utc>>,
        now: chrono::DateTime<chrono::Utc>,
    ) {
        if let Some(at) = completed_at {
            let labels = [template, namespace];
            self.last_cycle_timestamp_seconds
                .with_label_values(&labels)
                .set(at.timestamp());
            self.cycle_age_seconds
                .with_label_values(&labels)
                .set((now - at).num_seconds().max(0));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crd::Phase;
    use chrono::{DateTime, Utc};

    fn at(s: i64) -> DateTime<Utc> {
        DateTime::<Utc>::from_timestamp(s, 0).unwrap()
    }

    #[test]
    fn progress_gauges_and_phase_histogram_reach_the_registry() {
        let registry = Registry::new();
        let m = LiveMetrics::register(&registry);
        let mut p = LiveProgress::new(Phase::Planning, at(1_000), None);
        p.last_progress_at = at(1_100);
        p.resources_done = 7;
        p.resources_total = Some(10);
        m.publish_progress("t", "ns", &p, at(1_160));
        m.publish_cycle("t", "ns", Some(at(400)), at(1_160));
        p.finished_at = Some(at(1_300));
        m.finish_phase("t", "ns", &p);
        m.record_event_delivery("ring");
        let text = {
            use prometheus::Encoder;
            let mut buf = Vec::new();
            prometheus::TextEncoder::new()
                .encode(&registry.gather(), &mut buf)
                .unwrap();
            String::from_utf8(buf).unwrap()
        };
        assert!(text.contains(
            "pangea_template_progress_resources_done{namespace=\"ns\",template=\"t\"} 7"
        ));
        assert!(
            text.contains("pangea_template_cycle_age_seconds{namespace=\"ns\",template=\"t\"} 760")
        );
        assert!(text.contains("pangea_template_phase_duration_seconds_sum{phase=\"Planning\"} 300"));
        assert!(text.contains("pangea_events_total{api=\"ring\"} 1"));
        assert!(text
            .contains("pangea_template_seconds_since_progress{namespace=\"ns\",template=\"t\"} 0"));
    }
}
