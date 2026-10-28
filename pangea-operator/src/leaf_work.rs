use std::collections::BTreeMap;
use std::sync::Mutex;

use pangea_leaf::work::{ActionCounts, LeafWorkStatus, PendingLeaf};

use crate::config::WorkersConfig;
use crate::crd::{InfrastructureTemplate, Phase};

static SETTINGS: Mutex<Option<WorkersConfig>> = Mutex::new(None);

pub fn configure(cfg: &WorkersConfig) {
    *SETTINGS.lock().expect("leaf work mutex poisoned") = Some(cfg.clone());
}

#[must_use]
pub fn settings() -> WorkersConfig {
    SETTINGS
        .lock()
        .expect("leaf work mutex poisoned")
        .clone()
        .unwrap_or_default()
}

#[must_use]
pub fn is_pending(t: &InfrastructureTemplate) -> bool {
    if t.spec.suspend || t.metadata.deletion_timestamp.is_some() {
        return false;
    }
    let status = t.status.as_ref();
    let behind = t.metadata.generation.unwrap_or(0) > status.map_or(0, |s| s.observed_generation);
    let busy = !matches!(
        status.and_then(|s| s.phase),
        Some(Phase::Ready | Phase::Failed | Phase::CompileBlocked | Phase::Destroying)
    );
    behind || busy
}

#[must_use]
pub fn counts_of(t: &InfrastructureTemplate) -> ActionCounts {
    let status = t.status.as_ref();
    let resources = status
        .and_then(|s| s.resources.as_ref())
        .map_or(0, |r| u64::from(r.total));
    let Some(cycle) = status.and_then(|s| s.last_cycle.as_ref()) else {
        return ActionCounts {
            refresh: resources,
            ..ActionCounts::default()
        };
    };
    let Some(d) = cycle.action_distribution.as_ref() else {
        return ActionCounts {
            refresh: resources,
            import: u64::from(cycle.summary.imported),
            ..ActionCounts::default()
        };
    };
    let existing =
        u64::from(d.no_op) + u64::from(d.update) + u64::from(d.delete) + u64::from(d.replace);
    ActionCounts {
        refresh: resources.max(existing),
        create: u64::from(d.create),
        update: u64::from(d.update),
        delete: u64::from(d.delete),
        import: u64::from(cycle.summary.imported),
        replace: u64::from(d.replace),
    }
}

fn paced(t: &InfrastructureTemplate) -> bool {
    t.spec
        .provider_credentials
        .as_ref()
        .is_some_and(|c| c.github.is_some())
}

#[must_use]
pub fn status_with(
    templates: &[InfrastructureTemplate],
    cfg: &WorkersConfig,
    github_rate: f64,
) -> Option<LeafWorkStatus> {
    if !cfg.autoscaling.enabled {
        return None;
    }
    let mut rates = BTreeMap::new();
    let pending: Vec<PendingLeaf> = templates
        .iter()
        .filter(|t| is_pending(t))
        .map(|t| {
            let credential = crate::controller::template::leaf_gate::credential_of(t);
            if let Some(c) = credential.as_ref().filter(|_| paced(t)) {
                rates.insert(c.clone(), github_rate);
            }
            PendingLeaf {
                credential,
                work: pangea_leaf::work::work(&counts_of(t), &cfg.weights),
            }
        })
        .collect();
    let a = &cfg.autoscaling;
    Some(crate::leaf_engine::engine().work(
        &pending,
        &rates,
        a.horizon_secs,
        a.target,
        (a.min, a.max),
    ))
}

#[must_use]
pub fn memory_with(
    templates: &[InfrastructureTemplate],
    setting: pangea_leaf::memory::WorkerMemory,
    cap_worker: u64,
) -> pangea_leaf::memory::WorkerMemoryStatus {
    let inputs = pangea_leaf::memory::MemoryInputs {
        base_rss: pangea_leaf::memory::BASE_RSS_BYTES,
        cap_worker,
        max_leaf_peak: pangea_leaf::memory::max_peak(templates.iter().map(|t| {
            t.status
                .as_ref()
                .and_then(|s| s.leaf.as_ref())
                .and_then(|l| l.peak_rss_bytes)
        })),
        providers: 1,
        provider_rss: pangea_leaf::memory::GITHUB_PROVIDER_RSS_BYTES,
    };
    crate::leaf_engine::engine().memory(setting, &inputs)
}

#[must_use]
pub fn memory(templates: &[InfrastructureTemplate]) -> pangea_leaf::memory::WorkerMemoryStatus {
    let leaf = crate::controller::template::leaf::settings();
    let cap = if leaf.per_credential_cap == 0 {
        16
    } else {
        leaf.per_credential_cap
    };
    memory_with(
        templates,
        settings().memory,
        u64::try_from(cap).unwrap_or(u64::MAX),
    )
}

#[must_use]
pub fn status(templates: &[InfrastructureTemplate]) -> Option<LeafWorkStatus> {
    status_with(
        templates,
        &settings(),
        crate::executor::pacer_lane::github_rate_per_sec(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn leaf(
        name: &str,
        phase: &str,
        resources: u32,
        cycle: Option<serde_json::Value>,
    ) -> InfrastructureTemplate {
        let mut status = serde_json::json!({"phase": phase, "observedGeneration": 1, "resources": {"total": resources}});
        if let Some(c) = cycle {
            status["lastCycle"] = c;
        }
        serde_json::from_value(serde_json::json!({
            "apiVersion": "pangea.pleme.io/v1alpha1",
            "kind": "InfrastructureTemplate",
            "metadata": {"name": name, "namespace": "n", "generation": 1},
            "spec": {
                "source": {}, "pangeaNamespace": "ns",
                "providerCredentials": {"github": {"secretRef": {"name": "gh", "key": "token"}}}
            },
            "status": status
        }))
        .unwrap()
    }

    fn cycle(create: u32, update: u32, no_op: u32) -> serde_json::Value {
        serde_json::json!({
            "cycle": 1, "startedAt": "2026-10-28T00:00:00Z", "completedAt": "2026-10-28T00:00:09Z",
            "summary": {},
            "actionDistribution": {"create": create, "update": update, "noOp": no_op}
        })
    }

    #[test]
    fn a_fixed_set_of_leaves_gives_the_hand_computed_pending_work() {
        let templates = vec![
            leaf("planning-new", "Planning", 2, None),
            leaf("planning-changed", "Planning", 3, Some(cycle(1, 1, 1))),
            leaf("ready", "Ready", 50, None),
        ];
        let s = status_with(&templates, &WorkersConfig::default(), 3.125).unwrap();
        assert_eq!(s.metric, 2 + (3 + 3 + 2));
        assert_eq!(s.desired, 1);
        assert_eq!(s.credentials.len(), 1);
        assert_eq!(s.credentials[0].cap, 937);
    }

    #[test]
    fn a_starved_credential_caps_at_its_rate_times_the_horizon() {
        let templates: Vec<InfrastructureTemplate> = (0..1023)
            .map(|i| leaf(&format!("org-{i}"), "Planning", 2, None))
            .collect();
        let s = status_with(&templates, &WorkersConfig::default(), 3.125).unwrap();
        assert_eq!(s.credentials[0].pending, 2046);
        assert_eq!(s.metric, 937);
        assert_eq!(s.desired, 2);
    }

    #[test]
    fn the_derived_worker_memory_equals_the_formula_on_fixture_leaves() {
        let mi = 1_048_576u64;
        let mut templates = vec![
            leaf("a", "Ready", 2, None),
            leaf("b", "Ready", 2, None),
            leaf("c", "Ready", 2, None),
        ];
        for (t, peak) in templates
            .iter_mut()
            .zip([Some(12 * mi), Some(30 * mi), None])
        {
            if let Some(s) = t.status.as_mut() {
                s.leaf = serde_json::from_value(serde_json::json!({
                    "leafKeys": 1, "fat": false, "peakRssBytes": peak
                }))
                .ok();
            }
        }
        let m = memory_with(&templates, pangea_leaf::memory::WorkerMemory::Derived, 8);
        let request = pangea_leaf::memory::BASE_RSS_BYTES
            + 8 * 30 * mi
            + pangea_leaf::memory::GITHUB_PROVIDER_RSS_BYTES;
        assert_eq!(m.request_bytes, request);
        assert_eq!(m.limit_bytes, request + request / 2);
        assert!(m.calibrated);
        let fixed = memory_with(
            &templates,
            pangea_leaf::memory::WorkerMemory::Fixed(256 * mi),
            8,
        );
        assert_eq!(fixed.request_bytes, 256 * mi);
    }

    #[test]
    fn autoscaling_off_publishes_no_work() {
        let mut cfg = WorkersConfig::default();
        cfg.autoscaling.enabled = false;
        assert_eq!(
            status_with(&[leaf("a", "Planning", 2, None)], &cfg, 3.125),
            None
        );
    }
}
