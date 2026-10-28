use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use kube::ResourceExt;
use pangea_leaf::admit::{drift_slots, Candidate, Caps, InFlight, WorkClass};

use crate::config::LeafConfig;
use crate::controller::scheduling::{BudgetConfig, BudgetPermit, FairBudget};
use crate::crd::{InfrastructureTemplate, Phase};

pub const WAIT_TTL: Duration = Duration::from_secs(30);

struct Waiting {
    seen: Instant,
    candidate: Candidate,
    scope_cap: Option<usize>,
}

pub struct LeafGate {
    waiting: Mutex<BTreeMap<String, Waiting>>,
    blocked: Mutex<BTreeSet<String>>,
    credentials: Arc<FairBudget>,
    drift: Arc<FairBudget>,
    epoch: Instant,
}

pub struct LeafPermit {
    _scope: BudgetPermit,
    _credential: Option<BudgetPermit>,
    _drift: Option<BudgetPermit>,
}

impl Default for LeafGate {
    fn default() -> Self {
        let unbounded = BudgetConfig {
            per_scope: usize::MAX,
            global: usize::MAX,
        };
        Self {
            waiting: Mutex::new(BTreeMap::new()),
            blocked: Mutex::new(BTreeSet::new()),
            credentials: FairBudget::new(unbounded),
            drift: FairBudget::new(unbounded),
            epoch: Instant::now(),
        }
    }
}

#[must_use]
pub fn key_of(template: &InfrastructureTemplate) -> String {
    [
        template.namespace().unwrap_or_default(),
        template.name_any(),
    ]
    .join("/")
}

#[must_use]
pub fn credential_of(template: &InfrastructureTemplate) -> Option<String> {
    let creds = template.spec.provider_credentials.as_ref()?;
    let value = serde_json::to_value(creds).ok()?;
    if value.as_object().is_some_and(serde_json::Map::is_empty) {
        return None;
    }
    let mut s = template.namespace().unwrap_or_default();
    s.push('|');
    s.push_str(&value.to_string());
    Some(blake3::hash(s.as_bytes()).to_hex()[..16].to_string())
}

#[must_use]
pub fn work_class(template: &InfrastructureTemplate, phase: Phase, was_blocked: bool) -> WorkClass {
    if phase == Phase::Applying {
        return WorkClass::Apply;
    }
    let generation = template.metadata.generation.unwrap_or(0);
    let status = template.status.as_ref();
    if generation > status.map_or(0, |s| s.observed_generation) {
        return WorkClass::Change;
    }
    if was_blocked {
        return WorkClass::Unblocked;
    }
    if status.is_some_and(|s| s.failure_count > 0) {
        return WorkClass::Retry;
    }
    WorkClass::Drift
}

#[must_use]
pub fn depth_of(template: &InfrastructureTemplate) -> u32 {
    template
        .spec
        .variable_refs
        .as_ref()
        .map_or(0, |r| u32::try_from(r.len().min(1)).unwrap_or(0))
}

impl LeafGate {
    pub fn note_blocked(&self, key: &str) {
        self.blocked
            .lock()
            .expect("leaf gate mutex poisoned")
            .insert(key.to_string());
    }

    #[must_use]
    pub fn take_blocked(&self, key: &str) -> bool {
        self.blocked
            .lock()
            .expect("leaf gate mutex poisoned")
            .remove(key)
    }

    #[must_use]
    pub fn waiting(&self) -> usize {
        self.waiting.lock().expect("leaf gate mutex poisoned").len()
    }

    pub fn try_admit(
        &self,
        budgets: &Arc<FairBudget>,
        mut candidate: Candidate,
        scope_cap: Option<usize>,
        cfg: &LeafConfig,
        now: Instant,
    ) -> Option<LeafPermit> {
        let mut waiting = self.waiting.lock().expect("leaf gate mutex poisoned");
        waiting.retain(|_, w| now.saturating_duration_since(w.seen) < WAIT_TTL);
        let key = candidate.name.clone();
        candidate.since = waiting.get(&key).map_or_else(
            || {
                u64::try_from(now.saturating_duration_since(self.epoch).as_millis())
                    .unwrap_or(u64::MAX)
            },
            |w| w.candidate.since,
        );
        waiting.insert(
            key.clone(),
            Waiting {
                seen: now,
                candidate: candidate.clone(),
                scope_cap,
            },
        );
        let budget_caps = budgets.caps();
        let mut caps = Caps {
            global: budget_caps.global,
            per_scope: budget_caps.per_scope,
            scope_caps: BTreeMap::new(),
            per_credential: cfg.per_credential_cap,
            drift_share_permille: cfg.drift_share.permille(),
            priority: cfg.priority,
        };
        if cfg.calibrate {
            caps.global = 1;
        }
        let mut in_flight = InFlight {
            total: budgets.total_in_flight(),
            ..InFlight::default()
        };
        let mut candidates = Vec::with_capacity(waiting.len());
        for w in waiting.values() {
            let c = &w.candidate;
            if let Some(n) = w.scope_cap {
                caps.scope_caps.insert(c.scope.clone(), n);
            }
            in_flight
                .by_scope
                .insert(c.scope.clone(), budgets.in_flight(&c.scope));
            in_flight
                .drift_by_scope
                .insert(c.scope.clone(), self.drift.in_flight(&c.scope));
            if let Some(cred) = &c.credential {
                in_flight
                    .by_credential
                    .insert(cred.clone(), self.credentials.in_flight(cred));
            }
            candidates.push(c.clone());
        }
        let admitted = crate::leaf_engine::engine().admit(&candidates, &caps, &in_flight);
        if !admitted.contains(&key) {
            return None;
        }
        let scope = budgets.try_acquire_capped(&candidate.scope, scope_cap)?;
        let credential = match (&candidate.credential, cfg.per_credential_cap) {
            (Some(cred), n) if n > 0 => Some(self.credentials.try_acquire_capped(cred, Some(n))?),
            _ => None,
        };
        let drift = if candidate.class == WorkClass::Drift && caps.drift_share_permille > 0 {
            let slots = drift_slots(caps.scope_cap(&candidate.scope), caps.drift_share_permille);
            Some(
                self.drift
                    .try_acquire_capped(&candidate.scope, Some(slots))?,
            )
        } else {
            None
        };
        waiting.remove(&key);
        Some(LeafPermit {
            _scope: scope,
            _credential: credential,
            _drift: drift,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pangea_leaf::admit::PriorityMode;

    fn cand(name: &str, class: WorkClass) -> Candidate {
        Candidate {
            name: name.into(),
            scope: "org".into(),
            credential: Some("gh".into()),
            class,
            depth: 0,
            since: 0,
        }
    }

    fn budgets(per_scope: usize) -> Arc<FairBudget> {
        FairBudget::new(BudgetConfig {
            per_scope,
            global: 16,
        })
    }

    #[test]
    fn a_waiting_change_outranks_drift_that_polls_first_at_the_live_gate() {
        let gate = LeafGate::default();
        let b = budgets(4);
        let cfg = LeafConfig::default();
        let now = Instant::now();
        let mut held = Vec::new();
        for i in 0..20 {
            if let Some(p) = gate.try_admit(
                &b,
                cand(&format!("drift-{i:02}"), WorkClass::Drift),
                None,
                &cfg,
                now,
            ) {
                held.push(p);
            }
        }
        assert_eq!(held.len(), 2, "drift holds its share of four slots");
        let change = gate.try_admit(&b, cand("changed-2", WorkClass::Change), None, &cfg, now);
        assert!(change.is_some(), "a change still finds a slot");
    }

    #[test]
    fn a_drift_leaf_that_polls_while_a_change_waits_defers_to_it() {
        let gate = LeafGate::default();
        let b = budgets(1);
        let cfg = LeafConfig::default();
        let now = Instant::now();
        let busy = b.try_acquire("org").unwrap();
        assert!(gate
            .try_admit(&b, cand("changed", WorkClass::Change), None, &cfg, now)
            .is_none());
        drop(busy);
        assert!(gate
            .try_admit(&b, cand("drift", WorkClass::Drift), None, &cfg, now)
            .is_none());
        assert!(gate
            .try_admit(&b, cand("changed", WorkClass::Change), None, &cfg, now)
            .is_some());
    }

    #[test]
    fn priority_off_admits_the_first_to_poll() {
        let gate = LeafGate::default();
        let b = budgets(1);
        let cfg = LeafConfig {
            priority: PriorityMode::Off,
            ..LeafConfig::default()
        };
        let now = Instant::now();
        let later = now + Duration::from_millis(5);
        let busy = b.try_acquire("org").unwrap();
        assert!(gate
            .try_admit(&b, cand("drift", WorkClass::Drift), None, &cfg, now)
            .is_none());
        assert!(gate
            .try_admit(&b, cand("changed", WorkClass::Change), None, &cfg, later)
            .is_none());
        drop(busy);
        assert!(gate
            .try_admit(&b, cand("changed", WorkClass::Change), None, &cfg, later)
            .is_none());
        assert!(gate
            .try_admit(&b, cand("drift", WorkClass::Drift), None, &cfg, later)
            .is_some());
    }

    #[test]
    fn a_candidate_that_stops_polling_expires_and_frees_its_rank() {
        let gate = LeafGate::default();
        let b = budgets(1);
        let cfg = LeafConfig::default();
        let now = Instant::now();
        let busy = b.try_acquire("org").unwrap();
        assert!(gate
            .try_admit(&b, cand("gone", WorkClass::Change), None, &cfg, now)
            .is_none());
        drop(busy);
        let later = now + WAIT_TTL + Duration::from_secs(1);
        assert!(gate
            .try_admit(&b, cand("drift", WorkClass::Drift), None, &cfg, later)
            .is_some());
    }

    #[test]
    fn a_calibration_pass_admits_one_expensive_phase_at_a_time() {
        let gate = LeafGate::default();
        let b = budgets(4);
        let cfg = LeafConfig {
            calibrate: true,
            ..LeafConfig::default()
        };
        let now = Instant::now();
        let first = gate.try_admit(&b, cand("a", WorkClass::Change), None, &cfg, now);
        assert!(first.is_some());
        assert!(gate
            .try_admit(&b, cand("b", WorkClass::Change), None, &cfg, now)
            .is_none());
        drop(first);
        assert!(gate
            .try_admit(&b, cand("b", WorkClass::Change), None, &cfg, now)
            .is_some());
    }

    #[test]
    fn the_per_credential_cap_holds_across_scopes_at_the_live_gate() {
        let gate = LeafGate::default();
        let b = FairBudget::new(BudgetConfig {
            per_scope: 4,
            global: 64,
        });
        let cfg = LeafConfig {
            per_credential_cap: 3,
            ..LeafConfig::default()
        };
        let now = Instant::now();
        let mut held = Vec::new();
        for s in 0..3 {
            for i in 0..2 {
                let mut c = cand(&format!("s{s}-{i}"), WorkClass::Change);
                c.scope = format!("s{s}");
                if let Some(p) = gate.try_admit(&b, c, None, &cfg, now) {
                    held.push(p);
                }
            }
        }
        assert_eq!(held.len(), 3);
    }

    fn template(generation: i64, observed: i64, failures: u32) -> InfrastructureTemplate {
        let mut t: InfrastructureTemplate = serde_json::from_value(serde_json::json!({
            "apiVersion": "pangea.pleme.io/v1alpha1",
            "kind": "InfrastructureTemplate",
            "metadata": {"name": "t", "namespace": "n", "generation": generation},
            "spec": {"source": {}, "pangeaNamespace": "ns"}
        }))
        .unwrap();
        t.status = Some(crate::crd::InfrastructureTemplateStatus {
            observed_generation: observed,
            failure_count: failures,
            ..Default::default()
        });
        t
    }

    #[test]
    fn the_class_reads_the_phase_the_generation_the_block_and_the_failures() {
        assert_eq!(
            work_class(&template(3, 3, 0), Phase::Applying, false),
            WorkClass::Apply
        );
        assert_eq!(
            work_class(&template(4, 3, 0), Phase::Planning, false),
            WorkClass::Change
        );
        assert_eq!(
            work_class(&template(3, 3, 0), Phase::Planning, true),
            WorkClass::Unblocked
        );
        assert_eq!(
            work_class(&template(3, 3, 2), Phase::Compiling, false),
            WorkClass::Retry
        );
        assert_eq!(
            work_class(&template(3, 3, 0), Phase::Planning, false),
            WorkClass::Drift
        );
    }

    #[test]
    fn a_template_with_no_provider_credentials_has_no_credential_key() {
        let t = template(1, 1, 0);
        assert_eq!(credential_of(&t), None);
        let mut a = t.clone();
        a.spec.provider_credentials = serde_json::from_value(serde_json::json!({
            "github": {"secretRef": {"name": "gh", "key": "token"}}
        }))
        .ok();
        let b = a.clone();
        assert!(credential_of(&a).is_some());
        assert_eq!(credential_of(&a), credential_of(&b));
    }
}
