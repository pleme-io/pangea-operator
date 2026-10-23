use std::collections::{BTreeMap, BTreeSet};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use kube::runtime::events::EventType;
use kube::ResourceExt;
use pangea_leaf::address::AddressError;
use pangea_leaf::admission::{
    decide, skip_declaration, Admission, AdmissionMode, SKIP_LEAF_ANNOTATION,
};
use pangea_leaf::classify::{classify, Classification};
use pangea_leaf::ratchet::{next_status, CycleWindow, LeafStatus};
use pangea_leaf::schedule::{blocked_by, inputs_digest, Upstream};
use pangea_leaf::sources::normalize_address;
use tracing::warn;

use crate::config::LeafConfig;
use crate::controller::template::events::record_event;
use crate::controller::ControllerState;

use crate::crd::{InfrastructureTemplate, InfrastructureTemplateStatus, WorkspaceCatalog};
use crate::executor::cycle_artifact::CycleArtifact;
use crate::executor::Plan;

static SETTINGS: Mutex<Option<LeafConfig>> = Mutex::new(None);
static WINDOWS: Mutex<BTreeMap<String, CycleWindow>> = Mutex::new(BTreeMap::new());

pub fn configure(cfg: &LeafConfig) {
    *SETTINGS.lock().expect("leaf settings mutex poisoned") = Some(cfg.clone());
}

#[must_use]
pub fn settings() -> LeafConfig {
    SETTINGS
        .lock()
        .expect("leaf settings mutex poisoned")
        .clone()
        .unwrap_or_default()
}

pub const MIN_SAMPLES: usize = 5;

fn observe_cycle(key: &str, ms: u64) -> Option<u64> {
    let mut windows = WINDOWS.lock().expect("leaf windows mutex poisoned");
    let w = windows.entry(key.to_string()).or_default();
    w.push(ms);
    if w.samples_ms.len() < MIN_SAMPLES {
        return None;
    }
    w.p95_ms()
}

#[must_use]
pub fn never_reconciled(status: Option<&InfrastructureTemplateStatus>) -> bool {
    status.map_or(true, |s| s.cycle_count == 0 && s.last_applied_at.is_none())
}

#[must_use]
pub fn plan_addresses(
    raw_show_json: &str,
    artifact: Option<&CycleArtifact>,
) -> Option<Vec<String>> {
    if !raw_show_json.is_empty() {
        let plan = Plan::from_json(raw_show_json).ok()?;
        return Some(
            plan.resource_changes
                .iter()
                .map(|c| c.address.clone())
                .collect(),
        );
    }
    let art = artifact?;
    art.resource_changes
        .iter()
        .map(|c| normalize_address(&c.address).ok())
        .collect()
}

#[derive(Debug, Clone, PartialEq)]
pub struct Assessment {
    pub classification: Classification,
    pub admission: Admission,
}

pub fn assess(
    template: &InfrastructureTemplate,
    mode: AdmissionMode,
    addresses: &[String],
    cycle_p95_ms: Option<u64>,
    budget_ms: Option<u64>,
) -> Result<Assessment, AddressError> {
    let classification = classify(
        addresses.iter().map(String::as_str),
        cycle_p95_ms,
        budget_ms,
    )?;
    let annotation = template
        .metadata
        .annotations
        .as_ref()
        .and_then(|a| a.get(SKIP_LEAF_ANNOTATION))
        .map(String::as_str);
    let admission = decide(
        mode,
        never_reconciled(template.status.as_ref()),
        &classification,
        &skip_declaration(annotation),
    );
    Ok(Assessment {
        classification,
        admission,
    })
}

const CATALOG_TTL: Duration = Duration::from_secs(30);

#[derive(Debug, Clone, Default)]
pub struct CatalogView {
    pub max_concurrent_leaves: Option<u32>,
    pub variables: Option<serde_json::Value>,
}

static CATALOGS: Mutex<BTreeMap<String, (Instant, CatalogView)>> = Mutex::new(BTreeMap::new());

pub async fn catalog_view(
    state: &ControllerState,
    template: &InfrastructureTemplate,
) -> CatalogView {
    let Some(name) = template
        .metadata
        .labels
        .as_ref()
        .and_then(|l| l.get(crate::controller::workspace_catalog_controller::WORKSPACE_LABEL))
    else {
        return CatalogView::default();
    };
    if let Some((at, v)) = CATALOGS
        .lock()
        .expect("leaf catalog mutex poisoned")
        .get(name)
    {
        if at.elapsed() < CATALOG_TTL {
            return v.clone();
        }
    }
    let api: kube::Api<WorkspaceCatalog> = kube::Api::all(state.client.clone());
    let view = match api.get_opt(name).await {
        Ok(Some(wsc)) => CatalogView {
            max_concurrent_leaves: wsc.spec.max_concurrent_leaves,
            variables: wsc
                .spec
                .variables
                .as_ref()
                .and_then(|v| serde_json::to_value(v).ok()),
        },
        Ok(None) => CatalogView::default(),
        Err(e) => {
            warn!(error = %e, catalog = %name, "WorkspaceCatalog lookup failed; leaf cap unset");
            return CatalogView::default();
        }
    };
    CATALOGS
        .lock()
        .expect("leaf catalog mutex poisoned")
        .insert(name.clone(), (Instant::now(), view.clone()));
    view
}

#[must_use]
pub fn digest_of(
    template: &InfrastructureTemplate,
    catalog_variables: Option<&serde_json::Value>,
) -> String {
    let vars = template
        .spec
        .variables
        .as_ref()
        .and_then(|v| serde_json::to_value(v).ok());
    let refs = template
        .spec
        .variable_refs
        .as_ref()
        .and_then(|v| serde_json::to_value(v).ok());
    inputs_digest(catalog_variables, vars.as_ref(), refs.as_ref())
}

pub async fn blocked_upstreams(
    state: &ControllerState,
    template: &InfrastructureTemplate,
    applying: bool,
) -> Vec<String> {
    let Some(refs) = template
        .spec
        .variable_refs
        .as_ref()
        .filter(|r| !r.is_empty())
    else {
        return Vec::new();
    };
    let own_ns = template.namespace().unwrap_or_default();
    let mut upstreams = Vec::new();
    let mut ready = BTreeSet::new();
    let mut missing = BTreeSet::new();
    for r in refs.values() {
        let name = r.template_ref.name.clone();
        let ns = r
            .template_ref
            .namespace
            .clone()
            .unwrap_or_else(|| own_ns.clone());
        let api: kube::Api<InfrastructureTemplate> =
            kube::Api::namespaced(state.client.clone(), &ns);
        let has_output = match api.get_opt(&name).await {
            Ok(Some(up)) => up
                .status
                .as_ref()
                .and_then(|s| s.outputs.as_ref())
                .is_some_and(|o| o.contains_key(&r.output_key)),
            _ => false,
        };
        if has_output {
            ready.insert(name.clone());
        } else {
            missing.insert(name.clone());
        }
        upstreams.push(Upstream {
            template: name,
            has_mock: r.mock_output.is_some(),
        });
    }
    ready.retain(|n| !missing.contains(n));
    blocked_by(&template.name_any(), &upstreams, &ready, applying)
}

pub enum Judgement {
    Admitted,
    Refused(String),
}

fn skip_reason_of(admission: &Admission) -> Option<String> {
    match admission {
        Admission::AdmitSkipped(r) => Some(r.as_str().to_string()),
        _ => None,
    }
}

pub async fn judge(
    template: &InfrastructureTemplate,
    state: &ControllerState,
    addresses: &[String],
    plan_ms: u64,
) -> Judgement {
    let cfg = settings();
    let key = format!(
        "{}/{}",
        template.namespace().unwrap_or_default(),
        template.name_any()
    );
    let p95 = observe_cycle(&key, plan_ms);
    let budget_ms = cfg.cycle_budget_secs.map(|s| s.saturating_mul(1_000));
    let assessment = match assess(template, cfg.admission, addresses, p95, budget_ms) {
        Ok(a) => a,
        Err(e) => {
            warn!(error = %e, "leaf classification skipped: a plan address did not parse");
            return Judgement::Admitted;
        }
    };
    if let Admission::Refuse(msg) = &assessment.admission {
        return Judgement::Refused(msg.clone());
    }
    let prev = template.status.as_ref().and_then(|s| s.leaf.as_ref());
    let next = next_status(
        prev,
        u32::try_from(assessment.classification.leaf_keys).unwrap_or(u32::MAX),
        assessment.classification.fat,
        p95,
        cfg.ratchet_tolerance_percent,
        skip_reason_of(&assessment.admission),
        Some(digest_of(
            template,
            catalog_view(state, template).await.variables.as_ref(),
        )),
    );
    if prev != Some(&next) {
        write_status(template, state, &next, prev).await;
    }
    Judgement::Admitted
}

async fn write_status(
    template: &InfrastructureTemplate,
    state: &ControllerState,
    next: &LeafStatus,
    prev: Option<&LeafStatus>,
) {
    let patch = serde_json::json!({ "status": { "leaf": next } });
    if let Err(e) =
        crate::controller::status_patch::patch_status(template, &state.client, patch).await
    {
        warn!(error = %e, "status.leaf could not be written");
        return;
    }
    let prior = prev.map(|p| p.regressions.as_slice()).unwrap_or(&[]);
    if !next.regressions.is_empty() && next.regressions != prior {
        let msg = next
            .regressions
            .iter()
            .map(|r| r.describe())
            .collect::<Vec<_>>()
            .join("; ");
        record_event(
            template,
            state,
            EventType::Warning,
            "LeafRatchetRegressed",
            &msg,
        )
        .await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    fn template(annotation: Option<&str>, reconciled: bool) -> InfrastructureTemplate {
        let mut t: InfrastructureTemplate = serde_json::from_value(serde_json::json!({
            "apiVersion": "pangea.pleme.io/v1alpha1",
            "kind": "InfrastructureTemplate",
            "metadata": {"name": "t", "namespace": "n"},
            "spec": {"source": {}, "pangeaNamespace": "ns"}
        }))
        .unwrap();
        if let Some(a) = annotation {
            t.metadata.annotations = Some(BTreeMap::from([(
                SKIP_LEAF_ANNOTATION.to_string(),
                a.to_string(),
            )]));
        }
        if reconciled {
            t.status = Some(InfrastructureTemplateStatus {
                cycle_count: 3,
                ..Default::default()
            });
        }
        t
    }

    fn fat() -> Vec<String> {
        vec!["a.b".into(), "c.d".into()]
    }

    #[test]
    fn a_new_fat_template_is_refused() {
        let a = assess(
            &template(None, false),
            AdmissionMode::Enforce,
            &fat(),
            None,
            None,
        )
        .unwrap();
        assert!(a.admission.refused());
        assert!(a.classification.fat);
    }

    #[test]
    fn the_annotation_admits_it() {
        let a = assess(
            &template(Some("atomic-bootstrap"), false),
            AdmissionMode::Enforce,
            &fat(),
            None,
            None,
        )
        .unwrap();
        assert!(!a.admission.refused());
    }

    #[test]
    fn the_setting_admits_it() {
        let a = assess(
            &template(None, false),
            AdmissionMode::Off,
            &fat(),
            None,
            None,
        )
        .unwrap();
        assert_eq!(a.admission, Admission::AdmitOff);
    }

    #[test]
    fn a_reconciled_template_is_grandfathered() {
        let a = assess(
            &template(None, true),
            AdmissionMode::Enforce,
            &fat(),
            None,
            None,
        )
        .unwrap();
        assert_eq!(a.admission, Admission::AdmitGrandfathered);
    }

    #[test]
    fn a_leaf_template_is_admitted() {
        let one = vec!["a.b[\"x\"]".to_string(), "c.d[\"x\"]".to_string()];
        let a = assess(
            &template(None, false),
            AdmissionMode::Enforce,
            &one,
            None,
            None,
        )
        .unwrap();
        assert_eq!(a.admission, Admission::Admit);
    }

    #[test]
    fn plan_addresses_read_a_tofu_show_json() {
        let json = r#"{"format_version":"1.2","terraform_version":"1.7.0","resource_changes":[
            {"address":"a.b","type":"a","name":"b","provider_name":"p","change":{"actions":["create"]}}]}"#;
        assert_eq!(plan_addresses(json, None), Some(vec!["a.b".to_string()]));
    }

    #[test]
    fn no_plan_output_is_unobserved_not_empty() {
        assert_eq!(plan_addresses("", None), None);
    }

    #[test]
    fn configure_round_trips_the_settings() {
        let cfg = LeafConfig {
            admission: AdmissionMode::Off,
            ratchet_tolerance_percent: 3,
            cycle_budget_secs: Some(9),
        };
        configure(&cfg);
        assert_eq!(settings(), cfg);
        configure(&LeafConfig::default());
        assert_eq!(settings().admission, AdmissionMode::Enforce);
    }

    #[test]
    fn the_window_yields_a_p95_per_template() {
        let k = "ns/window-test";
        for ms in 1..MIN_SAMPLES as u64 {
            assert_eq!(observe_cycle(k, ms), None);
        }
        for ms in MIN_SAMPLES as u64..=20 {
            observe_cycle(k, ms);
        }
        assert_eq!(observe_cycle(k, 20), Some(20));
        assert_eq!(observe_cycle("ns/other", 5), None);
    }
}
