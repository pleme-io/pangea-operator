use std::sync::atomic::{AtomicU8, Ordering};

use pangea_leaf::address::AddressError;
use pangea_leaf::admission::{
    decide, skip_declaration, Admission, AdmissionMode, SKIP_LEAF_ANNOTATION,
};
use pangea_leaf::classify::{classify, Classification};
use pangea_leaf::sources::normalize_address;

use crate::crd::{InfrastructureTemplate, InfrastructureTemplateStatus};
use crate::executor::cycle_artifact::CycleArtifact;
use crate::executor::Plan;

static ADMISSION_MODE: AtomicU8 = AtomicU8::new(0);

pub fn set_admission_mode(mode: AdmissionMode) {
    ADMISSION_MODE.store(
        match mode {
            AdmissionMode::Enforce => 0,
            AdmissionMode::Off => 1,
        },
        Ordering::Relaxed,
    );
}

#[must_use]
pub fn admission_mode() -> AdmissionMode {
    match ADMISSION_MODE.load(Ordering::Relaxed) {
        1 => AdmissionMode::Off,
        _ => AdmissionMode::Enforce,
    }
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
    fn the_mode_switch_round_trips() {
        set_admission_mode(AdmissionMode::Off);
        assert_eq!(admission_mode(), AdmissionMode::Off);
        set_admission_mode(AdmissionMode::Enforce);
        assert_eq!(admission_mode(), AdmissionMode::Enforce);
    }
}
