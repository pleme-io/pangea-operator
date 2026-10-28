use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::classify::Classification;

pub const SKIP_LEAF_ANNOTATION: &str = "pangea.pleme.io/skip-leaf";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum AdmissionMode {
    Generate,
    #[default]
    Enforce,
    Off,
}

impl AdmissionMode {
    #[must_use]
    pub fn parse(raw: &str) -> Option<Self> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "generate" => Some(Self::Generate),
            "enforce" | "true" | "1" | "on" => Some(Self::Enforce),
            "off" | "false" | "0" => Some(Self::Off),
            _ => None,
        }
    }
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, JsonSchema,
)]
#[serde(rename_all = "kebab-case")]
pub enum SkipReason {
    ProviderSingleton,
    AtomicBootstrap,
    ExternalContract,
    SharedLifecycle,
}

impl SkipReason {
    pub const ALL: [Self; 4] = [
        Self::ProviderSingleton,
        Self::AtomicBootstrap,
        Self::ExternalContract,
        Self::SharedLifecycle,
    ];

    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::ProviderSingleton => "provider-singleton",
            Self::AtomicBootstrap => "atomic-bootstrap",
            Self::ExternalContract => "external-contract",
            Self::SharedLifecycle => "shared-lifecycle",
        }
    }

    #[must_use]
    pub fn parse(raw: &str) -> Option<Self> {
        let head = raw.split(':').next().unwrap_or("").trim();
        Self::ALL.into_iter().find(|r| r.as_str() == head)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SkipDeclaration {
    Absent,
    Valid(SkipReason),
    Invalid(String),
}

#[must_use]
pub fn skip_declaration(annotation: Option<&str>) -> SkipDeclaration {
    match annotation {
        None => SkipDeclaration::Absent,
        Some(raw) => match SkipReason::parse(raw) {
            Some(r) => SkipDeclaration::Valid(r),
            None => SkipDeclaration::Invalid(raw.to_string()),
        },
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Admission {
    Admit,
    AdmitOff,
    AdmitGrandfathered,
    AdmitSkipped(SkipReason),
    Refuse(String),
}

impl Admission {
    #[must_use]
    pub fn refused(&self) -> bool {
        matches!(self, Self::Refuse(_))
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Fix {
    pub workspace: Option<String>,
    pub draft: Option<String>,
}

#[must_use]
pub fn leaves_path(workspace: &str) -> String {
    ["workspaces/", workspace, "/leaves.b"].concat()
}

#[must_use]
pub fn decide(
    mode: AdmissionMode,
    never_reconciled: bool,
    classification: &Classification,
    skip: &SkipDeclaration,
) -> Admission {
    decide_with(
        mode,
        never_reconciled,
        classification,
        skip,
        &Fix::default(),
    )
}

#[must_use]
pub fn decide_with(
    mode: AdmissionMode,
    never_reconciled: bool,
    classification: &Classification,
    skip: &SkipDeclaration,
    fix: &Fix,
) -> Admission {
    if !classification.fat {
        return Admission::Admit;
    }
    if mode == AdmissionMode::Off {
        return Admission::AdmitOff;
    }
    if let SkipDeclaration::Valid(r) = skip {
        return Admission::AdmitSkipped(*r);
    }
    if !never_reconciled {
        return Admission::AdmitGrandfathered;
    }
    let valid: Vec<&str> = SkipReason::ALL.iter().map(|r| r.as_str()).collect();
    let skip_note = match skip {
        SkipDeclaration::Invalid(v) => {
            format!("annotation {SKIP_LEAF_ANNOTATION}=`{v}` is not a typed world-fact reason")
        }
        _ => format!("no {SKIP_LEAF_ANNOTATION} annotation"),
    };
    let path = leaves_path(fix.workspace.as_deref().unwrap_or("<workspace>"));
    let mut msg = format!(
        "fat template refused at admission: {} leaf key sets in one state ({}). \
         Declare its leaves in pangea-architectures `{path}` (one `leaves(...)` entry, \
         rendered by `nix run .#render-leaves`; `pangea leaf plan` shows the split), \
         or declare {SKIP_LEAF_ANNOTATION}=<{}> (a world-fact, never time pressure), \
         or set leaf.admission=off in the operator config",
        classification.leaf_keys,
        skip_note,
        valid.join("|")
    );
    if let (AdmissionMode::Generate, Some(draft)) = (mode, fix.draft.as_deref()) {
        msg.push_str("\nentry for ");
        msg.push_str(&path);
        msg.push_str(":\n");
        msg.push_str(draft);
    }
    Admission::Refuse(msg)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::classify::classify_counts;

    fn fat() -> Classification {
        classify_counts(3, None, None)
    }

    fn lean() -> Classification {
        classify_counts(1, None, None)
    }

    #[test]
    fn a_leaf_is_always_admitted() {
        assert_eq!(
            decide(
                AdmissionMode::Enforce,
                true,
                &lean(),
                &SkipDeclaration::Absent
            ),
            Admission::Admit
        );
    }

    #[test]
    fn a_new_fat_template_is_refused_with_the_remedies_named() {
        let a = decide(
            AdmissionMode::Enforce,
            true,
            &fat(),
            &SkipDeclaration::Absent,
        );
        let Admission::Refuse(msg) = a else {
            panic!("expected refusal")
        };
        assert!(msg.contains("pangea leaf plan"));
        assert!(msg.contains(SKIP_LEAF_ANNOTATION));
        assert!(msg.contains("leaf.admission=off"));
        assert!(msg.contains("3 leaf key sets"));
    }

    #[test]
    fn the_refusal_names_the_workspaces_leaves_declaration() {
        let fix = Fix {
            workspace: Some("pleme-io-github".into()),
            draft: Some("leaves(\"org\")".into()),
        };
        let a = decide_with(
            AdmissionMode::Enforce,
            true,
            &fat(),
            &SkipDeclaration::Absent,
            &fix,
        );
        let Admission::Refuse(msg) = a else {
            panic!("expected refusal")
        };
        assert!(msg.contains("workspaces/pleme-io-github/leaves.b"));
        assert!(!msg.contains("entry for"));
        let g = decide_with(
            AdmissionMode::Generate,
            true,
            &fat(),
            &SkipDeclaration::Absent,
            &fix,
        );
        let Admission::Refuse(msg) = g else {
            panic!("generate refuses too")
        };
        assert!(msg.contains("entry for workspaces/pleme-io-github/leaves.b:\nleaves(\"org\")"));
        assert_eq!(
            decide_with(
                AdmissionMode::Generate,
                false,
                &fat(),
                &SkipDeclaration::Absent,
                &fix
            ),
            Admission::AdmitGrandfathered
        );
    }

    #[test]
    fn a_declared_world_fact_skips_the_refusal() {
        let s = skip_declaration(Some("provider-singleton: one account"));
        assert_eq!(
            decide(AdmissionMode::Enforce, true, &fat(), &s),
            Admission::AdmitSkipped(SkipReason::ProviderSingleton)
        );
    }

    #[test]
    fn time_pressure_is_not_a_typed_reason() {
        let s = skip_declaration(Some("no-time"));
        assert!(matches!(s, SkipDeclaration::Invalid(_)));
        let a = decide(AdmissionMode::Enforce, true, &fat(), &s);
        let Admission::Refuse(msg) = a else {
            panic!("expected refusal")
        };
        assert!(msg.contains("not a typed world-fact reason"));
    }

    #[test]
    fn the_setting_turns_the_refusal_off_without_deleting_it() {
        assert_eq!(
            decide(AdmissionMode::Off, true, &fat(), &SkipDeclaration::Absent),
            Admission::AdmitOff
        );
        assert!(decide(
            AdmissionMode::Enforce,
            true,
            &fat(),
            &SkipDeclaration::Absent
        )
        .refused());
    }

    #[test]
    fn a_template_that_already_reconciled_is_grandfathered_not_refused() {
        assert_eq!(
            decide(
                AdmissionMode::Enforce,
                false,
                &fat(),
                &SkipDeclaration::Absent
            ),
            Admission::AdmitGrandfathered
        );
    }

    #[test]
    fn every_reason_round_trips() {
        for r in SkipReason::ALL {
            assert_eq!(SkipReason::parse(r.as_str()), Some(r));
        }
    }

    #[test]
    fn the_default_mode_enforces() {
        assert_eq!(AdmissionMode::default(), AdmissionMode::Enforce);
    }

    #[test]
    fn mode_parsing_is_closed() {
        assert_eq!(AdmissionMode::parse("OFF"), Some(AdmissionMode::Off));
        assert_eq!(
            AdmissionMode::parse("enforce"),
            Some(AdmissionMode::Enforce)
        );
        assert_eq!(AdmissionMode::parse("maybe"), None);
        assert_eq!(
            AdmissionMode::parse("generate"),
            Some(AdmissionMode::Generate)
        );
    }
}
