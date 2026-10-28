use std::sync::atomic::{AtomicU64, Ordering};

use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};

use crate::controller::status::{conditions_observably_equal, merge_condition_transitions};
use crate::crd::{Condition, InfrastructureTemplate};

pub const VARIABLES_UPDATED_AT: &str = "pangea.pleme.io/variables-updated-at";
pub const INPUTS_STALE: &str = "InputsStale";
pub const STALE_AFTER_ENV: &str = "PANGEA_INPUTS_STALE_AFTER_SECONDS";
pub const DEFAULT_STALE_AFTER_SECONDS: u64 = 7_200;

static STALE_AFTER_SECS: AtomicU64 = AtomicU64::new(DEFAULT_STALE_AFTER_SECONDS);

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InputsConfig {
    #[serde(default = "default_stale_after_secs")]
    pub stale_after_secs: u64,
}

fn default_stale_after_secs() -> u64 {
    DEFAULT_STALE_AFTER_SECONDS
}

impl Default for InputsConfig {
    fn default() -> Self {
        Self {
            stale_after_secs: DEFAULT_STALE_AFTER_SECONDS,
        }
    }
}

pub fn configure(cfg: &InputsConfig) {
    STALE_AFTER_SECS.store(cfg.stale_after_secs, Ordering::Relaxed);
}

#[must_use]
pub fn stale_after() -> StaleAfter {
    StaleAfter::from_secs(STALE_AFTER_SECS.load(Ordering::Relaxed))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StaleAfter {
    Off,
    After(Duration),
}

impl Default for StaleAfter {
    fn default() -> Self {
        Self::from_secs(DEFAULT_STALE_AFTER_SECONDS)
    }
}

impl StaleAfter {
    #[must_use]
    pub fn from_secs(secs: u64) -> Self {
        match i64::try_from(secs).ok().and_then(Duration::try_seconds) {
            Some(d) if secs > 0 => Self::After(d),
            _ => Self::Off,
        }
    }

    #[must_use]
    pub fn secs(self) -> u64 {
        match self {
            Self::Off => 0,
            Self::After(d) => u64::try_from(d.num_seconds()).unwrap_or(0),
        }
    }

    #[must_use]
    pub fn parse(raw: Option<&str>) -> Self {
        match raw.map(str::trim) {
            None | Some("") => Self::default(),
            Some("off") => Self::Off,
            Some(s) => match s.parse::<i64>() {
                Ok(n) if n > 0 => Self::from_secs(n.unsigned_abs()),
                Ok(_) => Self::Off,
                Err(_) => Self::default(),
            },
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InputsAge {
    Untracked,
    Unreadable(String),
    Fresh {
        updated_at: DateTime<Utc>,
    },
    Stale {
        updated_at: DateTime<Utc>,
        after: Duration,
    },
}

#[must_use]
pub fn stamp(at: DateTime<Utc>) -> String {
    at.to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
}

#[must_use]
pub fn inputs_age(t: &InfrastructureTemplate, now: DateTime<Utc>, after: StaleAfter) -> InputsAge {
    let StaleAfter::After(after) = after else {
        return InputsAge::Untracked;
    };
    let Some(raw) = t
        .metadata
        .annotations
        .as_ref()
        .and_then(|a| a.get(VARIABLES_UPDATED_AT))
    else {
        return InputsAge::Untracked;
    };
    match DateTime::parse_from_rfc3339(raw) {
        Err(_) => InputsAge::Unreadable(raw.clone()),
        Ok(at) => {
            let updated_at = at.with_timezone(&Utc);
            if now - updated_at > after {
                InputsAge::Stale { updated_at, after }
            } else {
                InputsAge::Fresh { updated_at }
            }
        }
    }
}

#[must_use]
pub fn age_seconds(age: &InputsAge, now: DateTime<Utc>) -> Option<i64> {
    match age {
        InputsAge::Fresh { updated_at } | InputsAge::Stale { updated_at, .. } => {
            Some((now - *updated_at).num_seconds().max(0))
        }
        InputsAge::Untracked | InputsAge::Unreadable(_) => None,
    }
}

#[must_use]
pub fn inputs_condition(
    age: &InputsAge,
    generation: Option<i64>,
    now: DateTime<Utc>,
) -> Option<Condition> {
    let (status, reason, message) = match age {
        InputsAge::Untracked => return None,
        InputsAge::Unreadable(raw) => (
            "Unknown",
            "UnreadableTimestamp",
            format!("annotation {VARIABLES_UPDATED_AT} is not RFC 3339: {raw}"),
        ),
        InputsAge::Fresh { updated_at } => (
            "False",
            "VariablesFresh",
            format!("spec.variables updated at {}", stamp(*updated_at)),
        ),
        InputsAge::Stale { updated_at, after } => (
            "True",
            "VariablesNotUpdated",
            format!(
                "spec.variables not updated since {}, longer than {}s; check the job that writes them",
                stamp(*updated_at),
                after.num_seconds()
            ),
        ),
    };
    Some(Condition {
        r#type: INPUTS_STALE.to_string(),
        status: status.to_string(),
        last_transition_time: now,
        reason: reason.to_string(),
        message,
        observed_generation: generation,
    })
}

#[must_use]
pub fn condition_patch(prev: &[Condition], next: Option<Condition>) -> Option<serde_json::Value> {
    let mut list: Vec<Condition> = prev
        .iter()
        .filter(|c| c.r#type != INPUTS_STALE)
        .cloned()
        .collect();
    list.extend(next);
    let list = merge_condition_transitions(prev, list);
    if conditions_observably_equal(prev, &list) {
        return None;
    }
    Some(serde_json::json!({ "status": { "conditions": list } }))
}

#[must_use]
pub fn patch_for(
    t: &InfrastructureTemplate,
    now: DateTime<Utc>,
    after: StaleAfter,
) -> Option<serde_json::Value> {
    let prev = t
        .status
        .as_ref()
        .map(|s| s.conditions.clone())
        .unwrap_or_default();
    let next = inputs_condition(&inputs_age(t, now, after), t.metadata.generation, now);
    condition_patch(&prev, next)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    fn at(s: i64) -> DateTime<Utc> {
        DateTime::from_timestamp(1_800_000_000 + s, 0).unwrap()
    }

    fn template(annotation: Option<&str>, conditions: Vec<Condition>) -> InfrastructureTemplate {
        let mut t: InfrastructureTemplate = serde_json::from_value(serde_json::json!({
            "apiVersion": "pangea.pleme.io/v1alpha1",
            "kind": "InfrastructureTemplate",
            "metadata": { "name": "t", "namespace": "n", "generation": 3 },
            "spec": { "source": {}, "pangeaNamespace": "ns" }
        }))
        .unwrap_or_else(|e| panic!("fixture template: {e}"));
        if let Some(a) = annotation {
            t.metadata.annotations = Some(BTreeMap::from([(
                VARIABLES_UPDATED_AT.to_string(),
                a.to_string(),
            )]));
        }
        let mut status = t.status.clone().unwrap_or_default();
        status.conditions = conditions;
        t.status = Some(status);
        t
    }

    fn after(secs: i64) -> StaleAfter {
        StaleAfter::After(Duration::seconds(secs))
    }

    #[test]
    fn the_switch_parses_off_zero_and_a_bad_value() {
        assert_eq!(StaleAfter::parse(None), StaleAfter::default());
        assert_eq!(StaleAfter::parse(Some("off")), StaleAfter::Off);
        assert_eq!(StaleAfter::parse(Some("0")), StaleAfter::Off);
        assert_eq!(StaleAfter::parse(Some("60")), after(60));
        assert_eq!(StaleAfter::parse(Some("soon")), StaleAfter::default());
    }

    #[test]
    fn seconds_round_trip_through_the_switch() {
        assert_eq!(StaleAfter::from_secs(0), StaleAfter::Off);
        assert_eq!(StaleAfter::from_secs(60), after(60));
        assert_eq!(StaleAfter::from_secs(u64::MAX), StaleAfter::Off);
        assert_eq!(StaleAfter::Off.secs(), 0);
        assert_eq!(after(60).secs(), 60);
        assert_eq!(StaleAfter::parse(Some("90")).secs(), 90);
    }

    #[test]
    fn a_template_without_the_annotation_gets_no_condition() {
        let t = template(None, vec![]);
        assert_eq!(inputs_age(&t, at(0), after(60)), InputsAge::Untracked);
        assert!(patch_for(&t, at(0), after(60)).is_none());
    }

    #[test]
    fn variables_older_than_the_threshold_read_as_stale() {
        let t = template(Some(&stamp(at(0))), vec![]);
        let fresh = inputs_age(&t, at(60), after(60));
        let stale = inputs_age(&t, at(61), after(60));
        assert!(matches!(fresh, InputsAge::Fresh { .. }));
        assert!(matches!(stale, InputsAge::Stale { .. }));
        assert_eq!(age_seconds(&stale, at(61)), Some(61));
        let c = inputs_condition(&stale, Some(3), at(61)).unwrap();
        assert_eq!(
            (c.status.as_str(), c.reason.as_str()),
            ("True", "VariablesNotUpdated")
        );
    }

    #[test]
    fn a_stale_condition_patch_is_written_once_and_then_stays_quiet() {
        let t = template(Some(&stamp(at(0))), vec![]);
        let patch = patch_for(&t, at(100), after(60)).expect("first patch");
        let conditions: Vec<Condition> =
            serde_json::from_value(patch["status"]["conditions"].clone()).unwrap();
        let settled = template(Some(&stamp(at(0))), conditions);
        assert!(patch_for(&settled, at(130), after(60)).is_none());
    }

    #[test]
    fn other_conditions_survive_and_turning_it_off_removes_it() {
        let ready = Condition {
            r#type: "Ready".into(),
            status: "True".into(),
            last_transition_time: at(0),
            reason: "Applied".into(),
            message: "ok".into(),
            observed_generation: Some(3),
        };
        let stale = inputs_condition(
            &InputsAge::Stale {
                updated_at: at(0),
                after: Duration::seconds(60),
            },
            Some(3),
            at(100),
        )
        .unwrap();
        let t = template(Some(&stamp(at(0))), vec![ready.clone(), stale]);
        let patch = patch_for(&t, at(100), StaleAfter::Off).expect("removal patch");
        let conditions: Vec<Condition> =
            serde_json::from_value(patch["status"]["conditions"].clone()).unwrap();
        assert_eq!(conditions.len(), 1);
        assert_eq!(conditions[0].r#type, "Ready");
    }

    #[test]
    fn an_unparseable_annotation_is_unknown_not_fresh() {
        let t = template(Some("yesterday"), vec![]);
        let c = inputs_condition(&inputs_age(&t, at(0), after(60)), Some(3), at(0)).unwrap();
        assert_eq!(c.status, "Unknown");
    }
}
