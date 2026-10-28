use std::collections::BTreeMap;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, JsonSchema,
)]
#[serde(rename_all = "camelCase")]
pub enum WorkClass {
    Apply,
    Change,
    Unblocked,
    Retry,
    Drift,
}

impl WorkClass {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Apply => "apply",
            Self::Change => "change",
            Self::Unblocked => "unblocked",
            Self::Retry => "retry",
            Self::Drift => "drift",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum PriorityMode {
    #[default]
    On,
    Off,
}

impl PriorityMode {
    #[must_use]
    pub fn parse(raw: &str) -> Option<Self> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "on" | "true" | "1" => Some(Self::On),
            "off" | "false" | "0" => Some(Self::Off),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Share(u16);

impl Share {
    pub const HALF: Self = Self(500);
    pub const NONE: Self = Self(0);

    #[must_use]
    pub fn from_fraction(f: f64) -> Option<Self> {
        if f.is_nan() || !(0.0..1.0).contains(&f) {
            return None;
        }
        let permille = (f * 1000.0).round();
        u16::try_from(permille as i64)
            .ok()
            .filter(|p| *p < 1000)
            .map(Self)
    }

    #[must_use]
    pub fn parse(raw: &str) -> Option<Self> {
        Self::from_fraction(raw.trim().parse::<f64>().ok()?)
    }

    #[must_use]
    pub fn permille(self) -> u16 {
        self.0
    }

    #[must_use]
    pub fn fraction(self) -> f64 {
        f64::from(self.0) / 1000.0
    }
}

impl Default for Share {
    fn default() -> Self {
        Self::HALF
    }
}

impl Serialize for Share {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_f64(self.fraction())
    }
}

impl<'de> Deserialize<'de> for Share {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let f = f64::deserialize(d)?;
        Self::from_fraction(f)
            .ok_or_else(|| serde::de::Error::custom("a share is a fraction in [0, 1)"))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Candidate {
    pub name: String,
    pub scope: String,
    pub credential: Option<String>,
    pub class: WorkClass,
    pub depth: u32,
    pub since: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Caps {
    pub global: usize,
    pub per_scope: usize,
    pub scope_caps: BTreeMap<String, usize>,
    pub per_credential: usize,
    pub drift_share_permille: u16,
    pub priority: PriorityMode,
}

impl Caps {
    #[must_use]
    pub fn scope_cap(&self, scope: &str) -> usize {
        self.scope_caps
            .get(scope)
            .copied()
            .unwrap_or(self.per_scope)
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct InFlight {
    pub total: usize,
    pub by_scope: BTreeMap<String, usize>,
    pub drift_by_scope: BTreeMap<String, usize>,
    pub by_credential: BTreeMap<String, usize>,
}

#[must_use]
pub fn drift_slots(scope_cap: usize, share_permille: u16) -> usize {
    if share_permille == 0 || scope_cap == 0 {
        return 0;
    }
    let share = usize::from(share_permille.min(1000));
    let raw = scope_cap.saturating_mul(share) / 1000;
    raw.clamp(1, scope_cap.saturating_sub(1).max(1))
}

#[must_use]
pub fn order(candidates: &[Candidate], mode: PriorityMode) -> Vec<&Candidate> {
    let mut v: Vec<&Candidate> = candidates.iter().collect();
    match mode {
        PriorityMode::On => v.sort_by(|a, b| {
            let age = |c: &Candidate| {
                if c.class == WorkClass::Drift {
                    c.since
                } else {
                    0
                }
            };
            (a.class, age(a), a.depth, &a.name).cmp(&(b.class, age(b), b.depth, &b.name))
        }),
        PriorityMode::Off => v.sort_by(|a, b| (a.since, &a.name).cmp(&(b.since, &b.name))),
    }
    v
}

#[must_use]
pub fn admit(candidates: &[Candidate], caps: &Caps, in_flight: &InFlight) -> Vec<String> {
    let mut f = in_flight.clone();
    let mut out = Vec::new();
    for c in order(candidates, caps.priority) {
        if f.total >= caps.global {
            break;
        }
        let scope_cap = caps.scope_cap(&c.scope);
        let in_scope = f.by_scope.get(&c.scope).copied().unwrap_or(0);
        if in_scope >= scope_cap {
            continue;
        }
        if c.class == WorkClass::Drift && caps.drift_share_permille > 0 {
            let held = f.drift_by_scope.get(&c.scope).copied().unwrap_or(0);
            if held >= drift_slots(scope_cap, caps.drift_share_permille) {
                continue;
            }
        }
        if let Some(cred) = &c.credential {
            if caps.per_credential > 0
                && f.by_credential.get(cred).copied().unwrap_or(0) >= caps.per_credential
            {
                continue;
            }
        }
        f.total += 1;
        *f.by_scope.entry(c.scope.clone()).or_default() += 1;
        if c.class == WorkClass::Drift {
            *f.drift_by_scope.entry(c.scope.clone()).or_default() += 1;
        }
        if let Some(cred) = &c.credential {
            *f.by_credential.entry(cred.clone()).or_default() += 1;
        }
        out.push(c.name.clone());
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cand(name: &str, class: WorkClass, since: u64) -> Candidate {
        Candidate {
            name: name.into(),
            scope: "org".into(),
            credential: Some("gh".into()),
            class,
            depth: 0,
            since,
        }
    }

    fn caps(slots: usize) -> Caps {
        Caps {
            global: 16,
            per_scope: slots,
            scope_caps: BTreeMap::new(),
            per_credential: 8,
            drift_share_permille: 500,
            priority: PriorityMode::On,
        }
    }

    fn twenty_drift_then_one_change() -> Vec<Candidate> {
        let mut v: Vec<Candidate> = (0..20)
            .map(|i| cand(&format!("drift-{i:02}"), WorkClass::Drift, i))
            .collect();
        v.push(cand("changed", WorkClass::Change, 100));
        v
    }

    #[test]
    fn twenty_drift_due_leaves_and_one_changed_leaf_with_four_slots_admit_the_changed_leaf_first() {
        let admitted = admit(
            &twenty_drift_then_one_change(),
            &caps(4),
            &InFlight::default(),
        );
        assert_eq!(admitted.first().map(String::as_str), Some("changed"));
        assert_eq!(admitted, vec!["changed", "drift-00", "drift-01"]);
    }

    #[test]
    fn drift_never_holds_more_than_its_share_so_a_change_always_finds_a_slot() {
        let drift: Vec<Candidate> = (0..20)
            .map(|i| cand(&format!("d{i:02}"), WorkClass::Drift, i))
            .collect();
        assert_eq!(admit(&drift, &caps(4), &InFlight::default()).len(), 2);
        let mut busy = InFlight::default();
        busy.total = 2;
        busy.by_scope.insert("org".into(), 2);
        busy.drift_by_scope.insert("org".into(), 2);
        assert!(admit(&drift, &caps(4), &busy).is_empty());
        let change = [cand("c", WorkClass::Change, 0)];
        assert_eq!(admit(&change, &caps(4), &busy), vec!["c"]);
    }

    #[test]
    fn the_classes_rank_apply_change_unblocked_retry_drift() {
        let v = vec![
            cand("e", WorkClass::Drift, 0),
            cand("d", WorkClass::Retry, 0),
            cand("c", WorkClass::Unblocked, 0),
            cand("b", WorkClass::Change, 0),
            cand("a", WorkClass::Apply, 0),
        ];
        let mut c = caps(16);
        c.per_credential = 0;
        assert_eq!(
            admit(&v, &c, &InFlight::default()),
            vec!["a", "b", "c", "d", "e"]
        );
    }

    #[test]
    fn ties_break_upstream_first_then_by_name_and_drift_oldest_first() {
        let mut deep = cand("a-deep", WorkClass::Change, 0);
        deep.depth = 2;
        let shallow = cand("z-shallow", WorkClass::Change, 0);
        let pair = [deep, shallow];
        let names: Vec<&str> = order(&pair, PriorityMode::On)
            .iter()
            .map(|c| c.name.as_str())
            .collect();
        assert_eq!(names, vec!["z-shallow", "a-deep"]);
        let old = cand("b", WorkClass::Drift, 1);
        let young = cand("a", WorkClass::Drift, 9);
        let pair = [young, old];
        let names: Vec<&str> = order(&pair, PriorityMode::On)
            .iter()
            .map(|c| c.name.as_str())
            .collect();
        assert_eq!(names, vec!["b", "a"]);
    }

    #[test]
    fn priority_off_is_arrival_order() {
        let mut c = caps(4);
        c.priority = PriorityMode::Off;
        c.drift_share_permille = 0;
        let admitted = admit(&twenty_drift_then_one_change(), &c, &InFlight::default());
        assert!(!admitted.contains(&"changed".to_string()));
        assert_eq!(
            admitted,
            vec!["drift-00", "drift-01", "drift-02", "drift-03"]
        );
    }

    #[test]
    fn the_per_credential_cap_binds_across_scopes_and_zero_is_no_cap() {
        let mut v = Vec::new();
        for s in 0..4 {
            for i in 0..4 {
                let mut c = cand(&format!("s{s}-{i}"), WorkClass::Change, 0);
                c.scope = format!("s{s}");
                v.push(c);
            }
        }
        let mut c = caps(4);
        c.per_credential = 8;
        assert_eq!(admit(&v, &c, &InFlight::default()).len(), 8);
        c.per_credential = 0;
        assert_eq!(admit(&v, &c, &InFlight::default()).len(), 16);
    }

    #[test]
    fn the_global_and_scope_caps_hold() {
        let v: Vec<Candidate> = (0..10)
            .map(|i| cand(&format!("c{i}"), WorkClass::Change, 0))
            .collect();
        let mut c = caps(10);
        c.global = 3;
        assert_eq!(admit(&v, &c, &InFlight::default()).len(), 3);
        let mut c = caps(10);
        c.scope_caps.insert("org".into(), 2);
        assert_eq!(admit(&v, &c, &InFlight::default()).len(), 2);
    }

    #[test]
    fn a_zero_drift_share_lifts_the_reserve_as_a_monolith_runs() {
        let drift: Vec<Candidate> = (0..20)
            .map(|i| cand(&format!("d{i:02}"), WorkClass::Drift, i))
            .collect();
        let mut c = caps(4);
        c.drift_share_permille = 0;
        assert_eq!(admit(&drift, &c, &InFlight::default()).len(), 4);
    }

    #[test]
    fn a_share_is_a_fraction_below_one() {
        assert_eq!(Share::parse("0.5"), Some(Share::HALF));
        assert_eq!(Share::parse("0"), Some(Share::NONE));
        assert_eq!(Share::parse("1.5"), None);
        assert_eq!(Share::parse("1"), None);
        assert_eq!(Share::parse("0.999").map(Share::permille), Some(999));
        assert_eq!(Share::parse("-0.1"), None);
        assert_eq!(Share::parse("NaN"), None);
        let s: Share = serde_json::from_str("0.25").unwrap();
        assert_eq!(s.permille(), 250);
        assert!(serde_json::from_str::<Share>("2").is_err());
        assert_eq!(serde_json::to_string(&s).unwrap(), "0.25");
    }

    #[test]
    fn drift_slots_reserve_at_least_one_change_slot() {
        assert_eq!(drift_slots(4, 500), 2);
        assert_eq!(drift_slots(4, 1000), 3);
        assert_eq!(drift_slots(1, 500), 1);
        assert_eq!(drift_slots(16, 0), 0);
        assert_eq!(drift_slots(3, 100), 1);
    }
}
