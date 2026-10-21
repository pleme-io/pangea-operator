use chrono::{DateTime, Duration, Utc};

use crate::controller::status::merge_condition_transitions;
use crate::crd::{Condition, InfrastructureTemplateStatus, PassKind, PassStatus, Phase};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Queued {
    pub generation: i64,
    pub since: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PassPhase {
    Settled {
        generation: i64,
    },
    Pending {
        generation: i64,
        since: DateTime<Utc>,
    },
    Planning {
        generation: i64,
        started_at: DateTime<Utc>,
        heartbeat: DateTime<Utc>,
        queued: Option<Queued>,
    },
    Applying {
        generation: i64,
        started_at: DateTime<Utc>,
        heartbeat: DateTime<Utc>,
        queued: Option<Queued>,
    },
    Failed {
        generation: i64,
        reason: String,
    },
    Stalled {
        generation: i64,
        since: DateTime<Utc>,
        reason: String,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Deadlines {
    pub heartbeat: Duration,
    pub pending_start: Duration,
}

impl Default for Deadlines {
    fn default() -> Self {
        Self {
            heartbeat: Duration::minutes(5),
            pending_start: Duration::minutes(30),
        }
    }
}

impl Deadlines {
    pub fn from_env() -> Self {
        let read = |name: &str, default: Duration| {
            std::env::var(name)
                .ok()
                .and_then(|v| v.trim().parse::<i64>().ok())
                .filter(|s| *s > 0)
                .map(Duration::seconds)
                .unwrap_or(default)
        };
        let d = Self::default();
        Self {
            heartbeat: read("PANGEA_WATCHDOG_HEARTBEAT_SECS", d.heartbeat),
            pending_start: read("PANGEA_WATCHDOG_PENDING_START_SECS", d.pending_start),
        }
    }
}

impl PassPhase {
    pub fn generation(&self) -> i64 {
        match self {
            Self::Settled { generation }
            | Self::Pending { generation, .. }
            | Self::Planning { generation, .. }
            | Self::Applying { generation, .. }
            | Self::Failed { generation, .. }
            | Self::Stalled { generation, .. } => *generation,
        }
    }

    pub fn kind(&self) -> PassKind {
        match self {
            Self::Settled { .. } => PassKind::Settled,
            Self::Pending { .. } => PassKind::Pending,
            Self::Planning { .. } => PassKind::Planning,
            Self::Applying { .. } => PassKind::Applying,
            Self::Failed { .. } => PassKind::Failed,
            Self::Stalled { .. } => PassKind::Stalled,
        }
    }

    pub fn is_active(&self) -> bool {
        matches!(self, Self::Planning { .. } | Self::Applying { .. })
    }

    pub fn queued(&self) -> Option<&Queued> {
        match self {
            Self::Planning { queued, .. } | Self::Applying { queued, .. } => queued.as_ref(),
            _ => None,
        }
    }

    pub fn blocked_behind(&self) -> Option<String> {
        match self {
            Self::Planning {
                generation,
                started_at,
                queued: Some(q),
                ..
            } => Some(format!(
                "generation {} is queued behind the Planning pass for generation {generation} started {}",
                q.generation,
                started_at.to_rfc3339()
            )),
            Self::Applying {
                generation,
                started_at,
                queued: Some(q),
                ..
            } => Some(format!(
                "generation {} is queued behind the Applying pass for generation {generation} started {}",
                q.generation,
                started_at.to_rfc3339()
            )),
            _ => None,
        }
    }

    pub fn lag_since(&self) -> Option<DateTime<Utc>> {
        match self {
            Self::Pending { since, .. } => Some(*since),
            other => other.queued().map(|q| q.since),
        }
    }

    pub fn observed_generation(&self, previous: i64) -> i64 {
        match self {
            Self::Settled { generation } => previous.max(*generation),
            Self::Failed { generation, reason } if !reason.is_empty() => previous.max(*generation),
            _ => previous,
        }
    }

    pub fn from_status(status: &PassStatus, fallback: DateTime<Utc>) -> Self {
        let generation = status.generation;
        let since = status.since.unwrap_or(fallback);
        let queued = match (status.queued_generation, status.queued_since) {
            (Some(g), Some(s)) => Some(Queued {
                generation: g,
                since: s,
            }),
            (Some(g), None) => Some(Queued {
                generation: g,
                since: fallback,
            }),
            _ => None,
        };
        let reason = status.reason.clone().unwrap_or_default();
        match status.kind {
            PassKind::Settled => Self::Settled { generation },
            PassKind::Pending => Self::Pending { generation, since },
            PassKind::Planning => Self::Planning {
                generation,
                started_at: since,
                heartbeat: status.heartbeat_at.unwrap_or(since),
                queued,
            },
            PassKind::Applying => Self::Applying {
                generation,
                started_at: since,
                heartbeat: status.heartbeat_at.unwrap_or(since),
                queued,
            },
            PassKind::Failed => Self::Failed { generation, reason },
            PassKind::Stalled => Self::Stalled {
                generation,
                since,
                reason,
            },
        }
    }

    pub fn to_status(&self) -> PassStatus {
        let mut s = PassStatus {
            kind: self.kind(),
            generation: self.generation(),
            since: None,
            heartbeat_at: None,
            reason: None,
            queued_generation: None,
            queued_since: None,
        };
        match self {
            Self::Settled { .. } => {}
            Self::Pending { since, .. } => s.since = Some(*since),
            Self::Planning {
                started_at,
                heartbeat,
                queued,
                ..
            }
            | Self::Applying {
                started_at,
                heartbeat,
                queued,
                ..
            } => {
                s.since = Some(*started_at);
                s.heartbeat_at = Some(*heartbeat);
                s.queued_generation = queued.as_ref().map(|q| q.generation);
                s.queued_since = queued.as_ref().map(|q| q.since);
            }
            Self::Failed { reason, .. } => s.reason = Some(reason.clone()),
            Self::Stalled { since, reason, .. } => {
                s.since = Some(*since);
                s.reason = Some(reason.clone());
            }
        }
        s
    }

    pub fn entering(
        phase: Phase,
        generation: i64,
        error: Option<&str>,
        previous: Option<&PassPhase>,
        now: DateTime<Utc>,
    ) -> Self {
        let carried_queue = previous.and_then(|p| p.queued()).cloned();
        match phase {
            Phase::Ready | Phase::Drifted => Self::Settled { generation },
            Phase::Failed | Phase::CompileBlocked => Self::Failed {
                generation,
                reason: error
                    .map(str::to_string)
                    .or_else(|| match previous {
                        Some(Self::Failed { reason, .. }) => Some(reason.clone()),
                        _ => None,
                    })
                    .unwrap_or_else(|| phase.to_string()),
            },
            Phase::Planning | Phase::Compiling | Phase::Initializing => {
                let started_at = match previous {
                    Some(Self::Planning {
                        generation: g,
                        started_at,
                        ..
                    }) if *g == generation => *started_at,
                    _ => now,
                };
                Self::Planning {
                    generation,
                    started_at,
                    heartbeat: now,
                    queued: carried_queue,
                }
            }
            Phase::Applying => {
                let started_at = match previous {
                    Some(Self::Applying {
                        generation: g,
                        started_at,
                        ..
                    }) if *g == generation => *started_at,
                    _ => now,
                };
                Self::Applying {
                    generation,
                    started_at,
                    heartbeat: now,
                    queued: carried_queue,
                }
            }
            Phase::Pending | Phase::Verifying | Phase::Verified | Phase::Destroying => {
                let since = match previous {
                    Some(Self::Pending {
                        generation: g,
                        since,
                    }) if *g == generation => *since,
                    _ => now,
                };
                Self::Pending { generation, since }
            }
        }
    }

    pub fn observe(self, live_generation: i64, now: DateTime<Utc>) -> Self {
        if live_generation <= self.generation() {
            return match self {
                Self::Planning {
                    generation,
                    started_at,
                    heartbeat,
                    queued: Some(q),
                } if q.generation <= generation => Self::Planning {
                    generation,
                    started_at,
                    heartbeat,
                    queued: None,
                },
                Self::Applying {
                    generation,
                    started_at,
                    heartbeat,
                    queued: Some(q),
                } if q.generation <= generation => Self::Applying {
                    generation,
                    started_at,
                    heartbeat,
                    queued: None,
                },
                other => other,
            };
        }
        match self {
            Self::Settled { .. } | Self::Failed { .. } => Self::Pending {
                generation: live_generation,
                since: now,
            },
            Self::Pending { since, .. } => Self::Pending {
                generation: live_generation,
                since,
            },
            Self::Planning {
                generation,
                started_at,
                heartbeat,
                queued,
            } => Self::Planning {
                generation,
                started_at,
                heartbeat,
                queued: Some(Queued {
                    generation: live_generation,
                    since: queued.map(|q| q.since).unwrap_or(now),
                }),
            },
            Self::Applying {
                generation,
                started_at,
                heartbeat,
                queued,
            } => Self::Applying {
                generation,
                started_at,
                heartbeat,
                queued: Some(Queued {
                    generation: live_generation,
                    since: queued.map(|q| q.since).unwrap_or(now),
                }),
            },
            Self::Stalled { since, reason, .. } => Self::Stalled {
                generation: live_generation,
                since,
                reason,
            },
        }
    }

    pub fn beat(self, now: DateTime<Utc>) -> Self {
        match self {
            Self::Planning {
                generation,
                started_at,
                queued,
                ..
            } => Self::Planning {
                generation,
                started_at,
                heartbeat: now,
                queued,
            },
            Self::Applying {
                generation,
                started_at,
                queued,
                ..
            } => Self::Applying {
                generation,
                started_at,
                heartbeat: now,
                queued,
            },
            other => other,
        }
    }

    pub fn watchdog(self, now: DateTime<Utc>, deadlines: &Deadlines) -> Self {
        match &self {
            Self::Planning {
                generation,
                started_at,
                heartbeat,
                queued,
            }
            | Self::Applying {
                generation,
                started_at,
                heartbeat,
                queued,
            } => {
                let label = if matches!(self, Self::Planning { .. }) {
                    "Planning"
                } else {
                    "Applying"
                };
                if now - *heartbeat > deadlines.heartbeat {
                    return Self::Stalled {
                        generation: queued.as_ref().map(|q| q.generation).unwrap_or(*generation),
                        since: now,
                        reason: format!(
                            "{label} pass for generation {generation} started {} has no heartbeat for {}s",
                            started_at.to_rfc3339(),
                            (now - *heartbeat).num_seconds()
                        ),
                    };
                }
                if let Some(q) = queued {
                    if now - q.since > deadlines.pending_start {
                        return Self::Stalled {
                            generation: q.generation,
                            since: now,
                            reason: format!(
                                "generation {} queued {}s behind {label} pass for generation {generation} started {}",
                                q.generation,
                                (now - q.since).num_seconds(),
                                started_at.to_rfc3339()
                            ),
                        };
                    }
                }
                self
            }
            Self::Pending { generation, since } if now - *since > deadlines.pending_start => {
                Self::Stalled {
                    generation: *generation,
                    since: now,
                    reason: format!(
                        "generation {generation} pending for {}s and no pass started",
                        (now - *since).num_seconds()
                    ),
                }
            }
            _ => self,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ConditionSet(Vec<Condition>);

impl ConditionSet {
    pub fn project(
        mut conditions: Vec<Condition>,
        pass: &PassPhase,
        observed_generation: i64,
        live_generation: i64,
        now: DateTime<Utc>,
    ) -> Self {
        let caught_up = observed_generation >= live_generation;
        let may_claim_good =
            caught_up && !matches!(pass, PassPhase::Stalled { .. } | PassPhase::Failed { .. });
        let behind_reason = pass.blocked_behind().unwrap_or_else(|| {
            format!(
                "observed generation {observed_generation} is behind generation {live_generation}"
            )
        });
        for c in &mut conditions {
            c.observed_generation = Some(live_generation);
            let claims_good = matches!(c.r#type.as_str(), "Ready" | "Settled");
            if claims_good && c.status == "True" && !may_claim_good {
                c.status = "False".to_string();
                c.reason = if caught_up {
                    "PassNotHealthy"
                } else {
                    "GenerationBehind"
                }
                .to_string();
                c.message = if caught_up {
                    format!(
                        "pass for generation {} is {}",
                        pass.generation(),
                        pass.kind()
                    )
                } else {
                    behind_reason.clone()
                };
            }
            if c.r#type == "Reconciling" && c.status != "True" && !caught_up {
                c.status = "True".to_string();
                c.reason = "GenerationBehind".to_string();
                c.message = behind_reason.clone();
            }
        }
        conditions.retain(|c| c.r#type != "Stalled");
        let (stalled, reason, message) = match pass {
            PassPhase::Stalled { reason, .. } => ("True", "Stalled", reason.clone()),
            _ => (
                "False",
                "NotStalled",
                "pass is making progress or idle".to_string(),
            ),
        };
        conditions.push(Condition {
            r#type: "Stalled".to_string(),
            status: stalled.to_string(),
            last_transition_time: now,
            reason: reason.to_string(),
            message,
            observed_generation: Some(live_generation),
        });
        Self(conditions)
    }

    pub fn conditions(&self) -> &[Condition] {
        &self.0
    }

    pub fn into_vec(self) -> Vec<Condition> {
        self.0
    }
}

pub fn violates_generation_invariant(
    conditions: &[Condition],
    observed_generation: i64,
    live_generation: i64,
) -> bool {
    conditions.iter().any(|c| {
        c.status == "True"
            && matches!(c.r#type.as_str(), "Ready" | "Settled")
            && (observed_generation < live_generation
                || c.observed_generation.map_or(true, |g| g < live_generation))
    })
}

#[derive(Debug, Clone)]
pub struct StatusProjection {
    pub pass: PassPhase,
    pub observed_generation: i64,
    pub conditions: Vec<Condition>,
    pub lag_since: Option<DateTime<Utc>>,
}

pub fn stored_pass(status: &InfrastructureTemplateStatus, now: DateTime<Utc>) -> PassPhase {
    match &status.pass {
        Some(p) => PassPhase::from_status(p, now),
        None => PassPhase::entering(
            status.phase.unwrap_or_default(),
            status.acknowledged(),
            status.last_error.as_deref(),
            None,
            now,
        ),
    }
}

pub fn project_status(
    status: &InfrastructureTemplateStatus,
    live_generation: i64,
    now: DateTime<Utc>,
    deadlines: &Deadlines,
) -> StatusProjection {
    let pass = stored_pass(status, now)
        .observe(live_generation, now)
        .watchdog(now, deadlines);
    let observed_generation = pass.observed_generation(status.observed_generation);
    let projected = ConditionSet::project(
        status.conditions.clone(),
        &pass,
        observed_generation,
        live_generation,
        now,
    )
    .into_vec();
    let conditions = merge_condition_transitions(&status.conditions, projected);
    let lag_since = pass.lag_since();
    StatusProjection {
        pass,
        observed_generation,
        conditions,
        lag_since,
    }
}

pub fn conditions_differ(a: &[Condition], b: &[Condition]) -> bool {
    let key = |c: &Condition| {
        (
            c.r#type.clone(),
            c.status.clone(),
            c.reason.clone(),
            c.message.clone(),
            c.observed_generation,
        )
    };
    let mut ka: Vec<_> = a.iter().map(key).collect();
    let mut kb: Vec<_> = b.iter().map(key).collect();
    ka.sort();
    kb.sort();
    ka != kb
}

pub fn projection_patch(
    status: &InfrastructureTemplateStatus,
    projection: &StatusProjection,
) -> Option<serde_json::Value> {
    let mut patch = serde_json::Map::new();
    let new_pass = projection.pass.to_status();
    match &status.pass {
        Some(old) if old.kind == new_pass.kind => {
            let mut fields = serde_json::Map::new();
            if old.generation != new_pass.generation {
                fields.insert("generation".into(), serde_json::json!(new_pass.generation));
            }
            if old.queued_generation != new_pass.queued_generation {
                fields.insert(
                    "queuedGeneration".into(),
                    serde_json::json!(new_pass.queued_generation),
                );
            }
            if old.queued_since != new_pass.queued_since {
                fields.insert(
                    "queuedSince".into(),
                    serde_json::json!(new_pass.queued_since),
                );
            }
            if !fields.is_empty() {
                patch.insert("pass".into(), serde_json::Value::Object(fields));
            }
        }
        _ => {
            patch.insert("pass".into(), serde_json::json!(new_pass));
        }
    }
    if projection.observed_generation != status.observed_generation {
        patch.insert(
            "observedGeneration".into(),
            serde_json::json!(projection.observed_generation),
        );
    }
    if conditions_differ(&status.conditions, &projection.conditions) {
        patch.insert(
            "conditions".into(),
            serde_json::json!(projection.conditions),
        );
    }
    if projection.lag_since != status.generation_lag_since {
        patch.insert(
            "generationLagSince".into(),
            serde_json::json!(projection.lag_since),
        );
    }
    if patch.is_empty() {
        None
    } else {
        Some(serde_json::json!({ "status": patch }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn at(secs: i64) -> DateTime<Utc> {
        Utc.timestamp_opt(1_800_000_000 + secs, 0).unwrap()
    }

    const ALL_KINDS: [PassKind; 6] = [
        PassKind::Settled,
        PassKind::Pending,
        PassKind::Planning,
        PassKind::Applying,
        PassKind::Failed,
        PassKind::Stalled,
    ];

    fn rank(kind: PassKind) -> usize {
        match kind {
            PassKind::Settled => 0,
            PassKind::Pending => 1,
            PassKind::Planning => 2,
            PassKind::Applying => 3,
            PassKind::Failed => 4,
            PassKind::Stalled => 5,
        }
    }

    fn sample(kind: PassKind, generation: i64) -> PassPhase {
        match kind {
            PassKind::Settled => PassPhase::Settled { generation },
            PassKind::Pending => PassPhase::Pending {
                generation,
                since: at(0),
            },
            PassKind::Planning => PassPhase::Planning {
                generation,
                started_at: at(0),
                heartbeat: at(0),
                queued: None,
            },
            PassKind::Applying => PassPhase::Applying {
                generation,
                started_at: at(0),
                heartbeat: at(0),
                queued: None,
            },
            PassKind::Failed => PassPhase::Failed {
                generation,
                reason: "boom".into(),
            },
            PassKind::Stalled => PassPhase::Stalled {
                generation,
                since: at(0),
                reason: "stuck".into(),
            },
        }
    }

    fn cond(kind: &str, status: &str) -> Condition {
        Condition {
            r#type: kind.into(),
            status: status.into(),
            last_transition_time: at(0),
            reason: "Ready".into(),
            message: "claim".into(),
            observed_generation: None,
        }
    }

    fn good_claims() -> Vec<Condition> {
        vec![
            cond("Ready", "True"),
            cond("Settled", "True"),
            cond("Reconciling", "False"),
            cond("DriftDetected", "False"),
        ]
    }

    fn status_of<'a>(set: &'a [Condition], kind: &str) -> &'a str {
        &set.iter().find(|c| c.r#type == kind).unwrap().status
    }

    #[test]
    fn every_kind_is_enumerated_once() {
        let mut ranks: Vec<usize> = ALL_KINDS.iter().map(|k| rank(*k)).collect();
        ranks.sort();
        assert_eq!(ranks, vec![0, 1, 2, 3, 4, 5]);
        for k in ALL_KINDS {
            assert_eq!(sample(k, 3).kind(), k);
        }
    }

    #[test]
    fn good_claims_are_unconstructible_while_observed_trails_generation() {
        for kind in ALL_KINDS {
            for pass_gen in 0..6 {
                for observed in 0..6 {
                    for live in 0..6 {
                        let set = ConditionSet::project(
                            good_claims(),
                            &sample(kind, pass_gen),
                            observed,
                            live,
                            at(10),
                        );
                        let conds = set.conditions();
                        assert!(
                            !violates_generation_invariant(conds, observed, live),
                            "{kind:?} pass_gen={pass_gen} observed={observed} live={live}"
                        );
                        let healthy = !matches!(kind, PassKind::Stalled | PassKind::Failed);
                        let may = observed >= live && healthy;
                        assert_eq!(status_of(conds, "Ready") == "True", may);
                        assert_eq!(status_of(conds, "Settled") == "True", may);
                        assert!(conds.iter().all(|c| c.observed_generation == Some(live)));
                        assert_eq!(
                            status_of(conds, "Stalled") == "True",
                            kind == PassKind::Stalled
                        );
                        if observed < live {
                            assert_eq!(status_of(conds, "Reconciling"), "True");
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn the_predicate_names_the_lie_it_guards() {
        let mut lie = good_claims();
        for c in &mut lie {
            c.observed_generation = Some(20);
        }
        assert!(violates_generation_invariant(&lie, 20, 21));
        assert!(violates_generation_invariant(&lie, 21, 21));
        assert!(!violates_generation_invariant(&lie, 20, 20));
    }

    #[test]
    fn observed_generation_advances_only_on_a_completed_or_reasoned_failed_pass() {
        for kind in ALL_KINDS {
            for pass_gen in 0..6 {
                for previous in 0..6 {
                    let next = sample(kind, pass_gen).observed_generation(previous);
                    let advances = matches!(kind, PassKind::Settled | PassKind::Failed);
                    let expected = if advances {
                        previous.max(pass_gen)
                    } else {
                        previous
                    };
                    assert_eq!(next, expected, "{kind:?} {pass_gen} {previous}");
                    assert!(next >= previous);
                }
            }
        }
        let unreasoned = PassPhase::Failed {
            generation: 9,
            reason: String::new(),
        };
        assert_eq!(unreasoned.observed_generation(2), 2);
    }

    #[test]
    fn a_new_generation_queues_behind_a_running_pass_and_keeps_its_first_sighting() {
        let running = sample(PassKind::Planning, 20);
        let first = running.observe(21, at(100));
        assert_eq!(
            first.queued(),
            Some(&Queued {
                generation: 21,
                since: at(100)
            })
        );
        let again = first.clone().observe(21, at(200));
        assert_eq!(again, first);
        let newer = first.observe(22, at(300));
        assert_eq!(
            newer.queued(),
            Some(&Queued {
                generation: 22,
                since: at(100)
            })
        );
        assert_eq!(newer.generation(), 20);
    }

    #[test]
    fn an_idle_pass_becomes_pending_at_the_live_generation() {
        assert_eq!(
            sample(PassKind::Settled, 20).observe(21, at(5)),
            PassPhase::Pending {
                generation: 21,
                since: at(5)
            }
        );
        assert_eq!(
            sample(PassKind::Failed, 20).observe(21, at(5)).kind(),
            PassKind::Pending
        );
        assert_eq!(
            sample(PassKind::Pending, 20).observe(22, at(9)),
            PassPhase::Pending {
                generation: 22,
                since: at(0)
            }
        );
        assert_eq!(
            sample(PassKind::Settled, 21).observe(21, at(5)),
            sample(PassKind::Settled, 21)
        );
    }

    #[test]
    fn the_watchdog_stalls_a_silent_active_pass() {
        let d = Deadlines::default();
        for kind in [PassKind::Planning, PassKind::Applying] {
            let live = sample(kind, 4);
            assert_eq!(live.clone().watchdog(at(299), &d), live);
            let stalled = live.watchdog(at(301), &d);
            assert_eq!(stalled.kind(), PassKind::Stalled);
            match stalled {
                PassPhase::Stalled { reason, since, .. } => {
                    assert!(reason.contains("no heartbeat"));
                    assert_eq!(since, at(301));
                }
                other => panic!("{other:?}"),
            }
        }
    }

    #[test]
    fn a_beat_keeps_a_pass_alive() {
        let d = Deadlines::default();
        let beaten = sample(PassKind::Applying, 4).beat(at(290));
        assert_eq!(beaten.clone().watchdog(at(500), &d), beaten);
        assert_eq!(
            sample(PassKind::Settled, 4).beat(at(290)),
            sample(PassKind::Settled, 4)
        );
    }

    #[test]
    fn the_watchdog_stalls_a_pending_generation_that_never_starts() {
        let d = Deadlines::default();
        let pending = sample(PassKind::Pending, 7);
        assert_eq!(pending.clone().watchdog(at(1799), &d), pending);
        match pending.watchdog(at(1801), &d) {
            PassPhase::Stalled {
                generation, reason, ..
            } => {
                assert_eq!(generation, 7);
                assert!(reason.contains("no pass started"));
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn the_watchdog_names_the_pass_a_queued_generation_is_blocked_behind() {
        let d = Deadlines::default();
        let blocked = sample(PassKind::Planning, 20)
            .observe(21, at(0))
            .beat(at(1790));
        match blocked.clone().watchdog(at(1801), &d) {
            PassPhase::Stalled {
                generation, reason, ..
            } => {
                assert_eq!(generation, 21);
                assert!(reason.contains("queued"));
                assert!(reason.contains("behind Planning pass for generation 20"));
            }
            other => panic!("{other:?}"),
        }
        assert_eq!(blocked.clone().watchdog(at(1000), &d), blocked);
    }

    #[test]
    fn settled_and_failed_and_stalled_never_stall_again() {
        let d = Deadlines::default();
        for kind in [PassKind::Settled, PassKind::Failed, PassKind::Stalled] {
            let p = sample(kind, 3);
            assert_eq!(p.clone().watchdog(at(100_000), &d), p);
        }
    }

    #[test]
    fn the_wire_form_round_trips_every_variant() {
        for kind in ALL_KINDS {
            let p = sample(kind, 8);
            assert_eq!(PassPhase::from_status(&p.to_status(), at(0)), p);
        }
        let queued = sample(PassKind::Applying, 8).observe(9, at(4));
        assert_eq!(PassPhase::from_status(&queued.to_status(), at(0)), queued);
    }

    #[test]
    fn legacy_phases_map_onto_the_pass_without_inventing_progress() {
        let n = at(50);
        let cases = [
            (Phase::Ready, PassKind::Settled),
            (Phase::Drifted, PassKind::Settled),
            (Phase::Failed, PassKind::Failed),
            (Phase::CompileBlocked, PassKind::Failed),
            (Phase::Planning, PassKind::Planning),
            (Phase::Compiling, PassKind::Planning),
            (Phase::Initializing, PassKind::Planning),
            (Phase::Applying, PassKind::Applying),
            (Phase::Pending, PassKind::Pending),
            (Phase::Verifying, PassKind::Pending),
            (Phase::Verified, PassKind::Pending),
            (Phase::Destroying, PassKind::Pending),
        ];
        for (phase, kind) in cases {
            assert_eq!(PassPhase::entering(phase, 3, None, None, n).kind(), kind);
        }
        let started = PassPhase::entering(Phase::Planning, 3, None, None, at(10));
        let resumed = PassPhase::entering(Phase::Planning, 3, None, Some(&started), at(90));
        match resumed {
            PassPhase::Planning {
                started_at,
                heartbeat,
                ..
            } => {
                assert_eq!(started_at, at(10));
                assert_eq!(heartbeat, at(90));
            }
            other => panic!("{other:?}"),
        }
        let reasoned = PassPhase::entering(Phase::Failed, 3, Some("bad"), None, n);
        assert_eq!(reasoned.observed_generation(0), 3);
    }

    fn stale_ready_status() -> InfrastructureTemplateStatus {
        InfrastructureTemplateStatus {
            phase: Some(Phase::Ready),
            observed_generation: 20,
            acknowledged_generation: 20,
            conditions: good_claims(),
            ..Default::default()
        }
    }

    #[test]
    fn a_ready_template_behind_its_spec_is_repaired_to_pending() {
        let status = stale_ready_status();
        let p = project_status(&status, 21, at(100), &Deadlines::default());
        assert_eq!(
            p.pass,
            PassPhase::Pending {
                generation: 21,
                since: at(100)
            }
        );
        assert_eq!(p.observed_generation, 20);
        assert_eq!(p.lag_since, Some(at(100)));
        assert_eq!(status_of(&p.conditions, "Ready"), "False");
        assert_eq!(status_of(&p.conditions, "Settled"), "False");
        assert_eq!(status_of(&p.conditions, "Reconciling"), "True");
        assert!(!violates_generation_invariant(&p.conditions, 20, 21));
        let patch = projection_patch(&status, &p).expect("a lying status must be patched");
        assert_eq!(patch["status"]["pass"]["kind"], "pending");
        assert_eq!(patch["status"]["pass"]["generation"], 21);
    }

    #[test]
    fn a_caught_up_ready_template_is_left_alone_after_one_adoption_patch() {
        let status = stale_ready_status();
        let first = project_status(&status, 20, at(100), &Deadlines::default());
        assert_eq!(status_of(&first.conditions, "Ready"), "True");
        let patch = projection_patch(&status, &first).expect("adoption writes pass and Stalled");
        assert_eq!(patch["status"]["pass"]["kind"], "settled");
        let mut adopted = status.clone();
        adopted.pass = Some(first.pass.to_status());
        adopted.conditions = first.conditions.clone();
        let second = project_status(&adopted, 20, at(200), &Deadlines::default());
        assert!(projection_patch(&adopted, &second).is_none());
    }

    #[test]
    fn a_pass_that_outlives_its_heartbeat_is_reported_stalled_not_ready() {
        let mut status = stale_ready_status();
        status.pass = Some(sample(PassKind::Planning, 20).to_status());
        let p = project_status(&status, 20, at(400), &Deadlines::default());
        assert_eq!(p.pass.kind(), PassKind::Stalled);
        assert_eq!(status_of(&p.conditions, "Stalled"), "True");
        assert_eq!(status_of(&p.conditions, "Ready"), "False");
        assert_eq!(p.observed_generation, 20);
    }

    #[test]
    fn a_generation_blocked_behind_a_long_pass_is_stalled_with_the_blocker_named() {
        let mut status = stale_ready_status();
        let mut running = sample(PassKind::Planning, 20).beat(at(1790));
        running = running.observe(21, at(0));
        status.pass = Some(running.to_status());
        let p = project_status(&status, 21, at(1801), &Deadlines::default());
        let reason = match &p.pass {
            PassPhase::Stalled { reason, .. } => reason.clone(),
            other => panic!("{other:?}"),
        };
        assert!(reason.contains("behind Planning pass for generation 20"));
        let stalled = p.conditions.iter().find(|c| c.r#type == "Stalled").unwrap();
        assert_eq!(stalled.message, reason);
    }

    #[test]
    fn receipt_2026_10_21_ready_true_while_generation_21_waited_behind_the_18_35_pass() {
        let base = Utc.with_ymd_and_hms(2026, 10, 21, 18, 35, 5).unwrap();
        let mut status = InfrastructureTemplateStatus {
            phase: Some(Phase::Ready),
            observed_generation: 20,
            acknowledged_generation: 20,
            conditions: good_claims(),
            ..Default::default()
        };
        let pass = PassPhase::Planning {
            generation: 20,
            started_at: base,
            heartbeat: base + Duration::minutes(10),
            queued: None,
        };
        status.pass = Some(pass.to_status());
        let patched_at = base + Duration::minutes(13);
        let p = project_status(&status, 21, patched_at, &Deadlines::default());
        assert_eq!(status_of(&p.conditions, "Ready"), "False");
        assert_eq!(status_of(&p.conditions, "Settled"), "False");
        assert_eq!(status_of(&p.conditions, "Reconciling"), "True");
        let reconciling = p
            .conditions
            .iter()
            .find(|c| c.r#type == "Reconciling")
            .unwrap();
        assert!(reconciling
            .message
            .contains("generation 21 is queued behind the Planning pass for generation 20"));
        assert_eq!(p.pass.lag_since(), Some(patched_at));
        let later = base + Duration::minutes(24);
        let mut st2 = status.clone();
        st2.pass = Some(p.pass.beat(base + Duration::minutes(23)).to_status());
        let p2 = project_status(&st2, 21, later, &Deadlines::default());
        assert_eq!(p2.pass.kind(), PassKind::Planning);
        assert_eq!(status_of(&p2.conditions, "Ready"), "False");
    }

    #[test]
    fn default_deadlines_are_five_and_thirty_minutes() {
        let d = Deadlines::default();
        assert_eq!(d.heartbeat, Duration::minutes(5));
        assert_eq!(d.pending_start, Duration::minutes(30));
    }
}
