use chrono::{DateTime, Utc};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use strum::{Display, EnumString};

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, Display, EnumString, Serialize, Deserialize, JsonSchema,
)]
#[serde(rename_all = "camelCase")]
#[strum(serialize_all = "camelCase")]
pub enum PassKind {
    Settled,
    Pending,
    Planning,
    Applying,
    Failed,
    Stalled,
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, Display, EnumString, Serialize, Deserialize, JsonSchema,
)]
#[serde(rename_all = "camelCase")]
#[strum(serialize_all = "camelCase")]
pub enum StallState {
    QueuedBehind,
    PendingNotStarted,
    HeartbeatLost,
    OrphanedPhase,
    NoProgress,
    Unnamed,
}

impl StallState {
    pub const ALL: [StallState; 6] = [
        StallState::QueuedBehind,
        StallState::PendingNotStarted,
        StallState::HeartbeatLost,
        StallState::OrphanedPhase,
        StallState::NoProgress,
        StallState::Unnamed,
    ];

    pub fn remedy(self) -> &'static str {
        match self {
            StallState::QueuedBehind => "wait and report the blocking pass",
            StallState::PendingNotStarted => "re-dispatch the reconcile",
            StallState::HeartbeatLost => "re-dispatch the reconcile",
            StallState::OrphanedPhase => "resume the pass under this incarnation",
            StallState::NoProgress => "re-dispatch the reconcile",
            StallState::Unnamed => "report an unnamed state so it can be named",
        }
    }

    pub fn redispatches(self) -> bool {
        matches!(
            self,
            StallState::PendingNotStarted
                | StallState::HeartbeatLost
                | StallState::OrphanedPhase
                | StallState::NoProgress
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct PassStatus {
    pub kind: PassKind,
    pub generation: i64,
    #[serde(default)]
    pub since: Option<DateTime<Utc>>,
    #[serde(default)]
    pub heartbeat_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub reason: Option<String>,
    #[serde(default)]
    pub queued_generation: Option<i64>,
    #[serde(default)]
    pub queued_since: Option<DateTime<Utc>>,
    #[serde(default)]
    pub stall: Option<StallState>,
    #[serde(default)]
    pub incarnation: Option<String>,
}
