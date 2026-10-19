use chrono::{DateTime, Utc};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use strum::{Display, EnumString};

use super::infrastructure_template::Phase;

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, Display, EnumString, Serialize, Deserialize, JsonSchema,
)]
#[serde(rename_all = "camelCase")]
#[strum(serialize_all = "camelCase")]
pub enum ProgressStep {
    Preflight,
    Compile,
    Refresh,
    Plan,
    Apply,
    Record,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct InFlightCall {
    pub address: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rpc: Option<String>,
    pub since: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ResumeRecord {
    pub at: DateTime<Utc>,
    pub phase: Phase,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub previous_phase_started_at: Option<DateTime<Utc>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub previous_pod: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct LiveProgress {
    pub phase: Phase,
    pub phase_started_at: DateTime<Utc>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub step: Option<ProgressStep>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub step_started_at: Option<DateTime<Utc>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resources_total: Option<u64>,
    #[serde(default)]
    pub resources_done: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub in_flight: Option<InFlightCall>,
    pub last_progress_at: DateTime<Utc>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub credential_expires_at: Option<DateTime<Utc>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pod: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub finished_at: Option<DateTime<Utc>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resumed: Option<ResumeRecord>,
}

impl LiveProgress {
    pub fn new(phase: Phase, now: DateTime<Utc>, pod: Option<String>) -> Self {
        Self {
            phase,
            phase_started_at: now,
            step: None,
            step_started_at: None,
            resources_total: None,
            resources_done: 0,
            in_flight: None,
            last_progress_at: now,
            credential_expires_at: None,
            pod,
            finished_at: None,
            resumed: None,
        }
    }
}
