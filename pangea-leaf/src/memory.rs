use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

const MIB: f64 = 1_048_576.0;

pub const FAT_PLAN_PEAK_BYTES: u64 = (55.6 * MIB) as u64;
pub const BASE_RSS_BYTES: u64 = (23.6 * MIB) as u64;
pub const GITHUB_PROVIDER_RSS_BYTES: u64 = (33.0 * MIB) as u64;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum WorkerMemory {
    #[default]
    Derived,
    Fixed(u64),
}

#[must_use]
pub fn parse_quantity(raw: &str) -> Option<u64> {
    let raw = raw.trim();
    let split = raw
        .find(|c: char| !(c.is_ascii_digit() || c == '.'))
        .unwrap_or(raw.len());
    let (num, unit) = raw.split_at(split);
    let n: f64 = num.parse().ok()?;
    let scale: f64 = match unit {
        "" => 1.0,
        "k" => 1e3,
        "M" => 1e6,
        "G" => 1e9,
        "Ki" => 1024.0,
        "Mi" => MIB,
        "Gi" => MIB * 1024.0,
        _ => return None,
    };
    let bytes = n * scale;
    (bytes.is_finite() && bytes > 0.0).then(|| bytes.round() as u64)
}

impl WorkerMemory {
    #[must_use]
    pub fn parse(raw: &str) -> Option<Self> {
        if raw.trim().eq_ignore_ascii_case("derived") {
            return Some(Self::Derived);
        }
        parse_quantity(raw).map(Self::Fixed)
    }
}

impl Serialize for WorkerMemory {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        match self {
            Self::Derived => s.serialize_str("derived"),
            Self::Fixed(b) => s.serialize_str(&b.to_string()),
        }
    }
}

impl<'de> Deserialize<'de> for WorkerMemory {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let raw = String::deserialize(d)?;
        Self::parse(&raw).ok_or_else(|| {
            serde::de::Error::custom("workers.memory is `derived` or a quantity such as 512Mi")
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MemoryInputs {
    pub base_rss: u64,
    pub cap_worker: u64,
    pub max_leaf_peak: Option<u64>,
    pub providers: u64,
    pub provider_rss: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct WorkerMemoryStatus {
    pub request_bytes: u64,
    pub limit_bytes: u64,
    pub source: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_leaf_peak_bytes: Option<u64>,
    pub calibrated: bool,
}

#[must_use]
pub fn max_peak(peaks: impl IntoIterator<Item = Option<u64>>) -> Option<u64> {
    peaks.into_iter().flatten().max()
}

fn limit_of(request: u64) -> u64 {
    request.saturating_add(request / 2)
}

#[must_use]
pub fn derive(setting: WorkerMemory, inputs: &MemoryInputs) -> WorkerMemoryStatus {
    match setting {
        WorkerMemory::Fixed(q) => WorkerMemoryStatus {
            request_bytes: q,
            limit_bytes: limit_of(q),
            source: "fixed".into(),
            max_leaf_peak_bytes: inputs.max_leaf_peak,
            calibrated: inputs.max_leaf_peak.is_some(),
        },
        WorkerMemory::Derived => {
            let peak = inputs.max_leaf_peak.unwrap_or(FAT_PLAN_PEAK_BYTES);
            let request = inputs
                .base_rss
                .saturating_add(inputs.cap_worker.saturating_mul(peak))
                .saturating_add(inputs.providers.saturating_mul(inputs.provider_rss));
            WorkerMemoryStatus {
                request_bytes: request,
                limit_bytes: limit_of(request),
                source: "derived".into(),
                max_leaf_peak_bytes: Some(peak),
                calibrated: inputs.max_leaf_peak.is_some(),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MI: u64 = 1_048_576;

    fn inputs(peaks: &[Option<u64>]) -> MemoryInputs {
        MemoryInputs {
            base_rss: BASE_RSS_BYTES,
            cap_worker: 8,
            max_leaf_peak: max_peak(peaks.iter().copied()),
            providers: 1,
            provider_rss: GITHUB_PROVIDER_RSS_BYTES,
        }
    }

    #[test]
    fn the_derived_request_equals_the_formula_on_fixture_leaves() {
        let i = inputs(&[Some(10 * MI), Some(40 * MI), None, Some(25 * MI)]);
        let s = derive(WorkerMemory::Derived, &i);
        let request = BASE_RSS_BYTES + 8 * 40 * MI + GITHUB_PROVIDER_RSS_BYTES;
        assert_eq!(s.request_bytes, request);
        assert_eq!(s.limit_bytes, request + request / 2);
        assert!(s.calibrated);
        assert_eq!(s.max_leaf_peak_bytes, Some(40 * MI));
    }

    #[test]
    fn before_calibration_the_fat_plan_peak_bounds_one_leaf() {
        let s = derive(WorkerMemory::Derived, &inputs(&[None, None]));
        assert!(!s.calibrated);
        assert_eq!(
            s.request_bytes,
            BASE_RSS_BYTES + 8 * FAT_PLAN_PEAK_BYTES + GITHUB_PROVIDER_RSS_BYTES
        );
    }

    #[test]
    fn a_fixed_quantity_is_the_request() {
        let s = derive(WorkerMemory::Fixed(512 * MI), &inputs(&[Some(MI)]));
        assert_eq!(s.request_bytes, 512 * MI);
        assert_eq!(s.limit_bytes, 768 * MI);
        assert_eq!(s.source, "fixed");
    }

    #[test]
    fn the_setting_reads_derived_or_a_quantity_and_refuses_the_rest() {
        assert_eq!(WorkerMemory::parse("derived"), Some(WorkerMemory::Derived));
        assert_eq!(
            WorkerMemory::parse("512Mi"),
            Some(WorkerMemory::Fixed(512 * MI))
        );
        assert_eq!(
            WorkerMemory::parse("1G"),
            Some(WorkerMemory::Fixed(1_000_000_000))
        );
        assert_eq!(WorkerMemory::parse("lots"), None);
        assert_eq!(WorkerMemory::parse("0"), None);
        assert!(serde_json::from_str::<WorkerMemory>("\"12Xi\"").is_err());
    }
}
