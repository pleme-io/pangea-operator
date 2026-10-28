use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

pub const DEFAULT_SERIES_MAX: usize = 256;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SeriesMode {
    #[default]
    Unhealthy,
    All,
    Off,
}

impl SeriesMode {
    #[must_use]
    pub fn parse(raw: &str) -> Option<Self> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "unhealthy" => Some(Self::Unhealthy),
            "all" => Some(Self::All),
            "off" => Some(Self::Off),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SeriesRow {
    pub name: String,
    pub workspace: String,
    pub healthy: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Selection {
    pub labelled: BTreeSet<String>,
    pub unlabeled: BTreeMap<String, u32>,
}

#[must_use]
pub fn select(rows: &[SeriesRow], mode: SeriesMode, max: usize) -> Selection {
    let mut sorted: Vec<&SeriesRow> = rows.iter().collect();
    sorted.sort_by(|a, b| (&a.workspace, &a.name).cmp(&(&b.workspace, &b.name)));
    let mut out = Selection::default();
    for r in sorted {
        out.unlabeled.entry(r.workspace.clone()).or_insert(0);
        let wanted = match mode {
            SeriesMode::All => true,
            SeriesMode::Unhealthy => !r.healthy,
            SeriesMode::Off => false,
        };
        let capped = mode == SeriesMode::Unhealthy && out.labelled.len() >= max;
        if wanted && !capped {
            out.labelled.insert(r.name.clone());
        } else if !r.healthy {
            *out.unlabeled.entry(r.workspace.clone()).or_insert(0) += 1;
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rows(n: usize, healthy: bool) -> Vec<SeriesRow> {
        (0..n)
            .map(|i| SeriesRow {
                name: format!("org-repo-{i:04}"),
                workspace: "org".into(),
                healthy,
            })
            .collect()
    }

    #[test]
    fn three_hundred_unhealthy_leaves_emit_256_labelled_and_44_unlabeled() {
        let s = select(&rows(300, false), SeriesMode::Unhealthy, DEFAULT_SERIES_MAX);
        assert_eq!(s.labelled.len(), 256);
        assert_eq!(s.unlabeled.get("org"), Some(&44));
    }

    #[test]
    fn healthy_leaves_are_aggregated_only_by_default() {
        let mut v = rows(10, true);
        v.extend(rows(3, false).into_iter().map(|mut r| {
            r.name.push_str("-bad");
            r
        }));
        let s = select(&v, SeriesMode::Unhealthy, DEFAULT_SERIES_MAX);
        assert_eq!(s.labelled.len(), 3);
        assert_eq!(s.unlabeled.get("org"), Some(&0));
    }

    #[test]
    fn all_labels_every_leaf_and_off_labels_none() {
        let v = rows(300, false);
        assert_eq!(
            select(&v, SeriesMode::All, DEFAULT_SERIES_MAX)
                .labelled
                .len(),
            300
        );
        let off = select(&v, SeriesMode::Off, DEFAULT_SERIES_MAX);
        assert!(off.labelled.is_empty());
        assert_eq!(off.unlabeled.get("org"), Some(&300));
    }

    #[test]
    fn the_mode_parses_its_three_words() {
        assert_eq!(SeriesMode::parse("ALL"), Some(SeriesMode::All));
        assert_eq!(SeriesMode::parse("off"), Some(SeriesMode::Off));
        assert_eq!(SeriesMode::parse("unhealthy"), Some(SeriesMode::Unhealthy));
        assert_eq!(SeriesMode::parse("some"), None);
    }
}
