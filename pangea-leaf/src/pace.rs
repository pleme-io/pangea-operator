use crate::admit::Share;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Lane {
    Change,
    Drift,
}

impl Lane {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Change => "change",
            Self::Drift => "drift",
        }
    }
}

#[must_use]
pub fn lane_pacer(key: &str, quota: f64, share: Share, lane: Option<Lane>) -> (String, f64) {
    let Some(lane) = lane.filter(|_| share.permille() > 0) else {
        return (key.to_string(), quota);
    };
    let fraction = match lane {
        Lane::Change => 1.0 - share.fraction(),
        Lane::Drift => share.fraction(),
    };
    ([key, lane.as_str()].join(":"), quota * fraction)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_change_and_drift_lanes_sum_to_the_credentials_quota() {
        for permille in [100u16, 250, 500, 900, 999] {
            let share = Share::from_fraction(f64::from(permille) / 1000.0).unwrap();
            let (ck, cq) = lane_pacer("k", 0.9, share, Some(Lane::Change));
            let (dk, dq) = lane_pacer("k", 0.9, share, Some(Lane::Drift));
            assert_eq!(ck, "k:change");
            assert_eq!(dk, "k:drift");
            assert!((cq + dq - 0.9).abs() < 1e-9, "{permille}: {cq} + {dq}");
            assert!(cq > 0.0 && dq > 0.0);
        }
    }

    #[test]
    fn no_lane_or_a_zero_share_keeps_the_one_pacer() {
        assert_eq!(
            lane_pacer("k", 0.7, Share::HALF, None),
            ("k".to_string(), 0.7)
        );
        assert_eq!(
            lane_pacer("k", 0.7, Share::NONE, Some(Lane::Drift)),
            ("k".to_string(), 0.7)
        );
    }
}
