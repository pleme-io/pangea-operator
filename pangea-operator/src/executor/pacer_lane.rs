use std::future::Future;

use pangea_leaf::admit::Share;
use pangea_leaf::pace::Lane;

tokio::task_local! {
    static LANE: (Lane, Share);
}

#[must_use]
pub fn current() -> Option<(Lane, Share)> {
    LANE.try_with(|l| *l).ok()
}

pub async fn within<F: Future>(lane: Option<(Lane, Share)>, f: F) -> F::Output {
    match lane {
        Some(l) => LANE.scope(l, f).await,
        None => f.await,
    }
}

#[must_use]
pub fn quota_pct() -> f64 {
    std::env::var("PANGEA_PACER_QUOTA_PCT")
        .ok()
        .and_then(|v| v.parse::<f64>().ok())
        .filter(|v| *v > 0.0 && *v <= 1.0)
        .unwrap_or(0.7)
}

#[must_use]
pub fn github_budget_rph() -> f64 {
    std::env::var("PANGEA_GITHUB_BUDGET_RPH")
        .ok()
        .and_then(|v| v.parse::<f64>().ok())
        .filter(|v| *v > 0.0)
        .unwrap_or(5000.0)
}

#[must_use]
pub fn github_rate_per_sec() -> f64 {
    github_budget_rph() * quota_pct() / 3600.0
}

#[must_use]
pub fn pacer_key(key: &str, quota: f64) -> (String, f64) {
    match current() {
        Some((lane, share)) => pangea_leaf::pace::lane_pacer(key, quota, share, Some(lane)),
        None => (key.to_string(), quota),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn a_drift_plan_draws_from_the_drift_pacer_and_a_monolith_from_the_one_pacer() {
        assert_eq!(pacer_key("k", 0.9), ("k".to_string(), 0.9));
        let (key, quota) = within(Some((Lane::Drift, Share::HALF)), async {
            pacer_key("k", 0.9)
        })
        .await;
        assert_eq!(key, "k:drift");
        assert!((quota - 0.45).abs() < 1e-9);
        let (key, _) = within(Some((Lane::Change, Share::HALF)), async {
            pacer_key("k", 0.9)
        })
        .await;
        assert_eq!(key, "k:change");
        let (key, _) = within(None, async { pacer_key("k", 0.9) }).await;
        assert_eq!(key, "k");
    }
}
