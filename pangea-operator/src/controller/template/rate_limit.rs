use std::time::Duration;

use chrono::{DateTime, Utc};

pub const RATE_LIMITED_REASON: &str = "RateLimited";

fn lower(message: &str) -> String {
    message.to_ascii_lowercase()
}

fn limited(text: &str) -> bool {
    let status = ["403", "429"].iter().any(|code| {
        text.match_indices(code).any(|(i, _)| {
            let before = text[..i].chars().next_back();
            let after = text[i + code.len()..].chars().next();
            !before.is_some_and(|c| c.is_ascii_digit())
                && !after.is_some_and(|c| c.is_ascii_digit())
        })
    });
    status && (text.contains("rate limit") || text.contains("retry-after"))
}

fn value_after<'a>(
    lowered: &str,
    original: &'a str,
    marker: &str,
    stops: &[char],
) -> Option<&'a str> {
    let at = lowered.find(marker)? + marker.len();
    let rest = original[at..].trim_start_matches([':', '=', ' ']);
    let end = rest.find(stops).unwrap_or(rest.len());
    Some(rest[..end].trim())
}

const STOPS: &[char] = &[',', ';', ']', ')', '\n'];
const DATE_STOPS: &[char] = &[';', ']', ')', '\n'];

fn go_duration(raw: &str) -> Option<Duration> {
    let mut total = 0u64;
    let mut digits = String::new();
    let mut seen = false;
    for c in raw.chars() {
        if c.is_ascii_digit() {
            digits.push(c);
            continue;
        }
        let n: u64 = digits.parse().ok()?;
        digits.clear();
        total += match c {
            'h' => n * 3600,
            'm' => n * 60,
            's' => n,
            _ => return None,
        };
        seen = true;
    }
    (seen && digits.is_empty()).then(|| Duration::from_secs(total))
}

fn at(now: DateTime<Utc>, wait: Duration) -> Option<DateTime<Utc>> {
    chrono::Duration::from_std(wait).ok().map(|d| now + d)
}

#[must_use]
pub fn rate_limited_until(message: &str, now: DateTime<Utc>) -> Option<DateTime<Utc>> {
    let text = lower(message);
    if !limited(&text) {
        return None;
    }
    let advice = value_after(&text, message, "retry-after", STOPS)
        .and_then(handan::parse_retry_after)
        .or_else(|| {
            value_after(&text, message, "retry-after", DATE_STOPS)
                .and_then(handan::parse_retry_after)
        });
    if let Some(advice) = advice {
        return match advice {
            handan::RetryAdvice::After(d) => at(now, d),
            handan::RetryAdvice::At { unix_secs } => {
                DateTime::from_timestamp(i64::try_from(unix_secs).ok()?, 0)
            }
            _ => None,
        };
    }
    if let Some(d) = value_after(&text, message, "rate reset in", STOPS).and_then(go_duration) {
        return at(now, d);
    }
    value_after(&text, message, "x-ratelimit-reset", STOPS)
        .and_then(|v| v.parse::<i64>().ok())
        .and_then(|secs| DateTime::from_timestamp(secs, 0))
}

#[must_use]
pub fn wait_for(until: Option<DateTime<Utc>>, now: DateTime<Utc>) -> Option<Duration> {
    let until = until?;
    (until > now).then(|| (until - now).to_std().unwrap_or(Duration::ZERO))
}

pub async fn hold(
    template: &crate::crd::InfrastructureTemplate,
    state: &crate::controller::ControllerState,
) -> Option<Duration> {
    let until = template
        .status
        .as_ref()
        .and_then(|s| s.rate_limited_until)?;
    if let Some(wait) = wait_for(Some(until), Utc::now()) {
        return Some(wait);
    }
    let patch = serde_json::json!({ "status": { "rateLimitedUntil": null } });
    if let Err(e) =
        crate::controller::status_patch::patch_status(template, &state.client, patch).await
    {
        tracing::warn!(error = %e, "status.rateLimitedUntil could not be cleared");
    }
    None
}

pub async fn record(
    template: &crate::crd::InfrastructureTemplate,
    state: &crate::controller::ControllerState,
    until: DateTime<Utc>,
    message: &str,
) {
    let stamp = until.to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
    let patch = serde_json::json!({ "status": { "rateLimitedUntil": stamp } });
    if let Err(e) =
        crate::controller::status_patch::patch_status(template, &state.client, patch).await
    {
        tracing::warn!(error = %e, "status.rateLimitedUntil could not be written");
    }
    let first = message.lines().next().unwrap_or_default();
    let note = ["until ", &stamp, ": ", first].concat();
    crate::controller::template::events::record_event(
        template,
        state,
        kube::runtime::events::EventType::Warning,
        RATE_LIMITED_REASON,
        &crate::text_util::truncate_utf8_safe(&note, 512, "…"),
    )
    .await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::LeafConfig;
    use crate::controller::scheduling::{BudgetConfig, FairBudget};
    use crate::controller::template::leaf_gate::LeafGate;
    use pangea_leaf::admit::{Candidate, WorkClass};

    fn now() -> DateTime<Utc> {
        DateTime::from_timestamp(1_800_000_000, 0).unwrap()
    }

    fn leaf(name: &str) -> Candidate {
        Candidate {
            name: name.into(),
            scope: "org".into(),
            credential: Some("gh".into()),
            class: WorkClass::Apply,
            depth: 0,
            since: 0,
        }
    }

    #[test]
    fn a_403_with_retry_after_makes_the_leaf_wait_while_its_siblings_apply() {
        let gate = LeafGate::default();
        let budgets = FairBudget::new(BudgetConfig {
            per_scope: 1,
            global: 16,
        });
        let cfg = LeafConfig::default();
        let tick = std::time::Instant::now();
        let permit = gate
            .try_admit(&budgets, leaf("org-a"), None, &cfg, tick)
            .unwrap();
        let error = "Error: GET https://api.github.com/repos/pleme-io/a/actions/permissions: 403 API rate limit exceeded for installation ID 1; retry-after: 60";
        let until =
            rate_limited_until(error, now()).expect("a 403 with a retry hint is a rate limit");
        drop(permit);
        assert_eq!(until, now() + chrono::Duration::seconds(60));
        let later = now() + chrono::Duration::seconds(10);
        assert_eq!(wait_for(Some(until), later), Some(Duration::from_secs(50)));
        let sibling = gate.try_admit(&budgets, leaf("org-b"), None, &cfg, tick);
        assert!(sibling.is_some(), "the released slot goes to a sibling");
        drop(sibling);
        let after = now() + chrono::Duration::seconds(61);
        assert_eq!(wait_for(Some(until), after), None);
        assert!(gate
            .try_admit(&budgets, leaf("org-a"), None, &cfg, tick)
            .is_some());
    }

    #[test]
    fn the_hint_is_read_from_retry_after_a_rate_reset_or_a_reset_epoch() {
        let reset = "403 API rate limit exceeded for user ID 7. [rate reset in 12m30s]";
        assert_eq!(
            rate_limited_until(reset, now()),
            Some(now() + chrono::Duration::seconds(750))
        );
        let epoch = "429 Too Many Requests: rate limit; x-ratelimit-reset: 1800000300";
        assert_eq!(
            rate_limited_until(epoch, now()),
            Some(now() + chrono::Duration::seconds(300))
        );
        let date = "429 rate limit, Retry-After: Fri, 15 Jan 2027 08:00:00 GMT";
        assert!(rate_limited_until(date, now()).is_some());
    }

    #[test]
    fn an_error_without_a_status_or_a_hint_is_not_a_rate_limit() {
        assert_eq!(
            rate_limited_until("403 Resource not accessible by integration", now()),
            None
        );
        assert_eq!(
            rate_limited_until("rate limit, retry-after: 5", now()),
            None
        );
        assert_eq!(
            rate_limited_until("4030 rate limit retry-after: 5", now()),
            None
        );
        assert_eq!(
            rate_limited_until("403 API rate limit exceeded", now()),
            None
        );
    }
}
