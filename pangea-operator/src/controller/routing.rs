//! Routing-delivery layer for reactive-policy escalations.
//!
//! `ApprovalRouting` declares WHERE escalations should go (ntfy
//! topic, Slack channel, GitHub issue template). This module is the
//! ACTUAL delivery — wraps reqwest, posts to the configured
//! channel(s), maps `ReactiveAction` to per-channel urgency.
//!
//! Today: ntfy is implemented end-to-end. Slack + GitHub are stubbed
//! with TODOs because they need a secret-resolution surface (Slack
//! webhook URL, GitHub App token) which lands in a follow-up.
//!
//! Configuration:
//!   PANGEA_NTFY_BASE_URL — ntfy server base URL. Defaults to
//!                         https://ntfy.sh. Homelab clusters override
//!                         with their internal ntfy URL.
//!
//! Failure modes are non-fatal: a routing delivery that fails (network
//! error, 4xx, 5xx) is logged but doesn't propagate up to the caller.
//! The reactive-policy log line still emits regardless, so observers
//! see the escalation even when the routing channel is down.

use crate::crd::architecture_gem::{ApprovalRouting, ReactiveAction};
use std::time::Duration;
use tracing::{error, info, warn};

/// Wraps the side-effects for routing escalations to ntfy / Slack /
/// GitHub. Held on `ControllerState` and shared across reconcilers.
#[derive(Clone)]
pub struct RoutingClient {
    http: reqwest::Client,
    ntfy_base_url: String,
}

impl RoutingClient {
    /// Build from environment + sensible defaults.
    pub fn from_env() -> Self {
        let ntfy_base_url =
            std::env::var("PANGEA_NTFY_BASE_URL").unwrap_or_else(|_| "https://ntfy.sh".to_string());
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .expect("reqwest client builds");
        Self {
            http,
            ntfy_base_url,
        }
    }

    /// Build from an explicit ntfy base URL — used by tests + advanced
    /// callers that want to point at a homelab ntfy server without
    /// touching env.
    pub fn with_ntfy_base_url(ntfy_base_url: String) -> Self {
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .expect("reqwest client builds");
        Self {
            http,
            ntfy_base_url,
        }
    }

    /// Deliver an escalation through every configured channel in
    /// `routing`. Every channel is best-effort — failures log but
    /// don't propagate.
    pub async fn deliver(
        &self,
        action: ReactiveAction,
        title: &str,
        body: &str,
        routing: Option<&ApprovalRouting>,
    ) {
        let routing = match routing {
            Some(r) => r,
            None => return,
        };

        if let Some(topic) = &routing.ntfy_topic {
            self.deliver_ntfy(action, topic, title, body).await;
        }
        if routing.slack_channel.is_some() {
            // TODO(routing): Slack incoming-webhook delivery.
            // Needs (a) a webhook-URL Secret reference on
            // ApprovalRouting (or operator-global env var), and (b)
            // a samba-style rate-limited consumer wrapper since
            // Slack throttles per-workspace. For now, the slack
            // intent is logged via emit_escalation_log; this stub
            // keeps the typed surface honest.
            warn!(
                channel = %routing.slack_channel.as_deref().unwrap_or(""),
                "Slack routing requested but delivery is not yet implemented (slack_channel ignored)"
            );
        }
        if routing.github_issue_template.is_some() {
            // TODO(routing): GitHub-issue creation via gh app token.
            // Needs (a) repo + token resolution from operator config,
            // (b) issue-template hydration with the escalation
            // payload. Same samba-style rate-limit wrapper applies.
            warn!(
                template = %routing.github_issue_template.as_deref().unwrap_or(""),
                "GitHub-issue routing requested but delivery is not yet implemented"
            );
        }
    }

    async fn deliver_ntfy(&self, action: ReactiveAction, topic: &str, title: &str, body: &str) {
        let url = format!("{}/{}", self.ntfy_base_url.trim_end_matches('/'), topic);
        let priority = ntfy_priority(action);
        let tags = ntfy_tags(action);
        let result = self
            .http
            .post(&url)
            .header("Title", title)
            .header("Priority", priority)
            .header("Tags", tags)
            .body(body.to_string())
            .send()
            .await;
        match result {
            Ok(resp) if resp.status().is_success() => {
                info!(
                    target: "pangea_operator::routing::ntfy",
                    topic = %topic,
                    priority = %priority,
                    status = %resp.status(),
                    "ntfy delivered"
                );
            }
            Ok(resp) => {
                let status = resp.status();
                let body_excerpt = resp
                    .text()
                    .await
                    .unwrap_or_default()
                    .chars()
                    .take(160)
                    .collect::<String>();
                warn!(
                    target: "pangea_operator::routing::ntfy",
                    topic = %topic,
                    %status,
                    body = %body_excerpt,
                    "ntfy non-2xx response"
                );
            }
            Err(e) => {
                error!(
                    target: "pangea_operator::routing::ntfy",
                    topic = %topic,
                    error = %e,
                    "ntfy delivery failed"
                );
            }
        }
    }
}

/// Map a ReactiveAction to an ntfy `Priority:` header value. Five
/// ntfy priorities exist (min/low/default/high/urgent); we use three.
pub fn ntfy_priority(action: ReactiveAction) -> &'static str {
    match action {
        ReactiveAction::Alert => "default",
        ReactiveAction::Suspend => "high",
        ReactiveAction::Page => "urgent",
    }
}

/// Map a ReactiveAction to an ntfy `Tags:` header. Tags become
/// emoji + category labels in the ntfy UI / mobile push.
pub fn ntfy_tags(action: ReactiveAction) -> &'static str {
    match action {
        ReactiveAction::Alert => "warning",
        ReactiveAction::Suspend => "no_entry,warning",
        ReactiveAction::Page => "rotating_light,red_circle",
    }
}

/// Build the ntfy URL for a topic. Pure helper so tests can verify
/// trailing-slash handling without spinning up a server.
pub fn ntfy_url(base: &str, topic: &str) -> String {
    format!("{}/{}", base.trim_end_matches('/'), topic)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ntfy_priority_maps_each_action() {
        assert_eq!(ntfy_priority(ReactiveAction::Alert), "default");
        assert_eq!(ntfy_priority(ReactiveAction::Suspend), "high");
        assert_eq!(ntfy_priority(ReactiveAction::Page), "urgent");
    }

    #[test]
    fn ntfy_tags_include_severity_emoji() {
        // Page should escalate to the most attention-grabbing tag set.
        let page_tags = ntfy_tags(ReactiveAction::Page);
        assert!(page_tags.contains("rotating_light"));
        assert!(page_tags.contains("red_circle"));
        // Alert is the lightest.
        assert_eq!(ntfy_tags(ReactiveAction::Alert), "warning");
    }

    #[test]
    fn ntfy_url_strips_trailing_slash_on_base() {
        assert_eq!(
            ntfy_url("https://ntfy.sh/", "rio-critical"),
            "https://ntfy.sh/rio-critical"
        );
        assert_eq!(
            ntfy_url("https://ntfy.sh", "rio-critical"),
            "https://ntfy.sh/rio-critical"
        );
    }

    #[test]
    fn ntfy_url_handles_homelab_path() {
        // Internal ntfy URLs sometimes carry a path prefix.
        assert_eq!(
            ntfy_url("https://ntfy.bristol.quero.cloud/", "rio-critical"),
            "https://ntfy.bristol.quero.cloud/rio-critical"
        );
    }

    #[test]
    fn from_env_uses_default_when_unset() {
        // Defensive — we don't want a misconfigured env var to make
        // the operator fail to start. from_env always succeeds.
        std::env::remove_var("PANGEA_NTFY_BASE_URL");
        let client = RoutingClient::from_env();
        assert_eq!(client.ntfy_base_url, "https://ntfy.sh");
    }

    #[tokio::test]
    async fn deliver_with_no_routing_is_noop() {
        let client = RoutingClient::with_ntfy_base_url("http://127.0.0.1:1".into());
        // Should not panic, should not attempt network — None routing
        // skips every channel.
        client
            .deliver(ReactiveAction::Page, "title", "body", None)
            .await;
    }

    #[tokio::test]
    async fn deliver_with_only_slack_logs_warning_and_returns() {
        // Slack is stubbed; the call should complete without panic
        // and without attempting network. The TODO branch warns.
        let client = RoutingClient::with_ntfy_base_url("http://127.0.0.1:1".into());
        let routing = ApprovalRouting {
            ntfy_topic: None,
            slack_channel: Some("#oncall".into()),
            github_issue_template: None,
        };
        client
            .deliver(ReactiveAction::Alert, "t", "b", Some(&routing))
            .await;
    }
}
