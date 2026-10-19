use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use futures::future::BoxFuture;
use reqwest::header::HeaderMap;
use todoku::{retry_with_verdict, Jitter, RetryError, RetryPolicy, RetryVerdict};

pub const GITHUB_API: &str = "https://api.github.com";
pub const USER_AGENT: &str = "pangea-operator-org-resolve";
pub const PER_PAGE: u32 = 100;
pub const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
pub const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
pub const MAX_ADVISED_WAIT: Duration = Duration::from_secs(120);

pub type Sleeper = Arc<dyn Fn(Duration) -> BoxFuture<'static, ()> + Send + Sync>;

#[must_use]
pub fn default_policy() -> RetryPolicy {
    RetryPolicy {
        max_retries: 5,
        initial_backoff: Duration::from_millis(500),
        max_backoff: Duration::from_secs(30),
        multiplier: 2.0,
        retry_statuses: vec![429, 500, 502, 503, 504],
    }
}

#[must_use]
pub fn tokio_sleeper() -> Sleeper {
    Arc::new(|d| Box::pin(tokio::time::sleep(d)))
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum GetFailure {
    #[error("transport error: {0}")]
    Transport(String),
    #[error("HTTP {status} from GitHub{}", advised(*wait))]
    Server { status: u16, wait: Option<Duration> },
    #[error("rate limited (HTTP {status}){}{}", advised(*wait), reset_text(*reset_unix))]
    RateLimited {
        status: u16,
        wait: Option<Duration>,
        reset_unix: Option<u64>,
    },
    #[error(
        "rate limited (HTTP {status}){}: the advised wait of {}s exceeds the {}s bound",
        reset_text(*reset_unix),
        wait.as_secs(),
        bound.as_secs()
    )]
    RateLimitBeyondBound {
        status: u16,
        wait: Duration,
        bound: Duration,
        reset_unix: Option<u64>,
    },
    #[error("HTTP 401: GitHub rejected the token")]
    Unauthorized,
    #[error("HTTP 403 that is not a rate limit: the token lacks access")]
    Forbidden,
    #[error(
        "HTTP {status}: refusing to treat this as 'absent', which would plan a CREATE \
         against a repo that may exist"
    )]
    Unexpected { status: u16 },
    #[error("decoding body: {0}")]
    Decode(String),
    #[error("refusing to follow pagination to {0}, outside the API base")]
    ForeignPage(String),
}

fn advised(wait: Option<Duration>) -> String {
    wait.map(|w| format!(", server advised waiting {}s", w.as_secs()))
        .unwrap_or_default()
}

fn reset_text(reset_unix: Option<u64>) -> String {
    reset_unix
        .and_then(|s| i64::try_from(s).ok())
        .and_then(|s| chrono::DateTime::<chrono::Utc>::from_timestamp(s, 0))
        .map(|t| format!(", resets at {}", t.to_rfc3339()))
        .unwrap_or_default()
}

impl GetFailure {
    #[must_use]
    pub fn verdict(&self) -> RetryVerdict {
        match self {
            Self::Transport(_) => RetryVerdict::Backoff,
            Self::Server { wait, .. } | Self::RateLimited { wait, .. } => {
                wait.map_or(RetryVerdict::Backoff, RetryVerdict::After)
            }
            Self::RateLimitBeyondBound { .. }
            | Self::Unauthorized
            | Self::Forbidden
            | Self::Unexpected { .. }
            | Self::Decode(_)
            | Self::ForeignPage(_) => RetryVerdict::Stop,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("GET {url}: {cause} ({attempts} attempt(s), {})", if *exhausted { "retries exhausted" } else { "not retryable" })]
pub struct GetError {
    pub url: String,
    pub attempts: u32,
    pub exhausted: bool,
    pub cause: GetFailure,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Page {
    pub body: serde_json::Value,
    pub next: Option<String>,
}

#[derive(Clone)]
pub struct GitHubReader {
    client: reqwest::Client,
    base: String,
    token: Option<String>,
    policy: RetryPolicy,
    jitter: Jitter,
    max_advised_wait: Duration,
    sleep: Sleeper,
    requests: Arc<AtomicU64>,
}

impl std::fmt::Debug for GitHubReader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GitHubReader")
            .field("base", &self.base)
            .field("authenticated", &self.token.is_some())
            .field("policy", &self.policy)
            .field("jitter", &self.jitter)
            .field("max_advised_wait", &self.max_advised_wait)
            .finish_non_exhaustive()
    }
}

#[must_use]
pub fn visibility_of(repo: &serde_json::Value) -> String {
    repo.get("visibility")
        .and_then(serde_json::Value::as_str)
        .map(str::to_string)
        .unwrap_or_else(|| {
            if repo.get("private").and_then(serde_json::Value::as_bool) == Some(false) {
                "public".to_string()
            } else {
                "private".to_string()
            }
        })
}

#[must_use]
pub fn next_link(headers: &HeaderMap) -> Option<String> {
    headers
        .get_all(reqwest::header::LINK)
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|v| v.split(','))
        .find_map(|part| {
            let mut pieces = part.split(';');
            let target = pieces.next()?.trim();
            let is_next = pieces.any(|p| {
                let p = p.trim();
                p == "rel=\"next\"" || p == "rel=next"
            });
            is_next.then(|| {
                target
                    .trim_start_matches('<')
                    .trim_end_matches('>')
                    .to_string()
            })
        })
}

fn header_str<'a>(headers: &'a HeaderMap, name: &str) -> Option<&'a str> {
    headers
        .get(name)
        .and_then(|v| v.to_str().ok())
        .map(str::trim)
}

fn now_unix() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs())
}

fn transport(e: &reqwest::Error) -> GetFailure {
    let mut text = e.to_string();
    let mut source = std::error::Error::source(e);
    while let Some(s) = source {
        text.push_str(": ");
        text.push_str(&s.to_string());
        source = s.source();
    }
    GetFailure::Transport(text)
}

impl GitHubReader {
    pub fn new(token: Option<&str>) -> Result<Self, String> {
        Self::with_timeouts(token, CONNECT_TIMEOUT, REQUEST_TIMEOUT)
    }

    pub fn with_timeouts(
        token: Option<&str>,
        connect: Duration,
        request: Duration,
    ) -> Result<Self, String> {
        let client = reqwest::Client::builder()
            .connect_timeout(connect)
            .timeout(request)
            .build()
            .map_err(|e| format!("building the GitHub HTTP client: {e}"))?;
        Ok(Self::from_client(client, token))
    }

    #[must_use]
    pub fn from_client(client: reqwest::Client, token: Option<&str>) -> Self {
        Self {
            client,
            base: GITHUB_API.to_string(),
            token: token.map(str::to_string),
            policy: default_policy(),
            jitter: Jitter::Full,
            max_advised_wait: MAX_ADVISED_WAIT,
            sleep: tokio_sleeper(),
            requests: Arc::new(AtomicU64::new(0)),
        }
    }

    #[must_use]
    pub fn base(mut self, base: &str) -> Self {
        self.base = base.trim_end_matches('/').to_string();
        self
    }

    #[must_use]
    pub fn retry(mut self, policy: RetryPolicy, jitter: Jitter) -> Self {
        self.policy = policy;
        self.jitter = jitter;
        self
    }

    #[must_use]
    pub fn max_advised_wait(mut self, bound: Duration) -> Self {
        self.max_advised_wait = bound;
        self
    }

    #[must_use]
    pub fn sleeper(mut self, sleep: Sleeper) -> Self {
        self.sleep = sleep;
        self
    }

    #[must_use]
    pub fn requests_sent(&self) -> u64 {
        self.requests.load(Ordering::Relaxed)
    }

    pub async fn get(&self, url: &str) -> Result<Option<Page>, GetError> {
        let attempts = AtomicU32::new(0);
        let sleep = self.sleep.clone();
        let result = retry_with_verdict(
            &self.policy,
            self.jitter,
            || {
                attempts.fetch_add(1, Ordering::Relaxed);
                self.attempt(url)
            },
            GetFailure::verdict,
            |d| sleep(d),
        )
        .await;
        result.map_err(|e| {
            let exhausted = matches!(e, RetryError::Exhausted { .. });
            GetError {
                url: url.to_string(),
                attempts: attempts.load(Ordering::Relaxed),
                exhausted,
                cause: e.into_inner(),
            }
        })
    }

    async fn attempt(&self, url: &str) -> Result<Option<Page>, GetFailure> {
        let mut req = self
            .client
            .get(url)
            .header("User-Agent", USER_AGENT)
            .header("Accept", "application/vnd.github+json");
        if let Some(t) = &self.token {
            req = req.bearer_auth(t);
        }
        self.requests.fetch_add(1, Ordering::Relaxed);
        let resp = req.send().await.map_err(|e| transport(&e))?;
        let status = resp.status().as_u16();
        let headers = resp.headers().clone();
        match status {
            200..=299 => {
                let bytes = resp.bytes().await.map_err(|e| transport(&e))?;
                let body = serde_json::from_slice(&bytes)
                    .map_err(|e| GetFailure::Decode(e.to_string()))?;
                Ok(Some(Page {
                    body,
                    next: next_link(&headers),
                }))
            }
            404 => Ok(None),
            401 => Err(GetFailure::Unauthorized),
            _ => {
                let body = resp.text().await.unwrap_or_default();
                Err(self.refusal(status, &headers, &body))
            }
        }
    }

    fn refusal(&self, status: u16, headers: &HeaderMap, body: &str) -> GetFailure {
        let retry_after = header_str(headers, "retry-after");
        let exhausted_quota = header_str(headers, "x-ratelimit-remaining") == Some("0");
        let secondary = body.to_ascii_lowercase().contains("secondary rate limit");
        let signalled = retry_after.is_some() || exhausted_quota || secondary;
        let now = now_unix();
        let reset_header =
            header_str(headers, "x-ratelimit-reset").and_then(|v| v.parse::<u64>().ok());
        let advice = retry_after.and_then(handan::parse_retry_after).or_else(|| {
            exhausted_quota
                .then_some(reset_header)
                .flatten()
                .map(|unix_secs| handan::RetryAdvice::At { unix_secs })
        });
        let wait = advice.map(|a| a.delay_from(now));
        let reset_unix = match advice {
            Some(handan::RetryAdvice::At { unix_secs }) => Some(unix_secs),
            Some(a) => Some(now + a.delay_from(now).as_secs()),
            None => reset_header,
        };
        match handan::judge_signalled(status, signalled).verdict() {
            handan::Verdict::Throttled => match wait {
                Some(w) if w > self.max_advised_wait => GetFailure::RateLimitBeyondBound {
                    status,
                    wait: w,
                    bound: self.max_advised_wait,
                    reset_unix,
                },
                _ => GetFailure::RateLimited {
                    status,
                    wait,
                    reset_unix,
                },
            },
            handan::Verdict::ServerError => match wait {
                Some(w) if w > self.max_advised_wait => GetFailure::RateLimitBeyondBound {
                    status,
                    wait: w,
                    bound: self.max_advised_wait,
                    reset_unix,
                },
                _ => GetFailure::Server { status, wait },
            },
            handan::Verdict::Unauthorized if status == 403 => GetFailure::Forbidden,
            handan::Verdict::Unauthorized => GetFailure::Unauthorized,
            _ => GetFailure::Unexpected { status },
        }
    }

    pub async fn repo_visibility(
        &self,
        owner: &str,
        repo: &str,
    ) -> Result<Option<String>, GetError> {
        let url = format!("{}/repos/{owner}/{repo}", self.base);
        Ok(self.get(&url).await?.map(|p| visibility_of(&p.body)))
    }

    pub async fn org_visibilities(
        &self,
        owner: &str,
    ) -> Result<Option<BTreeMap<String, String>>, GetError> {
        let mut url = format!(
            "{}/orgs/{owner}/repos?type=all&per_page={PER_PAGE}",
            self.base
        );
        let mut out = BTreeMap::new();
        let mut first = true;
        loop {
            let refuse = |cause| GetError {
                url: url.clone(),
                attempts: 1,
                exhausted: false,
                cause,
            };
            let Some(page) = self.get(&url).await? else {
                if first {
                    return Ok(None);
                }
                return Err(refuse(GetFailure::Unexpected { status: 404 }));
            };
            let repos = page.body.as_array().ok_or_else(|| {
                refuse(GetFailure::Decode("the listing is not a JSON array".into()))
            })?;
            for repo in repos {
                let name = repo
                    .get("name")
                    .and_then(serde_json::Value::as_str)
                    .ok_or_else(|| {
                        refuse(GetFailure::Decode("a listed repo has no name".into()))
                    })?;
                out.insert(name.to_string(), visibility_of(repo));
            }
            match page.next {
                None => return Ok(Some(out)),
                Some(next) if next.starts_with(&format!("{}/", self.base)) => {
                    url = next;
                    first = false;
                }
                Some(next) => return Err(refuse(GetFailure::ForeignPage(next))),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use wiremock::matchers::{header, method, path, query_param, query_param_is_missing};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn fast() -> RetryPolicy {
        RetryPolicy {
            max_retries: 5,
            initial_backoff: Duration::from_millis(1),
            max_backoff: Duration::from_millis(4),
            multiplier: 2.0,
            retry_statuses: vec![],
        }
    }

    fn recording() -> (Arc<Mutex<Vec<Duration>>>, Sleeper) {
        let slept = Arc::new(Mutex::new(Vec::new()));
        let sink = slept.clone();
        let sleeper: Sleeper = Arc::new(move |d| {
            sink.lock().unwrap().push(d);
            Box::pin(async {})
        });
        (slept, sleeper)
    }

    fn reader(base: &str, sleeper: Sleeper) -> GitHubReader {
        GitHubReader::with_timeouts(
            Some("t0ken"),
            Duration::from_secs(2),
            Duration::from_millis(500),
        )
        .unwrap()
        .base(base)
        .retry(fast(), Jitter::Full)
        .sleeper(sleeper)
    }

    fn repo_json(name: &str, visibility: &str) -> serde_json::Value {
        serde_json::json!({ "name": name, "visibility": visibility, "private": visibility != "public" })
    }

    #[tokio::test]
    async fn a_transient_502_then_200_succeeds() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/repos/o/r"))
            .respond_with(ResponseTemplate::new(502))
            .up_to_n_times(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/repos/o/r"))
            .and(header("authorization", "Bearer t0ken"))
            .and(header("user-agent", USER_AGENT))
            .respond_with(ResponseTemplate::new(200).set_body_json(repo_json("r", "internal")))
            .mount(&server)
            .await;
        let (slept, sleeper) = recording();
        let r = reader(&server.uri(), sleeper);
        assert_eq!(
            r.repo_visibility("o", "r").await.unwrap().as_deref(),
            Some("internal")
        );
        assert_eq!(r.requests_sent(), 2);
        assert_eq!(slept.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn a_request_timeout_then_success_is_retried() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/repos/o/r"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(repo_json("r", "public"))
                    .set_delay(Duration::from_secs(3)),
            )
            .up_to_n_times(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/repos/o/r"))
            .respond_with(ResponseTemplate::new(200).set_body_json(repo_json("r", "public")))
            .mount(&server)
            .await;
        let (_, sleeper) = recording();
        let r = reader(&server.uri(), sleeper);
        assert_eq!(
            r.repo_visibility("o", "r").await.unwrap().as_deref(),
            Some("public")
        );
        assert_eq!(r.requests_sent(), 2);
    }

    #[tokio::test]
    async fn a_dropped_connection_then_success_is_retried() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (dropped, _) = listener.accept().await.unwrap();
            drop(dropped);
            let (mut sock, _) = listener.accept().await.unwrap();
            let mut buf = [0u8; 4096];
            let _ = sock.read(&mut buf).await.unwrap();
            let body = r#"{"name":"r","visibility":"private"}"#;
            let resp = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                body.len()
            );
            sock.write_all(resp.as_bytes()).await.unwrap();
        });
        let (slept, sleeper) = recording();
        let r = reader(&format!("http://{addr}"), sleeper);
        assert_eq!(
            r.repo_visibility("o", "r").await.unwrap().as_deref(),
            Some("private")
        );
        assert_eq!(r.requests_sent(), 2);
        assert_eq!(slept.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn retry_after_on_429_is_honoured_exactly() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/repos/o/r"))
            .respond_with(ResponseTemplate::new(429).insert_header("retry-after", "7"))
            .up_to_n_times(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/repos/o/r"))
            .respond_with(ResponseTemplate::new(200).set_body_json(repo_json("r", "public")))
            .mount(&server)
            .await;
        let (slept, sleeper) = recording();
        let r = reader(&server.uri(), sleeper);
        assert!(r.repo_visibility("o", "r").await.unwrap().is_some());
        assert_eq!(*slept.lock().unwrap(), vec![Duration::from_secs(7)]);
    }

    #[tokio::test]
    async fn a_primary_rate_limit_403_waits_for_the_reset() {
        let server = MockServer::start().await;
        let reset = now_unix() + 30;
        Mock::given(method("GET"))
            .and(path("/repos/o/r"))
            .respond_with(
                ResponseTemplate::new(403)
                    .insert_header("x-ratelimit-remaining", "0")
                    .insert_header("x-ratelimit-reset", reset.to_string().as_str())
                    .set_body_string(r#"{"message":"API rate limit exceeded"}"#),
            )
            .up_to_n_times(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/repos/o/r"))
            .respond_with(ResponseTemplate::new(200).set_body_json(repo_json("r", "public")))
            .mount(&server)
            .await;
        let (slept, sleeper) = recording();
        let r = reader(&server.uri(), sleeper);
        assert!(r.repo_visibility("o", "r").await.unwrap().is_some());
        let slept = slept.lock().unwrap();
        assert_eq!(slept.len(), 1);
        assert!(
            slept[0] >= Duration::from_secs(28) && slept[0] <= Duration::from_secs(30),
            "slept {:?}",
            slept[0]
        );
    }

    #[tokio::test]
    async fn a_secondary_rate_limit_403_is_retried() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/repos/o/r"))
            .respond_with(ResponseTemplate::new(403).set_body_string(
                r#"{"message":"You have exceeded a secondary rate limit. Please wait a few minutes before you try again."}"#,
            ))
            .up_to_n_times(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/repos/o/r"))
            .respond_with(ResponseTemplate::new(200).set_body_json(repo_json("r", "public")))
            .mount(&server)
            .await;
        let (slept, sleeper) = recording();
        let r = reader(&server.uri(), sleeper);
        assert!(r.repo_visibility("o", "r").await.unwrap().is_some());
        assert_eq!(slept.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn a_reset_beyond_the_bound_fails_naming_the_reset() {
        let server = MockServer::start().await;
        let reset = now_unix() + 3600;
        Mock::given(method("GET"))
            .and(path("/repos/o/r"))
            .respond_with(
                ResponseTemplate::new(403)
                    .insert_header("x-ratelimit-remaining", "0")
                    .insert_header("x-ratelimit-reset", reset.to_string().as_str()),
            )
            .expect(1)
            .mount(&server)
            .await;
        let (slept, sleeper) = recording();
        let r = reader(&server.uri(), sleeper);
        let err = r.repo_visibility("o", "r").await.unwrap_err();
        assert!(
            matches!(err.cause, GetFailure::RateLimitBeyondBound { reset_unix: Some(s), .. } if s == reset)
        );
        let text = err.to_string();
        let stamp =
            chrono::DateTime::<chrono::Utc>::from_timestamp(i64::try_from(reset).unwrap(), 0)
                .unwrap()
                .to_rfc3339();
        assert!(text.contains(&stamp), "{text}");
        assert!(text.contains("1 attempt(s), not retryable"), "{text}");
        assert!(slept.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn a_401_is_fatal_after_exactly_one_request() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/repos/o/r"))
            .respond_with(ResponseTemplate::new(401).insert_header("retry-after", "1"))
            .expect(1)
            .mount(&server)
            .await;
        let (slept, sleeper) = recording();
        let r = reader(&server.uri(), sleeper);
        let err = r.repo_visibility("o", "r").await.unwrap_err();
        assert_eq!(err.cause, GetFailure::Unauthorized);
        assert_eq!(err.attempts, 1);
        assert_eq!(r.requests_sent(), 1);
        assert!(slept.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn a_plain_403_is_fatal_after_exactly_one_request() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/repos/o/r"))
            .respond_with(
                ResponseTemplate::new(403)
                    .set_body_string(r#"{"message":"Resource not accessible by integration"}"#),
            )
            .expect(1)
            .mount(&server)
            .await;
        let (_, sleeper) = recording();
        let r = reader(&server.uri(), sleeper);
        assert_eq!(
            r.repo_visibility("o", "r").await.unwrap_err().cause,
            GetFailure::Forbidden
        );
    }

    #[tokio::test]
    async fn a_404_is_absent_not_an_error() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/repos/o/missing"))
            .respond_with(ResponseTemplate::new(404))
            .expect(1)
            .mount(&server)
            .await;
        let (_, sleeper) = recording();
        let r = reader(&server.uri(), sleeper);
        assert_eq!(r.repo_visibility("o", "missing").await.unwrap(), None);
    }

    #[tokio::test]
    async fn exhausted_retries_name_the_url_attempts_and_cause() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/repos/o/r"))
            .respond_with(ResponseTemplate::new(503))
            .expect(6)
            .mount(&server)
            .await;
        let (slept, sleeper) = recording();
        let r = reader(&server.uri(), sleeper);
        let err = r.repo_visibility("o", "r").await.unwrap_err();
        assert_eq!(err.attempts, 6);
        assert!(err.exhausted);
        let text = err.to_string();
        assert!(text.contains("/repos/o/r"), "{text}");
        assert!(text.contains("HTTP 503"), "{text}");
        assert!(text.contains("6 attempt(s), retries exhausted"), "{text}");
        let slept = slept.lock().unwrap();
        assert_eq!(slept.len(), 5);
        for (i, d) in slept.iter().enumerate() {
            assert!(*d <= fast().backoff_for(u32::try_from(i).unwrap()));
        }
    }

    #[tokio::test]
    async fn the_listing_follows_three_pages() {
        let server = MockServer::start().await;
        let base = server.uri();
        let link = |n: u32| {
            format!("<{base}/orgs/o/repos?type=all&per_page=100&page={n}>; rel=\"next\", <{base}/orgs/o/repos?type=all&per_page=100&page=3>; rel=\"last\"")
        };
        Mock::given(method("GET"))
            .and(path("/orgs/o/repos"))
            .and(query_param("type", "all"))
            .and(query_param("per_page", "100"))
            .and(query_param_is_missing("page"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("link", link(2).as_str())
                    .set_body_json(serde_json::json!([
                        repo_json("a", "public"),
                        repo_json("b", "private")
                    ])),
            )
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/orgs/o/repos"))
            .and(query_param("page", "2"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("link", link(3).as_str())
                    .set_body_json(serde_json::json!([{ "name": "c", "private": false }])),
            )
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/orgs/o/repos"))
            .and(query_param("page", "3"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(serde_json::json!([repo_json("d", "internal")])),
            )
            .expect(1)
            .mount(&server)
            .await;
        let (_, sleeper) = recording();
        let r = reader(&base, sleeper);
        let listed = r.org_visibilities("o").await.unwrap().unwrap();
        assert_eq!(
            listed.into_iter().collect::<Vec<_>>(),
            vec![
                ("a".to_string(), "public".to_string()),
                ("b".to_string(), "private".to_string()),
                ("c".to_string(), "public".to_string()),
                ("d".to_string(), "internal".to_string()),
            ]
        );
        assert_eq!(r.requests_sent(), 3);
    }

    #[tokio::test]
    async fn a_page_failure_mid_listing_fails_the_whole_listing() {
        let server = MockServer::start().await;
        let base = server.uri();
        Mock::given(method("GET"))
            .and(path("/orgs/o/repos"))
            .and(query_param_is_missing("page"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header(
                        "link",
                        format!("<{base}/orgs/o/repos?page=2>; rel=\"next\"").as_str(),
                    )
                    .set_body_json(serde_json::json!([repo_json("a", "public")])),
            )
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/orgs/o/repos"))
            .and(query_param("page", "2"))
            .respond_with(ResponseTemplate::new(500))
            .mount(&server)
            .await;
        let (_, sleeper) = recording();
        let err = reader(&base, sleeper)
            .org_visibilities("o")
            .await
            .unwrap_err();
        assert!(err.url.ends_with("page=2"), "{err}");
        assert!(err.exhausted);
    }

    #[tokio::test]
    async fn pagination_never_follows_a_link_off_the_api_base() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/orgs/o/repos"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header(
                        "link",
                        "<https://elsewhere.example/orgs/o/repos?page=2>; rel=\"next\"",
                    )
                    .set_body_json(serde_json::json!([])),
            )
            .expect(1)
            .mount(&server)
            .await;
        let (_, sleeper) = recording();
        let err = reader(&server.uri(), sleeper)
            .org_visibilities("o")
            .await
            .unwrap_err();
        assert!(matches!(err.cause, GetFailure::ForeignPage(_)), "{err}");
    }

    #[tokio::test]
    async fn an_owner_with_no_org_listing_reports_none() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/orgs/someone/repos"))
            .respond_with(ResponseTemplate::new(404))
            .mount(&server)
            .await;
        let (_, sleeper) = recording();
        assert_eq!(
            reader(&server.uri(), sleeper)
                .org_visibilities("someone")
                .await
                .unwrap(),
            None
        );
    }

    #[test]
    fn next_link_reads_only_the_next_relation() {
        let mut h = HeaderMap::new();
        h.insert(
            reqwest::header::LINK,
            "<https://api.github.com/x?page=1>; rel=\"prev\", <https://api.github.com/x?page=3>; rel=\"next\""
                .parse()
                .unwrap(),
        );
        assert_eq!(
            next_link(&h).as_deref(),
            Some("https://api.github.com/x?page=3")
        );
        h.insert(
            reqwest::header::LINK,
            "<https://api.github.com/x?page=1>; rel=\"prev\""
                .parse()
                .unwrap(),
        );
        assert_eq!(next_link(&h), None);
        assert_eq!(next_link(&HeaderMap::new()), None);
    }

    #[test]
    fn the_production_client_is_bounded() {
        let r = GitHubReader::new(None).unwrap();
        assert_eq!(r.base, GITHUB_API);
        assert_eq!(r.jitter, Jitter::Full);
        assert_eq!(r.max_advised_wait, MAX_ADVISED_WAIT);
        assert!(r.policy.max_retries >= 3);
        assert_eq!(CONNECT_TIMEOUT, Duration::from_secs(10));
        assert_eq!(REQUEST_TIMEOUT, Duration::from_secs(30));
    }
}
