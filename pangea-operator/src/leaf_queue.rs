use std::collections::{BTreeMap, VecDeque};
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use pangea_leaf::queue::{consumer_name, credential_filter, LeafWork, COST_HEADER};

use crate::config::{LeafConfig, LeafQueueKind};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueueSettings {
    pub stream: String,
    pub subject_root: String,
    pub ack_wait: Duration,
    pub max_deliver: u64,
    pub duplicate_window: Duration,
    pub max_ack_pending: usize,
}

impl QueueSettings {
    #[must_use]
    pub fn from_config(cfg: &LeafConfig) -> Self {
        Self {
            stream: pangea_leaf::queue::STREAM.to_string(),
            subject_root: pangea_leaf::queue::SUBJECT_ROOT.to_string(),
            ack_wait: Duration::from_secs(cfg.nats.ack_wait_secs.max(1)),
            max_deliver: cfg.nats.max_deliver.max(1),
            duplicate_window: Duration::from_secs(cfg.nats.duplicate_window_secs.max(1)),
            max_ack_pending: if cfg.per_credential_cap == 0 {
                usize::MAX
            } else {
                cfg.per_credential_cap
            },
        }
    }

    #[must_use]
    pub fn heartbeat(&self) -> Duration {
        self.ack_wait / 2
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Published {
    Queued,
    Duplicate,
}

#[derive(Debug, thiserror::Error)]
pub enum QueueError {
    #[error("leaf queue: {0}")]
    Broker(String),
    #[error("leaf queue: a message did not read as leaf work: {0}")]
    Payload(String),
}

fn broker<E: std::fmt::Display>(e: E) -> QueueError {
    QueueError::Broker(e.to_string())
}

enum Handle {
    InProcess { state: Arc<Mutex<State>>, seq: u64 },
    JetStream(Box<async_nats::jetstream::Message>),
}

pub struct Claim {
    work: LeafWork,
    delivered: u64,
    handle: Handle,
}

impl Claim {
    #[must_use]
    pub fn work(&self) -> &LeafWork {
        &self.work
    }

    #[must_use]
    pub fn delivered(&self) -> u64 {
        self.delivered
    }

    pub async fn progress(&self) -> Result<(), QueueError> {
        match &self.handle {
            Handle::InProcess { state, seq } => {
                let mut s = state.lock().expect("leaf queue mutex poisoned");
                let wait = s.settings.ack_wait;
                if let Some(entry) = s.inflight.get_mut(seq) {
                    entry.deadline = Instant::now() + wait;
                }
                Ok(())
            }
            Handle::JetStream(m) => m
                .ack_with(async_nats::jetstream::AckKind::Progress)
                .await
                .map_err(broker),
        }
    }

    pub async fn ack(self) -> Result<(), QueueError> {
        match self.handle {
            Handle::InProcess { state, seq } => {
                state
                    .lock()
                    .expect("leaf queue mutex poisoned")
                    .inflight
                    .remove(&seq);
                Ok(())
            }
            Handle::JetStream(m) => m.ack().await.map_err(broker),
        }
    }

    pub async fn nak(self, delay: Option<Duration>) -> Result<(), QueueError> {
        match self.handle {
            Handle::InProcess { state, seq } => {
                let mut s = state.lock().expect("leaf queue mutex poisoned");
                if let Some(entry) = s.inflight.remove(&seq) {
                    s.ready.push_back(entry.entry);
                }
                Ok(())
            }
            Handle::JetStream(m) => m
                .ack_with(async_nats::jetstream::AckKind::Nak(delay))
                .await
                .map_err(broker),
        }
    }
}

#[async_trait]
pub trait LeafQueue: Send + Sync {
    fn kind(&self) -> LeafQueueKind;

    async fn publish(&self, work: &LeafWork) -> Result<Published, QueueError>;

    async fn pull(&self, credential: &str, max: usize) -> Result<Vec<Claim>, QueueError>;

    async fn exhausted(&self) -> Result<Vec<LeafWork>, QueueError>;
}

pub async fn work_on<T, E, F>(
    claim: Claim,
    heartbeat: Duration,
    fut: F,
) -> Result<Result<T, E>, QueueError>
where
    F: Future<Output = Result<T, E>>,
{
    tokio::pin!(fut);
    let mut beat = tokio::time::interval(heartbeat.max(Duration::from_millis(10)));
    beat.tick().await;
    let out = loop {
        tokio::select! {
            out = &mut fut => break out,
            _ = beat.tick() => claim.progress().await?,
        }
    };
    match &out {
        Ok(_) => claim.ack().await?,
        Err(_) => claim.nak(None).await?,
    }
    Ok(out)
}

#[derive(Clone)]
struct Entry {
    seq: u64,
    work: LeafWork,
    delivered: u64,
}

struct Inflight {
    entry: Entry,
    deadline: Instant,
}

struct State {
    settings: QueueSettings,
    seen: BTreeMap<String, Instant>,
    ready: VecDeque<Entry>,
    inflight: BTreeMap<u64, Inflight>,
    dead: Vec<LeafWork>,
    next: u64,
}

impl State {
    fn expire(&mut self, now: Instant) {
        let window = self.settings.duplicate_window;
        self.seen
            .retain(|_, at| now.saturating_duration_since(*at) < window);
        let due: Vec<u64> = self
            .inflight
            .iter()
            .filter(|(_, f)| f.deadline <= now)
            .map(|(seq, _)| *seq)
            .collect();
        for seq in due {
            if let Some(f) = self.inflight.remove(&seq) {
                if f.entry.delivered >= self.settings.max_deliver {
                    self.dead.push(f.entry.work);
                } else {
                    self.ready.push_back(f.entry);
                }
            }
        }
    }
}

pub struct InProcessQueue {
    state: Arc<Mutex<State>>,
}

impl InProcessQueue {
    #[must_use]
    pub fn new(settings: QueueSettings) -> Self {
        Self {
            state: Arc::new(Mutex::new(State {
                settings,
                seen: BTreeMap::new(),
                ready: VecDeque::new(),
                inflight: BTreeMap::new(),
                dead: Vec::new(),
                next: 1,
            })),
        }
    }

    #[must_use]
    pub fn pending_cost(&self, credential: &str) -> u64 {
        let s = self.state.lock().expect("leaf queue mutex poisoned");
        s.ready
            .iter()
            .map(|e| &e.work)
            .chain(s.inflight.values().map(|f| &f.entry.work))
            .filter(|w| w.credential == credential)
            .map(|w| w.cost)
            .sum()
    }
}

#[async_trait]
impl LeafQueue for InProcessQueue {
    fn kind(&self) -> LeafQueueKind {
        LeafQueueKind::InProcess
    }

    async fn publish(&self, work: &LeafWork) -> Result<Published, QueueError> {
        let now = Instant::now();
        let mut s = self.state.lock().expect("leaf queue mutex poisoned");
        s.expire(now);
        let id = work.msg_id();
        if s.seen.contains_key(&id) {
            return Ok(Published::Duplicate);
        }
        s.seen.insert(id, now);
        let seq = s.next;
        s.next += 1;
        s.ready.push_back(Entry {
            seq,
            work: work.clone(),
            delivered: 0,
        });
        Ok(Published::Queued)
    }

    async fn pull(&self, credential: &str, max: usize) -> Result<Vec<Claim>, QueueError> {
        let now = Instant::now();
        let mut s = self.state.lock().expect("leaf queue mutex poisoned");
        s.expire(now);
        let held = s
            .inflight
            .values()
            .filter(|f| f.entry.work.credential == credential)
            .count();
        let room = s.settings.max_ack_pending.saturating_sub(held).min(max);
        let mut out = Vec::new();
        let mut keep = VecDeque::new();
        while let Some(mut e) = s.ready.pop_front() {
            if out.len() < room && e.work.credential == credential {
                e.delivered += 1;
                let deadline = now + s.settings.ack_wait;
                out.push(Claim {
                    work: e.work.clone(),
                    delivered: e.delivered,
                    handle: Handle::InProcess {
                        state: Arc::clone(&self.state),
                        seq: e.seq,
                    },
                });
                s.inflight.insert(e.seq, Inflight { entry: e, deadline });
            } else {
                keep.push_back(e);
            }
        }
        s.ready = keep;
        Ok(out)
    }

    async fn exhausted(&self) -> Result<Vec<LeafWork>, QueueError> {
        let mut s = self.state.lock().expect("leaf queue mutex poisoned");
        s.expire(Instant::now());
        Ok(std::mem::take(&mut s.dead))
    }
}

pub struct JetStreamQueue {
    js: async_nats::jetstream::Context,
    stream: async_nats::jetstream::stream::Stream,
    settings: QueueSettings,
    consumers: tokio::sync::Mutex<
        BTreeMap<
            String,
            async_nats::jetstream::consumer::Consumer<
                async_nats::jetstream::consumer::pull::Config,
            >,
        >,
    >,
    exhausted: Arc<Mutex<Vec<u64>>>,
}

impl JetStreamQueue {
    pub async fn connect(url: &str, settings: QueueSettings) -> Result<Self, QueueError> {
        let client = async_nats::connect(url).await.map_err(broker)?;
        let js = async_nats::jetstream::new(client.clone());
        let stream = js
            .get_or_create_stream(async_nats::jetstream::stream::Config {
                name: settings.stream.clone(),
                subjects: vec![[settings.subject_root.as_str(), ">"].join(".")],
                duplicate_window: settings.duplicate_window,
                retention: async_nats::jetstream::stream::RetentionPolicy::WorkQueue,
                ..Default::default()
            })
            .await
            .map_err(broker)?;
        let advisory = [
            "$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES",
            settings.stream.as_str(),
            "*",
        ]
        .join(".");
        let mut sub = client.subscribe(advisory).await.map_err(broker)?;
        let exhausted = Arc::new(Mutex::new(Vec::new()));
        let sink = Arc::clone(&exhausted);
        tokio::spawn(async move {
            use futures::StreamExt;
            while let Some(msg) = sub.next().await {
                let seq = serde_json::from_slice::<serde_json::Value>(&msg.payload)
                    .ok()
                    .and_then(|v| v.get("stream_seq").and_then(serde_json::Value::as_u64));
                if let Some(seq) = seq {
                    sink.lock().expect("leaf queue mutex poisoned").push(seq);
                }
            }
        });
        Ok(Self {
            js,
            stream,
            settings,
            consumers: tokio::sync::Mutex::new(BTreeMap::new()),
            exhausted,
        })
    }

    async fn consumer(
        &self,
        credential: &str,
    ) -> Result<
        async_nats::jetstream::consumer::Consumer<async_nats::jetstream::consumer::pull::Config>,
        QueueError,
    > {
        let mut held = self.consumers.lock().await;
        if let Some(c) = held.get(credential) {
            return Ok(c.clone());
        }
        let name = consumer_name(credential);
        let max_ack_pending = i64::try_from(self.settings.max_ack_pending).unwrap_or(-1);
        let c = self
            .stream
            .get_or_create_consumer(
                &name,
                async_nats::jetstream::consumer::pull::Config {
                    durable_name: Some(name.clone()),
                    filter_subject: credential_filter(&self.settings.subject_root, credential),
                    ack_wait: self.settings.ack_wait,
                    max_deliver: i64::try_from(self.settings.max_deliver).unwrap_or(i64::MAX),
                    max_ack_pending,
                    ack_policy: async_nats::jetstream::consumer::AckPolicy::Explicit,
                    ..Default::default()
                },
            )
            .await
            .map_err(broker)?;
        held.insert(credential.to_string(), c.clone());
        Ok(c)
    }
}

#[async_trait]
impl LeafQueue for JetStreamQueue {
    fn kind(&self) -> LeafQueueKind {
        LeafQueueKind::Jetstream
    }

    async fn publish(&self, work: &LeafWork) -> Result<Published, QueueError> {
        let payload = serde_json::to_vec(work).map_err(|e| QueueError::Payload(e.to_string()))?;
        let message = async_nats::jetstream::message::PublishMessage::build()
            .message_id(work.msg_id())
            .header(COST_HEADER, work.cost.to_string().as_str())
            .payload(payload.into());
        let ack = self
            .js
            .send_publish(work.subject(&self.settings.subject_root), message)
            .await
            .map_err(broker)?
            .await
            .map_err(broker)?;
        Ok(if ack.duplicate {
            Published::Duplicate
        } else {
            Published::Queued
        })
    }

    async fn pull(&self, credential: &str, max: usize) -> Result<Vec<Claim>, QueueError> {
        use futures::StreamExt;
        let consumer = self.consumer(credential).await?;
        let mut batch = consumer
            .fetch()
            .max_messages(max.max(1))
            .expires(Duration::from_millis(500))
            .messages()
            .await
            .map_err(broker)?;
        let mut out = Vec::new();
        while let Some(m) = batch.next().await {
            let m = m.map_err(broker)?;
            let work: LeafWork = serde_json::from_slice(&m.payload)
                .map_err(|e| QueueError::Payload(e.to_string()))?;
            let delivered = m
                .info()
                .map(|i| u64::try_from(i.delivered).unwrap_or(0))
                .unwrap_or(0);
            out.push(Claim {
                work,
                delivered,
                handle: Handle::JetStream(Box::new(m)),
            });
        }
        Ok(out)
    }

    async fn exhausted(&self) -> Result<Vec<LeafWork>, QueueError> {
        let seqs = std::mem::take(&mut *self.exhausted.lock().expect("leaf queue mutex poisoned"));
        let mut out = Vec::new();
        for seq in seqs {
            let raw = self.stream.get_raw_message(seq).await.map_err(broker)?;
            let work: LeafWork = serde_json::from_slice(&raw.payload)
                .map_err(|e| QueueError::Payload(e.to_string()))?;
            out.push(work);
        }
        Ok(out)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QueueChoice {
    InProcess,
    JetStream { url: String },
}

pub fn choose(cfg: &LeafConfig) -> (QueueChoice, Option<String>) {
    match (cfg.queue, cfg.nats.url.as_deref().filter(|u| !u.trim().is_empty())) {
        (LeafQueueKind::InProcess, _) => (QueueChoice::InProcess, None),
        (LeafQueueKind::Jetstream, Some(url)) => (
            QueueChoice::JetStream {
                url: url.to_string(),
            },
            None,
        ),
        (LeafQueueKind::Jetstream, None) => (
            QueueChoice::InProcess,
            Some("leaf.queue jetstream needs leaf.nats.url; the entry is refused and the queue stays in process".into()),
        ),
    }
}

pub async fn build(cfg: &LeafConfig) -> (Arc<dyn LeafQueue>, Option<String>) {
    let settings = QueueSettings::from_config(cfg);
    match choose(cfg) {
        (QueueChoice::InProcess, refusal) => (Arc::new(InProcessQueue::new(settings)), refusal),
        (QueueChoice::JetStream { url }, _) => {
            match JetStreamQueue::connect(&url, settings.clone()).await {
                Ok(q) => (Arc::new(q), None),
                Err(e) => (
                    Arc::new(InProcessQueue::new(settings)),
                    Some(
                        [
                            "leaf.queue jetstream could not connect (",
                            &e.to_string(),
                            "); the queue stays in process",
                        ]
                        .concat(),
                    ),
                ),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pangea_leaf::queue::LeafAction;

    fn settings(ack_wait_ms: u64) -> QueueSettings {
        QueueSettings {
            stream: "T".into(),
            subject_root: "pangea.leaf".into(),
            ack_wait: Duration::from_millis(ack_wait_ms),
            max_deliver: 3,
            duplicate_window: Duration::from_secs(600),
            max_ack_pending: 8,
        }
    }

    fn work(leaf: &str, digest: &str) -> LeafWork {
        LeafWork {
            leaf: leaf.into(),
            credential: "gh".into(),
            action: LeafAction::Plan,
            digest: digest.into(),
            cost: 4,
        }
    }

    #[tokio::test]
    async fn a_republished_action_is_dropped_and_a_new_digest_is_not() {
        let q = InProcessQueue::new(settings(1000));
        assert_eq!(
            q.publish(&work("a", "d1")).await.unwrap(),
            Published::Queued
        );
        assert_eq!(
            q.publish(&work("a", "d1")).await.unwrap(),
            Published::Duplicate
        );
        assert_eq!(
            q.publish(&work("a", "d2")).await.unwrap(),
            Published::Queued
        );
        assert_eq!(q.pull("gh", 10).await.unwrap().len(), 2);
    }

    #[tokio::test]
    async fn a_dead_workers_message_reaches_the_other_worker_after_ack_wait() {
        let q = InProcessQueue::new(settings(100));
        q.publish(&work("a", "d1")).await.unwrap();
        let first = q.pull("gh", 1).await.unwrap();
        assert_eq!(first.len(), 1);
        drop(first);
        assert!(q.pull("gh", 1).await.unwrap().is_empty());
        tokio::time::sleep(Duration::from_millis(150)).await;
        let second = q.pull("gh", 1).await.unwrap();
        assert_eq!(second.len(), 1);
        assert_eq!(second[0].delivered(), 2);
        assert_eq!(second[0].work().leaf, "a");
    }

    #[tokio::test]
    async fn in_progress_acks_keep_a_live_worker_from_losing_its_message() {
        let q = InProcessQueue::new(settings(100));
        q.publish(&work("a", "d1")).await.unwrap();
        let claim = q.pull("gh", 1).await.unwrap().pop().unwrap();
        let out = work_on(claim, Duration::from_millis(40), async {
            tokio::time::sleep(Duration::from_millis(300)).await;
            assert!(q.pull("gh", 1).await.unwrap().is_empty());
            Ok::<_, ()>(7)
        })
        .await
        .unwrap();
        assert_eq!(out, Ok(7));
        tokio::time::sleep(Duration::from_millis(150)).await;
        assert!(q.pull("gh", 1).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn a_message_past_max_deliver_is_reported_exhausted() {
        let q = InProcessQueue::new(settings(30));
        q.publish(&work("a", "d1")).await.unwrap();
        for _ in 0..3 {
            let c = q.pull("gh", 1).await.unwrap();
            assert_eq!(c.len(), 1);
            drop(c);
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert!(q.pull("gh", 1).await.unwrap().is_empty());
        let dead = q.exhausted().await.unwrap();
        assert_eq!(dead.len(), 1);
        assert_eq!(dead[0].leaf, "a");
    }

    #[tokio::test]
    async fn max_ack_pending_is_the_per_credential_cap_and_cost_is_counted() {
        let mut s = settings(1000);
        s.max_ack_pending = 2;
        let q = InProcessQueue::new(s);
        for i in 0..5 {
            q.publish(&work(&format!("l{i}"), "d")).await.unwrap();
        }
        assert_eq!(q.pending_cost("gh"), 20);
        let held = q.pull("gh", 10).await.unwrap();
        assert_eq!(held.len(), 2);
        assert!(q.pull("gh", 10).await.unwrap().is_empty());
        for c in held {
            c.ack().await.unwrap();
        }
        assert_eq!(q.pull("gh", 10).await.unwrap().len(), 2);
        assert_eq!(q.pending_cost("other"), 0);
    }

    #[test]
    fn jetstream_without_a_url_is_refused_alone_and_stays_in_process() {
        let mut cfg = LeafConfig {
            queue: LeafQueueKind::Jetstream,
            ..LeafConfig::default()
        };
        let (choice, refusal) = choose(&cfg);
        assert_eq!(choice, QueueChoice::InProcess);
        assert!(refusal.unwrap().contains("leaf.nats.url"));
        cfg.nats.url = Some("nats://127.0.0.1:4222".into());
        assert_eq!(
            choose(&cfg),
            (
                QueueChoice::JetStream {
                    url: "nats://127.0.0.1:4222".into()
                },
                None
            )
        );
        assert_eq!(
            choose(&LeafConfig::default()),
            (QueueChoice::InProcess, None)
        );
    }
}
