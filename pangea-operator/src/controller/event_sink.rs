use async_trait::async_trait;
use chrono::{DateTime, Utc};
use k8s_openapi::api::core::v1::{Event as CoreEvent, EventSource, ObjectReference};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{ObjectMeta, Time};
use kube::api::{Api, PostParams};
use kube::runtime::events::{Event, EventType, Recorder, Reporter};
use kube::Client;
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use tracing::{info, warn};

use crate::crd::{RecordedEvent, RecordedEventType};

pub const RING_CAPACITY: usize = 20;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum EventApi {
    EventsV1,
    CoreV1,
    Unavailable,
}

impl EventApi {
    pub fn label(self) -> &'static str {
        match self {
            EventApi::EventsV1 => "events.k8s.io/v1",
            EventApi::CoreV1 => "v1",
            EventApi::Unavailable => "none",
        }
    }

    pub fn demoted(self) -> EventApi {
        match self {
            EventApi::EventsV1 => EventApi::CoreV1,
            EventApi::CoreV1 | EventApi::Unavailable => EventApi::Unavailable,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TransportError {
    NotServed(String),
    Failed(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Served {
    pub events_v1: Option<bool>,
    pub core_v1: Option<bool>,
}

impl Served {
    pub fn select(self) -> EventApi {
        match (self.events_v1, self.core_v1) {
            (Some(true), _) | (None, _) => EventApi::EventsV1,
            (Some(false), Some(true)) | (Some(false), None) => EventApi::CoreV1,
            (Some(false), Some(false)) => EventApi::Unavailable,
        }
    }
}

#[async_trait]
pub trait EventTransport: Send + Sync {
    async fn served(&self) -> Served;
    async fn publish(
        &self,
        api: EventApi,
        reference: &ObjectReference,
        event: &Event,
    ) -> Result<(), TransportError>;
}

pub fn classify_kube_error(err: &kube::Error) -> TransportError {
    let text = err.to_string();
    match err {
        kube::Error::Api(resp) if resp.code == 404 && !text.contains("namespaces \"") => {
            TransportError::NotServed(text)
        }
        _ if text.contains("unknown kind") => TransportError::NotServed(text),
        _ => TransportError::Failed(text),
    }
}

fn resource_listed(list: &k8s_openapi::apimachinery::pkg::apis::meta::v1::APIResourceList) -> bool {
    list.resources.iter().any(|r| r.name == "events")
}

fn discovery_answer(
    result: std::result::Result<
        k8s_openapi::apimachinery::pkg::apis::meta::v1::APIResourceList,
        kube::Error,
    >,
) -> Option<bool> {
    match result {
        Ok(list) => Some(resource_listed(&list)),
        Err(e) => match classify_kube_error(&e) {
            TransportError::NotServed(_) => Some(false),
            TransportError::Failed(_) => None,
        },
    }
}

pub struct KubeEventTransport {
    client: Client,
    reporter: Reporter,
    recorder: Recorder,
}

impl KubeEventTransport {
    pub fn new(client: Client) -> Self {
        let reporter = Reporter {
            controller: "pangea-operator".into(),
            instance: std::env::var("POD_NAME").ok(),
        };
        let recorder = Recorder::new(client.clone(), reporter.clone());
        Self {
            client,
            reporter,
            recorder,
        }
    }

    fn core_event(&self, reference: &ObjectReference, event: &Event) -> CoreEvent {
        let now = Time(Utc::now());
        let base = reference.name.clone().unwrap_or_default();
        CoreEvent {
            metadata: ObjectMeta {
                generate_name: Some([base.as_str(), "."].concat()),
                namespace: reference.namespace.clone(),
                ..Default::default()
            },
            involved_object: reference.clone(),
            reason: Some(event.reason.clone()),
            message: event.note.clone(),
            type_: Some(
                match event.type_ {
                    EventType::Normal => "Normal",
                    EventType::Warning => "Warning",
                }
                .to_string(),
            ),
            action: Some(event.action.clone()),
            source: Some(EventSource {
                component: Some(self.reporter.controller.clone()),
                host: None,
            }),
            reporting_component: Some(self.reporter.controller.clone()),
            reporting_instance: self.reporter.instance.clone(),
            first_timestamp: Some(now.clone()),
            last_timestamp: Some(now),
            count: Some(1),
            ..Default::default()
        }
    }
}

#[async_trait]
impl EventTransport for KubeEventTransport {
    async fn served(&self) -> Served {
        Served {
            events_v1: discovery_answer(
                self.client
                    .list_api_group_resources("events.k8s.io/v1")
                    .await,
            ),
            core_v1: discovery_answer(self.client.list_core_api_resources("v1").await),
        }
    }

    async fn publish(
        &self,
        api: EventApi,
        reference: &ObjectReference,
        event: &Event,
    ) -> Result<(), TransportError> {
        match api {
            EventApi::EventsV1 => self
                .recorder
                .publish(event, reference)
                .await
                .map_err(|e| classify_kube_error(&e)),
            EventApi::CoreV1 => {
                let ns = reference
                    .namespace
                    .clone()
                    .unwrap_or_else(|| "default".to_string());
                let api: Api<CoreEvent> = Api::namespaced(self.client.clone(), &ns);
                api.create(&PostParams::default(), &self.core_event(reference, event))
                    .await
                    .map(|_| ())
                    .map_err(|e| classify_kube_error(&e))
            }
            EventApi::Unavailable => Err(TransportError::NotServed(
                "no Event API is served".to_string(),
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Delivery {
    Published(EventApi),
    Ring { reason: String },
}

impl Delivery {
    pub fn outcome_label(&self) -> &'static str {
        match self {
            Delivery::Published(api) => api.label(),
            Delivery::Ring { .. } => "ring",
        }
    }
}

pub fn reference_key(reference: &ObjectReference) -> String {
    [
        reference.kind.as_deref().unwrap_or_default(),
        reference.namespace.as_deref().unwrap_or_default(),
        reference.name.as_deref().unwrap_or_default(),
    ]
    .join("/")
}

pub fn push_ring(ring: &mut VecDeque<RecordedEvent>, entry: RecordedEvent, capacity: usize) {
    if let Some(last) = ring.back_mut() {
        if last.reason == entry.reason && last.message == entry.message && last.type_ == entry.type_
        {
            last.count = last.count.saturating_add(1);
            last.at = entry.at;
            return;
        }
    }
    ring.push_back(entry);
    while ring.len() > capacity {
        ring.pop_front();
    }
}

pub fn recorded(event: &Event, at: DateTime<Utc>) -> RecordedEvent {
    RecordedEvent {
        at,
        type_: match event.type_ {
            EventType::Normal => RecordedEventType::Normal,
            EventType::Warning => RecordedEventType::Warning,
        },
        reason: event.reason.clone(),
        message: crate::text_util::truncate_utf8_safe(
            event.note.as_deref().unwrap_or_default(),
            512,
            "…",
        ),
        count: 1,
    }
}

pub struct EventSink {
    transport: Arc<dyn EventTransport>,
    api: Mutex<Option<EventApi>>,
    rings: Mutex<HashMap<String, VecDeque<RecordedEvent>>>,
    capacity: usize,
}

impl std::fmt::Debug for EventSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EventSink")
            .field("api", &self.current_api())
            .finish()
    }
}

impl EventSink {
    pub fn new(transport: Arc<dyn EventTransport>) -> Self {
        Self {
            transport,
            api: Mutex::new(None),
            rings: Mutex::new(HashMap::new()),
            capacity: RING_CAPACITY,
        }
    }

    pub fn for_client(client: Client) -> Self {
        Self::new(Arc::new(KubeEventTransport::new(client)))
    }

    pub fn current_api(&self) -> Option<EventApi> {
        *self.api.lock().unwrap_or_else(|p| p.into_inner())
    }

    fn set_api(&self, api: EventApi) {
        *self.api.lock().unwrap_or_else(|p| p.into_inner()) = Some(api);
    }

    pub async fn discover(&self) -> EventApi {
        let served = self.transport.served().await;
        let api = served.select();
        self.set_api(api);
        info!(
            api = api.label(),
            events_v1 = ?served.events_v1,
            core_v1 = ?served.core_v1,
            "event API discovered"
        );
        api
    }

    async fn resolved_api(&self) -> EventApi {
        match self.current_api() {
            Some(api) => api,
            None => self.discover().await,
        }
    }

    pub fn seed_ring(&self, key: &str, existing: &[RecordedEvent]) {
        let mut rings = self.rings.lock().unwrap_or_else(|p| p.into_inner());
        rings
            .entry(key.to_string())
            .or_insert_with(|| existing.iter().cloned().collect());
    }

    pub fn ring(&self, key: &str) -> Vec<RecordedEvent> {
        self.rings
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .get(key)
            .map(|r| r.iter().cloned().collect())
            .unwrap_or_default()
    }

    fn push(&self, key: &str, entry: RecordedEvent) {
        let mut rings = self.rings.lock().unwrap_or_else(|p| p.into_inner());
        let ring = rings.entry(key.to_string()).or_default();
        push_ring(ring, entry, self.capacity);
    }

    pub async fn record(&self, reference: &ObjectReference, event: &Event) -> Delivery {
        let mut api = self.resolved_api().await;
        let reason = loop {
            if api == EventApi::Unavailable {
                break "no Event API is served by this cluster".to_string();
            }
            match self.transport.publish(api, reference, event).await {
                Ok(()) => return Delivery::Published(api),
                Err(TransportError::NotServed(msg)) => {
                    let next = api.demoted();
                    warn!(
                        from = api.label(),
                        to = next.label(),
                        error = %msg,
                        "event API not served; falling back"
                    );
                    self.set_api(next);
                    api = next;
                }
                Err(TransportError::Failed(msg)) => break msg,
            }
        };
        self.push(&reference_key(reference), recorded(event, Utc::now()));
        warn!(
            reason = %event.reason,
            object = %reference_key(reference),
            why = %reason,
            "event kept in the status ring rather than the Event API"
        );
        Delivery::Ring { reason }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct FakeTransport {
        served: Served,
        events_v1: Result<(), TransportError>,
        core_v1: Result<(), TransportError>,
        calls: Mutex<Vec<EventApi>>,
        discoveries: AtomicUsize,
    }

    impl FakeTransport {
        fn new(
            served: Served,
            events_v1: Result<(), TransportError>,
            core_v1: Result<(), TransportError>,
        ) -> Arc<Self> {
            Arc::new(Self {
                served,
                events_v1,
                core_v1,
                calls: Mutex::new(Vec::new()),
                discoveries: AtomicUsize::new(0),
            })
        }

        fn calls(&self) -> Vec<EventApi> {
            self.calls.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl EventTransport for FakeTransport {
        async fn served(&self) -> Served {
            self.discoveries.fetch_add(1, Ordering::SeqCst);
            self.served
        }

        async fn publish(
            &self,
            api: EventApi,
            _reference: &ObjectReference,
            _event: &Event,
        ) -> Result<(), TransportError> {
            self.calls.lock().unwrap().push(api);
            match api {
                EventApi::EventsV1 => self.events_v1.clone(),
                EventApi::CoreV1 => self.core_v1.clone(),
                EventApi::Unavailable => Err(TransportError::NotServed("none".into())),
            }
        }
    }

    fn reference() -> ObjectReference {
        ObjectReference {
            kind: Some("InfrastructureTemplate".into()),
            namespace: Some("default".into()),
            name: Some("pleme-io-github-repos".into()),
            ..Default::default()
        }
    }

    fn event(reason: &str) -> Event {
        Event {
            type_: EventType::Warning,
            reason: reason.into(),
            note: Some("plan stalled".into()),
            action: reason.into(),
            secondary: None,
        }
    }

    fn not_served() -> TransportError {
        TransportError::NotServed(
            "ApiError: resource not found: unknown kind: events.k8s.io/v1/events".into(),
        )
    }

    const ALL_UNKNOWN: Served = Served {
        events_v1: None,
        core_v1: None,
    };

    #[tokio::test]
    async fn events_v1_served_publishes_there() {
        let t = FakeTransport::new(ALL_UNKNOWN, Ok(()), Ok(()));
        let sink = EventSink::new(t.clone());
        let d = sink.record(&reference(), &event("Applied")).await;
        assert_eq!(d, Delivery::Published(EventApi::EventsV1));
        assert_eq!(t.calls(), vec![EventApi::EventsV1]);
        assert!(sink.ring(&reference_key(&reference())).is_empty());
    }

    #[tokio::test]
    async fn events_v1_not_served_falls_back_to_core_and_stays_there() {
        let t = FakeTransport::new(ALL_UNKNOWN, Err(not_served()), Ok(()));
        let sink = EventSink::new(t.clone());
        let first = sink.record(&reference(), &event("Applied")).await;
        let second = sink.record(&reference(), &event("Applied")).await;
        assert_eq!(first, Delivery::Published(EventApi::CoreV1));
        assert_eq!(second, Delivery::Published(EventApi::CoreV1));
        assert_eq!(
            t.calls(),
            vec![EventApi::EventsV1, EventApi::CoreV1, EventApi::CoreV1]
        );
        assert_eq!(sink.current_api(), Some(EventApi::CoreV1));
    }

    #[tokio::test]
    async fn discovery_without_events_v1_goes_straight_to_core() {
        let t = FakeTransport::new(
            Served {
                events_v1: Some(false),
                core_v1: Some(true),
            },
            Err(not_served()),
            Ok(()),
        );
        let sink = EventSink::new(t.clone());
        assert_eq!(sink.discover().await, EventApi::CoreV1);
        let d = sink.record(&reference(), &event("Applied")).await;
        assert_eq!(d, Delivery::Published(EventApi::CoreV1));
        assert_eq!(t.calls(), vec![EventApi::CoreV1]);
    }

    #[tokio::test]
    async fn neither_api_served_lands_in_the_ring_and_stops_calling_the_api() {
        let t = FakeTransport::new(ALL_UNKNOWN, Err(not_served()), Err(not_served()));
        let sink = EventSink::new(t.clone());
        let d = sink.record(&reference(), &event("PlanStalled")).await;
        assert!(matches!(d, Delivery::Ring { .. }));
        let d2 = sink.record(&reference(), &event("Applied")).await;
        assert!(matches!(d2, Delivery::Ring { .. }));
        assert_eq!(t.calls(), vec![EventApi::EventsV1, EventApi::CoreV1]);
        let ring = sink.ring(&reference_key(&reference()));
        assert_eq!(ring.len(), 2);
        assert_eq!(ring[0].reason, "PlanStalled");
        assert_eq!(ring[1].reason, "Applied");
    }

    #[tokio::test]
    async fn a_transient_failure_rings_the_event_without_demoting_the_api() {
        let t = FakeTransport::new(
            ALL_UNKNOWN,
            Err(TransportError::Failed("connection reset".into())),
            Ok(()),
        );
        let sink = EventSink::new(t.clone());
        let d = sink.record(&reference(), &event("Applied")).await;
        assert_eq!(
            d,
            Delivery::Ring {
                reason: "connection reset".into()
            }
        );
        assert_eq!(sink.current_api(), Some(EventApi::EventsV1));
        assert_eq!(sink.ring(&reference_key(&reference())).len(), 1);
    }

    #[tokio::test]
    async fn discovery_runs_once() {
        let t = FakeTransport::new(ALL_UNKNOWN, Ok(()), Ok(()));
        let sink = EventSink::new(t.clone());
        sink.record(&reference(), &event("A")).await;
        sink.record(&reference(), &event("B")).await;
        assert_eq!(t.discoveries.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn ring_collapses_repeats_and_is_bounded() {
        let mut ring = VecDeque::new();
        let at = |s| DateTime::<Utc>::from_timestamp(s, 0).unwrap();
        for i in 0..3 {
            push_ring(&mut ring, recorded(&event("Same"), at(i)), 5);
        }
        assert_eq!(ring.len(), 1);
        assert_eq!(ring[0].count, 3);
        assert_eq!(ring[0].at, at(2));
        for i in 0..10 {
            let r = ["R", &i.to_string()].concat();
            push_ring(&mut ring, recorded(&event(&r), at(10 + i)), 5);
        }
        assert_eq!(ring.len(), 5);
        assert_eq!(ring[0].reason, "R5");
        assert_eq!(ring[4].reason, "R9");
    }

    #[test]
    fn engenhos_unknown_kind_404_is_not_served() {
        let err = kube::Error::Api(kube::core::ErrorResponse {
            status: "Failure".into(),
            message: "resource not found".into(),
            reason: "unknown kind: events.k8s.io/v1/events".into(),
            code: 404,
        });
        assert!(matches!(
            classify_kube_error(&err),
            TransportError::NotServed(_)
        ));
    }

    #[test]
    fn a_missing_namespace_404_is_a_failure_not_an_unserved_api() {
        let err = kube::Error::Api(kube::core::ErrorResponse {
            status: "Failure".into(),
            message: "namespaces \"gone\" not found".into(),
            reason: "NotFound".into(),
            code: 404,
        });
        assert!(matches!(
            classify_kube_error(&err),
            TransportError::Failed(_)
        ));
    }

    #[test]
    fn served_selection_prefers_events_v1_and_treats_unknown_as_served() {
        let s = |a, b| Served {
            events_v1: a,
            core_v1: b,
        };
        assert_eq!(s(Some(true), Some(true)).select(), EventApi::EventsV1);
        assert_eq!(s(None, Some(true)).select(), EventApi::EventsV1);
        assert_eq!(s(Some(false), Some(true)).select(), EventApi::CoreV1);
        assert_eq!(s(Some(false), None).select(), EventApi::CoreV1);
        assert_eq!(s(Some(false), Some(false)).select(), EventApi::Unavailable);
    }
}
