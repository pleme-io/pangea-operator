//! Generation-based watch-stream filter — defense in depth on top of
//! every controller's status-write diff-gate.
//!
//! ## Why this exists
//!
//! The diff-gates (template_controller, operator_policy_controller,
//! flow_controller, compliance_*, synthesizer_format,
//! template/reactive_policy, template/status, fleet_status_controller)
//! all sit at the *write* boundary: "I'm about to PATCH status; is
//! the new state observably different?" That breaks the closed loop
//! at the source.
//!
//! `predicate_filter(predicates::generation)` adds a second layer
//! BEFORE the controller ever decides to reconcile: it filters out
//! every watch event where `metadata.generation` is unchanged from
//! the previous time we saw the object. Status-only writes do NOT
//! bump `metadata.generation` (it advances only on spec mutations);
//! the apiserver guarantees this. So status PATCHes — including the
//! ones we issue ourselves during legitimate updates — never refire
//! a reconcile through this stream. Spec changes (the only thing a
//! controller actually cares about) do.
//!
//! Combined with `Action::requeue(REFRESH_INTERVAL)` for periodic
//! state-of-world refreshes, this gives every controller exactly two
//! sources of work:
//!
//!   1. an actual spec mutation (or new resource creation), and
//!   2. its own scheduled refresh tick.
//!
//! No more self-trigger watch loops, period.
//!
//! ## Why we need `Controller::for_stream` (not `Controller::new`)
//!
//! `predicate_filter` is a `WatchStreamExt` method — it operates on
//! the watcher stream BEFORE the Controller wraps it. To inject the
//! filter, we have to build the stream ourselves and hand it to
//! `Controller::for_stream`, which is the constructor that takes a
//! pre-built stream + a reflector store. Both APIs require the
//! `unstable-runtime-stream-control` feature flag (enabled via
//! `unstable-runtime` at the workspace Cargo.toml level).
//!
//! ## How callers use it
//!
//! ```ignore
//! use crate::controller::generation_filter::filtered_controller;
//!
//! let controller = filtered_controller::<MyCrd>(client.clone());
//! controller
//!     .run(reconcile, error_policy, ctx)
//!     .for_each(|_| futures::future::ready(()))
//!     .await;
//! ```
//!
//! Identical surface to `Controller::new(api, Config::default())` but
//! with the predicate filter built in.

use kube::api::Api;
use kube::runtime::controller::Controller;
use kube::runtime::reflector;
use kube::runtime::watcher;
use kube::runtime::WatchStreamExt;
use kube::Client;
use kube::Resource;
use serde::de::DeserializeOwned;
use std::collections::hash_map::DefaultHasher;
use std::fmt::Debug;
use std::hash::Hash;
use std::hash::Hasher;
use std::sync::atomic::{AtomicBool, Ordering};

/// The annotation whose change asks for a reconcile now (the GraphQL
/// `requestReconcile` mutation writes it).
pub const RECONCILE_REQUESTED_ANNOTATION: &str = "pangea.pleme.io/reconcile-requested";

static HONOR_RECONCILE_REQUESTS: AtomicBool = AtomicBool::new(false);

/// Make a change of [`RECONCILE_REQUESTED_ANNOTATION`] a reconcile trigger on
/// every filtered controller (`reactive.honorReconcileRequests`). Read per
/// event, so it may be set after the controllers are built.
pub fn set_honor_reconcile_requests(on: bool) {
    HONOR_RECONCILE_REQUESTS.store(on, Ordering::Relaxed);
}

#[must_use]
pub fn honors_reconcile_requests() -> bool {
    HONOR_RECONCILE_REQUESTS.load(Ordering::Relaxed)
}

/// The value whose change lets a watch event through. Off, exactly
/// `predicates::generation` (spec mutations only). On, also the reconcile
/// request annotation, so a metadata-only edit that asks for work is a
/// trigger and a status write still is not.
#[must_use]
pub fn trigger_key<K: Resource>(obj: &K, honor_requests: bool) -> Option<u64> {
    let generation = obj.meta().generation.map(|g| g as u64);
    if !honor_requests {
        return generation;
    }
    let requested = obj
        .meta()
        .annotations
        .as_ref()
        .and_then(|a| a.get(RECONCILE_REQUESTED_ANNOTATION));
    let mut h = DefaultHasher::new();
    h.write_u64(generation.unwrap_or(0));
    if let Some(v) = requested {
        h.write(v.as_bytes());
    }
    Some(h.finish())
}

/// Build a `Controller<K>` whose trigger stream filters out watch
/// events that don't bump `metadata.generation`.
///
/// Equivalent to `Controller::new(Api::all(client), Config::default())`
/// except the watcher stream is wrapped with
/// `predicate_filter(predicates::generation)` first. This drops
/// status-only Modify events before they reach the reconciler — even
/// if some future controller forgets its in-reconcile diff-gate, the
/// stream-level filter prevents the closed-loop self-trigger.
///
/// Generic over any cluster-scoped or namespace-scoped CRD with a
/// `()` `DynamicType` (i.e., the static-type kube-rs derive path —
/// every pangea CRD).
pub fn filtered_controller<K>(client: Client) -> Controller<K>
where
    K: Resource<DynamicType = ()> + Clone + DeserializeOwned + Debug + Send + Sync + 'static,
    K::DynamicType: Eq + Hash + Clone + Default,
{
    let api: Api<K> = Api::all(client);
    let (reader, writer) = reflector::store();
    let stream = watcher(api, watcher::Config::default())
        .default_backoff()
        .reflect(writer)
        .applied_objects()
        .predicate_filter(|obj: &K| trigger_key(obj, honors_reconcile_requests()));
    Controller::for_stream(stream, reader)
}

#[cfg(test)]
mod tests {
    use super::*;
    use k8s_openapi::api::core::v1::ConfigMap;
    use std::collections::BTreeMap;

    fn cm(generation: i64, requested: Option<&str>) -> ConfigMap {
        let mut c = ConfigMap::default();
        c.metadata.generation = Some(generation);
        if let Some(r) = requested {
            c.metadata.annotations = Some(BTreeMap::from([(
                RECONCILE_REQUESTED_ANNOTATION.to_string(),
                r.to_string(),
            )]));
        }
        c
    }

    #[test]
    fn off_it_is_exactly_the_generation() {
        assert_eq!(trigger_key(&cm(3, Some("a")), false), Some(3));
        assert_eq!(
            trigger_key(&cm(3, Some("a")), false),
            trigger_key(&cm(3, Some("b")), false),
            "a request annotation is inert when not honored"
        );
    }

    #[test]
    fn on_a_new_request_is_a_new_trigger() {
        assert_ne!(
            trigger_key(&cm(3, Some("a")), true),
            trigger_key(&cm(3, Some("b")), true)
        );
        assert_ne!(
            trigger_key(&cm(3, None), true),
            trigger_key(&cm(3, Some("a")), true)
        );
    }

    #[test]
    fn on_an_unchanged_object_is_not_a_new_trigger() {
        assert_eq!(
            trigger_key(&cm(3, Some("a")), true),
            trigger_key(&cm(3, Some("a")), true)
        );
    }

    #[test]
    fn on_a_spec_change_still_triggers() {
        assert_ne!(
            trigger_key(&cm(3, Some("a")), true),
            trigger_key(&cm(4, Some("a")), true)
        );
    }
}
