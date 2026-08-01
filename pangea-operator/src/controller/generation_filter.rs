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
use std::fmt::Debug;
use std::hash::Hash;

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
        .predicate_filter(kube::runtime::predicates::generation);
    Controller::for_stream(stream, reader)
}
