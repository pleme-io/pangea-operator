//! Lease-based leader election for the operator.
//!
//! # Why
//!
//! The operator is a single *logical* reconciler: magma/tofu hold per-workspace
//! state locks, so two pods reconciling the same workspace concurrently corrupt
//! state. Historically the chart enforced this *structurally* with
//! `replicaCount: 1` + `strategy: Recreate` — which makes every image rollout a
//! HARD outage: the old (healthy) pod is killed *before* the new one is Ready,
//! and a broken new image leaves ZERO operators running (this is exactly what
//! took the operator down on 2026-06-19 when a corrupt image blob wedged the
//! new pod in `CreateContainerError`).
//!
//! With a `coordination.k8s.io/Lease`, the chart can move to
//! `RollingUpdate` (`maxUnavailable: 0`, `maxSurge: 1`):
//!
//! * The new pod boots and passes readiness **without holding the lease**, so a
//!   broken image simply never becomes Ready and the old pod keeps the lease and
//!   keeps reconciling — **a bad bump becomes a no-op, not an outage**.
//! * Only when the new pod *is* Ready does Kubernetes terminate the old pod,
//!   which releases the lease (or lets it expire), and the new pod then acquires
//!   it and starts reconciling. Exactly one reconciler at any instant.
//!
//! The load-bearing invariant: **readiness MUST NOT depend on holding the
//! lease.** `main.rs` brings the health server up (and the DB-backed `/readyz`)
//! *before* election, and only the controllers are gated on leadership.
//!
//! # Algorithm
//!
//! Standard optimistic-concurrency leader election (the same shape as
//! client-go's `LeaderElector`):
//!
//! 1. Read the Lease (create it, held by us, if absent).
//! 2. [`decide`] whether we may hold it: unheld, held-by-us, or held-by-other
//!    whose lease has expired → acquire; otherwise wait.
//! 3. On acquire, write our identity + a fresh `renewTime` back using the
//!    object's `resourceVersion` for compare-and-set. A `409 Conflict` means
//!    another pod moved first — we simply retry on the next tick.
//!
//! The acquire/expiry decision is a pure function ([`decide`]) so it is unit
//! tested without a cluster.

use std::time::Duration;

use chrono::{DateTime, Utc};
use k8s_openapi::api::coordination::v1::{Lease, LeaseSpec};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{MicroTime, ObjectMeta};
use kube::api::{Api, PostParams};
use kube::Client;
use tracing::{debug, info, warn};

use crate::error::{Error, Result};

/// Configuration for the leader-election lease.
#[derive(Debug, Clone)]
pub struct LeaderConfig {
    /// Name of the `Lease` object.
    pub lease_name: String,
    /// Namespace the `Lease` lives in (the operator's own namespace).
    pub namespace: String,
    /// Unique identity of this pod (used as `holderIdentity`).
    pub identity: String,
    /// How long a lease is valid without a renewal (the takeover window).
    pub lease_duration: Duration,
    /// How often the holder renews while it is the leader.
    pub renew_interval: Duration,
    /// How often a non-holder retries to acquire.
    pub retry_interval: Duration,
}

/// Resolve this pod's identity from the environment. `POD_NAME` (downward
/// API) is the canonical per-pod identity; `HOSTNAME` is the pod name too
/// under Kubernetes; the pid fallback only matters off cluster, where
/// there is a single instance anyway.
///
/// Shared beyond leader election: `backend::StateLock::try_acquire`'s
/// `holder` argument uses the SAME identity, so a stuck advisory lock and
/// a stuck lease both name the exact same pod in their respective
/// tracking tables — one identity source, not two independently-derived
/// strings that could drift.
pub fn pod_identity() -> String {
    std::env::var("POD_NAME")
        .ok()
        .filter(|s| !s.is_empty())
        .or_else(|| std::env::var("HOSTNAME").ok().filter(|s| !s.is_empty()))
        .unwrap_or_else(|| format!("pangea-operator-{}", std::process::id()))
}

impl LeaderConfig {
    /// Build from the environment. `POD_NAMESPACE` / `POD_NAME` come from the
    /// downward API (wired in the chart); both fall back sensibly so the
    /// operator still elects when run outside that wiring (e.g. local).
    pub fn from_env() -> Self {
        let namespace = std::env::var("POD_NAMESPACE")
            .ok()
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| "pangea-system".to_string());

        Self {
            lease_name: std::env::var("LEADER_LEASE_NAME")
                .ok()
                .filter(|s| !s.is_empty())
                .unwrap_or_else(|| "pangea-operator-leader".to_string()),
            namespace,
            identity: pod_identity(),
            lease_duration: Duration::from_secs(env_secs("LEADER_LEASE_DURATION_SECS", 15)),
            renew_interval: Duration::from_secs(env_secs("LEADER_RENEW_SECS", 5)),
            retry_interval: Duration::from_secs(env_secs("LEADER_RETRY_SECS", 3)),
        }
    }
}

fn env_secs(key: &str, default: u64) -> u64 {
    std::env::var(key)
        .ok()
        .and_then(|s| s.parse().ok())
        .filter(|n| *n > 0)
        .unwrap_or(default)
}

/// Outcome of an [`LeaderElector::acquire`] attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Leadership {
    /// We hold the lease and should start (and keep renewing) the controllers.
    Acquired,
    /// The Lease API is unusable (e.g. RBAC for `coordination.k8s.io/leases`
    /// has not been granted yet). The caller falls back to running as a
    /// singleton — preserving the pre-leader-election behavior so a chart vs.
    /// operator-image deploy-ordering skew can never brick the operator.
    Unavailable,
}

/// The leadership decision for a single tick, given the current lease state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LeaseDecision {
    /// We are (or may become) the holder — write our identity + `renewTime`.
    Acquire,
    /// Someone else holds a still-valid lease — wait and retry.
    Wait,
}

/// Pure decision: may `identity` acquire or keep the lease right now?
///
/// * `holder`             — current `holderIdentity` (None / empty == unheld).
/// * `renew_time`         — the holder's last `renewTime`.
/// * `lease_duration_secs`— the lease's recorded `leaseDurationSeconds`.
/// * `now`                — the current clock.
///
/// A lease held by *another* identity is only taken over once it has expired
/// (`now - renewTime > leaseDuration`), or if it carries no `renewTime` at all
/// (a malformed / freshly-created-but-never-renewed record).
pub fn decide(
    identity: &str,
    holder: Option<&str>,
    renew_time: Option<DateTime<Utc>>,
    lease_duration_secs: i32,
    now: DateTime<Utc>,
) -> LeaseDecision {
    match holder {
        None => LeaseDecision::Acquire,
        Some(h) if h.is_empty() => LeaseDecision::Acquire,
        Some(h) if h == identity => LeaseDecision::Acquire,
        Some(_) => {
            let expired = match renew_time {
                None => true,
                Some(rt) => {
                    now.signed_duration_since(rt)
                        > chrono::Duration::seconds(lease_duration_secs.max(0) as i64)
                }
            };
            if expired {
                LeaseDecision::Acquire
            } else {
                LeaseDecision::Wait
            }
        }
    }
}

/// A lease-backed leader elector.
#[derive(Clone)]
pub struct LeaderElector {
    api: Api<Lease>,
    cfg: LeaderConfig,
}

impl LeaderElector {
    /// Construct an elector scoped to the lease's namespace.
    pub fn new(client: Client, cfg: LeaderConfig) -> Self {
        let api = Api::<Lease>::namespaced(client, &cfg.namespace);
        Self { api, cfg }
    }

    /// This pod's holder identity.
    pub fn identity(&self) -> &str {
        &self.cfg.identity
    }

    /// The lease object's name.
    pub fn lease_name(&self) -> &str {
        &self.cfg.lease_name
    }

    /// Block until this pod becomes the leader, retrying every
    /// `retry_interval`. Returns [`Leadership::Acquired`] once the lease is
    /// held. Transient API errors are logged and retried (a flaky apiserver
    /// must not crash the operator). A `403 Forbidden` is treated as terminal
    /// for election — the Lease RBAC is absent — and returns
    /// [`Leadership::Unavailable`] so the caller can fall back to a singleton.
    pub async fn acquire(&self) -> Leadership {
        loop {
            match self.try_acquire_or_renew().await {
                Ok(true) => {
                    info!(
                        identity = %self.cfg.identity,
                        lease = %self.cfg.lease_name,
                        "Acquired leadership"
                    );
                    return Leadership::Acquired;
                }
                Ok(false) => {
                    debug!(
                        identity = %self.cfg.identity,
                        "Not leader yet — another holder is active"
                    );
                }
                Err(Error::Kube(kube::Error::Api(ref ae))) if ae.code == 403 => {
                    warn!(
                        code = ae.code,
                        "Lease API forbidden — leader election unavailable (coordination.k8s.io \
                         RBAC not granted?); falling back to running as a singleton"
                    );
                    return Leadership::Unavailable;
                }
                Err(e) => {
                    warn!(error = %e, "Leader election acquire attempt failed — retrying");
                }
            }
            tokio::time::sleep(self.cfg.retry_interval).await;
        }
    }

    /// Renew the lease for as long as we remain the leader. Returns when
    /// leadership is lost (another pod took over) — the caller treats that as a
    /// signal to shut down so a single reconciler is preserved.
    pub async fn keep_renewed(&self) {
        loop {
            tokio::time::sleep(self.cfg.renew_interval).await;
            match self.try_acquire_or_renew().await {
                Ok(true) => {
                    debug!(identity = %self.cfg.identity, "Renewed leadership lease");
                }
                Ok(false) => {
                    warn!(
                        identity = %self.cfg.identity,
                        lease = %self.cfg.lease_name,
                        "Lost leadership — another pod holds the lease; stepping down"
                    );
                    return;
                }
                Err(e) => {
                    // A single failed renew is not fatal — keep trying until the
                    // lease would actually expire. We err toward staying leader
                    // (the alternative, stepping down on one blip, risks a no-op
                    // gap with nobody reconciling).
                    warn!(error = %e, "Lease renewal attempt failed — will retry");
                }
            }
        }
    }

    /// One election tick. Returns `Ok(true)` if we hold the lease afterward,
    /// `Ok(false)` if another pod holds a still-valid lease.
    async fn try_acquire_or_renew(&self) -> Result<bool> {
        let now = Utc::now();
        let name = &self.cfg.lease_name;

        // 1. Read the current lease, or create it held by us if absent.
        let existing = match self.api.get_opt(name).await? {
            Some(l) => l,
            None => {
                let fresh = self.build_lease(None, now, now, 0);
                match self.api.create(&PostParams::default(), &fresh).await {
                    Ok(_) => return Ok(true), // we created it ⇒ we hold it
                    // Another pod created it first — fall through to next tick.
                    Err(kube::Error::Api(ae)) if ae.code == 409 => return Ok(false),
                    Err(e) => return Err(e.into()),
                }
            }
        };

        let spec = existing.spec.clone().unwrap_or_default();
        let holder = spec.holder_identity.as_deref();
        let renew_time = spec.renew_time.as_ref().map(|t| t.0);
        let duration_secs = spec
            .lease_duration_seconds
            .unwrap_or(self.cfg.lease_duration.as_secs() as i32);

        match decide(&self.cfg.identity, holder, renew_time, duration_secs, now) {
            LeaseDecision::Wait => Ok(false),
            LeaseDecision::Acquire => {
                let held_by_us = holder == Some(self.cfg.identity.as_str());
                // Preserve acquireTime + transition count when we already hold
                // it; bump both on a takeover (a leadership transition).
                let acquire_time = if held_by_us {
                    spec.acquire_time.as_ref().map(|t| t.0).unwrap_or(now)
                } else {
                    now
                };
                let transitions = spec.lease_transitions.unwrap_or(0)
                    + if held_by_us { 0 } else { 1 };

                // Compare-and-set on resourceVersion: replace() carries the
                // version from `existing`, so a competing write since our read
                // yields 409 and we retry on the next tick.
                let updated = self.build_lease_from(
                    &existing,
                    acquire_time,
                    now,
                    transitions,
                );
                match self.api.replace(name, &PostParams::default(), &updated).await {
                    Ok(_) => Ok(true),
                    Err(kube::Error::Api(ae)) if ae.code == 409 => Ok(false),
                    Err(e) => Err(e.into()),
                }
            }
        }
    }

    /// Build a brand-new Lease object held by us (for the create path).
    fn build_lease(
        &self,
        resource_version: Option<String>,
        acquire_time: DateTime<Utc>,
        renew_time: DateTime<Utc>,
        transitions: i32,
    ) -> Lease {
        Lease {
            metadata: ObjectMeta {
                name: Some(self.cfg.lease_name.clone()),
                namespace: Some(self.cfg.namespace.clone()),
                resource_version,
                ..Default::default()
            },
            spec: Some(LeaseSpec {
                holder_identity: Some(self.cfg.identity.clone()),
                lease_duration_seconds: Some(self.cfg.lease_duration.as_secs() as i32),
                acquire_time: Some(MicroTime(acquire_time)),
                renew_time: Some(MicroTime(renew_time)),
                lease_transitions: Some(transitions),
                ..Default::default()
            }),
        }
    }

    /// Rebuild an existing Lease (keeping its `resourceVersion` for CAS) with
    /// us as holder and a fresh renew time.
    fn build_lease_from(
        &self,
        existing: &Lease,
        acquire_time: DateTime<Utc>,
        renew_time: DateTime<Utc>,
        transitions: i32,
    ) -> Lease {
        self.build_lease(
            existing.metadata.resource_version.clone(),
            acquire_time,
            renew_time,
            transitions,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn t(secs: i64) -> DateTime<Utc> {
        DateTime::<Utc>::from_timestamp(1_700_000_000 + secs, 0).unwrap()
    }

    #[test]
    fn unheld_lease_is_acquired() {
        assert_eq!(decide("me", None, None, 15, t(0)), LeaseDecision::Acquire);
        assert_eq!(
            decide("me", Some(""), None, 15, t(0)),
            LeaseDecision::Acquire
        );
    }

    #[test]
    fn self_held_lease_is_renewed() {
        // We hold it and renewed 3s ago — keep it (Acquire == renew).
        assert_eq!(
            decide("me", Some("me"), Some(t(0)), 15, t(3)),
            LeaseDecision::Acquire
        );
    }

    #[test]
    fn other_held_fresh_lease_waits() {
        // Someone else renewed 5s ago, 15s duration — still valid → wait.
        assert_eq!(
            decide("me", Some("other"), Some(t(0)), 15, t(5)),
            LeaseDecision::Wait
        );
    }

    #[test]
    fn other_held_expired_lease_is_taken_over() {
        // Someone else renewed 20s ago, 15s duration → expired → take over.
        assert_eq!(
            decide("me", Some("other"), Some(t(0)), 15, t(20)),
            LeaseDecision::Acquire
        );
    }

    #[test]
    fn other_held_lease_at_exact_boundary_still_waits() {
        // age == duration is NOT yet expired (strict >).
        assert_eq!(
            decide("me", Some("other"), Some(t(0)), 15, t(15)),
            LeaseDecision::Wait
        );
        // one second past the boundary → expired.
        assert_eq!(
            decide("me", Some("other"), Some(t(0)), 15, t(16)),
            LeaseDecision::Acquire
        );
    }

    #[test]
    fn other_held_without_renew_time_is_taken_over() {
        // A holder set but no renewTime recorded → treat as expired.
        assert_eq!(
            decide("me", Some("other"), None, 15, t(100)),
            LeaseDecision::Acquire
        );
    }
}
