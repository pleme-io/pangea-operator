//! The typed apply-anomaly taxonomy + classifier — the reacting FSM's
//! **detection border** (theory/ANOMALY-REACTIVE-RECONCILE.md §IV, §VII.2).
//!
//! # Why this module exists
//!
//! A reconciler does not live in the clean world its happy-path code
//! assumes. Providers are missing, objects exist out-of-band, APIs
//! rate-limit, networks blip. **The anomaly is the expected operating
//! mode, not the exception.** The model says: model the whole service as
//! one reacting FSM whose normal job is to detect and remediate anomalies
//! until the declared state holds — and **error handling IS the detector**.
//!
//! magma's apply engine is infallible: per-resource failures land in
//! `outcome.failed: Vec<FailedChange>`, each carrying a free-form
//! `reason: String` (the formatted `EngineError`). The wire the operator
//! classifies on is therefore the *substring* of that string. This module
//! turns each `reason` into a member of a **closed, exhaustive** typed sum
//! ([`ApplyAnomaly`]) via [`classify`]. A failure no arm matches is itself a
//! typed [`ApplyAnomaly::Unclassified`] — surfaced, never retried-forever —
//! and every `Unclassified` occurrence is a forcing function to add the
//! missing arm (§II, §X).
//!
//! # The single classifier (folds the scattered substring checks)
//!
//! Before this module the operator had **four** scattered substring
//! classifiers — `is_self_healable_apply_error` (stale-plan),
//! `is_stale_plan`/`is_empty_workspace` (tc.rs), `conflict::classify_kind`
//! (already-exists). This module is the one typed taxonomy they fold into;
//! the substrings here mirror magma's own `is_already_exists`
//! (engine.rs:814-821) and `ProviderLocateError::Display`
//! (magma-providers lib.rs:15-22) so the two stay coherent.
//!
//! # Decoupled from the magma types on purpose
//!
//! [`classify`] takes `&str` reason + address (not `magma_apply::FailedChange`)
//! so it is unit-testable with the **real reason strings** without linking
//! magma, and so the same taxonomy applies whether the failure arrived as a
//! structured `FailedChange` (magma path) or as a stdout/stderr blob (tofu
//! path). The executor adapts its `FailedChange`s into
//! [`FailedChangeRecord`]s ([`crate::executor::tofu::FailedChangeRecord`])
//! at the executor border; this module classifies those records.

/// The convergence **mode** of a remediation — the Lyapunov settle-bound
/// made explicit (theory/ANOMALY-REACTIVE-RECONCILE.md §IV, §VI).
///
/// Total open-anomaly weight is a Lyapunov function: non-increasing always,
/// strictly decreasing while any non-`Hold` anomaly is open. Each anomaly's
/// remediation declares which kind of decrease it guarantees.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RemediationMode {
    /// Effectiveness → 0 in **one tick**. The anomaly is gone next tick
    /// (e.g. `StageProvider` once the durable `MAGMA_PROVIDER_DIR` is baked;
    /// `Import` adoption of an out-of-band object). The strongest tier.
    Absolute,
    /// Effectiveness strictly, **monotonically → 0 over ticks** — bounded
    /// exponential backoff / samba-paced retry. A non-strictly-decreasing
    /// "decaying" remediation is a *livelock*, forbidden by the model.
    Decaying,
    /// A typed escalation that has **surfaced** (Warning event +
    /// `Healthy=False`) and **stopped consuming the worker on a tight loop**.
    /// Never a silent infinite retry. The `Unclassified` / `PermissionDenied`
    /// terminal — it does not converge on its own; it waits for an operator
    /// or a new taxonomy arm. This is the C1 ceiling (liveness, not a
    /// compile-proof) — §IX.
    Hold,
}

impl RemediationMode {
    /// Stable label for tracing + events + metrics. Pick once, never change.
    pub fn as_str(self) -> &'static str {
        match self {
            RemediationMode::Absolute => "Absolute",
            RemediationMode::Decaying => "Decaying",
            RemediationMode::Hold => "Hold",
        }
    }
}

/// The typed apply-anomaly sum — one exhaustive taxonomy over every failure
/// class the apply stage can detect (theory/ANOMALY-REACTIVE-RECONCILE.md
/// §IV). **Closed enum on purpose** (NOT `#[non_exhaustive]`): a new provider
/// failure mode that isn't classified is an [`ApplyAnomaly::Unclassified`]
/// value — a runtime forcing function — and adding an arm here is a
/// compile-time prompt to wire its remediation in the exhaustive `match`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ApplyAnomaly {
    /// The provider plugin binary couldn't be located. Live rio anomaly A:
    /// magma's `EngineError::Locate` → `ProviderLocateError` (no
    /// `.terraform/providers` dir, or binary missing under a root).
    /// Remediation: `StageProvider` — providers now resolve from the durable
    /// baked `MAGMA_PROVIDER_DIR` (flake.nix), so this is Absolute once the
    /// image carries the mirror.
    ProviderUnavailable { provider: String },
    /// A `Create` collided with an object that already exists out-of-band
    /// (409 / 422 / "already exists"). Live rio anomaly B: the
    /// grafana.quero.cloud cloudflare_dns_record created via the CF API.
    /// Remediation: `Import` — adopt the resource into state, reclassify
    /// `Create → adopted`.
    ObjectExistsUntracked { address: String, action: String },
    /// The provider returned (or magma's retry exhausted on) a rate limit
    /// (429 / secondary-limit). Remediation: samba-paced / backoff `Retry`
    /// (Decaying).
    RateLimited,
    /// A transient network fault (dial / timeout / connection refused /
    /// spawn). Remediation: bounded exponential backoff `Retry` (Decaying).
    TransientNetwork,
    /// OpenTofu refused a cached `-out` plan because state moved since plan
    /// (`Saved plan is stale`). Self-healable within one reconcile: drop the
    /// cached plan + replan. (tofu-path; folded in so there is ONE taxonomy.)
    StalePlan,
    /// The workspace has no rendered config (`No configuration files` /
    /// `Apply requires configuration to be present`) — usually a pod restart
    /// mid-self-heal that wiped `main.tf.json`. Recovery: clean + bounce to
    /// Pending to re-render. (tofu-path.)
    EmptyWorkspace,
    /// The provider denied the operation (403 / AccessDenied / not an admin).
    /// Remediation: `SurfaceAndHold` — never silently retried; an operator
    /// (or alçada lockdown intent) must act.
    PermissionDenied,
    /// No arm matched. **Typed, surfaced, never silent** — the backlog
    /// signal that forces a new arm (§II, §X). Carries the raw reason so the
    /// operator sees exactly what to classify next.
    Unclassified { reason: String },
}

impl ApplyAnomaly {
    /// The convergence mode of THIS anomaly's remediation — the Lyapunov
    /// tier (§IV). Recorded into events/status so the settle-bound is
    /// explicit per anomaly (Part 4 of the build contract).
    pub fn mode(&self) -> RemediationMode {
        match self {
            ApplyAnomaly::ProviderUnavailable { .. } => RemediationMode::Absolute,
            ApplyAnomaly::ObjectExistsUntracked { .. } => RemediationMode::Absolute,
            ApplyAnomaly::StalePlan => RemediationMode::Absolute,
            ApplyAnomaly::EmptyWorkspace => RemediationMode::Absolute,
            ApplyAnomaly::RateLimited => RemediationMode::Decaying,
            ApplyAnomaly::TransientNetwork => RemediationMode::Decaying,
            ApplyAnomaly::PermissionDenied => RemediationMode::Hold,
            ApplyAnomaly::Unclassified { .. } => RemediationMode::Hold,
        }
    }

    /// Stable discriminant label for tracing / metrics / event reasons.
    /// Decoupled from `Debug` so dashboards don't break on a field change.
    pub fn kind_str(&self) -> &'static str {
        match self {
            ApplyAnomaly::ProviderUnavailable { .. } => "ProviderUnavailable",
            ApplyAnomaly::ObjectExistsUntracked { .. } => "ObjectExistsUntracked",
            ApplyAnomaly::RateLimited => "RateLimited",
            ApplyAnomaly::TransientNetwork => "TransientNetwork",
            ApplyAnomaly::StalePlan => "StalePlan",
            ApplyAnomaly::EmptyWorkspace => "EmptyWorkspace",
            ApplyAnomaly::PermissionDenied => "PermissionDenied",
            ApplyAnomaly::Unclassified { .. } => "Unclassified",
        }
    }

    /// A stable K8s Event reason for this anomaly's surfaced remediation.
    pub fn event_reason(&self) -> &'static str {
        match self {
            ApplyAnomaly::ProviderUnavailable { .. } => "AnomalyProviderUnavailable",
            ApplyAnomaly::ObjectExistsUntracked { .. } => "AnomalyObjectExistsUntracked",
            ApplyAnomaly::RateLimited => "AnomalyRateLimited",
            ApplyAnomaly::TransientNetwork => "AnomalyTransientNetwork",
            ApplyAnomaly::StalePlan => "AnomalyStalePlan",
            ApplyAnomaly::EmptyWorkspace => "AnomalyEmptyWorkspace",
            ApplyAnomaly::PermissionDenied => "AnomalyPermissionDenied",
            ApplyAnomaly::Unclassified { .. } => "AnomalyUnclassified",
        }
    }
}

/// Classify ONE apply failure into a typed [`ApplyAnomaly`].
///
/// `reason` is the free-form failure string (magma's `FailedChange.reason` =
/// formatted `EngineError`, or a tofu stdout/stderr block). `address` is the
/// resource address (`cloudflare_dns_record.foo`) for the anomalies that
/// carry it; `action` is the raw action grammar (`create`/`update`/…).
///
/// Substrings mirror the real wire (investigation `reason_strings`) +
/// magma's own classifiers so the two stay coherent. Order matters: the
/// most specific / most actionable arms are checked first.
pub fn classify(reason: &str, address: &str, action: &str) -> ApplyAnomaly {
    let r = reason.to_ascii_lowercase();

    // ── ProviderUnavailable (live rio anomaly A) ──────────────────────
    // magma: EngineError::Locate("locate provider \"<name>\": <inner>")
    //   inner = "no .terraform/providers dir under workspace … run `init` first"
    //         | "provider binary for \"<name>\" not found under <root>"
    if r.contains("locate provider")
        || r.contains("no .terraform/providers")
        || r.contains("run `init` first")
        || r.contains("provider binary for")
    {
        return ApplyAnomaly::ProviderUnavailable {
            provider: provider_from_address(address)
                .or_else(|| provider_from_locate_reason(reason))
                .unwrap_or_else(|| "unknown".to_string()),
        };
    }

    // ── EmptyWorkspace (tofu-path; check before the conflict arms so an
    // "Apply requires configuration" message can't be mis-routed) ──────
    if r.contains("no configuration files")
        || r.contains("apply requires configuration to be present")
    {
        return ApplyAnomaly::EmptyWorkspace;
    }

    // ── StalePlan (tofu-path) ─────────────────────────────────────────
    if r.contains("saved plan is stale") || r.contains("plan is stale") {
        return ApplyAnomaly::StalePlan;
    }

    // ── PermissionDenied (check before rate-limit: a 403 can mention
    // "limit" in unrelated text, but auth denial is the harder hold) ───
    if r.contains("403")
        || r.contains("accessdenied")
        || r.contains("access denied")
        || r.contains("permission denied")
        || r.contains("forbidden")
        || r.contains("not an admin")
        || r.contains("must have admin")
    {
        return ApplyAnomaly::PermissionDenied;
    }

    // ── RateLimited (Decaying) ────────────────────────────────────────
    // magma's rpc_retry! exhausts on secondary limits → a plain Rpc string;
    // there's no guaranteed 429 token, so match the common shapes.
    if r.contains("429")
        || r.contains("rate limit")
        || r.contains("rate-limit")
        || r.contains("ratelimit")
        || r.contains("secondary")
        || r.contains("too many requests")
        || r.contains("throttl")
    {
        return ApplyAnomaly::RateLimited;
    }

    // ── ObjectExistsUntracked (live rio anomaly B) ────────────────────
    // Mirror magma's is_already_exists (engine.rs:814-821) + the operator's
    // conflict::classify_kind (conflict.rs:96-107) substrings EXACTLY.
    if r.contains("already exists")
        || r.contains("already been")
        || r.contains("already been taken")
        || r.contains("name_already_in_use")
        || r.contains("name already")
        || r.contains("already protected")
        || r.contains("protection already exists")
        || r.contains("422")
        || r.contains("409")
    {
        return ApplyAnomaly::ObjectExistsUntracked {
            address: address.to_string(),
            action: action.to_string(),
        };
    }

    // ── TransientNetwork (Decaying) ───────────────────────────────────
    // magma: EngineError::Spawn("spawn provider …: <msg>") or a dial Rpc.
    if r.contains("dial")
        || r.contains("timeout")
        || r.contains("timed out")
        || r.contains("connection refused")
        || r.contains("connection reset")
        || r.contains("spawn provider")
        || r.contains("no such host")
        || r.contains("i/o timeout")
        || r.contains("temporary failure")
        || r.contains("eof")
    {
        return ApplyAnomaly::TransientNetwork;
    }

    // ── Unclassified — typed, surfaced, never silent (the backlog) ────
    ApplyAnomaly::Unclassified {
        reason: reason.to_string(),
    }
}

/// Recover the provider local name from a resource address
/// (`cloudflare_dns_record.foo` → `cloudflare`). Mirrors magma's
/// `provider_local_name` (engine.rs:984 = `type_id.split('_').next()`).
fn provider_from_address(address: &str) -> Option<String> {
    let ty = address.split('.').next().unwrap_or(address);
    let first = ty.split('_').next().unwrap_or(ty);
    if first.is_empty() {
        None
    } else {
        Some(first.to_string())
    }
}

/// Recover the provider name from a `locate provider "<name>": …` reason
/// when no address is available.
fn provider_from_locate_reason(reason: &str) -> Option<String> {
    // EngineError::Locate renders `locate provider "<name>": <inner>` —
    // the `{0:?}` debug-quotes the name. Pull the first quoted token.
    let start = reason.find('"')?;
    let rest = &reason[start + 1..];
    let end = rest.find('"')?;
    let name = &rest[..end];
    if name.is_empty() {
        None
    } else {
        Some(name.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── ProviderUnavailable (live rio anomaly A) ──────────────────────

    #[test]
    fn classifies_the_verbatim_live_rio_provider_locate_error() {
        // The verbatim live rio failure string.
        let reason = "locate provider \"cloudflare\": no .terraform/providers \
                      dir under workspace \"/var/pangea/workspaces/rio\" — run `init` first";
        let a = classify(reason, "cloudflare_dns_record.rio-grafana-quero-cloud-cname", "create");
        assert_eq!(
            a,
            ApplyAnomaly::ProviderUnavailable { provider: "cloudflare".into() }
        );
        assert_eq!(a.mode(), RemediationMode::Absolute);
        assert_eq!(a.kind_str(), "ProviderUnavailable");
    }

    #[test]
    fn classifies_provider_binary_missing_under_root() {
        let reason = "locate provider \"github\": provider binary for \"github\" \
                      not found under \"/nix/store/…/terraform-providers\"";
        let a = classify(reason, "github_repository.galho", "create");
        assert_eq!(
            a,
            ApplyAnomaly::ProviderUnavailable { provider: "github".into() }
        );
    }

    #[test]
    fn provider_name_recovered_from_address_when_reason_has_no_quote() {
        let a = classify(
            "no .terraform/providers dir",
            "aws_iam_role.bar",
            "create",
        );
        assert_eq!(
            a,
            ApplyAnomaly::ProviderUnavailable { provider: "aws".into() }
        );
    }

    // ── ObjectExistsUntracked (live rio anomaly B) ────────────────────

    #[test]
    fn classifies_github_422_name_already_exists() {
        // Real observed string (conflict.rs test line 412).
        let reason = "provider \"github\" RPC: POST https://api.github.com/orgs/pleme-io/repos: \
                      422 Validation Failed [{Resource:Repository Field:name Code:custom \
                      Message:name already exists on this account}]";
        let a = classify(reason, "github_repository.galho", "create");
        assert_eq!(
            a,
            ApplyAnomaly::ObjectExistsUntracked {
                address: "github_repository.galho".into(),
                action: "create".into()
            }
        );
        assert_eq!(a.mode(), RemediationMode::Absolute);
    }

    #[test]
    fn classifies_cloudflare_409_already_exists() {
        let reason = "provider \"cloudflare\" RPC: 409 Conflict: record already exists";
        let a = classify(
            reason,
            "cloudflare_dns_record.rio-grafana-quero-cloud-cname",
            "create",
        );
        assert_eq!(
            a,
            ApplyAnomaly::ObjectExistsUntracked {
                address: "cloudflare_dns_record.rio-grafana-quero-cloud-cname".into(),
                action: "create".into()
            }
        );
    }

    #[test]
    fn classifies_name_already_in_use_token() {
        let a = classify("error: NAME_ALREADY_IN_USE", "x_y.z", "create");
        assert!(matches!(a, ApplyAnomaly::ObjectExistsUntracked { .. }));
    }

    #[test]
    fn classifies_already_protected_branch() {
        let a = classify(
            "branch protection already exists on main",
            "github_branch_protection.main",
            "create",
        );
        assert!(matches!(a, ApplyAnomaly::ObjectExistsUntracked { .. }));
    }

    // ── RateLimited (Decaying) ────────────────────────────────────────

    #[test]
    fn classifies_429_rate_limit() {
        let a = classify("HTTP 429: API rate limit exceeded", "github_repository.a", "create");
        assert_eq!(a, ApplyAnomaly::RateLimited);
        assert_eq!(a.mode(), RemediationMode::Decaying);
    }

    #[test]
    fn classifies_secondary_rate_limit() {
        let a = classify(
            "You have exceeded a secondary rate limit",
            "github_repository.a",
            "create",
        );
        assert_eq!(a, ApplyAnomaly::RateLimited);
    }

    // ── TransientNetwork (Decaying) ───────────────────────────────────

    #[test]
    fn classifies_dial_timeout() {
        let a = classify(
            "spawn provider \"aws\": dial tcp: i/o timeout",
            "aws_vpc.main",
            "create",
        );
        // spawn provider + dial + i/o timeout all point to TransientNetwork.
        assert_eq!(a, ApplyAnomaly::TransientNetwork);
        assert_eq!(a.mode(), RemediationMode::Decaying);
    }

    #[test]
    fn classifies_connection_refused() {
        let a = classify("connection refused", "aws_vpc.main", "create");
        assert_eq!(a, ApplyAnomaly::TransientNetwork);
    }

    // ── PermissionDenied (Hold) ───────────────────────────────────────

    #[test]
    fn classifies_403_access_denied() {
        let a = classify(
            "provider \"aws\" RPC: AccessDenied: 403 not authorized",
            "aws_s3_bucket.x",
            "create",
        );
        assert_eq!(a, ApplyAnomaly::PermissionDenied);
        assert_eq!(a.mode(), RemediationMode::Hold);
    }

    #[test]
    fn classifies_not_an_admin_org_push() {
        let a = classify(
            "denied: you must have admin rights / not an admin of this org",
            "github_repository.x",
            "create",
        );
        assert_eq!(a, ApplyAnomaly::PermissionDenied);
    }

    // ── StalePlan / EmptyWorkspace (tofu-path, folded in) ─────────────

    #[test]
    fn classifies_stale_plan() {
        let a = classify(
            "Error: Saved plan is stale\nThe given plan file can no longer be applied",
            "",
            "",
        );
        assert_eq!(a, ApplyAnomaly::StalePlan);
        assert_eq!(a.mode(), RemediationMode::Absolute);
    }

    #[test]
    fn classifies_empty_workspace() {
        let a = classify(
            "Error: No configuration files\nApply requires configuration to be present",
            "",
            "",
        );
        assert_eq!(a, ApplyAnomaly::EmptyWorkspace);
    }

    // ── Unclassified — typed, surfaced, never silent (the backlog) ────

    #[test]
    fn unmatched_reason_is_typed_unclassified_not_silent() {
        let reason = "provider \"weird\" RPC: an entirely novel failure shape";
        let a = classify(reason, "weird_thing.x", "create");
        assert_eq!(
            a,
            ApplyAnomaly::Unclassified { reason: reason.to_string() }
        );
        // Unclassified holds (never an infinite silent retry).
        assert_eq!(a.mode(), RemediationMode::Hold);
        assert_eq!(a.event_reason(), "AnomalyUnclassified");
    }

    #[test]
    fn every_mode_label_is_stable() {
        assert_eq!(RemediationMode::Absolute.as_str(), "Absolute");
        assert_eq!(RemediationMode::Decaying.as_str(), "Decaying");
        assert_eq!(RemediationMode::Hold.as_str(), "Hold");
    }

    #[test]
    fn permission_denied_wins_over_incidental_limit_text() {
        // A 403 body that happens to mention "limit" must classify as the
        // harder Hold, not a Decaying retry.
        let a = classify(
            "403 Forbidden: your plan limit does not allow this org operation",
            "github_repository.x",
            "create",
        );
        assert_eq!(a, ApplyAnomaly::PermissionDenied);
    }
}
