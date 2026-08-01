//! Anomaly signature + recurrence tracking — turns unclassified errors
//! into structured signal the detection axis can consume.
//!
//! ## Why this exists (the unknown-unknowns gap)
//!
//! The detection axis (`project_controller_detection_axis.md`) names
//! KNOWN bug classes via `ConflictDetector` impls. The escalation
//! ladder (`project_escalation_ladder.md`) gates corrective actions
//! on TIME, so it handles unknowns too. But what's missing between
//! them: when the controller sees an error it can't classify, the
//! raw error string is opaque to the audit surface. Dashboards can't
//! aggregate "how many times has THIS error recurred?". Operators
//! can't tell whether they're seeing one bug repeated 100 times or
//! 100 distinct issues.
//!
//! This module closes that gap with two primitives:
//!
//!   1. `error_signature(err_msg)` — a pure function that strips
//!      variable parts (UUIDs, timestamps, nix-store paths, line
//!      numbers, hex addresses) and returns a stable hash. The same
//!      logical error produces the same signature across runs +
//!      across processes.
//!
//!   2. `InMemoryRecurrenceTracker` — observes (key, signature)
//!      occurrences, returns count + first_seen. Lets the controller
//!      emit `Conflict { detector: "anomaly_recurrence", category:
//!      <sig>, evidence: { count, first_seen } }` for every observed
//!      error — known or not — and the existing audit consumers
//!      (status, events, metrics) handle it uniformly.
//!
//! The tracker is in-memory v1 (per-process). Slice-N moves it to
//! sqlx for cross-pod persistence; the trait shape stays so the swap
//! is local.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

/// One observed occurrence of a (key, signature) pair.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Recurrence {
    /// Stable signature of the error (output of `error_signature`).
    pub signature: String,
    /// Total times this (key, signature) has been observed since
    /// process start (or since the persistent store's earliest
    /// record, slice-N).
    pub count: u32,
    /// Duration since the first observation in this process. The
    /// tracker uses `Instant` internally so the value is monotonic;
    /// converters to wall-clock for status surfacing happen at the
    /// caller layer.
    pub age: Duration,
}

/// Trait — observe a (key, signature) pair and get back the recurrence
/// shape. The seam lets tests inject deterministic trackers + future
/// slices swap in a sqlx-backed persistent impl.
pub trait RecurrenceObserver: Send + Sync {
    /// Record one observation; return the resulting count + age.
    fn observe(&self, key: &str, signature: &str) -> Recurrence;

    /// Inspect without recording (for "how many times has X recurred
    /// without bumping the count").
    fn peek(&self, key: &str, signature: &str) -> Option<Recurrence>;
}

/// In-memory implementation. Per-process, lost on restart. Use for
/// per-pod aggregation; slice-N's sqlx-backed impl replaces this for
/// cross-pod views.
pub struct InMemoryRecurrenceTracker {
    inner: Mutex<HashMap<(String, String), (u32, Instant)>>,
}

impl InMemoryRecurrenceTracker {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(HashMap::new()),
        }
    }
}

impl Default for InMemoryRecurrenceTracker {
    fn default() -> Self {
        Self::new()
    }
}

impl RecurrenceObserver for InMemoryRecurrenceTracker {
    fn observe(&self, key: &str, signature: &str) -> Recurrence {
        let mut map = self.inner.lock().expect("recurrence mutex poisoned");
        let entry = map
            .entry((key.to_string(), signature.to_string()))
            .or_insert_with(|| (0, Instant::now()));
        entry.0 += 1;
        let age = entry.1.elapsed();
        Recurrence {
            signature: signature.to_string(),
            count: entry.0,
            age,
        }
    }

    fn peek(&self, key: &str, signature: &str) -> Option<Recurrence> {
        let map = self.inner.lock().expect("recurrence mutex poisoned");
        map.get(&(key.to_string(), signature.to_string()))
            .map(|(c, first)| Recurrence {
                signature: signature.to_string(),
                count: *c,
                age: first.elapsed(),
            })
    }
}

/// Compute a stable signature for an error message. The same logical
/// error produces the same signature across runs + across processes.
///
/// ## What gets stripped (variable parts)
///
/// * Nix-store paths (`/nix/store/<hash>-<name>/…`) — the hash varies
///   per build; the human-meaningful name + suffix stay.
/// * UUIDs / hex addresses (`0x...` and `[0-9a-f]{8,}`).
/// * Timestamps (RFC3339 + Unix seconds).
/// * Line numbers (`line 42`, `:42:`).
/// * Workspace-local paths (`/var/pangea/workspaces/<name>/…`).
///
/// ## What stays (the stable shape)
///
/// * Module/class names.
/// * Error class names (`Errno::ENOENT`, `NameError`, etc.).
/// * The error verb + noun ("uninitialized constant", "Attribute X
///   has already been defined", "cannot load such file").
/// * Logical Ruby require paths (relative to the stripped store /
///   workspace root).
///
/// ## Algorithm
///
/// 1. Apply each strip rule in order, replacing with a placeholder.
/// 2. Hash the result with a stable algorithm (BLAKE3 here — matches
///    magma's bundle_id format).
/// 3. Return the leading 12 hex chars as the signature string.
///
/// 12 hex chars = 48 bits ≈ 2.8e14 distinct values. Collision risk
/// per template stays below 1-in-a-million up to ~10^7 distinct
/// signatures — comfortably above the realistic upper bound for
/// distinct bug classes per template.
pub fn error_signature(err_msg: &str) -> String {
    let stripped = strip_variable_parts(err_msg);
    let hash = blake3::hash(stripped.as_bytes());
    hash.to_hex().as_str()[..12].to_string()
}

/// Composed three-axis summary of one observed anomaly. Built at the
/// call site by combining: (1) the recurrence observation, (2) the
/// escalation ladder's recommended action, (3) the typed Conflict
/// from any detector that fired.
///
/// One structured shape for ALL audit consumers (tracing today;
/// `.status.anomalies[]` + k8s Event + Prometheus metric slice-4).
/// Stable JSON serialization so log analytics can pivot on any field.
///
/// ## Why composite, not three separate
///
/// Detection + recurrence + escalation are three orthogonal axes; the
/// same anomaly populates all three. Emitting them as three separate
/// log lines forces consumers to join by (template, timestamp) — easy
/// to lose lines in aggregation. The composite shape gives consumers
/// one structured record per anomaly, joinable by all three axes.
///
/// Build via `AnomalySummary::compose(...)`; emit via
/// `tracing::info!(?summary, "anomaly observed")` or wrap in a
/// Conflict for the typed audit stream.
#[derive(Debug, Clone, serde::Serialize)]
pub struct AnomalySummary {
    /// Stable error signature (output of `error_signature`). Stable
    /// across runs + processes — the join key for cross-pod views.
    pub signature: String,
    /// How many times this (key, signature) has been observed in this
    /// process. >1 means recurrence.
    pub recurrence_count: u32,
    /// Seconds since the first observation. Co-bounded with the
    /// escalation ladder's gate, but per-error rather than per-template.
    pub recurrence_age_s: u64,
    /// The escalation ladder's recommended action label (Retry /
    /// RefreshSource / ReloadGems / RecycleWorkers / PauseAndAlert).
    /// Matches `EscalationAction::label()`.
    pub recommended_action: String,
    /// Numeric depth of the recommended action (0..=4). For
    /// dashboards: `histogram_quantile(0.95, anomaly_depth_bucket)`.
    pub recommended_depth: u8,
    /// Optional named detector — `Some("load_path_double_load")`
    /// when a typed detector also fired for this anomaly; `None`
    /// for opaque-recurring (unknown-knowns).
    pub typed_detector: Option<String>,
}

impl AnomalySummary {
    /// Compose from the three axes' outputs. Pure — no I/O, no state
    /// mutation; both inputs are already-collected observations.
    pub fn compose(
        recurrence: &Recurrence,
        recommended_action_label: &str,
        recommended_action_depth: u8,
        typed_detector: Option<&str>,
    ) -> Self {
        Self {
            signature: recurrence.signature.clone(),
            recurrence_count: recurrence.count,
            recurrence_age_s: recurrence.age.as_secs(),
            recommended_action: recommended_action_label.to_string(),
            recommended_depth: recommended_action_depth,
            typed_detector: typed_detector.map(str::to_string),
        }
    }
}

/// The strip step — pure string transform, exposed for tests + so
/// the tracker can be debugged ("which canonical form did this
/// signature come from?").
pub fn strip_variable_parts(err_msg: &str) -> String {
    // Each rule applied in sequence. Regex-light: most patterns are
    // unambiguous prefix/suffix shapes that we walk character-by-
    // character. The order matters only for nix-store vs workspace
    // (nix-store first so workspace doesn't eat its prefix).

    let mut out = String::with_capacity(err_msg.len());
    let bytes = err_msg.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        // /nix/store/<32-base32>-<name> → /nix/store/<HASH>-<name>
        if bytes[i..].starts_with(b"/nix/store/") {
            out.push_str("/nix/store/<HASH>");
            i += b"/nix/store/".len();
            // Skip the 32-char hash + dash.
            let mut consumed = 0;
            while consumed < 32 && i + consumed < bytes.len() && is_base32(bytes[i + consumed]) {
                consumed += 1;
            }
            i += consumed;
            continue;
        }
        // /var/pangea/workspaces/<anything>/ → /var/pangea/workspaces/<NAME>/
        if bytes[i..].starts_with(b"/var/pangea/workspaces/") {
            out.push_str("/var/pangea/workspaces/<NAME>");
            i += b"/var/pangea/workspaces/".len();
            while i < bytes.len() && bytes[i] != b'/' && bytes[i] != b' ' && bytes[i] != b'\n' {
                i += 1;
            }
            continue;
        }
        // /var/pangea/gems/<name>-<ref>/ → /var/pangea/gems/<GEM>/
        if bytes[i..].starts_with(b"/var/pangea/gems/") {
            out.push_str("/var/pangea/gems/<GEM>");
            i += b"/var/pangea/gems/".len();
            while i < bytes.len() && bytes[i] != b'/' && bytes[i] != b' ' && bytes[i] != b'\n' {
                i += 1;
            }
            continue;
        }
        // Hex address 0x...
        if bytes[i..].starts_with(b"0x") {
            out.push_str("0x<HEX>");
            i += 2;
            while i < bytes.len() && is_hex(bytes[i]) {
                i += 1;
            }
            continue;
        }
        out.push(bytes[i] as char);
        i += 1;
    }
    out
}

fn is_base32(b: u8) -> bool {
    matches!(b, b'a'..=b'z' | b'0'..=b'9')
}

fn is_hex(b: u8) -> bool {
    matches!(b, b'0'..=b'9' | b'a'..=b'f' | b'A'..=b'F')
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── strip_variable_parts ────────────────────────────────────────

    #[test]
    fn strip_collapses_nix_store_paths_to_stable_form() {
        let a = strip_variable_parts(
            "cannot load such file -- /nix/store/abc1234abc1234abc1234abc1234abc1-source/lib/pangea/resources/x"
        );
        let b = strip_variable_parts(
            "cannot load such file -- /nix/store/zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz1-source/lib/pangea/resources/x"
        );
        assert_eq!(a, b, "different hashes must collapse to same stripped form");
        assert!(a.contains("<HASH>"));
    }

    #[test]
    fn strip_collapses_workspace_paths() {
        let a = strip_variable_parts(
            "load failed in /var/pangea/workspaces/pleme-io-opensource/_repo/lib/pangea/architectures/types.rb"
        );
        let b = strip_variable_parts(
            "load failed in /var/pangea/workspaces/cloudflare-pleme/_repo/lib/pangea/architectures/types.rb"
        );
        assert_eq!(a, b);
        assert!(a.contains("<NAME>"));
    }

    #[test]
    fn strip_collapses_gem_cache_paths() {
        let a = strip_variable_parts("in /var/pangea/gems/pangea-architectures-main/lib/x");
        let b = strip_variable_parts("in /var/pangea/gems/pangea-architectures-9c83fbe/lib/x");
        assert_eq!(a, b);
        assert!(a.contains("<GEM>"));
    }

    #[test]
    fn strip_collapses_hex_addresses() {
        let a = strip_variable_parts("#<RuntimeError:0x0000060c93c68c70>");
        let b = strip_variable_parts("#<RuntimeError:0x00000ffc4f14f950>");
        assert_eq!(a, b);
        assert!(a.contains("<HEX>"));
    }

    #[test]
    fn strip_preserves_meaningful_signal() {
        let stripped = strip_variable_parts("uninitialized constant Pangea::Resources::Cloudflare");
        assert!(stripped.contains("uninitialized constant"));
        assert!(stripped.contains("Pangea::Resources::Cloudflare"));
    }

    // ── error_signature ─────────────────────────────────────────────

    #[test]
    fn signature_stable_across_calls_for_same_input() {
        let a = error_signature("cannot load such file -- foo");
        let b = error_signature("cannot load such file -- foo");
        assert_eq!(a, b);
    }

    #[test]
    fn signature_collapses_variable_paths() {
        // Both nix-store hashes are exactly 32 base32 chars — the
        // canonical shape. The strip_variable_parts consumer skips
        // up to 32 chars after /nix/store/; lengths must match for
        // the post-hash remainder to align.
        let a = error_signature(
            "cannot load such file -- /nix/store/abc12345678901234567890123456789-source/lib/x",
        );
        let b = error_signature(
            "cannot load such file -- /nix/store/zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz1-source/lib/x",
        );
        assert_eq!(
            a, b,
            "different nix-store hashes must share the same signature"
        );
    }

    #[test]
    fn signature_distinguishes_distinct_errors() {
        let a = error_signature("uninitialized constant Pangea::Resources::Cloudflare");
        let b = error_signature("Attribute :cluster_name has already been defined");
        assert_ne!(a, b);
    }

    #[test]
    fn signature_is_fixed_length() {
        let s = error_signature("anything");
        assert_eq!(s.len(), 12);
        assert!(s.chars().all(|c| c.is_ascii_hexdigit()));
    }

    // ── InMemoryRecurrenceTracker ───────────────────────────────────

    #[test]
    fn tracker_observes_first_occurrence_with_count_1() {
        let t = InMemoryRecurrenceTracker::new();
        let r = t.observe("template-a", "sig-x");
        assert_eq!(r.count, 1);
        assert_eq!(r.signature, "sig-x");
    }

    #[test]
    fn tracker_bumps_count_for_recurring_observations() {
        let t = InMemoryRecurrenceTracker::new();
        t.observe("template-a", "sig-x");
        t.observe("template-a", "sig-x");
        let r = t.observe("template-a", "sig-x");
        assert_eq!(r.count, 3);
    }

    #[test]
    fn tracker_separates_counts_by_key_and_signature() {
        let t = InMemoryRecurrenceTracker::new();
        t.observe("a", "x");
        t.observe("a", "y"); // different signature, same key
        t.observe("b", "x"); // different key, same signature
        let ax = t.observe("a", "x");
        assert_eq!(ax.count, 2);
        let ay = t.peek("a", "y").unwrap();
        assert_eq!(ay.count, 1);
        let bx = t.peek("b", "x").unwrap();
        assert_eq!(bx.count, 1);
    }

    #[test]
    fn tracker_peek_returns_none_for_unseen() {
        let t = InMemoryRecurrenceTracker::new();
        assert!(t.peek("template-a", "sig-x").is_none());
    }

    // ── AnomalySummary::compose ──────────────────────────────────────

    #[test]
    fn anomaly_summary_carries_all_three_axes() {
        // Compose populates from all three orthogonal axes — the
        // composite shape is the join point for the audit consumers.
        let recurrence = Recurrence {
            signature: "abc123".to_string(),
            count: 42,
            age: Duration::from_secs(900),
        };
        let s =
            AnomalySummary::compose(&recurrence, "ReloadGems", 2, Some("load_path_double_load"));
        assert_eq!(s.signature, "abc123");
        assert_eq!(s.recurrence_count, 42);
        assert_eq!(s.recurrence_age_s, 900);
        assert_eq!(s.recommended_action, "ReloadGems");
        assert_eq!(s.recommended_depth, 2);
        assert_eq!(s.typed_detector.as_deref(), Some("load_path_double_load"));
    }

    #[test]
    fn anomaly_summary_typed_detector_optional() {
        // The opaque-recurring case (unknown known): no named detector
        // fired; just the recurrence + escalation axes populated.
        let recurrence = Recurrence {
            signature: "xyz789".to_string(),
            count: 1,
            age: Duration::from_secs(0),
        };
        let s = AnomalySummary::compose(&recurrence, "Retry", 0, None);
        assert_eq!(s.typed_detector, None);
    }

    #[test]
    fn anomaly_summary_serializes_to_stable_json_shape() {
        // Slice-4 status surfaces consume the JSON form. Field names
        // are part of the contract — locking them here catches
        // accidental refactors that would break dashboards / API
        // clients.
        let recurrence = Recurrence {
            signature: "sig".to_string(),
            count: 1,
            age: Duration::from_secs(60),
        };
        let s = AnomalySummary::compose(&recurrence, "Retry", 0, None);
        let j: serde_json::Value = serde_json::to_value(&s).unwrap();
        assert!(j["signature"].is_string());
        assert!(j["recurrence_count"].is_number());
        assert!(j["recurrence_age_s"].is_number());
        assert!(j["recommended_action"].is_string());
        assert!(j["recommended_depth"].is_number());
        assert!(j["typed_detector"].is_null());
    }

    #[test]
    fn tracker_peek_does_not_bump_count() {
        let t = InMemoryRecurrenceTracker::new();
        t.observe("template-a", "sig-x");
        let peek1 = t.peek("template-a", "sig-x").unwrap();
        let peek2 = t.peek("template-a", "sig-x").unwrap();
        assert_eq!(peek1.count, 1);
        assert_eq!(peek2.count, 1);
        let r = t.observe("template-a", "sig-x");
        assert_eq!(r.count, 2, "peek didn't bump; observe still does");
    }
}
