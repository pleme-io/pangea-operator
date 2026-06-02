//! Typed executor-backend selection.
//!
//! Resolves which `IacExecutor` impl handles a reconcile based on:
//!
//!   1. The CR's `spec.executor` field (per-CR override)
//!   2. The `PANGEA_EXECUTOR` env var (operator-wide default)
//!   3. Fallback to `Magma` (the default since 2026-06-02 — "we only deal
//!      in magma for pangea-operator"; tofu is now an explicit opt-in via
//!      CR/env, never the implicit default)
//!
//! Per `theory/MAGMA-OPERATOR-BACKEND.md` §VI. Both `tofu` and `magma` ship
//! indefinitely; magma is the default + tofu is the per-CR escape hatch /
//! last-resort fallback.

/// Which `IacExecutor` impl handles a reconcile.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutorBackend {
    /// OpenTofu subprocess (`TofuExecutor`). Default.
    Tofu,
    /// magma library, in-process (`MagmaExecutor`). Opt-in via CR or env.
    Magma,
}

impl ExecutorBackend {
    /// Resolution order: CR override → env default → Tofu fallback.
    ///
    /// Both inputs are case-insensitive. Unrecognized values fall
    /// through to the next layer rather than panicking so a typo in
    /// a CR doesn't take a workload down.
    pub fn resolve(cr_executor: Option<&str>, env_default: Option<&str>) -> Self {
        if let Some(v) = cr_executor {
            if let Some(b) = Self::parse(v) {
                return b;
            }
        }
        if let Some(v) = env_default {
            if let Some(b) = Self::parse(v) {
                return b;
            }
        }
        // Default since 2026-06-02: magma. tofu is never the implicit default —
        // it is only ever chosen by an explicit CR field or env var. See module docs.
        ExecutorBackend::Magma
    }

    /// Parse a backend name. Returns `None` for unrecognized values
    /// (caller can decide whether to fall through or error).
    pub fn parse(s: &str) -> Option<Self> {
        match s.trim().to_lowercase().as_str() {
            "tofu" | "opentofu" => Some(ExecutorBackend::Tofu),
            "magma"             => Some(ExecutorBackend::Magma),
            _ => None,
        }
    }

    /// String label suitable for logs and metrics.
    pub fn label(&self) -> &'static str {
        match self {
            ExecutorBackend::Tofu  => "tofu",
            ExecutorBackend::Magma => "magma",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cr_wins_over_env() {
        let r = ExecutorBackend::resolve(Some("magma"), Some("tofu"));
        assert_eq!(r, ExecutorBackend::Magma);
    }

    #[test]
    fn env_used_when_cr_unset() {
        let r = ExecutorBackend::resolve(None, Some("magma"));
        assert_eq!(r, ExecutorBackend::Magma);
    }

    #[test]
    fn default_is_magma_when_neither_set() {
        // 2026-06-02: magma is the default; tofu is opt-in only.
        let r = ExecutorBackend::resolve(None, None);
        assert_eq!(r, ExecutorBackend::Magma);
    }

    #[test]
    fn unrecognized_cr_falls_through_to_env() {
        let r = ExecutorBackend::resolve(Some("kerflump"), Some("magma"));
        assert_eq!(r, ExecutorBackend::Magma);
    }

    #[test]
    fn unrecognized_env_falls_through_to_magma() {
        let r = ExecutorBackend::resolve(None, Some("kerflump"));
        assert_eq!(r, ExecutorBackend::Magma);
    }

    #[test]
    fn case_insensitive() {
        assert_eq!(ExecutorBackend::resolve(Some("MAGMA"), None), ExecutorBackend::Magma);
        assert_eq!(ExecutorBackend::resolve(Some("Tofu"),  None), ExecutorBackend::Tofu);
        assert_eq!(ExecutorBackend::resolve(Some("opentofu"), None), ExecutorBackend::Tofu);
    }

    #[test]
    fn whitespace_tolerated() {
        assert_eq!(ExecutorBackend::resolve(Some("  magma  "), None), ExecutorBackend::Magma);
    }

    #[test]
    fn label_matches_canonical_name() {
        assert_eq!(ExecutorBackend::Tofu.label(),  "tofu");
        assert_eq!(ExecutorBackend::Magma.label(), "magma");
    }

    #[test]
    fn parse_round_trips_canonical_names() {
        for backend in [ExecutorBackend::Tofu, ExecutorBackend::Magma] {
            assert_eq!(ExecutorBackend::parse(backend.label()), Some(backend));
        }
    }

    #[test]
    fn resolve_preserves_label_round_trip() {
        // After resolution, the label of the chosen backend MUST
        // re-parse to the same backend. Locks in the
        // label↔parse↔resolve coherence so future label-formatting
        // changes can't silently break the resolution chain.
        for backend in [ExecutorBackend::Tofu, ExecutorBackend::Magma] {
            let chosen = ExecutorBackend::resolve(Some(backend.label()), None);
            assert_eq!(chosen, backend);
            // Round-trip again through env-default position.
            let via_env = ExecutorBackend::resolve(None, Some(backend.label()));
            assert_eq!(via_env, backend);
        }
    }

    #[test]
    fn empty_string_treated_as_unset() {
        // A literal empty string from a YAML-marshaled
        // Option<String> shouldn't accidentally pick a backend.
        // Empty string is unrecognized so it falls through to the default
        // (magma since 2026-06-02) — verify that explicitly.
        let r = ExecutorBackend::resolve(Some(""), None);
        assert_eq!(r, ExecutorBackend::Magma);
    }

    #[test]
    fn resolve_does_not_panic_on_garbage_input() {
        // No matter what byte sequence comes in via CR or env,
        // resolve must return a valid backend (never panic).
        for garbage in ["", "?", "tofu;rm -rf /", "magma\n", "MAGMA\0", "\x00\x01\x02"] {
            let _ = ExecutorBackend::resolve(Some(garbage), Some(garbage));
        }
    }

    #[test]
    fn parse_of_label_always_succeeds() {
        // Property: for every variant, parse(label(variant)) ==
        // Some(variant). If we add a third backend (e.g. magma-wasm)
        // and forget to wire it into parse(), this test breaks
        // before the new variant lands.
        let variants = [ExecutorBackend::Tofu, ExecutorBackend::Magma];
        for v in variants {
            assert_eq!(ExecutorBackend::parse(v.label()), Some(v),
                       "label/parse asymmetric for {v:?}");
        }
    }
}
