//! Typed executor-backend selection.
//!
//! Resolves which `IacExecutor` impl handles a reconcile based on:
//!
//!   1. The CR's `spec.executor` field (per-CR override)
//!   2. The `PANGEA_EXECUTOR` env var (operator-wide default)
//!   3. Fallback to `Tofu` (the conservative default)
//!
//! Per `theory/MAGMA-OPERATOR-BACKEND.md` §VI. Both `tofu` and
//! `magma` ship indefinitely; this is the load-bearing per-CR
//! opt-in for the magma backend during burn-in.

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
        ExecutorBackend::Tofu
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
    fn default_is_tofu_when_neither_set() {
        let r = ExecutorBackend::resolve(None, None);
        assert_eq!(r, ExecutorBackend::Tofu);
    }

    #[test]
    fn unrecognized_cr_falls_through_to_env() {
        let r = ExecutorBackend::resolve(Some("kerflump"), Some("magma"));
        assert_eq!(r, ExecutorBackend::Magma);
    }

    #[test]
    fn unrecognized_env_falls_through_to_tofu() {
        let r = ExecutorBackend::resolve(None, Some("kerflump"));
        assert_eq!(r, ExecutorBackend::Tofu);
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
}
