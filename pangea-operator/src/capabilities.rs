//! What this binary can actually do — declared by the build, readable by
//! anyone, and checked against the runtime configuration before any I/O.
//!
//! ## Why this module exists
//!
//! `pangea-operator`'s compiler backend is chosen at RUNTIME
//! (`PANGEA_COMPILER_BACKEND`) but its capability to serve that choice is
//! fixed at BUILD time (the `embedded_ruby` cargo feature). Those two facts
//! live in different files, in different languages, resolved at different
//! times — so they are free to disagree, and every layer stays green while
//! they do:
//!
//! - `Cargo.toml` says `default = ["graphql","grpc","executor_magma"]` — no Ruby.
//! - A deployment says `PANGEA_COMPILER_BACKEND=embedded`.
//! - Nix builds an image *named* `pangea-operator-embedded`.
//!
//! Nothing in that chain compares the three. Measured 2026-09-06: the
//! `embedded-ruby` image had been building the Ruby-FREE binary — the
//! `rootFeatures` argument is not honored on the lockfile-builder path, so the
//! non-default feature never reached the compiler — and the artifact carried
//! ruby, git, opentofu and packer around a binary that could not call any of
//! them. The name promised a capability the bytes did not have, for months.
//!
//! ## What it changes
//!
//! A binary that can DESCRIBE ITSELF turns that from an inference into a
//! measurement. `--capabilities` prints this struct as JSON, so a Nix check, a
//! CI step, or an operator at a shell can ask the artifact rather than trust
//! its filename.
//!
//! ## Tier
//!
//! **parse-time-rejected, not unrepresentable.** The honest grade: a cargo
//! feature is not visible to the type system, so nothing here makes the
//! mismatched pair impossible to BUILD. What it makes impossible is shipping
//! one undetected — [`Capabilities::check`] refuses the pair before the process
//! does any work, and it runs early enough that the refusal costs a startup
//! rather than a reconcile.
//!
//! The ceiling is worth naming precisely: this catches the pair at STARTUP.
//! Catching it at BUILD is the nix module's job (a `readOnly` derived package,
//! so the operator never picks a backend and a package separately), and that is
//! genuinely earlier. Both exist because they fail at different times and a
//! reader deserves to know which one caught them.

use serde::{Deserialize, Serialize};

/// A compiler backend the operator can be asked to use.
///
/// Deliberately NOT a bare string comparison at the call site: this is the one
/// place that knows which backends exist and which need a build feature, so a
/// new backend is one arm here rather than a fifth `== "embedded"` somewhere.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Backend {
    /// tatara-lisp architectures evaluated by lava. No Ruby in the process.
    Lava,
    /// In-process CRuby via magnus. Requires the `embedded_ruby` feature.
    Embedded,
    /// The sunset HTTP compiler sidecar. Kept as a migration escape hatch.
    Http,
}

impl Backend {
    /// Parse the `PANGEA_COMPILER_BACKEND` value.
    ///
    /// Returns `None` for an unrecognized value so the caller can decide —
    /// today `main` warns and falls back to `Lava`, which is a deliberate
    /// choice and not this function's to make.
    #[must_use]
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "lava" => Some(Self::Lava),
            "embedded" => Some(Self::Embedded),
            "http" => Some(Self::Http),
            _ => None,
        }
    }

    /// The build feature this backend needs, if any.
    #[must_use]
    pub fn required_feature(self) -> Option<&'static str> {
        match self {
            Self::Embedded => Some("embedded_ruby"),
            Self::Lava | Self::Http => None,
        }
    }

    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Lava => "lava",
            Self::Embedded => "embedded",
            Self::Http => "http",
        }
    }
}

/// What the compiled artifact can do.
///
/// Every field is a `cfg!` read, so this is the build's own account of itself
/// rather than a hand-maintained list that could drift from the features that
/// were actually enabled.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Capabilities {
    pub version: &'static str,
    /// `true` when magnus/CRuby is linked in — the `embedded_ruby` feature.
    pub embedded_ruby: bool,
    pub executor_magma: bool,
    pub graphql: bool,
    pub grpc: bool,
    /// Every backend this binary can actually serve, derived from the flags
    /// above rather than listed separately.
    pub backends: Vec<&'static str>,
}

impl Capabilities {
    /// Read the capabilities compiled into THIS binary.
    #[must_use]
    pub fn current() -> Self {
        let embedded_ruby = cfg!(feature = "embedded_ruby");
        let mut backends = vec![Backend::Lava.as_str(), Backend::Http.as_str()];
        if embedded_ruby {
            backends.push(Backend::Embedded.as_str());
        }
        backends.sort_unstable();
        Self {
            version: env!("CARGO_PKG_VERSION"),
            embedded_ruby,
            executor_magma: cfg!(feature = "executor_magma"),
            graphql: cfg!(feature = "graphql"),
            grpc: cfg!(feature = "grpc"),
            backends,
        }
    }

    /// Can this binary serve `backend`?
    #[must_use]
    pub fn supports(&self, backend: Backend) -> bool {
        match backend.required_feature() {
            None => true,
            Some("embedded_ruby") => self.embedded_ruby,
            // A backend declaring a feature this struct does not model is a
            // gap in THIS module, and answering `true` would paper over it.
            Some(_) => false,
        }
    }

    /// Refuse an incompatible (build, run) pair.
    ///
    /// Call this before any I/O. The error names both halves and the exact fix,
    /// because the failure is a *configuration* mismatch and the operator
    /// reading it needs to know which half to change.
    ///
    /// An unrecognized backend string is NOT an error here — `main` owns that
    /// fallback policy, and duplicating it would let the two disagree.
    ///
    /// # Errors
    /// Returns the incompatibility when the requested backend needs a feature
    /// this build lacks.
    pub fn check(&self, requested: &str) -> Result<(), IncompatibleBuild> {
        let Some(backend) = Backend::parse(requested) else {
            return Ok(());
        };
        if self.supports(backend) {
            return Ok(());
        }
        Err(IncompatibleBuild {
            requested: backend,
            missing_feature: backend.required_feature().unwrap_or("<unmodelled>"),
            available: self.backends.clone(),
        })
    }
}

/// The (build, run) pair that cannot work.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IncompatibleBuild {
    pub requested: Backend,
    pub missing_feature: &'static str,
    pub available: Vec<&'static str>,
}

impl std::fmt::Display for IncompatibleBuild {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "incompatible build and run configuration: \
             PANGEA_COMPILER_BACKEND={requested} needs the `{feature}` cargo feature, \
             which this binary was not built with. \
             This binary serves: {available}. \
             Fix ONE of the two halves — either set PANGEA_COMPILER_BACKEND to a \
             backend this artifact serves, or deploy the artifact built from \
             Cargo.ruby.build-spec.json (the variant with `{feature}` enabled). \
             Refusing to start rather than silently serving a different backend.",
            requested = self.requested.as_str(),
            feature = self.missing_feature,
            available = self.available.join(", "),
        )
    }
}

impl std::error::Error for IncompatibleBuild {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lava_and_http_need_no_feature() {
        assert_eq!(Backend::Lava.required_feature(), None);
        assert_eq!(Backend::Http.required_feature(), None);
        assert_eq!(Backend::Embedded.required_feature(), Some("embedded_ruby"));
    }

    #[test]
    fn parse_round_trips_and_rejects_junk() {
        for b in [Backend::Lava, Backend::Embedded, Backend::Http] {
            assert_eq!(Backend::parse(b.as_str()), Some(b));
        }
        assert_eq!(Backend::parse("ruby"), None);
        assert_eq!(Backend::parse(""), None);
    }

    /// The anti-vacuity case. A build WITHOUT the feature must refuse
    /// `embedded`; a build WITH it must accept. Asserting only the current
    /// build's arm would leave the other direction untested in every CI run,
    /// which is how a one-sided guard goes green forever.
    #[test]
    fn the_pair_is_checked_in_both_directions() {
        let ruby_free = Capabilities {
            version: "test",
            embedded_ruby: false,
            executor_magma: true,
            graphql: true,
            grpc: true,
            backends: vec!["http", "lava"],
        };
        let err = ruby_free.check("embedded").expect_err("must refuse");
        assert_eq!(err.requested, Backend::Embedded);
        assert_eq!(err.missing_feature, "embedded_ruby");

        let with_ruby = Capabilities {
            embedded_ruby: true,
            backends: vec!["embedded", "http", "lava"],
            ..ruby_free.clone()
        };
        with_ruby.check("embedded").expect("must accept");

        // The backends needing no feature are served by both.
        for b in ["lava", "http"] {
            ruby_free.check(b).expect("no feature required");
            with_ruby.check(b).expect("no feature required");
        }
    }

    #[test]
    fn an_unknown_backend_is_mains_policy_not_an_incompatibility() {
        let c = Capabilities::current();
        c.check("wat").expect("unknown backends fall through to main's warn+default");
    }

    /// `backends` is DERIVED from the feature flags, so it cannot claim a
    /// backend the binary does not serve.
    #[test]
    fn advertised_backends_match_what_is_actually_supported() {
        let c = Capabilities::current();
        for name in &c.backends {
            let b = Backend::parse(name).expect("advertised name must parse");
            assert!(c.supports(b), "advertised {name} but cannot serve it");
        }
        assert_eq!(c.backends.contains(&"embedded"), c.embedded_ruby);
        assert_eq!(c.embedded_ruby, cfg!(feature = "embedded_ruby"));
    }

    #[test]
    fn the_error_names_both_halves_and_the_fix() {
        let ruby_free = Capabilities {
            version: "test",
            embedded_ruby: false,
            executor_magma: true,
            graphql: true,
            grpc: true,
            backends: vec!["http", "lava"],
        };
        let msg = ruby_free.check("embedded").unwrap_err().to_string();
        assert!(msg.contains("PANGEA_COMPILER_BACKEND=embedded"), "{msg}");
        assert!(msg.contains("embedded_ruby"), "{msg}");
        assert!(msg.contains("Cargo.ruby.build-spec.json"), "{msg}");
    }
}
