//! The ONE derivation of a Pangea namespace's PostgreSQL schema identity.
//!
//! # Why this module exists
//!
//! The string `"{prefix}{namespace}"` is the address of LIVE OpenTofu
//! state in Postgres. Everything downstream — `TofuPgStateBackend`'s
//! `"{schema}_{template}_states".states` table, `StateStore`'s
//! `schema."{template}_states"`, the artifact store's rendered-config
//! and bundle rows, the advisory mutation lock — is keyed off it. If two
//! code paths derive it differently, one of them addresses a schema that
//! does not hold the state it thinks it does; magma then plans against an
//! empty state and proposes to create everything from scratch.
//!
//! Before this module the identity had three independent derivations:
//!
//!   1. `PangeaNamespace::schema_name()` — the only one that honoured
//!      `spec.backend.pg.schemaPrefix`.
//!   2. `format!("pangea_{}", template.spec.pangea_namespace)`,
//!      hand-copied at SEVEN call sites, prefix hardcoded.
//!   3. The doc comments describing (2), which had already begun to
//!      disagree with each other about how many copies there were.
//!
//! They could drift independently, and (2) could not observe a
//! non-default `schemaPrefix` at all.
//!
//! # The absolute constraint on this module: byte-identical output
//!
//! Unifying the derivation MUST NOT change the emitted string. Live
//! `_states` schemas in Postgres carry hyphens and dots verbatim (the
//! 863-repo `pleme-io-opensource` workspace is one), because a k8s object
//! name may contain them and this derivation has never sanitized. A
//! "tidy-up" that normalized `-`/`.` to `_` here would silently re-point
//! every one of those templates at a different, EMPTY schema and the next
//! reconcile would plan a full re-create. So:
//!
//!   * the default prefix stays `pangea_`;
//!   * the derivation stays a bare concatenation — **no sanitizing**,
//!     no lower-casing, no length cap, no validation;
//!   * [`tests::template_schema_name_is_byte_identical_to_the_hand_copy`]
//!     pins that against the literal the seven copies used, over a corpus
//!     that includes hyphens and dots.
//!
//! Injection safety is NOT this module's job and must not migrate into
//! it: `StateStore::qualified_state_table` and
//! `ArtifactStore::live_state_schema` already validate a sanitized
//! PROJECTION of the name and quote the original. Validating here would
//! change what this function returns; validating there does not.
//!
//! # Tier honesty
//!
//! * The three derivations agreeing — **CI-gate-caught**. The byte-identity
//!   and cross-derivation tests below fail the build on a change to the
//!   emitted string. Nothing in the type system prevents the change.
//! * A NEW hand-copy being introduced — **CI-gate-caught**, and only for
//!   the textual shape `format!("pangea_…`. [`tests::no_hand_copied_schema_derivation_outside_this_module`]
//!   scans the crate source for it. A copy spelled some other way
//!   (`String::from("pangea_") + ns`, a `write!`, a const concat) slips
//!   past — that residue is **only-mitigated**.
//! * Phase 1 does NOT make a wrong schema name unrepresentable. Every
//!   consumer still takes `&str`.
//!
//! # The destination this is a step toward
//!
//! Phase 2: `template_schema_name` resolves the referenced
//! `PangeaNamespace` and honours its real `schemaPrefix`, so a
//! non-default prefix stops being silently ignored. That is a
//! BEHAVIOUR change (it moves where state is read) and needs its own
//! migration, which is exactly why it is not in this commit — but it is
//! now a one-function change instead of a seven-site change.
//!
//! Phase 3: a `SchemaName` newtype whose only constructor lives here,
//! threaded through `StateStore` / `ArtifactStore` / the lock helpers, so
//! passing a hand-built string becomes a type error rather than a test
//! failure — the tier this module is currently short of.

use crate::crd::infrastructure_template::InfrastructureTemplate;

/// The default PostgreSQL schema prefix — the serde default for
/// `PangeaNamespace.spec.backend.pg.schemaPrefix`, and the prefix the
/// template-side derivation assumes.
///
/// Do not change this value. It names live schemas.
pub const DEFAULT_SCHEMA_PREFIX: &str = "pangea_";

/// THE derivation. Every schema name in the operator comes from here.
///
/// A bare concatenation, deliberately: see the module doc on why
/// sanitizing is forbidden.
#[must_use]
pub fn schema_name(prefix: &str, namespace_name: &str) -> String {
    // TYPED EMISSION note: this `format!` is not emitting syntax — it is
    // reproducing a pre-existing string IDENTITY byte-for-byte. Replacing
    // it with anything that renders differently is the outage this module
    // exists to prevent.
    format!("{prefix}{namespace_name}")
}

/// The schema for a namespace known only BY NAME — the caller holds a
/// `spec.pangeaNamespace` string, not the `PangeaNamespace` CR.
///
/// Assumes the DEFAULT prefix, which is what the seven hand-copies did.
/// This single site is where Phase 2 will resolve the real prefix.
#[must_use]
pub fn schema_name_for_namespace(namespace_name: &str) -> String {
    schema_name(DEFAULT_SCHEMA_PREFIX, namespace_name)
}

/// The schema an `InfrastructureTemplate`'s state, rendered config,
/// bundles and advisory mutation lock are keyed under.
///
/// This MUST agree with `PangeaNamespace::schema_name()` for the
/// referenced namespace, or a template writes state where nothing reads
/// it. Today they agree because every live `PangeaNamespace` uses the
/// default prefix; Phase 2 makes them agree by construction.
#[must_use]
pub fn template_schema_name(template: &InfrastructureTemplate) -> String {
    schema_name_for_namespace(&template.spec.pangea_namespace)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The corpus. Includes the shapes a k8s object name really carries
    /// (hyphens, dots) plus the live workspace whose state would be
    /// orphaned by a normalization, plus shapes no k8s name has but the
    /// function must still pass through untouched — because "it can't
    /// happen" is not the same as "the derivation changed".
    const CORPUS: &[&str] = &[
        // Live on camelot-eks today.
        "camelot",
        "pleme-io-opensource",
        // The shapes a normalization would mangle.
        "with-hyphen",
        "with.dot",
        "hyphen-and.dot-mixed",
        "-leading-hyphen",
        "trailing-hyphen-",
        ".leading.dot",
        // Boundary + adversarial.
        "",
        "default",
        "a",
        "pangea_already_prefixed",
        "UPPER-Case",
        "123-numeric",
        "under_score",
    ];

    /// **The byte-identity proof.**
    ///
    /// `legacy` is a verbatim reproduction of the literal that stood at
    /// `controller/mod.rs:508`, `template_controller.rs:{1380,2244,2429,3455,5635}`
    /// and `template/cycle_receipts.rs:391` before unification. If this
    /// test ever fails, the unification changed what the operator asks
    /// Postgres for — which is a data-addressing change, not a refactor.
    #[test]
    fn template_schema_name_is_byte_identical_to_the_hand_copy() {
        for ns in CORPUS {
            let legacy = format!("pangea_{ns}");
            let unified = schema_name_for_namespace(ns);
            assert_eq!(
                unified, legacy,
                "unified derivation diverged from the hand-copied literal for {ns:?} — \
                 this re-points live Postgres state"
            );
        }
    }

    /// The non-sanitizing property, asserted directly rather than only as
    /// a consequence of the identity test above. Stated separately so the
    /// intent survives a future edit to the corpus.
    #[test]
    fn hyphens_and_dots_survive_verbatim() {
        assert_eq!(
            schema_name_for_namespace("pleme-io-opensource"),
            "pangea_pleme-io-opensource"
        );
        assert_eq!(schema_name_for_namespace("a.b.c"), "pangea_a.b.c");
        assert_eq!(
            schema_name_for_namespace("mix-ed.na-me"),
            "pangea_mix-ed.na-me"
        );
    }

    /// The prefix is honoured when one is supplied — the property
    /// `PangeaNamespace::schema_name()` relies on and the template-side
    /// derivation deliberately does not use yet.
    #[test]
    fn a_supplied_prefix_replaces_the_default() {
        assert_eq!(schema_name("tf_", "camelot"), "tf_camelot");
        assert_eq!(schema_name("", "camelot"), "camelot");
        assert_eq!(
            schema_name(DEFAULT_SCHEMA_PREFIX, "camelot"),
            schema_name_for_namespace("camelot")
        );
    }

    /// `template_schema_name` is the by-name derivation applied to
    /// `spec.pangeaNamespace`, and nothing else — so the seven call sites
    /// it replaced still ask for the same schema. Built by deserializing
    /// a minimal spec rather than restating the 26-field literal, so this
    /// test does not itself become the duplication it is guarding.
    #[test]
    fn template_schema_name_reads_spec_pangea_namespace() {
        for ns in CORPUS {
            let spec: crate::crd::InfrastructureTemplateSpec =
                serde_json::from_value(serde_json::json!({
                    "source": { "inline": "" },
                    "pangeaNamespace": ns,
                }))
                .expect("minimal InfrastructureTemplateSpec must deserialize");
            let t = InfrastructureTemplate::new("any-template", spec);
            assert_eq!(template_schema_name(&t), schema_name_for_namespace(ns));
            // And equals the pre-unification literal, through the CR.
            assert_eq!(template_schema_name(&t), format!("pangea_{ns}"));
        }
    }

    /// The reintroduction gate.
    ///
    /// Unifying the derivation is worthless if the eighth copy lands next
    /// week. This scans the crate's own source for the literal shape the
    /// seven copies had and fails the build on a new one.
    ///
    /// Tier: **CI-gate-caught**, and only for this textual shape — see the
    /// module doc. It is not unrepresentability and must not be described
    /// as such.
    #[test]
    fn no_hand_copied_schema_derivation_outside_this_module() {
        // The needle is written in pieces so this test's own source does
        // not match it.
        let needle = concat!("format!(\"", "pangea_");
        let src = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src");

        let mut offenders = Vec::new();
        let mut stack = vec![src.clone()];
        let mut files_scanned = 0usize;
        while let Some(dir) = stack.pop() {
            for entry in std::fs::read_dir(&dir).expect("crate src/ must be readable") {
                let path = entry.expect("readable dir entry").path();
                if path.is_dir() {
                    stack.push(path);
                    continue;
                }
                if path.extension().and_then(|e| e.to_str()) != Some("rs") {
                    continue;
                }
                // This module is the one legitimate home.
                if path.ends_with("crd/schema_identity.rs") {
                    continue;
                }
                files_scanned += 1;
                let text = std::fs::read_to_string(&path).expect("source file must be UTF-8");
                for (i, line) in text.lines().enumerate() {
                    if line.contains(needle) {
                        offenders.push(format!(
                            "{}:{}: {}",
                            path.strip_prefix(&src).unwrap_or(&path).display(),
                            i + 1,
                            line.trim()
                        ));
                    }
                }
            }
        }

        // A guard that scanned nothing is vacuous and would pass forever.
        assert!(
            files_scanned > 100,
            "reintroduction guard scanned only {files_scanned} files — it is not \
             looking at the crate source and would pass vacuously"
        );
        assert!(
            offenders.is_empty(),
            "a hand-copied schema derivation was reintroduced. Call \
             `crd::schema_identity::template_schema_name(template)` instead — \
             the emitted string is the address of live Postgres state and must \
             have exactly one derivation.\n{}",
            offenders.join("\n")
        );
    }
}
