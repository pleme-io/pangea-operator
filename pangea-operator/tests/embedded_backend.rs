//! End-to-end test of the embedded compiler backend path.
//!
//! Spawns a `RubyOwner`, wraps it in `EmbeddedCompilerBackend`,
//! exercises `list_architectures` and `smoke_test` through the trait
//! that reconcilers consume in production.
//!
//! Gated on the `embedded_ruby` feature (which links libruby). Run with:
//!     cargo test -p pangea-operator --features embedded_ruby \
//!       --test embedded_backend
//!
//! All assertions are bundled into one #[test] because CRuby is
//! one-init-per-process: each `RubyOwner::spawn` calls `Init_ruby`,
//! which panics on second call. We share one owner across all
//! sub-steps.

#![cfg(feature = "embedded_ruby")]

use pangea_operator::ruby::{
    CompilerBackend, EmbeddedCompilerBackend, RubyOwner, SmokeRequest,
};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn embedded_backend_smoke() {
    let owner = RubyOwner::spawn(vec![]).await.expect("spawn ruby owner");
    let backend = EmbeddedCompilerBackend::new(owner.tx_handle());

    // Step 1 — listing an unknown gem returns empty classes/version
    // (matches the sidecar's behavior; the operator surfaces this as
    // an `ExpectedClassesMissing` typed condition on the CR).
    let listing = backend
        .list_architectures("definitely-not-a-real-gem-name")
        .await
        .expect("list_architectures returns Ok even for unknown gem");
    assert_eq!(listing.gem, "definitely-not-a-real-gem-name");
    assert!(listing.classes.is_empty(), "unknown gem yields no classes");
    assert!(listing.version.is_none());

    // Step 2 — smoke-testing against an unloaded gem with a relative
    // fixture path returns a typed failure (passed=false, error
    // contains "Gem not loaded"). The relative path triggers the
    // Gem.loaded_specs lookup which fails when the gem isn't loaded.
    let outcome = backend
        .smoke_test(SmokeRequest {
            gem: "definitely-not-a-real-gem-name".into(),
            class_name: "Pangea::Architectures::DoesNotExist".into(),
            fixture_path: "spec/fixtures/whatever.yaml".into(),
        })
        .await
        .expect("smoke_test returns Ok even on missing-gem (typed failure inside)");
    assert!(!outcome.passed);
    let err = outcome.error.as_deref().unwrap_or_default();
    assert!(
        err.contains("Gem not loaded") || err.contains("uninitialized constant"),
        "expected missing-gem error, got: {err}"
    );

    // Step 3 — M8.3: absolute fixture path skips the gem lookup; Rust
    // reads + SHA-256 + YAML parses the file; injected as $pangea_inputs.
    // Class still doesn't exist (we don't load any pangea-* gem in
    // this test), so we expect passed=false with a "uninitialized
    // constant" error from the const_get reduce. But input_hash MUST
    // be Some(_) — proving Rust did the SHA-256 before Ruby blew up.
    let abs_fixture = std::env::current_dir()
        .unwrap()
        .join("tests/fixtures/sample.yaml");
    assert!(
        abs_fixture.exists(),
        "test fixture missing at {}",
        abs_fixture.display()
    );

    let outcome = backend
        .smoke_test(SmokeRequest {
            gem: "ignored-when-fixture-is-absolute".into(),
            class_name: "Pangea::Architectures::DoesNotExist".into(),
            fixture_path: abs_fixture.to_string_lossy().into_owned(),
        })
        .await
        .expect("smoke_test with absolute fixture path");
    assert!(!outcome.passed, "class doesn't exist; should fail");
    assert!(
        outcome.input_hash.is_some(),
        "Rust-side SHA-256 must run before Ruby resolves the class"
    );
    let hash = outcome.input_hash.as_deref().unwrap();
    assert_eq!(hash.len(), 12, "input_hash is 12-char SHA-256 prefix");
    let err = outcome.error.as_deref().unwrap_or_default();
    assert!(
        err.contains("uninitialized constant") || err.contains("DoesNotExist"),
        "expected class-not-found error, got: {err}"
    );

    owner.shutdown().await;
}
