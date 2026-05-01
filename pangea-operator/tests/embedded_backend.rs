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
    CompileRequest, CompilerBackend, EmbeddedCompilerBackend, RubyOwner, SmokeRequest,
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

    // ------------------------------------------------------------
    // M8.4 — compile() works end-to-end via the embedded path.
    // ------------------------------------------------------------
    //
    // We don't have terraform-synthesizer in the dev shell, so we
    // bootstrap a tiny stub class on the interpreter via the
    // RubyRequest::Eval channel. This proves the captured-block +
    // instance_eval pattern works against a real synth.
    //
    // The stub records every method call into @manifest, returns
    // self for fluent chains, and exposes .synthesis as a Hash —
    // exactly the shape app.rb's TerraformSynthesizer presents.
    {
        use pangea_operator::ruby::RubyRequest;
        let (rtx, rrx) = tokio::sync::oneshot::channel();
        owner
            .tx_handle()
            .send(RubyRequest::Eval {
                source: r#"
                class TerraformSynthesizer
                  def initialize
                    @manifest = {}
                  end
                  def method_missing(name, *args, **kwargs, &blk)
                    nested = @manifest
                    section = name.to_s
                    nested[section] ||= {}
                    if args.length >= 2
                      nested[section][args[0].to_s] ||= {}
                      nested[section][args[0].to_s][args[1].to_s] = kwargs.transform_keys(&:to_s)
                    else
                      nested[section][args[0].to_s] = kwargs.transform_keys(&:to_s)
                    end
                    self
                  end
                  def synthesis
                    @manifest
                  end
                end
                module Pangea; module Resources; end; end
                "stub-installed"
                "#
                .to_string(),
                respond: rtx,
            })
            .await
            .expect("send eval");
        let _ = rrx.await.expect("eval reply").expect("eval ok");
    }

    // Step 4: source-mode compile against a Pangea-shaped template.
    // template :hello do
    //   resource :null_resource, :greeter, message: "world"
    //   output :greeting, value: "world"
    // end
    let compile_result = backend
        .compile(CompileRequest {
            source: Some(
                r#"
                template :hello do
                  resource :null_resource, :greeter, message: "world"
                  output :greeting, value: "world"
                end
                "#
                .to_string(),
            ),
            template_path: None,
            rubylib_paths: vec![],
            variables: std::collections::HashMap::new(),
            template_name: Some("hello".to_string()),
        })
        .await
        .expect("compile via embedded backend");

    // The resulting terraform_json is pretty-printed JSON of the
    // synth's @manifest. Parse it back and verify the captured block
    // ran end-to-end (resource + output sections present).
    let parsed: serde_json::Value =
        serde_json::from_str(&compile_result.terraform_json).expect("compile result is valid JSON");
    assert!(
        parsed["resource"]["null_resource"]["greeter"]["message"] == "world",
        "resource not captured: {parsed:?}"
    );
    assert!(
        parsed["output"]["greeting"]["value"] == "world",
        "output not captured: {parsed:?}"
    );

    // Step 5: variables get injected as ENV + as $pangea_variables.
    // The template references both ENV.fetch (provider-style) and the
    // $pangea_variables global (M8.4 contract).
    let mut vars = std::collections::HashMap::new();
    vars.insert("CF_API_TOKEN".to_string(), serde_json::json!("env-tok-42"));
    vars.insert("zone".to_string(), serde_json::json!("quero.cloud"));
    let compile_result = backend
        .compile(CompileRequest {
            source: Some(
                r#"
                template :env_check do
                  resource :null_resource, :env_proof, token: ENV.fetch("CF_API_TOKEN"), zone: $pangea_variables["zone"]
                end
                "#
                .to_string(),
            ),
            template_path: None,
            rubylib_paths: vec![],
            variables: vars,
            template_name: Some("env_check".to_string()),
        })
        .await
        .expect("compile with vars");
    let parsed: serde_json::Value =
        serde_json::from_str(&compile_result.terraform_json).expect("vars compile is valid JSON");
    assert_eq!(
        parsed["resource"]["null_resource"]["env_proof"]["token"], "env-tok-42",
        "ENV.fetch did not see injected variable"
    );
    assert_eq!(
        parsed["resource"]["null_resource"]["env_proof"]["zone"], "quero.cloud",
        "$pangea_variables['zone'] did not see injected variable"
    );

    // Step 6: ENV bracketing must restore prior state. After step 5
    // the variable should NOT be set in the host process.
    assert!(
        std::env::var("CF_API_TOKEN").is_err(),
        "CF_API_TOKEN leaked out of compile() — with_env bracketing failed"
    );

    owner.shutdown().await;
}
