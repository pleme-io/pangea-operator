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
    CompileRequest, CompilerBackend, EmbeddedCompilerBackend, GemCache, RubyPool, SmokeRequest,
};
use std::sync::Arc;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn embedded_backend_smoke() {
    // One-worker pool — observationally identical to the pre-S2
    // single-owner shape this test was written against (the S2 pool
    // refactor changed the constructor from a tx handle to
    // Arc<RubyPool>; this test drifted until repaired 2026-06-10).
    let pool = Arc::new(RubyPool::spawn(1, vec![]).await.expect("spawn ruby pool"));
    let backend = EmbeddedCompilerBackend::new(pool.clone());

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
        pool
            .next_sender()
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

    // ------------------------------------------------------------
    // M8.4.1 — gem cache + load-path prepend round-trip.
    // ------------------------------------------------------------
    //
    // We don't shell out to git in this test (network + ssh keys
    // make it flaky). Instead we drive the cache layout directly:
    // pre-create a fake gem tree, point GemCache at the parent dir,
    // call entry_dir to verify path computation, then prepend its
    // lib path through the owner thread and confirm Ruby sees it
    // on $LOAD_PATH.
    //
    // ensure() with a real git URL is exercised in M8.4.2 against a
    // fixture repo; this test isolates the cache-layout + Ruby-side
    // contract.
    let tmp = tempdir_for_test();
    std::env::set_var("PANGEA_GEM_CACHE_DIR", tmp.as_os_str());
    let cache = GemCache::from_env();
    assert_eq!(cache.base_dir(), tmp.as_path());

    let entry_dir = cache
        .entry_dir("pangea-fixture-gem", "abc123")
        .expect("entry_dir");
    assert_eq!(
        entry_dir,
        tmp.join("pangea-fixture-gem-abc123"),
        "cache layout should be {{name}}-{{ref}}/"
    );

    let lib_dir = entry_dir.join("lib");
    std::fs::create_dir_all(&lib_dir).expect("mkdir -p");
    std::fs::write(
        lib_dir.join("fake_gem.rb"),
        b"module FakeGem; VERSION = \"0.0.1\".freeze; end\n",
    )
    .expect("write fake gem entrypoint");

    pool.broadcast_prepend_load_path(lib_dir.clone())
        .await
        .expect("prepend_load_path round-trip");

    // M8.4.2 — exercise the higher-level prepare_gem path. We pre-seed
    // an entry dir under the cache base; cache.ensure() should hit
    // (no clone), and the backend's prepare_gem should prepend its
    // lib path. Distinct gem name from the previous step so we can
    // observe the "first call → prepend" behavior.
    //
    // The no-network cache-hit contract is: SHA-shaped ref + a `.git`
    // marker present. (An earlier revision of ensure() tolerated a
    // missing .git; it now treats that as a stale dir and re-clones —
    // this fixture tracks the current contract.)
    let prepared_sha = "0123456789abcdef0123456789abcdef01234567";
    let prepared_gem_dir = tmp.join(format!("pangea-prepared-gem-{prepared_sha}"));
    let prepared_lib_dir = prepared_gem_dir.join("lib");
    std::fs::create_dir_all(&prepared_lib_dir).expect("mkdir -p prepared lib");
    std::fs::create_dir_all(prepared_gem_dir.join(".git")).expect("mkdir -p .git marker");
    std::fs::write(
        prepared_lib_dir.join("prepared_gem.rb"),
        b"module PreparedGem; STAMP = \"42\".freeze; end\n",
    )
    .expect("write prepared_gem entrypoint");

    let backend_with_cache = pangea_operator::ruby::EmbeddedCompilerBackend::with_cache(
        pool.clone(),
        cache.clone(),
    );
    backend_with_cache
        .prepare_gem(&pangea_operator::ruby::GemSource {
            name: "pangea-prepared-gem".to_string(),
            git_url: "https://example.invalid/pangea-prepared-gem.git".to_string(),
            git_ref: "0123456789abcdef0123456789abcdef01234567".to_string(),
            kind: pangea_operator::ruby::SourceKind::Ruby,
        })
        .await
        .expect("prepare_gem round-trip");

    {
        use pangea_operator::ruby::RubyRequest;
        let (rtx, rrx) = tokio::sync::oneshot::channel();
        pool
            .next_sender()
            .send(RubyRequest::Eval {
                source: r#"
                require 'prepared_gem'
                { "stamp" => PreparedGem::STAMP }
                "#
                .to_string(),
                respond: rtx,
            })
            .await
            .expect("send eval");
        let result = rrx.await.expect("eval reply").expect("eval ok");
        assert_eq!(
            result["stamp"],
            serde_json::json!("42"),
            "prepare_gem should have made the gem requirable"
        );
    }

    // Idempotency: second prepare_gem call with same (name, ref) is a
    // fast-path no-op (cache hit; HashSet skip on the prepend send).
    backend_with_cache
        .prepare_gem(&pangea_operator::ruby::GemSource {
            name: "pangea-prepared-gem".to_string(),
            git_url: "https://example.invalid/pangea-prepared-gem.git".to_string(),
            git_ref: "0123456789abcdef0123456789abcdef01234567".to_string(),
            kind: pangea_operator::ruby::SourceKind::Ruby,
        })
        .await
        .expect("prepare_gem idempotent");

    // Step 10 — terreno reservation: prepare_gem with kind=Lisp must
    // return a typed NotImplemented error pointing at TERRENO.md M2.
    // This is the parallel-track invariant from theory/TERRENO.md:
    // the schema + dispatch surface exist NOW; terreno-eval (M2)
    // wires the actual implementation later. Until then the
    // controller surfaces this as `GemPrepareFailed` on the CR.
    let terreno_err = backend_with_cache
        .prepare_gem(&pangea_operator::ruby::GemSource {
            name: "terreno-cloudflare".to_string(),
            git_url: "https://example.invalid/terreno-cloudflare.git".to_string(),
            git_ref: "main".to_string(),
            kind: pangea_operator::ruby::SourceKind::Lisp,
        })
        .await
        .expect_err("kind=Lisp must return typed NotImplemented");
    let msg = format!("{terreno_err}");
    assert!(
        msg.contains("Lisp not yet implemented") || msg.contains("terreno"),
        "Lisp reservation should surface terreno NotImplemented; got: {msg}"
    );

    let wasm_err = backend_with_cache
        .prepare_gem(&pangea_operator::ruby::GemSource {
            name: "wasm-fixture".to_string(),
            git_url: "oci://example.invalid/wasm-fixture".to_string(),
            git_ref: "v1".to_string(),
            kind: pangea_operator::ruby::SourceKind::Wasm,
        })
        .await
        .expect_err("kind=Wasm must return typed NotImplemented");
    let msg = format!("{wasm_err}");
    assert!(
        msg.contains("Wasm not yet implemented") || msg.contains("wasmtime"),
        "Wasm reservation should surface NotImplemented; got: {msg}"
    );

    // Confirm the path is on $LOAD_PATH and require works.
    {
        use pangea_operator::ruby::RubyRequest;
        let (rtx, rrx) = tokio::sync::oneshot::channel();
        pool
            .next_sender()
            .send(RubyRequest::Eval {
                source: format!(
                    r#"
                    on_path = $LOAD_PATH.include?("{lib}")
                    require 'fake_gem'
                    {{ "on_path" => on_path, "version" => FakeGem::VERSION }}
                    "#,
                    lib = lib_dir.to_string_lossy(),
                ),
                respond: rtx,
            })
            .await
            .expect("send eval");
        let result = rrx.await.expect("eval reply").expect("eval ok");
        assert_eq!(result["on_path"], serde_json::json!(true));
        assert_eq!(result["version"], serde_json::json!("0.0.1"));
    }

    // Cleanup
    std::env::remove_var("PANGEA_GEM_CACHE_DIR");
    let _ = std::fs::remove_dir_all(&tmp);

    // Pool drop signals each owner thread to shut down.
    drop(backend_with_cache);
    drop(backend);
    drop(pool);
}

/// Per-test temp dir. We can't pull in `tempfile` for one helper, and
/// std doesn't give us auto-cleanup, so we do it by hand at end-of-test.
fn tempdir_for_test() -> std::path::PathBuf {
    let stamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let dir = std::env::temp_dir().join(format!("pangea-eval-test-{stamp}"));
    std::fs::create_dir_all(&dir).expect("mkdir tempdir");
    dir
}
