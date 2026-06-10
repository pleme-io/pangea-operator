//! Load-path XOR end-to-end — the 2026-05-28 wedge as a regression
//! fixture. A workspace `_repo/lib` mirroring the broadcast gem's
//! `pangea/` tree (the exact shape that wedged pleme-io-opensource +
//! cloudflare-pleme into `Phase::Failed` with `Attribute
//! :cluster_name has already been defined`) must now compile — twice
//! — through the planner-derived purge pipeline, with the typed
//! `Conflict` surface riding the `CompileResult`. The refusal arm
//! then proves a residual the purge plan CANNOT cover comes back as
//! `BackendError::DualLoad`, never a silent half-compile.
//!
//! Gated on the `embedded_ruby` feature (links libruby). Run with:
//!     cargo test -p pangea-operator --features embedded_ruby \
//!       --test load_path_xor
//!
//! All assertions are bundled into one #[test] because CRuby is
//! one-init-per-process (same constraint as `embedded_backend.rs`).

#![cfg(feature = "embedded_ruby")]

mod common;

use pangea_operator::ruby::{
    BackendError, CompileRequest, CompilerBackend, EmbeddedCompilerBackend, RubyPool,
    RubyRequest,
};
use pangea_ruby_eval::LoadPathEntry;
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// A `pangea/architectures` tree whose class raises on attribute
/// redefinition — the Dry::Struct-equivalent guard, no gem dep. A
/// second un-purged load of `repo.rb` against a surviving class
/// reproduces the wedge error verbatim.
fn write_architectures_tree(lib: &Path) {
    let arch_dir = lib.join("pangea").join("architectures");
    std::fs::create_dir_all(&arch_dir).expect("mkdir architectures");
    std::fs::write(
        lib.join("pangea").join("architectures.rb"),
        "require 'pangea/architectures/repo'\n",
    )
    .expect("write architectures.rb");
    std::fs::write(
        arch_dir.join("repo.rb"),
        r#"
module Pangea
  module Architectures
    class Repo
      def self.attribute(name)
        @attributes ||= []
        if @attributes.include?(name)
          raise "Attribute :#{name} has already been defined"
        end
        @attributes << name
      end
      attribute :cluster_name
    end
  end
end
"#,
    )
    .expect("write repo.rb");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn load_path_xor_dissolves_the_2026_05_28_wedge() {
    // Canonicalized roots (macOS tempdirs live under /var/folders →
    // /private/var; compile_template canonicalizes before its
    // trusted-root check, so the env vars must match).
    let workspace_base = tempfile::TempDir::new().expect("workspace base");
    let workspace_base = std::fs::canonicalize(workspace_base.path()).unwrap();
    let gem_cache = tempfile::TempDir::new().expect("gem cache");
    let gem_cache = std::fs::canonicalize(gem_cache.path()).unwrap();
    std::env::set_var("PANGEA_WORKSPACE_BASE", &workspace_base);
    std::env::set_var("PANGEA_GEM_CACHE_DIR", &gem_cache);

    // The broadcast gem, shaped like the live cache entry
    // `pangea-architectures-main`.
    let gem_lib = gem_cache.join("pangea-architectures-main").join("lib");
    write_architectures_tree(&gem_lib);

    // The workspace mirror — same logical files, different root.
    let ws_lib: PathBuf = workspace_base.join("ws1").join("_repo").join("lib");
    write_architectures_tree(&ws_lib);
    let template_path = workspace_base.join("ws1").join("_repo").join("main.rb");
    std::fs::write(
        &template_path,
        r#"
require 'pangea/architectures'
template :xor do
  resource :null_resource, :probe, marker: Pangea::Architectures::Repo.name
end
"#,
    )
    .expect("write template");

    // One-worker pool: deterministic dispatch (every request lands on
    // the same long-lived VM — the wedge is a one-VM state-residue
    // bug, so the test needs exactly that shape).
    let pool = Arc::new(RubyPool::spawn(1, vec![]).await.expect("spawn ruby pool"));
    let backend = EmbeddedCompilerBackend::new(pool.clone());

    // TerraformSynthesizer stub (the dev shell has no
    // terraform-synthesizer gem) — same shape as embedded_backend.rs.
    eval(
        &pool,
        r#"
        class TerraformSynthesizer
          def initialize; @manifest = {}; end
          def method_missing(name, *args, **kwargs, &blk)
            section = (@manifest[name.to_s] ||= {})
            if args.length >= 2
              (section[args[0].to_s] ||= {})[args[1].to_s] = kwargs.transform_keys(&:to_s).transform_values(&:to_s)
            else
              section[args[0].to_s] = kwargs.transform_keys(&:to_s).transform_values(&:to_s)
            end
            self
          end
          def synthesis; @manifest; end
        end
        module Pangea; module Resources; end; end
        "stub-installed"
        "#,
    )
    .await;

    // Seed the wedge: broadcast the gem lib onto the live $LOAD_PATH
    // (what prepare_gem does) and load its copy FIRST — exactly the
    // pre-compile interpreter state of 2026-05-28 ($LOADED_FEATURES
    // carries the gem's absolute paths; the guarded class is defined).
    pool.broadcast_prepend_load_path(gem_lib.clone())
        .await
        .expect("broadcast gem lib");
    eval(&pool, "require 'pangea/architectures'; 'gem-loaded'").await;

    // Both entries LABELED — the typed border. (A raw Vec<String>
    // here is E0308; that is the 1a tier proof, enforced by the
    // compiler rather than this test.)
    let request = || CompileRequest {
        template_path: Some(template_path.to_string_lossy().into_owned()),
        rubylib_paths: vec![
            LoadPathEntry::workspace(&ws_lib),
            LoadPathEntry::gem(&gem_lib, "pangea-architectures"),
        ],
        variables: std::collections::HashMap::new(),
        template_name: Some("xor".into()),
        source: None,
    };

    // Compile #1: the planner derives the purge from the labels (no
    // hardcoded path can match this tempdir cache), so the workspace
    // copy defines fresh — no redefinition raise.
    let first = backend.compile(request()).await;
    let first = match first {
        Ok(r) => r,
        Err(e) => panic!("first compile must succeed (got: {e}) — and never \
                          'already been defined'"),
    };
    assert!(
        !first.terraform_json.contains("already been defined"),
        "wedge error leaked into synthesis"
    );
    assert!(
        !first.conflicts.is_empty(),
        "the workspace/gem mirror must surface typed Conflicts on the result"
    );

    // Compile #2 — the historical failure mode was ORDER-dependent
    // (second compile hits the residue the first one left). Must
    // succeed with the same derived purges.
    let second = backend
        .compile(request())
        .await
        .unwrap_or_else(|e| panic!("second compile must succeed (got: {e})"));
    assert!(
        !second.conflicts.is_empty(),
        "second compile sees the same mirror conflicts"
    );

    // ── The refusal arm ──────────────────────────────────────────
    // An UNCOVERED residual: a live $LOAD_PATH copy of a logical file
    // that is neither a labeled gem (no plan purge prefix) nor under
    // the /pangea/architectures substring purge. The compile must
    // refuse with BackendError::DualLoad — loud, typed, never a
    // silent half-compile.
    let poison_root = tempfile::TempDir::new().expect("poison lib");
    let poison_lib = std::fs::canonicalize(poison_root.path()).unwrap().join("lib");
    std::fs::create_dir_all(poison_lib.join("pangea")).unwrap();
    std::fs::write(
        poison_lib.join("pangea").join("poison.rb"),
        "module Pangea; module Poison; end; end\n",
    )
    .unwrap();
    pool.broadcast_prepend_load_path(poison_lib.clone())
        .await
        .expect("prepend poison lib");

    let ws2_lib = workspace_base.join("ws2").join("_repo").join("lib");
    std::fs::create_dir_all(ws2_lib.join("pangea")).unwrap();
    std::fs::write(
        ws2_lib.join("pangea").join("poison.rb"),
        "module Pangea; module Poison; end; end\n",
    )
    .unwrap();
    let template2 = workspace_base.join("ws2").join("_repo").join("main.rb");
    std::fs::write(
        &template2,
        "template :poisoned do\n  resource :null_resource, :x, marker: \"x\"\nend\n",
    )
    .unwrap();

    let refused = backend
        .compile(CompileRequest {
            template_path: Some(template2.to_string_lossy().into_owned()),
            // Workspace label only — the poison copy on the live
            // $LOAD_PATH has no plan entry, so no purge can cover it.
            rubylib_paths: vec![LoadPathEntry::workspace(&ws2_lib)],
            variables: std::collections::HashMap::new(),
            template_name: Some("poisoned".into()),
            source: None,
        })
        .await
        .expect_err("uncovered residual dual-load must refuse");
    match refused {
        BackendError::DualLoad(conflicts) => {
            assert!(
                conflicts.iter().any(|c| c.category == "pangea/poison"),
                "refusal must name the residual logical path; got: {conflicts:?}"
            );
        }
        other => panic!("expected BackendError::DualLoad, got: {other}"),
    }

    std::env::remove_var("PANGEA_WORKSPACE_BASE");
    std::env::remove_var("PANGEA_GEM_CACHE_DIR");
    // Pool drop signals each owner thread to shut down.
    drop(backend);
    drop(pool);
}

/// Eval helper over the pool's (single) worker channel — same pattern
/// as `embedded_backend.rs`.
async fn eval(pool: &Arc<RubyPool>, source: &str) {
    let (rtx, rrx) = tokio::sync::oneshot::channel();
    pool.next_sender()
        .send(RubyRequest::Eval {
            source: source.to_string(),
            respond: rtx,
        })
        .await
        .expect("send eval");
    let _ = rrx.await.expect("eval reply").expect("eval ok");
}
