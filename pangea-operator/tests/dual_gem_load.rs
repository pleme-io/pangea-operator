//! M0 — REPRODUCTION HARNESS for the dual-gem-load wedge.
//!
//! Reproduces, in an embedded CRuby VM, the production failure that wedges
//! the `pleme-io-opensource` InfrastructureTemplate:
//!   `uninitialized constant Pangea::Architectures::OpenSourceRepo`
//!
//! ## The bug (confirmed live on rio, operator image r48)
//! The operator loads pangea-architectures TWICE: the baked gem at
//! `$PANGEA_GEM_CACHE_DIR/pangea-architectures-main/lib` (primed once at
//! startup) AND the per-compile workspace clone at
//! `$PANGEA_WORKSPACE_BASE/<ns>/<name>/_repo/lib` (because the workspace SOURCE
//! *is* pangea-architectures — a "gem mirror"). The live `compile_template`
//! strategy (`owner.rs`: `with_purge_modules(["Pangea::Architectures"])` +
//! `purge_feature_prefixes()` + workspace-first `$LOAD_PATH`) PURGES the gem's
//! `$LOADED_FEATURES` and re-requires from the prepended clone copy. Re-running a
//! Dry::Struct file whose attribute registry already carries `:cluster_name`
//! (the registry survives the Ruby constant removal) raises "Attribute
//! :cluster_name has already been defined" mid-require → `OpenSourceRepo` never
//! finishes defining → `uninitialized constant` when it's referenced.
//!
//! ## Fidelity
//! A HERMETIC `Dry::Struct` stub models the exact mechanism: its attribute
//! registry is a class-var that is NOT under the purge prefix, so it survives
//! the constant purge exactly like dry-struct/dry-types' global registry does —
//! the second `attribute` call on the re-defined class raises. No real
//! dry-struct/pangea gem closure needed; the failure is the genuine double-
//! execution defect, not a mock.
//!
//! ## Run
//!   nix shell nixpkgs#ruby_3_3 --command \
//!     cargo test -p pangea-operator --features embedded_ruby --test dual_gem_load
//! (rb-sys/magnus link libruby; system Ruby 2.6 fails the stable-API check.)
//!
//! One `#[test]`: CRuby is one-init-per-process; this file is its own binary so
//! it never collides with `embedded_backend.rs`.

#![cfg(feature = "embedded_ruby")]

use std::path::{Path, PathBuf};
use std::sync::Arc;

use pangea_operator::ruby::{
    CompileRequest, CompilerBackend, EmbeddedCompilerBackend, RubyPool, RubyRequest,
};
use tokio::sync::oneshot;

/// The gem-cache root name the live `purge_feature_prefixes` hardcodes
/// (`/var/pangea/gems/pangea-architectures-main/`). We stage under a temp root
/// but keep the `pangea-architectures-main/lib` shape so the purge prefix logic
/// the harness exercises matches production.
const GEM_DIR: &str = "pangea-architectures-main";

/// A `Dry::Struct` stub whose attribute registry (`@@defined`) is a class-var on
/// the stub — loaded ONCE from a lib dir that is NOT under any purge prefix, so
/// it survives the constant purge exactly like dry-types' global registry.
/// Re-defining `OpenSourceRepo < Dry::Struct` and calling `attribute :cluster_name`
/// a second time raises — the genuine production mechanism.
const DRY_STRUCT_STUB: &str = r##"
module Dry
  class Struct
    @@defined = {}
    def self.attribute(name)
      key = "#{self.name}::#{name}"
      if @@defined[key]
        raise "Attribute :#{name} has already been defined"
      end
      @@defined[key] = true
    end
  end
end
"##;

/// `pangea/architectures/types.rb` — the Dry::Struct-bearing file that is mounted
/// in BOTH the gem and the workspace clone (byte-identical, distinct abs paths).
const TYPES_RB: &str = r#"
require 'dry-struct'
module Pangea
  module Architectures
    class OpenSourceRepo < Dry::Struct
      attribute :cluster_name
    end
  end
end
"#;

fn write_file(path: &Path, body: &str) {
    std::fs::create_dir_all(path.parent().unwrap()).expect("mkdir -p");
    std::fs::write(path, body).expect("write file");
}

/// Send a `RubyRequest::Eval` through the pool and await the typed result.
async fn eval(pool: &RubyPool, source: &str) -> Result<serde_json::Value, String> {
    let (tx, rx) = oneshot::channel();
    pool.next_sender()
        .send(RubyRequest::Eval {
            source: source.to_string(),
            respond: tx,
        })
        .await
        .map_err(|_| "ruby channel closed".to_string())?;
    rx.await
        .map_err(|_| "eval reply lost".to_string())?
        .map_err(|e| e.to_string())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dual_gem_load_reproduces_uninitialized_open_source_repo() {
    // ── Temp roots, wired to the env the compile path validates against ──
    // Canonicalize the root so paths match the env base (macOS /var → /private/var
    // symlink; the compile-path base check uses canonicalize, so the env must too).
    let tmp = std::env::temp_dir().join(format!("pangea-dualgem-{}", std::process::id()));
    std::fs::create_dir_all(&tmp).expect("mkdir tmp root");
    let tmp = std::fs::canonicalize(&tmp).expect("canonicalize tmp root");
    let gem_cache = tmp.join("gems");
    let ws_base = tmp.join("workspaces");
    let stubs = tmp.join("stubs"); // dry-struct stub — NOT under any purge prefix
    std::env::set_var("PANGEA_GEM_CACHE_DIR", &gem_cache);
    std::env::set_var("PANGEA_WORKSPACE_BASE", &ws_base);

    // dry-struct stub on its own lib dir (survives the purge).
    write_file(&stubs.join("dry-struct.rb"), DRY_STRUCT_STUB);

    // GEM copy: the baked gem mirror.
    let gem_lib = gem_cache.join(GEM_DIR).join("lib");
    write_file(&gem_lib.join("pangea/architectures/types.rb"), TYPES_RB);

    // WORKSPACE clone copy: byte-identical body, DIFFERENT abs path (the second
    // logical-but-distinct file Ruby's $LOADED_FEATURES dedup fails across).
    let ws_repo = ws_base.join("pleme-io-opensource/pleme-io-opensource/_repo");
    let ws_lib = ws_repo.join("lib");
    write_file(&ws_lib.join("pangea/architectures/types.rb"), TYPES_RB);

    // The git-source template: requires types + references the masked constant.
    let template_path = ws_repo.join("pleme_io_opensource.rb");
    write_file(
        &template_path,
        "require 'pangea/architectures/types'\n\
         template :pleme_io_opensource do\n\
         \x20 _ = Pangea::Architectures::OpenSourceRepo\n\
         end\n",
    );

    // ── Boot ONE long-lived VM (1 worker, no gem at boot — matches prod) ──
    let pool = Arc::new(RubyPool::spawn(1, vec![]).await.expect("spawn ruby pool"));

    // Put the dry-struct stub on $LOAD_PATH (permanent) so both copies' require
    // of 'dry-struct' resolves to the single surviving registry.
    pool.broadcast_prepend_load_path(stubs.clone())
        .await
        .expect("prepend stubs");

    // A minimal TerraformSynthesizer so the compile's synthesis phase completes
    // (production bundles terraform-synthesizer; the harness doesn't). Lets a
    // SUCCESSFUL mirror compile return Ok — the real proof the dual-load is gone.
    eval(
        &pool,
        "class TerraformSynthesizer; def synthesis; {}; end; end; nil",
    )
    .await
    .expect("define TerraformSynthesizer stub");

    // STEP A — prime the gem copy: prepend the gem lib, require types once →
    // OpenSourceRepo defined, attribute :cluster_name registered once.
    pool.broadcast_prepend_load_path(gem_lib.clone())
        .await
        .expect("prepend gem lib");
    eval(&pool, "require 'pangea/architectures/types'; nil")
        .await
        .expect("STEP A: the gem copy loads cleanly once");
    // Sanity: it's defined exactly once now.
    let defined = eval(
        &pool,
        "Pangea::Architectures.const_defined?(:OpenSourceRepo)",
    )
    .await
    .unwrap();
    assert_eq!(
        defined,
        serde_json::json!(true),
        "STEP A primed OpenSourceRepo"
    );

    // ── PROBE (M1) — capture the EXACT Ruby mechanism, with its message ──
    // Re-load the CLONE copy directly (a second execution of the same logical
    // file at a distinct abs path) and report class+message. This is what the
    // purge+reload does under the hood; surfacing the message tells M1 whether
    // the stub model is faithful to real dry-struct.
    // NOTE: bracket $LOAD_PATH (save/restore) — the probe must NOT pollute the
    // VM's $LOAD_PATH for STEP B (an un-bracketed unshift would permanently put the
    // clone at the head, making STEP B's require resolve to the clone and defeating
    // the skip-prepend fix — a harness artifact, not the real path).
    let probe = eval(
        &pool,
        &format!(
            "saved = $LOAD_PATH.dup; $LOAD_PATH.unshift({:?}); begin; load({:?}); 'NO-RAISE'; rescue Exception => e; e.class.to_s + ': ' + e.message; ensure; $LOAD_PATH.replace(saved); end",
            ws_lib.to_string_lossy(),
            ws_lib.join("pangea/architectures/types.rb").to_string_lossy(),
        ),
    )
    .await
    .expect("probe eval runs");
    eprintln!("M1 PROBE (direct clone re-load) => {probe}");
    // The probe INDEPENDENTLY confirms the exact mechanism: re-executing the clone
    // copy re-runs `attribute :cluster_name` on the re-defined class → raises. This
    // is the production dual-load defect, modeled hermetically (dry-struct's global
    // attribute registry survives the constant purge, keyed by class NAME).
    assert!(
        probe.to_string().contains("has already been defined"),
        "M1: the dual-load mechanism did not reproduce in the probe. Got: {probe}"
    );

    // DEBUG: VM state going into STEP B (is the gem types.rb already loaded?).
    let state = eval(
        &pool,
        "[$LOAD_PATH.first(3), $LOADED_FEATURES.grep(/architectures.types/), Pangea::Architectures.const_defined?(:OpenSourceRepo)].inspect",
    )
    .await
    .unwrap();
    eprintln!("PRE-STEP-B VM STATE => {state}");

    // ── STEP B — the git-source compile: the real backend purge+reload path ──
    let backend = EmbeddedCompilerBackend::new(pool.clone());
    let req = CompileRequest {
        source: None,
        template_path: Some(template_path.to_string_lossy().into_owned()),
        rubylib_paths: vec![ws_lib.to_string_lossy().into_owned()],
        variables: Default::default(),
        template_name: Some("pleme_io_opensource".into()),
    };
    let result = backend.compile(req).await;

    // ── POST-FIX assertion (GREEN): the gem-mirror skip-prepend fix detects that
    // the clone lib mirrors the baked gem, so it is NOT prepended and NOT purged →
    // the template's `require` resolves to the already-loaded gem copy (single
    // execution) → no Dry::Struct redefinition → OpenSourceRepo resolves → compile
    // succeeds. (The PROBE above shows the raw double-load STILL raises — the fix
    // works by AVOIDING the second execution, not by suppressing the symptom.) ──
    match &result {
        Ok(_) => eprintln!(
            "M2 FIX VERIFIED: the dual-mount compile now SUCCEEDS (gem-mirror skip-prepend)"
        ),
        Err(e) => {
            let msg = e.to_string();
            // A regression would surface the redefinition / uninitialized constant.
            assert!(
                !msg.contains("has already been defined")
                    && !msg.contains("RuntimeError")
                    && !msg
                        .contains("uninitialized constant Pangea::Architectures::OpenSourceRepo"),
                "FIX REGRESSED — the dual-load failure is back: {msg}"
            );
            panic!(
                "the mirror compile failed for an unrelated reason (not the dual-load bug): {msg}"
            );
        }
    }
    assert!(
        result.is_ok(),
        "the gem-mirror compile must succeed with the skip-prepend fix"
    );

    let _keep: &Arc<RubyPool> = &pool; // keep the VM alive to the end
    let _ = PathBuf::from(&tmp); // tmp left for inspection
}
