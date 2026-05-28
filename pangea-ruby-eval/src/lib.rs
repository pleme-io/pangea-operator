//! Embedded CRuby evaluator for the Pangea DSL.
//!
//! This crate replaces the `pangea-compiler` Ruby HTTP sidecar with an
//! in-process evaluator. The host (pangea-operator) parses HTTP + JSON +
//! YAML in Rust, hands the evaluator a typed input, and receives a typed
//! output. Ruby's only job is to evaluate the Pangea DSL against an
//! injected synthesizer object.
//!
//! Backend: [`magnus`] over real CRuby via `rb-sys`. Single interpreter
//! per process (CRuby GVL constraint).
//!
//! # Threading model
//!
//! CRuby is a strictly single-threaded VM: `embed::init()` may only be
//! called once per process, and the resulting `Ruby` token is only
//! valid on the calling thread. This means:
//!
//! - In production, the evaluator MUST be created on a dedicated
//!   long-lived thread; HTTP handlers post evaluation requests to it
//!   via channels.
//! - In tests, all assertions involving Ruby must run on the *same*
//!   thread that called `embed::init()`. cargo's libtest spawns a
//!   fresh thread per `#[test]` even with `--test-threads=1`, so we
//!   bundle all assertions under a single `#[test]` entry point.
//!   See `smoke_all` below.
//!
//! See `theory/PANGEA-WORKSPACE-RECONCILIATION.md` § M8 and
//! `theory/PANGEA-COMPILER-CDEP-AUDIT.md` for the design.

#![allow(clippy::missing_errors_doc)]

use thiserror::Error;

pub mod evaluator;
pub mod fixture;
pub mod value;

pub use evaluator::{
    detect_load_path_conflicts, module_name_to_require_path, CompileContext, Conflict,
    ConflictDetector, ContextWarnings, LoadPathConflict, LoadPathConflictDetector, RubyEvaluator,
};
pub use fixture::{parse_yaml_fixture, short_sha256_hex, ParsedFixture};
pub use value::{json_to_ruby, ruby_hash_to_json, ruby_value_to_json, JsonHash};

#[derive(Debug, Error)]
pub enum EvalError {
    #[error("ruby exception: {0}")]
    RubyException(String),

    #[error("ruby raised but did not return a string message")]
    UnknownRubyException,

    #[error("value conversion: {0}")]
    Conversion(String),

    #[error("interpreter not initialized")]
    NotInitialized,

    #[error("{0}")]
    Other(String),
}

/// Bootstrap CRuby on the current thread and return a `Ruby` token.
///
/// MUST be called only once per process. Panics on second call (CRuby
/// VM-already-initialized assertion). The returned token is valid only
/// on the calling thread.
///
/// In production, host code should call this from a dedicated owner
/// thread and never let the token escape that thread.
///
/// # Safety
///
/// Calling more than once per process panics. Calling from a thread
/// that is not the intended Ruby owner thread is undefined behavior.
pub unsafe fn boot_ruby_unchecked() -> magnus::embed::Cleanup {
    magnus::embed::init()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::evaluator::RubyEvaluator;
    use crate::value::{json_to_ruby, ruby_value_to_json};
    use serde_json::{json, Value as Json};

    /// Single bundled smoke test — all assertions run on one thread to
    /// satisfy CRuby's single-VM-per-process + single-thread-per-VM
    /// constraint. This proves the magnus viability for M8.2.0.
    ///
    /// Each `step_*` is independently meaningful:
    ///
    /// - `step_arithmetic` / `step_string` — magnus links + evaluates.
    /// - `step_value_conversions` — Ruby ↔ serde_json round trips.
    /// - `step_load_paths` / `step_env` — bracketed mutation primitives
    ///   that match `pangea-compiler/app.rb`'s patterns.
    /// - `step_template_capture` — the captured-block + instance_eval
    ///   pattern. The single most-load-bearing magnus capability for
    ///   M8.2's `/compile` route port.
    /// - `step_typed_input_injection` — proves M8.3's contract: Pangea
    ///   Ruby never parses JSON/YAML; Rust hands it a Hash.
    #[test]
    fn smoke_all() {
        // SAFETY: this is the only `embed::init` in the test binary;
        // libtest will run this fn on a single thread for its entire
        // lifetime. `Cleanup` lives until end-of-test via the `_keep_alive`
        // binding.
        let _keep_alive = unsafe { boot_ruby_unchecked() };
        let ruby = magnus::Ruby::get().expect("Ruby::get on owner thread");

        step_arithmetic(&ruby);
        step_string(&ruby);
        step_value_conversions(&ruby);
        step_load_paths();
        step_env();
        step_template_capture();
        step_typed_input_injection();
    }

    fn step_arithmetic(ruby: &magnus::Ruby) {
        let result: i64 = ruby.eval("1 + 1").expect("1 + 1");
        assert_eq!(result, 2, "arithmetic eval failed");
    }

    fn step_string(ruby: &magnus::Ruby) {
        let result: String = ruby
            .eval(r#""hello, " + "pangea""#)
            .expect("string concat");
        assert_eq!(result, "hello, pangea");
    }

    fn step_value_conversions(ruby: &magnus::Ruby) {
        // nil → null
        let v: magnus::Value = ruby.eval("nil").unwrap();
        assert_eq!(ruby_value_to_json(v).unwrap(), Json::Null);

        // hash with mixed types
        let v: magnus::Value = ruby
            .eval(r#"{"name" => "rio", "ports" => [80, 443], "enabled" => true}"#)
            .unwrap();
        assert_eq!(
            ruby_value_to_json(v).unwrap(),
            json!({ "name": "rio", "ports": [80, 443], "enabled": true })
        );

        // symbol keys collapse
        let v: magnus::Value = ruby.eval(r#"{ name: "rio", port: 80 }"#).unwrap();
        assert_eq!(
            ruby_value_to_json(v).unwrap(),
            json!({ "name": "rio", "port": 80 })
        );

        // round-trip
        let original = json!({
            "resource": {
                "cloudflare_record": {
                    "auth_quero_cloud": {
                        "name": "auth",
                        "type": "CNAME",
                        "value": "tunnel.example.com",
                        "ttl": 300,
                    }
                }
            }
        });
        let r = json_to_ruby(ruby, &original).unwrap();
        let back = ruby_value_to_json(r).unwrap();
        assert_eq!(original, back);
    }

    fn step_load_paths() {
        let evaluator = RubyEvaluator::new().expect("new");
        let tmp = std::env::temp_dir();

        let before: bool = evaluator
            .ruby()
            .eval(&format!(
                r#"$LOAD_PATH.include?("{}")"#,
                tmp.to_string_lossy()
            ))
            .unwrap();
        assert!(!before);

        evaluator
            .with_load_paths(&[tmp.clone()], |ev| {
                let inside: bool = ev
                    .ruby()
                    .eval(&format!(
                        r#"$LOAD_PATH.include?("{}")"#,
                        tmp.to_string_lossy()
                    ))
                    .unwrap();
                assert!(inside);
                Ok(())
            })
            .unwrap();

        let after: bool = evaluator
            .ruby()
            .eval(&format!(
                r#"$LOAD_PATH.include?("{}")"#,
                tmp.to_string_lossy()
            ))
            .unwrap();
        assert!(!after, "load path should pop on with_load_paths exit");
    }

    fn step_env() {
        let evaluator = RubyEvaluator::new().expect("new");
        let key = "PANGEA_TEST_VAR_XYZ";
        std::env::remove_var(key);

        evaluator
            .with_env(
                &[(key.to_string(), "hello".to_string())],
                |ev| {
                    let v: String = ev
                        .ruby()
                        .eval(&format!(r#"ENV.fetch("{key}")"#))
                        .unwrap();
                    assert_eq!(v, "hello");
                    Ok(())
                },
            )
            .unwrap();

        assert!(
            std::env::var(key).is_err(),
            "env should be unset after with_env"
        );
    }

    /// THE LOAD-BEARING TEST. The captured-block + instance_eval pattern
    /// from `pangea-compiler/app.rb` lines 439-543. If this works, the
    /// `/compile` route port to magnus is mechanically achievable.
    fn step_template_capture() {
        let evaluator = RubyEvaluator::new().expect("new");
        let result: Json = evaluator
            .eval_string(
                r#"
                $captured_block = nil
                Object.send(:define_method, :template) do |_name, &blk|
                  $captured_block = blk
                end

                template :rio do
                  resource :cloudflare_record, :auth, name: "auth", type: "CNAME"
                  resource :cloudflare_record, :cracha, name: "cracha", type: "CNAME"
                end

                synth = Object.new
                def synth.calls; @calls ||= []; end
                def synth.method_missing(name, *args, **kwargs, &blk)
                  calls << [name.to_s, args.map(&:to_s), kwargs.transform_keys(&:to_s).transform_values(&:to_s)]
                  self
                end

                synth.instance_eval(&$captured_block)
                synth.calls
                "#,
            )
            .expect("eval");

        let expected = json!([
            ["resource", ["cloudflare_record", "auth"], { "name": "auth", "type": "CNAME" }],
            ["resource", ["cloudflare_record", "cracha"], { "name": "cracha", "type": "CNAME" }],
        ]);
        assert_eq!(result, expected);
    }

    fn step_typed_input_injection() {
        let evaluator = RubyEvaluator::new().expect("new");
        let input = json!({
            "name": "rio",
            "zone": "quero.cloud",
            "ingress": [
                { "hostname": "auth.quero.cloud", "service": "http://authentik" },
                { "hostname": "cracha.quero.cloud", "service": "http://cracha" },
            ]
        });

        let r_input = json_to_ruby(evaluator.ruby(), &input).unwrap();
        evaluator
            .ruby()
            .define_variable("$pangea_input", r_input)
            .unwrap();

        let result: Json = evaluator
            .eval_string(
                r#"
                input = $pangea_input
                out = []
                input["ingress"].each do |entry|
                  out << {
                    "type" => "cloudflare_record",
                    "name" => entry["hostname"].split(".").first,
                    "value" => entry["service"],
                  }
                end
                out
                "#,
            )
            .unwrap();

        assert_eq!(
            result,
            json!([
                { "type": "cloudflare_record", "name": "auth", "value": "http://authentik" },
                { "type": "cloudflare_record", "name": "cracha", "value": "http://cracha" },
            ])
        );
    }
}
