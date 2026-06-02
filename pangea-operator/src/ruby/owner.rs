//! Ruby owner thread — owns the magnus interpreter, drives RPC requests.
//!
//! CRuby is single-VM-per-process + single-thread-per-VM. The owner
//! thread is the only place in the operator where `Ruby::get()` is
//! valid. All other code paths (axum handlers, kube-rs reconcilers)
//! talk to it via a `tokio::sync::mpsc` channel + `oneshot` reply.
//!
//! Spawned once at operator startup (when `embedded_ruby` feature is
//! compiled in AND `PANGEA_COMPILER_BACKEND=embedded` at runtime).
//! Cleanly shuts down when the channel sender is dropped or
//! [`RubyOwner::shutdown`] is called.

use pangea_ruby_eval::{
    boot_ruby_unchecked, json_to_ruby, parse_yaml_fixture, RubyEvaluator,
};
use serde_json::Value as Json;
use std::path::Path;
use std::thread::JoinHandle;
use tokio::sync::{mpsc, oneshot};
use tracing::{error, info, warn};

use super::backend::{
    ArchListing, BackendError, CompileRequest, CompileResult, FixtureOutcome, SmokeRequest,
};

/// Typed RPC the controllers send into the owner thread.
pub enum RubyRequest {
    ListArchitectures {
        gem: String,
        respond: oneshot::Sender<Result<ArchListing, BackendError>>,
    },
    SmokeTest {
        req: SmokeRequest,
        respond: oneshot::Sender<Result<FixtureOutcome, BackendError>>,
    },
    /// Compile a Pangea Ruby DSL template — equivalent of
    /// `POST /compile` in `pangea-compiler/app.rb`. M8.4 implements
    /// the captured-block + instance_eval pattern in Rust.
    Compile {
        req: CompileRequest,
        respond: oneshot::Sender<Result<CompileResult, BackendError>>,
    },
    /// Eval an arbitrary string. Used by tests + utility callers.
    /// Returns the Ruby return value as JSON.
    Eval {
        source: String,
        respond: oneshot::Sender<Result<Json, BackendError>>,
    },
    /// Permanently prepend a path to `$LOAD_PATH`. Used after the
    /// gem-cache clones a gem, so that subsequent `require`s in
    /// fixture smoke-tests / template compiles can resolve it.
    /// Idempotent at the Ruby level — duplicate prepends are
    /// harmless ($LOAD_PATH.uniq! is implicit by usage).
    PrependLoadPath {
        path: std::path::PathBuf,
        respond: oneshot::Sender<Result<(), BackendError>>,
    },
    /// Cooperative shutdown — owner thread drains, returns from its
    /// loop, runs CRuby cleanup.
    Shutdown,
}

/// Handle to the owner thread. Cheaply cloneable via `tx_handle`.
pub struct RubyOwner {
    tx: mpsc::Sender<RubyRequest>,
    handle: Option<JoinHandle<()>>,
}

impl RubyOwner {
    /// Spawn the owner thread. Boots CRuby and enters a request loop.
    ///
    /// Returns once the thread has confirmed magnus + Pangea load
    /// succeeded (or returns an error if the boot panicked).
    ///
    /// `gem_paths` is the initial `$LOAD_PATH` prepend list — used to
    /// point the embedded interpreter at the bundled pangea-* gems.
    /// Per-CR gem clones are added/removed via `with_load_paths` later
    /// (M8.4).
    pub async fn spawn(gem_paths: Vec<std::path::PathBuf>) -> Result<Self, BackendError> {
        let (tx, rx) = mpsc::channel::<RubyRequest>(64);
        let (boot_tx, boot_rx) = oneshot::channel::<Result<(), String>>();

        let handle = std::thread::Builder::new()
            .name("pangea-ruby-owner".into())
            .spawn(move || run_owner_loop(gem_paths, rx, boot_tx))
            .map_err(|e| BackendError::Ruby(format!("spawn ruby owner thread: {e}")))?;

        // Wait on boot ack before returning so callers know the
        // interpreter is alive before they send requests. The owner
        // sends Ok(()) immediately after `Init_ruby`.
        let boot = boot_rx
            .await
            .map_err(|_| BackendError::Ruby("ruby owner thread aborted before boot ack".into()))?;
        match boot {
            Ok(()) => {
                info!("ruby owner thread booted");
                Ok(Self {
                    tx,
                    handle: Some(handle),
                })
            }
            Err(e) => Err(BackendError::Ruby(format!("ruby boot failed: {e}"))),
        }
    }

    pub fn tx_handle(&self) -> mpsc::Sender<RubyRequest> {
        self.tx.clone()
    }

    /// Convenience: prepend `path/lib` (the gem's standard public-API
    /// location) to `$LOAD_PATH` so subsequent requires resolve.
    /// Used by the gem-cache integration after a fresh clone.
    pub async fn prepend_load_path(
        &self,
        path: std::path::PathBuf,
    ) -> Result<(), BackendError> {
        let (rtx, rrx) = oneshot::channel();
        self.tx
            .send(RubyRequest::PrependLoadPath {
                path,
                respond: rtx,
            })
            .await
            .map_err(|_| BackendError::Ruby("ruby owner channel closed".into()))?;
        rrx.await
            .map_err(|_| BackendError::Ruby("prepend reply lost".into()))?
    }

    /// Cooperative shutdown. Waits up to ~5s for the owner thread to
    /// drain.
    pub async fn shutdown(mut self) {
        let _ = self.tx.send(RubyRequest::Shutdown).await;
        if let Some(h) = self.handle.take() {
            let _ = std::thread::Builder::new()
                .name("pangea-ruby-owner-joiner".into())
                .spawn(move || {
                    if let Err(e) = h.join() {
                        warn!(?e, "ruby owner thread panicked during shutdown");
                    }
                });
        }
    }
}

fn run_owner_loop(
    gem_paths: Vec<std::path::PathBuf>,
    mut rx: mpsc::Receiver<RubyRequest>,
    boot_tx: oneshot::Sender<Result<(), String>>,
) {
    // SAFETY: this is the dedicated CRuby owner thread; spawn() is
    // called at most once per process.
    let _cleanup = unsafe { boot_ruby_unchecked() };

    let evaluator = match RubyEvaluator::new() {
        Ok(e) => e,
        Err(e) => {
            let _ = boot_tx.send(Err(format!("RubyEvaluator::new: {e}")));
            return;
        }
    };

    // Prepend bundled gem load paths (so `require 'pangea/cloudflare'`
    // resolves). Per-CR clones get added/removed via with_load_paths
    // around individual evaluations later. We do this via an
    // eval_string call (rather than touching magnus types directly)
    // to keep this file's surface narrow — pangea-ruby-eval's API is
    // the seam.
    for p in gem_paths.iter().rev() {
        let escaped = p.to_string_lossy().replace('\\', "\\\\").replace('"', "\\\"");
        let src = format!(r#"$LOAD_PATH.unshift("{escaped}")"#);
        if let Err(e) = evaluator.eval_string(&src) {
            let _ = boot_tx.send(Err(format!("$LOAD_PATH.unshift: {e}")));
            return;
        }
    }

    // Boot succeeded.
    let _ = boot_tx.send(Ok(()));

    while let Some(req) = rx.blocking_recv() {
        match req {
            RubyRequest::ListArchitectures { gem, respond } => {
                let res = list_architectures(&evaluator, &gem);
                let _ = respond.send(res);
            }
            RubyRequest::SmokeTest { req, respond } => {
                let res = smoke_test(&evaluator, &req);
                let _ = respond.send(res);
            }
            RubyRequest::Compile { req, respond } => {
                let res = compile_template(&evaluator, &req);
                let _ = respond.send(res);
            }
            RubyRequest::Eval { source, respond } => {
                let res = evaluator
                    .eval_string(&source)
                    .map_err(|e| BackendError::Ruby(format!("eval: {e}")));
                let _ = respond.send(res);
            }
            RubyRequest::PrependLoadPath { path, respond } => {
                let escaped = path
                    .to_string_lossy()
                    .replace('\\', "\\\\")
                    .replace('"', "\\\"");
                let src = format!(
                    r#"$LOAD_PATH.unshift("{escaped}") unless $LOAD_PATH.include?("{escaped}")"#
                );
                let res = evaluator
                    .eval_string(&src)
                    .map(|_| ())
                    .map_err(|e| BackendError::Ruby(format!("prepend $LOAD_PATH: {e}")));
                let _ = respond.send(res);
            }
            RubyRequest::Shutdown => break,
        }
    }
    info!("ruby owner thread exiting");
}

/// In-process equivalent of `GET /v1/architectures?gem=<gem>`.
///
/// Mirrors `pangea-compiler/app.rb` lines 265-291.
fn list_architectures(
    evaluator: &RubyEvaluator,
    gem: &str,
) -> Result<ArchListing, BackendError> {
    // Try requiring the gem under both the dashed name (canonical
    // gem name; matches the existing pangea-compiler bundler path)
    // and the slashed form (matches gems whose entry file is at
    // lib/<dashed>/<arch>.rb instead of lib/<gem-name>.rb — e.g.
    // pangea-architectures' entry is lib/pangea/architectures.rb).
    // Missing-gem is a typed condition the controller surfaces.
    let dashed = gem.replace('\'', "\\'");
    let slashed = gem.replace('-', "/").replace('\'', "\\'");
    let require_src = format!(r#"
      loaded = false
      ['{dashed}', '{slashed}'].each do |require_path|
        begin
          require require_path
          loaded = true
          break
        rescue LoadError
          next
        end
      end
      loaded ? :ok : :load_error
    "#);
    let _ = evaluator
        .eval_string(&require_src)
        .map_err(|e| BackendError::Ruby(format!("require {gem}: {e}")))?;

    // List Pangea::Architectures.constants. Each const_get triggers
    // autoload which may transitively `require` files the gem ships
    // with dangling references (e.g. pangea-architectures' main has
    // several architecture .rb files that `require 'pangea/helpers/X'`
    // without shipping X). Without per-constant LoadError handling,
    // ANY broken autoload aborts the whole iteration and returns
    // loaded=[]. Be tolerant: skip broken constants, return whichever
    // ones load cleanly. The CR's expectedClasses comparison surfaces
    // missing classes upstream from this layer.
    let listing_json = evaluator
        .eval_string(
            r#"
            classes = []
            if defined?(Pangea::Architectures)
              Pangea::Architectures.constants.each do |c|
                begin
                  const = Pangea::Architectures.const_get(c)
                  if const.is_a?(Class) || const.is_a?(Module)
                    classes << "Pangea::Architectures::#{c}"
                  end
                rescue LoadError, NameError, StandardError
                  # autoload chain hit a missing file or undefined const;
                  # skip this constant and keep iterating
                  next
                end
              end
              classes.sort!
            end
            classes
            "#,
        )
        .map_err(|e| BackendError::Ruby(format!("list constants: {e}")))?;

    let classes: Vec<String> = match listing_json {
        Json::Array(arr) => arr
            .into_iter()
            .filter_map(|v| match v {
                Json::String(s) => Some(s),
                _ => None,
            })
            .collect(),
        _ => Vec::new(),
    };

    // Optional version lookup via Gem.loaded_specs[gem].
    let version_json = evaluator
        .eval_string(&format!(
            r#"
            spec = Gem.loaded_specs['{}'] rescue nil
            spec ? spec.version.to_s : nil
            "#,
            gem.replace('\'', "\\'")
        ))
        .unwrap_or(Json::Null);
    let version = match version_json {
        Json::String(s) => Some(s),
        _ => None,
    };

    Ok(ArchListing {
        gem: gem.to_string(),
        classes,
        version,
    })
}

/// In-process equivalent of `POST /v1/architectures/smoke-test`.
///
/// M8.3: file I/O + SHA-256 + YAML parsing all happen Rust-side. The
/// Ruby evaluator only ever sees a pre-built Hash + the class name to
/// resolve. Mirrors `pangea-compiler/app.rb` lines 293-365 in shape
/// but drops `require 'yaml'` and `require 'digest'` from the Ruby
/// surface.
fn smoke_test(
    evaluator: &RubyEvaluator,
    req: &SmokeRequest,
) -> Result<FixtureOutcome, BackendError> {
    // Step 1: resolve fixture path. Absolute paths are honored as-is;
    // relative paths get joined onto the gem's full_gem_path (which
    // we look up via a tiny Ruby eval — Gem.loaded_specs is the
    // authoritative source for this until M8.4's per-CR clone-cache
    // makes it Rust-side).
    let fixture_path = match resolve_fixture_path(evaluator, &req.gem, &req.fixture_path) {
        Ok(p) => p,
        Err(reason) => {
            return Ok(FixtureOutcome {
                passed: false,
                error: Some(reason),
                input_hash: None,
            })
        }
    };

    if !fixture_path.exists() {
        return Ok(FixtureOutcome {
            passed: false,
            error: Some(format!("Fixture not found: {}", fixture_path.display())),
            input_hash: None,
        });
    }

    // Step 2-4: read + SHA-256 + YAML parse + key-stringify. All Rust.
    let parsed = match parse_yaml_fixture(&fixture_path) {
        Ok(p) => p,
        Err(e) => {
            return Ok(FixtureOutcome {
                passed: false,
                error: Some(format!("Parse fixture: {e}")),
                input_hash: None,
            })
        }
    };

    // Step 5: inject the parsed Hash into Ruby as a global, then run
    // a tiny eval that only does class resolution + .build + synthesis
    // check. No yaml/digest/file requires; Ruby never sees the bytes.
    let r_inputs = match json_to_ruby(evaluator.ruby(), &parsed.inputs) {
        Ok(v) => v,
        Err(e) => {
            return Ok(FixtureOutcome {
                passed: false,
                error: Some(format!("Inject inputs: {e}")),
                input_hash: Some(parsed.input_hash),
            })
        }
    };
    if let Err(e) = evaluator
        .ruby()
        .define_variable("$pangea_inputs", r_inputs)
    {
        return Ok(FixtureOutcome {
            passed: false,
            error: Some(format!("Define $pangea_inputs: {e}")),
            input_hash: Some(parsed.input_hash),
        });
    }

    let eval_src = format!(
        r#"
        begin
          require 'terraform-synthesizer'
          klass = '{klass}'.split("::").reduce(Object) {{ |m, c| m.const_get(c) }}
          synth = TerraformSynthesizer.new
          klass.build(synth, $pangea_inputs)
          result = synth.synthesis
          {{ "passed" => !result.nil? && !result.empty? }}
        rescue StandardError => e
          {{ "passed" => false, "error" => e.message }}
        ensure
          $pangea_inputs = nil
        end
        "#,
        klass = req.class_name.replace('\'', "\\'"),
    );

    let result = evaluator
        .eval_string(&eval_src)
        .map_err(|e| BackendError::Ruby(format!("smoke eval: {e}")))?;

    let obj = match result {
        Json::Object(m) => m,
        _ => {
            return Ok(FixtureOutcome {
                passed: false,
                error: Some("smoke-test returned non-object".into()),
                input_hash: Some(parsed.input_hash),
            })
        }
    };
    let passed = obj
        .get("passed")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let error = obj
        .get("error")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());

    Ok(FixtureOutcome {
        passed,
        error,
        input_hash: Some(parsed.input_hash),
    })
}

/// In-process equivalent of `POST /compile`.
///
/// M8.4: implements the captured-block + instance_eval pattern from
/// `pangea-compiler/app.rb` lines 391-560 in Rust. Two source modes:
///
///   1. `req.source` — legacy inline-eval mode. The Ruby string
///      gets eval'd; the workspace template's `template :name do … end`
///      stashes its block into `$pangea_captured_block` via the
///      Object-level `template` method we install around the eval.
///
///   2. `req.template_path` + `req.rubylib_paths` — gitRepository
///      mode. Validates the path is under PANGEA_WORKSPACE_BASE
///      (default `/var/pangea/workspaces`); brackets `$LOAD_PATH`
///      with rubylib_paths via [`RubyEvaluator::with_load_paths`];
///      `Dir.chdir(File.dirname(path))` so __dir__-relative
///      File.read calls in the workspace .rb resolve correctly;
///      `load(path, true)` to capture the block.
///
/// In both modes the captured block is `synth.instance_eval`'d
/// against a fresh `TerraformSynthesizer` extended with every
/// `Pangea::Resources::*` module. The synthesis Hash is converted
/// back to `serde_json::Value` Rust-side and pretty-serialized — the
/// fourth and final Pangea-Ruby require (`json`) deleted.
///
/// Variables are injected two ways (matching app.rb's contract):
///   - As process ENV vars (workspace templates use
///     `ENV.fetch('CF_API_TOKEN')` for provider creds);
///   - As a `$pangea_variables` Ruby Hash global (so source-mode
///     templates can reach them via that name).
///
/// Both are bracketed via [`RubyEvaluator::with_env`] and a Rust-side
/// `ensure { $pangea_variables = nil }` so concurrent compile
/// requests in different fleets don't see each other's leaks.
fn compile_template(
    evaluator: &RubyEvaluator,
    req: &CompileRequest,
) -> Result<CompileResult, BackendError> {
    if req.source.is_none() && req.template_path.is_none() {
        return Err(BackendError::Compiler(
            "compile request needs either source or template_path".into(),
        ));
    }

    // PANGEA_WORKSPACE_BASE validation (template_path mode only).
    if let Some(path) = req.template_path.as_deref() {
        let base =
            std::env::var("PANGEA_WORKSPACE_BASE").unwrap_or_else(|_| "/var/pangea/workspaces".to_string());
        let real = std::fs::canonicalize(path)
            .map_err(|e| BackendError::Compiler(format!("template_path canonicalize {path}: {e}")))?;
        if !real.starts_with(format!("{base}/")) && !real.starts_with(&base) {
            return Err(BackendError::Compiler(format!(
                "template_path must resolve under PANGEA_WORKSPACE_BASE ({base}): {}",
                real.display()
            )));
        }
        if !real.is_file() {
            return Err(BackendError::Compiler(format!(
                "template_path is not a regular file: {}",
                real.display()
            )));
        }
        // Trusted root set for rubylib entries. Historically this was
        // PANGEA_WORKSPACE_BASE only — correct when the sole entry was
        // the cloned workspace's `_repo/lib`. The magma-rubygems
        // resolver (resolve_ruby_env_for_roots) now produces a
        // dependency-ordered closure whose entries legitimately live in
        // the per-CR gem cache (PANGEA_GEM_CACHE_DIR, default
        // /var/pangea/gems) and the image's baked Nix closure
        // (/nix/store/...). All three are operator-trusted roots — the
        // same trust boundary the GemRootsLocator + ManifestLocator
        // resolve against — so admit any entry under one of them.
        let gem_cache_base = std::env::var("PANGEA_GEM_CACHE_DIR")
            .unwrap_or_else(|_| "/var/pangea/gems".to_string());
        let trusted_roots = [base.clone(), gem_cache_base, "/nix/store".to_string()];
        for rl in &req.rubylib_paths {
            let r = std::fs::canonicalize(rl)
                .map_err(|e| BackendError::Compiler(format!("rubylib_path canonicalize {rl}: {e}")))?;
            let under_trusted = trusted_roots
                .iter()
                .any(|t| r.starts_with(format!("{t}/")) || r.starts_with(t));
            if !under_trusted {
                return Err(BackendError::Compiler(format!(
                    "rubylib_paths entries must resolve under a trusted root \
                     (workspace base {base}, gem cache, or /nix/store): {rl}"
                )));
            }
            if !r.is_dir() {
                return Err(BackendError::Compiler(format!(
                    "rubylib_path is not a directory: {rl}"
                )));
            }
        }
    }

    // Variables → ENV-overrides Vec + injectable Ruby Hash. Every
    // value gets stringified for ENV (matches app.rb's `ENV[key] = v.to_s`).
    let env_overrides: Vec<(String, String)> = req
        .variables
        .iter()
        .map(|(k, v)| {
            let s = match v {
                serde_json::Value::String(s) => s.clone(),
                _ => v.to_string(),
            };
            (k.clone(), s)
        })
        .collect();

    let variables_json: serde_json::Value = serde_json::Value::Object(
        req.variables
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect(),
    );
    let r_vars = pangea_ruby_eval::json_to_ruby(evaluator.ruby(), &variables_json)
        .map_err(|e| BackendError::Ruby(format!("inject variables: {e}")))?;
    evaluator
        .ruby()
        .define_variable("$pangea_variables", r_vars)
        .map_err(|e| BackendError::Ruby(format!("define $pangea_variables: {e}")))?;

    // Build the structured compile manifest. Replaces the ad-hoc
    // with_env + with_load_paths + purge triplet with one transactional
    // primitive that:
    //   1. validates the manifest's shape,
    //   2. runs the algorithmic shield (load-path conflict detector)
    //      against the resulting $LOAD_PATH and emits warnings,
    //   3. purges configured modules + $LOADED_FEATURES entries,
    //   4. brackets ENV + $LOAD_PATH (drop-guarded restore),
    //   5. runs the body inside the controlled state.
    //
    // See `pangea_ruby_eval::CompileContext` doc for the full design
    // + memory/project_ruby_pool_double_load_fix.md for the bug
    // class this defends against.
    let load_paths: Vec<std::path::PathBuf> = req
        .rubylib_paths
        .iter()
        .map(std::path::PathBuf::from)
        .collect();

    let ctx = pangea_ruby_eval::CompileContext::new()
        .with_load_paths(load_paths)
        .with_env(env_overrides.clone())
        // Pangea::Architectures is the canonical bug-prone module
        // (Dry::Struct classes with strict attribute-redefinition
        // semantics; loaded by prepare_gem at runtime + by every
        // workspace compile). Purging before each compile gives the
        // workspace a fresh definition surface.
        //
        // Sibling namespaces (Pangea::Resources, Pangea::Helpers) are
        // NOT purged here: production attempts to extend the purge
        // list risk re-require cascades that hit Ruby's stack limit
        // (the bug we already paid for once on 2026-05-28). The
        // structural answer is the planner's source classification +
        // module-discovery loop (project_controller_detection_axis.md)
        // — derive the purge surface from the broadcast gem's actual
        // load tree, not from a hardcoded list that drifts.
        .with_purge_modules(["Pangea::Architectures"])
        // Scoped tightly to the bundle's exact path. Earlier attempts
        // used /nix/store/ — too broad, unloaded pangea-core +
        // dry-struct + dry-types, triggering re-require cascades that
        // hit Ruby's stack limit. Only purge what the workspace will
        // genuinely shadow.
        .with_purge_feature_prefixes([
            "/var/pangea/gems/pangea-architectures-main/",
        ]);

    let (synthesis_json, warnings) = evaluator
        .compile_in_context(&ctx, |ev| {
            // Inner returns BackendError; translate to EvalError at
            // the bracket boundary. The outer ? maps back via the
            // From impl on backend.rs.
            run_capture_and_synthesize(ev, req).map_err(|e| {
                pangea_ruby_eval::EvalError::Other(format!("compile: {e}"))
            })
        })
        .map_err(BackendError::from)?;

    // Surface manifest + conflict warnings as a single log line per
    // compile. These are the audit trail — every observable
    // invariant violation lands here, structured + greppable.
    for msg in &warnings.messages {
        tracing::warn!(
            template = req.template_name.as_deref().unwrap_or("?"),
            warning = %msg,
            "compile-context warning"
        );
    }

    // Pretty-serialize Rust-side for the disk/tofu consumer. JSON.
    // pretty_generate on the Ruby side disappears with this — third
    // Pangea-Ruby require deleted. Carry the typed value alongside
    // so in-process consumers (magma plan, preview, equivalence
    // tests) skip the re-parse round-trip; see
    // theory/IN-MEMORY-PIPELINE.md.
    let terraform_json = serde_json::to_string_pretty(&synthesis_json).map_err(|e| {
        BackendError::Compiler(format!("serialize synthesis to JSON: {e}"))
    })?;

    Ok(CompileResult {
        terraform_json,
        synthesis_value: Some(synthesis_json),
    })
}

/// The inner eval — must run while `with_env` + `with_load_paths`
/// are still active. Installs the toplevel `template` capture method
/// + a `$pangea_captured_block` global, evals/loads the source,
/// runs `synth.instance_eval(&captured_block)`, returns
/// `synth.synthesis` as JSON.
fn run_capture_and_synthesize(
    evaluator: &RubyEvaluator,
    req: &CompileRequest,
) -> Result<serde_json::Value, BackendError> {
    // Phase 1: install the capture method + run the source/load.
    let load_phase = if let Some(path) = req.template_path.as_deref() {
        let escaped_path = path.replace('\\', "\\\\").replace('"', "\\\"");
        // chdir + load(path, true). `wrap=true` isolates the loaded
        // file's constants in an anonymous module so repeated /compile
        // calls don't accumulate const namespaces.
        format!(
            r#"
            Dir.chdir(File.dirname("{escaped_path}")) do
              load("{escaped_path}", true)
            end
            "#,
        )
    } else {
        // Inline source mode. We eval at the toplevel binding so
        // `template` is in scope.
        let source = req.source.as_deref().unwrap_or("");
        // The source string can be arbitrary Ruby; we wrap it in a
        // here-doc-shaped Ruby literal to keep eval correct against
        // arbitrary content (including embedded backticks/quotes).
        // Use eval with a sentinel binding context so SyntaxError
        // surfaces with a useful filename.
        format!(
            r#"
            eval({source_literal}, TOPLEVEL_BINDING, "(pangea-template)", 1)
            "#,
            source_literal = ruby_string_literal(source),
        )
    };

    let main_src = format!(
        r#"
        $pangea_captured_block = nil
        begin
          Object.send(:define_method, :template) do |_name, &blk|
            $pangea_captured_block = blk
          end
          {load_phase}
          if $pangea_captured_block.nil?
            raise "no template :name do … end block found"
          end

          synth =
            if defined?(TerraformSynthesizer)
              s = TerraformSynthesizer.new
              if defined?(Pangea::Resources)
                Pangea::Resources.constants.each do |c|
                  m = Pangea::Resources.const_get(c)
                  s.extend(m) if m.is_a?(Module) && !m.is_a?(Class)
                end
              end
              s
            else
              raise "TerraformSynthesizer not loaded — bundle terraform-synthesizer or set PANGEA_COMPILER_BACKEND=http"
            end
          synth.instance_eval(&$pangea_captured_block)
          out = synth.synthesis
          # Finalize: auto-derive terraform.required_providers from the
          # resources this workspace emitted, so the rendered config satisfies
          # magma's preflight laws A2/A3 (every used provider declared). tofu's
          # resolution is unchanged — the providers it inferred from the
          # resource prefixes, now stated explicitly. Guarded for older
          # pangea-core that predates the registry. See Pangea::ProviderRegistry
          # + theory/EXECUTOR-MIGRATION.md.
          Pangea::ProviderRegistry.inject_into_synthesis(out) if defined?(Pangea::ProviderRegistry)
          out
        ensure
          if Object.method_defined?(:template, true) || Object.private_method_defined?(:template, true)
            Object.send(:remove_method, :template) rescue nil
          end
          $pangea_captured_block = nil
        end
        "#,
    );

    let result = evaluator
        .eval_string(&main_src)
        .map_err(|e| BackendError::Compiler(format!("compile eval: {e}")))?;

    // Clear the variables global on the way out so subsequent
    // compiles don't see this one's leak.
    let _ = evaluator.eval_string(r#"$pangea_variables = nil"#);

    Ok(result)
}

// `ruby_string_literal` was lifted to `super::escape::ruby_string_literal`
// during U1 so its eval-safety tests can run without the
// `embedded_ruby` feature (which requires system Ruby for build).
use super::escape::ruby_string_literal;

/// Resolve a fixture path. Absolute paths pass through; relative
/// paths require the gem to be loaded so we can look up its
/// `full_gem_path`. Returns a typed-failure reason string when the
/// gem isn't loaded — caller surfaces it as a FixtureOutcome.
fn resolve_fixture_path(
    evaluator: &RubyEvaluator,
    gem: &str,
    fixture_path: &str,
) -> Result<std::path::PathBuf, String> {
    if Path::new(fixture_path).is_absolute() {
        return Ok(std::path::PathBuf::from(fixture_path));
    }

    let lookup_src = format!(
        r#"
        spec = Gem.loaded_specs['{gem}']
        spec ? spec.full_gem_path : nil
        "#,
        gem = gem.replace('\'', "\\'"),
    );
    let result = evaluator
        .eval_string(&lookup_src)
        .map_err(|e| format!("Gem path lookup: {e}"))?;
    match result {
        Json::String(gem_path) => Ok(Path::new(&gem_path).join(fixture_path)),
        _ => Err(format!("Gem not loaded: {gem}")),
    }
}

impl Drop for RubyOwner {
    fn drop(&mut self) {
        // Best-effort: if the user didn't await shutdown(), we at
        // least signal the owner thread to exit.
        let tx = self.tx.clone();
        let _ = tx.try_send(RubyRequest::Shutdown);
        if let Some(h) = self.handle.take() {
            // Don't block the drop; spawn a joiner.
            let _ = std::thread::Builder::new()
                .name("pangea-ruby-owner-drop-joiner".into())
                .spawn(move || {
                    if let Err(e) = h.join() {
                        error!(?e, "ruby owner thread panicked during drop");
                    }
                });
        }
    }
}

