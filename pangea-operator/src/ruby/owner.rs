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

use pangea_ruby_eval::{boot_ruby_unchecked, RubyEvaluator};
use serde_json::Value as Json;
use std::thread::JoinHandle;
use tokio::sync::{mpsc, oneshot};
use tracing::{error, info, warn};

use super::backend::{ArchListing, BackendError, FixtureOutcome, SmokeRequest};

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
    /// Eval an arbitrary string. Used by tests + by the future
    /// `/compile` route port. Returns the Ruby return value as JSON.
    Eval {
        source: String,
        respond: oneshot::Sender<Result<Json, BackendError>>,
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
            RubyRequest::Eval { source, respond } => {
                let res = evaluator
                    .eval_string(&source)
                    .map_err(|e| BackendError::Ruby(format!("eval: {e}")));
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
    // Try requiring the gem; missing-gem is a typed condition the
    // controller surfaces, so we absorb LoadError into an empty
    // listing rather than propagating.
    let require_src = format!(r#"
      begin
        require '{}'
        :ok
      rescue LoadError => e
        :load_error
      end
    "#, gem.replace('\'', "\\'"));
    let _ = evaluator
        .eval_string(&require_src)
        .map_err(|e| BackendError::Ruby(format!("require {gem}: {e}")))?;

    // List Pangea::Architectures.constants — same logic as the sidecar.
    let listing_json = evaluator
        .eval_string(
            r#"
            classes = []
            if defined?(Pangea::Architectures)
              classes = Pangea::Architectures.constants.map do |c|
                full = "Pangea::Architectures::#{c}"
                const = Pangea::Architectures.const_get(c)
                if const.is_a?(Class) || const.is_a?(Module)
                  full
                else
                  nil
                end
              end.compact.sort
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
/// Mirrors `pangea-compiler/app.rb` lines 293-365. YAML fixture parsing
/// will move to Rust in M8.3 — for now we let Ruby parse so this fn
/// stays drop-in equivalent to the sidecar.
fn smoke_test(
    evaluator: &RubyEvaluator,
    req: &SmokeRequest,
) -> Result<FixtureOutcome, BackendError> {
    let src = format!(
        r#"
        require 'yaml'
        require 'digest'
        require 'terraform-synthesizer'

        gem_spec = Gem.loaded_specs['{gem}']
        if gem_spec.nil?
          {{ "passed" => false, "error" => "Gem not loaded: {gem}" }}
        else
          fixture_path = '{fixture}'.start_with?('/') ?
                         '{fixture}' :
                         File.join(gem_spec.full_gem_path, '{fixture}')
          if !File.exist?(fixture_path)
            {{ "passed" => false, "error" => "Fixture not found: #{{fixture_path}}" }}
          else
            begin
              raw = File.read(fixture_path)
              input_hash = Digest::SHA256.hexdigest(raw)[0, 12]
              inputs = YAML.safe_load(raw, permitted_classes: [Symbol], aliases: true) || {{}}

              klass = '{klass}'.split("::").reduce(Object) {{ |mod, c| mod.const_get(c) }}

              synth = TerraformSynthesizer.new
              str_inputs = inputs.transform_keys(&:to_s)
              klass.build(synth, str_inputs)
              result = synth.synthesis
              passed = !result.nil? && !result.empty?
              {{ "passed" => passed, "input_hash" => input_hash }}
            rescue StandardError => e
              {{ "passed" => false, "error" => e.message }}
            end
          end
        end
        "#,
        gem = req.gem.replace('\'', "\\'"),
        klass = req.class_name.replace('\'', "\\'"),
        fixture = req.fixture_path.replace('\'', "\\'"),
    );

    let result = evaluator
        .eval_string(&src)
        .map_err(|e| BackendError::Ruby(format!("smoke eval: {e}")))?;

    let obj = match result {
        Json::Object(m) => m,
        _ => {
            return Err(BackendError::Ruby(
                "smoke-test returned non-object".into(),
            ))
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
    let input_hash = obj
        .get("input_hash")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());

    Ok(FixtureOutcome {
        passed,
        error,
        input_hash,
    })
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
