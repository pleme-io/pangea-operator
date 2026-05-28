//! High-level evaluator API.
//!
//! `RubyEvaluator` is the only type host code (pangea-operator) needs to
//! know about. It bootstraps CRuby on first use, manages `$LOAD_PATH`,
//! and exposes a small set of evaluation primitives that mirror the
//! existing pangea-compiler RPC shapes.
//!
//! Mirrors the Ruby-side patterns from `pangea-compiler/app.rb` 1:1, but
//! with Rust managing all the bracketing (`$LOAD_PATH` push/pop, ENV
//! restore, captured-block lifecycle).

use crate::value::ruby_value_to_json;
use crate::EvalError;
use magnus::{RArray, Ruby, Value};
use serde_json::Value as Json;
use std::path::{Path, PathBuf};

/// Owns a CRuby interpreter and exposes evaluator primitives.
///
/// One per process. Pinned to the thread that called [`RubyEvaluator::new`].
pub struct RubyEvaluator {
    ruby: Ruby,
}

impl RubyEvaluator {
    /// Construct from an existing `Ruby` thread token. The caller is
    /// responsible for having bootstrapped CRuby on the current thread
    /// (see [`crate::boot_ruby_unchecked`]).
    pub fn new() -> Result<Self, EvalError> {
        let ruby = magnus::Ruby::get()
            .map_err(|e| EvalError::Other(format!("Ruby::get failed: {e:?}")))?;
        Ok(Self { ruby })
    }

    /// Direct access to the underlying `magnus::Ruby` token. Most callers
    /// should not need this; provided for tests and for advanced use
    /// cases (defining additional methods on the global Object).
    pub fn ruby(&self) -> &Ruby {
        &self.ruby
    }

    /// Evaluate an arbitrary Ruby string, returning the result converted
    /// to JSON. Used by tests and trivial RPCs (e.g. `/healthz`-style
    /// liveness checks).
    pub fn eval_string(&self, source: &str) -> Result<Json, EvalError> {
        let val: Value = self
            .ruby
            .eval(source)
            .map_err(|e| EvalError::RubyException(format!("{e}")))?;
        ruby_value_to_json(val)
    }

    /// Push paths onto `$LOAD_PATH` for the duration of `f`.
    ///
    /// Mirrors the bracketed `$LOAD_PATH.unshift(*paths)` /
    /// `paths.length.times { $LOAD_PATH.shift }` dance in
    /// `pangea-compiler/app.rb`. Rust's drop-on-error ensures the pop
    /// happens even if `f` panics.
    pub fn with_load_paths<F, T>(&self, paths: &[PathBuf], f: F) -> Result<T, EvalError>
    where
        F: FnOnce(&Self) -> Result<T, EvalError>,
    {
        let count = paths.len();
        if count == 0 {
            return f(self);
        }

        // Validate every path exists + is a directory; surface a typed
        // error before we touch $LOAD_PATH (matches app.rb's
        // realdirpath validation, but Rust-side).
        for p in paths {
            if !p.is_dir() {
                return Err(EvalError::Other(format!(
                    "load_paths entry is not a directory: {}",
                    p.display()
                )));
            }
        }

        let load_path: RArray = self
            .ruby
            .eval("$LOAD_PATH")
            .map_err(|e| EvalError::Other(format!("$LOAD_PATH lookup: {e}")))?;

        // Push (in reverse so the first path ends up at index 0 — matches
        // Ruby's $LOAD_PATH.unshift(*paths) semantics where *paths is
        // splatted left-to-right).
        for p in paths.iter().rev() {
            let s = self.ruby.str_new(&p.to_string_lossy());
            load_path
                .unshift(s)
                .map_err(|e| EvalError::Other(format!("$LOAD_PATH.unshift: {e}")))?;
        }

        // Run the body. Use a guard to ensure pop even on Err return.
        struct PopGuard<'a> {
            load_path: RArray,
            count: usize,
            _phantom: std::marker::PhantomData<&'a ()>,
        }
        impl<'a> Drop for PopGuard<'a> {
            fn drop(&mut self) {
                for _ in 0..self.count {
                    let _ = self.load_path.shift::<Value>();
                }
            }
        }
        let _guard = PopGuard {
            load_path,
            count,
            _phantom: std::marker::PhantomData,
        };

        f(self)
    }

    /// Set ENV variables for the duration of `f`. Restores prior state
    /// (including unset → unset) on exit. Matches the env_overrides
    /// dance in app.rb's `/compile`.
    pub fn with_env<F, T>(&self, vars: &[(String, String)], f: F) -> Result<T, EvalError>
    where
        F: FnOnce(&Self) -> Result<T, EvalError>,
    {
        // Capture prior values (None == unset).
        let mut prior: Vec<(String, Option<String>)> = Vec::with_capacity(vars.len());
        for (k, v) in vars {
            prior.push((k.clone(), std::env::var(k).ok()));
            std::env::set_var(k, v);
        }

        struct EnvGuard {
            prior: Vec<(String, Option<String>)>,
        }
        impl Drop for EnvGuard {
            fn drop(&mut self) {
                for (k, v) in self.prior.drain(..) {
                    match v {
                        Some(prev) => std::env::set_var(&k, prev),
                        None => std::env::remove_var(&k),
                    }
                }
            }
        }
        let _guard = EnvGuard { prior };

        f(self)
    }

    /// Load a Ruby file from disk into the interpreter (equivalent of
    /// `load(path, true)` — wrap=true isolates constants to a private
    /// anonymous module).
    ///
    /// Used by `/compile` to load a workspace's `<template>.rb` file
    /// from a clone-tree dir under `PANGEA_WORKSPACE_BASE`.
    pub fn load_file(&self, path: &Path) -> Result<(), EvalError> {
        let path_str = path.to_string_lossy();
        let escaped = path_str.replace('\\', "\\\\").replace('"', "\\\"");
        let src = format!(r#"load("{escaped}", true)"#);
        let _: Value = self
            .ruby
            .eval(&src)
            .map_err(|e| EvalError::RubyException(format!("{e}")))?;
        Ok(())
    }

    /// Reset CRuby state for a fresh workspace compile.
    ///
    /// ## The bug this exists to fix
    ///
    /// `pangea-ruby-eval` runs ONE long-lived CRuby interpreter per
    /// process (Magnus can't boot a second one — `Ruby already
    /// initialized`). State accumulates across compiles: `$LOAD_PATH`
    /// gets new dirs, `$LOADED_FEATURES` remembers every file ever
    /// loaded, and module constants survive.
    ///
    /// When `prepare_gem` (`ruby/embedded_backend.rs`) clones
    /// `pangea-architectures` into `/var/pangea/gems/.../lib` and
    /// broadcasts it onto `$LOAD_PATH`, the bundled gem's `types.rb`
    /// gets `require`-loaded — defining `Pangea::Architectures::Types::*`
    /// as `Dry::Struct` classes with strict attribute-redefinition
    /// semantics.
    ///
    /// Later, a workspace compile pushes its own `_repo/lib` onto
    /// `$LOAD_PATH` (so workspaces can ride newer architectures
    /// versions than the bundle). The workspace's
    /// `pangea/architectures/types.rb` resolves to a DIFFERENT
    /// absolute path. Ruby's `$LOADED_FEATURES` deduplicates by
    /// absolute path, so it loads BOTH. The second load reopens the
    /// existing `Dry::Struct` classes and re-runs `attribute
    /// :cluster_name, …` — which fatally raises
    /// `"Attribute :cluster_name has already been defined"`.
    ///
    /// Observed on rio 2026-05-28 wedging both pleme-io-opensource
    /// (magma, 1054 resources) and cloudflare-pleme (tofu) into
    /// `Phase::Failed` after the slice-2c-part-2 deploy; the bug
    /// was latent for months (intermittent on pod restart ordering).
    ///
    /// ## What this method does
    ///
    /// Before each workspace compile, undo the side-effects of any
    /// prior bundle load that the workspace's own `_repo/lib` is
    /// about to shadow:
    ///
    ///   1. Drop `$LOADED_FEATURES` entries whose path matches any
    ///      `feature_path_prefixes` (typically the bundled gem cache
    ///      dir — `/var/pangea/gems/`). Ruby's require-dedup forgets
    ///      those files; the upcoming workspace require will reload
    ///      them from the workspace's lib.
    ///   2. `Object.send(:remove_const, …)` on each module in
    ///      `modules_to_purge` (typically `["Pangea::Architectures"]`).
    ///      Removes the constant + every nested class so the
    ///      workspace's load DEFINES them fresh (no reopening, no
    ///      `Dry::Struct` redefine error).
    ///
    /// State is NOT restored on exit. The newly-loaded workspace
    /// version of `Pangea::Architectures` survives the compile.
    /// Subsequent compiles re-purge + reload. Operator-side code
    /// that introspects `Pangea::Architectures` (list_architectures,
    /// smoke_test) sees whichever workspace compiled most recently.
    /// That's acceptable because every architecture-introspecting
    /// RPC is self-bracketing — it triggers its own require chain
    /// before reading constants.
    ///
    /// ## Why not restore on exit?
    ///
    /// Restoration would require re-requiring the bundle's
    /// `types.rb`. But the workspace's load already populated
    /// `Pangea::Architectures` with the workspace's classes — the
    /// bundle's re-require would hit the same redefine error.
    /// Cleaner to leave the workspace's version installed; it
    /// represents the LATEST workspace state on disk anyway.
    pub fn purge_for_workspace_compile(
        &self,
        modules_to_purge: &[&str],
        feature_path_prefixes: &[&str],
    ) -> Result<(), EvalError> {
        // Build a Ruby array literal of the path prefixes — embedded
        // into the eval source as a string-quoted literal. Each
        // prefix is double-escaped (so paths with backslashes / dquotes
        // survive Ruby parsing).
        let prefix_list = feature_path_prefixes
            .iter()
            .map(|p| format!(r#""{}""#, p.replace('\\', "\\\\").replace('"', "\\\"")))
            .collect::<Vec<_>>()
            .join(", ");
        let module_list = modules_to_purge
            .iter()
            .map(|m| format!(r#""{}""#, m.replace('\\', "\\\\").replace('"', "\\\"")))
            .collect::<Vec<_>>()
            .join(", ");

        // ── Drop $LOADED_FEATURES entries under prefix paths. ─────
        // After this, `require 'pangea/architectures/types'` finds
        // the workspace's copy via $LOAD_PATH and loads it fresh.
        //
        // ── Remove module constants. ──────────────────────────────
        // Walks `"Pangea::Architectures"` → owner = `Pangea`, child
        // = `:Architectures`, then `owner.send(:remove_const, child)`.
        // Top-level `Object.send(:remove_const, …)` for un-namespaced
        // modules.
        let src = format!(
            r#"
            prefixes = [{prefix_list}]
            $LOADED_FEATURES.reject! {{ |f| prefixes.any? {{ |p| f.start_with?(p) }} }}

            modules = [{module_list}]
            modules.each do |full_name|
              parts = full_name.split('::')
              child = parts.pop.to_sym
              owner = if parts.empty?
                        Object
                      else
                        # Walk down the parents. If any segment is
                        # already absent we have nothing to purge.
                        parts.inject(Object) do |mod, name|
                          break nil unless mod.const_defined?(name, false)
                          mod.const_get(name, false)
                        end
                      end
              next unless owner && owner.const_defined?(child, false)
              owner.send(:remove_const, child) rescue nil
            end
            "#,
        );
        let _: Value = self
            .ruby
            .eval(&src)
            .map_err(|e| EvalError::RubyException(format!("purge_for_workspace_compile: {e}")))?;
        Ok(())
    }
}

