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
}

