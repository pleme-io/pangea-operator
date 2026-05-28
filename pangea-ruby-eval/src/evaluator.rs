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
//!
//! ## CompileContext (the structured isolation primitive)
//!
//! CRuby is one-VM-per-process. State accumulates across compiles —
//! `$LOAD_PATH`, `$LOADED_FEATURES`, module constants are all global
//! and process-scoped. Ad-hoc bracketing of subsets (just `$LOAD_PATH`,
//! or just ENV) leaks the rest, leading to bugs that depend on compile
//! ordering or undocumented prior state.
//!
//! `CompileContext` is the structured fix. Each compile declares
//! UPFRONT what state it needs:
//!
//!   * `load_paths`             — `$LOAD_PATH` entries (priority head).
//!   * `env`                    — ENV overrides.
//!   * `purge_modules`          — module constants to remove before
//!                                 compile so workspace's loads define
//!                                 fresh (no Dry::Struct redefine
//!                                 errors). NOT restored.
//!   * `purge_feature_prefixes` — `$LOADED_FEATURES` path prefixes to
//!                                 drop so workspace's require can
//!                                 reload from its own lib.
//!
//! The primitive `compile_in_context` applies the manifest in one
//! transaction: snapshot $LOAD_PATH + ENV, purge modules + features,
//! push load_paths, set env, run body, restore $LOAD_PATH + ENV on
//! drop (purged modules/features are NOT restored — they re-load on
//! demand). Invariants are checked at apply time and emitted as
//! warnings so misconfigurations don't silently fall through to
//! arcane Ruby errors.
//!
//! This makes compile isolation a TYPED + AUDITABLE primitive rather
//! than an accumulation of ad-hoc bracketing.

use crate::value::ruby_value_to_json;
use crate::EvalError;
use magnus::{RArray, Ruby, TryConvert, Value};
use serde_json::Value as Json;
use std::path::{Path, PathBuf};

/// Manifest declaring the Ruby state a single compile needs.
///
/// Each compile builds one of these and hands it to
/// `RubyEvaluator::compile_in_context`. The primitive applies the
/// manifest transactionally, checks invariants, runs the body, and
/// restores process-scoped state on exit.
///
/// The manifest is the explicit, auditable answer to "what does this
/// compile depend on?". Ad-hoc bracketing of subsets (e.g.,
/// `$LOAD_PATH` only) lets implicit prior state leak in — the bugs
/// that flow from that depend on compile ordering, which is not a
/// stable invariant. Manifest-driven isolation is the structured
/// alternative.
#[derive(Debug, Clone, Default)]
pub struct CompileContext {
    /// `$LOAD_PATH` entries to unshift (priority head). Earlier
    /// entries win over later ones; entries here win over any
    /// existing `$LOAD_PATH` entries that fall through.
    pub load_paths: Vec<PathBuf>,

    /// ENV overrides for the compile. Restored on exit (including
    /// unset → unset). Keys not listed here keep their existing
    /// values.
    pub env: Vec<(String, String)>,

    /// Module constants to remove BEFORE compile body runs. Removal
    /// drops the constant + every nested class so the workspace's
    /// load can DEFINE the module fresh (no class-reopening, no
    /// `Dry::Struct` "attribute already defined" errors). Names use
    /// `::` separators (`"Pangea::Architectures"`).
    ///
    /// Purged modules are NOT restored on exit — the workspace's
    /// version stays installed. Subsequent compiles re-purge.
    /// Architecture-introspecting consumers re-load on demand.
    pub purge_modules: Vec<String>,

    /// `$LOADED_FEATURES` path prefixes to drop BEFORE compile body
    /// runs. Ruby's require dedups by absolute path; dropping an
    /// entry here lets the workspace's require re-load the same
    /// logical file from its own `_repo/lib`.
    ///
    /// Scope these to the EXACT bundled paths that the workspace's
    /// `load_paths` will shadow. Over-scoping (e.g. `/nix/store/`)
    /// unloads foundational gems and triggers re-require cascades
    /// that hit Ruby's stack limit — observed on 2026-05-28 (one
    /// SHA before this commit).
    pub purge_feature_prefixes: Vec<String>,
}

/// Result of `CompileContext::validate`. Returned to the caller
/// alongside the compile result so violations are observable +
/// auditable without panicking.
#[derive(Debug, Clone, Default)]
pub struct ContextWarnings {
    /// Validation messages — non-fatal, but a signal that the
    /// manifest may be misconfigured.
    pub messages: Vec<String>,
}

impl ContextWarnings {
    pub fn is_empty(&self) -> bool { self.messages.is_empty() }
}

/// One load-path conflict — a single logical Ruby file (e.g.
/// `"pangea/architectures/types"`) reachable via >1 absolute path
/// in the resulting `$LOAD_PATH`. The canonical cause of class-
/// redefinition errors when Ruby's `require` loads both copies.
///
/// `shadowing_path` is the absolute path that wins resolution (the
/// one earlier in `$LOAD_PATH`); `shadowed_paths` are the ones that
/// would lose if `require` looked them up purely by name. The
/// conflict matters because Ruby's `require` dedups by absolute
/// path, so anything that calls `load` (or constructs an absolute
/// require) on a shadowed path triggers a second load + redefinition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LoadPathConflict {
    /// The logical require name (`"pangea/architectures/types"`).
    pub logical_path: String,
    /// The absolute path Ruby's `require '<logical_path>'` resolves to.
    pub shadowing_path: PathBuf,
    /// Other absolute paths exposing the same logical name. Empty
    /// means no conflict (not returned in that case).
    pub shadowed_paths: Vec<PathBuf>,
}

/// One conflict surfaced by a `ConflictDetector` — the typed shape
/// that flows from any detector through `compile_in_context` into the
/// caller's audit log (today: `tracing::warn!`; slice 4: k8s Events
/// + `.status.anomalies[]`).
///
/// Detectors emit `Conflict`s; the controller routes them. This is
/// the unified shape so a future microservice or GraphQL stream
/// consumes one schema regardless of which detector fired.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Conflict {
    /// Stable detector identifier — `"load_path_double_load"`,
    /// `"tf_address_collision"`, etc. Used as a label in metrics +
    /// event reasons + dashboard filters.
    pub detector: &'static str,

    /// The logical subject of the conflict — the thing observed to
    /// be in trouble. For load-path conflicts: the logical Ruby file
    /// path. For terraform-address conflicts: the resource address.
    /// For env shadowing: the var name. Stable enough to deduplicate
    /// + count.
    pub category: String,

    /// Human-readable summary. Suitable for logs + events; the
    /// `evidence` field carries the structured form for programmatic
    /// consumers.
    pub message: String,

    /// Structured evidence — the specific paths, addresses, or
    /// fields involved. `serde_json::Value` so future sinks
    /// (GraphQL, REST APIs) can transform freely; today's tracing
    /// renders it inline.
    pub evidence: serde_json::Value,
}

/// Trait — a detector that surfaces conflicts in a `CompileContext`'s
/// resulting state. Pure function: given the manifest + current
/// `$LOAD_PATH`, return a list of conflicts. Detectors are stateless
/// + reusable across compile requests.
///
/// ## The seam this trait exists for
///
/// pangea does a lot of preprocessing + DSL processing. Each one has
/// its own bug-class risk surface — file double-load (the canonical
/// case, fixed here), Terraform address collisions, gem version
/// mismatches, env-var shadowing, provider-source drift. Each can be
/// expressed as a detector: pure scan over inputs, typed conflicts
/// out.
///
/// New detectors slot in by:
///   1. Define a struct implementing `ConflictDetector`.
///   2. Add it to the default detector list (or pass via the
///      explicit-detector entry point).
/// 3. Future bug-class regressions get caught at apply time, named
///    + classified, before they become cryptic downstream errors.
///
/// All detectors emit the same `Conflict` shape so the audit
/// surface (today tracing; future Events + status fields + GraphQL
/// subscriptions) reads one schema.
pub trait ConflictDetector: Send + Sync {
    /// Stable identifier. Used as `Conflict.detector` and as the
    /// detector's tracing/metric label.
    fn name(&self) -> &'static str;

    /// Run the detector against the manifest + the current live
    /// `$LOAD_PATH` (for detectors that need it). Returns typed
    /// conflicts; empty = no findings. MUST be pure (no Ruby
    /// state mutation, no I/O beyond what the inputs imply).
    fn detect(
        &self,
        ctx: &CompileContext,
        existing_load_path: &[PathBuf],
    ) -> Vec<Conflict>;
}

/// First-class `ConflictDetector` for the load-path double-load
/// pattern. Wraps `detect_load_path_conflicts` in the trait shape +
/// converts the typed `LoadPathConflict` into the unified `Conflict`.
///
/// The default detector for every workspace compile via
/// `CompileContext::default_detectors`. Future detectors plug in
/// alongside it.
pub struct LoadPathConflictDetector {
    /// Which namespace prefixes to scan (`["pangea/"]` for the
    /// pangea-operator surface). Filtering keeps scan cost bounded
    /// — `O(L × F)` where F is files under the prefix.
    pub namespace_prefixes: Vec<String>,
}

impl LoadPathConflictDetector {
    /// Default detector configured for the pangea-operator surface —
    /// scans the `pangea/` namespace, which covers every gem with a
    /// double-load risk (pangea-architectures, pangea-resources,
    /// pangea-core's submodules).
    pub fn pangea() -> Self {
        Self { namespace_prefixes: vec!["pangea/".to_string()] }
    }
}

impl ConflictDetector for LoadPathConflictDetector {
    fn name(&self) -> &'static str {
        "load_path_double_load"
    }

    fn detect(
        &self,
        ctx: &CompileContext,
        existing_load_path: &[PathBuf],
    ) -> Vec<Conflict> {
        let prefixes: Vec<&str> = self.namespace_prefixes.iter().map(String::as_str).collect();
        detect_load_path_conflicts(existing_load_path, &ctx.load_paths, &prefixes)
            .into_iter()
            .map(|lpc| Conflict {
                detector: "load_path_double_load",
                category: lpc.logical_path.clone(),
                message: format!(
                    "'{}' reachable via {} paths — winner: {}, shadowed: [{}]",
                    lpc.logical_path,
                    lpc.shadowed_paths.len() + 1,
                    lpc.shadowing_path.display(),
                    lpc.shadowed_paths
                        .iter()
                        .map(|p| p.display().to_string())
                        .collect::<Vec<_>>()
                        .join(", ")
                ),
                evidence: serde_json::json!({
                    "logical_path": lpc.logical_path,
                    "shadowing_path": lpc.shadowing_path.display().to_string(),
                    "shadowed_paths": lpc.shadowed_paths
                        .iter()
                        .map(|p| p.display().to_string())
                        .collect::<Vec<_>>(),
                }),
            })
            .collect()
    }
}

/// Scan `$LOAD_PATH` for logical Ruby files reachable via >1 absolute
/// path under any of `namespace_prefixes`.
///
/// ## Why this is a shield, not a check
///
/// In a long-lived CRuby process that handles N workspaces, each with
/// its own `_repo/lib` getting unshifted onto `$LOAD_PATH`, the chance
/// of two paths exposing the same logical file (e.g.
/// `pangea/architectures/types.rb`) grows quadratically with N.
/// Without an algorithmic detector, the bug surfaces as cryptic
/// Ruby errors at compile time — `"Attribute :cluster_name has
/// already been defined"`, `"stack level too deep"`, etc. — that
/// require deep debugging to root-cause.
///
/// With this detector running at every `compile_in_context` entry,
/// the conflict surfaces as a STRUCTURED warning naming the exact
/// logical path + the competing absolute paths. The audit trail
/// makes the bug class diagnosable in seconds rather than hours.
///
/// ## What "logical path" means
///
/// Ruby's `require 'foo/bar'` resolves to the first `foo/bar.rb`
/// found by walking `$LOAD_PATH` in order. The "logical path" is
/// `foo/bar` — the require name without the `.rb` suffix.
///
/// ## Namespace scoping
///
/// `namespace_prefixes` filters which logical paths to scan. The
/// pangea-operator's bug surface is pangea-* gems, so passing
/// `["pangea/"]` covers it. Passing `[""]` would scan EVERYTHING in
/// every `$LOAD_PATH` dir, which is correct but expensive on the
/// operator image's ~150-entry `$LOAD_PATH`.
///
/// ## Cost
///
/// O(L × F) where L is the number of `$LOAD_PATH` entries to scan
/// and F is the total `.rb` files under `namespace_prefixes` in those
/// dirs. For pangea-operator's typical case this is ~hundreds of
/// files; the scan is sub-millisecond on cold disk and effectively
/// free with the OS file cache warm.
pub fn detect_load_path_conflicts(
    existing_load_path: &[PathBuf],
    new_paths: &[PathBuf],
    namespace_prefixes: &[&str],
) -> Vec<LoadPathConflict> {
    // Resulting $LOAD_PATH order: new_paths first (priority head),
    // then existing entries. Mirrors `RArray::unshift` order from
    // `with_load_paths`.
    let combined: Vec<&Path> = new_paths
        .iter()
        .chain(existing_load_path.iter())
        .map(|p| p.as_path())
        .collect();

    // Walk each entry under each namespace prefix; build a map of
    // logical_path → ordered list of absolute paths exposing it.
    use std::collections::BTreeMap;
    let mut by_logical: BTreeMap<String, Vec<PathBuf>> = BTreeMap::new();
    for dir in &combined {
        for prefix in namespace_prefixes {
            let scan_root = dir.join(prefix.trim_end_matches('/'));
            if !scan_root.is_dir() {
                continue;
            }
            scan_rb_files(&scan_root, dir, &mut by_logical);
        }
    }

    // Anything with >1 path is a conflict.
    by_logical
        .into_iter()
        .filter(|(_, paths)| paths.len() > 1)
        .map(|(logical, mut paths)| {
            // First-in-combined wins resolution (Ruby's require
            // resolution order). The remainder are shadowed.
            let shadowing_path = paths.remove(0);
            LoadPathConflict {
                logical_path: logical,
                shadowing_path,
                shadowed_paths: paths,
            }
        })
        .collect()
}

/// Convert a Ruby module-style name (`Pangea::Architectures`) to the
/// conventional Ruby `require` path (`pangea/architectures`).
///
/// Each `::`-separated segment is snake_case'd: insert `_` before
/// any uppercase letter that's preceded by a lowercase letter or
/// digit, then lowercase the whole thing.
///
/// Examples:
///   `Pangea::Architectures`         → `pangea/architectures`
///   `Pangea::Resources::Cloudflare` → `pangea/resources/cloudflare`
///   `Foo::BarBaz`                   → `foo/bar_baz`
///   `Foo::HTTPServer`               → `foo/h_t_t_p_server`  ← acronyms
///
/// The acronym case (HTTPServer → h_t_t_p_server) is deliberately
/// simple — Ruby's actual convention varies. For pangea-* modules
/// (the only consumers today), acronyms aren't present in module
/// names, so the simple rule suffices.
pub fn module_name_to_require_path(module_name: &str) -> String {
    module_name
        .split("::")
        .map(|segment| {
            let mut out = String::with_capacity(segment.len() + 4);
            let mut prev_was_lowercase_or_digit = false;
            for c in segment.chars() {
                if c.is_uppercase() && prev_was_lowercase_or_digit {
                    out.push('_');
                }
                out.push(c.to_ascii_lowercase());
                prev_was_lowercase_or_digit = c.is_lowercase() || c.is_ascii_digit();
            }
            out
        })
        .collect::<Vec<_>>()
        .join("/")
}

/// Recursive helper: walk every `.rb` file under `root`, compute its
/// logical path relative to `load_path_root` (the `$LOAD_PATH` entry
/// it lives under), and append to `acc[logical] = [paths…]`.
fn scan_rb_files(
    root: &Path,
    load_path_root: &Path,
    acc: &mut std::collections::BTreeMap<String, Vec<PathBuf>>,
) {
    let entries = match std::fs::read_dir(root) {
        Ok(e) => e,
        Err(_) => return,
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            scan_rb_files(&path, load_path_root, acc);
        } else if path.extension().and_then(|e| e.to_str()) == Some("rb") {
            // logical_path = path relative to load_path_root, minus .rb
            if let Ok(rel) = path.strip_prefix(load_path_root) {
                let mut s = rel.to_string_lossy().into_owned();
                if let Some(stripped) = s.strip_suffix(".rb") {
                    s = stripped.to_string();
                }
                acc.entry(s).or_default().push(path);
            }
        }
    }
}

impl CompileContext {
    /// Build a context with just `load_paths` populated (the
    /// minimum legal manifest). Most callers add `env` +
    /// `purge_modules` + `purge_feature_prefixes` via the builder
    /// methods.
    pub fn new() -> Self { Self::default() }

    pub fn with_load_paths(mut self, paths: Vec<PathBuf>) -> Self {
        self.load_paths = paths;
        self
    }

    pub fn with_env(mut self, env: Vec<(String, String)>) -> Self {
        self.env = env;
        self
    }

    pub fn with_purge_modules<I, S>(mut self, modules: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.purge_modules = modules.into_iter().map(Into::into).collect();
        self
    }

    pub fn with_purge_feature_prefixes<I, S>(mut self, prefixes: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.purge_feature_prefixes = prefixes.into_iter().map(Into::into).collect();
        self
    }

    /// The default detector set used by `compile_in_context` when no
    /// explicit detectors are provided. Currently a single detector
    /// (`LoadPathConflictDetector::pangea`) covering the canonical
    /// double-load pattern; future bug-class detectors land here as
    /// they're added.
    ///
    /// Held as `Box<dyn ConflictDetector>` so the list is open for
    /// extension without changing the constructor's signature. Per-CR
    /// custom detector sets go through
    /// `compile_in_context_with_detectors` instead of overriding here.
    pub fn default_detectors() -> Vec<Box<dyn ConflictDetector>> {
        vec![Box::new(LoadPathConflictDetector::pangea())]
    }

    /// Check structural invariants on the manifest. Returns
    /// `ContextWarnings` carrying any violations. None of the checks
    /// are fatal — a violation is a misconfiguration signal but the
    /// compile proceeds. (The alternative — refusing to compile on
    /// warning — is too strict for a system where invariants evolve.)
    ///
    /// Current invariants:
    ///
    /// 1. **Distinct load_paths.** Duplicate entries waste resolution
    ///    cycles and signal a manifest bug.
    /// 2. **purge_feature_prefixes are absolute.** A relative prefix
    ///    would match by accident; absolute prefixes are unambiguous.
    /// 3. **purge_modules look like Ruby module paths.** A typo here
    ///    silently fails to purge (the module name doesn't exist),
    ///    so we surface obvious shape violations.
    pub fn validate(&self) -> ContextWarnings {
        let mut w = ContextWarnings::default();
        // 1. distinct load_paths
        let mut seen = std::collections::HashSet::new();
        for p in &self.load_paths {
            if !seen.insert(p) {
                w.messages.push(format!("duplicate load_path entry: {}", p.display()));
            }
        }
        // 2. absolute purge_feature_prefixes
        for p in &self.purge_feature_prefixes {
            if !p.starts_with('/') {
                w.messages.push(format!(
                    "purge_feature_prefixes entry is not absolute: {p:?} — relative prefixes match by accident; use absolute paths"
                ));
            }
        }
        // 3. purge_modules look like Ruby module paths
        for m in &self.purge_modules {
            if m.is_empty() || !m.chars().all(|c| c.is_ascii_alphanumeric() || c == ':' || c == '_') {
                w.messages.push(format!(
                    "purge_modules entry doesn't look like a Ruby module path: {m:?}"
                ));
            }
        }
        w
    }
}

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

    /// Apply a `CompileContext` as a transaction with the default
    /// detector set (`CompileContext::default_detectors`). Convenience
    /// wrapper around `compile_in_context_with_detectors` for the
    /// common case.
    pub fn compile_in_context<F, T>(
        &self,
        ctx: &CompileContext,
        f: F,
    ) -> Result<(T, ContextWarnings), EvalError>
    where
        F: FnOnce(&Self) -> Result<T, EvalError>,
    {
        let detectors = CompileContext::default_detectors();
        let detector_refs: Vec<&dyn ConflictDetector> =
            detectors.iter().map(|d| d.as_ref()).collect();
        self.compile_in_context_with_detectors(ctx, &detector_refs, f)
    }

    /// Apply a `CompileContext` as a transaction with an explicit
    /// detector list. Use this when the call site wants to customize
    /// which detectors run (test code; future per-CR detector
    /// overrides via spec.compileDetectors).
    ///
    /// This is the structured-defense entry point. Steps in order:
    ///
    ///   1. **Validate** the manifest's shape (`ctx.validate()`).
    ///      Warnings get returned via `ContextWarnings`; non-fatal.
    ///   2. **Detect** load-path conflicts (`detect_load_path_conflicts`)
    ///      between the manifest's `load_paths` and the existing
    ///      `$LOAD_PATH`. Each conflict is a logical Ruby file
    ///      reachable via >1 absolute path — the canonical cause of
    ///      class-redefinition errors. Warnings only; the conflict
    ///      classifier emits them so they're observable in CR status
    ///      + Events without refusing the compile.
    ///   3. **Snapshot** prior state (`$LOAD_PATH`, `$LOADED_FEATURES`,
    ///      ENV keys we're about to touch). Used for the
    ///      restore-on-exit guard.
    ///   4. **Purge** configured modules + `$LOADED_FEATURES` entries.
    ///      Workspace's loads can now define fresh.
    ///   5. **Apply** the manifest: unshift `load_paths`, set ENV.
    ///   6. **Run** the body.
    ///   7. **Restore** `$LOAD_PATH` + ENV on exit. (Purged modules +
    ///      features are NOT restored — they re-load on demand; restoring
    ///      them would just resurrect the conflict.)
    ///
    /// Returns `(T, ContextWarnings)` — the body's result plus every
    /// observable violation the apply detected. Caller logs or
    /// surfaces the warnings; they're the audit trail.
    pub fn compile_in_context_with_detectors<F, T>(
        &self,
        ctx: &CompileContext,
        detectors: &[&dyn ConflictDetector],
        f: F,
    ) -> Result<(T, ContextWarnings), EvalError>
    where
        F: FnOnce(&Self) -> Result<T, EvalError>,
    {
        // Step 1: manifest validation.
        let mut warnings = ctx.validate();

        // Step 2: run every configured ConflictDetector. Each detector
        // is a typed scan over the resulting $LOAD_PATH + manifest;
        // findings flow into `warnings` with `detector:<name>` prefix
        // so log/metric/event consumers can filter by class.
        let current_load_path: Vec<PathBuf> = self
            .ruby
            .eval::<RArray>("$LOAD_PATH")
            .map_err(|e| EvalError::Other(format!("$LOAD_PATH capture: {e}")))?
            .into_iter()
            .filter_map(|v| String::try_convert(v).ok())
            .map(PathBuf::from)
            .collect();
        for detector in detectors {
            for c in detector.detect(ctx, &current_load_path) {
                warnings.messages.push(format!("detector:{}: {}", c.detector, c.message));
            }
        }

        // Steps 3-7 happen INSIDE the with_load_paths bracket so
        // that the workspace's lib is at $LOAD_PATH's priority head
        // when re-require resolves. Order matters:
        //
        //   3. bracket ENV + $LOAD_PATH (workspace's lib unshifted).
        //   4. purge configured modules + $LOADED_FEATURES entries
        //      (so the workspace's load defines fresh).
        //   5. re-require each purged module from its conventional
        //      Ruby path. This re-establishes `Pangea::Architectures`
        //      (and any other purged module) WITH autoloads, so
        //      workspace code that accesses constants without an
        //      explicit `require` doesn't see "uninitialized
        //      constant". Crucially, the re-require resolves
        //      AGAINST the just-bracketed $LOAD_PATH — so when the
        //      workspace's lib has the file (workspace fully
        //      overrides the bundle), workspace's version wins;
        //      when it doesn't, bundle's wins (the workspace gets
        //      the same shape it had before purge).
        //   6. run f (the compile body).
        //   7. (on drop) restore $LOAD_PATH + ENV.
        let purge_modules_vec: Vec<&str> = ctx.purge_modules.iter().map(String::as_str).collect();
        let purge_prefixes_vec: Vec<&str> = ctx.purge_feature_prefixes.iter().map(String::as_str).collect();
        let result = self.with_env(&ctx.env, |ev| {
            ev.with_load_paths(&ctx.load_paths, |ev| {
                ev.purge_for_workspace_compile(&purge_modules_vec, &purge_prefixes_vec)?;
                ev.restore_purged_modules(&purge_modules_vec)?;
                f(ev)
            })
        })?;

        Ok((result, warnings))
    }

    /// Re-`require` each purged module from its conventional Ruby
    /// path. Companion to `purge_for_workspace_compile`: after the
    /// purge removes a module constant, the next workspace
    /// reference to the constant would raise `uninitialized
    /// constant` — workspaces assume the operator preloads these.
    ///
    /// Ruby's convention maps a module name to a file path by
    /// snake_case'ing each segment and joining with `/`:
    ///
    ///   * `Pangea::Architectures`            → `pangea/architectures`
    ///   * `Pangea::Resources::Cloudflare`    → `pangea/resources/cloudflare`
    ///   * `Foo::BarBaz::QuxQuux`             → `foo/bar_baz/qux_quux`
    ///
    /// The require resolves against the CURRENT `$LOAD_PATH`. When
    /// called from inside `compile_in_context` AFTER the workspace's
    /// lib is unshifted, the workspace's file (if present) wins.
    /// Otherwise the operator's bundled gem wins (fallback). Either
    /// way, the module + its autoloads are re-established.
    ///
    /// Best-effort: a failed require for one module is logged but
    /// doesn't abort the loop. Subsequent purge_modules might
    /// succeed independently.
    pub fn restore_purged_modules(&self, modules: &[&str]) -> Result<(), EvalError> {
        for module_name in modules {
            let require_path = module_name_to_require_path(module_name);
            let src = format!(r#"require '{require_path}'; nil"#);
            if let Err(e) = self.ruby.eval::<Value>(&src) {
                tracing::warn!(
                    module = module_name,
                    require_path = %require_path,
                    error = %e,
                    "restore_purged_modules: re-require failed (continuing)"
                );
            }
        }
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

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    // ── CompileContext::validate ─────────────────────────────────

    #[test]
    fn validate_flags_duplicate_load_paths() {
        let ctx = CompileContext::new().with_load_paths(vec![
            PathBuf::from("/var/pangea/workspaces/a"),
            PathBuf::from("/var/pangea/workspaces/a"),
        ]);
        let w = ctx.validate();
        assert!(w.messages.iter().any(|m| m.contains("duplicate load_path")));
    }

    #[test]
    fn validate_flags_relative_purge_feature_prefixes() {
        let ctx = CompileContext::new()
            .with_purge_feature_prefixes(["not-absolute/path"]);
        let w = ctx.validate();
        assert!(w.messages.iter().any(|m| m.contains("not absolute")));
    }

    #[test]
    fn validate_flags_malformed_module_names() {
        let ctx = CompileContext::new()
            .with_purge_modules(["Pangea Architectures"]); // space in name
        let w = ctx.validate();
        assert!(w.messages.iter().any(|m| m.contains("doesn't look like")));
    }

    #[test]
    fn validate_clean_manifest_has_no_warnings() {
        let ctx = CompileContext::new()
            .with_load_paths(vec![PathBuf::from("/var/pangea/workspaces/a")])
            .with_purge_modules(["Pangea::Architectures"])
            .with_purge_feature_prefixes(["/var/pangea/gems/pangea-architectures-main/"]);
        let w = ctx.validate();
        assert!(w.is_empty(), "clean manifest should produce no warnings, got: {:?}", w.messages);
    }

    // ── detect_load_path_conflicts (the SHIELD) ──────────────────

    /// The exact bug pattern that wedged pleme-io-opensource +
    /// cloudflare-pleme into Phase::Failed on 2026-05-28: a workspace's
    /// _repo/lib AND the operator's bundled gem BOTH ship
    /// pangea/architectures/types.rb. The detector MUST find this.
    /// Regression coverage for [[project_ruby_pool_double_load_fix]].
    #[test]
    fn detects_workspace_vs_bundle_pangea_architectures_double_load() {
        let bundle = TempDir::new().unwrap();
        let workspace = TempDir::new().unwrap();
        // Both expose pangea/architectures/types.rb at different
        // absolute paths.
        write_dummy(bundle.path(), "pangea/architectures/types.rb");
        write_dummy(workspace.path(), "pangea/architectures/types.rb");

        // Workspace's lib is the "new" path (would be unshifted to
        // priority head). Bundle is in "existing" $LOAD_PATH.
        let conflicts = detect_load_path_conflicts(
            &[bundle.path().to_path_buf()],
            &[workspace.path().to_path_buf()],
            &["pangea/"],
        );

        assert_eq!(conflicts.len(), 1, "should detect exactly one conflict");
        let c = &conflicts[0];
        assert_eq!(c.logical_path, "pangea/architectures/types");
        // workspace wins (was new_paths = priority head).
        assert!(c.shadowing_path.starts_with(workspace.path()));
        assert_eq!(c.shadowed_paths.len(), 1);
        assert!(c.shadowed_paths[0].starts_with(bundle.path()));
    }

    #[test]
    fn no_conflict_when_logical_paths_are_disjoint() {
        let a = TempDir::new().unwrap();
        let b = TempDir::new().unwrap();
        write_dummy(a.path(), "pangea/architectures/types.rb");
        write_dummy(b.path(), "pangea/resources/cloudflare.rb");
        let conflicts = detect_load_path_conflicts(
            &[a.path().to_path_buf()],
            &[b.path().to_path_buf()],
            &["pangea/"],
        );
        assert!(conflicts.is_empty());
    }

    #[test]
    fn namespace_prefix_filters_scan_scope() {
        let a = TempDir::new().unwrap();
        let b = TempDir::new().unwrap();
        // Two paths exposing the same logical file under "other/".
        // Pangea-prefix scan must NOT report this.
        write_dummy(a.path(), "other/lib.rb");
        write_dummy(b.path(), "other/lib.rb");
        let conflicts = detect_load_path_conflicts(
            &[a.path().to_path_buf()],
            &[b.path().to_path_buf()],
            &["pangea/"],
        );
        assert!(conflicts.is_empty(), "namespace prefix should filter scan scope");

        // Widening the prefix DOES catch it.
        let conflicts = detect_load_path_conflicts(
            &[a.path().to_path_buf()],
            &[b.path().to_path_buf()],
            &["other/"],
        );
        assert_eq!(conflicts.len(), 1);
    }

    #[test]
    fn detects_three_way_conflict() {
        // The pathological case: 3+ paths exposing the same file.
        // Detector should still find it.
        let a = TempDir::new().unwrap();
        let b = TempDir::new().unwrap();
        let c = TempDir::new().unwrap();
        write_dummy(a.path(), "pangea/architectures/types.rb");
        write_dummy(b.path(), "pangea/architectures/types.rb");
        write_dummy(c.path(), "pangea/architectures/types.rb");

        let conflicts = detect_load_path_conflicts(
            &[a.path().to_path_buf(), b.path().to_path_buf()],
            &[c.path().to_path_buf()],
            &["pangea/"],
        );
        assert_eq!(conflicts.len(), 1);
        assert_eq!(conflicts[0].shadowed_paths.len(), 2);
    }

    #[test]
    fn detector_handles_nested_namespaces() {
        // Files under deeply-nested paths still resolve to the
        // correct logical name.
        let a = TempDir::new().unwrap();
        let b = TempDir::new().unwrap();
        write_dummy(a.path(), "pangea/architectures/aws_vpc_network.rb");
        write_dummy(b.path(), "pangea/architectures/aws_vpc_network.rb");
        let conflicts = detect_load_path_conflicts(
            &[a.path().to_path_buf()],
            &[b.path().to_path_buf()],
            &["pangea/"],
        );
        assert_eq!(conflicts.len(), 1);
        assert_eq!(conflicts[0].logical_path, "pangea/architectures/aws_vpc_network");
    }

    #[test]
    fn missing_namespace_dir_is_no_op() {
        // A $LOAD_PATH entry without a `pangea/` subdir contributes
        // nothing; not an error.
        let empty = TempDir::new().unwrap();
        let with_file = TempDir::new().unwrap();
        write_dummy(with_file.path(), "pangea/architectures/types.rb");
        let conflicts = detect_load_path_conflicts(
            &[empty.path().to_path_buf()],
            &[with_file.path().to_path_buf()],
            &["pangea/"],
        );
        assert!(conflicts.is_empty());
    }

    fn write_dummy(root: &Path, relative: &str) {
        let full = root.join(relative);
        std::fs::create_dir_all(full.parent().unwrap()).unwrap();
        std::fs::write(&full, b"# stub\n").unwrap();
    }

    // ── module_name_to_require_path ───────────────────────────────

    #[test]
    fn module_name_simple_two_segments() {
        assert_eq!(module_name_to_require_path("Pangea::Architectures"), "pangea/architectures");
    }

    #[test]
    fn module_name_three_segments() {
        assert_eq!(
            module_name_to_require_path("Pangea::Resources::Cloudflare"),
            "pangea/resources/cloudflare"
        );
    }

    #[test]
    fn module_name_camelcase_within_segment_becomes_snake() {
        assert_eq!(module_name_to_require_path("Foo::BarBaz"), "foo/bar_baz");
        assert_eq!(
            module_name_to_require_path("Pangea::ProviderRegistry"),
            "pangea/provider_registry"
        );
    }

    #[test]
    fn module_name_handles_single_segment() {
        assert_eq!(module_name_to_require_path("Pangea"), "pangea");
    }

    #[test]
    fn module_name_handles_digits() {
        // Digits don't trigger snake-case insertion; they're treated
        // as lowercase-adjacent.
        assert_eq!(module_name_to_require_path("Magma2::Plan"), "magma2/plan");
    }
}

