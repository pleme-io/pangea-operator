# Pangea-Operator

> **★★★ CSE / Knowable Construction.** This repo operates under **Constructive Substrate Engineering** — canonical specification at [`pleme-io/theory/CONSTRUCTIVE-SUBSTRATE-ENGINEERING.md`](https://github.com/pleme-io/theory/blob/main/CONSTRUCTIVE-SUBSTRATE-ENGINEERING.md). The Compounding Directive (operational rules: solve once, load-bearing fixes only, idiom-first, models stay current, direction beats velocity) is in the org-level pleme-io/CLAUDE.md ★★★ section. Read both before non-trivial changes.

Pangea Kubernetes operator, CLI, and web UI (Rust). Workspace members:

- `pangea-types` — shared types (CRD specs, GraphQL schema bridges).
- `pangea-operator` — the operator binary (kube-rs reconcilers, axum,
  GraphQL/gRPC). Two compiler-backend modes (M8.2+):
  HTTP sidecar (default) or in-process magnus + CRuby (`embedded_ruby`
  feature). See `theory/PANGEA-WORKSPACE-RECONCILIATION.md` § M8.
- `pangea-cli` — operator-side CLI tool.
- `pangea-ruby-eval` — embedded CRuby evaluator (M8.2.0+). Wraps
  magnus 0.8 + rb-sys; gives the operator a typed `RubyEvaluator`,
  `parse_yaml_fixture`, `json_to_ruby` / `ruby_value_to_json`. Single
  CRuby interpreter per process; production code accesses it through
  the `RubyOwner` thread.
- `pangea-web` — Yew/wasm32 web UI (not in workspace; built separately).
- `pangea-compiler` — Ruby Sinatra sidecar that today serves
  `/compile` + `/v1/architectures*`. Slated for deletion in M8.5.3
  once the embedded path proves out on rio.

## Compiler backend selection (M8.2+)

The operator dispatches Pangea DSL compilation through the
`CompilerBackend` trait at `pangea-operator/src/ruby/`. Two impls:

  - `HttpCompilerBackend`     — wraps reqwest to the `pangea-compiler`
                                 sidecar. Default. Always built.
  - `EmbeddedCompilerBackend` — sends typed RPCs to a `RubyOwner`
                                 thread that owns the magnus
                                 interpreter. Built only with
                                 `--features embedded_ruby`.

`PANGEA_COMPILER_BACKEND` env var picks the active backend at
startup (`http` default, `embedded` when feature is on).
`PANGEA_GEM_CACHE_DIR` (default `/var/pangea/gems`) is the per-CR
git-clone cache for the embedded path; `prepare_gem` clones each
ArchitectureGem's `gitRepository` source into
`{cacheDir}/{name}-{ref}/` and prepends `lib/` to `$LOAD_PATH`.

## Build

```sh
# Default (HTTP backend; no libruby linkage)
cargo build -p pangea-operator
nix build .#dockerImage-amd64

# Embedded backend (links libruby; needs ruby_3_4 + libclang at build time)
nix develop .#ruby-eval -c cargo build -p pangea-operator --features embedded_ruby
NIXPKGS_ALLOW_UNFREE=1 nix build --impure .#dockerImage-operator-embedded-arm64
```

## Test

```sh
# pangea-ruby-eval bundled smoke (1 test, 7 internal steps)
nix develop .#ruby-eval -c cargo test -p pangea-ruby-eval --lib

# embedded backend integration test (9 steps)
nix develop .#ruby-eval -c cargo test \
  -p pangea-operator --features embedded_ruby --test embedded_backend
```

## Helm rollout

Chart `helmworks/charts/pangea-operator` 0.7.0 ships
`useEmbeddedRuby` (default false). When true: drops the compiler
sidecar from the pod spec, sets `PANGEA_COMPILER_BACKEND=embedded`,
mounts an emptyDir gem-cache at `/var/pangea/gems`. Operator image
must be the `<sha>-embedded` variant (built with feature on).
