# Contributing to pangea-operator

Thanks for considering a contribution. This guide covers the practical workflow.

## Code of conduct

All participants agree to the [Code of Conduct](CODE_OF_CONDUCT.md).

## How to contribute

### File an issue first

For non-trivial changes, please open a GitHub issue first describing the problem and the proposed approach. This avoids duplicate work and lets maintainers shape the design before code review.

- Bug? Use the bug report template.
- Feature? Use the feature request template.
- Security vulnerability? **Do not file a public issue** — see [SECURITY.md](SECURITY.md).

### Develop locally

The repo is fully Nix-managed; no language toolchains need to be installed system-wide.

```bash
git clone https://github.com/pleme-io/pangea-operator.git
cd pangea-operator

# 5-min full CI gate (cargo + flake checks).
nix flake check

# Iterative dev.
nix develop -c cargo test --workspace
nix develop -c cargo clippy --workspace -- -D warnings
nix develop -c cargo fmt --all -- --check

# Build the operator image locally.
nix build .#dockerImage-operator-embedded-amd64
```

For working on the embedded Ruby evaluator specifically:

```bash
nix develop .#ruby-eval -c cargo test -p pangea-ruby-eval --lib --tests -- --test-threads=1
```

### Style + standards

- **Formatting**: `cargo fmt` (enforced in CI).
- **Lints**: `cargo clippy --workspace -- -D warnings` must pass.
- **Tests**: every new feature or bug fix carries at least one test. Unit tests for pure logic; integration tests for kube-rs reconciler behaviour (under `pangea-operator/tests/`); flake checks for Nix-level invariants.
- **Commits**: Conventional Commits style preferred — `feat(scope): …`, `fix(scope): …`, `chore(scope): …`.
- **Docs**: behaviour changes also update the relevant section of [CLAUDE.md](CLAUDE.md) and, where user-visible, the chart's [README](pangea-operator/charts/pangea-operator/README.md).

### Open the PR

1. Push your branch + open a PR against `main`.
2. The PR template asks for: summary, motivation, test plan. Fill it in honestly.
3. CI must be green before review.
4. Squash-merge is the default. Keep your commit history clean so the squash message is meaningful.

### Release process

Releases are manual + tag-driven. Maintainers tag `v<semver>` on `main`; the [release workflow](.github/workflows/release.yml) builds binaries, the operator image, and the Helm chart, then publishes to GH Release + ghcr.io + (via ArtifactHub) the Helm OCI registry. See [CHANGELOG.md](CHANGELOG.md).

## Project layout

```
pangea-operator/
├── pangea-operator/      # operator binary (controllers, executors)
│   ├── src/
│   ├── tests/
│   └── charts/pangea-operator/   # Helm chart
├── pangea-cli/           # `pangea` CLI binary
├── pangea-types/         # CRD definitions, shared types
├── pangea-ruby-eval/     # embedded CRuby evaluator (magnus 0.8)
├── pangea-compiler/      # legacy HTTP backend (transitional)
├── pangea-web/           # Yew/wasm32 web UI (out-of-workspace build)
├── deploy/               # raw manifests + kustomize bases
├── docs/                 # AUTHORING.md, RUNBOOKS.md
├── flake.nix             # Nix entry point (substrate + ruby-nix + crate2nix)
└── Cargo.toml            # workspace
```

For the reconciliation model and CRD reference, start at [README.md](README.md) and [CLAUDE.md](CLAUDE.md).

## Reporting

- **Bugs / features**: <https://github.com/pleme-io/pangea-operator/issues>
- **Security**: see [SECURITY.md](SECURITY.md)
- **Maintainers**: engineering@pleme.io
