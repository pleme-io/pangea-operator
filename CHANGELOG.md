# Changelog

Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versioning follows [SemVer](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- (placeholder for next release)

## [0.1.0] — 2026-05-21

First public release. Pangea-operator graduates from internal-only Forge-driven distribution to a community-installable Kubernetes operator shipped through standard channels (Helm OCI on ArtifactHub, container images on ghcr.io, cross-arch binaries on GitHub Releases).

The semver is intentionally reset from the internal `0.7.x` line — `0.1.0` signals "new public API surface". Internal deployments continue on the internal track until they re-pin to `0.1.x`.

### Added

- Public release pipeline composed of substrate primitives:
  - `helm-chart-release.yml` → ArtifactHub OCI at `oci://ghcr.io/pleme-io/charts/pangea-operator`
  - `image-push.yml` → `ghcr.io/pleme-io/pangea-operator`
  - `rust-binary-release.yml` → cross-arch binaries on GitHub Releases (linux+macOS × x86_64+aarch64)
- Helm chart polish for public consumption: `charts/pangea-operator/README.md`, `templates/NOTES.txt`, expanded `values.yaml` documentation, `Chart.yaml` populated with `home`, `sources`, `keywords`, `maintainers`.
- OSS surface: `README.md` (badges, install steps, quickstart CR), `LICENSE` (Apache-2.0), `CONTRIBUTING.md`, `CODE_OF_CONDUCT.md`, `SECURITY.md`, `.github/` issue + PR templates + dependabot.

### Reconciled CRDs (this release)

- `ArchitectureGem` — gem source registry + smoke gate
- `WorkspaceCatalog` — workspace metadata + cascade root
- `InfrastructureTemplate` — reconciler state machine
- `PangeaNamespace` — tofu state isolation

[Unreleased]: https://github.com/pleme-io/pangea-operator/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/pleme-io/pangea-operator/releases/tag/v0.1.0
