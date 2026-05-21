# pangea-operator

[![CI](https://github.com/pleme-io/pangea-operator/actions/workflows/ci.yml/badge.svg)](https://github.com/pleme-io/pangea-operator/actions/workflows/ci.yml)
[![Release](https://github.com/pleme-io/pangea-operator/actions/workflows/release.yml/badge.svg)](https://github.com/pleme-io/pangea-operator/actions/workflows/release.yml)
[![Artifact Hub](https://img.shields.io/endpoint?url=https://artifacthub.io/badge/repository/pangea-operator)](https://artifacthub.io/packages/helm/pangea-operator/pangea-operator)
[![License: Apache 2.0](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)

A Rust Kubernetes operator that reconciles infrastructure declared in the
Pangea Ruby DSL end-to-end: clones the template's gem source, evaluates
through embedded magnus, synthesizes Terraform JSON, runs `tofu plan`/
`tofu apply`, and emits typed cycle receipts. Declarative reactive
policies escalate when things don't reach a good state.

## What you get

- **Four CRDs** (`ArchitectureGem`, `WorkspaceCatalog`, `InfrastructureTemplate`, `PangeaNamespace`) — a typed authoring surface for IaC-as-Kubernetes-objects.
- **Embedded CRuby** — Pangea Ruby DSL evaluated in-process via magnus 0.8 (no compiler sidecar required in v0.1.0).
- **`tofu plan` / `tofu apply`** — every reconcile produces a typed plan + a typed cycle receipt.
- **Declarative reactive policies** — `driftReaction`, `settlingPolicy`, `approvalRouting`, `reactive` escalations cascade from gem → workspace → template → resource.
- **Polymorphic executor** — `TofuExecutor` (default) and `MagmaExecutor` (in-process Rust plan/apply) selectable per CR.
- **GraphQL + gRPC + Prometheus + OpenTelemetry** — first-class observability.

## Install

### Helm (recommended)

```bash
helm install pangea oci://ghcr.io/pleme-io/charts/pangea-operator \
  --version 0.1.0 \
  --namespace pangea-system --create-namespace
```

See [`charts/pangea-operator/README.md`](pangea-operator/charts/pangea-operator/README.md) for the full values reference + upgrade guidance.

### Raw manifests / kustomize

CRDs + RBAC + Deployment are available from each GitHub Release:

```bash
kubectl create namespace pangea-system
kubectl apply -n pangea-system \
  -f https://github.com/pleme-io/pangea-operator/releases/download/v0.1.0/install.yaml
```

(Or render the chart locally: `helm template oci://ghcr.io/pleme-io/charts/pangea-operator --version 0.1.0 | kubectl apply -f -`.)

## Quickstart

```yaml
apiVersion: pangea.pleme.io/v1
kind: ArchitectureGem
metadata:
  name: pangea-aws
spec:
  source:
    git: https://github.com/pleme-io/pangea-aws
    ref: main
  smokeTest:
    template: aws::vpc::dev
---
apiVersion: pangea.pleme.io/v1
kind: InfrastructureTemplate
metadata:
  name: vpc-dev
  namespace: default
spec:
  templateName: aws::vpc::dev
  requiredGem: pangea-aws
  pangeaNamespace: dev-state
  variables:
    region: us-east-1
    cidr: 10.0.0.0/16
  policy:
    defaultDecision: requireApproval
```

For practical recipes ("I want to provision X via Pangea") see [`docs/AUTHORING.md`](docs/AUTHORING.md).

## Workspace members

| Crate | Role |
|---|---|
| `pangea-operator` | The operator binary — kube-rs reconcilers, axum HTTP / gRPC / GraphQL surface, tofu/packer executors |
| `pangea-types` | Shared types — CRD specs, GraphQL bridges |
| `pangea-cli` | Operator-side CLI for ad-hoc plan/apply/explain (binary: `pangea`) |
| `pangea-ruby-eval` | Embedded CRuby evaluator (magnus 0.8) |
| `pangea-web` | Yew/wasm32 web UI (built separately, not part of this Cargo workspace) |
| `pangea-compiler` | Legacy Ruby Sinatra HTTP backend (transitional; slated for removal) |

## Architecture

```
You author (YAML)               Operator owns (Rust)
─────────────────              ──────────────────────
ArchitectureGem    ─────►      gem registry + smoke gate
WorkspaceCatalog   ─────►      workspace metadata + cascade root
InfrastructureTemplate ─►      reconciler state machine
PangeaNamespace    ─────►      tofu state isolation
                                 │
                                 ├── compile via embedded magnus
                                 ├── tofu plan / apply
                                 ├── reactive escalation
                                 └── cycle receipt
```

Authors stay in YAML; the operator enforces typed contracts and emits typed receipts.

## Development

```bash
# All commands route through Nix.
nix flake check                                    # 5-min CI gate
nix develop -c cargo test --workspace              # unit + integration tests
nix build .#dockerImage-operator-embedded-amd64    # build operator image
nix run .#push-image-operator-embedded-amd64       # push to ghcr.io
```

See [`CLAUDE.md`](CLAUDE.md) for the full operator reference (CRDs, cascade, reactive policies, build/test/rollout commands).

## Theory

For the methodological frame:

- [`PANGEA-WORKSPACE-RECONCILIATION.md`](https://github.com/pleme-io/theory/blob/main/PANGEA-WORKSPACE-RECONCILIATION.md) — design intent + milestone history.
- [`MAGMA-OPERATOR-BACKEND.md`](https://github.com/pleme-io/theory/blob/main/MAGMA-OPERATOR-BACKEND.md) — the in-process Rust executor alternative to tofu.
- [`CONSTRUCTIVE-SUBSTRATE-ENGINEERING.md`](https://github.com/pleme-io/theory/blob/main/CONSTRUCTIVE-SUBSTRATE-ENGINEERING.md) — the broader engineering methodology.

## Contributing

See [`CONTRIBUTING.md`](CONTRIBUTING.md). All contributors agree to the [`CODE_OF_CONDUCT.md`](CODE_OF_CONDUCT.md). Vulnerability disclosure: [`SECURITY.md`](SECURITY.md).

## License

[Apache-2.0](LICENSE) © 2026 Pleme.
