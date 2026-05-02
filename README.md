# pangea-operator

Pangea Kubernetes operator (Rust). Reconciles
`InfrastructureTemplate` CRs end-to-end: clones the template's
gem source → loads it through embedded magnus → synthesizes
Terraform JSON via the Pangea Ruby DSL → runs `tofu plan/apply` →
emits typed cycle receipts → escalates via reactive policies when
things don't reach a good state.

## Quick links

- **[CLAUDE.md](./CLAUDE.md)** — full operator reference: CRDs,
  cascade, reactive policies, import hints, cycle receipts, build/
  test/rollout commands.
- **[docs/AUTHORING.md](./docs/AUTHORING.md)** — practical recipes:
  "I want to provision X via Pangea; here's the minimal CR set".
- **[Theory: PANGEA-WORKSPACE-RECONCILIATION.md](https://github.com/pleme-io/theory/blob/main/PANGEA-WORKSPACE-RECONCILIATION.md)**
  — design intent + milestone history.

## Workspace members

| Crate | Role |
|---|---|
| `pangea-operator` | The operator binary — kube-rs reconcilers, axum HTTP / gRPC / GraphQL surface, tofu/packer executors |
| `pangea-types` | Shared types — CRD specs, GraphQL bridges |
| `pangea-cli` | Operator-side CLI for ad-hoc plan/apply/explain |
| `pangea-ruby-eval` | Embedded CRuby evaluator (magnus 0.8) — `RubyEvaluator`, `parse_yaml_fixture`, JSON↔Ruby converters |
| `pangea-web` | Yew/wasm32 web UI (built separately, not part of this Cargo workspace) |
| `pangea-compiler` | Legacy Ruby Sinatra HTTP backend (kept for transitional rollouts; slated for removal once embedded magnus has a stable run on every cluster) |

## What you author vs what the operator owns

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

You stay in YAML. The operator enforces typed contracts and
emits typed receipts. Reactive policies declare what to do when
things go wrong; routing delivery (ntfy today; Slack/GitHub
follow-up) carries the alerts.

## Status (rio, 2026-05-02)

- Operator: `embedded-amd64-9704e39`
- Chart: `pangea-operator-0.7.8`
- Live workloads: 1 `WorkspaceCatalog` (`rio-architectures`,
  `verified=true`, `templateCount=1`), 1 `ArchitectureGem`
  (`pangea-architectures`, `Loaded(80)`, `Smoke=Passed`), 5
  `InfrastructureTemplate` instances reconciling.
