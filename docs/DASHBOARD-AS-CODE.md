# Dashboard-as-Code — the full Helm → CRD → operator → Ruby → Grafana pipeline

> **Destination (named first, per Operating Principle #0).** Every Grafana
> dashboard on the fleet is *declared in Helm values*, flows down through
> FluxCD as a typed **`PangeaDashboard`** CRD, is reconciled by
> **pangea-operator** which compiles its inline **Pangea Ruby** (using the
> typed `Pangea::Dashboards::Library` component vocabulary) into Grafana JSON,
> and is delivered to the cluster's Grafana. No hand-authored dashboard JSON,
> no `kubectl apply` of a ConfigMap, no Grafana UI clicking — a dashboard
> becomes one entry in a values list, and the same operator that manages cloud
> infrastructure manages observability.

This is the **in-cluster control-plane corollary** of the typed dashboard
component library (`pangea-dashboards/docs/COMPONENT-LIBRARY.md`): the library
is *what* a dashboard is made of; this pipeline is *how* it is declared,
delivered, and kept converged.

## The layered vocabulary (Helm → CRD → operator → Ruby)

```
┌─ Helm values (the author surface) ────────────────────────────────────┐
│  dashboards:                                                           │
│    - name: payments                                                    │
│      folder: rio                                                       │
│      architecture: WorkloadOverview        # a Library mixin           │
│      params: { name: payments, jobs: [payments], namespace: payments,  │
│               rate_metric: http_requests_total, ... }                  │
└───────────────────────────────────────────────────────────────────────┘
        │  lareira-pangea-dashboards chart renders →
        ▼
┌─ PangeaDashboard CRD (the typed border) ──────────────────────────────┐
│  kind: PangeaDashboard                                                 │
│  spec:                                                                 │
│    folder: rio                                                         │
│    source: { inline: { ruby: |                                        │
│      Pangea::Dashboards::Library::WorkloadOverview.build(...)          │
│        .then { |d| Pangea::Dashboards::Render::Grafana.render(d) } } } │
└───────────────────────────────────────────────────────────────────────┘
        │  FluxCD applies → pangea-operator DashboardController reconciles
        ▼
┌─ pangea-operator (the executor) ──────────────────────────────────────┐
│  1. eval the inline Ruby in the embedded magnus interpreter            │
│     (pangea-dashboards on $LOAD_PATH) → Grafana dashboard JSON         │
│  2. upsert a sidecar-labelled ConfigMap                               │
│     (labels.grafana_dashboard="1", annotations.grafana_folder=<f>,     │
│      ownerReference → the PangeaDashboard)                             │
│  3. patch status: Ready, configMapName, dashboardUid, sourceHash      │
└───────────────────────────────────────────────────────────────────────┘
        │  the Grafana sidecar (searchNamespace: ALL) loads the CM
        ▼
        Grafana (grafana.quero.cloud) — the dashboard is live + converged
```

### Why a sidecar ConfigMap (not a grafana-operator CR)

rio runs the `victoria-metrics-k8s-stack` bundled Grafana — a plain Deployment
with the dashboard **sidecar** (`label grafana_dashboard`, `searchNamespace:
ALL`), not a grafana-operator-managed `Grafana` CR. So the
`DashboardController` delivers via a **sidecar-labelled ConfigMap** — the exact
mechanism the helmworks dashboard charts (`lareira-rio-dashboards`,
`lareira-breathe-observability`) already use. The grafana-operator
`GrafanaDashboard` CR path stays a typed, config-selectable delivery target for
clusters that run grafana-operator (a `delivery:` enum on the spec) — never a
hard dependency. This keeps the zero-extra-infra promise: a dashboard CRD works
on rio today with the Grafana that is already there.

## The three Helm layers (the "workspaces of various kinds" interface)

| Chart | Role | Renders |
|---|---|---|
| **`lareira-pangea-platform`** (core) | Deploys + fully manages pangea-operator (depends on the `pangea-operator` chart) and exposes a single typed values interface to *layer workspaces of various kinds*: `gems:` (ArchitectureGem), `namespaces:` (PangeaNamespace), `workspaces:` (WorkspaceCatalog + InfrastructureTemplate), `dashboards:` (PangeaDashboard). | the operator + the registry + every declared workspace/CRD |
| **`lareira-pangea-workspace`** (library) | The generic "a workspace of some kind" sub-pattern — a typed values surface → `WorkspaceCatalog` + N `InfrastructureTemplate`s with the policy cascade. Reused by every cloud-IaC workspace. | one workspace's CRDs |
| **`lareira-pangea-dashboards`** (a kind) | A workspace *kind* specialized for observability: from a `dashboards:` list, emits one `PangeaDashboard` per dashboard, each carrying the inline Ruby that calls a `Pangea::Dashboards::Library` mixin/architecture. | N `PangeaDashboard` CRs |

The **kind** is the unit of extension: a cloud-IaC kind, a DNS kind, a secrets
kind, and now a **dashboard kind** all share `lareira-pangea-workspace`'s shape
and the operator's CRD vocabulary; a new kind is a new values schema + the
typed architecture it renders, never a new control plane.

## The Ruby that "puts it all together"

`Pangea::Architectures::GrafanaDashboardWorkspace` (in pangea-architectures) is
the typed seam: given a dashboard spec `{ architecture:, params:, folder: }`,
it dispatches to the named `Pangea::Dashboards::Library` mixin
(`WorkloadOverview`, `ControllerRuntimeDashboard`, `LogExplorerDashboard`, …),
builds the typed `Types::Dashboard`, and renders it via
`Pangea::Dashboards::Render::Grafana` to the JSON the controller delivers. The
operator loads it through the existing `ArchitectureGem` registry (the
`pangea-architectures` gem, with `pangea-dashboards` on its load path), so a new
dashboard kind is *authored once in Ruby, parameterized by YAML the CRD passes
in* — the same author-once/parameterize-by-CRD law as every cloud architecture.

## Convergence — a dashboard is a reconciled promise

Because a `PangeaDashboard` is reconciled (not a one-shot apply), it inherits
the operator's guarantees: the `sourceHash` diff-gate means an unchanged
dashboard never churns; a compile error surfaces as `phase: Failed` + a typed
condition (not a silently-broken panel); deleting the CR garbage-collects its
ConfigMap (ownerReference); and the dashboard re-converges after any drift. The
observation surface becomes self-describing and version-controlled like every
other artifact — closing the loop the org CLAUDE.md's ★★ PLATFORM-MEDIATED
INFRASTRUCTURE rule describes: *declare a CRD, observe through Pangea-defined
Grafana, never click.*

## Status

- `PangeaDashboard` CRD: **shipped** (`crd/pangea_dashboard.rs`).
- `DashboardController`: **implemented this cycle** — eval inline Ruby → sidecar
  ConfigMap → status (was a stub: `// TODO` synthesis).
- `pangea-dashboards` on the operator's embedded `$LOAD_PATH`: **PENDING** —
  the render code is complete, but the gem is not yet in the operator image
  bundle (absent from `flake.nix` `pangeaInputs` + `pangea-compiler/Gemfile`).
  Until it is bundled, a `PangeaDashboard` reconciles to `phase: Failed` with a
  typed `LoadError` in `status.error` (the controller surfaces it correctly —
  no panic, no silent success). Bundling requires the bundix regen of
  `Gemfile.lock` + `gemset.nix` (the same follow-up the Gemfile defers for
  `pangea-architectures`) + an operator image rebuild.
- Helm vocabulary (`lareira-pangea-platform` / `-dashboards`): **shipped**.
- e2e on rio: gated on the gem-bundle + image rebuild above; the worked values
  example renders a valid `PangeaDashboard` CR today.

**Skill:** `dashboard-as-code` (the operator-facing author flow).
**Library:** `pangea-dashboards/docs/COMPONENT-LIBRARY.md` (the component vocabulary).
