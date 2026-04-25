# 0001 — `PangeaArchitecture` CRD

**Status:** Proposed (target: pangea-operator v0.3.0)
**Author:** drzzln (with Claude Opus 4.7)
**Date:** 2026-04-25

## Why

The `pangea-architectures` Ruby gem already encodes ~25 reusable infra
patterns (`AwsVpcNetwork`, `CloudflareTunnel`, `PlatformEKS`,
`StateBackend`, `BackupRecovery`, …). Today, consuming one of them in
the cluster requires authoring an `InfrastructureTemplate` whose
`source.inline` is a block of Ruby that calls
`Pangea::Architectures::X.build(synth, config)`.

That works but it's leaky — the K8s author has to know Ruby AND the
architecture's hand-rolled API. We want a typed K8s-native surface so:

1. Authors write YAML, not Ruby.
2. K8s admission validates the per-class config schema before the CR
   is even persisted (no more "applied a typo, waited 10min for
   reconcile to fail").
3. Pangea's API surface is *discoverable* via standard `kubectl`
   (`kubectl explain pangeaarchitecture.spec.config` shows the per-
   class schema, `kubectl get architectureregistry` enumerates loaded
   classes).
4. Operators can RBAC at the architecture-class level (e.g. give a
   team write on `Edge::CloudflareTunnel` but not `Compliance::*`).
5. GitOps consumes the same shape regardless of which architecture is
   in play — every CR looks the same; only `spec.architecture.class`
   + `spec.config` differ.

## Shape

```yaml
apiVersion: pangea.pleme.io/v1alpha1
kind: PangeaArchitecture
metadata:
  name: drive-cloudflare-tunnel
  namespace: rio-architectures
spec:
  pangeaNamespace: rio-infra        # PG schema partition (existing)

  architecture:
    class: CloudflareTunnel         # Pangea::Architectures::<class>
    version: "0.6.x"                 # semver constraint pinning the
                                     # pangea-architectures gem

  # Class-specific config; validated by an OpenAPI schema the
  # operator pulls from the compiler sidecar at admission time.
  config:
    accountId:    abc123…
    zoneId:       def456…
    tunnelName:   rio
    tunnelSecret: ${secret:cloudflare-pangea/tunnel-secret}   # interpolated
    ingress:
      - hostname: drive.bristol.quero.cloud
        service:  http://ocis.drive.svc.cluster.local:9200
      - hostname: rio.novaskyn.com
        service:  ssh://127.0.0.1:22
      - service:  http_status:404

  # Same lifecycle knobs as InfrastructureTemplate — reused as-is.
  autoApprove:       false
  destroyProtection: true
  refreshInterval:   10m
  dryRun:            false
  providerCredentials:
    cloudflare:
      secretRef:
        name:      cloudflare-pangea
        namespace: pangea-system

  # New: outputs the architecture exposes (declared by the class itself).
  # The operator writes them to status.outputs after a successful apply.
  exposeOutputs:
    - tunnelId
    - cnameTarget

status:
  phase: Ready                       # Pending|Planning|AwaitingApproval|Applying|Drifted|Ready|Failed
  observedGeneration: 3
  lastPlanAt: 2026-04-25T22:14:00Z
  lastApplyAt: 2026-04-25T22:15:00Z
  lastDriftCheckAt: 2026-04-25T22:24:00Z
  resourceCount: 4
  approvedBy: drzzln@pleme.io        # who patched status.approved=true
  outputs:
    tunnelId:    abc-def-…
    cnameTarget: abc-def-….cfargotunnel.com
  conditions:
    - type: Ready
      status: "True"
      reason: ApplySucceeded
    - type: ComplianceGated
      status: "False"
      reason: NoBindings
```

## Reconciliation flow (Rust changes)

Treat `PangeaArchitecture` as a *higher-order* `InfrastructureTemplate`.
Most of the existing reconciler is reused; the new pieces are:

```
  ┌─ admission webhook ───────────────────────────────────────────┐
  │ 1. Look up class schema in cached registry                    │
  │ 2. Validate spec.config against OpenAPI                       │
  │ 3. Reject if class name unknown / version unavailable         │
  └─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
  ┌─ controller (reconcile) ───────────────────────────────────────┐
  │ 1. Resolve spec.architecture.{class,version} → sidecar         │
  │ 2. Resolve interpolations (${secret:...}, ${ref:other.outputs.x}) │
  │ 3. POST {class, version, config} → http://localhost:8082/render │
  │ 4. Compiler sidecar:                                            │
  │    a. require 'pangea-architectures'                            │
  │    b. instance_eval — Pangea::Architectures::<class>.build(synth, config) │
  │    c. Serialize → { tfJson, k8sManifests, exposedOutputs, schemaVersion } │
  │ 5. Persist tfJson into PG (same path as InfrastructureTemplate) │
  │ 6. tofu plan → tofu apply (gated by autoApprove/approval)       │
  │ 7. SSA k8sManifests (none for most architectures)               │
  │ 8. Write outputs to status.outputs                              │
  │ 9. Schedule next refreshInterval drift check                    │
  └─────────────────────────────────────────────────────────────────┘
```

The compiler sidecar (existing for inline-Ruby InfrastructureTemplate
support) gains a new endpoint:

```
POST /render
  body: {
    class:   "CloudflareTunnel",
    version: "0.6.x",
    config:  { … },
  }
  response 200: {
    tfJson:        { resource: { … } },
    k8sManifests:  [],
    outputs:       ["tunnelId", "cnameTarget"],
    schemaVersion: "0.6.3",     # actual gem version that satisfied the constraint
    architectureMetadata: {
      requiredProviders: ["cloudflare"],
      destroyOrder:      "reverse",
    }
  }
  response 422: { error: "Unknown class CloudflareTunnel; loaded: [...]" }
```

The compiler also exposes:

```
GET /classes                 → enumerate loaded classes + versions
GET /classes/:class/schema   → return OpenAPIv3 JSON schema for spec.config
GET /classes/:class/outputs  → return declared output keys + types
```

The operator's admission webhook caches `/classes/:class/schema`
results with a short TTL (1 min) and uses them for spec.config
validation.

## Companion CRD: `PangeaArchitectureRegistry` (cluster-scoped, status-only)

Read-only CR populated by the operator from `/classes`. Lets users
discover what's available without shelling into the sidecar.

```yaml
apiVersion: pangea.pleme.io/v1alpha1
kind: PangeaArchitectureRegistry
metadata:
  name: default
status:
  loadedAt: 2026-04-25T22:00:00Z
  classes:
    - name: CloudflareTunnel
      version: 0.6.3
      providers: [cloudflare]
      outputs: [tunnelId, cnameTarget]
    - name: AwsVpcNetwork
      version: 0.6.3
      providers: [aws]
      outputs: [vpcId, publicSubnetIds, privateSubnetIds]
    # … 23 more
```

`kubectl get par` (alias) becomes a one-stop "what can I do" list.

## Interpolation grammar

`spec.config` may contain string values matching:

- `${secret:<secret-name>/<key>}` — resolved from a Secret in the
  same namespace as the CR (or `<ns>/<secret>/<key>` for cross-ns).
- `${ref:<other-architecture>.outputs.<key>}` — resolved at
  reconcile-time from the named CR's `status.outputs`. Interpolation
  is recursive; cycles are detected and rejected.
- `${env:<NAME>}` — environment variable lookup (audited).

Interpolation happens AFTER admission validation but BEFORE the call to
the compiler sidecar — so the sidecar always sees concrete values.

## RBAC granularity

Each architecture class becomes an admission-time label. ClusterRoles
can target them:

```yaml
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: pangea-edge-author
rules:
  - apiGroups: [pangea.pleme.io]
    resources: [pangeaarchitectures]
    verbs: [create, update, delete]
    resourceNames: []         # cluster-wide
    # Selection by label injected by admission webhook from spec.architecture.class
    # (since K8s RBAC doesn't natively support spec-field selection,
    # this is enforced by the webhook itself, not native RBAC).
```

The webhook reads the requesting user's groups, looks up an in-cluster
`PangeaArchitectureRBAC` policy CR, and decides allow/deny.

## Per-class default observability

The operator ships a default `PangeaDashboard` and `PrometheusRule`
template per *class*. When the first `PangeaArchitecture` of a given
class lands, the operator instantiates these from the templates,
labelled `class=<class>` so metrics + alerts auto-aggregate per
architecture.

## Versioning + migration

The compiler sidecar bundles every supported gem version in a
versioned subdirectory. Initial v0.3.0 supports the latest two minor
versions of pangea-architectures (e.g. 0.5.x + 0.6.x). Migration:

- Author bumps `spec.architecture.version: "0.7.x"` in git
- Flux applies; operator detects version change → re-renders via
  sidecar with the new version
- Plan diff shown; if approved, applied
- If apply succeeds, `status.architectureVersion` is updated

## Migration path from inline-Ruby InfrastructureTemplate

Day 0 (today): authors write `InfrastructureTemplate` with
`source.inline` calling `Pangea::Architectures::X.build`. Works, but
leaky. — *We're here.*

Day N (operator v0.3.0): the same use case becomes a
`PangeaArchitecture` CR. Both CRDs coexist; operator handles both.

Day N+M: tooling deprecates direct inline Ruby for any case covered
by an existing class. Inline-Ruby `InfrastructureTemplate` remains for
one-off / experimental work that hasn't been promoted to a class.

## Implementation chunks (Rust)

1. **CRD schema** in `pangea-operator/src/crd/pangea_architecture.rs`
   (kube-rs derive macros, OpenAPI v3 generation).
2. **Reconciler** in `pangea-operator/src/controller/architecture.rs`.
   Reuses existing `controller/template.rs` plumbing for the post-
   render-to-Terraform path.
3. **Compiler sidecar** new endpoints in `pangea-compiler/src/server.rs`
   (Ruby side: load `pangea-architectures` gem, expose `Pangea::Architectures.constants`).
4. **Admission webhook** new `validate_architecture` handler.
5. **Registry CR** simple status reflector that polls `/classes`.
6. **Interpolation** new module
   `pangea-operator/src/controller/interpolation.rs`.
7. **Per-class observability templates** — ship `PangeaDashboardTemplate`
   + `PrometheusRuleTemplate` in `pangea-operator/deploy/templates/<class>.yaml`.
8. **Helm chart bump** (helmworks `pleme-pangea-operator`) to v0.3.0
   carrying the new CRD + sidecar config.

## Out of scope for v0.3.0 (deferred)

- Cross-cluster `PangeaArchitecture` (one CR fans out to multiple
  K8s clusters via federation). v0.4.0.
- DAG composition between `PangeaArchitecture` CRs. Use existing
  `InfrastructureFlow` wrapping their underlying templates for now.
- Cost estimation. Sidecar plugin model TBD.
- `nix-synthesizer` → `PangeaArchitecture` bridge. Requires
  synthesizer-core work.

## Open questions

1. **Naming.** Is `PangeaArchitecture` too long? `PA` short alias is
   fine but `kubectl get pangeaarchitecture` is clunky. Considered
   `PArch` (`kubectl get parch`); flagging for bikeshed.
2. **Class namespace.** Today architecture classes are flat
   (`CloudflareTunnel`, not `Edge::CloudflareTunnel`). Should we
   introduce nested namespaces in the class string for the CR
   (`spec.architecture.class: Edge::CloudflareTunnel`)? Backward-compat
   shim possible.
3. **Approval workflow.** Manual `kubectl patch status.approved=true`
   is functional but rough. Worth a small CLI (`pangea approve <name>`)?
4. **Output type system.** Currently `outputs: { stringKey: string }`.
   Architectures may emit lists / maps. Decision: outputs always
   stringify in `status.outputs`; structured access via the status PG
   row if needed.

## Acceptance criteria

- `kubectl apply -f rio-drive-cloudflare-tunnel.par.yaml` (the new
  shape) produces the same Terraform resources as the equivalent
  inline-Ruby `InfrastructureTemplate`.
- Bad config (typo, wrong type) is rejected at admission with a clear
  message naming the offending field.
- `kubectl get pangeaarchitectureregistry default -o yaml` lists every
  loaded class.
- Operator metrics include `pangea_architecture_renders_total{class=..., result=...}`.
- A failing render leaves `status.phase=Failed` with `status.message`
  containing the compiler sidecar's error.
