# 0003 — helmworks-style chart + breathable worker pool

**Status:** Proposed (target: pangea-operator v0.4.0)
**Author:** drzzln (with Claude Opus 4.7)
**Date:** 2026-04-26
**Supersedes parts of:** raw `deploy/` kustomize tree (still valid as fallback)

## Why

Two pressures on the current `replicas: 1` `strategy: Recreate` operator
deployment:

1. **Many-workspace fan-out.** Each `PangeaArchitecture` /
   `InfrastructureTemplate` reconciliation runs `pangea synth + tofu plan/apply`,
   which is a 30s–5m operation. The internal `parallelism` knob in
   `flow_scheduler.rs` bounds concurrent steps *within one CR*, but
   doesn't help when 50 CRs all need to reconcile after a fleet-wide
   git push. Sequential reconciliation behind one pod scales linearly
   with workspace count.

2. **Lifecycle weight.** Tofu is heavy: every reconciliation downloads
   provider plugins (sometimes hundreds of MiB), holds them in memory,
   and shells out per-resource. Bundling that into the controller pod
   inflates idle resource cost and makes pod restarts costly.

The pleme-io fleet already has the answer:
[`pleme-helm`](https://github.com/pleme-io/helmworks/tree/main/charts/pleme-lib)
+ [`pleme-worker`](https://github.com/pleme-io/helmworks/tree/main/charts/pleme-worker)
already encode breathability + autoscaling for background workers.
We pick those up as library deps.

## Design

### Two-tier deployment

```
┌──────────────────────────────────────────────────────────────────┐
│ pangea-operator chart (helmworks/charts/pangea-operator)          │
│                                                                   │
│   ┌──────────────────────────────┐  via pleme-operator lib chart  │
│   │ pangea-controller (1 pod)    │                                │
│   │                              │                                │
│   │   - watches CRs              │                                │
│   │   - leader election (lease)  │                                │
│   │   - dispatches WorkUnits     │  ────►   PangeaWorkUnit CRs    │
│   │   - reconciles status        │                                │
│   │                              │                                │
│   │   resources: 100m / 256Mi    │                                │
│   └──────────────────────────────┘                                │
│                                                                   │
│   ┌──────────────────────────────┐  via pleme-worker lib chart    │
│   │ pangea-worker (0..N pods)    │  + KEDA ScaledObject           │
│   │                              │                                │
│   │   - claims a PangeaWorkUnit  │  ◄────   PangeaWorkUnit CRs    │
│   │     via optimistic lock      │  (scaled by Pending count)     │
│   │   - runs pangea synth +      │                                │
│   │     tofu plan/apply          │                                │
│   │   - reports status            │                                │
│   │                              │                                │
│   │   resources: per WorkClass    │                                │
│   │   replicas: ceil(pending/    │                                │
│   │     concurrencyPerWorker)    │                                │
│   └──────────────────────────────┘                                │
└──────────────────────────────────────────────────────────────────┘
```

Controller stays light. Workers scale 0→N→0 with the queue. When the
fleet is idle, the cluster's pangea footprint is one Deployment of one
pod (~256 MiB resident).

### New CRD: `PangeaWorkUnit`

```yaml
apiVersion: pangea.pleme.io/v1alpha1
kind: PangeaWorkUnit
metadata:
  name: drive-cloudflare-tunnel-apply-1729891234
  namespace: pangea-system
  labels:
    pangea.pleme.io/parent-cr: drive-cloudflare-tunnel
    pangea.pleme.io/parent-kind: PangeaArchitecture
    pangea.pleme.io/work-class: medium
spec:
  parent:
    apiVersion: pangea.pleme.io/v1alpha1
    kind: PangeaArchitecture
    name: drive-cloudflare-tunnel
    uid: <parent-uid>
  action: apply              # plan | apply | destroy | refresh
  workClass: medium          # selects worker resource profile
  templateSnapshot: |        # rendered template content (immutable)
    require 'pangea-aws'
    template :drive_tunnel do
      ...
    end
  config: { ... }            # spec.config from parent
  providerCreds:
    - secretRef: { name: cloudflare-pangea, namespace: pangea-system }
  state:
    backend: s3
    bucket: pleme-pangea-state
    key: pangea/drive-cloudflare-tunnel
status:
  phase: Pending             # Pending | Claimed | Running | Succeeded | Failed
  claimedBy: pangea-worker-7d8f-xyz   # pod that holds the lock
  claimedAt: "2026-04-26T03:14:15Z"
  startedAt: ...
  finishedAt: ...
  exitCode: 0
  outputs: { ... }
  error: ...
  ttlSecondsAfterFinished: 600
```

**Lifecycle**:

1. Controller creates `PangeaWorkUnit` with `phase: Pending` whenever a
   parent CR needs work.
2. KEDA sees N pending → scales `pangea-worker` Deployment.
3. Each worker pod loops: list Pending WorkUnits → atomically transition
   one to `phase: Claimed` (via `kubectl patch` with `resourceVersion`
   match) → run it → write final `phase: Succeeded|Failed`.
4. Controller observes terminal phase → propagates outputs to parent CR
   `.status.outputs`.
5. Controller cleans up Succeeded WorkUnits after `ttlSecondsAfterFinished`.

### Worker classes (StorageClass-style)

```yaml
apiVersion: pangea.pleme.io/v1alpha1
kind: PangeaWorkClass
metadata:
  name: small
spec:
  resources:
    requests: { cpu: 100m, memory: 256Mi }
    limits:   { cpu: 1, memory: 1Gi }
  timeout: 5m
  concurrencyPerWorker: 1
---
apiVersion: pangea.pleme.io/v1alpha1
kind: PangeaWorkClass
metadata:
  name: medium
spec:
  resources:
    requests: { cpu: 500m, memory: 1Gi }
    limits:   { cpu: 2, memory: 2Gi }
  timeout: 15m
  concurrencyPerWorker: 1
---
apiVersion: pangea.pleme.io/v1alpha1
kind: PangeaWorkClass
metadata:
  name: large
spec:
  resources:
    requests: { cpu: 2, memory: 4Gi }
    limits:   { cpu: 4, memory: 8Gi }
  timeout: 30m
  concurrencyPerWorker: 1
```

Authors set `spec.workClass: medium` on the parent CR (or default).
The controller stamps it on the WorkUnit; the worker pod uses the
referenced PodTemplate spec.

### Helm chart composition

`helmworks/charts/pangea-operator/Chart.yaml`:

```yaml
apiVersion: v2
name: pangea-operator
version: 0.1.0
appVersion: "0.4.0"
type: application
description: Pangea operator + breathable worker pool for fleet-wide IaC reconciliation

dependencies:
  - name: pleme-operator           # controller deployment + RBAC + CRDs
    version: "~0.1.0"
    repository: "file://../pleme-operator"
    condition: controller.enabled
  - name: pleme-worker             # worker deployment with HPA→KEDA
    version: "~0.1.0"
    repository: "file://../pleme-worker"
    condition: workers.enabled
    alias: workers
```

`values.yaml` (high-level shape):

```yaml
# Default-OFF until the operator is approved for the cluster.
enabled: false

controller:
  enabled: true
  image:
    repository: ghcr.io/pleme-io/pangea-operator
    tag: ""               # falls back to appVersion
  replicas: 1             # leader-elected; can run 2+ for HA
  resources:
    requests: { cpu: 100m, memory: 256Mi }
    limits:   { cpu: 500m, memory: 512Mi }
  config:
    workdir: /var/lib/pangea
    workClassDefault: medium
    workUnitTtlSeconds: 600

workers:
  enabled: true
  image:
    repository: ghcr.io/pleme-io/pangea-operator-worker
    tag: ""
  # Breathability — workers scale 0→N→0 driven by KEDA on the
  # PangeaWorkUnit pending count. min=0 only takes effect AFTER the
  # cluster has settled and KEDA cooldownPeriod elapses.
  breathability:
    enabled: true
    cooldownPeriod: 300       # seconds idle before scaling down
    pollingInterval: 15
  scaling:
    minReplicas: 0
    maxReplicas: 10
    targetPendingPerReplica: 1
  # Per-class worker pools (each gets its own Deployment + ScaledObject).
  classes:
    small:
      resources:
        requests: { cpu: 100m, memory: 256Mi }
        limits:   { cpu: 1, memory: 1Gi }
      maxReplicas: 5
    medium:
      resources:
        requests: { cpu: 500m, memory: 1Gi }
        limits:   { cpu: 2, memory: 2Gi }
      maxReplicas: 10
    large:
      resources:
        requests: { cpu: 2, memory: 4Gi }
        limits:   { cpu: 4, memory: 8Gi }
      maxReplicas: 3

# KEDA must be installed in the cluster (pleme-io's standard infra).
keda:
  enabled: true

crds:
  install: true
  upgrade: true   # pangea-operator owns its CRDs

# Datadog/Prometheus metrics
observability:
  serviceMonitor: true
  prometheusRule: true
```

`templates/scaledobject-{class}.yaml` (KEDA scaler — one per class):

```yaml
apiVersion: keda.sh/v1alpha1
kind: ScaledObject
metadata:
  name: pangea-worker-{{ $class }}
spec:
  scaleTargetRef:
    name: pangea-worker-{{ $class }}
  minReplicaCount: {{ .minReplicas }}
  maxReplicaCount: {{ .maxReplicas }}
  pollingInterval: {{ $.Values.workers.breathability.pollingInterval }}
  cooldownPeriod:  {{ $.Values.workers.breathability.cooldownPeriod }}
  triggers:
    - type: kubernetes-workload
      metadata:
        # KEDA's kubernetes-workload scaler counts pods matching a label.
        # We use the controller-emitted PangeaWorkUnit count via the
        # `kube-state-metrics` exporter + a metrics-API trigger.
        # See alternative: prometheus trigger below.
    - type: prometheus
      metadata:
        serverAddress: http://vmsingle-vm.monitoring.svc:8429
        metricName: pangea_workunit_pending_total
        threshold: "{{ .targetPendingPerReplica }}"
        query: |
          sum(pangea_workunit_pending_total{work_class="{{ $class }}"})
```

The controller exposes `pangea_workunit_pending_total` as a Prometheus
gauge (per work class). KEDA's prometheus trigger reads it and scales
the matching worker Deployment.

## Breathability invariants

1. **Idle = zero pods.** With `minReplicaCount: 0` and `cooldownPeriod: 300`,
   a fully idle pangea fleet has 1 controller pod + 0 workers. Total
   resident: ~300 MiB.
2. **First WorkUnit awakens the pool.** KEDA polls every 15s; cold start
   to first worker pod ready is ~30s typical (image already pulled).
3. **Bursts settle gracefully.** Scaling factor = `pending / target`,
   capped by `maxReplicaCount`. A burst of 50 medium WorkUnits scales
   workers to 10 (`maxReplicas: 10`), drains in 5 batches.
4. **Per-class isolation.** A backed-up large queue can't starve small
   workers; each class has its own Deployment + ScaledObject.

## Migration plan

1. **v0.4.0-alpha**: Land the new chart + WorkUnit CRD alongside the
   existing `deploy/` kustomize tree. Both work; new chart is opt-in.
2. **v0.4.0**: Controller code emits WorkUnits in addition to the
   inline executor path. Behavior gated by `--enable-workunit-dispatch`
   flag (off by default).
3. **v0.4.1**: Default the flag to on. Workers required for new
   reconciliations.
4. **v0.5.0**: Remove the inline executor path. WorkUnits are the only
   reconciliation surface.

Each step is reversible — revert the flag, rollback the chart, keep
the kustomize tree.

## Open questions

- **WorkUnit scoping**: One per CR-action (current proposal) vs. one
  per resource within the rendered terraform graph. Per-CR is simpler;
  per-resource enables finer-grained parallelism but explodes WorkUnit
  count. Lean per-CR.
- **State backend**: Today via S3 with DynamoDB locking. Per-WorkUnit
  state lock via the existing tofu lock; controller-level dedup of
  concurrent WorkUnits for the same parent CR. Don't dispatch a new
  WorkUnit while one for the same parent is in `Running`.
- **Provider plugin cache**: Each worker pod re-downloads providers
  from scratch unless we mount a shared volume (PVC) for `~/.terraform/`.
  Add `controller.providerPluginCache.enabled` flag — RWX PVC with
  workers all mounting it readonly, populated by an init pod or by
  the first worker.

## What this does NOT change

- Existing `PangeaArchitecture` + `InfrastructureTemplate` + `PangeaNamespace`
  CRDs are unchanged.
- Existing `flow_scheduler.rs` parallelism within a CR is unchanged
  (workers can use it internally too).
- The `deploy/` kustomize tree stays valid for the v0.3.x line.

## See also

- [pleme-helm library chart](https://github.com/pleme-io/helmworks/tree/main/charts/pleme-lib)
- [pleme-worker chart](https://github.com/pleme-io/helmworks/tree/main/charts/pleme-worker)
- [pleme-operator chart](https://github.com/pleme-io/helmworks/tree/main/charts/pleme-operator)
- [KEDA scalers — kubernetes-workload + prometheus](https://keda.sh/docs/scalers/)
- [pangea-architectures Pillar 11 — mandatory alert layer](../../../pangea-architectures/CLAUDE.md)
