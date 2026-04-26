# 0004 — workspace-aware breathability

**Status:** Proposed (target: pangea-operator v0.5.0)
**Author:** drzzln (with Claude Opus 4.7)
**Date:** 2026-04-26
**Refines:** [0003 — helmworks chart + breathable workers](0003-helmworks-chart-and-breathable-workers.md)
**Frame:** [theory/BREATHABILITY.md](../../../theory/BREATHABILITY.md) §III

## Why this refinement

0003 introduced WorkClass as `{small, medium, large}` keyed by CPU/memory.
That misses the structural axes that actually matter for the pleme-io
workspace fleet:

| Axis | What it determines | 0003 handles? |
|---|---|---|
| Provider plugin set | Cold-start cost, cache reuse | ❌ |
| Time profile | Cooldown tuning, lease-aware scaling | ❌ |
| Lock contention pattern | Dispatchable vs pending count | ❌ |
| Cross-workspace deps | Blocked vs ready WorkUnits | ❌ |
| Per-account isolation | Secrets blast radius | ❌ |

A homelab fleet of ~50 workspaces with the above shapes will scale
poorly under 0003: workers will thrash through cold starts (no plugin
cache), KEDA will over-scale on lock-blocked queues, long Packer
builds will get their workers preempted at the 5-min cooldown, and
all accounts will share creds.

## Refined CRDs

### `PangeaWorkClass` v0.2 (additive)

```yaml
apiVersion: pangea.pleme.io/v1alpha1
kind: PangeaWorkClass
metadata:
  name: aws-medium
spec:
  # ── existing fields (0003) ──
  resources:
    requests: { cpu: 500m, memory: 1Gi }
    limits:   { cpu: 2,    memory: 2Gi }
  workerImage:
    repository: ghcr.io/pleme-io/pangea-operator-worker
    tag: 0.5.0
  timeout: 15m
  concurrencyPerWorker: 1

  # ── NEW (0004) ──
  providers:                  # affinity hint for KEDA + scheduler
    - aws                     # also drives provider plugin cache pre-pull
  expectedDuration: 5m        # informs cooldown scaling
  account: ""                 # optional account-scoped pool
  cooldownPolicy:
    minSeconds: 60            # short class → fast scale-down
    maxSeconds: 600           # long class → keep around longer
    leaseAware: true          # never scale down while a lease is live
  cacheVolumeRef:             # optional shared provider plugin cache
    pvc: pangea-aws-providers # RWX PVC the operator pre-populates
    mountPath: /home/pangea/.terraform/plugin-cache
```

**Provider-aware classes per the §III.1 catalog:**

```
aws-tiny         (DNS, IAM updates)            ~80 MiB plugin     30s timeout
aws-small        (single-resource architectures) ~80 MiB           3 min
aws-medium       (VPCs, IAM bundles)            ~80 MiB            15 min
aws-large        (EKS clusters, multi-stage)    ~80 MiB            30 min
aws-packer-large (AMI builds, image pipelines)  ~120 MiB           60 min
cloudflare-small (zone records, tunnels)        ~30 MiB            3 min
akeyless-small   (auth methods, secrets)        ~40 MiB            3 min
multi-cloud-medium (mixed AWS+CF+AK)            ~250 MiB           15 min
http-tiny        (Grafana, Datadog HTTP-only)   ~10 MiB            1 min
```

The `http-tiny` class is what powers `rio-observability` — Grafana
folder + datasources + dashboards via the Grafana provider. No tofu
state to lock, no AWS plugin to cache, ~5 s applies. It deserves a
class with `cooldownPolicy.minSeconds: 30`.

### `PangeaWorkUnit` v0.2 (additive)

New fields on `.status`:

```yaml
status:
  phase: Pending
  blockedOn:                  # NEW — explicit dependency tracking
    - ref:
        kind: PangeaWorkUnit
        name: parent-vpc-apply-1729...
      reason: cross-workspace-output-not-ready
    - ref:
        kind: Lease
        name: tofu-state-lock-rio-vpc
      reason: state-lock-held
  dispatchable: false         # NEW — derived from blockedOn list
  lease:                      # NEW — heartbeat-aware lifecycle
    holderId: pangea-worker-7d8f-xyz
    acquiredAt: ...
    renewedAt: ...
    ttl: 60s                  # renewed every ttl/3 by the holder
```

The controller computes `.status.dispatchable = (blockedOn is empty)`.
KEDA's prometheus trigger reads
`pangea_workunit_dispatchable_total{work_class=...}`, never the raw
`pending` count.

## Refined controller behavior

### Provider plugin cache

When a WorkClass declares `cacheVolumeRef`, the controller:

1. Ensures the named PVC exists (creates RWX-claim if missing).
2. Schedules a one-shot Job at chart install + on `pangea
   provider sync` CLI: pulls the union of plugins for all WorkClasses
   referencing this PVC, populates `<mount>/registry.terraform.io/...`.
3. Worker pods mount the PVC read-only at the configured path.
   tofu's `plugin_cache_dir` env var points at it.

Result: cold-start saving ~10 s per worker for AWS classes;
~30 s for multi-cloud classes.

### Lease-aware cooldown

Each running worker emits heartbeats:

```
PUT /apis/pangea.pleme.io/v1alpha1/.../pangeaworkunits/<name>/status
  patch: { lease: { renewedAt: <now>, ttl: 60s } }
```

Every `ttl/3` seconds while the work is in progress. The controller
derives a corrected gauge:

```
pangea_workunit_active_total{work_class=X} = count(WorkUnits where
  status.phase == Running AND
  status.lease.renewedAt > now() - status.lease.ttl)
```

KEDA scaling:

- **Scale-up trigger**: `dispatchable - capacity < 0` (more dispatchable
  WorkUnits than free workers can handle).
- **Scale-down trigger**: `dispatchable == 0 AND active == 0` for
  `cooldownPolicy.maxSeconds`. As long as `active > 0`, workers are
  not preempted.

### Lock-aware dispatch dedup

When the controller would dispatch a new WorkUnit for parent CR `X`
but another for `X` is in `Running` or `Pending`, it sets the new
unit's `status.blockedOn` to the existing one. This prevents
KEDA from over-scaling for a queue that can only progress sequentially.

### Cross-workspace dependency tracking

Pangea workspaces can reference outputs from sibling workspaces via
tofu `data` blocks (e.g., `data "terraform_remote_state" "vpc" { ... }`).
The controller parses the rendered template's `data.terraform_remote_state.*`
blocks; if any reference an output from another `PangeaArchitecture` /
`InfrastructureTemplate` in the cluster, it adds those parents to
`status.blockedOn` until they're `Synced`.

## Refined chart values shape

```yaml
workers:
  enabled: false

  # (existing 0003 keys unchanged — backward compat)

  # ── NEW: per-class refinement ──
  classes:
    aws-medium:
      enabled: true
      providers: [aws]
      expectedDuration: 5m
      cooldownPolicy:
        minSeconds: 60
        maxSeconds: 300
        leaseAware: true
      cacheVolume:
        enabled: true
        size: 2Gi
        accessMode: ReadWriteMany
        storageClassName: ""       # cluster-default RWX
      account: ""                  # shared (default)
      resources:
        requests: { cpu: 500m, memory: 1Gi }
        limits:   { cpu: 2, memory: 2Gi }
      scaling:
        minReplicas: 0
        maxReplicas: 10

    aws-packer-large:
      enabled: false   # opt-in
      providers: [aws]
      expectedDuration: 30m
      cooldownPolicy:
        minSeconds: 600
        maxSeconds: 1800
        leaseAware: true
      cacheVolume:
        enabled: true
        size: 5Gi
        accessMode: ReadWriteMany
      resources:
        requests: { cpu: 2, memory: 4Gi }
        limits:   { cpu: 4, memory: 8Gi }
      scaling:
        minReplicas: 0
        maxReplicas: 3

    http-tiny:
      enabled: true
      providers: [grafana, datadog]   # HTTP-only, no tofu providers
      expectedDuration: 30s
      cooldownPolicy:
        minSeconds: 30
        maxSeconds: 120
      resources:
        requests: { cpu: 50m, memory: 128Mi }
        limits:   { cpu: 500m, memory: 256Mi }
      scaling:
        minReplicas: 0
        maxReplicas: 5

    multi-cloud-medium:
      enabled: false   # opt-in, big closure
      providers: [aws, cloudflare, akeyless]
      expectedDuration: 10m
      cooldownPolicy:
        minSeconds: 120
        maxSeconds: 600
      cacheVolume:
        enabled: true
        size: 10Gi
      resources:
        requests: { cpu: 1, memory: 2Gi }
        limits:   { cpu: 2, memory: 4Gi }
      scaling:
        minReplicas: 0
        maxReplicas: 5
```

## Per-account isolation pattern

Security-conscious clusters opt in by giving each account its own
named class:

```yaml
classes:
  aws-prod-medium:
    enabled: true
    providers: [aws]
    account: "akeyless-prod"   # used to filter WorkUnits + scope creds
    resources: ...
    scaling: ...

  aws-dev-medium:
    enabled: true
    providers: [aws]
    account: "akeyless-development"
    resources: ...
    scaling: ...
```

The controller, when dispatching, picks the class whose `account`
matches the parent CR's `spec.account`. Workers in the prod pool
mount the `pangea-worker-aws-prod` ServiceAccount with prod-only
secrets references; dev pool gets the dev SA. Cross-account leakage
requires an explicit RBAC change.

For homelab clusters, omit `account:` on every class — workers share.

## Migration plan from 0003

1. **v0.4.x** (0003 lands): single set of `{small, medium, large}` classes,
   no provider awareness, no lease-aware cooldown.
2. **v0.5.0-alpha**: add the new fields as *optional* on PangeaWorkClass.
   Existing classes work unchanged. New classes can opt into provider
   awareness, lease-aware cooldown, account isolation.
3. **v0.5.0**: controller emits `dispatchable_total` gauge (alongside
   `pending_total`). KEDA can use either trigger.
4. **v0.5.x**: chart `workers.classes` shape extended. Defaults shift
   from `{small, medium, large}` → `{aws-{tiny,small,medium,large},
   cloudflare-small, akeyless-small, http-tiny, multi-cloud-medium}`.
   Old class names continue to work via aliases.
5. **v0.6.0**: provider plugin cache populated by an init Job at chart
   install time.

Every step is reversible: revert the chart values, redeploy, classes
fall back to the older shape.

## Cost model

For a typical home-edge cluster (rio-class) reconciling ~20 small
workspaces + 3 medium daily, with a fleet push 2× per day:

| Phase | 0003 | 0004 |
|---|---|---|
| Idle (most of the day) | 1 controller pod (~256 MiB) | unchanged |
| Fleet push burst | 4 workers × 5 min × 2/day = 40 worker-min/day | 4 workers × 3 min × 2/day = 24 worker-min/day (faster cold start via cache) |
| Long Packer run | preempted at 5 min, retried | held until lease drops |
| Stale lock thrash | KEDA over-scales on locked queue | dispatchable gauge prevents |

Net resident cost reduction at burst: ~40 % (better cold-start, no
lock-thrash overscaling). Net latency-to-first-byte for a freshly-
pushed change reduces from ~45 s (cold + provider download) to ~10 s
(cold + cache hit).

## Open questions still

- **Provider cache invalidation.** When the operator upgrades and
  changes provider versions, the cache PVC needs purging. Today this
  is a manual step; ideally the cache-populator Job is content-addressed
  (cache key = hash of locked provider versions).

- **Mixed-provider workspaces and class selection.** A workspace
  using AWS + Cloudflare today must pick `multi-cloud-medium`, even
  if 95% of the work is AWS. Could split a single workspace's
  reconciliation into multiple WorkUnits (one per provider segment),
  but that breaks the tofu graph. Lean: keep one WorkUnit per
  workspace, accept the over-provisioned class.

- **Sticky workers.** A worker that just ran an AWS workspace and has
  the AWS provider in its in-memory tofu cache *should* preferentially
  take the next AWS WorkUnit. KEDA doesn't natively model stickiness.
  Consider a lightweight router pod that re-orders the queue by
  affinity. Defer until traffic warrants.

- **Per-class dashboards.** Each enabled class deserves its own
  Grafana dashboard (queue depth, cold-start latency, lease activity,
  cooldown effectiveness). Built via pangea-dashboards Library
  module — see `Library::PangeaWorkerPanels` (TODO).

## See also

- [0003 — helmworks chart + breathable workers](0003-helmworks-chart-and-breathable-workers.md)
- [theory/BREATHABILITY.md](../../../theory/BREATHABILITY.md) — fleet-wide pattern this fits inside.
- [theory/THEORY.md §VII.3 — Pillar 11](../../../theory/THEORY.md) — the foundational invariant.
