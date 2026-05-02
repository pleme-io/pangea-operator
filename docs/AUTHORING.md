# Authoring infrastructure with pangea-operator

Practical recipes for using the operator. For the theoretical frame,
read [`theory/PANGEA-WORKSPACE-RECONCILIATION.md`](https://github.com/pleme-io/theory/blob/main/PANGEA-WORKSPACE-RECONCILIATION.md);
for the full operator reference, read [`../CLAUDE.md`](../CLAUDE.md).

## Mental model

You declare three things in YAML:

1. **What gems exist** — `ArchitectureGem` CRs (cluster-scoped). The
   operator clones each gem's git source, loads its Pangea-Ruby DSL
   classes, runs smoke fixtures, and refuses to advance any template
   that needs an unloaded class.
2. **What workspaces exist** — `WorkspaceCatalog` CRs (cluster-scoped).
   A workspace is a logical grouping of templates that share a
   git source + required gems + workspace-level reactive policy.
3. **What infrastructure exists** — `InfrastructureTemplate` CRs
   (namespaced). Each one is a Pangea Ruby template the operator
   compiles + reconciles. Templates label themselves with
   `pangea.pleme.io/workspace=<catalog-name>` to opt into a workspace.

The operator handles everything else: gem loading + smoke testing,
template compilation via embedded magnus, `tofu plan` / `tofu apply`,
drift detection, settling, reactive escalation, cycle receipts.

## The minimal recipe — provision a Cloudflare Worker

```yaml
# 1. Declare the gem (cluster-scoped; one-time)
apiVersion: pangea.pleme.io/v1alpha1
kind: ArchitectureGem
metadata:
  name: pangea-architectures
spec:
  gemName: pangea-architectures
  version: "0.x"
  source:
    kind: Ruby
    gitRepository:
      url: https://github.com/pleme-io/pangea-architectures.git
      ref: main
      path: .
  expectedClasses:
    - Pangea::Architectures::CloudflareTunnel
    - Pangea::Architectures::CloudflareWorker
  fixtures:
    - className: Pangea::Architectures::CloudflareWorker
      fixturePath: spec/fixtures/cloudflare_worker.yaml
  refreshInterval: 5m
---
# 2. Declare the workspace (cluster-scoped; one per logical grouping)
apiVersion: pangea.pleme.io/v1alpha1
kind: WorkspaceCatalog
metadata:
  name: my-workspace
spec:
  source:
    gitRepository:
      url: https://github.com/myorg/myrepo.git
      ref: main
      path: workspaces/my-workspace
  requiredGems:
    - pangea-architectures
  policy:
    driftReaction: autoApply
    reactive:
      failureEscalation:
        maxConsecutiveFailures: 3
        onExhaustion: Page
        routing:
          ntfyTopic: my-workspace-critical
---
# 3. Declare the namespace (cluster-scoped; one per state isolation boundary)
apiVersion: pangea.pleme.io/v1alpha1
kind: PangeaNamespace
metadata:
  name: my-infra
spec:
  backend:
    pg:
      host: pangea-database-rw.pangea-system.svc.cluster.local
      database: pangea
      schemaName: my_infra
      secretRef:
        name: pangea-database-credentials
        namespace: pangea-system
---
# 4. Declare the template (namespaced)
apiVersion: pangea.pleme.io/v1alpha1
kind: InfrastructureTemplate
metadata:
  name: my-worker
  namespace: my-infra
  labels:
    pangea.pleme.io/workspace: my-workspace      # opts into the catalog
spec:
  source:
    gitRepository:
      url: https://github.com/myorg/myrepo.git
      ref: main
      path: workspaces/my-workspace/worker.rb
  pangeaNamespace: my-infra
  destroyProtection: true                         # don't auto-destroy
  refreshInterval: 10m
  variables:
    cf_account_id: "0123abcd"
    worker_name:   "hello-world"
  providerCredentials:
    cloudflare:
      secretRef:
        name: cloudflare-api-token
```

That's the whole author surface. The operator owns the rest.

## Verifying the deploy

```sh
# Did the gem load + smoke?
kubectl get architecturegem pangea-architectures
# NAME                   PHASE    LOADED   SMOKE    AGE
# pangea-architectures   Loaded   80       Passed   27h

# Is the workspace verified?
kubectl get workspacecatalog my-workspace
# NAME           SOURCE                          TEMPLATES   VERIFIED   AGE
# my-workspace   https://github.com/.../...      1           true       7h

# Is the template progressing through phases?
kubectl get infrastructuretemplate -A
# NAMESPACE   NAME       PHASE    RESOURCES  CYCLE  MATCHED  UPDATED  DRIFTED  HEALTHY  SUSPENDED  ...
# my-infra    my-worker  Ready    3          5      2        1        0
```

## Reading what the operator did

After every reconcile, `status.lastCycle` holds a typed receipt:

```sh
kubectl get infrastructuretemplate -n my-infra my-worker \
  -o jsonpath='{.status.lastCycle}' | jq
```

```json
{
  "cycle": 5,
  "startedAt": "2026-05-02T00:00:00Z",
  "completedAt": "2026-05-02T00:00:30Z",
  "planSummary": "+0 ~1 -0",
  "summary": {
    "matched": 2,
    "updated": 1,
    "created": 0,
    "destroyed": 0,
    "imported": 0,
    "driftedUncorrected": 0,
    "failed": 0
  },
  "outcomes": [
    {
      "address": "cloudflare_workers_script.hello_world",
      "outcome": "Updated",
      "action": "update"
    }
  ]
}
```

Outcome enum: `Matched | Updated | Created | Destroyed | Imported |
Drifted | Failed`. The aggregate counts answer "what happened?";
per-resource outcomes answer "to what?".

## Adopting an existing cloud resource (importHints)

If the resource already exists out-of-band, declare an import hint to
adopt it instead of creating a duplicate:

```yaml
spec:
  importHints:
    "cloudflare_dns_record.foo":  "{{ .zone_id }}/{{ .foo_record_id }}"
    "aws_iam_role.bar":           "my-role-name"
  variables:
    zone_id:        "0123abcd"
    foo_record_id:  "4567beef"
```

Before each `tofu apply`, the operator runs `tofu import` for every
`create` action whose address has a hint. Successful imports surface
as `Outcome::Imported` in the next cycle receipt.

`{{ .var }}` (or `{{ var }}`) substitutes from `spec.variables`.
Hints with unresolved tokens emit a Warning event and are skipped.

## Reacting when things don't reach a good state

`ReactivePolicy` declares what to do when:
- consecutive failed reconciles exceed a threshold (`failureEscalation`)
- a template is stuck in a non-terminal phase (`phaseTimeout`)
- the `Verified` gate stays blocked (`verifiedBlocked`)

```yaml
spec:
  reactivePolicy:
    failureEscalation:
      maxConsecutiveFailures: 3
      onExhaustion: Suspend       # circuit-break: halt reconcile
      routing:
        ntfyTopic: my-critical
    phaseTimeout:
      compiling: 5m
      planning:  10m
      applying:  30m
      onTimeout: Alert            # event + Healthy=False, keep trying
    verifiedBlocked:
      timeout:    10m
      onBlocked:  Page             # urgent ntfy, no halt
```

**Actions** (worst-action-wins on multi-trigger: Suspend > Page > Alert):

| Action | What happens |
|---|---|
| `Alert` | Warning event + `conditions[Healthy]=False` + ntfy at default priority + structured log line. Reconcile loop continues unchanged. |
| `Suspend` | Set `status.autoSuspended=true`. Halt reconcile until manually cleared. **Typed circuit breaker.** |
| `Page` | ntfy at urgent priority + `Healthy=False`. No other state change. |

**Defaults when unset everywhere** (cascade resolves to):
- `failureEscalation`: 5 retries → Alert
- `phaseTimeout`: 5m / 10m / 30m → Alert
- `verifiedBlocked`: 10m → Alert

To resume an auto-suspended template:
```sh
kubectl patch infrastructuretemplate -n <ns> <name> \
  --subresource status --type merge \
  -p '{"status":{"autoSuspended":false}}'
```

## The four-level cascade

`ReactivePolicy` (and `driftReaction`, `settlingPolicy`) live at every
level. Innermost-set wins per field:

```
ArchitectureGem.spec.policy.reactive          (gem-level fallback)
  → WorkspaceCatalog.spec.policy.reactive     (workspace-level fallback)
    → InfrastructureTemplate.spec.reactivePolicy   (template-level)
      → resource-level (M2; not yet wired)
```

`refuse > requireApproval > autoApply` for safety precedence on
`driftReaction`. `Suspend > Page > Alert` for reactive actions.

A workspace declares "be aggressive" once; templates inherit unless
they explicitly opt out:

```yaml
# WorkspaceCatalog
spec:
  policy:
    driftReaction: autoApply
    reactive:
      failureEscalation:
        maxConsecutiveFailures: 5
        onExhaustion: Page
        routing:
          ntfyTopic: my-workspace
```

```yaml
# Template — inherits everything from workspace
spec:
  source: { ... }
  pangeaNamespace: my-infra
  # no reactivePolicy → uses workspace's
```

```yaml
# Different template — overrides the workspace's onExhaustion
spec:
  source: { ... }
  reactivePolicy:
    failureEscalation:
      maxConsecutiveFailures: 5      # inherited via cascade
      onExhaustion: Suspend           # template-level override
```

## Per-resource policy rules

For finer-grained control over which changes auto-apply vs require
approval, declare `spec.policies`:

```yaml
spec:
  policies:
    - name: refuse-zone-destroy
      match:
        resourceTypes: ["cloudflare_zone"]
        actions:       ["delete"]
      decision: refuse                  # hard block
    - name: approve-dns-deletes
      match:
        resourceTypes: ["cloudflare_dns_record"]
        actions:       ["delete"]
      decision: requireApproval         # wait for approvedPlanHash
    - name: approve-secret-changes
      match:
        attributes: ["secret*", "token*"]
      decision: requireApproval
  defaultDecision: autoApply             # everything else
```

Aggregation: any `refuse` → operator marks plan Failed; else any
`requireApproval` → wait for `approvedPlanHash`; else apply.

## Approving a pending plan

When a policy requires approval, the operator computes a plan hash and
sets `status.pendingPlanHash`:

```sh
kubectl patch infrastructuretemplate -n <ns> <name> \
  --subresource status --type merge \
  -p '{"status":{"approvedPlanHash":"abc1234..."}}'
```

The next reconcile sees `pendingPlanHash == approvedPlanHash` and
applies.

## Suspending reconciliation

Three ways to suspend (most-specific wins):

```yaml
# Per-template (in spec)
spec:
  suspend: true

# Per-workspace (in WorkspaceCatalog spec) — cascades to every template
spec:
  suspend: true

# Auto-suspend (set by ReactivePolicy in status)
status:
  autoSuspended: true
```

Manual `spec.suspend` is for planned outages. `status.autoSuspended` is
the typed circuit breaker fired by `ReactivePolicy.onExhaustion: Suspend`.

## Routing delivery (ntfy today; Slack + GitHub coming)

Every routing destination uses the same `ApprovalRouting` shape:

```yaml
routing:
  ntfyTopic:           rio-critical
  slackChannel:        "#oncall"
  githubIssueTemplate: stuck-template
```

| Channel | Status | Behavior |
|---|---|---|
| `ntfyTopic` | **live** | POST to `{base}/{topic}` with `Title:` `Priority:` `Tags:` headers. Priority maps from action: `Alert→default`, `Suspend→high`, `Page→urgent`. Base from `PANGEA_NTFY_BASE_URL` env (default `https://ntfy.sh`). |
| `slackChannel` | stub | Logs a warning when set. Real delivery requires a Slack webhook URL secret-resolution surface (samba pattern follow-up). |
| `githubIssueTemplate` | stub | Logs a warning when set. Real delivery requires gh app token (samba pattern follow-up). |

Routing fires once per **entry into the bad state** (debounced by
`status.lastEscalationReason`). When the state clears + re-enters, it
fires again. Steady-state crashloops don't re-page every reconcile.

## Lifecycle summary

```
Author writes YAML  →  kubectl apply  →  Operator reconciles
                                              │
                                              ├── Pending → Verifying → Verified
                                              │              ↑
                                              │   (gate: every requiredGem is Loaded)
                                              │
                                              ├── Compiling → Initializing → Planning
                                              │
                                              ├── (importHints pre-pass)
                                              │
                                              ├── (policy evaluation)
                                              │       ↓
                                              │     Refuse → Failed
                                              │     RequireApproval → wait for approvedPlanHash
                                              │     AutoApply → continue
                                              │
                                              ├── Applying → Ready
                                              │       │
                                              │       └── (status.lastCycle written)
                                              │
                                              └── (Ready → drift check → Ready | Drifted)
                                                  (settling counter; cycle counter)
                                                  (reactive evaluation if anything's wrong)
```

## When things go wrong

| Symptom | Where to look | What to do |
|---|---|---|
| Template stuck in `Verifying` | `kubectl describe archgem <gem>` | Check `status.missingClasses` + fixture results |
| Template stuck in `Planning` for >10m | `kubectl describe infra <name>` | `status.lastError`; if `phaseTimeout` reactive set, escalation should have fired |
| Same drift cycle repeating | `kubectl get infra <name> -o yaml \| yq .status` | `consecutiveDriftCycles` + `stuckResources`; usually a provider known-after-apply churn (use `lifecycle.ignore_changes` in the gem) |
| Auto-suspended | `status.lastEscalationReason` | Investigate cause; clear via `kubectl patch ... 'autoSuspended:false'` |
| `Healthy=False` but no escalation | Operator log around `apply_reactive_policy` | Reactive cascade resolution; effective defaults applied |
| Apply error keeps happening | `status.lastError` + `failureCount` | If `failureCount >= maxConsecutiveFailures`, Alert/Suspend/Page should have fired |

## Common idioms

### Workspace-driven fleet rollout
Every cluster gets a `WorkspaceCatalog` per logical grouping; templates
label themselves with the catalog. Workspace policy is the cascade root
— templates inherit unless they explicitly override. Adding a new
template = one labeled `InfrastructureTemplate` CR.

### Bootstrap-tier templates
Use `destroyProtection: true` for anything the cluster needs in order
to function (the cluster's own DNS, the cluster's own ingress tunnel,
the cluster's own state backend). The operator refuses `tofu destroy`
even when the CR is deleted.

### Out-of-band-resource adoption
Use `importHints` for resources created manually before pangea took
over, OR resources owned by a different IaC tool that's being
decommissioned. The cycle receipt records `Outcome::Imported` so audits
clearly show which resources were adopted vs created fresh.

### Escalation routing per cluster
ntfy topics by convention: `<cluster>-critical` / `<cluster>-warning` /
`<cluster>-info`. WorkspaceCatalog declares the workspace's default
routing; templates can override per-resource via `policies[].routing`.

### Suspend before maintenance
Workspace-level `suspend: true` on a `WorkspaceCatalog` halts every
template under it. Use during cluster maintenance to avoid
auto-applying during a known-disrupted window.

## Cross-references

- [`../CLAUDE.md`](../CLAUDE.md) — full operator reference
- [`docs/design/`](./design/) — ADR-style design records (CRD shape,
  FluxCD integration, helmworks chart, breathability)
- [`pleme-io/theory/PANGEA-WORKSPACE-RECONCILIATION.md`](https://github.com/pleme-io/theory/blob/main/PANGEA-WORKSPACE-RECONCILIATION.md)
  — design intent + milestone history
