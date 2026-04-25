# 0002 — `PangeaArchitecture` × FluxCD × Kenshi: gating, testing, attestation

**Status:** Proposed (companion to 0001)
**Date:** 2026-04-25

This doc evaluates how `PangeaArchitecture` (CRD design 0001) fits
into the existing GitOps + test-gate + attestation stack, where it
draws boundaries against neighboring operators, and what the integrated
end-to-end author flow looks like.

## The orchestra

```
  GitHub commit         ──►  FluxCD            ──►  PangeaArchitecture (this CR)
  (architecture YAML)        (kustomize-       │     ├─ admission webhook (schema check)
                              controller +     │     ├─ controller (compile → plan → gate → apply → attest)
                              helm-controller) │     └─ status surface
                                               │
                                               ├──►  Kenshi (gating + testing)
                                               │     ├─ TestGate (block apply until tests green)
                                               │     ├─ DriftWatcher (re-trigger on upstream change)
                                               │     └─ Promotion (cross-env propagation)
                                               │
                                               ├──►  Tameshi (BLAKE3 attestation of plan/apply artifacts)
                                               │
                                               └──►  Sekiban (admission webhook enforcing attested state)

  Observability layer (kube-prometheus-stack):
    ├─ ServiceMonitor on pangea-operator       — render rate, drift detection, plan/apply duration
    ├─ ServiceMonitor on kenshi                 — gate pass/fail, test duration
    └─ PangeaDashboard CRs                      — auto-generated per architecture class
```

## FluxCD: the GitOps front door

PangeaArchitecture CRs live in `clusters/<cluster>/architectures/`,
reconciled by a `Kustomization` that depends on
`infrastructure-pangea` (the operator + DB + CRDs) being healthy:

```yaml
# clusters/rio/flux-kustomizations/architectures.yaml
apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata:
  name: architectures
  namespace: flux-system
spec:
  interval: 5m
  path: ./clusters/rio/architectures
  prune: true
  dependsOn:
    - name: workloads-pangea-test     # health-check InfrastructureTemplate Ready
  decryption:
    provider: sops
    secretRef:
      name: sops-age
  healthChecks:
    # Gate the architectures Kustomization on each PangeaArchitecture
    # reaching Ready — flux retries until apply succeeds, surfaces the
    # final status in `flux get ks architectures`.
    - apiVersion: pangea.pleme.io/v1alpha1
      kind: PangeaArchitecture
      name: rio-drive-cloudflare-tunnel
      namespace: rio-architectures
```

**Key interactions:**

1. **`spec.dependsOn`** chains `architectures` after the operator's
   own deployment, so first-boot ordering is correct.
2. **`spec.healthChecks`** with `kind: PangeaArchitecture` makes
   downstream Kustomizations gate on architectures reaching `Ready`.
   Example: `apps-drive` could `dependsOn: architectures` so the oCIS
   HelmRelease only deploys after the Cloudflare Tunnel is provisioned.
3. **`spec.decryption.sops`** decrypts any SOPS-encrypted Secret in the
   architectures dir (provider tokens, signing keys). The
   `${secret:…}` interpolation in `PangeaArchitecture.spec.config`
   reads from the *decrypted* Secret at reconcile-time, so the
   plaintext only ever lives in K8s memory + audit log.
4. **`spec.prune: true`** means deleting an architecture YAML from git
   triggers the operator's destroy path (gated by
   `destroyProtection`).
5. **Suspend/resume**: `kubectl annotate ks architectures
   kustomize.toolkit.fluxcd.io/reconcile=disabled` freezes all
   architectures during incident response — Pangea operator stops
   re-applying (its own watch still sees the CRs, but they remain at
   their last applied state).

**FluxCD's role is delivery + dependency wiring.** It doesn't know
anything about Pangea; it just reconciles YAMLs and watches CR
healthChecks. That separation is right — Flux stays general, Pangea
stays domain-specific.

## Kenshi: gating, testing, drift, promotion

Kenshi is the test-gate operator. Its CRDs map cleanly onto the
PangeaArchitecture lifecycle:

### `TestGate` — block apply until tests pass

A TestGate CR can reference a PangeaArchitecture as the *gated
resource*. The operator's reconciler checks for matching gates before
moving to the apply phase:

```yaml
# clusters/rio/architectures/drive-cloudflare-tunnel.yaml (companion gate)
---
apiVersion: kenshi.pleme.io/v1alpha1
kind: TestGate
metadata:
  name: drive-cloudflare-tunnel-pre-apply
  namespace: rio-architectures
spec:
  gates:
    # Block the apply phase of this PangeaArchitecture
    - kind: PangeaArchitecture
      name: rio-drive-cloudflare-tunnel
      apiGroup: pangea.pleme.io
      phase: AwaitingApproval        # must pass before phase advances to Applying
  testSuites:
    - name: cf-token-scope-check
      image: ghcr.io/pleme-io/security-tests:latest
      command: ["test-cf-token-has-only-edit-zone"]
      timeout: 30s
      retries: 2
    - name: ingress-backend-reachable
      image: ghcr.io/pleme-io/integration-tests:latest
      command: ["check-svc", "ocis.drive.svc.cluster.local:9200"]
      timeout: 60s
  validityWindow: 1h                  # tests valid for 1h after pass
  # Failure routes to Discord per kenshi conventions
```

**Reconciler interaction:**

The Pangea operator watches for `kenshi.pleme.io/v1alpha1.TestGate`
CRs targeting it. When a `PangeaArchitecture` enters `AwaitingApproval`
or `Planning` (whichever phase the gate names), it:

1. Looks up matching TestGate CRs.
2. Submits a `RunTestSuite` request to kenshi via gRPC.
3. Subscribes to the resulting `TestSuiteRun` status.
4. Holds at the gated phase until: `passed` → advance, `failed` →
   `phase=GateBlocked` (new Pangea phase) with `status.conditions`
   pointing at the failed test.

**No Pangea code needs to run tests directly** — kenshi owns test
execution; Pangea just consults the gate result.

### `DriftWatcher` — re-trigger on upstream change

`DriftWatcher` watches OCI images, git refs, or external sources and
fires events when they change. Useful for architectures whose source
is a git ref:

```yaml
apiVersion: kenshi.pleme.io/v1alpha1
kind: DriftWatcher
metadata:
  name: pangea-architectures-gem
spec:
  sources:
    - type: git
      url: https://github.com/pleme-io/pangea-architectures
      ref: main
  pollInterval: 5m
  triggers:
    # When pangea-architectures advances on main, re-render every
    # PangeaArchitecture pinned to a floating version constraint.
    - kind: PangeaArchitecture
      apiGroup: pangea.pleme.io
      label: "pangea.pleme.io/version-constraint=floating"
      action: reconcile
```

The Pangea operator interprets `action: reconcile` as "annotate the CR
with a fresh `reconcile.fluxcd.io/requestedAt`" — which kicks the
Pangea controller's reconciler.

### `Promotion` — cross-environment propagation

When the rio-staging variant of an architecture reaches `Ready` + tests
green, kenshi can promote the same `PangeaArchitecture` source to
rio-production by patching the production CR's
`spec.architecture.version` to match the just-applied version.

This is the *closest* parallel to FluxCD's image-update-controller, but
operates at the Pangea-architecture-version level instead of container
image tags.

## Tameshi: BLAKE3 attestation

Pangea-operator already supports the tameshi attestation framework
(per the existing `ComplianceSchedule` shape). PangeaArchitecture
inherits this:

```yaml
status:
  attestation:
    planHash:    blake3:abc123…           # hash of the rendered Terraform JSON
    applyHash:   blake3:def456…           # hash of the apply log + outputs
    schemaHash:  blake3:ghi789…           # hash of the architecture's declared schema
    attestedAt:  2026-04-25T22:15:00Z
    attestedBy:  pangea-operator-rio.attestor
```

**Use cases:**

- The compliance team can prove "this architecture was applied on this
  date with these inputs" without needing the original git commit.
- A downstream PangeaArchitecture can `dependsOn: { hash: blake3:abc123… }`
  another architecture's plan — refusing to render until the upstream
  architecture's plan matches a known-good hash.
- Audit trail for SOC 2 / ISO 27001: every cloud-resource change has
  a tamper-evident attestation chain.

## Sekiban: admission enforcement of attested state

Sekiban is the K8s ValidatingAdmissionWebhook that enforces tameshi
attestations. For PangeaArchitecture:

```yaml
# Cluster-wide policy
apiVersion: sekiban.pleme.io/v1alpha1
kind: AttestationPolicy
metadata:
  name: pangea-architectures-must-be-attested
spec:
  selector:
    apiGroups: [pangea.pleme.io]
    kinds: [PangeaArchitecture]
  required:
    - field: status.attestation.planHash
      pattern: ^blake3:[0-9a-f]{64}$
    - field: status.attestation.attestedBy
      oneOf: [pangea-operator-rio.attestor, pangea-operator-plo.attestor]
  exemptNamespaces: [pangea-test]   # health-checks are exempt
```

If a `PangeaArchitecture` somehow reaches `Ready` without an
attestation chain, sekiban rejects the status update — forcing the
operator to redo the attestation phase before declaring success.

This is the production-grade version of "no apply ever happens
unattested." The dev-cluster version skips sekiban; the staging /
production clusters enforce it via this policy.

## End-to-end author flow (the dream)

```
1. Author writes clusters/rio/architectures/drive-cloudflare-tunnel.par.yaml
   ├─ kind: PangeaArchitecture
   ├─ spec.architecture.class: CloudflareTunnel
   └─ spec.config: { ingress: [...], … }

2. git commit && git push

3. FluxCD source-controller pulls; kustomize-controller applies the YAML
   ├─ admission webhook validates spec.config against the CloudflareTunnel
   │   schema served by the Pangea compiler sidecar
   └─ CR persists in etcd

4. Pangea operator watches the CR, transitions phase: Pending → Planning
   ├─ Resolves ${secret:cloudflare-pangea/api-token} via SOPS-decrypted Secret
   ├─ POST {class, version, config} to compiler sidecar
   ├─ Sidecar calls Pangea::Architectures::CloudflareTunnel.build
   ├─ Receives Terraform JSON
   ├─ Persists JSON in PG (rio_infra.cloudflare_tunnel.plans)
   └─ Runs tofu plan → stores diff in status.plan

5. Operator transitions: Planning → AwaitingApproval (autoApprove=false)
   ├─ Kenshi TestGate CR sees AwaitingApproval
   ├─ Triggers cf-token-scope-check + ingress-backend-reachable test pods
   └─ Tests pass → gate releases

6. Author OR auto-approval policy patches status.approved=true
   └─ Operator transitions: AwaitingApproval → Applying

7. Operator runs tofu apply
   ├─ Writes outputs to status.outputs (tunnelId, cnameTarget)
   ├─ Computes BLAKE3 hashes (planHash, applyHash, schemaHash)
   ├─ Posts attestation to tameshi
   ├─ Sekiban admission webhook validates attestation present + signed
   └─ Operator transitions: Applying → Ready

8. FluxCD healthCheck sees PangeaArchitecture phase=Ready
   └─ Downstream apps-drive Kustomization (which dependsOn architectures)
      proceeds to deploy oCIS

9. Periodic drift check (every refreshInterval=10m):
   ├─ Re-runs tofu plan
   ├─ If diff: phase=Drifted, fires PrometheusRule alert
   ├─ Operator OR DriftWatcher policy decides remediation
   └─ Re-applies if autoRemediate=true

10. Audit query weeks later:
    ├─ kubectl get pangeaarchitecture rio-drive-cloudflare-tunnel -o yaml
    ├─ status.attestation.planHash → tameshi server → original rendered JSON
    ├─ status.attestation.applyHash → tameshi server → original apply log
    └─ Full provenance from git commit → terraform → cloud API call → outputs
```

## Where boundaries draw cleanly

| Concern | Owner | Why |
|---|---|---|
| Source-of-truth (YAMLs) | git + FluxCD | git is immutable; Flux makes it declarative |
| Schema validation | Pangea admission webhook | Per-class schemas live in pangea-architectures |
| Compilation (Ruby DSL → TF JSON) | pangea-compiler sidecar | One language; ruby gem is the IR |
| Plan / apply / state | pangea-operator + tofu + PG | Existing pattern, extends to PangeaArchitecture |
| Test gating | kenshi | Kenshi already runs test pods; Pangea calls into it |
| Drift detection | Pangea operator (live) + kenshi DriftWatcher (upstream) | Pangea owns reconcile-time drift; kenshi owns source-change drift |
| Promotion across envs | kenshi Promotion CR | Already does this for container images; trivially extends to architecture versions |
| Attestation | tameshi | Hashing + signing is a separate concern |
| Admission enforcement of attestation | sekiban | Cross-cutting policy webhook |
| Observability | kube-prometheus-stack + auto-rendered PangeaDashboard CRs | Pangea ships per-class templates; cluster ops owns aggregation |
| RBAC | Pangea admission webhook (per-class) | Native K8s RBAC can't select on spec fields |

## What we're explicitly NOT doing

- **Pangea owning test execution.** Tests are kenshi's job. Pangea
  asks kenshi "is this gate green?" and waits.
- **Kenshi owning the apply.** Kenshi gates the apply but doesn't
  call OpenTofu — that stays in Pangea.
- **FluxCD owning rollback.** Flux's rollback is "revert the YAML."
  Pangea owns infrastructure rollback (re-applying the previous
  template revision from PG).
- **One operator owning all of it.** Tempting, but coupling the
  GitOps + test-gate + IaC + attestation domains into one process
  reverses the whole "small composable controllers" K8s philosophy.
  We get more leverage from clean handoffs at well-named CR
  boundaries.

## Implementation effort estimate

| Chunk | Effort | Dep |
|---|---|---|
| `PangeaArchitecture` CRD + reconciler (Rust) | 1.5 weeks | — |
| Compiler sidecar `/render`, `/classes`, `/schema` endpoints (Ruby) | 1 week | CRD merge |
| Admission webhook (Rust) | 4 days | CRD + sidecar |
| Interpolation (Rust) | 3 days | — |
| Kenshi gate-check integration (Rust gRPC client) | 4 days | kenshi `TestGate.spec.gates` shape stable |
| Tameshi attestation phase (Rust) | 3 days | tameshi server reachable |
| Sekiban policy CR + webhook update | 3 days | tameshi attestation phase |
| Per-class observability templates | 2 days | — |
| Helm chart bump to v0.3.0 | 1 day | all the above |
| GitOps starter set in pleme-io/k8s | 1 day | chart published |

Total: ~5–6 weeks for a feature-complete v0.3.0. Most chunks are
parallelizable across two engineers.

## Acceptance criteria (end-to-end)

A successful v0.3.0 release means:

```bash
$ cat <<'YAML' | kubectl apply -f -
apiVersion: pangea.pleme.io/v1alpha1
kind: PangeaArchitecture
metadata:
  name: rio-test
  namespace: pangea-test
spec:
  pangeaNamespace: rio-infra
  architecture: { class: CloudflareTunnel, version: "0.6.x" }
  config: { … }
  autoApprove: true
YAML

$ kubectl wait par/rio-test --for=condition=Ready --timeout=2m
pangeaarchitecture.pangea.pleme.io/rio-test condition met

$ kubectl get par/rio-test -o jsonpath='{.status.outputs}'
{"tunnelId":"…","cnameTarget":"….cfargotunnel.com"}

$ kubectl get parch    # alias works
$ kubectl explain par.spec.config       # per-class schema visible
$ kubectl get pangeaarchitectureregistry/default -o yaml | yq '.status.classes[].name'
CloudflareTunnel
AwsVpcNetwork
…
```

…with kenshi gates blocking bad applies, tameshi attesting every
plan/apply, sekiban enforcing attestation in production, and the whole
loop driven by a `git push`.
