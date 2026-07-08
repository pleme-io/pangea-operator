# Pangea-Operator

> **★★★ CSE / Knowable Construction.** This repo operates under **Constructive
> Substrate Engineering** — canonical specification at
> [`pleme-io/theory/CONSTRUCTIVE-SUBSTRATE-ENGINEERING.md`](https://github.com/pleme-io/theory/blob/main/CONSTRUCTIVE-SUBSTRATE-ENGINEERING.md).
> The Compounding Directive (operational rules: solve once, load-bearing
> fixes only, idiom-first, models stay current, direction beats velocity)
> is in the org-level pleme-io/CLAUDE.md ★★★ section. Read both before
> non-trivial changes.

Pangea Kubernetes operator, CLI, and web UI (Rust). Workspace members:

- `pangea-types` — shared types (CRD specs, GraphQL schema bridges).
- `pangea-operator` — the operator binary (kube-rs reconcilers, axum,
  GraphQL/gRPC).
- `pangea-cli` — operator-side CLI tool.
- `pangea-ruby-eval` — embedded CRuby evaluator. Wraps magnus 0.8 +
  rb-sys; one CRuby interpreter per process; production code accesses
  it through the `RubyOwner` thread.
- `pangea-web` — Yew/wasm32 web UI (not in workspace; built separately).
- `pangea-compiler` — Ruby Sinatra sidecar (legacy HTTP backend; slated
  for deletion once embedded magnus has been live for ~2 weeks).

For practical authoring recipes, read [`docs/AUTHORING.md`](./docs/AUTHORING.md).
For the theoretical frame, read
[`pleme-io/theory/PANGEA-WORKSPACE-RECONCILIATION.md`](https://github.com/pleme-io/theory/blob/main/PANGEA-WORKSPACE-RECONCILIATION.md).

## What it does

Given a Pangea Ruby DSL template that declares cloud resources, the
operator: clones the template's gem source → resolves a typed
`ArchitectureGem` registry → runs the DSL through embedded magnus →
synthesizes Terraform JSON → **magma plan** → **magma provider-RPC
apply** (`run_plan_with_providers`, gRPC calls into the providers
in-process) → persists `rendered_config` + `plan` + `bundle` + state
to Postgres (zero disk on the magma path) → emits a typed reconcile-
cycle receipt → escalates declaratively when things don't reach a good
state.

magma is the default executor (`PANGEA_EXECUTOR=magma`,
`PANGEA_FORBID_TOFU=true` on rio); `tofu` remains only as a
config-selectable legacy executor (per-template `spec.executor`, or the
DB-less fallback when no `PGPASSWORD` is wired). Humans never run plan
or apply — they edit CRs and the operator reconciles (per the
★★ PLATFORM-MEDIATED INFRASTRUCTURE rule in the org CLAUDE.md).

Every step has a typed surface; every transition has a typed receipt;
every reactive response has a typed cascade root. Authors stay in YAML;
contracts are enforced by Rust + Ruby + BPF + iptables underneath.

## The four CRDs (typed authoring surface)

| Kind | Scope | What it declares |
|---|---|---|
| `ArchitectureGem` | cluster | A Pangea-Ruby gem the operator must load + smoke-test before any template referencing it can advance past `Verified` |
| `WorkspaceCatalog` | cluster | A logical workspace (git source + required gems + workspace-level reactive policy). Templates label themselves with `pangea.pleme.io/workspace=<name>` to opt in |
| `InfrastructureTemplate` | namespaced | One Pangea Ruby template the operator should reconcile. Carries source, variables, per-resource policies, reactive policy, import hints |
| `PangeaNamespace` | cluster | State-storage isolation boundary (PostgreSQL schema or S3 prefix); referenced by templates via `spec.pangeaNamespace` |

```
ArchitectureGem (gem registry + smoke gate)
  └─ WorkspaceCatalog (workspace metadata + cascade root for templates)
       └─ InfrastructureTemplate (template + per-resource policy)
              └─ magma state in PangeaNamespace (Postgres; tofu-wire-compatible)
```

## The four-level reconciliation policy cascade

`ArchitectureGem.spec.policy → WorkspaceCatalog.spec.policy →
InfrastructureTemplate.spec → resource (M2)`. Innermost-set wins per
field; defaults fill any holes. Safety-precedence on conflict:
`refuse > requireApproval > autoApply`.

The cascade carries:
- `driftReaction` — what to do when drift is detected
- `settlingPolicy` — when to give up if drift won't settle
- `approvalRouting` — where to send notifications
- `reactive` — `ReactivePolicy` (failure / phase-timeout /
  verified-blocked escalations; new in M8.5+)

Resolution lives in `controller/template_controller.rs` for the
template level; `controller/reactive.rs::EffectiveReactivePolicy::resolve`
for reactive cascade.

## ReactivePolicy — declarative responses to bad states

Every level of the cascade can declare a `ReactivePolicy`:

```yaml
reactivePolicy:
  failureEscalation:
    maxConsecutiveFailures: 5     # default 5
    onExhaustion: Alert            # Alert | Suspend | Page
    routing:
      ntfyTopic: rio-critical
      slackChannel: "#oncall"      # stub today
      githubIssueTemplate: stuck    # stub today
  phaseTimeout:
    compiling: 5m                  # default 5m
    planning: 10m                  # default 10m
    applying: 30m                  # default 30m
    onTimeout: Alert
  verifiedBlocked:
    timeout: 10m                   # default 10m
    onBlocked: Alert
```

**Actions** (worst-action-wins on multi-trigger: Suspend > Page > Alert):
- `Alert` — Warning event + `Healthy=False` condition + ntfy at
  default priority + structured log line. Reconcile loop continues.
- `Suspend` — set `status.autoSuspended=true`, halt reconcile until
  operator-human clears manually. Typed circuit breaker.
- `Page` — ntfy at urgent priority + `Healthy=False` condition. No
  other state change.

When unset everywhere, defaults apply: 5 consecutive failures → Alert,
phase timeouts 5m/10m/30m → Alert, verified-blocked 10m → Alert.

Routing today: **ntfy is wired end-to-end** (POST to `{base}/{topic}`
with `Title:`, `Priority:`, `Tags:` headers; base from
`PANGEA_NTFY_BASE_URL`, default `https://ntfy.sh`). Slack + GitHub
stubs log a warning when set; real delivery lands in a follow-up.

To resume an auto-suspended template:
```sh
kubectl patch infrastructuretemplate -n <ns> <name> \
  --subresource status --type merge \
  -p '{"status":{"autoSuspended":false}}'
```

## status.lastCycle — typed reconcile receipts

After every plan→apply pair (or every plan-with-no-changes), the
operator writes a typed `ReconcileCycle`:

```yaml
status:
  cycleCount: 162
  lastCycle:
    cycle: 162
    startedAt: 2026-05-02T00:00:00Z
    completedAt: 2026-05-02T00:00:30Z
    sourceRevision: abc1234
    planSummary: "+0 ~1 -0"
    summary:
      matched: 19
      updated: 1
      created: 0
      destroyed: 0
      imported: 0
      driftedUncorrected: 0
      failed: 0
    outcomes:
      - address: cloudflare_workers_script.zuihitsu_webhook
        outcome: Updated         # Matched | Updated | Created | Destroyed
                                  # | Imported | Drifted | Failed
        action: update            # raw executor action (tofu-wire grammar)
        message: null             # context for Drifted/Failed
```

Surfaced via printer columns:
```
NAME              PHASE   RESOURCES  CYCLE  MATCHED  UPDATED  DRIFTED  HEALTHY  SUSPENDED  ...
cloudflare-pleme  Ready   20         162    19       1        0
```

Aggregate counts answer "what just converged?"; per-resource outcomes
answer "to what?". The receipt only patches when content changes
(steady-state matched-only cycles don't churn etcd).

## DB-backed data plane (zero-disk)

On the magma path **all durable reconcile data lives in Postgres,
never pod disk** — the only sanctioned filesystem reach is loading the
provider gRPC plugin binaries (the OS must `exec` them from a path).
This is the ★★ MAGMA-NATIVE EXECUTION posture made concrete in the
operator. Source: `backend/artifact_store.rs`; theory:
[`pleme-io/theory/MAGMA-OPERATOR-BACKEND.md`](https://github.com/pleme-io/theory/blob/main/MAGMA-OPERATOR-BACKEND.md) §II-bis.

- **`pangea_meta.artifacts`** — one row per
  `(schema_name, template_name, kind)` (that triple is the PRIMARY
  KEY; latest-wins upsert), `kind ∈ {rendered_config, plan, bundle}`.
  Every blob carries a BLAKE3 `content_hash` written on `put` and
  **re-verified on `get`** — a mismatch is a typed integrity error,
  not a silent stale read.
- **`{schema}_{template}_states.states`** — the magma state backend,
  OpenTofu-wire-compatible (the `terraform.tfstate` v4 shape) so a
  config-selected tofu executor reads the same rows.
- **`put_apply_result` is atomic.** The post-apply state row and the
  `bundle` artifact are written in **one Postgres transaction** — a
  half-applied reconcile (state advanced but bundle stale, or vice
  versa) is unrepresentable.
- **Restart re-reads the plan from Postgres**, never from a pod-local
  file — the apply→pod-roll→`ENOENT` restart-loop class is gone
  because there is no disk plan file to lose.
- **`artifact_store=Some` is the gate.** `PGPASSWORD` at startup wires
  `ControllerState::with_db_pool`, which sets `artifact_store: Some(…)`
  (the DB path). With no `PGPASSWORD` the field stays `None` and the
  operator keeps the **legacy disk workspace** — reserved for DB-less
  unit tests and the config-selected tofu executor.

New durable datum → a typed Postgres structure, a content-addressed
key, and an atomic writer; never a pod-disk file.

## importHints — adopt out-of-band cloud resources

```yaml
spec:
  importHints:
    "cloudflare_dns_record.foo": "{{ .zone_id }}/{{ .record_id }}"
    "aws_iam_role.bar": "my-role-name"
  variables:
    zone_id: 0123abcd
    record_id: 4567beef
```

Before each apply, every drift entry with `action: create` whose
address has a hint is imported into state first (a magma import RPC on
the magma path; `tofu import <addr> <substituted-id>` on the legacy
tofu path). Successful imports surface as `Outcome::Imported` in the
cycle receipt instead of `Outcome::Created`.

`{{ .var }}` (or `{{ var }}`) substitutes from `spec.variables`;
unresolved tokens emit a Warning event and skip that hint.

## Compiler backend (M8.2+)

The operator dispatches Pangea DSL compilation through the
`CompilerBackend` trait at `pangea-operator/src/ruby/`. Two impls:

- `HttpCompilerBackend` — wraps reqwest to the `pangea-compiler`
  sidecar. Built always.
- `EmbeddedCompilerBackend` — sends typed RPCs to a `RubyOwner` thread
  that owns the magnus interpreter. Built only with
  `--features embedded_ruby`.

`PANGEA_COMPILER_BACKEND` env var picks the active backend at startup
(`http` default, `embedded` when feature is on).
`PANGEA_GEM_CACHE_DIR` (default `/var/pangea/gems`) is the per-CR
git-clone cache for the embedded path; `prepare_gem` clones each
ArchitectureGem's `gitRepository` source into `{cacheDir}/{name}-{ref}/`
and prepends `lib/` to `$LOAD_PATH`. Bundler-resolved gems (dry-struct,
terraform-synthesizer, …) come from the operator image's runtime
closure via `RUBYLIB`.

## Reconciler state machine

```
Pending → Verifying → Verified → Compiling → Initializing → Planning
   ↑                                                             ↓
   └─ Drifted ← Ready ← Applying ←─────────────────────────────┘
                  ↓
                Destroying (on CR delete; protected by spec.destroyProtection)
                  ↓
                Failed   (on apply error or settling exhaustion)
```

`status.phaseEnteredAt` bumps only on real transitions — that's what
ReactivePolicy's `phaseTimeout` measures against.

## Build

```sh
# Default (HTTP backend; no libruby linkage)
cargo build -p pangea-operator
nix build .#dockerImage-amd64

# Embedded backend (links libruby; needs ruby_3_3 + libclang at build time)
nix develop .#ruby-eval -c cargo build -p pangea-operator --features embedded_ruby
NIXPKGS_ALLOW_UNFREE=1 nix build --impure .#dockerImage-operator-embedded-amd64
```

## Test

```sh
# pangea-ruby-eval bundled smoke (1 test, 7 internal steps)
nix develop .#ruby-eval -c cargo test -p pangea-ruby-eval --lib

# All operator lib tests (300+ as of 0.7.8 — reactive, routing, cycle, WSC, …)
nix develop .#ruby-eval -c cargo test -p pangea-operator --lib

# embedded backend integration test (9 steps)
nix develop .#ruby-eval -c cargo test \
  -p pangea-operator --features embedded_ruby --test embedded_backend
```

## Helm rollout

Chart `helmworks/charts/pangea-operator` 0.7.8+ ships:
- `useEmbeddedRuby: true` — drops the compiler sidecar, sets
  `PANGEA_COMPILER_BACKEND=embedded`, mounts emptyDir gem-cache.
- All four CRDs (incl. WorkspaceCatalog) with the latest schemas.
- New printer columns: `Cycle / Matched / Updated / Drifted / Healthy /
  Suspended` on `kubectl get infrastructuretemplate`.
- `install.crds: Create` + `upgrade.crds: CreateReplace` so additive
  schema changes flow through chart upgrades.
- **0.8.30 `config:` — tiered-config surface (enjulho: config-as-reconciled-Helm).**
  A `config:` values block + `templates/tiered-config.yaml` consume the
  `pleme-lib.tieredConfig` mixin, rendering the operator's shikumi
  `OperatorConfig` **file tier** as a reconciled ConfigMap the `ConfigStore`
  discovers + hot-reloads. **DESTINATION FORM, default-off**
  (`config.file.enabled: false`) — the live render is byte-unchanged. Secrets
  (`PGPASSWORD` / `PANGEA_API_TOKEN` / `PANGEA_GEM_AUTH_TOKEN`) are NEVER
  rendered by the mixin; they stay direct env / secretKeyRef (the operator's
  deliberate secret exclusion, `src/config.rs`). **Follow-on (the cutover):**
  `OperatorConfig` (`src/config.rs`) resolves env + the pod-identity discovered
  tier only — it has no shikumi `ConfigStore` FILE discovery yet. Teaching it to
  discover `/etc/pangea/config.yaml` as the file tier makes the ConfigMap a live
  read and completes the env → reconciled-Helm cutover; until then the
  ConfigMap is the committed target, not consumed. Behavior-preserving +
  parity-tested when landed, same discipline as the env-surface migration.

The HR's image must be the `<sha>-embedded` variant (built with
`embedded_ruby` feature on).

## Generating CRDs

```sh
nix develop .#ruby-eval -c cargo run \
  -p pangea-operator --bin pangea-operator --no-default-features -- \
  --generate-crds > /tmp/all-crds.yaml
```

Then split via pyyaml round-trip (avoid sed-based extraction —
trailing `---` breaks Helm's CRD parser):

```sh
nix-shell -p python3Packages.pyyaml --run "python3 -c \"
import yaml
with open('/tmp/all-crds.yaml') as f:
    docs = [d for d in yaml.safe_load_all(f) if isinstance(d, dict)]
for d in docs:
    name = d['metadata']['name']
    base = name.split('.')[0]
    with open(f'/tmp/crd-{base}.yaml', 'w') as out:
        out.write('---\n')
        yaml.dump(d, out, default_flow_style=False, sort_keys=False)
\""
```

## Status-write loops — the canonical pattern

Every kube-rs controller in this operator MUST follow this two-layer
pattern when writing its own resource's `.status`. Pre-2026-05-07 the
shape was inconsistent across controllers; the rio firefighting wave
(commits c02ab09 → 6a9663f → 4f421cb → ab859b0 → 8a6ccb7 → a0c1370 →
9ccb221) standardized it. New controllers MUST follow the established
shape; existing controllers that drift back to direct `Controller::new`
or unconditional `patch_status` will be caught by the
`PangeaControllerReconcileRateHigh` alert (chart 0.8.14+) within ~1
minute of the regression.

**Why:** any status field built with `Utc::now()` (condition
`lastTransitionTime`, custom `lastUpdatedAt`, etc.) restamps on every
reconcile, so a byte-equal status check ALWAYS reports "differs" — the
PATCH refires the controller's own watch and creates a closed loop at
apiserver-event speed. Observed peaks on rio: 123 PATCH/sec on a
single template, 76 reconciles/sec on OperatorPolicy/default, 10/sec on
PangeaFleetStatus — collectively burning ~7.5 cores via amplified k3s
API churn.

### Layer 1: diff-gate at the write boundary

Every `patch_status` call site needs a `*_status_needs_patch(prev,
new_*) -> bool` helper that returns false when nothing observable
changed. Compare:

* Scalar / enum fields: direct `==`
* Conditions: use `crate::controller::status::conditions_observably_equal`
  — compares `(type, status, reason, message)` tuples, ignoring
  `lastTransitionTime`. Don't reimplement.

Example shape:

```rust
async fn update_full_status(...) -> Result<()> {
    let new_conditions = build_conditions();
    let needs_patch = my_status_needs_patch(
        cr.status.as_ref(),
        new_phase, new_observed_gen, &new_conditions, ...
    );
    if !needs_patch {
        debug!("status unchanged; skipping patch (avoids self-trigger watch loop)");
        return Ok(());
    }
    let patch = serde_json::json!({ "status": ... });
    crate::controller::status_patch::patch_status(cr, &state.client, patch).await?;
    Ok(())
}

fn my_status_needs_patch(
    prev: Option<&MyStatus>,
    new_phase: MyPhase,
    new_observed_gen: i64,
    new_conditions: &[crate::crd::Condition],
) -> bool {
    let prev_phase = prev.and_then(|s| s.phase);
    let prev_observed_gen = prev.map(|s| s.observed_generation).unwrap_or(0);
    let prev_conditions: &[_] = prev.map(|s| s.conditions.as_slice()).unwrap_or(&[]);
    let conditions_match = crate::controller::status::conditions_observably_equal(
        prev_conditions, new_conditions,
    );
    !conditions_match
        || prev_phase != Some(new_phase)
        || prev_observed_gen != new_observed_gen
        || prev.is_none()  // first reconcile must always patch
}
```

The helper is pure (no I/O), trivially testable. Each one comes
with 4-5 unit tests pinning: first-reconcile-must-patch, observable-
field-change-must-patch, timestamp-only-churn-must-skip.

**`obj.status` vs in-Context snapshot:** for controllers that write
their own *watched* resource (e.g., `operator_policy_controller`
writes OperatorPolicy, `fleet_status_controller` writes
PangeaFleetStatus), the kube-rs reflector cache LAGS the apiserver —
when the controller's own PATCH triggers a watch event, the reconcile
fires *before* the cache observes the patch, so `obj.status` from the
reconcile arg is stale-by-one. Compare against an in-Context
`Mutex<Option<MyStatus>>` snapshot updated AFTER successful PATCHes,
not against `obj.status`. See
`fleet_status_controller::Context.last_patched` for the canonical
shape. Other controllers (which write child resources, not their own
watched type) are not affected.

### Layer 2: predicate filter at the watch-stream boundary

Every `Controller::new(api, Config::default())` call site has been
replaced with `crate::controller::generation_filter::filtered_controller::<K>(client)`,
which wraps `watcher → default_backoff → reflect → applied_objects →
predicate_filter(predicates::generation) → Controller::for_stream`.
The filter drops every watch event where `metadata.generation` is
unchanged from the previous time we saw the object — and the apiserver
guarantees `metadata.generation` only advances on spec mutations, so
status PATCHes (even our own legitimate ones) never refire reconciles
through this stream. Combined with each controller's `Action::requeue`
floor, every controller has exactly two work sources:

  1. an actual spec mutation (or new resource creation), and
  2. its own scheduled refresh tick.

Requires the kube-rs `unstable-runtime` feature (transitively pulls in
`unstable-runtime-stream-control`). Already enabled in the workspace
Cargo.toml.

### Defense-in-depth metric

Every controller MUST call `state.metrics.record_reconcile(kind, "ok")`
in its reconcile success path (or `record_reconcile_named("foo", "ok")`
for the two self-driving controllers — operator_policy and
fleet_status — which intentionally aren't in `ControllerKind`). This
fills the denominator for the chart 0.8.14
`PangeaControllerReconcileRateHigh` alert; without it, a hot loop in
your new controller wouldn't trigger the alert and you'd be back to
the rio firefighting session shape.

### Reference call sites

For canonical examples to copy from when adding a new controller:

* Diff-gate helper: `compliance_binding_controller::binding_status_needs_patch`
* In-Context snapshot pattern: `fleet_status_controller`
* Suspended-skip diff-gate (when controller writes only conditions):
  `template_controller::suspended_conditions_already_set`
* `filtered_controller` migration: any of the 14 controllers — all
  use the same one-line replacement

## Common gotchas

- **CRDs don't upgrade with chart by default.** Helm/Flux skip CRDs on
  chart upgrade unless `upgrade.crds: CreateReplace` is set on the HR.
  Without it, additive status fields (like `lastCycle`) get silently
  truncated by API-server schema validation.
- **CRD scope is immutable.** Renaming a struct from cluster-scope to
  namespaced (or vice-versa) breaks every chart upgrade with
  `field is immutable`. Check `kubectl get crd <name> -o
  jsonpath='{.spec.scope}'` before regenerating.
- **NixOS firewall + Cilium**: `networking.firewall.checkReversePath`
  must be `"loose"` (or `false`) — strict RPF silently drops cilium-
  routed pod traffic. See `nodes/rio/wireguard.nix` for the fix.
- **chart re-pushes can be cached.** OCI tags are mutable but Flux
  caches by digest. Bump the chart version (0.7.4 → 0.7.5) rather
  than re-pushing the same tag if upgrades silently fail to pick up
  CRD changes.
- **rio token is rotated every ~7 days.** Native `nix run
  .#push-image-operator-embedded-amd64` from rio breaks with 401
  when the token expires. Workaround: scp the tarball to cid +
  `skopeo copy docker-archive:... docker://...` using cid's gh
  auth token.

## File map

```
pangea-operator/src/
├── api/                    # axum HTTP routes (admission, metrics, v1)
├── backend/                # magma/tofu state-backend selection (postgres / s3)
│   └── artifact_store.rs   # pangea_meta.artifacts (rendered_config /
│                           #   plan / bundle, BLAKE3-verified); atomic
│                           #   put_apply_result (state + bundle, 1 tx)
├── controller/
│   ├── architecture_gem_controller.rs   # M1 — gem registry + smoke
│   ├── workspace_catalog_controller.rs  # M3 — workspace metadata
│   ├── template_controller.rs           # core — InfrastructureTemplate
│   ├── reactive.rs                      # ReactivePolicy evaluation
│   ├── routing.rs                       # ntfy / Slack / GitHub delivery
│   ├── policy_cascade.rs                # per-resource policy rules
│   ├── settling.rs                      # drift loop detection
│   └── …                                # other CRDs (flow, packer, ami, …)
├── crd/                    # all CRD type definitions
│   ├── infrastructure_template.rs       # main author-facing CR
│   ├── workspace_catalog.rs
│   ├── architecture_gem.rs              # also home of shared types:
│   │                                       ApprovalRouting, ReactivePolicy,
│   │                                       FailureEscalation, …
│   └── …
├── executor/               # magma (default) / tofu (legacy) / packer
│                           #   / variable resolution
├── ruby/                   # CompilerBackend trait + impls + RubyOwner
└── observability/          # metrics + tracing
```
