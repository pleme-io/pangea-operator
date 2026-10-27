# Leaves — how the operator runs them

This doc holds the mechanics and the work items for `theory/BACIA.md` §9.1.
§9.1 is the rule; this doc does not restate it. §9 holds the migration method
and its ledger. This doc adds what §9 left open: the controller-layer
scheduler, the per-leaf observability, the failure isolation, the embedded
engine, the blue decisions, the derivation for each language, and the cutover
of every grandfathered fat template.

Sections 1 to 3 are the baseline. Sections 4 to 15 are one committed answer
per question, each with the alternative it rejects. Section 16 is the rollout
order. Section 17 is scaling by leaf work. Section 18 is the build plan.

---

## 1. Baseline (measured 2026-10-27, ryn's engenho, read only)

Source: the live `InfrastructureTemplate` status and events on context
`engenho-ryn-594e890c`, read 12:59 and 13:05 UTC, and the operator pod's env.
VictoriaMetrics on plo did not answer, and engenho serves no pod proxy or
port-forward, so no metric series was read. Stage times come from event
timestamps; the refresh share is derived from the pacer rate, not measured per
stage.

| Fact | Value | Denominator / source |
|---|---|---|
| Templates on ryn | 3 | `pleme-io-github-repos`, `roteador-buzios`, `repetidor-buzios` |
| `pleme-io-github-repos` resources | 2,053 | `status.resources.total`; `status.leaf.leafKeys` 2,053, `fat: true` |
| The CR's size | 1,121,629 bytes | `kubectl get -o json`; `spec.variables` alone is 597,215 bytes |
| Last full plan, cycle 34 | 24 min 0 s, "No changes", 2,053 no-op | `Initialized` 09:33:02 to `lastPlannedAt` 09:57:02 |
| The drift plan before 11:55:39 | 34 min | commit a1e6e3b receipt |
| Generation 27 (2 repos changed) | compile 61 s, init 2 s, plan 22 min 10 s, apply 44 min 34 s | events 22:30:32, 22:31:33, 22:31:35, 22:53:45, 23:38:19 (2026-10-26) |
| Generation 28 | compile 78 s, init 31 s, then `ReconcileFailed` after 3,600 s, two stalls, a restart; planned 12:18:58 to past 12:49 | events 23:38:20 to 12:05:44, and the brief |
| Generation 29 | `Compiling` 12:58:09, `Planning` by 13:05:04, under a new incarnation | `status.pass` |
| Stalls recorded | `heartbeatLost` 3, `orphanedPhase` 8, `unnamed` 1 (a generation-26 pass, 250,824 s) | `status.reactions.counts` |
| GitHub budget | 12,500 req/h × 0.9 = 11,250 req/h = 3.125 req/s | `PANGEA_GITHUB_BUDGET_RPH`, `PANGEA_PACER_QUOTA_PCT` |
| Calls in one full plan | about 4,500, about 2.2 per resource | 24 min × 3.125 req/s |
| Admission defaults | reconcile workers 4, per scope 4, global 16 | `config.rs` defaults; the config file tier is absent on ryn |
| Router templates | `roteador-buzios` 91 resources, `repetidor-buzios` 76; both `dialect: json`, no `status.leaf`, no `skip-leaf` | status read 13:05 |
| §9 dry run of `pleme-io-github-repos` | 1,023 leaves (1,018 of 2 resources, 5 of 3), bijection holds, 0/0/0 projected | BACIA §9 ledger, 2026-10-23 |

**Where the time goes.** Compile is 61 to 78 s, about 5% of a cycle. The
plan is about 95%: the embedded engine's `refresh_then_plan` reads every one
of 2,053 resources through the shared GitHub pacer. At 3.125 req/s the floor
for one full refresh is about 24 min. So the plan time is the refresh, and the
refresh is bound by the rate budget, not by CPU. A change to 2 repos pays the
refresh of 2,053.

**Why a 1-repo change takes 30 minutes.** The template is one state. Every
input change, and every drift pass, refreshes the whole state. A new
generation waits behind the running pass. One lost heartbeat holds all 2,053
resources.

---

## 2. What exists (build on it)

| Piece | Where | State |
|---|---|---|
| Leaf = `InfrastructureTemplate` with `variableRefs` edges | BACIA §2, §9 | doctrine |
| `pangea leaf plan` (classify, partition, lift, bijection, prove) | `pangea-leaf`, `pangea-cli` | built, offline |
| Admission refusal, `skip-leaf`, `leaf.admission` | `controller/template/leaf.rs`, `pangea-leaf/src/admission.rs` | built |
| Ratchet `status.leaf` | `pangea-leaf/src/ratchet.rs` | live on ryn (`leafKeys` 2,053) |
| Global and per-scope admission | `FairBudget` (`controller/scheduling.rs`, `shigoto-budget`) | live |
| Per-scope cap | `WorkspaceCatalog.spec.maxConcurrentLeaves`, `try_acquire_capped` | built |
| Upstream gate | `TemplateDag`, `leaf::blocked_upstreams` | dispatch gate only |
| Inputs digest, trigger by diff | `schedule::inputs_digest`, `request_changed_leaves` | built, off by default |
| Per-credential rate budget | `magma_apply::engine::shared_pacer` (samba `LeakyBucket`), keyed by a BLAKE3 of the provider config | live |
| Per-template state row | `"{schema}".states`, `name` unique; locks in `pangea_meta.state_locks` keyed `(schema, template)` | live |
| Pass, heartbeat, stall, preempt | `status.pass`, `pass_driver`, `pass_registry::preempt` (a1e6e3b) | live; another session owns it |
| Status projection metrics | `observability/projection.rs` (`pangea_template_stall`, `pangea_template_pass_heartbeat_age_seconds`, …) | live, `template` label |
| Embedded blue with no host I/O | kensa `src/attestation.rs`: `include_str!`, a `Loader`, `posture(&[])` | the precedent |
| Graph verbs | `blue/bidamas/mokuroku/graph/gurafu-provisional.b` (34 verbs: `graph`, `topo`, `reachable`, `cycle`, `reverse`, `subgraph`, …) | provisional, unbuilt |

---

## 3. The target

| Measure | Today | Target |
|---|---|---|
| Plan for a 1-repo change, from the leaf's generation bump to plan done | 22 to 39 min | **≤ 10 s p95** |
| Apply for that change | 44 min 34 s (gen 27) | ≤ 20 s p95 |
| API calls for the 1,022 unchanged leaves | about 4,500 | **0** |
| Compile for a 1-repo change | 61 to 78 s, whole org | < 1 s, one leaf (§9 inputs derivation) |
| Drift coverage | whole org, one pass, no fixed cadence | every leaf once per `refreshInterval`, spread evenly |
| Drift spend | about 4,500 calls in one burst | about 4,500 calls per hour, ≤ the drift share (§8) |
| Blast radius of one stall | 2,053 resources, the whole org | one leaf and its dependents |

The 10 s budget: one leaf compile < 1 s; state read from Postgres < 0.1 s; a
2-resource refresh is about 4.5 calls, about 1.5 s at 3.125 req/s; the
in-memory diff < 0.1 s; a pooled provider adds 0 s (a cold start is the one
cost not yet measured, §18 item M1).

---

## 4. The unit: one leaf per repository

**Answer.** A leaf is the §9 key with `--group-references`: one repository
and the resources that name it. For `pleme-io-github-repos` that is 1,023
leaves of 2 or 3 resources.

**Why.** A change to the world is a change to one repository. The unit must
be the smallest set that changes together and fails together. A repository's
branch protection and permissions name the repository, so they share its fate.

**Rejected: per shard** (`SHA256(name) % N`, as `pleme-io-opensource` does
with 4 shards). A 1-repo change still refreshes about 250 repos. One bad repo
fails its 250 neighbours. The shard count is a number that must grow with the
org.

**Rejected: per resource family** (all repositories, all protections). A
1-repo change touches several families. Each family is as large as the org.
Every protection needs a cross-unit edge to its repository.

## 5. The leaf is a child CR; its work runs in process

**Answer.** Each leaf is its own `InfrastructureTemplate`. The parent
template owns it (`ownerReferences`), labels it
`pangea.pleme.io/workspace: <parent>`, and aggregates it (§12). Every leaf's
compile, plan and apply runs as an async task in the operator, in the embedded
magma engine. Nothing spawns tofu.

**Why.** The per-template machinery already gives each leaf its own phase,
pass, heartbeat, stall verdict, preempt, state row, lock, events, admission
permit and ratchet. A child CR reuses all of it. kube-runtime reconciles
distinct objects in parallel and never two passes of one object, so the
object is the right isolation key.

**Rejected: in-process units inside one CR.** One object is one kube-runtime
key, so all units serialize behind one pass. The status of 1,023 units would
grow an object that is already 1.1 MB, and every patch rewrites all of it.
Per-unit heartbeat, stall and budget would be a second copy of `pass_driver`
and `FairBudget`, which is the parallel mechanism the operator forbade.

**Cost, stated.** 1,023 objects on ryn's engenho. Each leaf's spec is about
600 bytes of inputs (597 KB / 1,005 repos), so the store holds less than
today's one object. Writes fall: a status patch rewrites a few KB, not 1.1 MB.
Heartbeat writes are bounded by the global cap, because only admitted leaves
beat. engenho's behaviour at 1,000 objects is unmeasured; §18 item E1 measures
it before cutover.

## 6. State boundary and key

**Answer.** One state row per leaf in the parent's `PangeaNamespace` schema:
`"{schema}".states` with `name = <leaf template name>`. The lock is
`pangea_meta.state_locks (schema, leaf)`. The leaf name is
`<parent>-<leaf key slug>`, made by `pangea-leaf/src/names.rs` (to move to
blue, §11).

**Rejected: one `PangeaNamespace` per leaf.** 1,023 Postgres schemas, 1,023
DDL passes, and a namespace object per leaf, for no isolation that a row does
not give.

**Rejected: one shared state with resource-scoped targeting.** One lock and
one serial for all. A targeted plan still loads and writes the whole state.
One failure still owns everything.

## 7. The dependency graph

**Answer.** Edges are `variableRefs` (`templateRef` + `outputKey`, with
`mockOutput` for a plan before the upstream applies). This is Terragrunt's
`dependency` block. The graph is `gurafu` values built by `happa::graph` (§11).
A leaf plans when its upstreams have the output or a mock; it applies only
when they have the output (`schedule::blocked_by`, today). Destroy runs in
reverse order.

`pleme-io-github-repos` has no edges: team grants name repositories by
literal string, and `--group-references` already keeps each repository with
what names it.

**Rejected: `Pangea::RemoteState.output` read at synthesis.** The edge is
hidden inside Ruby. The scheduler cannot see it, so it cannot order, block or
isolate on it.

## 8. Change-scoped planning and drift

**Answer, change.** The parent writes every leaf with server-side apply. The
leaf spec carries only its own inputs. When a leaf's inputs do not change, the
apply is a no-op: no generation bump, no reconcile, no compile, no API call.
When they change, the generation moves and the existing generation filter
starts that leaf alone. The digest is `happa::digest` over the canonical
inputs, the source revision and the architecture's content hash. The parent
stamps it as `pangea.pleme.io/leaf-digest` for audit only; the generation is
the trigger.

**Answer, drift.** Each leaf runs its own drift plan once per
`refreshInterval`. `happa::drift_due` places leaf `k` at offset
`BLAKE3(name) mod interval`, so the org's refresh spreads evenly over the
interval and never bursts. A drift plan refreshes that leaf only.

**Answer, budget.** Drift draws from its own pacer:
`shared_pacer(<credential key>:drift, quota × drift_share, budget)`. Changes
draw from `shared_pacer(<credential key>, quota × (1 − drift_share), budget)`.
The two shares sum to today's quota, so the credential's ceiling holds. A
change never queues behind a drift sweep. Default `leaf.drift_share: 0.5`. For
`pleme-io-github-repos`, hourly drift needs about 4,500 calls, 40% of 11,250.

**Rejected: plan all leaves on every pass with `RefreshScope::Affected`.**
It still compiles and diffs all of them in one failure domain. The type exists
only in uncommitted magma work.

**Rejected: the template-wide `refreshInterval` sweep.** It is the burst that
blocks a change today.

## 9. Concurrency and the rate budget

Parallelism belongs to the controller. Each leaf is one task. No
engine-internal parallelism flag carries throughput. Every knob below has a
default and an off value, and each is one kousei entry in `happa::caps` (§11),
so a bad value is refused alone.

| Knob | Where | Default | Off |
|---|---|---|---|
| Global cap | `reconcile.budget_global` | 16 | `0` refused; monolithic value `1` |
| Reconcile workers | `reconcile.workers` | 4 → **16** (must be ≥ the global cap, or the global cap is moot) | as today |
| Per-scope cap | `WorkspaceCatalog.spec.maxConcurrentLeaves` | `reconcile.budget_per_workspace` (4) | unset |
| Per-credential cap | `leaf.per_credential_cap` (new) | 8 | `0` = no per-credential cap |
| Rate budget | `PANGEA_GITHUB_BUDGET_RPH` × `PANGEA_PACER_QUOTA_PCT` (shared pacer) | 5,000 × 0.7 | quota `0` = unpaced (refused for GitHub by `happa::caps`) |
| Drift share | `leaf.drift_share` (new) | 0.5 | `0` = no scheduled drift |
| Priority | `happa::admit` | on | `leaf.priority: off` = arrival order |

**Priority order** (`happa::admit`): (1) apply of an approved plan, (2) plan
of a changed leaf, (3) a leaf whose upstream just became ready, (4) a retry
after backoff, (5) drift due, oldest first. Ties break by graph depth, upstream
first, then by name. Drift may hold at most `drift_share` of a scope's slots,
so a change always finds a slot.

**Why 8 per credential.** The pacer, not the slot count, sets throughput:
3.125 req/s over about 4.5 calls per leaf is about 0.7 leaves/s. Eight slots
keep the pacer fed while a leaf waits on compile or Postgres. More slots only
hold permits idle.

**Rejected: a samba JetStream drain per credential.** It adds NATS to calls
that stay inside one process. The in-process `LeakyBucket` is already samba's
own bucket, shared by credential.

## 10. Every language derives leaves

`InfrastructureTemplate.spec.dialect` today is `auto | ruby | lava | json`
(`crd/infrastructure_template.rs`). `ArchitectureGem.sourceKind` adds `Lisp`
(terreno) and `Wasm`, both reserved. blue has no dialect yet. Two derivations
cover every surface:

- **render then slice (universal).** The parent compiles once with the
  language's own front end. `happa::split` partitions the rendered Terraform
  JSON by the §9 key with reference closure. `happa::slice` cuts each leaf's
  JSON. The leaf runs `dialect: json` with that body. Cost: one full compile
  per input change (61 to 78 s for this org today).
- **slice then render (keyed).** The source declares which input is the
  collection and which field names the leaf. `happa::inputs` cuts the inputs.
  Each leaf compiles its own slice with the same architecture. Cost: one small
  compile per changed leaf. The machine check: the union of the leaf renders
  equals the whole render, byte for byte, after canonical JSON.

| Surface | Front end | Leaf derivation | Keyed form |
|---|---|---|---|
| Ruby Pangea | `CompilerBackend` (magnus or HTTP) | render then slice | an architecture that iterates a collection declares `leaf_key:`; `GitHubOrgWorkspace`'s `shard:` evolves into it |
| lava (tatara-lisp architectures) | `lava_backend.rs` | render then slice | `deflava-interface` declares the leaf key (`github-org-repos`: the `repos` list, field `slug`) |
| tatara-lisp terreno (`sourceKind: Lisp`) | reserved, unwired | render then slice once wired | same declaration as lava |
| blue | none today (`Dialect::Blue` is new) | a blue infrastructure declaration is entries; each entry is a leaf before lowering, so the split is `happa::split` over entries, in blue | native: the split is blue |
| raw Terraform JSON (`dialect: json`) | passthrough | render then slice (the body is already rendered); references come from `${…}` | n/a |

**Where the default lives.** One place: the `leaf.admission` entry in the
operator's embedded `happa` declaration, read through `LeafConfig`. Its
default changes from `enforce` (refuse a new fat template) to **`generate`**:
a never-reconciled fat template becomes a parent and its leaves are derived
for it. `enforce` and `off` stay as values. A split that fails a check (a
cycle, a broken bijection, a key that does not parse) is refused by name.

**Opt out.** Per template: `pangea.pleme.io/skip-leaf: provider-singleton |
atomic-bootstrap | external-contract | shared-lifecycle`. For the operator:
`leaf.admission: off`. A template with either runs monolithic, under the same
controller. Both modes stay in the test suite (§18 item O2).

**Rejected: hand-written leaf CRs** (what `pangea leaf plan --emit-dir`
produces for a human to apply). 1,023 files to keep in step with org.yaml is
hand-written repetition; the parent generates them.

## 11. Decisions are blue: the `happa` bidama

`happa` (葉っぱ, leaf): a template's leaves — split, graph, change, schedule
and verdicts. Collision check 2026-10-27: 0 hits across the corpus
(control `shikumi` 1,793 files, `caixa` 138 repo dirs), no repo, theory file
or tatara keyword. Registration in `blue/bidamas/NAMES.md` lands with the
bidama.

It is embedded the way kensa embeds `kensa`: `include_str!`, a `Loader`, and
`posture(&[])`, so every host effect is a refusal. Rust passes the clock, the
addresses, the digests and the leaf states in as values.

| Word | Decides | Moves up from |
|---|---|---|
| `happa::key(address)` | a resource address's leaf key | `pangea-leaf/src/address.rs` |
| `happa::classify(addresses, p95, budget)` | fat or leaf | `classify.rs` |
| `happa::split(addresses, opts)` | the partition, with reference closure | `partition.rs` |
| `happa::bijection(before, leaves)` | the BLAKE3 multiset check | `partition.rs` |
| `happa::slice(rendered, leaf)` | a leaf's Terraform JSON | new |
| `happa::inputs(declaration, inputs)` | a leaf's input slice (keyed form) | new |
| `happa::name(parent, key)` | the leaf's object name | `names.rs` |
| `happa::digest(inputs)` | the canonical content hash | `schedule.rs` (`inputs_digest`) |
| `happa::changed(leaves, recorded)` | the leaves whose digest moved | `schedule.rs` (`stale_leaves`) |
| `happa::graph(leaves, refs)` | the leaf graph; a cycle is a refusal | `dag.rs` |
| `happa::blocked(leaf, graph, states, applying)` | which upstreams hold a leaf | `schedule.rs` (`blocked_by`) |
| `happa::caps(declaration)` | the knobs of §9, each an entry | `config.rs` (`LeafConfig`, budget) |
| `happa::admit(candidates, caps, in_flight)` | who runs next, in what class | new |
| `happa::drift_due(leaves, now, interval)` | which leaves are due, spread by hash | new |
| `happa::admission(classification, skip, mode, reconciled)` | admit, generate or refuse | `admission.rs` |
| `happa::ratchet(prev, measured)` | the `status.leaf` floor | `ratchet.rs` |
| `happa::health(leaf, now)` | one leaf's verdict and alert verdict (§13) | new |
| `happa::judge(leaves)` | the parent's partial-state verdict (§12) | new |

**Reuses.** `kousei` (each cap and setting an entry, off or refused alone),
`kyohi` (refusals as data), `junjo` (priority order), `gurafu` (topo,
reachable, cycle, reverse: realized from the provisional verb space, with
`happa` its first consumer and `mokuroku::finds_cycle` its second), `shomei`
(BLAKE3), `hadome` (the ratchet's floor), `toukei` (p95), `deeta`, `retsu`,
`shuugou`.

**Rust keeps** provider gRPC calls, the magma engine's refresh, diff and
apply, Postgres I/O, the clock, the tokio runtime, the permit counters in
`FairBudget`, and the kube API writes.

**The Rust stays.** `pangea-leaf` is not deleted. It becomes the oracle of a
differential test (the `docs/RESOLVER-DIFFERENTIAL.md` precedent): every word
above returns the same value as its Rust source on the 2026-10-23 dry-run
address set. `leaf.engine: blue | rust` selects the judge; `blue` is the
default once the differential is green.

## 12. Failure isolation and the parent's partial state

**Rule.** A failed, stalled or rate-limited leaf blocks itself and the leaves
that depend on it (`gurafu` reachable on the reversed graph). Every other leaf
plans and applies.

**Mechanism.** Each leaf holds its own permit, so a stalled leaf holds one
slot. `pass_registry::preempt` (a1e6e3b) already drops a stalled pass's
permit, heartbeat task, lock and kube-runtime slot, keyed by namespace/name,
so it works per leaf with no change. A rate-limited leaf (a GitHub 403 or 429
with a retry hint) writes `status.rateLimitedUntil`, releases its permit and
requeues at that time; the shared pacer drops to its emergency rate for the
credential, so its siblings slow down but continue.

**The parent's status.** `status.leaves`:

```yaml
leaves:
  verdict: Partial        # Converged | Progressing | Partial
  total: 1023
  counts: {ready: 1019, planning: 1, applying: 0, failed: 1, stalled: 1, rateLimited: 0, blocked: 1}
  failed:      [{leaf: pleme-io-github-repos-estudio, reason: ImportAbsent, since: …}]
  stalled:     [{leaf: …, state: heartbeatLost, since: …}]
  rateLimited: []
  blocked:     [{leaf: …, by: [pleme-io-github-repos-estudio]}]
  truncated: 0            # names past 50 per list are counted, not listed
```

The parent's `phase` is `Ready`, `Progressing` or `Partial`. It is never one
`Failed` for the whole org. A whole-template failure is a bad state, not a
mode (§9.1 row 6). `happa::judge` computes the verdict.

## 13. Per-leaf observability

**Per leaf, in its own status** (most fields exist today): `phase`,
`status.pass` (`kind`, `heartbeatAt`, `incarnation`), `lastPlannedAt`,
`lastCycle.startedAt`/`completedAt` (plan duration), `driftTotal`,
`lastDriftCheckAt`, `failureCount`, conditions, `status.leaf.cycleP95Ms`, and
the new `rateLimitedUntil`.

**Metrics.** Every `pangea_template_*` series gains a `workspace` label (the
parent). `template` names the leaf. New:

| Series | Labels | Meaning |
|---|---|---|
| `pangea_workspace_leaves` | `workspace, namespace, state` | leaves per state |
| `pangea_leaf_plan_seconds` | `workspace` (histogram) | leaf plan duration |
| `pangea_workspace_leaves_unlabeled` | `workspace` | leaves past the label cap |

**Cardinality bound.** `observability.leaf_series: unhealthy` (default) emits
per-leaf series only for leaves not `Ready`, at most
`observability.leaf_series_max: 256` leaves. About 6 per-leaf families gives
≤ 1,536 series, plus 8 states per workspace. `all` emits every leaf (1,023 ×
6 ≈ 6,100 series for this org); `off` emits aggregates only.

**Alerts and events.** The stall and alert work keys on these:

- `pangea_template_stall{template, namespace, workspace, state}` — names the leaf;
- `pangea_template_pass_heartbeat_age_seconds{template, namespace, workspace}`;
- `pangea_workspace_leaves{state="failed"|"stalled"|"rate_limited"}` — the parent's partial state;
- `status.leaves.failed[].leaf`, `.stalled[].leaf` on the parent; `status.pass` on the leaf.

plantao alerts carry `template` (the leaf) and `workspace` (the parent), and
group by `workspace`. Leaf events go to the leaf object. The parent records
`LeafFailed` and `LeafStalled` events that name the leaf, one per leaf per
transition. engenho does not serve `events.k8s.io/v1`, so on ryn
`status.recentEvents` is where events land.

## 14. The engine: embedded magma only

Plans and applies run in the magma engine inside the operator. A leaf's state
is a magma state, read from and written to its `states` row. A leaf's plan is
`refresh_then_plan` in memory: no state file, no subprocess of tofu. Provider
plugins stay processes, because Terraform providers ship as plugin binaries (a
world fact). A pool in the operator reuses them across leaves (§18 item M1).

**Configured-off legacy, not designed on:**

- `TofuExecutor` and `executor: tofu`; ryn still sets `TOFU_BINARY` and does
  not set `PANGEA_FORBID_TOFU`.
- The `PangeaNamespace` `secretRef` probe route, which the operator names "the
  tofu path" (`namespace_controller.rs`).
- `pangea-cli import`, which prints `tofu init` and `tofu state push`.
- The outcome text "adopted via tofu import" on a magma import.

## 15. State migration, in the engine's terms

`pangea leaf plan` already emits typed `StateMove { from_template,
to_template, pangea_namespace, addresses }` and the bijection. The missing
piece is its executor. In Postgres, in one transaction:

1. copy the fat template's `states` row to an archive row
   (`<parent>@pre-leaf`);
2. for each `StateMove`, write the leaf's row: the magma state with only that
   leaf's resource instances, attributes unchanged, a new lineage, serial 1;
3. write the bijection (BLAKE3 before and after, disjointness) to
   `pangea_meta.artifacts` as kind `leaf-migration`;
4. abort if the bijection fails. Nothing is written.

No provider call, no import, no create. Then every leaf plans against the
live world (§9 step 5) and must show 0 add, 0 change, 0 destroy. Every leaf
carries `destroyProtection: true`. That first full plan costs one refresh of
the org (about 24 min, once).

**Rejected: import each resource into a fresh leaf state.** 2,053 provider
reads and 2,053 chances of an ambiguous import (the 2026-10-26 `estudio`
import reported success and left no instance).

**Reverse.** Setting the parent back to monolithic runs the same executor the
other way: the leaf rows merge into one, with the same bijection. Monolithic
mode is one field away, never a rebuild.

## 16. Rollout order

1. `pleme-io-github-repos` (ryn). Keyed lava derivation. The dry run is done.
2. Every new template: `leaf.admission: generate` makes it leaves from birth.
3. `roteador-buzios`, `repetidor-buzios` (raw JSON, one router each). A router
   commits UCI as one transaction, so each is a `skip-leaf: shared-lifecycle`
   candidate; the classification and the cycle p95 decide, and the annotation
   records the answer.
4. The templates on plo (`nodes/plo/pangea-org-catalogue.nix`). Not measured:
   plo did not answer on 2026-10-27.

### What does not fit, and its next step

| Item | Next step |
|---|---|
| `pleme-io-github-repos`: fat, grandfathered, 2,053 resources | cutover by §15, item O10 |
| `roteador-buzios`, `repetidor-buzios`: no `status.leaf`, no `skip-leaf` | classify, then leaves or a typed `skip-leaf` |
| `pangea-architectures/workspaces/pleme-io-opensource`: one Ruby state plus 4 hash shards | the Ruby keyed form (item R1); hash shards retire into leaf keys |
| `helmworks` `github-org-repos.yaml`: static 15m/45m phase timeouts sized for a fat template | per-leaf budgets from `status.leaf.cycleP95Ms`; the static values stay as the monolithic mode's |
| `pangea-operator/CLAUDE.md` "prefer sharding the workspace" | read "leaves", point here |
| `docs/TERRAGRUNT-PARITY.md` Gap 3 and P3 | point here; the run-all DAG is `happa::graph` |
| `TemplateDag::apply_order`, `destroy_order`: no live caller | `happa::admit` and the reverse-destroy order use them |
| `pangea-leaf` decisions in Rust | `happa` (§11), Rust kept as oracle |
| §9 step 6 "suspend the fat template" | the fat template becomes the parent (generator), not suspended |
| `reconcile.workers` 4 below `budget_global` 16 | raise the default to 16 |
| tofu rows in §14 | `PANGEA_FORBID_TOFU=true` by default in the chart |
| one CR holding 597 KB of variables | each leaf holds its own slice |

---

## 17. Scaling by leaf work

### Baseline (2026-10-27, ryn, read only)

| Fact | Value | Source |
|---|---|---|
| Operator RSS during the generation-29 fat plan | 23.6 to 55.6 MiB, mean 45.6 MiB, 40 samples | `ps` every 15 s, 13:22:55 to 13:32:56 UTC (ryn hosts the pod as a native process) |
| GitHub provider RSS in the same window | 21.2 to 33.2 MiB, mean 30.4 MiB | same samples |
| Pod limits | request 128Mi, limit 512Mi | pod spec |
| Restarts | 2; `lastState` is empty | engenho records no termination reason |
| metrics.k8s.io for the pod | "Metrics not available" | `kubectl top` |

The peak is 11% of the limit, so this window gives no sign of an OOM. The two
restarts have no recorded cause. a1e6e3b names a lease-renewal failure for the
12:19 one. The per-leaf footprint after cutover is not measured: on
2026-10-27 the cutover stopped at §9 step 4 (theory e4851bf). It lacks the
`StateMove` executor (item O10) and a per-leaf input slice (items O2, L1).

### The metric: pending weighted actions, capped by budget

Answer: weight by action kind. The weight is the provider calls the action
costs, because calls are the bottleneck (§1).

```
work(leaf)  = Σ_k  w_k · n_k(leaf)          over k ∈ {refresh, create, update, delete, import, replace}
n_k(leaf)   = the action counts of the leaf's newest plan (status.lastCycle.actionDistribution);
              a leaf with no plan yet counts n_refresh = its resource count
pending_c   = Σ work(leaf)  over queued and running leaves whose credential is c
metric      = Σ_c min(pending_c, r_c · H)  +  Σ (work of leaves with no paced credential)
desired     = clamp(⌈metric / T⌉, min, max)
```

`r_c` is the paced rate for credential `c` (3.125 req/s on ryn). `H` is the
scale horizon (default 300 s). `T` is the per-replica target. The cap `r_c ·
H` keeps out replicas that one credential's budget cannot feed. More workers
never raise a rate ceiling. Default weights: refresh 1, delete 2, update 2,
import 2, create 3, replace 5. They are estimates until item O12 calibrates
them from `pangea_provider_rpc_seconds_count` by action. Default `T` = 600
weighted actions, about 3 minutes of one credential's budget.

**Rejected: an unweighted count of actions.** A create costs about three
calls and a no-op refresh one. An unweighted count over-scales on drift and
under-scales on creates.

### The scale-out unit: workers claim leaves from a Postgres lease

Answer: leaf worker replicas claim work from a queue the leader writes. The
leader keeps the lease (`leader.rs`), admission, the graph and `happa::admit`.
It writes the admitted, ordered queue to `pangea_meta.leaf_queue`. A worker
claims the head with `SELECT … FOR UPDATE SKIP LOCKED` into
`pangea_meta.leaf_claims (leaf, holder, lease_until)`. It renews the lease
with each heartbeat and runs the leaf in its own magma engine. The state lock
stays as it is. When a lease lapses, the leaf goes back to the queue, so a
dead worker loses one leaf's progress and nothing more. With `workers.replicas:
0` the leader runs every leaf in process, as it does today.

The rate budget stays one budget. For each credential, the leader gives
each worker a share of `r_c`. The share is in proportion to the leaves of
`c` that the worker holds (`happa::shares`). The worker builds its
`shared_pacer` with that quota. The sum of the shares never exceeds `r_c`.

**Rejected: consistent-hash shards of the leaf key.** A scale event moves
leaves while they plan. Hashing ignores load, so one shard can get every
changed leaf while the others sit idle.

### Memory: derived, not guessed

```
request = base_rss + cap_worker · max_leaf_peak + providers · provider_rss
limit   = request · 1.5
```

`base_rss` is the worker's idle RSS. `max_leaf_peak` is the largest
`status.leaf.peakRssBytes` (new). A calibration pass measures it: the pass
plans each leaf alone (cap 1) and records the RSS delta. `providers` is the
pool size (item M1), and `provider_rss` is 33 MiB for GitHub from the baseline.
The typed setting is `workers.memory: derived | <quantity>`, and `derived` is
the default. Until calibration runs, `derived` takes the fat-plan peak
(55.6 MiB) as the bound for one leaf. This bound is safe, because a leaf is a
subset of the fat plan.

### Rendering: a real HPA, and engenho's dormant controller woken

engenho serves `autoscaling/v2` `HorizontalPodAutoscaler` and
`metrics.k8s.io`. It serves no `custom.metrics.k8s.io` and no
`external.metrics.k8s.io` (both NotFound, 2026-10-27). Its HPA controller is
compiled but dormant (`Dormant::Hpa`, `DormantReason::NoMetricsSource`). It
also has a known defect: `target_gv` sends most kinds to the core group.
keisoku is a blue measurement bidama, not a metrics adapter.

Answer: the chart renders one standard `autoscaling/v2` HPA on the worker
Deployment. The HPA uses `type: External`, metric
`pangea_leaf_pending_work`, `target.averageValue: T`, and `min` and `max`
from typed config. The leader publishes the metric two ways: as a gauge on
`/metrics`, and in `PangeaFleetStatus/default` as `status.leafWork`. On a
standard cluster, prometheus-adapter serves the gauge. On engenho, item EH1
wakes the HPA controller with a `MetricsProvider` that reads
`status.leafWork`. That item also fixes `target_gv` to read the target's own
`apiVersion`. The HPA object is the same on both clusters.

| Setting | Default | Off |
|---|---|---|
| `workers.autoscaling.enabled` | `true` | `false` = fixed `workers.replicas` (default 0: the leader runs every leaf, as today) |
| `workers.autoscaling.min` / `max` | 0 / 4 | — |
| `workers.autoscaling.target` (`T`) | 600 | — |
| `workers.autoscaling.horizonSecs` (`H`) | 300 | — |
| `workers.memory` | `derived` | a fixed quantity |

**Rejected: an engenho-only scaler CRD.** The HPA API already exists on
engenho. A second scaler shape would split the chart by cluster.

## 18. Build plan

Each item is one repo. Each names its test, seen red then green, and its off
switch. Items in one row group are independent.

### blue

| # | Item | Test (red, then green) | Off |
|---|---|---|---|
| B1 | Realize `gurafu` from `mokuroku/graph/gurafu-provisional.b`: `graph`, `add_edge`, `topo`, `reachable`, `cycle`, `reverse`, `subgraph`, `leaves`, `nodes` | `blue test`: a cycle is a refusal naming the cycle; `topo` is stable on ties; red against the empty package | a library: callers choose it |

### pangea-operator

| # | Item | Test | Off |
|---|---|---|---|
| H1 | `blue/happa/happa.b` with the §11 words | `blue test`; differential: each word equals its `pangea-leaf` source on the 2026-10-23 address set (1,023 leaves) | `leaf.engine: rust` |
| O1 | Embed `happa` (`include_str!`, `Loader`, `posture(&[])`) in `src/leaf_engine.rs` | one refused host call per capability (kensa `event_tests.rs` form) | `leaf.engine: rust` |
| O2 | Parent generator: `spec.leaves { derive: render \| inputs, each, key, enabled }`; SSA of leaf CRs with owner, label, digest | a mock executor sees 0 calls when no input moved, 1 leaf planned when one moved; both modes (`leaves` and monolithic) in the suite | `spec.leaves.enabled: false` or `skip-leaf` |
| O3 | Parent rollup `status.leaves` and phase `Partial` | one leaf `Failed`: parent `Partial` names it, its siblings reach `Ready`, its dependent is `blocked` | none (§9.1 row 6) |
| O4 | `leaf.admission: generate` | a new fat template yields leaves; a failed split is refused by name | `leaf.admission: enforce \| off` |
| O5 | Priority and caps in the dispatch gate: `happa::admit`, `leaf.per_credential_cap`, drift reserve; workers default 16. **After** the stalled-pass work in `pass_driver` and the budget code lands | 20 drift-due leaves and 1 changed leaf with 4 slots: the changed leaf is admitted first | `leaf.priority: off`, caps at monolithic values |
| O6 | Drift lane: `happa::drift_due`, the `:drift` shared pacer, `leaf.drift_share` | over a fake hour, 1,023 leaves are each due once, no minute holds more than ⌈1,023/60⌉ + 1 | `leaf.drift_share: 0` |
| O7 | Rate-limited leaf: `status.rateLimitedUntil`, permit release, requeue | a mocked 403 with `retry-after`: the leaf waits, its siblings apply | none |
| O8 | Metrics: `workspace` label, `pangea_workspace_leaves`, `pangea_leaf_plan_seconds`, the label cap | 300 unhealthy leaves emit 256 labelled and `unlabeled 44` | `observability.leaf_series: all \| off` |
| O9 | Parent `LeafFailed` and `LeafStalled` events | one event per leaf per transition, none on a repeat | none |
| O10 | `StateMove` executor (`pangea leaf migrate`) in one Postgres transaction, with archive and `leaf-migration` artifact | a forged bijection writes nothing; a good one writes N rows that read back to the same multiset; reverse merge restores the fat row byte-equal | run nothing; the fat row is untouched |
| O11 | Worker mode: `pangea_meta.leaf_queue`, `leaf_claims` with lease, the leader writes the queue, workers claim with `SKIP LOCKED`, `happa::shares` splits each credential's rate | two workers and 20 leaves: each leaf runs once; a killed worker's leaf runs again on the other; the summed pacer quota stays at `r_c` | `workers.replicas: 0` |
| O12 | `pangea_leaf_pending_work` gauge and `PangeaFleetStatus.status.leafWork`, by the §17 formula; weights calibrated from `pangea_provider_rpc_seconds_count` | a fixed queue gives the hand-computed value; a starved credential caps at `r_c · H` | `workers.autoscaling.enabled: false` |
| O13 | `status.leaf.peakRssBytes` from a calibration pass, and `workers.memory: derived` | the derived request equals the formula on fixture leaves | `workers.memory: <quantity>` |

### magma

| # | Item | Test | Off |
|---|---|---|---|
| M1 | A pooled `ProviderFactory` keyed by provider, version and config digest | 50 leaf plans start 1 provider process; a crashed process is replaced | the default `GrpcProviderFactory` |

### lava-architectures

| # | Item | Test | Off |
|---|---|---|---|
| L1 | `github-org-repos` declares its leaf key (`repos`, `slug`) | the union of 1,005 one-repo renders equals the whole render after canonical JSON | no key: render then slice |

### pangea-architectures

| # | Item | Test | Off |
|---|---|---|---|
| R1 | `leaf_key:` for architectures that iterate a collection; `GitHubOrgWorkspace`'s `shard:` reads it | rspec: the union of keyed renders equals the whole render | no key: render then slice |

### helmworks

| # | Item | Test | Off |
|---|---|---|---|
| C1 | Regenerate CRDs (`status.leaf`, `maxConcurrentLeaves`, `spec.leaves`, `status.leaves`); clear the drift baseline | `crd-drift.yml` red before, green after | n/a |
| C2 | `github-org-repos.yaml` gains `leaves:`; `PANGEA_FORBID_TOFU` defaults on; alert rules group by `workspace` | `helm unittest` and the `promtool` rule tests with a `workspace` label | `githubOrgReconciliation.leaves.enabled: false` |
| C3 | Worker Deployment and its `autoscaling/v2` HPA (`External`, `pangea_leaf_pending_work`); requests and limits from `workers.memory` | `helm unittest`: `enabled: false` renders no HPA and fixed replicas | `workers.autoscaling.enabled: false` |

### engenho

| # | Item | Test | Off |
|---|---|---|---|
| EH1 | Wake `Dormant::Hpa`: a `MetricsProvider` that reads `PangeaFleetStatus.status.leafWork`; `target_gv` from the target's `apiVersion` | an HPA on a Deployment (`apps/v1`) scales it; the dormant-catalog test loses the row | the dormant row stays (no HPA controller runs) |

### Live (needs the operator's go-ahead; this doc made no cluster write)

| # | Item | Check |
|---|---|---|
| E1 | Load test engenho at 1,100 `InfrastructureTemplate` objects on a scratch cluster | list and watch latency, store size |
| E2 | Cut over `pleme-io-github-repos` with O10, then settle. Another session owns it. On 2026-10-27 it stopped at §9 step 4: there is no O10, and there is no per-leaf slice (O2, L1) (theory e4851bf, 1,024 leaves on serial 181) | 0/0/0 across all leaves; then a 1-repo change plans in ≤ 10 s; then the per-leaf RSS for O13 |
