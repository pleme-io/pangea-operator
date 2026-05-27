# 0005 — Autonomic Convergence on Magma (the in-memory executive core)

> **★★★ CSE / Knowable Construction.** Operates under Constructive Substrate
> Engineering ([`theory/CONSTRUCTIVE-SUBSTRATE-ENGINEERING.md`](https://github.com/pleme-io/theory/blob/main/CONSTRUCTIVE-SUBSTRATE-ENGINEERING.md)).
> Companion canon: [`theory/MAGMA.md`](https://github.com/pleme-io/theory/blob/main/MAGMA.md) (§II.9 in-memory pipelines).

**Status:** Draft RFC. Supersedes the implicit reconcile model of
[0002](./0002-pangea-architecture-with-fluxcd-and-kenshi.md); does not change the
four CRDs, refines how they are *realized*.

---

## Prime Directive

> **Continuously realize declared cloud state on the substrate — in memory,
> attested, relentlessly — and never stop.**

Pangea declares the supercontinent's shape; **magma is the molten executive
force that realizes it.** The operator is the organism that wields magma in a
closed loop forever: it senses the gap between *declared* and *observed*, closes
it through magma **in process, in memory**, confirms the result, keeps watch for
drift, and reports the truth — surviving anything the environment (or its own
substrate) does to it.

**The organism is three co-developed parts, one system:** the **operator** (the
MAPE-K control loop), **magma** (the molten in-memory executive force), and
**pangea-api** (the living, queryable model of the whole estate — the
*pangea-service*). Everything below is in service of that one sentence.

## Why now — what today's model costs us

The operator currently realizes state by shelling out to `tofu`
(`executor/tofu.rs`): subprocess spawn → `init` → `plan` (full provider refresh
of *every* resource, off disk) → `apply`, with state in S3. Observed costs,
from live incidents:

- **Latency:** a single `pleme-io-opensource` reconcile spends **5+ minutes in
  `Planning`** refreshing ~660 GitHub resources off disk/network — even when the
  change is *two repos*.
- **Responsiveness gap:** source changes are picked up only on the **30m**
  `refreshInterval` poll; a merged change waited 30m+ and ultimately needed a
  hand-nudge (generation bump) to reconcile.
- **Substrate fragility:** an rio containerd-snapshotter corruption took down
  DNS + all of flux-system; the operator (and its `tofu`/S3 round-trips) was
  dead in the water until the node was repaired by hand.
- **Disk/state round-trips & clone races:** shared git clones and S3 state are
  serialization points and failure surfaces.

Magma's §II.9 in-memory model removes the *category* of these costs: Pangea Ruby
evaluates in-process, **rendered architecture never touches disk**, every
operation is a typed `shigoto::Job`, the resource DAG is a `shigoto::Dag`, and
cross-workspace values pass through Rust — **not** state files through S3.

## The organism — MAPE-K closed-loop control

Adopt **MAPE-K** (autonomic computing) as the explicit spine; it is the formal
form of "achieve / confirm / verify / report over knowledge":

| Stage | In the operator | On magma |
|---|---|---|
| **Monitor** | watch sources (git rev), specs, and *reality* (provider state); detect drift | in-process provider reads; no `tofu refresh` subprocess |
| **Analyze** | diff declared vs observed; classify (create/update/replace/destroy/out-of-scope/anomaly) | `magma-plan`: `Config × State → []Action` as typed `shigoto::Job`s, in memory |
| **Plan** | minimal, safety-audited, dependency-ordered change set | `magma-graph` wave-planning over the `shigoto::Dag` |
| **Execute** | idempotent apply, staged by blast radius | `magma-apply`: `shigoto::Scheduler` over provider gRPC, in process |
| **Knowledge** | typed receipts, drift history, failure stats, learned cadences | `magma-attest` (BLAKE3) receipts; in-memory state held across the loop |

Control-theory framing: a **closed-loop regulator** driving error→0, **edge-
triggered** on real change (source rev, spec generation, detected drift) for
near-instant response, plus **level-triggered** periodic resync as the
anti-entropy safety net. Stability (Lyapunov-style): converge monotonically,
never oscillate → backoff + hysteresis so it never flaps on transient drift.

## Magma as the in-memory executive core

This is the load-bearing change. Magma already has a seam in the operator:
`executor/iac_executor.rs` (trait), `executor/backend_select.rs` (chooser),
`executor/magma.rs` (backend), beside `executor/tofu.rs`.

**Target:** the reconcile pipeline runs **entirely in process**:

```
Pangea Ruby (magnus, in-process)  ─►  magma-config (typed Config)
   ─►  magma-plan (Config × State → []shigoto::Job)   [NO tofu plan subprocess]
   ─►  magma-graph (shigoto::Dag wave plan)
   ─►  magma-apply (shigoto::Scheduler over provider gRPC)
   ─►  magma-attest (BLAKE3 receipt → Knowledge)
```

Consequences that directly serve the Prime Directive:

- **In-memory, no disk:** the rendered plan and intermediate values live in RAM
  as typed Rust; no `tofu` subprocess, no plan files, no S3 round-trip on the hot
  path. Kills the 5-minute `Planning` tax.
- **Typed end-to-end (shikumi):** plan/diff/action are typed values, so
  Confirm/Verify can compare *intent vs outcome* structurally, not by grepping
  CLI text.
- **shigoto-scheduled:** retries, budgets, parallelism, and dependency ordering
  are first-class (`shigoto::RetryPolicy`, `shigoto::Dag`) — the substrate for
  the immune system and blast-radius control below.
- **Attested (tameshi/BLAKE3):** every realized plan has a content-addressed
  receipt → tamper-evident audit + dedup.
- **Pluggable & reversible:** lands behind `iac_executor` as a flag-gated
  `MagmaBackend`, with `TofuExecutor` as the fallback during migration. No
  big-bang.

> **Constraint (honesty):** magma is Draft v1 / M0 pre-implementation. So the
> *reliability* layers below ship **now** on the existing executor trait (they
> are executor-agnostic), and the in-memory magma core swaps in per-workspace,
> flag-gated, as magma clears M0 (Tier-1 providers passing all 5 test levels).

## The whole stack — one organism, co-developed

The Prime Directive is served by a **single co-developed system** of pleme-io
primitives, each a typed layer — not separate services bolted together:

| Layer | Primitive | Role in the organism |
|---|---|---|
| Declaration | Pangea Ruby (in-process via magnus) | desired state as expressive, typed DSL |
| Config | shikumi (shikumi-go for Go) | strongly-typed inputs, discovery, hot-reload — no ad-hoc parsing |
| Work-graph | shigoto (`Job`/`Dag`/`Scheduler`/`RetryPolicy`) | every op a typed, suspendable, budgeted, retried Job |
| Execution | **magma** (in-memory, §II.9) | the molten executive force — disk-free typed work-graph over provider gRPC |
| Attestation | tameshi / tabeliao (BLAKE3) | tamper-evident provenance: what changed, why, by whom |
| **Product/API** | **pangea-api** (§II.10) — *the pangea-service* | full in-memory fleet state + microservice assist + user-facing drift/cadence/alerting |
| Durable state | cnpg PostgreSQL + SeaORM | the queryable system-of-record (history, audit, drift, receipts) |
| Control loop | pangea-operator | the MAPE-K regulator wielding all of the above, forever |
| Telemetry | Vector + Grafana | the afferent nerves |
| Look & feel | ishou / borealis | one brand across web + terminal |
| Packaging | substrate (crate2nix) + helmworks (helm) | hermetic build + declarative fleet deploy |

Because shikumi/shigoto/magma/tameshi are **linked, not RPC'd**, a change to a
magma type ripples *at compile time* through the operator and pangea-api. That
compile-time coherence **is** the reliability advantage — co-develop them as one
cargo workspace + one release.

## pangea-api — the in-memory product layer (the pangea-service)

§II.10 anchors **pangea-api** as the product layer on magma: it consumes magma's
Rust library (§II.8 interface 4), holds the **full fleet state in memory** (the
in-memory chain of §II.9 is *non-negotiable* for its responsiveness), and offers
**microservice-level assistance** to the operator and to humans/agents. The
operator already exposes a GraphQL/gRPC surface at `pangea-api.quero.local` —
pangea-api is that surface **grown into a stateful, resident brain**:

- **Live state** — every workspace's declared + observed + last-receipt state,
  resident in RAM (magma keeps it disk-free), queryable in milliseconds.
- **Assist for the operator** — plan-preview / what-if / dependency-graph /
  drift-query / flow-orchestration (`magma_flow_run` over a `shigoto::Dag`). The
  operator offloads heavy compute to one shared, cached, in-memory brain instead
  of re-cloning + re-planning per reconcile (the cost we just watched).
- **The §II.10 dimensions** — instantiation, the user-facing state model (drift
  detection + reconcile cadence + alerting), exposure (DNS/ingress derivable),
  troubleshooting (TickReceipts + tameshi + Vector → queryable "what changed,
  why, when, by whom").
- **Three symmetric faces** (§II.8) — library (operator links it), MCP (agents
  drive flows), API/CLI (humans). One typed core, three surfaces.

magma's native `daemon` / `watch` subcommands + the NixOS `services.magma`
per-workspace watcher (§II.7) are the resident-process substrate; pangea-api is
the fleet-level brain over them. **Magma frees the execution hot-path from disk;
pangea-api is what that freedom unlocks — a living, queryable model of the whole
estate.**

## Durable state — SeaORM + cnpg PostgreSQL (service standards)

In-memory ≠ amnesiac. The hot path is disk-free (magma §II.9), but the *service*
keeps a durable, queryable **system-of-record** so it survives restarts and
answers "what happened last quarter":

- **Today:** the operator uses **sqlx** + `PostgresStateBackend` (OpenTofu state
  in PostgreSQL) over the **cnpg `pangea-database`** already running in rio.
- **Proposal:** adopt **SeaORM** (entities + the SeaORM **migration** framework +
  the standard service hygiene — typed queries, pooling, schema versioning) for
  **pangea-api's user-facing durable model**: reconcile-cycle receipts, drift
  history, attestation index (tameshi), audit trail, learned cadences/backoffs
  (the Knowledge base). magma's own state surface stays as-is
  (`magma-state`/`magma-backend`); SeaORM governs the *product's* state.
- **The split that resolves "never go to disk":** disk is **never** a
  serialization point in *execution* (magma keeps values typed in RAM); postgres
  is the **asynchronous record-of-truth** for *history/audit/query*. Live truth
  in memory, durable truth in postgres — the loop never blocks on disk.

## The four duties — first-class, independently-measured sub-loops

Today these are blurred into one "reconcile." Split them; each gets its own
condition + SLO:

- **Achieve** — drive actual→desired (`magma-apply`). SLO: *time-to-converge*.
- **Confirm** — prove the apply did *exactly* what was planned: per-resource
  outcome vs the typed intent + post-apply read-back. Catches "provider returned
  OK but lied." (Extends today's `ReconcileCycle`.)
- **Verify** — continuously re-check that converged state *stays* converged.
  SLO: *drift-detection latency*.
- **Report** — typed, queryable evidence of all three (§ Observability).

## Responsiveness — edge + level, per-provider, rate-aware

- **Edge-trigger on source:** consume the Flux `GitRepository` as a `sourceRef`
  and *watch* it; a new artifact revision reconciles affected templates in
  seconds. (Today's direct clone + 30m poll is what stranded our merge.)
- **Edge-trigger on drift:** ingest provider events/webhooks where available.
- **Level resync as anti-entropy:** periodic full pass, sized **per provider** —
  GitHub's 660-resource org hits secondary limits (the 422 storms), so it gets
  edge-trigger + a moderate poll; AWS/Cloudflare can poll harder.
- **Adaptive cadence:** volatile workspaces resync faster, stable ones slower
  (*allostasis* — move the resync setpoint from observed volatility).

## The immune system — anomaly handling & substrate resilience

Failures are *expected inputs*. Map a **failure taxonomy → response policy**:

- **Transient** (429/403/422, 5xx, network): exponential backoff + jitter, retry
  within budget forever (`shigoto::RetryPolicy`). Relentless *with* backoff —
  never a tight loop.
- **Persistent-correctable** (token/secret expiry, quota): keep retrying **and**
  escalate with the exact remediation surfaced.
- **Substrate failure** (the rio containerd/DNS outage): the operator's own
  platform broke. Add **substrate health-gating** — detect a degraded control
  plane, enter safe-mode (don't thrash, alert loudly), auto-resume on recovery.
  *The organism must survive its own organs failing.*
- **Structural** (compile error, e.g. the cloudflare `uninitialized constant`):
  fail+isolate that template (**bulkhead**), keep the rest converging.

Mechanisms: **circuit breakers** (Nygard, *Release It!*), **bulkheads** (per-
template isolation), `settlingPolicy` (give-up-then-alert on un-settling drift),
worst-action-wins escalation (the existing `ReactivePolicy`, extended). Default
posture `onExhaustion: Alert` — keep reconciling; never silent-Suspend unless a
human asks.

## Anti-entropy & continuous verification

- **Drift as a continuous first-class signal** (Dynamo-style anti-entropy), not
  just "plan diff at poll time."
- **Refresh strategy** (kills the plan latency): skip provider refresh on
  frequent edge-triggered passes (we know what changed); full refresh only on the
  periodic anti-entropy sweep; **targeted** plans on the synth-diff subset.
- **Out-of-band adoption:** importHints so reality created elsewhere is adopted,
  not duplicated/destroyed.
- **State↔reality reconciliation:** detect + heal "state says exists, reality
  doesn't."

## Progressive convergence & blast-radius control

- **Stage by blast radius:** create/update auto-applied; replace/destroy gated
  (structured plan-scope audit + optional canary). Aggressive ≠ reckless.
- **Dependency-ordered convergence:** topo-order via `shigoto::Dag` (prereqs
  first).
- **Concurrency + per-provider rate budget** so one workspace can't storm the
  fleet.

## Aggressive performance without meltdown

- In-memory magma core (no subprocess/disk) is the headline win.
- Raise controller concurrency + apply parallelism within provider budgets.
- **Synth/source cache keyed by revision** (kills the shared-clone race we hit).
- **Incremental synth** — re-synth only changed workspaces.
- Keep every invariant guard — they make "fast" *safe*.

## Observability & reporting — the nervous system

- **Typed receipts** (`ReconcileCycle` + magma-attest): planned vs achieved vs
  confirmed, per-resource outcomes, drift, anomalies, timings, BLAKE3 digest.
- **SLO metrics:** time-to-converge, drift-detection latency, drift-correction
  success rate, reconcile error rate (`PangeaControllerReconcileRateHigh`
  lineage), per-provider API-budget burn, substrate-health.
- **Fleet convergence dashboard** + an audit trail of "what changed, when, why,
  to what."
- **Proactive push** of receipts/anomalies (ntfy/Slack/GitHub).

## Antifragility — learn & adapt

- **Adaptive backoff & cadence** from history (flaky resource → longer backoff;
  volatile workspace → faster resync).
- **Chronic-drift detection** — a resource that perpetually drifts is a bug or an
  external owner → report, don't fight forever.
- **Failure-pattern memory** in Knowledge → auto-surface known remediations.
- **Postmortem→guard feedback loop** (already practiced; formalize it).

## Invariants — what aggression must never break

1. **Safety > liveness on destructive ops** — never destroy/replace outside the
   audited scope; destroyProtection + plan-scope audit gate every apply.
2. **No self-resonance** — never reconcile in response to your own writes. Keep
   the diff-gate + generation predicate-filter + requeue floors that ended the
   123-PATCH/s, ~7.5-core storm
   ([postmortem 2026-05-07](../postmortems/2026-05-07-status-write-self-trigger-loops.md)).
   *Aggression is safe because of these.*
3. **Bounded blast radius** — one bad template/provider can't storm/starve the
   fleet.
4. **Truthful state** — `Ready` means *verified-converged*, not "apply exited 0".

## Formal guarantees & testing

- **Safety:** never destructive-out-of-scope; never self-resonate.
  **Liveness:** every declared state is eventually achieved-and-confirmed given a
  reachable provider. State both explicitly; assert in tests.
- **Convergence as a contraction mapping** — each cycle strictly reduces or holds
  error.
- **Chaos + property tests** — kill the substrate (containerd, DNS,
  source-controller — *the exact rio failure, made a permanent test case*),
  expire tokens, inject 429/5xx, partial applies → assert safe degradation +
  auto-recovery + honest reporting. Magma's own 5-level compat corpus
  (theory/MAGMA.md §II.6) gates the executor swap.

## Packaging & delivery — helm + rio (develop-as-one, ship-as-one)

- **Build:** substrate's crate2nix for the Rust crates (operator + magma +
  pangea-api in one cargo workspace), `mkGoTool` for Go consumers (borealis +
  shikumi-go, already published) — hermetic, reproducible.
- **Package:** extend `helmworks/charts/pangea-operator` into the unified
  release — operator + **pangea-api** (Deployment/Service/HPA + the §II.10 API
  ingress), magma linked into both, the four CRDs, RBAC, the cnpg
  `pangea-database` + a **SeaORM migration** Job (pre-upgrade hook), a Vector
  sidecar. Full helm exposure via `values.yaml`: backend selection
  (`tofu|magma`), per-provider cadence, concurrency/rate budgets, ReactivePolicy
  defaults, substrate-health-gating, pangea-api replicas + memory sizing.
- **Deliver to rio** on the **same Flux GitOps rail we just exercised**
  (`GitRepository` → operator): chart bump → HelmRelease → rio. **Then verify
  it's all up:** operator + pangea-api `Ready`, CRDs at the new schema, SeaORM
  migrations applied, API reachable, a smoke reconcile **converges + attests**,
  SLO metrics flowing to Grafana. (The rio recovery + reconcile path is proven
  end-to-end; this rides those rails.)

## Roadmap — guard-first, flag-gated, per-workspace

**Now (executor-agnostic; ships on the existing `tofu` backend, no magma dep):**
1. **Invariant guards + observability** — measurable, safe foundation.
2. **Source edge-trigger** (`GitRepository` watch) — biggest responsiveness win;
   this is exactly what would've created our repos in seconds instead of a nudge.
3. **Refresh/targeting + per-provider cadence** — kills the 5-min plan latency.
4. **Immune system** — failure taxonomy, circuit breakers, **substrate
   health-gating** (the rio outage made a first-class concern).

**As magma clears M0–M5 (theory/MAGMA.md §VI):**
5. **Magma in-memory core** behind `iac_executor` (`MagmaBackend` beside
   `TofuExecutor`), per-workspace, `backend: magma` flag-gated (§II.11).
6. **Stand up pangea-api** — resident in-memory state fabric + SeaORM durable
   model + the three §II.8 faces; operator offloads assist to it.
7. **Helm-unify + deliver to rio** (above), then **adaptation + formal/chaos
   tests** (incl. the rio failure as a permanent case) — continuous.

Every step behind a feature flag, rolled out per-workspace, validated against
SLOs before fleet-wide. **No big-bang; the organism keeps converging throughout.**

---

*This RFC is the destination. Magma's M0 is the first step down toward it; the
reliability layer (steps 1–4) is walkable today. The north star is one
in-memory, attested, relentless organism that imposes declared cloud state
forever — and tells the truth about it.*
