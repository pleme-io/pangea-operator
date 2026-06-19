# Pangea-Operator Lifecycle State Machines

> **Destination-first** (Operating Principle #0). This document leads with the
> absolute-best long-term shape of the operator's lifecycle, then phases the
> path. It is grounded in a faithful audit (2026-06-19) of the *current*
> implementation, not an aspiration.

## 1. The problem: a patchy, hand-operated lifecycle

The operator's job is one sentence: **when anything that defines an
infrastructure outcome changes, converge the world onto it — recompile, plan,
apply, verify, watch for drift — and never get stuck.** Today that loop is
real but *patchy*: the convergence is correct on the happy path, yet the edges
are firefought by hand. The audit found, concretely:

- **Transitions are implicit.** `Phase` (12 variants) has no transition table;
  the legal edges live scattered across the `handle_*` methods of
  `template_controller.rs`. There is no mechanical answer to "what are the
  legal transitions?", no proof a phase can reach a good terminal, no proof a
  phase isn't a wedge. (`pleme-io-opensource` wedged ~31 days; the
  env-fetch-compile class wedges in `Compiling` forever — both are
  *unrepresentable-by-hope*.)
- **Two weakly-coupled escalation systems** — the settling/compile-failure
  counters + `EscalationLadder`, and `ReactivePolicy` — evaluated in different
  places. `ReactivePolicy` only fires on a *Failed* transition, so its
  `phaseTimeout` cannot rescue a template wedged mid-phase.
- **`Verifying`/`Verified` are dead pass-through stubs** — the gem-readiness
  gate they name is never consulted; templates compile unconditionally.
- **The ruby environment is rebuilt by rebuilding the whole image.** A
  foundational-gem bump = flake-input bump + bundix regen + full image
  rebuild + autobump + roll. Architecture gems + templates *are* runtime
  (good), but the boundary is undocumented and the foundational path is
  heavyweight.
- **The runtime gem load-path is non-deterministic** — two clones of
  `pangea-architectures` on one process-global `$LOAD_PATH`, an older copy
  shadowing a newer one's autoload → `uninitialized constant
  ...::OpenSourceRepo`. The fix today is purge-prefix band-aids;
  `rubyIsolation: process` (the by-construction fix) is rolled back because
  the worker won't boot.
- **Disk is still load-bearing on the magma path** — the workspace + main.tf.json
  live on a pod-roll-wiped emptyDir; per-phase "main.tf.json missing → bounce
  to Compiling" guards are a band-aid over disk dependence.
- **Manual interventions** the redesign must delete: `refreshInterval` bumps to
  force recompile, gem-cache cold re-clone on every roll, build OOM tuning,
  manual rollback, `rubyIsolation` flip-flopping, the hand-maintained provider
  mirror list.

The destination removes the firefighting by making the lifecycle a **set of
typed, convergent, mechanically-proven state machines** that compose, run
in-memory, and make the wedge/contamination/stale classes *unrepresentable*.

## 2. The composition (reuse, don't reinvent)

Four shipped/canonical pleme-io primitives compose into the whole design. The
operator already references three of them shallowly — the move is to adopt
them for the *core loop*.

| Layer | Primitive | Owns | Maturity |
|---|---|---|---|
| **Lifecycle FSM** | eclusa/galho *pattern* (`theory/ECLUSA.md`) | one typed `transition_table()`, `PhaseClass` partition, two good terminals, `validate()` no-trap + `is_reachable()` BFS + always-restable comfort matrix | principles shipped in `galho-types`; copy the test shapes |
| **Authoring** | TYPED-SPEC triplet + `(deftyped-fsm)` | typed Rust border + Lisp spec (destination) + mockable interpreter behind an `OperatorEnv` trait | triplet ★★ shipped; `(deftyped-fsm)` codegen unshipped — hand-write legs 1+3 now |
| **Intra-cycle work graph** | shigoto (`theory/SHIGOTO.md`) | each lifecycle step = a `RecordingJob` in a `Dag`; `Gate`s, `RetryPolicy`, `BudgetTree`, `TickReceipt` → `status.lastCycle` | shipped; operator already pins `shigoto-types`; this makes it the 2nd prod consumer |
| **Convergence semantics** | Viggy Seven-Beat (`theory/CONTINUOUS-SOLUTION-MACHINE.md`) | the loop *is* Observe→Diff→Classify→Decide→Act→Attest→Tick; converge existing reactive/escalation/anomaly machinery onto canonical `RemediationPolicy`/`EscalationLadder` | directive ★★; promessa crate draft — adopt the shape |

## 3. The set of state machines

The operator is **not one FSM** — it is a small mesh of typed machines, each
owning one convergence concern, composed by the seven-beat tick.

### 3.1 `TemplateLifecycle` — the spine (the 12-phase machine)

The InfrastructureTemplate phase machine, but typed and proven. **Built — M0
(this commit):** `controller/lifecycle.rs`.

- One `TRANSITIONS` table is the single source of truth. `Phase::advance(trigger)`
  is a pure lookup; an illegal `(from, trigger)` is a typed `TransitionError`
  with a great error stack (names the phase, the trigger, and every legal
  trigger), never a silent `to_phase()` fall-through.
- `PhaseClass` partition: `Forward | Recovery | Settled | Terminal | Failure`.
  Two good terminals: `Ready` (Settled) and `Destroying` (Terminal → gone).
  `Failed`/`CompileBlocked` are **Failure detours that must carry a remediation
  edge** — never wedges.
- Four CI forcing-functions make whole classes unrepresentable-at-CI:
  `every_phase_is_enumerated` (no omitted variant — the legacy `Phase::ALL`
  *does* omit `CompileBlocked`), `no_traps`, `every_phase_reaches_a_good_terminal`
  (BFS), `every_phase_is_a_comfortable_berth` (always-restable matrix — the
  exact guarantee whose absence let `pleme-io-opensource` wedge).

Tier-honest: this is a controller reading `Phase` off a CR, so legality is
**parse-time-rejected** (a `Result::Err`), not a compile error; reachability +
no-trap + comfort are **mechanical CI proofs**, not type-level. We never round
up.

### 3.2 `RubyEnv` — deterministic, in-memory gem realization (kills the shadowing class)

The machine that owns "what Ruby code is loaded, and is it the right version?"
Replaces the non-deterministic process-global `$LOAD_PATH` + purge band-aids.

- **States:** `Empty → Cloning → Loaded{rev} → Stale{behind} → Recycling`.
- **Trigger of record:** a per-(gem, ref) content hash. A mutable ref (`main`)
  whose remote HEAD moved ⇒ `Stale` ⇒ re-clone ⇒ `Loaded{new-rev}`. The
  per-template compile reads its env's `Loaded` rev as a *value*, so a compile
  can never silently bind an older shadowing copy.
- **Unrepresentability target:** *one* logical gem version is loadable per
  compile. The destination is `rubyIsolation: process` — a fresh VM per
  compile makes cross-template contamination structurally impossible (no shared
  `$LOADED_FEATURES`). Until the worker-boot bug is fixed, the interim is a
  **content-addressed load-path** derived from the broadcast gem's actual load
  tree (the code already flags the hardcoded purge list as drift-prone), not
  the hardcoded `["Pangea::Architectures"]` prefix.
- **In-memory:** the gem cache moves from the pod-roll-wiped emptyDir toward a
  content-addressed store (DB/`maré`-style) so a roll doesn't cold-re-clone
  every gem.

### 3.3 `SourceFreshness` — change detection as a typed observation

Already largely real (`controller/template/freshness.rs`, the git-edge gate).
Promote it to a first-class machine:

- **States:** `Unobserved → Observed{head} → {Fresh | Behind{compiled,head}}`.
- The headline invariant is shipped: *"Settled against a stale compile is
  mechanically unutterable"* (`ready_drift_decision`). Tier-honest: this is a
  **C2 runtime observation** (per-tick renewed), never proven — that's the
  correct ceiling for "did the external world change."
- Folds the per-phase `main.tf.json`-missing restart-bounce into one
  `SourceStaleOrWorkspaceLost` recovery edge (already modeled in §3.1's table).

### 3.4 `GemReadiness` — make `Verifying`/`Verified` load-bearing

The dead stubs become real: the template's `Verifying → Compiling` edge is
**gated** on the parent `WorkspaceCatalog.status.verified` (every required
`ArchitectureGem` is `Loaded` + smoke-tested). A compile against an unloaded
gem becomes unreachable — the gate the phase names is finally enforced.

### 3.5 `Convergence` — the seven-beat tick that drives them all

The operator's reconcile becomes the Viggy tick, realized as a **depth-7
shigoto `Dag`** per template: `Observe → Diff → Classify → Decide → Act →
Attest → Tick`. Each beat is a `RecordingJob`; the intra-cycle work
(detect-change, ensure-ruby-env, recompile, init, plan, apply, verify, test)
are jobs with typed `Gate`s (`GemsLoaded`, `SourceFresh`), `RetryPolicy`, and a
`BudgetTree` (the `ReconciliationLoopSpec` already documents its parallelism in
shigoto vocabulary). `TickReceipt` *becomes* `status.lastCycle` — the
hand-rolled 989-LOC cycle-receipt code collapses onto the primitive.

### 3.6 `Remediation` — one escalation system, not two

Converge the settling counters + `EscalationLadder` + `ReactivePolicy` +
`anomaly_tracker` onto the canonical Viggy `RemediationPolicy` /
`EscalationLadder` / `AnomalyEmission` types. The escalation rungs
(`RefreshSource → ReloadGems → RecycleWorkers → PauseAndAlert`) stay, but as
*one* ladder evaluated on a real per-phase timer (fixing the gap where
`phaseTimeout` can't rescue a mid-phase wedge).

## 4. Unrepresentability ledger (tier-honest)

Per `theory/UNREPRESENTABILITY.md` §II — a `Result::Err` is *mitigation*, a
compile error / absent path is *unrepresentability*. We state the tier; we
never round up.

| Bad state | Mechanism | Tier |
|---|---|---|
| A `Phase` exists but isn't in the FSM | exhaustive `class()` match + `all_lifecycle` + `assert_phase_exhaustive` + `every_phase_is_enumerated` test | **compile-error + CI** |
| An illegal transition is realized | `Phase::advance` table lookup → typed `TransitionError` | **parse-time-rejected** |
| A phase is a wedge (no exit) | `no_traps` test | **mechanical CI** |
| A phase can't reach a good terminal | `every_phase_reaches_a_good_terminal` BFS test | **mechanical CI** |
| A phase isn't a comfortable berth | `every_phase_is_a_comfortable_berth` matrix test | **mechanical CI** |
| Settled against a stale compile | `ready_drift_decision` | **parse-time-rejected** (`Freshness::Stale` has no "settled" arm) |
| Cross-template gem contamination | `rubyIsolation: process` (destination) | **truly-unrep** (no shared VM) — *interim is mitigated* |
| Half-applied reconcile | `put_apply_result` 1-txn (state + bundle) | **truly-unrep** (shipped) |
| Lost workspace on pod roll | DB-persisted rendered config/plan (destination) | **truly-unrep** — *interim is the bounce guard (mitigated)* |

## 5. Phased path

- **M0 — typed FSM core (DONE, this commit).** `controller/lifecycle.rs`:
  `PhaseClass`, `Trigger`, `Transition`, `TRANSITIONS`, `Phase::advance` with a
  great error stack, + the four CI forcing-functions. Additive — does not yet
  rewire runtime dispatch, so zero behavior change; it is the source of truth
  the handlers migrate onto.
- **M1 — rewire dispatch onto the table.** Replace each `update_phase(next)`
  call site with `phase.advance(trigger)?` so the handlers *consume* the table.
  An illegal transition becomes a typed error at the call site, not a silent
  realization. Author leg 2 (`(deftyped-fsm pangea-operator-lifecycle)`) +
  leg 3 (mockable `OperatorEnv` interpreter) so the reconcile path is testable
  without kube/magma/Postgres.
- **M2 — `GemReadiness` gate.** Make `Verifying/Verified` consult
  `WorkspaceCatalog.verified`; remove the dead pass-through.
- **M3 — `RubyEnv` determinism.** Content-addressed load-path; land
  `rubyIsolation: process` (fix the worker boot) for by-construction isolation.
- **M4 — seven-beat shigoto Dag.** Reconcile becomes the depth-7 tick;
  `TickReceipt` replaces the hand-rolled cycle receipt. Operator becomes
  shigoto's 2nd production consumer (promotes shigoto to ★★).
- **M5 — one Remediation ladder.** Converge the two escalation systems;
  per-phase timeout fixes the mid-phase-wedge gap.
- **M6 — zero-disk.** DB-persist the rendered config/plan/bundle so the
  workspace clone + main.tf.json leave disk; delete the bounce guards.

Each milestone is independently shippable and leaves the operator
strictly-better; M0 is landed and proven by `cargo test
controller::lifecycle`.

---

## 7. Scaling & concurrency — the workspace seam, async, parallel

The operator is not one FSM but a **composition of typed FSMs at four
scopes**, keyed on the workspace — the seam where state isolation (one
`PangeaNamespace` schema), gem set, git source, and the policy cascade all
coincide. Sharding on that key is *isolation*, not mere partitioning, and the
DB-backed/recompute-from-Postgres model is what makes it safe (no shared
in-process mutable state to coordinate).

### 7.1 The four scopes

| Scope | FSM | Owns | Status |
|---|---|---|---|
| **Shard** | `Unassigned → Claiming → Owned → Draining → Released` | which replica reconciles which workspace; rebalance on scale change (lease-based, active-active) | designed (`controller::shard_lifecycle` — next) |
| **Workspace** | `Unloaded → LoadingGems → Ready → Converging → Settled` (+ `GemsFailed`/`Degraded` berths, `Draining → Released`) | per-workspace ruby env, the per-workspace concurrency **budget**, the template **dependency DAG**, drain-safe handoff | **built + proven** (`controller::workspace_lifecycle`, 7 CI proofs) |
| **Template** | the 12-phase lifecycle (M0/M1) — **now async** (gated on jobs) | one infra unit's converge loop | M0/M1 shipped |
| **Job** | shigoto `JobPhase` | one compile/plan/apply *execution* | reuse shigoto |

They nest: shard assigns a workspace → workspace loads its gems + budgets +
DAG-orders its templates → each template advances per-phase → each phase's work
runs as an async job. Every scope is a typed table with the four
forcing-functions (enumeration, no-trap, reachability, comfort), so determinism
and convergence are mechanical at *every* layer.

### 7.2 Async — phases are checkpoints, not blocking calls

The long, RPC-heavy, samba-paced work (compile/plan/apply) becomes **dispatched
async jobs**; the FSM observes their DB-persisted result and advances:

```
reconcile(Planning):  no job in-flight → dispatch PlanJob(template, base_state_hash); requeue   (fast)
PlanJob (executor pool):  magma plan over read-RPCs → Plan artifact to Postgres (content-hashed, from_state=base_hash)
reconcile(Planning):  observe Plan artifact → has_changes → advance (Ready | Applying)
```

The **control plane** (the FSM reconcile loop) never blocks — it observes
DB/job state and advances; the **data plane** (a bounded shigoto executor pool)
does the heavy work. The phase-exit is a shigoto **Gate** on the job reaching a
terminal `JobPhase` — the two FSMs compose by gating.

### 7.3 Parallel — unit = template, budgeted by workspace, ordered by DAG

- **Across templates** → parallel: state is per-template
  (`{schema}_{template}_states`), so different state rows never contend.
- **Within a workspace** → a shigoto `Dag` of templates: independents run in
  parallel `waves()`; cross-referencing templates (output → input) get
  dependency edges and serialize.
- **Per template** → serial with itself (magma `StackLock` per state root —
  cooperative join, not contend).
- **Concurrency bound** → shigoto `BudgetTree` keyed by workspace: each
  workspace gets a fair-share slice, so a wedged workspace cannot drain the
  pool from the others (kills the single-reconcile-worker starvation).
- **Across replicas** → workspace-sharded; per-namespace state isolation means
  no cross-replica DB contention.

### 7.4 The four correctness invariants (what makes async+parallel *safe*)

1. **Stale-plan refusal** — `Plan<S>` carries `from_state`; a base that moved
   between an async plan and its apply makes the apply refuse (eclusa). This is
   what makes the async plan→apply *gap* safe.
2. **Atomic apply** — state row + bundle in one Postgres tx → a half-applied
   parallel reconcile is unrepresentable (shipped).
3. **Idempotent + resumable jobs** — every job recomputes from DB; a pod roll or
   lease handoff mid-job loses nothing.
4. **Always-restable berth** — the comfort matrix (proven at template *and*
   workspace scope) is what makes shard draining safe: park at a comfortable
   phase, release the lease, a new replica resumes from Postgres. Drain-safe
   rebalancing is the comfort property applied at the shard scope.

And it **unifies the contamination fix**: a per-workspace ruby env loads only
that workspace's `requiredGems`, so there is no second `pangea-architectures` on
a shared `$LOAD_PATH` — the `OpenSourceRepo` shadowing and the scaling story are
the same move.

### 7.5 Scaling milestones (workspace-keyed, each shippable)

- **S0 — Workspace FSM (DONE).** `controller::workspace_lifecycle` typed +
  7-proof; the seam is now a typed convergence object.
- **S1 — per-workspace budget.** shigoto `BudgetTree` keyed by workspace; kills
  starvation; intra-replica, no new infra. (extends M4)
- **S2 — async plan/apply jobs.** Split Planning/Applying into dispatch+observe
  over a bounded shigoto pool; control/data-plane split.
- **S3 — Workspace controller + template DAG.** Wire the Workspace FSM to a real
  controller owning the budget + gem-env + dependency-ordered template Dag.
- **S4 — per-workspace ruby env.** Each replica/worker loads only its
  workspace's gems; kills the `$LOAD_PATH` shadowing by construction. (= M3)
- **S5 — Shard FSM (active-active).** Lease-based workspace→replica assignment
  replacing the singleton; horizontal scale; drain-safe rebalance via the
  comfort property.

Each guarantee is **tier-honest**: transition legality is parse-time-rejected
at every scope; reachability/no-trap/comfort/budget-fairness are mechanical CI
forcing-functions, not type-level proofs.
