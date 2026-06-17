# Source freshness — the operator tracks its git edge as a first-class FSM behavior

> Ends the failure category where the operator reported `phase=Ready` while its
> source was N commits behind `main@HEAD` (rio, 2026-06-16: `pleme-io-opensource`
> sat `Ready` 25 commits / 6 days behind because the only HEAD observation was a
> gated side-check that silently froze when the probe errored).

## The defect (root cause, in code)

The only remote-HEAD observation (`freshness::observe_head`, a `git ls-remote`)
lived inside `source_freshness_gate`, invoked from inside `handle_ready` **after**
the drift-throttle early-return — so HEAD was only observed on the 32m drift
cadence. Worse, the gate's `Err` arm returned `Proceed(Unknown)` **without**
advancing `lastFreshnessCheckAt`, so a failing probe (a misfiring `GIT_ASKPASS`
hanging to the 30s timeout) **froze** `observedHeadRevision`/`lastFreshnessCheckAt`
while `lastDriftCheckAt` kept advancing — the exact rio signature. And the `Ready`
condition was built from the `Phase` enum **alone** (`conditions_for_phase`), with
zero freshness input — so `Ready=True` was structurally decoupled from at-HEAD.

## The fix (shipped: A + B + C)

**A — HEAD observed as the first beat of every tick.** `handle_ready` now runs
`source_freshness_gate` (a 1-RTT `ls-remote`) **before** the drift-throttle and the
restart guard. A HEAD-advance bounces to `Compiling` immediately (re-render),
regardless of the drift interval; only the *expensive plan* stays throttled. A
failed probe advances the *attempt clock* (`ObservationOutcome::Unobserved` →
`build_freshness_patch` writes only `lastFreshnessCheckAt`) so it's visibly
"checking + failing", never silently frozen. `GIT_TERMINAL_PROMPT=0` (+ no
ambient git config) on every git invocation (`non_interactive_git_env`) means a
bad credential helper **fails fast** instead of hanging.

**B — `Ready ⟺ at-HEAD`.** `conditions_for_phase` now takes a `SourceFreshState`
(`Fresh | Behind | Unverified | NotApplicable`, derived by
`status::source_fresh_state` from `compiledRevision` vs `observedHeadRevision`).
The `Ready` condition is `true` **only** when the source permits it (`Fresh` or
non-git); a `Behind`/`Unverified` git source forces `Ready=False` even while
`phase==Ready`. A first-class `SourceFresh` condition surfaces the edge state.
**"Ready while behind HEAD" is no longer expressible** on the status surface
(test: `reconciler::tests::ready_condition_false_when_behind_head`).

**C — loud, not silent.** A probe failure emits a typed `SourceUnobservable`
Warning event. The Ready condition stays independently honest (a failed probe
doesn't advance `observed_head`, so Ready reflects the last *verified* edge).

**Tier honesty (do not round up):** A+B make "Ready while behind HEAD"
**parse-time-rejected** on the condition surface — `Phase::Ready` is still a
constructible enum and HEAD is a C2 external observation renewed per tick, not a
compile-time proof.

## Staged next (D — DB-backed source; the deeper structural piece)

The workspace source is still cloned to a pod-disk `emptyDir`
(`/var/pangea/workspaces`) — the **last** pod-disk dependence the
★★ MAGMA-OPERATOR-BACKEND directive has not yet eliminated. D moves it into
Postgres (content-addressed, mirroring `pangea_meta.artifacts`
`rendered_config/plan/bundle`), so a restart loses nothing and the source can't
wedge. Dependency-ordered plan:

- **D1** `backend/artifacts.rs` + `artifact_store.rs` — `ArtifactStore` trait
  (`PostgresArtifactStore` over the existing `Arc<PgPool>` + `InMemoryArtifactStore`
  mock), `pangea_meta.artifacts (schema, template, kind, content_hash, data,
  git_revision)`, `kind ∈ {source, rendered_config, plan, bundle}`, BLAKE3
  content-hash reused from `magma-bundle`.
- **D2** store the source content-addressed (`ruby/gem_cache.rs`); route plan +
  bundle through the store (`executor/magma.rs`, `magma_bundle.rs`).
- **D3** atomic apply: state + bundle + revision in ONE `pool.begin()` txn
  (`backend/mod.rs`) — "state advanced without its receipt" becomes unrepresentable.
- **D4** demote the `emptyDir` to provider-plugin-exec scratch; delete the
  `main.tf.json` restart guards (their precondition — a wiped clone — is gone);
  shrink the chart `workspaces` volume to `Memory` (`helmworks/charts/pangea-operator/values.yaml`).
- **D5** theory: `MAGMA-OPERATOR-BACKEND.md` (source = `kind=source`, last disk
  dependence removed), `PANGEA-WORKSPACE-RECONCILIATION.md` (the per-tick HEAD
  Observe beat + `Ready⟺at-HEAD`), `MAGMA.md` (git-HEAD drift cause).

D3's single-transaction atomicity is **truly-unrepresentable** (a half-applied
reconcile cannot commit); A+B+C are parse-time-rejected (above).
