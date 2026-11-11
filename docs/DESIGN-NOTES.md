# Operator design notes

Detail moved out of CLAUDE.md. The rules stay in CLAUDE.md.

## Two Postgres credential routes — probe the one that executes

The operator reaches Postgres by **two independent routes**, and they
resolve credentials from completely different places. Conflating them
produced a permanent false negative on `PangeaNamespace.status.backendReady`
(fixed 2026-07-28; `controller/namespace_controller.rs`).

| Route | Credentials from | Who uses it |
|---|---|---|
| **Operator pool** | process env `PGHOST`/`PGPORT`/`PGUSER`/`PGDATABASE`/`PGPASSWORD`, wired at startup in `main.rs` (`PGPASSWORD` is the gate) | **magma** — every plan, apply, state row and `pangea_meta.artifacts` row, via `ControllerState`'s `state_backend` / `artifact_store` / `state_lock` |
| **secretRef** | a Kubernetes Secret named by `PangeaNamespace.spec.backend.pg.secretRef` | the legacy **tofu** executor, which renders those coordinates into `backend.tf.json` |

**On the magma path the CR's `secretRef` is never used to connect.** So a
readiness probe that resolves `secretRef` is asking about a connection
nothing exercises. `verify_postgres_backend_with` therefore picks its
route by comparing `PgCoordinates` (host/port/database) — the operator's
pool when it addresses the same database the CR declares, `secretRef`
otherwise — and liveness-probes whichever one it picked.

**Why the `secretRef` fallback namespace cannot be repaired instead.**
`PangeaNamespace` is cluster-scoped, so a `secretRef` with no `namespace`
has no defensible default. `resolve_secret_namespace` falls back
`secretRef.namespace` → `POD_NAMESPACE` → `"pangea-system"`, and
`POD_NAMESPACE` is not in the deployment env, so every lookup landed in
`pangea-system` — which holds no Secrets at all. Setting `POD_NAMESPACE`
would *not* have fixed it: on camelot-eks the two CRs' Secrets live in
two different namespaces (`pangea-database-superuser` in `camelot`,
`pangea-db-creds` in `pleme-io-opensource`), so any single fallback is
wrong for at least one of them. Following the executor is the only fix
that works for both.

**A readiness probe must be able to say both things.** Never make one
unconditionally true to clear a false negative — that trades it for a
false positive, which is strictly worse for a health signal. `backendReady`
feeds `fleet_status_controller::aggregate_pangea_namespaces`, so a probe
stuck on one answer silently mis-reports fleet-wide backend health. Both
directions are pinned in `namespace_controller::backend_probe_tests` via
the `NamespaceBackendEnv` seam, including the case where a healthy but
unrelated operator pool must NOT be borrowed as proof.

## The `plan` / `bundle` artifact rows are rewritten every cycle — on purpose

`MagmaExecutor::plan` calls `put_plan` + `put_bundle` unconditionally,
before the `has_changes` branch. On a large workspace that is multiple MB
of TOAST + WAL per cycle (measured on camelot-eks 2026-07-28:
`pleme-io-opensource`'s `plan` row is 3,077,001 B and its `bundle`
3,064,303 B, against a table holding 2.3 MB of real data), and it looks
like obvious dead work. It is not safely skippable, and the reasons are
non-obvious enough to be worth stating (investigated + rejected
2026-07-28):

- **A `content_hash` skip can never fire.** `magma_types::Plan.created_at`
  and `magma_bundle::Bundle.built_at` (plus six more nested timestamps)
  are serialized fields, so the bytes — and their BLAKE3 — differ every
  cycle over an unchanged world. Such a gate would look like it worked
  while doing nothing. Use `plan.id` (`PlanId`, `Copy + Eq`, excludes
  `created_at`) or `Bundle.bundle_id` if you ever need a semantic key.
- **`put_plan` also re-stamps `source_revision`,** which
  `plan_reuse_is_current` (`executor/magma.rs:209`) compares against the
  `rendered_config` row's revision. Freeze it and a source change that
  doesn't alter the plan makes the gate permanently false: cache-miss →
  regenerate → still frozen → the hard error at `executor/magma.rs:1637`.
  A wedge, on the largest workspace, plus a doubled plan cost per cycle
  on the way there (plan measured at 651s on that workspace).
- **`put_bundle` is also the lifecycle-FSM reset.** `plan()` writes a
  fresh `LifecycleState` (Idle→Planning, `executor/magma.rs:1510`);
  `apply()` inherits `prev_bundle.lifecycle` (`:1750`) and appends to a
  `history` vec that is never truncated. Skip the write and that history
  grows without bound inside the very blob the skip was meant to stop
  rewriting — and `status.lastCycle.lifecyclePhase` reports a stale phase
  during Planning.

`updated_at` on that table, by contrast, is genuinely write-only — no
SELECT anywhere reads it — so freshness is not what makes the rewrite
load-bearing. The revision stamp and the FSM reset are.

Two documented soundness arguments name the unconditional re-persist
explicitly (`controller/template_controller.rs:2287` and `:6013`); one of
them closed a live cross-template `pendingPlanHash` collision. Any future
attempt must rewrite both. The clean version of this optimization is to
split the small lifecycle handoff out of the large compliance bundle
first, so the bundle write can be `bundle_id`-gated without touching FSM
semantics — a design change, not a write-skip.

## Stalled-pass recovery

A stalled pass is released, not waited on. kube-runtime never runs two
reconciles of one template at once, so the watchdog's re-dispatch annotation
(`template/pass_driver.rs`) waits behind whatever reconcile still holds it. Every
template reconcile runs inside `template/pass_registry.rs`: the re-dispatch first
aborts the in-process pass (dropping its budget permit, heartbeat task and state
lock) unless its own reconcile signed within the heartbeat deadline (`noProgress`
preempts regardless), a pass whose beat lands again withdraws a `heartbeatLost`
stall, and the first reconcile of a new incarnation adopts the pass its
predecessor died in (Pending, own incarnation) instead of leaving it for the
`orphanedPhase` verdict. All three are on by default:
`reactive.preempt_stalled_pass` (`PANGEA_PREEMPT_STALLED_PASS`),
`reactive.resume_stalled_on_heartbeat` (`PANGEA_RESUME_STALLED_ON_HEARTBEAT`) and
`reactive.adopt_orphaned_pass` (`PANGEA_ADOPT_ORPHANED_PASS`).
Receipt: ryn 2026-10-27, `pleme-io-github-repos` read Stalled heartbeatLost for
13 minutes while its 11:55 drift plan was still beating; three nudges queued
behind it until the pod restarted.

## Stale inputs

A template whose `spec.variables` another job writes (a CronJob that lists repos,
a sync that renders hosts) cannot tell a quiet world from a dead writer: the
operator reconciles the last variables it was given and reads Ready either way.
A writer that stamps `pangea.pleme.io/variables-updated-at` (RFC 3339) on each
write gets an `InputsStale` condition (`template/inputs.rs`): `False`
VariablesFresh inside the window, `True` VariablesNotUpdated past it, `Unknown`
UnreadableTimestamp when the stamp does not parse. A template without the
annotation gets no condition. The condition is projected like every other one
(`pangea_template_condition`), so an alert reads it with no new series. The
window is `inputs.stale_after_secs` (`PANGEA_INPUTS_STALE_AFTER_SECONDS`;
default 7200); `0` is off and removes the condition. Suspended templates are not
judged.

## `status.planSummary` receipt

**Receipt, 2026-09-18 (the house router's repeater template).** Status read:
`planSummary: "+76 ~0 -0"`, `autoApprove: false`, `lastPlannedAt 17:40:32`,
`lastAppliedAt 17:42:26`. The obvious reading — "76 creates are pending
against a live router" — is the exact opposite of what happened. Ground truth
on the device: **no file under `/etc/config` modified today** (newest is
`Sep 17 11:36`), `uci changes` empty, uptime 1 day 8:50, hostname and
`network.lan.ipaddr` unchanged. The router was never written to.

What actually ran: plan at 17:40 against empty state → `+76`; the pre-apply
`run_import_prepass` (`:3699`) adopted all 76 via `importHints`; because
`imported_addresses` was non-empty the stale plan file was **discarded**
(`:3705` sets `plan_file = None`) and re-planned against the now-populated
state; the re-plan was empty, so the apply wrote nothing. `driftTotal: 0`
on every drift check since. **Import-first worked exactly as designed — only
the reporting lied.**

## Status-write diff-gate example

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
