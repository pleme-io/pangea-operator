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

