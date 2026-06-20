# Terragrunt Parity — the destination

**Goal:** pangea + pangea-operator express, across all four layers (CRD/Helm →
config cascade → Pangea Ruby functions/architectures → magma executor),
everything a real Terragrunt workload needs — with *easy inherit and data
semantics* — so an akeylesslabs-style fleet (single-root inheritance + per-unit
isolated state + a cross-unit dependency DAG) is fully authorable as pangea CRDs
and reconciled by the operator, never Terragrunt.

This doc leads with the destination (Operating Principle #0), grounded in two
code surveys: the **real** Terragrunt feature usage in `akeylesslabs/akeyless-
environments` (488 live units) and the **current** pangea expressive surface.

---

## What the real target actually is (akeylesslabs, 488 units)

Parity is **dominated by six features**; everything else is unused there:

1. **Single-root config inheritance** — one `root.hcl` deep-merged into every
   unit; `find_in_parent_folders` discovery; a path-based `env/region/tenant/
   cloud.hcl` hierarchy. (488/488)
2. **`generate "backend"` → per-unit S3 backend with a path-derived state key**
   (`<tenant>/<cloud>/saas/<region>/<service>/terraform.tfstate`). This IS the
   per-unit state isolation / collision-avoidance. (488/488, centralized in root)
3. **Local module resolution** into the repo's own `src/` tree (486/488; remote
   `git::`/registry sources essentially absent — 2 `tfr://`).
4. **`dependency` blocks: cross-unit OUTPUT wiring** — `dependency.<x>.outputs.*`
   flowing into `inputs`. (360 files / 662 blocks — the heaviest data feature)
5. **`mock_outputs`** so `run-all plan` works before deps exist. (202)
6. **`run-all` over the dependency DAG** — topological apply/plan across units;
   `dependencies { paths }` for ordering-only edges. (86 ordering blocks)

**Explicitly NOT needed (zero usage):** `remote_state` block (they use
`generate "backend"`), `before/after/error_hook`, `sops_decrypt_file`, `get_env`,
`run_cmd`, `merge_strategy`, `skip`, `terragrunt.stack.hcl`/stacks, leaf-level
`generate`, multiple/nested includes, remote git module sources.

---

## What pangea already has (and where)

### Ruby layer — parity-or-better for the hard parts
- **Config inheritance**: `Pangea::WorkspaceConfig.load(__dir__)` +
  `GitBoundaryDiscovery` parent-walk (stops at `.git`) + `DeepNamespaceMerger`/
  `OverrideTagMerger` deep-merge. (`pangea-architectures/lib/pangea/workspace_config.rb`)
  → the `root.hcl` + `find_in_parent_folders` analogue. **EXISTS.**
- **Cross-unit dependency-outputs**: `Pangea::RemoteState.output(template:,
  output:, state_key:)` reads an upstream `.tfstate` from S3 at synthesis time
  and inlines the value. (`pangea-core/lib/pangea/remote_state.rb`) → the
  `dependency.x.outputs` analogue. **EXISTS.**
- **Secrets**: `Pangea::Secrets.resolve` (ENV → sops-nix → `sops` CLI). Superset
  of `sops_decrypt_file`. **EXISTS.**
- **Verbs**: ~98 `Pangea::Architectures::*` compositions + ~5,000
  `Pangea::Resources::*` typed functions vs Terragrunt's ~30 built-ins.
  **EXISTS (superset).**
- Gaps at the Ruby layer (minor, since unused in the target): first-class
  `locals` (ABSENT — raw Ruby locals), typed `get_env` (PARTIAL — raw
  `ENV.fetch`), in-template `generate` blocks (PARTIAL — `provider :x` + typed
  emission).

### Operator/CRD layer — the real gaps
- **Per-unit isolated state**: `PangeaNamespace` (per-workspace Postgres schema /
  S3 prefix, magma backend). → the `generate "backend"` path-keyed isolation.
  **EXISTS structurally** (must-match #2).
- **`InfrastructureFlow` already implements the DAG + output-passing**:
  `spec.steps[]` with `dependsOn[]`, `{{ steps.<name>.outputs.<key> }}` resolved
  by `executor/variable_resolver.rs` (type-preserving), a real adjacency DAG +
  `parallelism` + reverse-dependency destroy in `flow_scheduler.rs`. → must-match
  #4 and #6, **already working** — but only *inside a Flow*, not across
  standalone templates.

---

## The three gaps to close (all operator-layer)

### Gap 1 — General config inheritance (not just policy)
**Today:** the cascade carries five *policy* fields only
(`driftReaction`/`destroyProtection`/`settlingPolicy`/`approvalRouting`/
`reactive`), innermost-wins replace + a `driftReaction` safety-precedence;
and the documented 4-level (gem→workspace→template→resource) resolver is
**unwired at reconcile** (`policy_cascade::resolve` is test-only; live path is
workspace→template). `variables`/`source`/`providerCredentials` do **not**
inherit.
**Destination:** a typed **deep-merge config cascade** —
`PangeaNamespace`/`WorkspaceCatalog` carry default `variables`, `source` defaults,
`providers`, `tags`, `complianceProfiles`; a template deep-merges them
(innermost-wins per key, lists configurable), giving the `root.hcl`-into-every-
unit behavior. Re-use the existing `CascadePolicy`/merge machinery; extend the
*scope* from policy to config. Wire the gem level (or formally retire it).

### Gap 2 — First-class cross-template dependency + outputs
**Today:** `spec.variableRefs` (`VariableRef{templateRef, outputKey}`) is a
**dead field, zero consumers**; the only working resolver is Flow-internal.
**Destination:** promote it — a standalone `InfrastructureTemplate` declares a
typed `dependency` on another template's `status.outputs`, resolved into its
`variables` by the **same `variable_resolver` the Flow already uses**, with a
`mockOutputs` fallback (must-match #5) so a plan can run before the upstream
applies. This is generalization of a proven mechanism, not net-new.

### Gap 3 — Cross-template run-all DAG
**Today:** standalone templates reconcile **independently** (no cross-template
graph); the real `dependsOn` DAG + parallelism + reverse-destroy lives only in
`flow_scheduler.rs`.
**Destination:** the operator reconciles templates in **cross-template
dependency order** derived from Gap-2's `dependency` edges — a `run-all`-
equivalent. Compose with the S-path scheduler: the `ReconcileQueue` already
ranks + budget-admits + anti-starves; add the **dependency-eligibility gate**
(`deps_satisfied`, today a test-only field) fed by the cross-template DAG, so a
unit becomes schedulable only once its upstreams are `Ready`. Reverse order on
destroy. This is `flow_scheduler.rs`'s DAG lifted to the workspace/template
layer, riding the scheduler we just built.

---

## Path (each gap is independently shippable)

- **P1 — Config cascade** (Gap 1): extend the cascade types + resolver from
  policy to config; CRD fields on `PangeaNamespace`/`WorkspaceCatalog`; deep-
  merge into the template's effective `variables`/`source`/providers at compile
  time. Pure-ish, unit-testable.
- **P2 — Template `dependency` + outputs** (Gap 2): un-dead `variableRefs` →
  typed `dependsOn`/`dependency` with `mockOutputs`; reuse `variable_resolver`;
  a template gates on its upstreams' `status.outputs`.
- **P3 — run-all DAG** (Gap 3): cross-template DAG from P2's edges feeding the
  `ReconcileQueue` `deps_satisfied` gate + reverse-destroy ordering. Lifts
  `flow_scheduler.rs`.

P1 is the most foundational (the inheritance the operator literally requested);
P2 unlocks P3. The Ruby layer (`WorkspaceConfig`, `RemoteState`, the architecture
vocabulary) and `PangeaNamespace` (state isolation) are already at parity and
need no new work — the destination is exposing that uniformly through the CRD
authoring surface + the reconcile DAG.

---

## Tier-honesty

- Must-match #1 (inheritance) and #4/#6 (dependency DAG) are **PARTIAL** today —
  the mechanisms exist (Ruby `WorkspaceConfig`/`RemoteState`; the Flow DAG) but
  are **not exposed uniformly** at the standalone-template CRD layer. This doc's
  three gaps close exactly that exposure.
- #2 (per-unit isolated state) and #3 (local modules) and the verb vocabulary
  are **at parity** already.
- The unused Terragrunt surface (hooks/SOPS-in-TG/stacks/remote-modules/get_env)
  is explicitly **out of scope** — matching it would be theoretical completeness,
  not parity with the real workload.
