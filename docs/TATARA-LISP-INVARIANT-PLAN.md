# pangea-operator: tatara-lisp Authoring Vocabulary + Unrepresentability Hardening

> **Destination-first** (Operating Principle #0), per the org's own
> `theory/UNREPRESENTABILITY.md` bright line: a `Result::Err` is
> *mitigation*, a compile error (or an absent code path) is
> *unrepresentability* — never conflate them. This document states the
> pinnacle unhedged, then names — plainly, without rounding up — how much
> of it is a tatara-lisp authoring project and how much is a pure Rust
> type-system project that has nothing to do with lisp at all.
>
> Grounded in an 8-agent recon+design+adversarial-verify pass
> (2026-07-16): 4 recon streams (CRD surface inventory, badness catalog,
> reusable primitives, executor/controller state-machine risk), 3 design
> angles (authoring-first, invariant-first, phased-adoption), 1
> adversarial verification pass that spot-checked 14 load-bearing claims
> directly against the current source tree (`git log`-cross-checked) and
> found **zero fabricated bugs**, three real overclaims (corrected below),
> and one clear winning thesis. Every finding below carries the file:line
> it was verified against; every tier claim uses the org's own tier
> vocabulary (`truly-unrepresentable` / `parse-time-rejected` /
> `only-mitigated` / `unmitigated`, with named `C1–C6` ceilings where a
> compile-time proof is structurally impossible).

---

## 1. Destination

**pangea-operator, fully realized, is two separate, independently-valuable
destinations that happen to share one repo — never one project, never
substitutable for each other.**

**Destination A — the authoring vocabulary.** Every one of the 15 CRD spec
types (`InfrastructureTemplate`, `ArchitectureGem`, `WorkspaceCatalog`,
`ComplianceSchedule`, `ImagePipeline`, `PackerBuild`, `InfrastructureFlow`,
`PangeaNamespace`, `AmiTest`, `SynthesizerFormat`, `ComplianceBinding`,
`ReconciliationLoop`, `OperatorPolicy`, `PangeaDashboard`, and the
deliberately-empty `PangeaFleetStatus`) is authored as a `(def...)`
tatara-lisp form alongside its existing YAML/CRD surface. Every
closed-vocabulary field (`executor`, `actions`, `risk_levels`,
`onError`, `sslMode`, `ApprovalMode.mode`, …) is a bare Lisp symbol
parsed directly against its real Rust enum — no intermediate `String`
layer, so the shadow-enum/wire-string divergence class cannot exist for
CRs minted this way. A compile-time `AuthoringEnvironment` seam gives the
one thing raw YAML + CRD OpenAPI schema structurally cannot: cross-CRD
referential integrity (`requiredGems`, `dependsOn`, `:policies` refs all
resolve against real declared siblings, checked before the form compiles,
not discovered at reconcile time or never at all).

**Destination B — the executor/controller hardened to its honest best
tier.** Every one of the ~19 real findings in the badness catalog (§3) is
closed to the tier it can actually earn: `Phase::advance` is the *sole*
path a phase transition can take (destination: a phantom-typestate
`Galho<P>`-style FSM where an illegal edge is `E0599`, not a runtime
`Err`); every mutating call (`destroy`, `apply`, `import`) requires a
private-field-gated clearance/authorization value constructible only
through the one checked evaluation path, so "an apply/destroy happened
with no policy floor consulted" has no code path; the executor-migration
FSM (`executor_migration.rs`) gates every magma↔tofu swap through its
already-built shadow-parity + divergence-budget + `lifeline()` machinery,
so the `rio-ssh` access-critical workspace cannot silently swap executors
mid-flight; the RBAC/credential boundary is scoped per-tenant instead of
falling through to the operator's own base IAM identity for any template
that omits `providerCredentials`.

**The two destinations do not fund each other.** Per the tier-honest scope
split (§2), authoring a cleaner `(definfratemplate ...)` form does not —
cannot — make `PolicyDecision::AutoApply` compute a plan-approval hash,
and hardening `Phase::advance` into a compile-time FSM does not produce a
single line of reusable Lisp. Naming this destination unhedged means
naming both halves fully, and naming their independence just as plainly —
rounding them into one undifferentiated "generate a vocabulary and make
it all unrepresentable" goal is itself the path-of-least-resistance
mistake this document exists to avoid.

---

## 2. Tier-honest scope split — read this before scheduling any work

**The single most important finding from this recon, stated without
hedging: none of the top 10 most severe badness-catalog findings need a
single line of tatara-lisp to close.** Every one of them is closed by pure
Rust — enum variants, `From`/`TryFrom` impls, private-field newtypes, and
signature narrowing on functions that already exist. This is not a
close call; the invariant-first design angle's own verdict (confirmed by
the adversarial pass, which found it the winning thesis "outright, and
it isn't close") is: *"generate a lisp authoring vocabulary" and "make
the badness catalog invariant" are two separable projects that happen to
share a repo, not one project.*

The authoring-first angle's own honest triage, run against all 21
findings from both recon streams (16 badness-catalog + 5 state-machine
risk areas):

| Reachability | Count | What it actually closes |
|---|---|---|
| **Fully closed by authoring alone** | 2 of 21 | The `AuthoringEnvironment` cross-reference cases — `requiredGems`/`dependsOn`/`:policies` refs, pure data-integrity gaps with no runtime counterpart needed |
| **Partially closed (the "silently implicit" half only)** | 3 of 21 | Shadow-enum/wire-string fields (parse-don't-validate at the reader — but see the correction below, this gap is narrower than first described); `default_decision`'s dangerous default (mandatory-keyword-with-no-default closes only the *absent-field* case, not the *deliberately-chosen-AutoApply-then-no-hash-check* case); the executor-typo-silent-fallthrough source half (raw `kubectl patch` still bypasses it) |
| **Unreachable — pure executor/controller/RBAC Rust bugs** | 16 of 21 | Everything else: the Phase FSM warn-only guard, the executor-migration dead FSM, the AutoApply hash bypass's deliberate-choice half, the RBAC/credential boundary, `mockOptput` applied for real, the mutex-poison hazard, the non-exhaustive `magma_types::Action` match, and more — no CRD field feeds any of these; the bug lives entirely in the reconciler's internal control flow |

**Corrections the adversarial pass forced onto the authoring-first
angle's own showcase examples** (do not schedule work against the
uncorrected claims):

1. **The `defpolicyrule` worked example's premise is stale.** The
   authoring-first proposal frames `actions`/`risk_levels` as backed by
   an *"unused"* `DriftAction`/`RiskLevel` shadow enum. `git log` shows
   commit `5397f3f` ("stringly-typed action/riskLevels/approval.mode
   silently swallow typos + real vocab drift") already wired
   `parse_wire()` into `actions_match`/`risk_levels_match`, which **warn**
   on every divergence today. The enums are not unused, and the `String`
   wire type is a *documented deliberate choice* ("a CRD instance with an
   out-of-vocabulary value still decodes instead of failing the whole
   watch stream"), not an oversight. The real remaining gap is narrower
   than first presented: upgrading a runtime `warn!` to a
   parse-time-rejected error, not closing an unaddressed hole.
2. **The `ApprovalRouting` cross-CRD reuse count is wrong in two
   independent design docs.** Both the authoring-first and
   phased-adoption angles claim `ApprovalRouting` is "reused verbatim by
   `WorkspaceCatalog`, `InfrastructureTemplate`, and `ReconciliationLoop`"
   (4 CRDs total). Direct grep across `src/crd/*.rs` shows
   `ApprovalRouting` (and `DriftReaction`) appear **only** in
   `architecture_gem.rs` and `workspace_catalog.rs` — **2 CRDs, not 4.**
   The claim genuinely *is* true for the other two types bundled in the
   same sentence — `ReactivePolicy`/`SettlingPolicy` really are shared
   across all 4 files. Both docs flattened this into one wrong number;
   it's a shared upstream-recon error, not two independent mistakes.
3. **`#[derive(ClosedSet)]`, as cited in the phased-adoption plan's M2,
   does not correspond to any shipped macro-farm derive** in
   `tatara-rust-ast`'s 15-name catalog (getter, builder, setter,
   isvariant, asmut, replace, take, implfrom, asref, deref, inner,
   allvariants, variantcount, variantnames, invalidating-setter). The
   real, shipped primitive that fixes the cited `Phase::ALL` bug is
   **`#[derive(AllVariants)]`** (`pleme-allvariants-derive`), which
   produces exactly the `pub const ALL: &'static [Self]` shape needed.
   *(A `ClosedSet` derive is separately claimed, by the reuse-map recon,
   to live in `tatara-lisp-derive` — not the same crate the adversarial
   pass checked. That claim was not independently re-verified in this
   pass; treat it as unconfirmed until checked directly against
   `tatara-lisp-derive`'s own source before relying on it.)*
4. **The "31 call sites" figure for the Phase FSM guard is off** — the
   real count is **33** (`update_phase(`/`update_phase_with_error(` call
   sites in `template_controller.rs`, excluding fn definitions and test
   modules). Cosmetic; both design docs state the same wrong number,
   again a shared upstream miscount. Doesn't change either fix's shape.

**What this means for scheduling:** the VOCAB lane (§5) is real, valuable,
future work — not now-work. It should not run as a coequal parallel lane
to the SAFETY lane. It should start only once the SAFETY lane's highest-
severity findings are closed, and its own internal ordering must harden
`InfrastructureTemplate`'s field *types* before authoring Lisp forms
against them (authoring against a `String` field that's about to become a
real enum means re-authoring every spec file later).

---

## 3. The badness catalog — ranked, corrected, file:line-grounded

Context: this session already shipped two fixes before this recon ran —
`policy.rs`'s `PROTECTED_RESOURCE_TYPES` floor (commit `2bf5377`, "floor
unconfigured destroys on protected resource types to `RequireApproval`")
and the stringly-typed action/riskLevels warn-on-divergence fix (commit
`5397f3f`). The findings below are what the floor and the divergence fix
**did not** close — several are new gaps *in the mechanism this session
just shipped*.

Every entry below was either explicitly spot-checked by the adversarial
pass (marked **✓verified**) or drawn from direct reads of the same files
the adversarial pass verified adjacent claims against (marked
**read-grounded**, not individually fact-checked line-by-line but with no
contradicting evidence found across 14 explicit spot-checks and a
6,832-line direct read of `template_controller.rs`).

### Critical severity

**#1 — Protected-resource floor has an escape hatch: `PlanAction::Other(s)` bypasses `is_destructive_action`** ✓verified
`src/executor/policy.rs:89` + `src/executor/cycle_artifact.rs:328,202`.
`is_destructive_action()` recognizes only the four exact lowercase
strings `"create"/"update"/"delete"/"replace"` and returns `false`
(non-destructive) for anything else. But `CycleArtifact::drift_details()`
— the **magma execution path, the live default executor** — builds
`DriftDetail.action` from `magma_types::PlanAction`, which has a real,
documented `Other(String)` catch-all variant. Any magma-classified action
outside the four canonical strings on a protected type
(`cloudflare_zone`, `aws_eks_cluster`, `aws_vpc`, …) silently defeats the
floor this session just shipped, through the one gap the fix didn't
close. **Current tier: only-mitigated, and the mitigation's own escape
hatch is unmitigated. Target: truly-unrepresentable** — delete the
`String` round-trip, make `PlanAction → DriftAction` a total exhaustive
`From` impl with an `Unknown(String)` arm classified fail-safe (`true`,
not `false`). *Adversarial-pass note: the code's own doc comment
(`policy.rs:81-88`) defends the current fail-open behavior as "the
conservative choice" — that reasoning is backwards for this call site
specifically, and the fix must correct the comment too, or a future
maintainer re-derives the same bug from the same wrong justification.*

**#2 — CR-deletion destroy bypasses the entire policy/floor engine** ✓verified
`src/controller/template_controller.rs:349-380` (deletion path) +
`4822-4914` (`handle_destroying`). On `deletionTimestamp`, the operator
calls `runner.destroy(&workspace, true)` **unconditionally** — no
`spec.policies`, no `PROTECTED_RESOURCE_TYPES`, no per-resource risk
classification. The only gate anywhere on this path is
`spec.destroy_protection` (opt-in bool, default `false`). A `kubectl
delete infrastructuretemplate`, or a GitOps prune firing because a
template's YAML was accidentally removed from git, destroys every
managed resource on an unconfigured template with no floor. **Current
tier: only-mitigated (single opt-in bool). Target: truly-unrepresentable
within-crate** — a `DestroyClearance(())` private-field newtype,
sole-constructible by a function that runs the same floor check
`handle_applying` already runs four times, required as a parameter of
`IacExecutor::destroy`. *Caveat stated honestly: this guarantee holds
only within this crate's call graph — if `IacExecutor` becomes
re-implementable from outside the crate, it doesn't travel. Same
crate-boundary caveat as the org's own `Attested<T>::seal()` precedent
(`arch-synthesizer/src/attested.rs`, graded `partially-unrepresentable`
for exactly this reason).*

**#3 — Executor-migration safety FSM fully built, fully tested, zero consumers** ✓verified
`src/executor_migration.rs` (whole file) + `src/crd/infrastructure_template.rs`
(missing `status.executorMigration` field) + `src/controller/mod.rs:317-330,351-375`
(`executor_for`/`executor_for_checked`). `MigrationPhase{Pinned, Shadow,
Held, Cutover, Verified, RolledBack}` + a pure, 12-test-covered `step()`
FSM with shadow-mode parity checking, a divergence budget, and a
`MigrationPolicy::lifeline()` preset built explicitly for "workspaces
that carry the access path to a cluster we can only reach over SSH...
labelled `pangea.pleme.io/lifeline=rio-ssh`" — is **entirely dead code**.
`executor_for_checked` re-resolves `ExecutorBackend::resolve(...)` fresh
on every reconcile with zero comparison to last cycle and zero
consultation of this FSM. No admission webhook exists anywhere in this
repo, so `spec.executor` is fully mutable post-creation with zero
immutability enforcement. **Setting `spec.executor: magma` on the literal
`rio-ssh` lifeline template — the one workspace this FSM was built
specifically to protect — takes effect on the very next reconcile with no
shadow plan, no parity check, no divergence budget.** **Current tier:
dead code (unmitigated). Target: parse-time-rejected**, escalating to
**truly-unrepresentable within-crate** by wrapping the FSM's output in a
private-field `ClearedExecutor(ExecutorBackend)` newtype constructible
only by the checked resolution function — add the missing status field,
route `executor_for_checked` through `step()`, derive `MigrationPolicy`
from the `pangea.pleme.io/lifeline` label.

**#4 — `PolicyDecision::AutoApply` (the documented default) skips `plan_approval_hash` entirely** ✓verified
`src/controller/template_controller.rs:2486-2547` (the `AutoApply` match
arm) + `4958-5036` (`plan_approval_hash`). The hash mechanism itself is
good — deterministic, state-fingerprinted, closes two real prior
incidents (non-deterministic stdout-ordering hashing; a textually
identical plan against genuinely different state). But it is computed
only inside `PolicyDecision::RequireApproval` and a narrow
`state_continuity_breach` sub-case. **`AutoApply` is the CRD-documented
default when `spec.defaultDecision` is unset** — most templates, out of
the box — and its match arm calls `update_phase(template,
Phase::Applying, state)` directly with zero fingerprint recorded
anywhere. **Current tier: unmitigated for the default path. Target:
truly-unrepresentable (fingerprint-exists), C2-ceilinged (plan-is-safe)**
— an `ApplyAuthorization { HashApproved{plan_hash} |
AutoApplyContinuous{fingerprint} }` enum required to construct
`Phase::Applying`, for **every** policy arm, not just `RequireApproval`.
This does not change AutoApply's product behavior (still applies without
human review — intentional); it makes every apply leave a re-derivable
fingerprint trail. *State plainly: this makes AutoApply auditable, not
safe — "a plan is actually safe" is a property of the real diff against
real state, unprovable at compile time (C2: external-world observation).
Do not round the fingerprint fix up to "AutoApply is now safe."*

### High / moderate-high severity

**#5 — `mockOutput` values compiled once can flow into a real apply** ✓verified
`src/controller/template_controller.rs:1128-1145` (`handle_compiling`) +
`src/controller/template_dependency.rs` (`DependencyResolution::fully_satisfied`,
confirmed unused outside its own test module). `handle_compiling` gates
only on `unresolved_templates.is_empty()`, then calls `.all_variables()`,
which merges `mocked` into `resolved` — despite the type's own doc
explicitly saying `mocked` is valid for PLAN only. If an upstream
template isn't `Ready` when compile runs, a placeholder value (a fake VPC
ID, a fake IAM role ARN) can get baked into the rendered config and then
genuinely applied. **Current tier: unmitigated (predicate exists,
unused). Target: truly-unrepresentable within-crate** — a sum-over-product
`RenderedConfig::{Real(inner) | ContainsMocks{inner, mocked_templates}}`
with `RenderedConfigInner`'s fields non-`pub` on the enum, so no call
site can hand a `ContainsMocks` value to `apply()` without an explicit
match arm that visibly discards the safety tag.

**#6 — `risk_level()`'s hand-rolled substring heuristic silently diverges from `PROTECTED_RESOURCE_TYPES`** ✓verified
`src/executor/plan.rs:360-378` vs `src/executor/policy.rs:63-73`. Two
independently-maintained catastrophic-type classifiers: `risk_level()`
substring-matches `dns_record|zone|database|rds|vpc|tunnel`;
`PROTECTED_RESOURCE_TYPES` is a 9-entry literal list. Verified
byte-for-byte: `"aws_db_instance".contains("database")` is `false` (it's
`db_instance`), and none of the other five substrings match either — a
`riskLevels: ["high"]` policy rule silently never fires on
`aws_db_instance`, despite it being in the protected list. **Current
tier: only-mitigated (2 independently-wrong lists). Target:
truly-unrepresentable for "two lists can diverge"** — delete the second
classifier; derive `risk_level` directly from `PROTECTED_RESOURCE_TYPES`.
Add `every_protected_type_classifies_high_on_delete` as a CI forcing
function (not a compile-time proof — Rust can't statically prove a
function's output against a `static` array; C1-ceilinged on
*correctness*, truly-unrepresentable on *divergence*). **This is the
single cleanest, lowest-risk, highest-confidence fix in the entire
catalog** — pure deletion, no new call site, no new type.

**#7 — `state_lock` silently no-ops without Postgres — double-writer race on real cloud RPCs** ✓verified
`src/controller/mod.rs:151,283,674` (`state_lock: Option<Arc<StateLock>>`)
+ `template_controller.rs:2123-2160` (`acquire_mutation_lock`). Returns
`LockDispatch::Proceed(None)` — "nothing to hold" — when `state_lock` is
`None`. Tofu-only/DB-less is a real, documented, config-selectable
deployment mode (★★ MAGMA-NATIVE EXECUTION keeps tofu supported), and in
that mode this guard against two operator pods both issuing real
create/update/destroy RPCs against the same template is not degraded —
**it is absent**. Matches the failure class of the camelot-eks incidents
noted in operator memory. **Current tier: only-mitigated, unmitigated in
one legitimate config. Target: parse-time-rejected for the silence
gap**, **C4-ceilinged for DB-less concurrency itself** (irreducibly
shared cloud state — no Rust type makes two unrelated OS processes
coordinate without a shared backend). Fix: `MutationLockMode::{Postgres(Arc<StateLock>)
| Unguarded{acknowledged_risk: bool}}` — fails closed
(`Err(UnguardedMutationsNotAcknowledged)`) unless an operator explicitly
sets a config flag acknowledging the risk. *This makes the gap visible
and operator-consented; it does not make DB-less concurrency safe — name
both.*

**#8 — Phase FSM guard is WARN-only; illegal transitions logged, then applied anyway** ✓verified
`src/controller/template/status.rs:53-77` (`update_phase`), `139-200`
(`update_phase_with_error`). `edge_is_legal(phase)` is checked against
the real typed `TRANSITIONS` table in `controller/lifecycle.rs` — which
has excellent CI forcing-functions proving the *table itself* is
well-formed (no wedges, every phase reaches `{Ready, Destroying}`) — but
on failure the code only `tracing::warn!`s and proceeds to patch
`status.phase` anyway. **33 call sites** (corrected from the design
docs' stated 31) in `template_controller.rs` all pass a hardcoded
`Phase::X` literal; `Phase::advance()` (the pure, total, typed lookup) is
called nowhere outside `lifecycle.rs`'s own tests. **Current tier:
below-`Result::Err` — an unenforced log line, not even the honest middle
tier. Target (minimal fix): only-mitigated** — flip `update_phase`'s
internal control flow from warn-and-proceed to `return
Err(IllegalPhaseTransition{from,to})`. Because every one of the 33 call
sites already does `update_phase(...).await?`, this single function-body
change closes all 33 simultaneously with **zero call-site edits**.
**Named destination, out of scope for the minimal fix:** migrate
handlers to compute their target phase via `Phase::advance(current,
trigger)` instead of a hardcoded literal, ultimately a phantom-typestate
`Galho<P>`-style FSM where an illegal edge is `E0599`. *State plainly:
the minimal fix is a runtime check, not a compile error — the org's own
bright line applies directly. This is also, per the adversarial pass,
the single highest-leverage fix in the whole catalog by call-site count,
and the highest blast radius if flipped without an observation window
first (see §6).*

**#9 — `PANGEA_FORBID_TOFU` enforced by caller convention, not structurally, on the import/conflict path** ✓verified
`src/controller/conflict.rs:298,344` (`run_import_prepass`,
`try_tofu_import`, `gather_attrs`) + `template_controller.rs:3954,4166`
(`resolve_conflicts_post_apply`) vs `controller/mod.rs`
(`executor_for_checked*`, the checked resolution path). `handle_applying`
and `handle_destroying` correctly resolve through the checked path
(typed `Error::TofuForbidden`). The import prepass and post-apply
conflict-resolution loop — explicitly documented as "the EXACT mechanism
that destructively replaced the real camelot-eks EKS cluster 2+ times"
— independently call the **unchecked** `state.executor_for(template)`.
Not live-exploitable today only because caller ordering happens to
guarantee the checked call already ran earlier in the same tick — nothing
in the type system enforces that ordering. **Current tier:
only-mitigated (caller-ordering convention). Target: truly-unrepresentable
within-crate** — same shape as #2/#3: a `CheckedExecutor(ExecutorBackend)`
private-field newtype, sole-constructible by `executor_for_checked`,
required as the parameter type of every RPC-issuing function.
*Design-efficiency note carried from the invariant-first design: #2's
`DestroyClearance`, #3's `ClearedExecutor`, and this finding's
`CheckedExecutor` are the same generic shape — a private-field wrapper,
sole-constructed by a checked function, required by every mutating call
site's signature. One small generic `Cleared<T>(T)` (or a macro emitting
the three concretes) is more consistent with the org's own emitter-
substrate discipline than three bespoke hand-written types.*

**#10 — Cross-template destroy ordering fully built, fully tested, never wired** read-grounded
`src/controller/template_dag.rs` (whole file, `apply_order`/`destroy_order`,
lines 92-137). Zero callers outside its own test module. `handle_destroying`
reconciles each `InfrastructureTemplate` independently with no
cross-template awareness — a GitOps prune of a whole environment can
destroy `vpc` while `db` (which depends on it) still references it, with
no ordering guarantee. **Current tier: only-mitigated (safe primitive
exists, unused — effectively unmitigated). Target: parse-time-rejected**
— wire `destroy_order()` into the deletion-finalizer path; a template
with live downstream dependents blocks/defers its own destroy.

**#11 — `policy_cascade.rs`'s `destroy_protection` uses innermost-wins, inconsistent with `drift_reaction`'s safety precedence; module is dead code with false documentation** read-grounded
`src/controller/policy_cascade.rs:94-152`. `resolve_drift_reaction`
correctly implements "strictest always wins regardless of depth."
`destroy_protection` instead uses `innermost_bool` — a
template-/resource-level `false` **silently overrides** a
gem-level (org-wide) `true` (proven by the file's own passing test,
`destroy_protection_innermost_wins`). Separately: the module's doc
comment claims three live consumers; `grep -rn "policy_cascade::resolve"`
outside the file returns zero hits — it's dead code whose documentation
actively misrepresents that. **Current tier: only-mitigated (latent
design bug + stale docs). Target: truly-unrepresentable for the
divergence** — apply the same strictest-wins walk to `destroy_protection`;
fix or remove the false "Used by" comment. Latent, not urgent today
(unwired) — but the bug activates the instant someone wires it in
trusting the doc.

### Moderate / low severity

**#12 — Non-exhaustive `_ => NoOp` misclassifies real destructive-adjacent actions in the post-apply audit record** read-grounded
`src/executor/magma.rs:1252,1262`. Matches `magma_types::Action` (9
variants) with a catch-all that silently folds `Read`/`Forget`/
`CreateThenDelete`/`DeleteThenCreate` into `NoOp` in the
**already-executed** outcome record — right next to a correctly
exhaustive mapping of the same enum a few dozen lines away in the same
file (`to_universal_plan`). Audit-trail integrity, not the live mutation
gate (the pre-apply gate is correct). **Current tier: only-mitigated
(non-exhaustive match, sibling of an exhaustive one in the same file).
Target: truly-unrepresentable** — delete the `_ =>` arm, copy the
already-correct mapping; the compiler enforces exhaustiveness on any
future `Action` variant.

**#13 — The new protected-resource floor is a fixed, hand-maintained deny-list, not a default-safe model** read-grounded
`src/executor/policy.rs:63-73` (`PROTECTED_RESOURCE_TYPES`). Root-cause
pattern tying #1 and #6 together: a 9-entry hardcoded allowlist of
"catastrophic" types. Any provider primitive not yet listed
(`azurerm_kubernetes_cluster`, `google_container_cluster`,
`aws_elasticache_cluster`, `aws_kms_key`, `aws_secretsmanager_secret`, …)
is silently unprotected until someone remembers to add it — the exact
bug class this session just fixed, one layer up. **Current tier:
only-mitigated (a maintained catalog, not a structural default). Target:
model inversion** — default every delete/replace on an unconfigured
template to `RequireApproval` unless the type is on an explicit
*safe-to-auto-destroy* allowlist, rather than defaulting to
auto-apply-unless-denylisted.

**#14 — Explicit, mistyped `spec.executor` choice is silently discarded, no log** read-grounded
`src/executor/backend_select.rs:30-44`. By design, an unrecognized
`spec.executor`/`PANGEA_EXECUTOR` value falls through so a typo doesn't
take a workload down — but `resolve()` never logs the fallthrough. An
operator who sets `spec.executor: "toffu"` meaning to route around a
magma-specific issue gets silently routed elsewhere with zero
observability. **Current tier: only-mitigated (silent fallthrough).
Target: only-mitigated + observable** — `tracing::warn!` on unrecognized-
but-nonempty values before falling through.

**#15 — `ReconciliationLoop` controller silently bypasses the operator's global kill-switch** read-grounded
`src/controller/reconciliation_loop_controller.rs` (wired live,
`main.rs:538-541`) + `src/crd/operator_policy.rs:299-312`
(`ControllerKind`, 12 variants). Every controller is supposed to call
`policy_pipeline::run_for*` first; `fleet_status_controller` and
`operator_policy_controller` bypass this deliberately, with an explicit
comment. `reconciliation_loop_controller` has no `ControllerKind`
variant, no policy-pipeline call, and no comment — it patches its own
status on every tick regardless of `globalSuspend=true`. **Current tier:
only-mitigated (silently-violated convention). Target: parse-time-checked
via the existing gate** — add a `ReconciliationLoop` variant, call
`policy_pipeline::run_for_controller`.

**#16 — Central fair-priority dispatch queue has a shared-mutex poison hazard** read-grounded
`src/controller/reconcile_scheduler.rs:105,115,120,127,136`.
`ReconcileQueue` guards its dispatch map with `std::sync::Mutex` and
unwraps via `.expect(...)` on every operation. Any panic while holding
the lock poisons it permanently — every subsequent call from every
reconcile panics, wedging fleet-wide scheduling from one panic anywhere
in the dispatch path. **Current tier: only-mitigated (a lock, not the
removed shared cell). Target: eliminate-the-shared-cell** — swap to a
channel/actor-owned queue, or at minimum `unwrap_or_else(|e|
e.into_inner())` since no guarded operation can leave the map genuinely
broken.

**#17 — `.to_str().unwrap()` panics on a non-UTF8 workspace path** read-grounded
`src/executor/tofu.rs:171,213,233`. Not reachable today (namespace/name
are DNS-1123-constrained, ASCII-safe) — latent, not live. **Current
tier: unguarded (would need a non-ASCII path today). Target:
parse-time-rejected** — `Refined<PathBuf, Utf8Bounds>` or `.to_str().ok_or_else(...)`.

**#18 — Destroy-failure diagnostics can be silently empty** read-grounded
`src/controller/template_controller.rs:4894-4899`. Uses only
`raw_stdout` on failure; `conflict.rs`'s `combined_output` (used for the
apply path) deliberately combines stdout+stderr because "OpenTofu writes
most diagnostics to stdout" implies stderr sometimes carries real
diagnostics too. A destroy failure landing only in stderr leaves
`status.lastError` empty. **Current tier: only-mitigated (omission).
Target: reuse `combined_output`** for the destroy path too.

### Confirmed strength (not a badness item — included for completeness)

**`destroyProtection` gate — well-hardened across 4 call sites** ✓verified
`src/controller/template_controller.rs:5152-5169`
(`evaluate_destroy_protection_gate`), invoked from `handle_applying`
(the ordinary path), a fresh recheck when `plan_file` is `None` (the
normal magma case), the stale-plan self-heal retry, and
`conflict.rs`'s post-import re-apply loop — the same predicate, never
forked, and directly traceable to two real incidents this fix already
closed. **One residual, low-severity, explicitly-named fail-open edge:**
`fresh_action_set_before_bare_apply` returns an empty `Vec` if *every*
tier of action-set recomputation fails, which the gate treats identically
to "no destructive action" — a deliberate design choice ("the recheck
itself couldn't run must never become a way to wedge every apply") that
leaves a genuinely-destructive apply coinciding with a total
recheck-machinery failure uncaught by this specific gate. Low severity;
name it, don't let it block anything else.

---

## 4. The reuse map — what already exists, so nothing above gets reinvented

**pangea-operator has zero tatara-lisp/`TataraDomain` dependency today**
(`grep -n "tatara" Cargo.toml Cargo.lock` — zero hits). This is a
**first-time adoption**, the same category of move as vendaval this
session, not an extension of existing wiring.

### 4.1 `#[derive(TataraDomain)]` — exact capabilities

Source: `~/code/github/pleme-io/tatara/tatara-lisp-derive/src/lib.rs` +
`tatara-lisp-derive/README.md` + `tatara/docs/rust-lisp.md`.

- **Registration is 6 lines:**
  ```rust
  #[derive(DeriveTataraDomain, Serialize, Deserialize, Debug, Clone)]
  #[tatara(keyword = "definfratemplate")]
  pub struct InfrastructureTemplateSpec { /* fields */ }

  pub fn register() { tatara_lisp::domain::register::<InfrastructureTemplateSpec>(); }
  ```
  Requires named-field structs (tuple structs, top-level enums rejected).
- **Field coverage is total for pangea-operator's shapes.** First-class
  extractors handle `String`/`Option<String>`/`Vec<String>`/ints/floats/
  `bool`. The **universal `Deserialize` fallthrough** (`sexp_to_json` +
  `serde_json::from_value`) covers everything else pangea-operator needs:
  any enum deriving `Deserialize` → a bare Lisp symbol; any nested struct
  → a kwargs sublist; `Option<T>`/`Vec<T>` for any `T: Deserialize`, not
  just the first-class primitives. `#[serde(default)]` (already used
  extensively across pangea-operator's optional CRD fields) makes a
  keyword optional, falling back to `Default::default()`, for free.
- **Confirmed coexistence**: a struct already deriving `CustomResource +
  Serialize + Deserialize + JsonSchema` — i.e. **every one of
  pangea-operator's 15 CRD spec structs** — adds `DeriveTataraDomain`
  with zero collision (`#[kube]`, `#[serde]`, `#[tatara]` are disjoint
  attribute namespaces). The derive's own README production example is
  literally a `kube::CustomResource` struct carrying both derives side by
  side.
- **Production-scale precedent**: `tatara-process::ProcessSpec` — 8
  fields, all nested structs/enums, `Vec<DependsOn>` of nested-struct-
  with-enum — "the derive handles every field, one line of macro, zero
  hand-rolling." This is a closer structural precedent for
  `InfrastructureTemplateSpec` (2075 lines, deeply nested) than any toy
  example.
- **Named gaps (none needed for pangea-operator today):** tuple structs,
  top-level enums, custom per-field error messages, positional args.

### 4.2 Emitter-substrate macro farm — one real hit, ten speculative non-hits

Source: `~/code/github/pleme-io/tatara-rust-ast/catalogs/pleme-derives.lisp`
(23 published derive specs).

**The one genuine Layer-A hit** (a real hand-kept table already exists,
per the org's own "collapse an existing table, don't add speculative
API" discipline): `infrastructure_template.rs:1443-1461`'s hand-authored
`impl Phase { pub const ALL: [Phase; 11] = [...]; }` — byte-for-byte the
shape `#[derive(AllVariants)]` targets, and it is also **the fix for the
`Phase::ALL` fleet-gauge undercount bug** (missing `CompileBlocked`,
confirmed by the adversarial pass, and already named as a known
deliberately-deferred item in the codebase's own test comments). Same
file also hand-rolls `PolicyDecision::as_str()` — the exact shape of
`pleme-variantstr-derive`.

**Ten sibling Phase-shaped enums exist across the other 14 CRD types**
(`ComplianceSchedulePhase`, `AmiTestPhase`, `SuitePhase`, `FlowPhase`,
`ImagePipelinePhase`, `PangeaDashboardPhase`, `PackerBuildPhase`,
`SynthesizerFormatPhase`, `LoopPhase`, `architecture_gem::Phase`) — **none
of them currently carry an `ALL`/`as_str` table.** Adopting derives on
those today would be speculative API, not table-collapse — correctly
out of scope.

**Not a fit at all today:** getter/setter/builder derives — pangea-operator's
CRD structs have no hand-rolled getters/setters/builders anywhere in
`src/crd/` outside one trait impl. Adopting those now would be
speculative, not table-collapse.

### 4.3 The TYPED-SPEC + INTERPRETER TRIPLET template

Source: `~/code/github/pleme-io/sui/sui-spec/src/fetcher.rs`, chosen
because it has a genuine I/O boundary (unlike `derivation.rs`, which is
pure computation with no `Environment` trait).

The exact shape to mirror for any pangea-operator domain with real I/O
(magma provider RPCs, Postgres state reads, k8s API calls):

1. **Typed Rust border**: `FetcherSpec { name, transport, hash_mode,
   output_kind, phases }` — `#[derive(DeriveTataraDomain)] #[tatara(keyword
   = "deffetcher")]`.
2. **Authored Lisp spec**: `sui-spec/specs/fetchers.lisp` — one
   `(deffetcher ...)` form per case (five cppnix fetcher builtins), each
   a different phase pipeline over the same typed border.
3. **Interpreter behind a mockable `Environment` trait**:
   ```rust
   pub trait FetcherEnvironment {
       fn fetch_bytes(&self, url: &str) -> Result<Vec<u8>, String>;
       fn hash_bytes(&self, bytes: &[u8]) -> String;
       fn write_to_store(&self, name: &str, bytes: &[u8]) -> Result<String, String>;
       fn cache_lookup(&self, _name: &str, _declared_hash: &str) -> Result<Option<String>, String> { Ok(None) }
   }
   pub fn apply<E: FetcherEnvironment>(spec: &FetcherSpec, /* .. */) -> Result<..., SpecError> { ... }
   ```
   Tests implement a `MockEnv`; production wires real HTTP/store clients.

For pangea-operator specifically: `fetcher.rs`'s shape is the template
for any domain with a real I/O boundary; `derivation.rs`'s shape
(pure computation, no trait) is the template for pangea-operator's
already-pure logic (e.g. the `PolicyDecision`/`DriftAction`/`RiskLevel`
classification functions targeted in §3's fixes).

### 4.4 UNREPRESENTABILITY catalog — one real precedent per technique

Every technique the fixes in §3 use has a shipped precedent elsewhere in
the fleet, graded honestly (not rounded up):

| Technique | Precedent | Tier (as shipped, not as aspired) |
|---|---|---|
| Parse-don't-validate (newtype + private field) | `arch-synthesizer/src/foundations.rs` — `CidrBlock`, `AwsAccountId`, `S3BucketName` | partially-unrepresentable (some fields `pub(crate)`, bypassed in-crate) |
| `Refined<T, Bounds>` | `ishou-tokens/src/refined.rs` + `mado/src/font_size.rs` | only-mitigated (runtime clamp; `Default` path skips it) |
| Phantom type-state / category tag | `galho-types/src/state.rs` — `TypedState<S>`, `Plan<S>` | parse-time-rejected at deserialize; only-mitigated at smart-ctor |
| Static typestate FSM | `galho-types/src/phase.rs` + `typestate.rs` — `Galho<P>` | truly-unrepresentable on the opt-in forward arc (`E0599`); only-mitigated on the erased surface |
| Sum-over-product | `galho-types/src/ir.rs` — `ResourceStatus::{Applied\|Pending\|Failed\|Drifted\|Tombstoned}` | partially-unrepresentable |
| Eliminate-the-shared-cell | `galho-storage/src/galho_tree.rs` — per-galho `refs/galhos/<name>` head | only-mitigated (cell relocated, not removed; raw ops still `pub`) |
| Append-only content-addressing | `galho-storage/src/object_store.rs` — CAS `cas_ref`, no `overwrite` | partially-unrepresentable |
| Proof-carrying capabilities | `arch-synthesizer/src/attested.rs` — `Attested<T>::seal()` | partially-unrepresentable (witness doesn't bind to payload) |
| Exhaustiveness (deny-unknown-fields) | `tatara-lisp-derive`'s own `parse_kwargs_strict` | only-mitigated (parse-time `Result`, public-field bypass exists) |
| Typed-emission AST | `repo-forge-render/src/nix_value.rs` + `magma-nix/src/ast.rs` — `NixValue` | partially-unrepresentable |

**A pangea-operator conversion should expect the same honest grading —
mostly `partially-unrepresentable`/`only-mitigated`, a narrow slice of
genuinely `truly-unrepresentable` — not claim a stronger tier than the
reference implementations themselves have earned.**

---

## 5. The phased plan

Two lanes. **SAFETY closes the badness catalog (§3); VOCAB builds the
authoring surface (§4).** Per §2's corrected scope split, these do
**not** run as coequal parallel lanes from day one — VOCAB starts only
once SAFETY's highest-severity phases are closed, correcting the
phased-adoption design angle's original framing (which the adversarial
pass flagged as borrowing credibility from claims — the `ApprovalRouting`
reuse count, the `ClosedSet` derive — that didn't hold up under
verification).

**The governing invariant for every SAFETY phase**: each phase is either
(a) strictly monotonically safer — adds a check that was silently
missing, never removes or weakens one — or (b) purely observational —
telemetry/shadow-mode with zero behavior change. No phase removes an
existing guard before its replacement is proven.

### M0 — the anchor slice (ship this first, today)

**Both design angles independently converged on this pairing, and the
adversarial pass explicitly signed off on it as "what I would sign off
on shipping today":**

1. **#1** — flip `is_destructive_action`'s unparseable-action default
   from `false` (fail-open) to `true` (fail-safe). One function, one
   file, `src/executor/policy.rs` only.
2. **#6** — derive `risk_level()` from `PROTECTED_RESOURCE_TYPES` instead
   of an independent substring list. Pure deletion of the second
   classifier, `src/executor/plan.rs` + `src/executor/policy.rs`.

Ship both together, in one PR, exactly as this session's own
`PROTECTED_RESOURCE_TYPES` floor (commit `2bf5377`) was sized:

- **One finding each, one file each, zero new call sites, zero new
  types.** #6 is a strict subtraction — the lowest-risk change possible.
- **Both close a currently-live, verified-real gap in the exact
  mechanism this session already shipped** — they complete work already
  in flight, not scope creep.
- **Both have obvious, cheap regression tests**:
  `is_destructive_action("some-unrecognized-future-verb") == true`;
  `every_protected_type_classifies_high_on_delete` (parameterized over
  all 9 `PROTECTED_RESOURCE_TYPES` entries).
- **Neither touches controller wiring, executor selection, lock
  semantics, or phase transitions** — the four areas where a mistake has
  real blast radius.
- Bundling them is defensible, not scope creep, because they are the
  *same class of bug* (a hand-maintained classifier silently failing to
  recognize an input it should) caught at two call sites the same
  session.
- **Revert cost**: two one-line reverts, zero coupling to anything else
  in this plan.

This is the template every subsequent SAFETY phase is sized against: one
finding, one file (or one function family), a unit test that fails on
the old behavior and passes on the new, no controller rewiring in the
same PR as a wiring change.

### SAFETY lane — M1 through M11

| # | Finding(s) | What ships | Files touched | Verified before moving on |
|---|---|---|---|---|
| **M1** | #2 | `DestroyClearance` private-newtype gate on `IacExecutor::destroy`; `handle_destroying` computes a plan before destroying (a forcing function — it computes none today) | `executor/policy.rs`, `executor/mod.rs` (trait sig), `controller/template_controller.rs::handle_destroying` | Unit: a `Destroying`-phase reconcile with a protected resource type in prior-drift is blocked exactly like `Applying` would be. Canary: scratch-namespace template with only non-protected resources deletes end-to-end without a false-positive block. |
| **M2** | #4 (the AutoApply hash bypass — **note:** the original phased-adoption design's own M-table omitted a dedicated phase for this finding despite naming it in scope; inserted here since its fix is fully spec'd and adversarial-pass-confirmed) | `ApplyAuthorization{HashApproved\|AutoApplyContinuous}` required to construct `Phase::Applying`, for every policy arm; persists a `status.appliedPlanFingerprint` | `executor/policy.rs`, `controller/template_controller.rs` (every `update_phase(.., Phase::Applying, ..)` call site) | Unit: an `AutoApply` reconcile now records a fingerprint on `status`; `RequireApproval` behavior unchanged. Regression: existing hash-comparison tests for `RequireApproval` stay green. |
| **M3** | Fleet gauge bug | `#[derive(AllVariants)]` on `Phase` (fixes the `Phase::ALL` `[Phase;11]` missing `CompileBlocked` bug) + `PolicyDecision` | `crd/infrastructure_template.rs` | Compile-time: generated `ALL` is exhaustive by construction. Regression: fleet gauge count includes a `CompileBlocked` fixture CR. *(Corrected from the original design's `ClosedSet` — see §2 correction 3.)* |
| **M4** | #5 | Gate the Applying-eligible cached render on `DependencyResolution::fully_satisfied()`; `RenderedConfig::{Real\|ContainsMocks}` sum type, non-`pub` inner fields | `controller/template_controller.rs::handle_compiling`, `controller/template_dependency.rs` | Unit: a compile with an unresolved (mocked) upstream dependency does not set the Applying-eligible flag. Regression fixture reproducing "fresh creation, upstream not yet Ready." |
| **M5** | #10 | Wire `TemplateDag::destroy_order()` into the deletion-finalizer path | `controller/template_dag.rs` (zero logic change — already 100% tested), `controller/template_controller.rs` (finalizer) | Existing `template_dag.rs` tests stay green. New integration test: two templates with a dependency edge, deleted concurrently — dependent's finalizer releases before upstream's. Canary in a scratch namespace first. |
| **M6** | #11 | Fix `policy_cascade.rs`'s `destroy_protection` merge to strictest-wins (matching `drift_reaction`); correct the false "already wired" doc comment | `controller/policy_cascade.rs` | Update `destroy_protection_innermost_wins` test to assert the *opposite*, correct precedence. Zero live risk (module has zero callers today). |
| **M7** | #12 | Exhaustive match on `magma_types::Action` in the post-apply audit record, deleting `_ => NoOp` | `executor/magma.rs` | Copy the already-correct mapping from `to_universal_plan` (~30 lines away, same file). Regression comparing both classifications for all 9 variants — must match. |
| **M8** | #9 | `CheckedExecutor` private-newtype gate — same shape as M1's `DestroyClearance` — required by `run_import_prepass`/`try_tofu_import`/`gather_attrs`/`resolve_conflicts_post_apply` | `controller/conflict.rs`, `controller/mod.rs` | Unit: constructing an executor with `PANGEA_FORBID_TOFU=true` + a tofu-selecting config fails at construction, parameterized over all four call sites. No canary needed — narrows an already-correct invariant to be structural. |
| **M9** | #15 | `ReconciliationLoop` added to `ControllerKind`, calls `policy_pipeline::run_for_controller` | `controller/reconciliation_loop_controller.rs`, `crd/operator_policy.rs` | Test: with `globalSuspend=true`, this controller now no-ops. Canary: flip `globalSuspend` in a scratch cluster, confirm every controller including this one stops mutating status. |
| **M10 (shadow)** | #8 part 1 | Phase FSM illegal-edge check becomes a counted, alertable metric — **zero behavior change** | `controller/template/status.rs` | Metric: `pangea_phase_fsm_illegal_edge_total{from,to}` exported and dashboarded. **Hard gate for M11**: a fixed, named minimum observation window (the org should set the number — 2 weeks is a reasonable starting proposal) with either zero occurrences or every occurrence triaged and allow-listed. |
| **M11 (enforce)** | #8 part 2 | Flip `update_phase`/`update_phase_with_error` from warn-and-apply to `return Err(IllegalPhaseTransition{from,to})` — zero call-site edits needed, all 33 already propagate via `?` | `controller/template/status.rs` | Full regression suite passes with the guard enforcing. Canary: a second, shorter non-prod window (e.g. 3 days) before the prod flip, watching the same metric. **Gated entirely on M10's data — do not ship in the same PR as M10, even though the code delta looks trivially small.** |

### SAFETY lane — higher blast-radius / needs an operational pre-flight

| # | Finding | What ships | Why it's gated separately |
|---|---|---|---|
| **M12** | #3 (executor migration) | Add `status.executorMigration`; route `executor_for_checked` through `executor_migration::step()`; derive `MigrationPolicy` from the `pangea.pleme.io/lifeline` label; wrap output in `ClearedExecutor` | **Highest blast-radius fix in the whole plan** — an unwitnessed magma/tofu swap on the rio-ssh lifeline template could sever cluster access. **Hard merge blocker**: a fixture `InfrastructureTemplate` with `status.executorMigration` absent (every existing prod template on day one) must resolve its executor byte-identically to pre-M12 code — get this wrong and every existing template silently stalls as "awaiting migration approval," a strictly worse state than today. **Operational pre-flight, not a code review item**: the `pangea.pleme.io/lifeline=rio-ssh` label must actually be applied to the real rio-ssh template *before* this ships. **Canary**: first live executor swap happens against a disposable scratch-namespace template through a full Shadow→Held→Cutover cycle before any swap touches a non-lifeline production template. |
| **M13** | #7 (state_lock) | `MutationLockMode::{Postgres\|Unguarded{acknowledged_risk}}`, fails closed on unacknowledged | **Deploy-time-affecting** — may require Postgres for any mutation-capable deployment. **Operational pre-flight**: confirm which live deployments run DB-less before merging; this cannot ship blind against an unknown fleet-wide config surface. |
| **M14 (shadow)** | RBAC/§3's confirmed-strength note area | Observability-only: instrument every cross-namespace `SecretRef`/`OutputSecretRef` read with `{requesting_namespace, target_namespace}` telemetry; reconcile the RBAC-verb documentation-vs-manifest discrepancy (`output_bindings.rs` claims `create/update/patch`, the checked-out `rbac.yaml` only grants `get/list/watch`) by checking the actually-deployed chart version | The RBAC/credential boundary is real but not implicated in the known incident — a lateral-movement/confused-deputy risk, not an active-fire one. Any enforcement here risks breaking legitimate cross-namespace flows nobody has inventoried — collect the inventory first. |
| **M15 (design-gated, not a committed code phase)** | RBAC follow-on | Namespace-scoping model for `secretRef`/`outputBindings.secretRef`, shape informed by M14's real usage data | Deliberately left undesigned until M14's data exists — designing the narrowing model blind risks trading a security gap for an availability outage. |
| **M16** | dead `role_arn` field | Deprecation warning event on any CR that sets `role_arn` (it currently does nothing); name real STS AssumeRole support as the destination, not committed to a phase | The field currently advertises a scoping control that silently does nothing — cheap to fix (an event, not a feature) independent of the larger RBAC redesign. |

### VOCAB lane — starts after SAFETY's M0–M9 land, not concurrently

| # | What ships | Why sequenced here |
|---|---|---|
| **M17 (pilot)** | First-ever `#[derive(TataraDomain)]` adoption on **`ReconciliationLoop`** (7 fields, lowest decision count, non-mutation-path — a bug here has zero blast radius on real infrastructure because nothing consumes the Lisp form at runtime yet) | Proves the mechanism — dependency add, derive, keyword collision check, round-trip test — on the cheapest possible surface. Round-trip property test: fixture struct → Lisp pretty-print → re-parse → byte-identical. Zero coupling to the SAFETY lane; runs entirely in CI. |
| **M18** | Convert `ArchitectureGem`'s shared types — `ReactivePolicy`/`SettlingPolicy` (genuinely reused across all 4 consuming CRDs) and `ApprovalRouting`/`DriftReaction` (reused across **2** CRDs — `WorkspaceCatalog` and `ArchitectureGem` only, per §2's correction, not 4) | These pay off multiple times over regardless of the corrected count; converting once here gives every consuming CRD's eventual vocabulary these sub-forms for free. |
| **M19 (gated, sub-phased)** | `InfrastructureTemplate`'s own type-hardening **first** — turn `executor: Option<String>`, `PolicyMatch`'s glob/regex/closed-vocab-as-string fields, and the import/output template mini-languages into real typed enums/a typed AST — **then** author its `(definfrastructuretemplate ...)` vocabulary atop the hardened shape | The highest-value, highest-complexity CRD (22 fields, ~17 decisions, the shadow-typing anti-pattern's epicenter). Internally sub-phased so the vocabulary is authored once, on final field shapes, not against soon-to-change `String` fields and redone later — the one place in this plan a VOCAB step is explicitly sequenced after a SAFETY-shaped step in the same file. |
| **M20 (bridge, gated)** | The Lisp→CR materialization/apply bridge tool — the first thing letting a `.lisp` form become a real, `kubectl apply`-equivalent CR | Everything before this phase produces an *inert* authoring surface (register + round-trip test only). **Hard gate**: an arbitrary-CR-generating property test, for every CRD converted so far, proving `CR spec → Lisp pretty-print → re-parse → apply() == original spec` byte-for-byte. **Canary**: first real usage against scratch-namespace/non-critical templates only — never a lifeline-labeled or production workspace — for a named observation window. |

*(A compile-time `AuthoringEnvironment` seam — resolving cross-CRD
references like `requiredGems`/`dependsOn`/`:policies` against the real
declared siblings — is the one authoring-layer capability genuinely
unavailable to YAML+schema without an admission webhook (none exists
today). It is real, well-reasoned, and worth keeping on the roadmap
inside M19/M20, but it is authoring-layer work with zero safety payoff —
correctly low priority relative to the SAFETY lane.)*

*(A static typestate `Galho<P>`-style phantom-type FSM for `Phase` — the
genuine truly-unrepresentable destination beyond M11's parse-time-rejected
middle tier — is explicitly out of this plan's near horizon. Named as
the destination (Operating Principle #0), not committed to a timeline.)*

---

## 6. Risk section

**1. Scope creep inside a single SAFETY phase — the most concrete
danger.** With ~19 findings in one catalog, the temptation once a
reviewer is inside `template_controller.rs` for (say) M1 is to "just also
fix M5 and M6 while we're here since we already understand the file."
M0's sizing (one finding, one function family, one test) is the whole
plan's size discipline for a reason. **Mitigation is procedural, not
technical**: PR review rejects any diff that closes a finding not named
in its phase, even if the fix is "free" while the file is open — file it
as the next phase instead.

**2. A phase shipping before its own prerequisite data exists — the M12
trap.** The single most likely way this plan produces an interim state
*worse* than either the start or the end: shipping `status.executorMigration`
and gating `executor_for` on it **without** first proving that a CR where
the field is absent (every existing prod template on day one) resolves
its executor byte-identically to pre-M12 code. Get this wrong and every
existing template could be silently reclassified as "awaiting migration
approval" and stall — strictly worse than today's "no migration-safety
FSM at all, but templates keep applying." The backward-compatibility
regression test is a **named hard merge blocker**, not an implicit
assumption; the `pangea.pleme.io/lifeline=rio-ssh` label's presence on
the real template is an *operational* pre-flight separate from code
review — a code-correct M12 merged against a cluster where nobody
applied that label yet still leaves the one workspace
`executor_migration.rs` was built to protect, unprotected.

**3. Flipping an enforcement gate before its shadow window has actually
run — the M10→M11 trap, generalized.** This pattern recurs wherever a
warn-only guard becomes a reject-and-requeue guard (only M10→M11 today,
but the pattern would recur for any future guard promotion). Concrete
failure mode: a reviewer sees M10 merged and reasons "the enforcement
code is basically the same, let's ship M11 in the same PR to save a
review cycle." Mitigation is structural — M10 and M11 are separate,
sequentially-numbered phases with a *named* minimum observation window in
M10's own verification column; skipping ahead requires explicitly
overriding a documented gate, not just missing an implicit one.

**4. Narrowing RBAC/credential scope on inventory nobody collected — the
M15 trap.** The tempting shortcut is to jump straight to "just make
`secretRef.namespace` same-namespace-only by default" — but without
M14's telemetry, that default flip could silently break a legitimate
cross-namespace output-binding flow currently in production, trading a
security gap for an availability outage. M15 is deliberately
design-gated and not committed to a code phase for exactly this reason.

**5. Authoring Lisp vocabulary against a field shape that's about to
change underneath it.** If `InfrastructureTemplate`'s Lisp form were
authored against the current `String`-typed `executor` field before it
becomes a real enum, every `.lisp` spec written in the interim commits to
a string literal needing re-authoring later — not a breaking mechanism
change, but wasted effort and a confusing mid-flight vocabulary change.
M19's internal ordering (harden types, then author) exists specifically
to avoid this.

**6. Trusting a recon claim without re-verifying it against the live
source before scheduling work against it — the risk this document itself
had to correct three times (§2).** Source drifts between when a claim is
written and when work against it is scheduled — the `DriftAction`/
`RiskLevel` "unused" claim was already stale by one commit
(`5397f3f`) at recon time. **Before executing any phase in §5, re-grep
the cited file:line against the current tree** — do not trust a citation
in this document (or any design doc) as permanently current. This is the
generalized form of risks 2–5 above: every one of them is a version of
"the plan assumed a fact that changed or was wrong."

**7. Running the VOCAB lane as a coequal parallel track, borrowing
credibility from claims that don't hold up.** The original phased-adoption
design framed VOCAB as "structurally incapable of causing harm" and
therefore safe to run alongside SAFETY from day one — true in isolation,
but the specific justifications used to argue *why it's worth doing now*
(the `ApprovalRouting`-reused-4-ways composability pitch, the `ClosedSet`
derive) were overclaims. §5 corrects this by sequencing VOCAB to start
only after SAFETY's first ~10 phases land — not because VOCAB is unsafe,
but because scheduling it as coequal implicitly claimed a safety payoff
it doesn't have (per §2, the top 10 findings need zero lisp), and that
implicit claim is itself a risk to the plan's own credibility.

**8. Some findings cannot reach a compile-time proof no matter how much
effort is spent, and treating them as open TODOs invites wasted work.**
#7 (state_lock) is C4-ceilinged on DB-less concurrency itself — no Rust
type makes two unrelated OS processes coordinate without a shared
backend. #4 (AutoApply)'s "the plan is actually safe" claim is
C2-ceilinged — unprovable at compile time, only auditable. The RBAC
boundary's *scoping* fix is a Helm chart change, not a Rust type at all
(no ceiling notation needed — it's simply out of `src/`'s reach). Naming
these ceilings up front (§3, §4.4) is what prevents a future engineer
from spending a sprint trying to typestate their way past a wall that
doesn't move.

---

## 7. Tier-honest ledger

Status column reflects this document's publication — **nothing below has
shipped yet; this is a plan, not a changelog.** Update this table as
phases land; do not mark a row "closed" until its target tier is
verified against the merged code, not the plan.

| # | Finding / component | Current tier | Target tier | Phase | Status |
|---|---|---|---|---|---|
| 1 | `PlanAction::Other(s)` escape hatch | only-mitigated (fail-open) | truly-unrepresentable | M0 | Not started |
| 6 | `risk_level()` diverges from `PROTECTED_RESOURCE_TYPES` | only-mitigated (2 lists) | truly-unrepresentable (divergence); C1 (correctness) | M0 | Not started |
| 2 | CR-deletion destroy skips floor | only-mitigated (opt-in bool) | truly-unrepresentable (in-crate) | M1 | Not started |
| 4 | `AutoApply` skips plan-hash fingerprint | unmitigated (default path) | truly-unrepresentable (fingerprint-exists); C2 (safety) | M2 | Not started |
| — | `Phase::ALL` missing `CompileBlocked` | unmitigated (hand array bug) | truly-unrepresentable (`AllVariants`) | M3 | Not started |
| 5 | `mockOutput` applied for real | unmitigated (predicate unused) | truly-unrepresentable (in-crate) | M4 | Not started |
| 10 | `TemplateDag::destroy_order` unwired | only-mitigated (unused primitive) | parse-time-rejected | M5 | Not started |
| 11 | `policy_cascade.rs` destroy_protection divergence + stale docs | only-mitigated (latent, dead code) | truly-unrepresentable (divergence) | M6 | Not started |
| 12 | `magma.rs` non-exhaustive `_ => NoOp` | only-mitigated | truly-unrepresentable | M7 | Not started |
| 9 | `PANGEA_FORBID_TOFU` convention-based on import path | only-mitigated (caller ordering) | truly-unrepresentable (in-crate) | M8 | Not started |
| 15 | `ReconciliationLoop` bypasses kill-switch | only-mitigated (silent convention violation) | parse-time-checked | M9 | Not started |
| 8 | Phase FSM guard warn-only (33 call sites) | below-`Result::Err` (log only) | only-mitigated (shadow: M10; enforce: M11); destination truly-unrep via `Galho<P>` | M10, M11 | Not started |
| 3 | Executor-migration FSM dead code | unmitigated (dead code) | parse-time-rejected → truly-unrep with `ClearedExecutor` | M12 | Not started |
| 7 | `state_lock` silently `None` w/o Postgres | only-mitigated / absent | parse-time-rejected (silence); C4 (DB-less concurrency) | M13 | Not started |
| — | RBAC/credential cross-namespace boundary | unchecked | telemetry (M14) → design-gated scoping (M15) | M14, M15 | Not started |
| — | Dead `role_arn` field | unchecked (advertises a control that does nothing) | deprecation event; destination = real STS AssumeRole | M16 | Not started |
| 13 | `PROTECTED_RESOURCE_TYPES` is a maintained denylist, not default-safe | only-mitigated | model inversion (default-safe-unless-allowlisted) | Not phased (named, undesigned) | Not started |
| 14 | Executor-typo silent fallthrough | only-mitigated (silent) | only-mitigated + observable | Not phased (small, low severity) | Not started |
| 16 | Mutex poison hazard on `ReconcileQueue` | only-mitigated (lock, not removed cell) | eliminate-the-shared-cell | Not phased (small, low severity) | Not started |
| 17 | `.to_str().unwrap()` panic (non-UTF8 path) | unguarded (latent) | parse-time-rejected | Not phased (small, low severity) | Not started |
| 18 | Destroy-failure message loses stderr | only-mitigated (omission) | reuse `combined_output` | Not phased (small, low severity) | Not started |
| — | `destroyProtection` gate (4 call sites) | only-mitigated, well-hardened, one named residual fail-open edge | (strength — residual edge not urgent) | N/A | Confirmed strength |
| — | 15 CRD spec types → `(def...)` vocabulary | no tatara-lisp dependency (first-time adoption) | full vocabulary, `AuthoringEnvironment` cross-referencing | M17–M20 | Not started |
| — | `ApprovalRouting`/`DriftReaction`/`ReactivePolicy`/`SettlingPolicy` shared-type conversion | duplicated per-CRD (2-way / 4-way, corrected count) | one Lisp sub-form, referenced by symbol | M18 | Not started |

---

**Companion documents in this repo**: [`LIFECYCLE-STATE-MACHINES.md`](./LIFECYCLE-STATE-MACHINES.md)
(the `Phase`/`TRANSITIONS` FSM this plan's #8/M10-M11 wires into is built
there — M0 in that document, dated 2026-06-19); [`AUTHORING.md`](./AUTHORING.md)
(today's pure-YAML authoring surface, the thing Destination A in §1
extends, never replaces); [`postmortems/2026-07-12-camelot-eks-state-wipe-duplicate-vpc.md`](./postmortems/2026-07-12-camelot-eks-state-wipe-duplicate-vpc.md)
(the incident class findings #2, #3, #9 in §3 all trace back to).
