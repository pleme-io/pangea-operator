# 2026-07-12 — `workspace.clean()` state wipe + stale plan-hash approval created a duplicate VPC on `camelot-eks`

- **Alert:** none — caught live by direct `k8s-pod-exec` inspection of
  `terraform.tfstate` cross-checked against `aws ec2 describe-vpcs`
  during an active provisioning session, not by any automated signal.
- **First observed:** 2026-07-12, during `camelot-eks`
  InfrastructureTemplate's first real apply (VPC + IAM roles under way).
- **Operator image at incident:** unchanged since `16b77f6`
  (`operator: add spec.secretFiles`) — no operator code changed between
  incident and this writeup; this is a diagnosis-and-workaround
  postmortem, not a fixed-and-shipped one (see Open follow-ups).
- **Affected resource:** `InfrastructureTemplate/camelot-eks` (namespace
  `camelot`, reconciled by Camelot Mode-1's pangea-operator pod
  `pangea-operator-5f689c58d4-gmhpd`), `executor: tofu` (disk-based —
  this Deployment has no `PGHOST`/`PGPASSWORD` wired, so `executor_for`
  always falls back to `tofu`, never `magma`, regardless of
  `spec.executor`).
- **Symptoms:**
  - A real, approved `+50 ~0 -0` apply had genuinely created
    `vpc-094734439e62440a8` + 2 IAM roles in AWS (confirmed live in
    the AWS console/CLI) when the reconcile loop, mid-cycle, logged
    `"Spec changed — cleaning workspace and restarting from Pending"`
    and reset to `Phase::Pending` — with no deliberate `.spec` edit
    from the operating session in that window.
  - The next automatic cycle re-compiled, re-planned against an EMPTY
    `terraform.tfstate` (all 50 resources `create`), and — because
    `status.approvedPlanHash` from the FIRST apply's approval still
    equaled the fresh `status.pendingPlanHash` — proceeded straight to
    `Applying` with **no human re-approval**.
  - That blind re-apply created a full SECOND VPC networking layer
    (`vpc-06987bc0cd6d8aaad`: VPC + IGW + 4 subnets + 2 explicit route
    tables + 2 explicit NACLs + 14 NACL rules + 4 route-table
    associations + 1 S3 VPC endpoint — every non-IAM resource in the
    plan) before hard-failing on `iam:CreateRole EntityAlreadyExists`
    for the 2 IAM roles the FIRST apply had already created.
  - Direct pod-exec inspection at the start of the recovery session
    found `/var/pangea/workspaces/camelot/camelot-eks/terraform.tfstate`
    did not exist at all (0 bytes / ENOENT) — state was genuinely gone,
    not just stale.
- **Mitigation (immediate):** `spec.suspend: true` flipped live via
  `kubectl patch --subresource status`-adjacent spec patch (committed
  immediately after, so GitOps reconvergence doesn't silently flip it
  back), stopping further blind retries. The two orphaned IAM roles
  from the FIRST apply were deleted directly via `aws iam delete-role`
  (safe — nothing referenced them, no EKS cluster ever got created).
  `vpc-094734439e62440a8` (the first, now-orphaned VPC) was
  **deliberately left alone** — a local guardrail hook refuses any
  `aws ec2 delete-vpc*`-shaped command for AI agents, by design, and it
  costs ~$0/mo idle (no NAT gateway, no compute) — left for the human
  operator to clean up by hand.
- **Mitigation (durable, this session):** adopted `vpc-06987bc0cd6d8aaad`
  (the SECOND, more-completely-built VPC) into state via
  `spec.importHints` — the operator's own CRD-native, sanctioned
  adopt-existing-resource mechanism
  (`deploy/crds/infrastructuretemplates.yaml:156`,
  `controller/import.rs`, `handle_applying`'s `run_import_prepass`
  call, `template_controller.rs:2375`) — 33 explicit
  `address → real-AWS-ID` hints, no hand-written `terraform.tfstate`
  JSON. Additionally, `status.approvedPlanHash` was explicitly cleared
  via a `--subresource status` patch BEFORE re-unsuspending, so this
  particular recovery got a genuine human checkpoint on the fresh plan
  instead of falling into the same stale-approval hole documented
  below. `pleme-io/akeyless-k8s@277ca2c`.

## Root cause(s) — two independent bugs that compounded

1. **`Workspace::clean()` deletes `terraform.tfstate` on EVERY spec
   generation bump under the disk-based `tofu` executor — not a rare
   edge case.**
   `executor/workspace.rs:228`:
   ```rust
   /// Clean the workspace (remove all files except .terraform).
   pub async fn clean(&self) -> Result<()> {
       // ... removes every entry in the workspace dir except
       // ".terraform" and ".terraform.lock.hcl" — including
       // terraform.tfstate — unconditionally.
   }
   ```
   `template_controller.rs:460`:
   ```rust
   if current_gen != observed_gen && current_phase != Phase::Pending && current_phase != Phase::Destroying {
       info!(current_gen, observed_gen, "Spec changed — cleaning workspace and restarting from Pending");
       let workspace = state.workspace_manager.get_workspace(&template).await?;
       workspace.clean().await?;
       update_phase(&template, Phase::Pending, &state).await?;
       ...
   }
   ```
   This fires on ANY `metadata.generation` bump — which Kubernetes
   applies on every `.spec` mutation, including ones that don't touch
   the rendered Terraform content at all (e.g. `spec.variables`-only
   edits, or — per the comment at `template_controller.rs:432-447` —
   edits the generation-aware render-reuse gate was specifically
   built to survive on the COMPILE side). The DB-backed `magma`
   executor is unaffected (state lives in Postgres, `clean()` never
   touches it); every CR still on the disk-based `tofu` executor loses
   its entire local state on every spec edit, not just the documented
   "benign generation bump" edge case this file's own header
   originally (and too narrowly) described the incident as.

2. **The plan-hash approval gate has no state fingerprint — a stale
   approval silently re-validates a structurally-identical LATER
   plan.** `template_controller.rs:2258-2259`:
   ```rust
   let plan_content = plan_result.raw_stdout.as_str();
   let plan_hash = format!("{:016x}", content_hash(plan_content));
   ```
   and the approval check, `template_controller.rs:2241-2250`:
   ```rust
   let is_approved = template.status.as_ref().and_then(|s| {
       match (&s.pending_plan_hash, &s.approved_plan_hash) {
           (Some(pending), Some(approved)) if !pending.is_empty() => Some(pending == approved),
           _ => None,
       }
   }).unwrap_or(false);
   ```
   The hash is computed purely from the plan's rendered text (resource
   addresses + actions) — it folds in NO state serial, state hash, or
   lineage ID. Two DIFFERENT `tofu plan` runs against two DIFFERENT
   (but structurally identical) states — e.g. "empty state, plan
   everything" computed once against a truly-empty pre-apply state,
   and AGAIN after `workspace.clean()` wiped a state that had
   partially-applied real resources — produce the SAME hash, because
   the plan TEXT is the same shape both times. `status.approvedPlanHash`
   is cleared only on a successful apply (`status.rs`), never on a
   `workspace.clean()`. So: a human approves plan hash `X` once,
   `clean()` wipes state, the next planning cycle re-derives the
   IDENTICAL hash `X` from the now-stale-but-textually-same plan, and
   `handle_planning`'s `PolicyDecision::RequireApproval` branch (
   `template_controller.rs:2252-2256`) sees `is_approved = true` and
   proceeds straight to `Applying` — no human ever looks at the SECOND
   plan, even though the underlying cloud reality it's about to act on
   has completely changed (a partially-built VPC now silently exists
   that the plan doesn't know about).

Bug 1 causes state loss; bug 2 removes the human checkpoint that would
otherwise have caught it before a second `tofu apply` ran blind. Either
bug alone is containable; together they turned one benign-looking spec
edit into an unattended duplicate-VPC apply.

## Relationship to already-named fleet doctrine

This is a live, concrete instance of the **★★ MAGMA-NATIVE EXECUTION**
"interim, not the destination" gap already named in
`pleme-io/CLAUDE.md` and `theory/CAMELOT.md` — pod-local disk state
under the `tofu` executor instead of the DB-backed `magma` path. The
existing writeup framed the risk as "a pod restart wipes the
workspace"; this incident shows the REAL blast radius is wider — the
SAME workspace wipe happens on every spec-generation bump, with a pod
that never restarted at all (`pangea-operator-5f689c58d4-gmhpd`,
`restartCount: 0` throughout this entire incident).

## Open follow-ups (none shipped yet — this is diagnosis + a per-CR
## workaround, not an operator code fix)

- **Fold a state-derived component into the plan hash.** The hash
  should change whenever the STATE the plan was computed against
  changes — e.g. hash `(plan_text, state_serial_or_content_hash)`
  together, or store the state's content hash alongside
  `pendingPlanHash` and require both to match on approval. This closes
  bug 2 directly: a wiped-and-regenerated state can never silently
  reuse a stale approval again, even if the plan text happens to be
  textually identical.
- **Stop treating disk-based `tofu` state as disposable on every spec
  edit.** Either (a) make `workspace.clean()` preserve
  `terraform.tfstate` by default and only wipe it on an explicit,
  narrower trigger (a real destroy, or an opt-in "reset state" flag —
  never a bare generation bump), or (b) accelerate migration of
  disk-executor CRs to the DB-backed `magma` path (state in Postgres,
  immune to workspace wipes entirely) per the already-named
  MAGMA-NATIVE EXECUTION destination.
- **Narrow the generation-mismatch trigger.** `template_controller.rs`
  already has a MORE PRECISE gate for the render-reuse question
  (`generation_invalidates_render`, comment at :432-447) that
  distinguishes "does this spec edit change the rendered Terraform
  content" from "did generation merely advance." The workspace-clean
  trigger at :460 uses the cruder `current_gen != observed_gen` check;
  it should use the same precision the render-reuse gate already has,
  so a genuinely no-op spec edit (or one that changes only
  operator-side bookkeeping fields, if any exist) doesn't wipe state
  for no reason.
- **Consider surfacing "state is empty but this CR previously had a
  successful partial apply" as a loud, typed warning** (event +
  condition), not just an info-level log line — this incident was
  caught by a human happening to inspect the pod filesystem directly,
  not by any operator-surfaced signal.

None of the above are implemented in this pass — this incident was
resolved per-CR via `spec.importHints` (a real, already-shipped,
sanctioned mechanism) rather than an operator code change, because the
priority was recovering `camelot-eks` quickly without risking a THIRD
duplicate VPC. The class of bug (stale approval surviving a state wipe)
remains live for every other disk-executor `InfrastructureTemplate` in
the fleet until the hardening above ships.
