# Pangea-operator alert runbooks

One section per alert rule emitted by the chart's PrometheusRule (see
`helmworks/charts/pangea-operator/values.yaml` `prometheusRules.rules`).
Each alert's `annotations.runbook_url` points at the section anchor on
this page.

When responding to an alert: read the **What it means** stanza, run the
**Quick checks**, and follow the **Resolution** path. If none match,
escalate per the postmortem template at the bottom.

---

## PangeaDriftDetected

**What it means:** A reconciled `InfrastructureTemplate` saw a non-zero
drift count vs. its desired state during the last 5 minutes.

**Quick checks:**

```bash
# Identify the drifting templates
kubectl get infrastructuretemplate -A -o wide
kubectl describe infra <name> -n <ns> | grep -A 20 'lastCycle'
```

**Resolution:** Drift is expected when external systems are touched
out-of-band. If the operator was paused and infra was hand-edited, drift
is the operator catching up. If not — investigate who wrote to that
provider outside the typed pipeline.

---

## PangeaReconciliationFailing

**What it means:** More than 5 reconciliation errors in 15 minutes.
Severity critical because this is the indicator that the operator
itself is broken (vs. a single template misbehaving).

**Quick checks:**

```bash
kubectl logs -n pangea-system -l app.kubernetes.io/name=pangea-operator -c pangea-operator --tail=200 | grep -iE 'error|reconcile'
kubectl get infrastructuretemplate -A | grep -vE 'Ready|Settled'
```

**Resolution:** Frequent causes:
* Compiler sidecar crashloop → check sidecar pod
* DB connectivity loss → `kubectl get cluster -n pangea-system pangea-database`
* Provider-credential expiry → check secrets named in
  `template.spec.providerCredentials`
* OperatorPolicy mis-set (`globalSuspend=true`) → that fires the dedicated
  PangeaOperatorPaused alert, not this one

---

## PangeaTemplateStuck

**What it means:** ≥1 InfrastructureTemplate has been in `phase=Failed`
for over 30 minutes. Failed phase is a terminal state — operator has
given up retrying without operator intervention.

**Quick checks:**

```bash
kubectl get infra -A | grep Failed
kubectl describe infra <name> -n <ns>
```

**Resolution:** Read `status.lastEscalationReason`. Common causes:
* Compile failure exceeding `settlingPolicy.maxConsecutiveDriftCycles`
* Provider rejection that needs a new auth method
* Manual intervention required (e.g., clean up an orphaned resource by hand)

After fixing, clear the failure: `kubectl patch infra <name> -n <ns> --subresource=status --type=merge -p '{"status":{"phase":"Pending","autoSuspended":false}}'`.

---

## PangeaTemplateNotSettled

**What it means:** A template's `pangea_settled == 0` for over 15 minutes.
Drift is reappearing on every reconcile cycle — auto-apply isn't converging.

**Quick checks:** Look at `status.stuckResources` on the template — those
are the resources the operator's drift detector keeps re-finding.

**Resolution:** Either the resource genuinely re-drifts every cycle (e.g.,
an external automation is fighting the operator) or there's a typed
policy refuse blocking the apply. The dedicated `PangeaPolicyRefuseFiring`
alert covers the latter.

---

## PangeaSettlingEscalation

**What it means:** State-settling tracker gave up on a template.
`status.stuckResources` will not converge on its own.

**Resolution:** Read `status.lastEscalationReason`. Hand-investigate the
named resource and either (a) make the spec match reality, (b) destroy
and recreate, or (c) add a policy refuse if the change is unwanted.

---

## PangeaPolicyRefuseFiring

**What it means:** A `spec.policies` rule with `decision: refuse` matched
≥1 change in the last 15 minutes — the plan was not applied.

**Resolution:** Either the refuse rule is correct (a destructive change
was prevented) or it's overzealous. Inspect the cycle receipt for the
matched action; tune the rule or relax the policy if the change is safe.

---

## PangeaHighRiskPending

**What it means:** ≥1 resource with `action ∈ {delete, replace}` and
`risk = high` is sitting in pending state.

**Resolution:** A high-risk action should be reviewed before it's allowed
to apply. Check the template's `lastCycle` for context; either approve
(remove suspend), refuse (add a policy rule), or destroy-protect the
specific resource via `spec.destroyProtection`.

---

## PackerBuildFailed / ImagePipelineStuck / AmiTestFailed

**What they mean:** AMI / packer / image-pipeline sub-workflows hit
terminal failure or non-progress states.

**Resolution:** These are workflow-specific. Read the resource's status
+ logs from the build pod. Common causes: source git ref invalid,
provider quota, manifest syntax error.

---

## ComplianceNonCompliant / ComplianceBindingGating

**What they mean:** A `ComplianceSchedule` evaluated to non-compliant,
or a `ComplianceBinding` is gating downstream targets.

**Resolution:** Severity is critical because compliance gating
auto-suspends bound infrastructure. Read the schedule's last evaluation;
either fix the underlying compliance gap or temporarily exempt the
target via `binding.spec.exemptions` (and document why).

---

## PangeaTemplateStuckCompiling

**What it means:** A template's `consecutiveCompileFailures >= 3` for
over 5 minutes.

**Quick checks:**

```bash
kubectl describe infra <name> -n <ns> | grep -A 10 'lastCompileError'
```

**Resolution:** Common causes (in order of frequency):
* Missing provider gem (e.g. `pangea-github` not bundled in the operator
  image — see `pangea-operator/flake.nix` `pangeaInputs`)
* Syntax error in workspace DSL — check the `.rb` source
* Unresolved git source — verify network + auth to the source

At `settlingPolicy.maxConsecutiveDriftCycles` (default 5), the operator
escalates to `phase=Failed`.

---

## PangeaOperatorPaused

**What it means:** `OperatorPolicy/default.spec.globalSuspend=true` and
≥10 reconciles were skipped in the last 5 minutes. The pause is
intentional; this is informational.

**Quick checks:** `kubectl get oppol default -o yaml`.

**Resolution:** When ready to resume:
```bash
kubectl patch oppol default --type=merge -p '{"spec":{"globalSuspend":false,"globalSuspendReason":""}}'
```

This alert uses `severity: info`. It should not page on-call. If it's
firing for an unintentional pause, that's a config issue — investigate
who set `globalSuspend=true` and why.

---

## PangeaControllerSuspended / PangeaWorkspacePaused / PangeaActiveOverridesActive

**What they mean:** Per-controller or per-workspace pause state in
effect. `Active` means a workspace has an explicit carve-out from a
more-general pause.

**Resolution:** All three are info-level by design. Use them to verify
the dashboard reflects the expected pause topology. Resolve by editing
`OperatorPolicy/default.spec.{controllerSuspend, workspaceSuspend}`.

---

## When none of the above match

Postmortem template:

```
- Alert: <name>
- First fired: <time>
- Affected resources: <list>
- Symptoms: <what was visible>
- Mitigation: <what stopped the alarm>
- Root cause: <why it happened>
- Action items: <what we'll change to prevent recurrence>
```

File under `pleme-io/pangea-operator/docs/postmortems/<date>-<slug>.md`.
