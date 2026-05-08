# 2026-05-07 — status-write self-trigger reconcile loops on rio

- **Alert:** none (the bug class is what motivated chart 0.8.14's
  reconcileRateAnomaly alert family). Detected by direct observation
  of host CPU + load.
- **First observed:** 2026-05-07, ~17:00 local (rio uptime ~20h).
  Earlier history likely days; this was the first session that
  diagnosed it.
- **Operator image at incident start:** `embedded-amd64-42b4837`
- **Operator chart at incident start:** `0.8.13`
- **Affected resource:** rio cluster (single-node home edge, k3s).
  Cluster-wide impact — pangea-operator was burning the API server
  for every pod on the box.
- **Symptoms (visible to operator-human):**
  - `top` showed `k3s-server` at 700% CPU.
  - `uptime` load1 = 9.57.
  - 10 unrelated pods in CrashLoopBackOff (helm-controller,
    source-controller, kustomize-controller, traefik,
    metrics-server, cnpg-operator, pangea-database-1/2,
    garage-0). Each was failing to reach `https://10.43.0.1:443/api`
    with "i/o timeout" because the apiserver was too busy.
  - `kubectl run --rm -i ... nslookup` from a fresh pod hung —
    in-cluster DNS broken.
- **Mitigation (immediate):** scaled `pangea-operator` to 0
  replicas. CPU returned to ~3.7% within seconds.
- **Mitigation (durable):** see Resolution below.
- **Root cause(s):**
  Compounded — the host networking layer had a separate cilium
  leftover that masked the operator's hot loops at the symptom
  layer (DNS timeout) until kube-proxy was repaired. Two
  independent root causes:

  1. **Operator: status-write self-trigger watch loops.** Three
     confirmed sites where every reconcile re-PATCHed `.status`
     with byte-equal content but a fresh `Utc::now()` timestamp:
     - `template_controller`'s suspended-skip path —
       `conditions_for_suspended()` constructs conditions via
       `create_condition()` which restamps `lastTransitionTime` on
       every call. Observed 123 PATCH/sec on `cloudflare-pleme`
       alone (52 reconciles/sec across 6 templates).
     - `operator_policy_controller` — every reconcile set
       `status.last_changed_at = Utc::now()` and echoed the live
       `OperatorPolicyCache.skipped` atomic into
       `status.reconcilesSkipped`. Persisted at 76 reconciles/sec
       on `OperatorPolicy/default` even with `globalSuspend=true`
       (this controller intentionally bypasses the policy gate).
     - `fleet_status_controller` — `aggregate_fleet_status` always
       returns a status with fresh `last_updated_at` and a
       fresh-stamped `Updated` condition. Observed 10
       reconciles/sec on `PangeaFleetStatus/default`. Bypasses the
       policy gate by design (read-only observability).

  2. **Host: cilium → flannel migration leftover.** The k3s config
     `disable-kube-proxy: true` (a cilium-era setting; cilium does
     kube-proxy replacement via eBPF) survived the cilium →
     flannel migration. Flannel doesn't replace kube-proxy, so
     iptables `KUBE-SVC-*` DNATs froze at pre-migration pod IPs.
     `10.43.0.10:53` (CoreDNS Service) was DNAT'd to a dead pod IP,
     which is why DNS hung for every pod even though the operator
     loops were the underlying CPU drain. Pod-to-pod via VXLAN
     kept working through this — only Service-IP traffic broke.
     Stale `lxc*` veth pairs (26), BPF program pins at
     `/sys/fs/bpf/cilium/`, and `/run/cilium/cgroupv2` mount
     additionally lingered.
- **Action items:** all completed during the same session.
  - Diff-gate every status PATCH at the in-controller boundary
    (`operator c02ab09` + `6a9663f` + `4f421cb` + `ab859b0`).
    Lifted helper `conditions_observably_equal` removes ~50 lines
    of duplication across 4 controllers (`9ccb221`).
  - Add `predicate_filter(predicates::generation)` at the
    watch-stream boundary as defense-in-depth (`8a6ccb7` + Cargo.nix
    regen `a0c1370`). All 14 controllers migrated to
    `controller::generation_filter::filtered_controller`.
  - Add chart 0.8.14 reconcileRateAnomaly alert family
    (`helmworks/charts/pangea-operator c51b5da`). Default OFF;
    enabled on rio (`k8s 0a77d42`).
  - Backfill `pangea_controller_reconciliations_total`
    denominator for the 4 missing controllers — including a new
    `Metrics::record_reconcile_named` helper for the two
    self-driving controllers that intentionally aren't in
    `ControllerKind` (`4a99f46`).
  - Document the canonical pattern in operator CLAUDE.md +
    RUNBOOKS.md sections for both alerts (`4a99f46`).
  - Add cargo doc-test for the canonical helper (`c388396`).
  - Host-side: drop `disable-kube-proxy: true` from
    `nodes/rio/configuration.nix` (`nix 028e663`). Document the
    trap and the cilium-leftover cleanup procedure in
    `nodes/rio/CLAUDE.md` (`ee79d6f`). Persist the
    cilium → flannel cleanup as a one-shot recipe.

## Resolution

The combined fix dropped rio from:

| metric | pre-fix | post-fix |
|---|---|---|
| k3s CPU instant | 700% | 3.7% |
| load1 | 9.57 | 0.34 |
| API req/s | 230 | 6 |
| operator pod CPU | 6300m | 1m |
| `template_controller` rate | 52/sec | 0/min |
| `operator_policy_controller` rate | 76/sec | 0.03/sec |
| `fleet_status_controller` rate | 10/sec | 0/min |
| failed pods | 10 CrashLoopBackOff | 0 |

The diff-gates + predicate-filter combined eliminated all three
observed loops AND the latent class. The watch-stream filter alone
(at the kube-rs layer) eliminates loops even if a future
status-write site forgets its in-reconcile gate — because the gate
operates on `metadata.generation` which the apiserver guarantees
only advances on spec mutations, not status writes.

**Image promotion sequence:**

1. `42b4837` (incident start) → `6a9663f` (template + operator_policy
   diff-gates + 29 unit tests).
2. → `4f421cb` (fleet_status diff-gate). At this image,
   fleet_status was still at 10/sec because the gate compared
   against `fs.status` (stale-by-one cache lag).
3. → `ab859b0` (in-Context `last_patched` snapshot replaces
   `fs.status` for the gate input). fleet_status drops to 0/min.
4. → `a0c1370` (predicate filter migration; Cargo.nix regen).
   No metric change vs ab859b0 (the diff-gates already had the
   loops covered) but defense-in-depth now layered.

## Phased reactivation

Ran the 7-stage reactivation discipline (memorialized in
`feedback_phased_operator_reactivation.md` per session memory):
scale-to-zero → rebuild → globalSuspend=true → carve-out one
workspace → carve-out a second → lift global pause → re-enable
watchdog. Each stage had explicit pass criteria (reconcile rate
floor, no rv churn) and one-command rollback. All passed cleanly;
the full lift took only the time to flip one CR field via
`kubectl patch` because the upstream stages had de-risked it.

**Stage 3 finding:** the carve-out stage was where we discovered
the kube-rs reflector cache lag — fleet_status's first
diff-gate cut compared `fs.status` from the watcher and the cache
lagged the apiserver, so the gate always returned "differs". Fixed
in `ab859b0` by tracking last-patched in `Context`.

## Lessons

Captured in session memory for future incident response:

- **The status-write loop antipattern is generic to all kube-rs
  operators.** Any field built with `Utc::now()` (condition
  `lastTransitionTime`, `lastUpdatedAt`, etc.) without a
  diff-gate is a latent hot loop. Defense-in-depth via
  `predicate_filter(predicates::generation)` at the watch layer
  catches the class even if a single site's gate is missing.
- **kube-rs reflector cache lags the apiserver.** When a
  controller writes its own watched resource, the watch event
  that fires next arrives BEFORE the cache observes the patch.
  Diff-gate against an in-Context snapshot, not `obj.status`.
- **cilium → flannel migration cleanup is not automatic.** The
  `disable-kube-proxy` k3s flag, lxc* veths, BPF cilium pins,
  `/run/cilium/cgroupv2` mount, and cilium.io CRDs all linger.
  Document the cleanup procedure on every host that's been
  through the migration.
- **Phased reactivation > big-bang restart.** 7 stages with
  explicit pass criteria caught the kube-rs cache-lag bug at
  stage 3 instead of shipping it broken to the full fleet.

## Hardening for next time

- Chart 0.8.14 alerts (default OFF, opted-in on rio) detect this
  class within ~1 minute of crossing the 1/sec WARN threshold
  per controller. The next hot-loop regression will fire an alert,
  not eat a multi-hour debug session.
- The lifted `conditions_observably_equal` helper +
  `filtered_controller` helper are documented as the canonical
  pattern in operator CLAUDE.md. New controllers MUST use them.
- Saved memories surface the antipatterns + the kube-rs cache lag
  + the cilium leftover cleanup + the phased reactivation
  discipline + the alert-enable precedent automatically in future
  sessions.

## Open follow-ups

None blocking. The chart 0.8.14 alerts are already enabled on
rio. If new pangea-operator deployments come online elsewhere
(currently only `inception` and `scale-test` exist, both at chart
0.1.x — pre-bug-class), they should bump to 0.8.14+ and enable
the alert family.
