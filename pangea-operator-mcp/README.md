# pangea-operator-mcp

The MCP surface a model (or operator) uses to **observe and control a running
pangea-operator** — so "I'm blind to the operator" is never the failure mode
again. The CRDs *are* the state; this is a typed kube facade, not a second
store (same shape as `breathe-mcp`).

## Tools

| Tool | Verb | What |
|---|---|---|
| `pangea_template_list` | observe | InfrastructureTemplate summaries (phase, suspended, lastError, planSummary, resourceCounts, lastCycle), optionally namespace-scoped |
| `pangea_template_get` | observe | full template CR (spec + `status.lastCycle`) — *why is this stuck / what did it last apply / what is the error* |
| `pangea_template_reconcile` | control | force a reconcile (stamp a reconcile-request annotation — the `flux reconcile` analog) |
| `pangea_template_suspend` | control | pause/resume one template via `spec.suspend` |
| `pangea_workspace_list` | observe | WorkspaceCatalog verify status (a workspace whose required gems aren't all Loaded blocks every template under it past Verified) |
| `pangea_gem_list` | observe | ArchitectureGem load phases (a not-Loaded gem is a common stuck-at-Verified root cause) |
| `pangea_operator_restart` | control | roll-restart the operator Deployment (break-glass for a wedged controller) |
| `pangea_operator_status` | observe | operator Deployment replicas/ready/available + conditions — *is the controller even up?* |

Every mutation is an idempotent metadata/spec patch (the `kubectl annotate` /
`kubectl rollout restart` analogs) — never destructive, never contends with the
controller's `status` co-write. The whole surface is behind the `PangeaStore`
trait, so the tools are mock-tested without a cluster (`cargo test`).

## How it reaches the operator (the crux)

The binary is a stdio MCP that takes a kube client via `from_env()` —
**in-cluster** *or* a local kubeconfig context. So it works the moment the
operator's K8s API is reachable. Two deployment shapes:

- **Local (M0, today):** run `pangea-operator-mcp` as a stdio MCP against a
  reachable kube context (tailscale/VPN to the cluster, or a tunnel-exposed
  API). Wire it into the mcp-fleet like any stdio MCP.
- **Remote (M1, the destination):** run it **in-cluster** (operator namespace)
  with in-cluster RBAC, exposed over an HTTP/SSE transport via the cluster's
  Cloudflare tunnel + saguão Access (exactly how `grafana-rio` reaches rio).
  Then the laptop MCP client reaches `pangea-operator-mcp.<cluster>.quero.cloud`
  with no VPN — which is the whole point: control the operator space remotely.

> **The reachability caveat is honest:** an in-cluster MCP deployed via the same
> Flux/operator loop can observe + restart a *healthy-but-stuck* operator, but
> cannot bootstrap-fix a *fully wedged* one (that still needs break-glass via
> direct cluster access). Its job is to make the *blind* case — "a template is
> stuck and I can't see why" — a one-tool query + a one-tool kick.

## Milestones

- **M0 (this crate):** facade + 8 tools + mock tests. Local stdio against a
  reachable context.
- **M1 (deploy):** Nix `dockerTools` image (no Dockerfile) + AUTO-RELEASE; a
  HelmRelease on the control cluster with namespace-scoped RBAC
  (get/list/patch on the pangea CRDs + the operator Deployment); an HTTP/SSE
  transport; a `pangea-operator-mcp.<cluster>.quero.cloud` ingress on the
  cluster tunnel; the mcp-fleet client entry in nix.
- **M2:** read the operator's Postgres artifacts (rendered config / plan /
  bundle by content hash) for the full reconcile receipt, and stream reconcile
  events.

## Pattern

Mirrors `breathe/breathe-mcp` (rmcp 0.15, stdio, `#[tool]`/`#[tool_router]`/
`#[tool_handler]`, a `*Store` trait + `KubeStore`). Decoupled from the
operator's internal CRD structs — reads/patches via kube's **dynamic** API
(group `pangea.pleme.io`, version `v1alpha1`), so it only needs the GVK + the
handful of status/spec fields it surfaces.
