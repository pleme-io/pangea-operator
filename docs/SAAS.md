# pangea-operator as a SaaS — `pangea.quero.cloud` on rio

> **Destination-first** (Operating Principle #0). This doc names the absolute-best
> long-term shape of "pangea-operator as a full SaaS," then the phased path. It is
> the **first dogfood** of the two pleme-io product doctrines:
> [`theory/URDUME.md`](https://github.com/pleme-io/theory/blob/main/URDUME.md)
> (the backend service) + [`theory/TELA.md`](https://github.com/pleme-io/theory/blob/main/TELA.md)
> (the full-Rust frontend). `tela + tecido = one full SaaS product`.

## The destination

**Infrastructure-as-a-Service, declared and observed — never planned or applied
by hand.** A tenant signs in at **`pangea.quero.cloud`** (saguao/Authentik SSO),
*declares* desired infrastructure (an `InfrastructureTemplate` / `ArchitectureGem`
authored through a typed Leptos UI), and *observes* the reconcile receipts
(`status.lastCycle`) + a Pangea-declared Grafana view — while `pangea-operator`
(already live on rio) does the only execution that ever runs: clone gem →
magnus compile → magma plan → magma provider-RPC apply → state+bundle atomic to
Postgres → typed receipt. The human's only two verbs are **declare** and
**observe** (the org ★★ PLATFORM-MEDIATED rule), now exposed as a product.

The SaaS is **not new infrastructure** — it is the existing operator with two
faces added: an **authenticated multi-tenant API** (Urdume) and a **Leptos web
console** (Tela), fronted at a public hostname via the proven
`grafana.quero.cloud` pattern.

## Current pangea-operator → Urdume tramas (what exists, what's the gap)

The backend is ~80% of an Urdume service already:

| Urdume trama | pangea-operator today | SaaS gap |
|---|---|---|
| L0 data spine | Postgres: `pangea_meta.artifacts` (rendered_config/plan/bundle, BLAKE3) + `{schema}_{template}_states` (magma state); atomic apply tx | tenant/account tables; per-tenant row-level isolation |
| L1 config | env-driven (`PANGEA_EXECUTOR=magma`, `PANGEA_FORBID_TOFU`) | shikumi `TieredConfig` (pending-shikumi) |
| L2 runtime | `pangea-operator` binary: kube-rs reconcilers **+ axum + GraphQL/gRPC** | a public, authn'd HTTP surface (vs in-cluster) |
| L3 API | GraphQL + gRPC over the operator core | a typed **tenant-scoped** GraphQL schema (declare templates, read receipts) |
| L4 BFF | — (the API is direct) | front with hanabi-style BFF **or** expose the operator's GraphQL through saguao `vigia` |
| L5 packaging | `flake.nix`, dockerImage (embedded magnus) | `(defcaixa :kind Servico)` |
| L6 delivery | live on rio via FluxCD; CRDs (`InfrastructureTemplate`/`ArchitectureGem`/`WorkspaceCatalog`/**`PangeaNamespace`**) | a public ingress + the web console chart |
| L7 mesh | in-cluster Services | n/a at M0 |
| L8 identity/secrets | cofre/Akeyless; CNPG creds | **saguao SSO** at the edge; per-tenant `PangeaNamespace` authz via `cracha` |
| L9 observability | operator metrics + Pangea-defined Grafana (grafana.quero.cloud) | a tenant-facing status view |

**Multi-tenancy is already modeled:** `PangeaNamespace` (the typed state-isolation
boundary) + `WorkspaceCatalog` give per-tenant isolation; `cracha` `AccessPolicy`
gates which tenant sees which namespace. A SaaS tenant ⇒ one `PangeaNamespace`.

## Current pangea-web → Tela

`pangea-web` is a **Yew/wasm32** UI (built separately, not in the workspace) —
per [`TELA.md`](https://github.com/pleme-io/theory/blob/main/TELA.md) §F3, **Yew
is the named migration backlog to Leptos 0.7**. The console is a **greenfield
Leptos rewrite** consuming the operator's GraphQL SDL (the Tela F5 seam), styled
from **ishou** tokens, gated by **saguao** (Tela F7, identity off the JS heap).

## The deploy pattern (proven by grafana.quero.cloud)

`pangea.quero.cloud` is **one ingress entry** in
`k8s/clusters/rio/architectures/drive-cloudflare-tunnel.yaml`, added before the
`{ service: http_status:404 }` terminator, exactly like `grafana.quero.cloud`:

```ruby
{ hostname: 'pangea.quero.cloud',
  service:  'http://pangea-saas.pangea-system.svc.cluster.local:80' },
```

Commit → pangea-operator reconciles the CNAME → `<tunnel_id>.cfargotunnel.com`
+ the tunnel ingress; `cloudflared` routes public traffic to the in-cluster
Service. Gate behind Authentik OIDC via saguao `vigia` (the grafana.quero.cloud
auth shape). `pangea.quero.cloud` is a **reserved 2-part control-plane hostname**
(saguao class), like `auth.quero.cloud`/`cracha.quero.cloud`.

## Phased path (each phase ships something live; never plan/apply by hand)

- **M0 — the authenticated read-only console (observe).** A Leptos `pangea-web`
  rewrite serving the operator's existing GraphQL (list templates, read
  `status.lastCycle` receipts), gated by saguao SSO, at `pangea.quero.cloud`.
  Backend: expose the operator's GraphQL through a `pangea-saas` Service +
  pleme-lib chart + FluxCD `HelmRelease` under `k8s/clusters/rio/` + the one
  tunnel-ingress line. **This is the smallest end-to-end dogfood of Urdume+Tela.**
- **M1 — declare (write).** Authenticated tenants author/edit an
  `InfrastructureTemplate` `spec` through the UI → committed to the GitOps source
  the operator watches → reconciled. (Declare-via-UI = a typed mutation that emits
  a commit, never a direct apply.)
- **M2 — multi-tenancy isolation.** One `PangeaNamespace` per tenant; `cracha`
  `AccessPolicy` scopes every query/mutation; row-level isolation on
  `pangea_meta.artifacts`. The X-Product/tenant axis threads auth + observability.
- **M3 — self-service + billing-shaped promessas.** Tenant onboarding flow
  (varanda-style portal), per-tenant `(defpromessa)` SLAs/cost-budgets, the
  OutcomeChain receipts as the tenant's audit trail.
- **M4 — the typed authoring surface.** Tenants compose typed
  `Pangea::Architectures::*` (not raw resources); the Tela `(defwidget)` forms
  generate the template-editor UI from the CRD schema.

## What this dogfoods (the doctrines under load)

- **Urdume**: the operator becomes a tier-honest reference — it already proves
  L6 (versioned chart + CRD + magma atomic apply) and L0 (everything in
  Postgres); the SaaS adds L4 (BFF/auth edge) + L8 (saguao multi-tenancy).
- **Tela**: `pangea-web`'s Yew→Leptos rewrite is the first Leptos migration off
  the backlog; the console consumes the operator GraphQL SDL (the F5 seam), is
  saguao-gated (F7, fixing the localStorage regression in a fresh build), and
  deploys via cargo-leptos+Nix (F9) at `pangea.quero.cloud`.

**Canonical specs:** [`URDUME.md`](https://github.com/pleme-io/theory/blob/main/URDUME.md)
+ [`TELA.md`](https://github.com/pleme-io/theory/blob/main/TELA.md). **Deploy
precedent:** `k8s/clusters/rio/RUNBOOK-observability-portal.md` (grafana.quero.cloud).
**Operator rule:** declare + observe only (org ★★ PLATFORM-MEDIATED).
