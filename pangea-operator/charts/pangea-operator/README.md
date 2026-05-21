# pangea-operator

[![Artifact Hub](https://img.shields.io/endpoint?url=https://artifacthub.io/badge/repository/pangea-operator)](https://artifacthub.io/packages/helm/pangea-operator/pangea-operator)

Pangea is a Rust Kubernetes operator that reconciles four CRDs end-to-end:

- **`ArchitectureGem`** — Pangea Ruby gem registry + smoke gate.
- **`WorkspaceCatalog`** — logical workspace metadata + cascade root.
- **`InfrastructureTemplate`** — one Pangea Ruby template to reconcile (source, variables, per-resource policies, reactive policy, import hints).
- **`PangeaNamespace`** — state-storage isolation boundary (PostgreSQL schema or S3 prefix).

The operator compiles each template through embedded magnus (CRuby in-process), synthesizes Terraform JSON, runs `tofu plan`/`tofu apply`, and emits typed cycle receipts. Declarative reactive policies escalate when things don't reach a good state.

## Install

```bash
helm install pangea oci://ghcr.io/pleme-io/charts/pangea-operator \
  --version 0.1.0 \
  --namespace pangea-system --create-namespace
```

To install a specific version, pin `--version <semver>`.

## Quickstart

After install, create your first `ArchitectureGem` + `InfrastructureTemplate`:

```yaml
apiVersion: pangea.pleme.io/v1
kind: ArchitectureGem
metadata:
  name: pangea-aws
spec:
  source:
    git: https://github.com/pleme-io/pangea-aws
    ref: main
  smokeTest:
    template: aws::vpc::dev
---
apiVersion: pangea.pleme.io/v1
kind: InfrastructureTemplate
metadata:
  name: vpc-dev
  namespace: default
spec:
  templateName: aws::vpc::dev
  requiredGem: pangea-aws
  pangeaNamespace: dev-state
  variables:
    region: us-east-1
    cidr: 10.0.0.0/16
  policy:
    defaultDecision: requireApproval
```

See [`docs/AUTHORING.md`](https://github.com/pleme-io/pangea-operator/blob/main/docs/AUTHORING.md) for practical recipes.

## Values reference

| Key | Default | Description |
|-----|---------|-------------|
| `replicaCount` | `1` | Number of operator pods. Leader election picks the active reconciler. |
| `image.repository` | `ghcr.io/pleme-io/pangea-operator` | OCI image (published by release.yml). |
| `image.tag` | `""` | Defaults to `.Chart.AppVersion`. |
| `image.pullPolicy` | `IfNotPresent` | Standard k8s pull policy. |
| `imagePullSecrets` | `[]` | Verbatim list passed to the pod spec. |
| `nameOverride` | `""` | Override the chart name prefix. |
| `fullnameOverride` | `""` | Fully override generated resource names. |
| `serviceAccount.create` | `true` | Create dedicated SA + RBAC. |
| `serviceAccount.automount` | `true` | Mount SA token automatically. |
| `serviceAccount.name` | `""` | External SA name when `create=false`. |
| `podAnnotations` | Prometheus scrape | Pod-level annotations. |
| `podSecurityContext.runAsNonRoot` | `true` | Hardened defaults. |
| `securityContext.readOnlyRootFilesystem` | `true` | Hardened defaults. |
| `config.logFormat` | `json` | `pretty` (dev) or `json` (prod). |
| `config.logLevel` | `info,pangea_operator=debug` | RUST_LOG filter. |
| `config.healthAddr` | `0.0.0.0:8080` | /healthz + /readyz bind. |
| `config.metricsAddr` | `0.0.0.0:9090` | /metrics (Prometheus) bind. |
| `config.graphqlAddr` | `0.0.0.0:8081` | GraphQL API bind. |
| `config.grpcAddr` | `0.0.0.0:50051` | gRPC API bind. |
| `config.otelEndpoint` | `""` | OTLP/HTTP endpoint; empty disables. |
| `service.type` | `ClusterIP` | Standard k8s service type. |
| `service.ports.{health,metrics,graphql,grpc}` | `8080/9090/8081/50051` | Per-protocol service ports. |
| `resources.limits.{cpu,memory}` | `500m / 512Mi` | Container resource limits. |
| `resources.requests.{cpu,memory}` | `100m / 128Mi` | Container resource requests. |
| `leaderElection.enabled` | `true` | Required when `replicaCount > 1`. |
| `useEmbeddedRuby` | `true` | Evaluate templates in-process (recommended). |
| `compilerSidecar.enabled` | `false` | Legacy HTTP backend; only needed when `useEmbeddedRuby=false`. |
| `compilerSidecar.image.repository` | `ghcr.io/pleme-io/pangea-compiler` | Sidecar image. |
| `compilerSidecar.image.tag` | `latest` | Sidecar tag. |
| `nodeSelector` / `tolerations` / `affinity` | `{}` / `[]` / `{}` | Standard scheduling. |
| `ingress.enabled` | `false` | Ingress for the GraphQL API. |
| `ingress.className` | `nginx` | Ingress class. |
| `ingress.hosts[0].host` | `pangea-api.example.local` | Default hostname (override). |
| `auth.enabled` | `true` | API token authentication. |
| `auth.existingSecret` | `""` | External Secret containing the API token. |
| `auth.secretKey` | `API_TOKEN` | Key inside the Secret. |
| `auth.token` | `""` | Chart-generated Secret value (dev only). |
| `serviceMonitor.enabled` | `false` | Render Prometheus Operator ServiceMonitor. |
| `serviceMonitor.interval` | `30s` | Scrape interval. |
| `installCRDs` | `true` | Install the four Pangea CRDs. Set false when CRDs are managed out-of-band. |

## Upgrading

The chart is at v0.1.0 — first public release. Future minor bumps will document any breaking value/CRD changes in the [CHANGELOG](https://github.com/pleme-io/pangea-operator/blob/main/CHANGELOG.md).

CRDs are installed in-chart for v0.1.0 simplicity; Helm intentionally does not upgrade CRDs by chart upgrade. Update CRDs out-of-band when bumping minor versions:

```bash
helm template pangea oci://ghcr.io/pleme-io/charts/pangea-operator --version <new> \
  | yq 'select(.kind == "CustomResourceDefinition")' \
  | kubectl apply -f -
```

## Uninstall

```bash
helm uninstall pangea --namespace pangea-system
kubectl delete crd \
  architecturegems.pangea.pleme.io \
  workspacecatalogs.pangea.pleme.io \
  infrastructuretemplates.pangea.pleme.io \
  pangeanamespaces.pangea.pleme.io
```

## Links

- Source: <https://github.com/pleme-io/pangea-operator>
- ArtifactHub: <https://artifacthub.io/packages/helm/pangea-operator/pangea-operator>
- Theory: <https://github.com/pleme-io/theory/blob/main/PANGEA-WORKSPACE-RECONCILIATION.md>
- License: Apache-2.0
