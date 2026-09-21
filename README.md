# Pipelines MultiKueue Plugin

A Kubernetes controller that wires [Kueue MultiKueue](https://kueue.sigs.k8s.io/docs/concepts/multikueue/)
up for Tekton `PipelineRuns` across an [Open Cluster Management](https://open-cluster-management.io/) (OCM/ACM)
fleet.

It runs on the **hub** cluster. For every `ManagedCluster` it discovers, it provisions the credentials,
RBAC, operators and Kueue objects needed for the hub to dispatch `PipelineRun`s onto that **managed**
(spoke) cluster — so a `PipelineRun` submitted to a local queue on the hub is admitted and executed on
a remote cluster with spare capacity.

## How it works

The binary (`cmd/main.go`) starts a controller-runtime manager with two pieces.

### 1. Hub bootstrap (`internal/reconcilers/operators.go`)

`ClusterBootstrap` runs once when the manager starts and makes the hub cluster MultiKueue-ready:

| Step | What it creates                                                                                                                                                                                                           |
| --- |---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Operators | OLM `Subscriptions` (and their `OperatorGroup`/namespace) for `openshift-pipelines-operator-rh`, `kueue-operator` and `openshift-cert-manager-operator`, then blocks until each reports an `InstalledCSV` (30 min timeout) |
| Kueue CR | `Kueue/cluster` with `tekton.dev/pipelineruns` registered as an external framework, both under `integrations` and `multiKueue`                                                                                            |
| ResourceFlavor | Create a Resource flavor named `default-flavor` on both Hub and Managed Clusters                                                                                                                                          |
| ClusterQueue | Creates `pipelines-multicluster-kueue` on Hub cluster, with a nominal quota of `100` `tekton.dev/pipelineruns` and the admission check attached                                                                           |
| LocalQueue | Creates `pipelines-kueue` in the `default` namespace, pointing at the ClusterQueue above on Hub cluster                                                                                                                   |
| AdmissionCheck | Create `pipelines-multikueue-ac`, controller `kueue.x-k8s.io/multikueue`, parameterised by `MultiKueueConfig/pipelines-multikueue-config`                                                                                 |

### 2. Per-cluster reconciler (`internal/reconcilers/reconcilers.go`)

`MultiKueueReconciler` watches `ManagedCluster` (plus `ManagedServiceAccount`s and their token `Secret`s,
mapped back to the owning cluster). Clusters labelled `local-cluster=true` are skipped. For every other
cluster it:

1. **Creates a `ManagedServiceAccount`** named `pipelines-multikueue` in the cluster's hub namespace
   (24 h token rotation, 24 h TTL). If the token `Secret` isn't there yet, it requeues after 10s.
2. **Builds a kubeconfig `Secret`** from the token + CA + the cluster's API server URL, stored in the
   Kueue namespace under the managed cluster's name — this is what MultiKueue consumes.
3. **Pushes a `ManifestWork`** (`multikueue-bootstrap`) carrying the `multikueue` `ClusterRole` and
   `ClusterRoleBinding` (`internal/manifest/manifests/`) to the spoke. ACM does not hand out spoke
   kubeconfigs, so this RBAC is what makes the ManagedServiceAccount token useful.
4. **Bootstraps the spoke** — connects with that token and runs the same operator + Kueue CR install as
   step 1 above, remotely.
5. **Registers the cluster with Kueue** — creates a `MultiKueueCluster` referencing the kubeconfig
   Secret and appends the cluster to `MultiKueueConfig/pipelines-multikueue-config`.

## Prerequisites

- An OpenShift hub cluster with ACM / OCM installed (`ManagedCluster`, `ManifestWork` and
  `ManagedServiceAccount` APIs available).
- OLM with the `redhat-operators` catalog source in `openshift-marketplace` — the controller installs
  operators via `Subscription`s from it.
- One or more registered `ManagedCluster`s.
- Go 1.26+ and a container tool (`docker` or `podman`) to build from source.

## Getting started

Everything is driven from the `Makefile`; `make help` lists all targets.

### Run against a cluster from your laptop

```sh
make run          # uses your current kubeconfig context
```

### Build and push the image

```sh
make docker-build docker-push IMG=quay.io/you/pipelines-multikueue-controller:dev
```

`make build` does the same via `docker buildx` for the platforms in `PLATFORMS` (default `linux/amd64`).

### Deploy

Render a single manifest with kustomize and apply it:

```sh
make release IMG=quay.io/you/pipelines-multikueue-controller:dev VERSION=dev
kubectl apply --server-side -f release/release-dev.yaml
```

`make apply` combines the build, render and apply steps and waits for the deployment to become
available. The controller is deployed into the `tekton-kueue` namespace with the
`pipelines-multikueue-plugin-` name prefix (see `config/kustomization.yaml`).

## Configuration

| Environment variable | Default | Effect |
| --- | --- | --- |
| `KUEUE_NAMESPACE` | `openshift-kueue-operator` | Namespace the generated MultiKueue kubeconfig Secrets are written to |

Health and readiness probes are served on `:8081` (`/healthz`, `/readyz`). Logging uses the
controller-runtime zap flags (`--zap-log-level`, `--zap-encoder`, …).

`internal/common/value.go` also defines `CLUSTER_PROXY_URL`, `CLUSTER_PROXY_IMPERSONATION_ENABLED` and
`ENABLE_CLUSTERPROFILE` for planned cluster-proxy and `ClusterProfile` support; they are not read by
the current reconcilers.

## Development

```sh
make fmt vet        # format and vet
make lint           # golangci-lint
make manifests      # regenerate RBAC from //+kubebuilder:rbac markers
make generate       # regenerate deepcopy code
make test           # unit tests against envtest
make test-e2e       # e2e tests, requires a running Kind cluster
```

RBAC for the controller is generated from the `//+kubebuilder:rbac` markers spread through
`internal/reconcilers/`; after changing them, run `make manifests` and re-render the release manifest.

Tooling (`kustomize`, `controller-gen`, `setup-envtest`, `golangci-lint`) is downloaded into `bin/` on
demand at the versions pinned near the bottom of the `Makefile`.

### Layout

```
cmd/main.go                    manager wiring and entrypoint
internal/reconcilers/          ClusterBootstrap + MultiKueueReconciler
  operators.go                 OLM subscription / namespace / operator-group handling
  kueue.go                     Kueue CR, flavors, queues, admission checks, MultiKueue objects
  manifestwork.go              ManifestWork that seeds RBAC on the spoke
internal/manifest/             embedded YAML shipped to managed clusters
internal/common/               env-var helpers and shared names
config/                        kustomize base: deployment, service account, RBAC
release/                       rendered install manifests
hack/                          release and dependency-inspection scripts
```

## License

Apache 2.0 — see [LICENSE](LICENSE).
