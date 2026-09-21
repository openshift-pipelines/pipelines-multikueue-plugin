# Pipelines MultiKueue Plugin

Pipelines MultiKueue Plugin configures an OpenShift hub and its managed clusters so [Kueue](https://kueue.sigs.k8s.io/) can dispatch [Tekton PipelineRuns](https://tekton.dev/docs/pipelines/pipelineruns/) across clusters.

The controller is intended to run on an Open Cluster Management hub. It watches non-local `ManagedCluster` resources and connects each eligible cluster to Kueue MultiKueue.

## What the controller does

On the hub, the controller:

- creates OLM subscriptions for Red Hat OpenShift Pipelines, the Red Hat build of Kueue Operator, and cert-manager Operator for Red Hat OpenShift;
- configures `tekton.dev/v1` `PipelineRun` as a Kueue external framework;
- creates the default resource flavor, cluster queue, local queue, admission check, and MultiKueue configuration; and
- creates a `MultiKueueCluster` for every eligible managed cluster.

For each managed cluster, it:

- creates a `ManagedServiceAccount` and the RBAC needed to access the cluster;
- installs and configures the required operators through the managed-cluster credentials; and
- maintains the kubeconfig Secret used by MultiKueue.

The local cluster is skipped when its `ManagedCluster` has the label `local-cluster=true`.

## Prerequisites

- An OpenShift cluster acting as an Open Cluster Management hub
- One or more registered `ManagedCluster` resources
- The Open Cluster Management `ManifestWork` API
- The Managed Service Account add-on and API
- OLM access to the `redhat-operators` catalog on the hub and managed clusters
- Cluster-admin access for deployment
- Go 1.26.5 for local development
- Docker or Podman and an image registry for building and deploying a custom image

> [!IMPORTANT]
> The controller creates cluster-scoped Kueue resources and installs operators on the hub and managed clusters. Review the defaults below before deploying it.

## Build and test

```sh
git clone https://github.com/openshift-pipelines/pipelines-multikueue-plugin.git
cd pipelines-multikueue-plugin

go test ./...
mkdir -p bin
go build -o bin/controller ./cmd
```

Build and push the controller image:

```sh
export IMG=quay.io/<account>/pipelines-multikueue-controller:dev
make docker-build IMG="$IMG"
make docker-push IMG="$IMG"
```

## Deploy

Point `KUBECONFIG` at the hub cluster, generate a release manifest for your image, and apply it:

```sh
export IMG=quay.io/<account>/pipelines-multikueue-controller:dev

kubectl create namespace tekton-kueue \
  --dry-run=client -o yaml | kubectl apply -f -

make release IMG="$IMG" VERSION=dev
kubectl apply --server-side -f release/release-dev.yaml
kubectl rollout status \
  deployment/pipelines-multikueue-plugin-controller \
  -n tekton-kueue \
  --timeout=5m
```

The `release` target updates the image in `config/kustomization.yaml` and writes the rendered manifest to `release/release-<version>.yaml`.

Verify the generated MultiKueue resources:

```sh
kubectl get multikueueclusters.kueue.x-k8s.io
kubectl get multikueueconfigs.kueue.x-k8s.io pipelines-multikueue-config
kubectl get clusterqueues.kueue.x-k8s.io pipelines-multicluster-kueue
kubectl get admissionchecks.kueue.x-k8s.io pipelines-multikueue-ac
kubectl get localqueues.kueue.x-k8s.io pipelines-kueue -n default
```

## Submit a PipelineRun

The default `LocalQueue` is created in the `default` namespace. Add its queue label to a `PipelineRun` in that namespace:

```yaml
apiVersion: tekton.dev/v1
kind: PipelineRun
metadata:
  generateName: multikueue-
  namespace: default
  labels:
    kueue.x-k8s.io/queue-name: pipelines-kueue
spec:
  # Add pipelineRef or pipelineSpec here.
```

Create a `LocalQueue` pointing to `pipelines-multicluster-kueue` before submitting queued `PipelineRuns` in another namespace.

## Defaults

| Setting | Default |
| --- | --- |
| OpenShift Pipelines channel | `pipelines-1.22` |
| Kueue Operator channel | `stable-v1.4` |
| cert-manager Operator channel | `stable-v1` |
| Kueue namespace | `openshift-kueue-operator` |
| Resource flavor | `default-flavor` |
| Cluster queue | `pipelines-multicluster-kueue` |
| Local queue | `pipelines-kueue` in `default` |
| Admission check | `pipelines-multikueue-ac` |
| MultiKueue config | `pipelines-multikueue-config` |
| PipelineRun quota | `100` |

Set `KUEUE_NAMESPACE` on the controller Deployment to use a different namespace for MultiKueue kubeconfig Secrets.

## Development

Useful targets are listed by:

```sh
make help
```

Run formatting and static checks with:

```sh
make fmt
make vet
```

## License

Licensed under the [Apache License 2.0](LICENSE).
