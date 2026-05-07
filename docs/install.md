# Installing APR on Kubernetes

This guide walks through installing the APR operator with Helm, applying a
sample `ArchiveRule`, and triggering an archive run.

## Prerequisites

- Kubernetes 1.28 or newer.
- A working `kubectl` and `helm` (v3+).
- An OCI image of APR available to your cluster's container runtime. To
  build it locally:

  ```bash
  make docker-build IMG=apr:dev
  # If using kind:
  kind load docker-image apr:dev
  ```

## Install

```bash
helm install apr ./charts/apr \
  --namespace apr-system \
  --create-namespace \
  --set image.repository=apr \
  --set image.tag=dev
```

This installs:

- The four CRDs (`ArchiveRule`, `RestoreRequest`, `DatabaseConnection`,
  `StorageBackend`) from the chart's `crds/` directory.
- A manager Deployment in `apr-system`.
- Two ServiceAccounts — `apr-manager` (the operator) and `apr-runner`
  (each archive/restore Job pod runs as this).
- ClusterRoles + ClusterRoleBindings for both.
- A `ClusterIP` Service exposing `/metrics` on port 8080.

Verify the manager pod is healthy:

```bash
kubectl -n apr-system get deploy apr-manager
kubectl -n apr-system get pods
kubectl -n apr-system logs deploy/apr-manager
```

## Apply your first ArchiveRule

Create a namespace for your data plane resources:

```bash
kubectl create namespace data
```

Apply a sample (assumes you have an in-cluster Postgres at
`postgres.data.svc.cluster.local`; see `docs/examples/` for variants):

```bash
kubectl apply -f docs/examples/postgres-filesystem.yaml
```

Verify the operator picked them up:

```bash
$ kubectl -n data get databaseconnection,storagebackend,archiverule
NAME                                  ENGINE     HOST                                      DATABASE   READY   AGE
databaseconnection.apr.dev/orders-db  postgres   postgres.data.svc.cluster.local           orders     True    5s

NAME                                STATUS   TYPE         BUCKET           READY   AGE
storagebackend.apr.dev/archive-pvc           filesystem   /var/archives    True    5s

NAME                                    TABLE    SCHEDULE    DAYS-ONLINE   LAST-RESULT   ROWS-ARCHIVED   NEXT-RUN   AGE
archiverule.apr.dev/orders-archive      orders   0 2 * * *   90                                          ...        5s
```

If `READY=True` doesn't appear, inspect the resource for the failure
reason:

```bash
kubectl -n data describe archiverule orders-archive
```

The operator creates a `CronJob` named `archiverule-orders-archive` in the
same namespace. To trigger an immediate run rather than waiting for the
schedule to fire:

```bash
kubectl -n data create job orders-archive-manual \
  --from=cronjob/archiverule-orders-archive
```

Watch the resulting pod:

```bash
kubectl -n data get jobs,pods
kubectl -n data logs -l app.kubernetes.io/name=apr,apr.dev/archive-rule=orders-archive
```

After the Job completes, the rule's status reflects the run:

```bash
kubectl -n data get archiverule orders-archive
# NAME              ...  LAST-RESULT  ROWS-ARCHIVED  ...
# orders-archive    ...  Succeeded    14823          ...
```

## Restore on demand

Apply a `RestoreRequest`. Each one is single-use:

```bash
kubectl apply -f - <<EOF
apiVersion: apr.dev/v1alpha1
kind: RestoreRequest
metadata:
  name: restore-orders-2026-04-01
  namespace: data
spec:
  archiveRuleRef:
    name: orders-archive
  date: "2026-04-01"
EOF
```

Check progress:

```bash
kubectl -n data get restorerequest
# NAME                            RULE             DATE         PHASE       ROWS-RESTORED  AGE
# restore-orders-2026-04-01       orders-archive   2026-04-01   Succeeded   14823          30s
```

## Uninstall

```bash
helm uninstall apr --namespace apr-system
kubectl delete namespace apr-system
```

> **Note:** Helm leaves CRDs in place after `helm uninstall` (this is the
> [official Helm convention][helm-crds] — CRDs persist user data). To
> remove the CRDs as well:
>
> ```bash
> kubectl delete crd \
>   archiverules.apr.dev \
>   restorerequests.apr.dev \
>   databaseconnections.apr.dev \
>   storagebackends.apr.dev
> ```
>
> This also deletes every `ArchiveRule`, `RestoreRequest`,
> `DatabaseConnection`, and `StorageBackend` in every namespace.

[helm-crds]: https://helm.sh/docs/topics/charts/#limitations-on-crds

## Upgrading

```bash
helm upgrade apr ./charts/apr --namespace apr-system
```

Helm does **not** update CRDs on upgrade. To pick up CRD changes from a
new chart version, apply them yourself before upgrading:

```bash
kubectl apply -f charts/apr/crds/
helm upgrade apr ./charts/apr --namespace apr-system
```

## Local-only CLI mode

If you want to run APR outside of Kubernetes, the `apr` binary still
supports the original YAML config workflow:

```bash
apr archive --config ./apr.yaml
apr restore --config ./apr.yaml --rule orders --date 2026-04-01
```

See `apr.yaml.example` for the YAML schema.
