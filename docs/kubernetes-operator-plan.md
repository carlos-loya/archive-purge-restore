# APR Kubernetes-Native Pivot — Design Document

**Status:** Draft
**Date:** 2026-05-06
**Owner:** Carlos Loya

## Goals

- Reposition APR from a standalone CLI to a **Kubernetes-native data platform component** for archiving database rows to object storage at scale.
- Allow operators to declaratively manage many databases, storage backends, and archive schedules via Custom Resources (`kubectl apply`).
- Preserve the existing CLI as a first-class local tool (development, ad-hoc operations, environments without Kubernetes).
- Deliver Milestone 1 as a **complete end-to-end product**: install → declare resources → archives run → restore on demand.

## Non-goals (Milestone 1)

- Not building a custom in-controller scheduler — Milestone 1 reconciles `ArchiveRule` into a stock `CronJob`. Controller-driven scheduling is deferred (see Future Work).
- Not building admission webhooks. Milestone 1 relies on OpenAPI/CEL validation in the CRD schema.
- Not building Prometheus metrics or tracing. Logs and Kubernetes Events are the only observability in M1.
- Not publishing an OperatorHub/OLM bundle.
- Not supporting multi-tenant isolation patterns beyond standard namespace-scoped RBAC.

## Background

Today APR is a Go CLI/daemon with a YAML config file describing rules. Archive logic lives in `internal/engine`, behind clean `database.Provider` and `storage.Provider` interfaces. The orchestration is already pluggable; what's missing is a Kubernetes-native control plane.

The pivot reuses `internal/engine` *unchanged* and adds an operator on top. Job pods invoke the same engine code via the existing `apr` binary — the controller's responsibility is purely translating CRDs into Kubernetes workloads and reflecting status back.

## Architecture

### Repository layout

```
cmd/
  apr/main.go                  # existing CLI (archive, restore, history, validate, daemon)
  manager/main.go              # NEW — controller-runtime entrypoint
api/
  v1alpha1/
    groupversion_info.go
    databaseconnection_types.go
    storagebackend_types.go
    archiverule_types.go
    restorerequest_types.go
    zz_generated_deepcopy.go   # generated
internal/
  engine/                      # UNCHANGED — pure archive/restore logic, no K8s deps
  config/                      # UNCHANGED — YAML config for CLI mode
  controller/
    archiverule_controller.go
    restorerequest_controller.go
    common.go                  # CR → engine config translation
  history/                     # extended: HistorySink interface, SQLite + K8s implementations
config/                        # kustomize manifests
  crd/                         # generated CRD YAMLs
  rbac/
  manager/
  samples/
charts/apr/                    # Helm chart wrapping the kustomize manifests
docs/
  kubernetes-operator-plan.md  # this document
  install.md                   # user-facing install guide (M1 deliverable)
  examples/                    # sample CRs (M1 deliverable)
```

### Engine isolation contract

The single most important architectural rule:

> `internal/engine` and `internal/format` MUST NOT import any Kubernetes packages (`k8s.io/...`, `sigs.k8s.io/...`).

The engine takes plain Go structs and runs. Both the CLI and controller-spawned Job pods construct those structs — the CLI from YAML, the Job from environment variables and mounted ConfigMaps populated by the controller from CRs. If we ever want to embed APR in another runtime, this discipline is what makes that possible.

To enforce this, M1 will add a `go vet`-style check (or just a `make lint` rule using `go list -deps`) that fails CI if engine packages depend on K8s libraries.

### Custom Resource Definitions

All resources live in `apr.dev/v1alpha1`. The four CRDs are designed to be small and composable.

#### `DatabaseConnection` (namespace-scoped)

A reusable handle to a database. Many `ArchiveRule`s reference the same connection.

```yaml
apiVersion: apr.dev/v1alpha1
kind: DatabaseConnection
metadata:
  name: orders-prod
  namespace: data-platform
spec:
  engine: postgres                   # postgres | mysql | timescaledb
  host: orders.svc.cluster.local
  port: 5432
  database: orders
  sslMode: require                   # postgres/timescaledb only
  credentialsSecretRef:
    name: orders-prod-credentials    # must contain keys: username, password
status:
  conditions:
    - type: Ready
      status: "True"
      reason: ConnectionVerified
      lastTransitionTime: ...
```

The controller's reconciliation for `DatabaseConnection` is read-only: it spawns a short-lived probe Job (or in-process `pgx`/`mysql` ping) to validate connectivity and reports `Ready`.

#### `StorageBackend` (namespace-scoped)

```yaml
apiVersion: apr.dev/v1alpha1
kind: StorageBackend
metadata:
  name: archive-bucket
  namespace: data-platform
spec:
  type: s3                           # filesystem | s3 | r2 | gcs
  bucket: company-archive
  region: us-west-2
  prefix: apr/                       # optional; prepended to all object keys
  credentialsSecretRef:
    name: archive-bucket-credentials
status:
  conditions:
    - type: Ready
      status: "True"
```

#### `ArchiveRule` (namespace-scoped)

```yaml
apiVersion: apr.dev/v1alpha1
kind: ArchiveRule
metadata:
  name: orders-archive
  namespace: data-platform
spec:
  databaseRef:
    name: orders-prod
  storageRef:
    name: archive-bucket
  table: orders
  dateColumn: created_at
  daysOnline: 90
  schedule: "0 2 * * *"              # standard cron
  batchSize: 10000                   # optional, defaults to engine default
  suspend: false                     # mirrors CronJob.spec.suspend
status:
  cronJobRef:
    name: archiverule-orders-archive
  lastRunTime: 2026-05-06T02:00:00Z
  lastRunResult: Succeeded           # Succeeded | Failed | Running
  lastRunRowsArchived: 14823
  lastRunID: 9f3a2b1c
  nextScheduledTime: 2026-05-07T02:00:00Z
  conditions:
    - type: Ready
      status: "True"
```

The controller reconciles this into a `CronJob` named `archiverule-{name}` in the same namespace. The Job pod runs `apr archive` against a generated config materialized from the CR plus its referenced `DatabaseConnection` and `StorageBackend`. Credentials are projected as env vars from the referenced Secrets.

#### `RestoreRequest` (namespace-scoped, one-shot)

```yaml
apiVersion: apr.dev/v1alpha1
kind: RestoreRequest
metadata:
  name: restore-orders-2026-04-01
  namespace: data-platform
spec:
  archiveRuleRef:
    name: orders-archive
  date: "2026-04-01"                 # optional filter
  runID: 9f3a2b1c                    # optional filter
status:
  phase: Succeeded                   # Pending | Running | Succeeded | Failed
  jobRef:
    name: restorerequest-restore-orders-2026-04-01
  rowsRestored: 14823
  startTime: ...
  completionTime: ...
```

The controller reconciles `RestoreRequest` into a one-shot `Job`. After `Job` completion the controller updates `status.phase` and is done — restore requests are immutable; to re-run, create a new CR.

### Execution model

```
   ┌────────────────────┐
   │  ArchiveRule (CR)  │
   └─────────┬──────────┘
             │ reconcile
             ▼
   ┌────────────────────┐    ┌──────────────────────────┐
   │   apr-manager      │───▶│  CronJob (kube-native)   │
   │  (controller pod)  │    │  schedule: 0 2 * * *     │
   └─────────┬──────────┘    └──────────┬───────────────┘
             │                          │ kubelet starts
             │ updates                  ▼
             │ status         ┌──────────────────────────┐
             └────────────────│  Pod: apr archive ...    │
                              │  reads CRs via k8s API   │
                              │  invokes engine package  │
                              │  emits Events + status   │
                              └──────────────────────────┘
```

The Job pod runs the existing `apr` binary with a new subcommand variant: `apr archive --from-cr <namespace>/<name>`. That subcommand:

1. Reads the `ArchiveRule` and its referenced `DatabaseConnection` + `StorageBackend` from the API server using the in-cluster ServiceAccount.
2. Reads credentials from the env vars projected by the Job spec (the controller wires those when constructing the Job).
3. Builds an in-memory `engine.Config` and invokes `engine.RunArchive` exactly as today.
4. On completion, patches `ArchiveRule.status` and emits a Kubernetes Event.

The CLI path (`apr archive --rule <name>` reading YAML config) remains untouched — it's a separate code path invoked when not running from a CR.

### Persistence (dual-path history)

The current `internal/history` package writes SQLite. We refactor it to a `HistorySink` interface with two implementations:

```go
type HistorySink interface {
    Record(ctx context.Context, run RunRecord) error
}
```

- `SQLiteSink` — current behavior, used by the CLI.
- `KubernetesSink` — patches the relevant CR's status and emits an `Event`. Used when the binary is invoked with `--from-cr`.

The engine constructs a `RunRecord` and writes it to whichever sink the caller injected. The engine itself never knows which path it's on.

## Milestone 1 — End-to-end MVP

### Deliverables

Functional scope:

- `DatabaseConnection`, `StorageBackend`, `ArchiveRule`, `RestoreRequest` CRDs (v1alpha1).
- Manager binary (`cmd/manager`) using `controller-runtime`, with reconcilers for all four CRDs.
- `apr archive --from-cr` and `apr restore --from-cr` subcommands.
- `KubernetesSink` history implementation; engine refactored to take a `HistorySink`.
- Helm chart at `charts/apr/` that installs CRDs, RBAC, and the manager Deployment.
- Sample CRs at `docs/examples/` demonstrating Postgres + S3 and MySQL + filesystem.
- Install + quickstart docs at `docs/install.md`.

Engineering scope:

- envtest-based unit tests for each reconciler (state transitions, idempotency, status updates).
- A kind-based end-to-end test in `integration/` (build tag `k8s`) that:
    1. Stands up kind with Postgres + MinIO sidecars.
    2. Installs the chart.
    3. Applies CRs.
    4. Manually triggers the CronJob (`kubectl create job --from=cronjob/...`).
    5. Asserts rows are archived to MinIO and `ArchiveRule.status` reflects the run.
    6. Applies a `RestoreRequest`, asserts rows return.
- CI job that runs the kind test on every PR.

### Acceptance criteria

A user — given a Kubernetes cluster, a Postgres database, and an S3 bucket — can:

1. `helm install apr charts/apr` and see the manager pod come up.
2. `kubectl apply -f` a Secret + `DatabaseConnection` + `StorageBackend` + `ArchiveRule`.
3. See `kubectl get archiverule` show `READY=True` and a `NEXT-RUN` time.
4. Wait for (or manually trigger) the CronJob and observe rows moved from Postgres to S3.
5. `kubectl get archiverule` shows `LAST-RESULT=Succeeded` and `ROWS-ARCHIVED`.
6. `kubectl apply` a `RestoreRequest` and observe rows restored to the database.

The CLI continues to work exactly as it does today for users running outside Kubernetes.

## Testing strategy

### Layer 1 — Unit tests (fast, in-process)

- Existing engine/provider unit tests run unchanged.
- New: controller reconciler tests using `sigs.k8s.io/controller-runtime/pkg/envtest`. Spins up a real `kube-apiserver` + `etcd` from the kubebuilder asset bundle (no Docker, no kubelet) and asserts reconcile loops produce the expected `CronJob`/`Job` objects and status transitions.
- Run with `make test`.

### Layer 2 — Local cluster tests (`kind`)

`kind` (Kubernetes-in-Docker) gives us a real, conformant cluster on a developer laptop in ~30 seconds.

```
make kind-up         # creates kind cluster, deploys Postgres + MinIO via Helm
make kind-install    # builds operator image, loads into kind, helm-installs APR
make test-k8s        # runs integration/k8s_test.go against the kind cluster
make kind-down
```

Inside `integration/k8s_test.go` (build tag `k8s`):
- Apply CRs via the typed client.
- Use `kubectl create job --from=cronjob/...` to trigger an immediate run rather than waiting on cron.
- Poll the resulting `ArchiveRule.status` for completion.
- Verify Parquet files exist in MinIO via the storage provider.
- Apply a `RestoreRequest`, poll for `Succeeded`, verify row counts.

This is the test we run in CI on every PR.

### Layer 3 — Real cluster smoke (manual, pre-release)

Before tagging a release, run the same CR examples on a real managed cluster (GKE Autopilot or DigitalOcean Kubernetes — both have low-cost paths) against a real managed Postgres (Cloud SQL or DO Managed DB) and a real S3 bucket. Document the procedure in `docs/release-checklist.md`.

The goal here is catching things kind hides: real IAM/IRSA flows, real cloud DNS, real LoadBalancer egress, real S3 latencies. Not run in CI; gated on a release.

## Future work (tracked as GitHub issues)

The following items are explicitly out of M1 and will each have a dedicated issue:

1. **Controller-driven scheduling** — replace `CronJob` with an in-controller scheduler for pause/resume, dynamic backoff, and centralized run history.
2. **Prometheus metrics + Grafana dashboard** — `/metrics` endpoint on the manager exposing reconcile counts, run durations, rows archived, errors.
3. **Validating admission webhooks** — reject CRs referencing missing Secrets or invalid cron expressions before they hit the reconciler.
4. **Status conditions API alignment** — adopt `metav1.Condition` patterns and `kubectl wait`-friendly condition types (`Ready`, `Progressing`, `Degraded`).
5. **OpenTelemetry tracing** — propagate trace context from controller → Job pod → engine.
6. **OperatorHub / OLM bundle** — package APR for OperatorHub.io distribution.
7. **CEL validation rules in CRDs** — schema-level validation (e.g., `daysOnline > 0`, valid cron format).
8. **Cluster-scoped operator mode** — option to install one operator across all namespaces vs per-namespace.

## Open questions

- **CR group/domain.** Tentatively `apr.dev`. If we want to publish to OperatorHub later, we need a domain we control.
- **Status writeback from Job pods.** Two options: (a) Job pod patches CR status directly (needs RBAC), (b) Job writes to a known location and the controller reconciles. Option (a) is simpler; M1 will use it. Revisit if RBAC concerns arise.
- **Backfill for existing CLI users.** Should we provide an `apr config-to-crs` command that converts an existing `apr.yaml` into equivalent CR manifests? Useful for the portfolio narrative; size unknown. Decide after M1 lands.
