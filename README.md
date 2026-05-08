# APR — Archive, Purge, Restore

**Kubernetes-native database row archival.** APR is a Kubernetes operator
(with a CLI for non-K8s use) that archives old database rows to cheap
object storage as Parquet files, deletes them from the source, and brings
them back on demand.

```yaml
apiVersion: apr.dev/v1alpha1
kind: ArchiveRule
metadata: { name: orders-archive, namespace: data }
spec:
  databaseRef: { name: orders-db }
  storageRef:  { name: archive-bucket }
  table: orders
  dateColumn: created_at
  daysOnline: 90
  schedule: "0 2 * * *"
```

```text
$ kubectl get archiverule orders-archive
NAME             TABLE    SCHEDULE    DAYS-ONLINE   LAST-RESULT   ROWS-ARCHIVED   NEXT-RUN
orders-archive   orders   0 2 * * *   90            Succeeded     14823           2026-05-09T02:00:00Z
```

## Why APR?

- **Declarative** — define your archive policy once in YAML; the operator enforces it forever
- **Kubernetes-native** — CRDs, in-controller scheduling, validating admission webhooks, Prometheus metrics, one `helm install`
- **Parquet output** — columnar and compressed, queryable directly by Spark, DuckDB, pandas, Athena, BigQuery
- **Zero-downtime** — two-phase archive guarantees no data loss (delete only after successful upload)
- **Multi-database** — PostgreSQL, MySQL, TimescaleDB (with chunk-aware `drop_chunks()`)
- **Multi-storage** — AWS S3, Google Cloud Storage, Cloudflare R2, local filesystem
- **CLI fallback** — same engine, same algorithm, runs without Kubernetes

## Supported databases & storage

| Databases | Storage backends |
|---|---|
| PostgreSQL | AWS S3 |
| MySQL | Google Cloud Storage |
| TimescaleDB | Cloudflare R2 |
|  | Local filesystem |

## Install (Kubernetes)

```bash
helm install apr ./charts/apr \
  --namespace apr-system \
  --create-namespace \
  --set webhooks.enabled=true     # requires cert-manager in-cluster
```

Then declare your data plane:

```yaml
# Reusable handles — referenced by ArchiveRules and RestoreRequests.
apiVersion: apr.dev/v1alpha1
kind: DatabaseConnection
metadata: { name: orders-db, namespace: data }
spec:
  engine: postgres
  host: orders.svc.cluster.local
  port: 5432
  database: orders
  credentialsSecretRef: { name: orders-creds }
---
apiVersion: apr.dev/v1alpha1
kind: StorageBackend
metadata: { name: archive-bucket, namespace: data }
spec:
  type: s3
  bucket: company-archive
  region: us-west-2
  credentialsSecretRef: { name: archive-creds }
---
# Recurring archive on a cron schedule.
apiVersion: apr.dev/v1alpha1
kind: ArchiveRule
metadata: { name: orders-archive, namespace: data }
spec:
  databaseRef: { name: orders-db }
  storageRef:  { name: archive-bucket }
  table: orders
  dateColumn: created_at
  daysOnline: 90
  schedule: "0 2 * * *"
```

`kubectl apply -f ...` and the operator takes over: it reconciles each
rule, spawns archive `Job`s on the configured cron schedule (no
`CronJob` indirection — the controller owns the cron loop itself),
writes results back to `status`, and exposes Prometheus metrics.

**Trigger an immediate run** without waiting for the next schedule:

```bash
kubectl annotate archiverule orders-archive \
  apr.dev/trigger-time=$(date -u +%Y-%m-%dT%H:%M:%SZ)
```

**Restore** archived rows back to the database:

```yaml
apiVersion: apr.dev/v1alpha1
kind: RestoreRequest
metadata: { name: restore-2024-06-15, namespace: data }
spec:
  archiveRuleRef: { name: orders-archive }
  date: "2024-06-15"
```

Full walkthrough: [docs/install.md](docs/install.md). Sample CRs:
[docs/examples/](docs/examples/). Architecture rationale:
[docs/kubernetes-operator-plan.md](docs/kubernetes-operator-plan.md).

## Custom resources

| Kind | What it is |
|---|---|
| `DatabaseConnection` | Reusable handle to a database; many rules can reference one DBC |
| `StorageBackend` | Reusable handle to an object-storage destination |
| `ArchiveRule` | Declarative recurring archive: table + cron + DBC + SB |
| `RestoreRequest` | One-shot restore from an existing archive (immutable spec) |

## Validating webhooks

When `webhooks.enabled=true` (requires cert-manager), the operator
rejects misconfigured CRs at `kubectl apply` time with field-pathed
errors:

```text
$ kubectl apply -f bad-rule.yaml
Error from server (Forbidden): admission webhook "varchiverule.apr.dev" denied the request:
  [spec.schedule: Invalid value: "garbage": invalid cron expression: expected exactly 5 fields,
   spec.databaseRef.name: Not found: "missing-dbc"]
```

## Observability

The manager pod exposes a `/metrics` endpoint with both standard
controller-runtime metrics (`controller_runtime_*`, `workqueue_*`,
`rest_client_*`) and APR-specific collectors:

| Metric | Type |
|---|---|
| `apr_archive_runs_total{rule, result}` | counter |
| `apr_archive_run_duration_seconds{rule}` | histogram |
| `apr_archive_rows_total{rule}` | counter |
| `apr_restore_runs_total{rule, result}` | counter |
| `apr_restore_run_duration_seconds{rule}` | histogram |
| `apr_restore_rows_total{rule}` | counter |
| `apr_storage_operation_duration_seconds{type, operation, result}` | histogram |

Set `prometheus.enabled=true` to ship a `ServiceMonitor` for the
Prometheus Operator. A starter Grafana dashboard lives at
[`charts/apr/dashboards/apr-overview.json`](charts/apr/dashboards/apr-overview.json).

## CLI mode (non-Kubernetes use)

The same `apr` binary runs against a YAML config when Kubernetes isn't
available:

```bash
go install github.com/carlos-loya/archive-purge-restore/cmd/apr@latest

apr archive prod-orders                                      # one-off
apr daemon                                                   # built-in scheduler
apr restore --rule prod-orders --date 2024-06-15
apr history --rule prod-orders
```

See [`apr.yaml.example`](apr.yaml.example) for the YAML config schema.

## How it works

1. **Extract** — query rows older than `daysOnline` in batches; write each batch to a `.pending` Parquet file
2. **Delete** — remove archived rows from the source database by primary key
3. **Finalize** — atomically rename `.pending` → final path (data is committed only after successful deletion)
4. **On failure** — all `.pending` files are cleaned up; no rows are deleted from the source

Archives are stored at `{database}/{table}/{date}/{runID}_{batch}.parquet`.

## Development

```bash
make build              # CLI binary           → ./apr
make build-manager      # Operator binary      → ./apr-manager
make test               # Unit tests
make lint               # go vet
make test-envtest       # Reconciler envtest suite (real apiserver, no cluster)
make test-integration   # Engine integration against Docker Postgres + MySQL + TimescaleDB
make test-k8s-clean     # Full kind end-to-end: cert-manager + Postgres + MinIO + chart + assertions
```

See [CONTRIBUTING.md](CONTRIBUTING.md) for the full development
workflow including adding new validators, metrics, reconcilers, database
engines, and storage backends.

## License

MIT
