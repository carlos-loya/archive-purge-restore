# Contributing to APR

Thanks for your interest in contributing to APR. This guide covers the
development workflow for both the operator (`apr-manager`) and the CLI
(`apr`).

## Development Setup

**Prerequisites:**

- Go 1.25+ (see `go.mod` for the exact version)
- CGO enabled (`CGO_ENABLED=1`, the default) — required by `mattn/go-sqlite3`
  for the CLI's history backend
- Docker — for engine integration tests against real Postgres / MySQL /
  TimescaleDB, and for building the operator container image
- Optional but recommended: `kubectl` and `helm` for chart work; `kind` is
  installed automatically by `make kind-tool`

**Getting started:**

```bash
git clone https://github.com/carlos-loya/archive-purge-restore.git
cd archive-purge-restore
make build               # CLI            → ./apr
make build-manager       # Operator       → ./apr-manager
./apr version
```

## Running Tests

APR has three test layers — unit, envtest (reconciler), and end-to-end
(engine integration + kind).

### Unit tests

```bash
make test                # go test ./... -v
make lint                # go vet ./...
```

### Reconciler envtest suite

Spins up a real `kube-apiserver` + `etcd` from the kubebuilder asset
bundle (no Docker, no kubelet), applies the CRDs, and exercises every
reconciler against typed clients.

```bash
make test-envtest        # Downloads asset binaries on first run
```

The harness lives at `internal/controller/suite_test.go`. Individual
test functions skip cleanly if `KUBEBUILDER_ASSETS` is unset, so the
plain `go test ./...` from `make test` still works without envtest
assets.

### Engine integration (Docker DBs)

```bash
make dev-up              # Postgres 16 + MySQL 8.0 + TimescaleDB
make test-integration    # go test -tags integration ./integration/...
make dev-down            # Tear down containers
```

### kind end-to-end (operator + chart + Postgres + MinIO + cert-manager)

```bash
make test-k8s-clean      # Boots a fresh kind cluster, deploys cert-manager,
                         # the in-cluster data plane, and the Helm chart
                         # (with webhooks + metrics enabled), runs k8s_test.go,
                         # and tears the cluster down. Useful for CI.

# Iterate against a long-lived cluster:
make kind-up
make kind-cert-manager
make kind-data-plane
make kind-load IMG=apr:dev
make kind-install IMG=apr:dev
make test-k8s
```

The integration test (`integration/k8s_test.go`, build tag `k8s`) runs
the full archive + restore loop and asserts:

- `ArchiveRule` reaches `Ready=True` after the operator resolves refs
- triggering via `apr.dev/trigger-time` annotation spawns a Job
- Postgres row count drops by the expected amount
- Parquet objects land in MinIO
- a `RestoreRequest` brings rows back to baseline
- the validating webhook rejects bad CRs at admission time
- the manager's `/metrics` endpoint exposes the APR-specific samples

### Run a single package's tests

```bash
go test ./internal/engine/ -v
go test ./internal/controller/ -run TestArchiveRule -v
go test ./internal/webhook/ -v
```

## Code Conventions

### Engine isolation

`internal/engine`, `internal/format`, and `internal/metrics` MUST NOT
import any `k8s.io/...` or `sigs.k8s.io/...` packages. The operator
path goes through `internal/cluster` (K8s-aware glue); the CLI path
goes through `cmd/apr`. This keeps the engine reusable in non-K8s
contexts and makes refactors of either control plane safe.

### Error handling

Wrap errors with context using `%w`:

```go
if err != nil {
    return fmt.Errorf("archiving table %s: %w", table, err)
}
```

### Identifier quoting

Each database engine quotes identifiers differently:

- **PostgreSQL / TimescaleDB** — `"double_quotes"` with `$1`, `$2`, … placeholders
- **MySQL** — ``backticks`` with `?` placeholders

### File layout

- **Interfaces** in `provider.go` (e.g., `database/provider.go`,
  `storage/provider.go`)
- **Implementations** in `{engine}.go` (e.g., `postgres/postgres.go`)
- **Tests** in `*_test.go` in the same package
- **Reconcilers** in `internal/controller/{kind}_controller.go` plus a
  `{kind}_controller_test.go` envtest counterpart
- **Webhook validators** in `internal/webhook/{kind}.go` with unit
  tests via `sigs.k8s.io/controller-runtime/pkg/client/fake`

### Webhook validators

Use `field.ErrorList` aggregates so users see field-pathed errors and
all violations are surfaced in one round-trip:

```go
var errs field.ErrorList
errs = append(errs, field.Invalid(specPath.Child("schedule"), s, "..."))
errs = append(errs, field.NotFound(specPath.Child("databaseRef").Child("name"), name))
return nil, errs.ToAggregate()
```

### Reconciler patterns

- Use `controllerutil.SetControllerReference` and
  `controllerutil.CreateOrUpdate` for owned objects.
- Return `ctrl.Result{RequeueAfter: ...}` for time-based scheduling
  (the `ArchiveRule` controller's cron loop is the canonical example).
- Keep the `Reconcile` body linear — extract phases as helper methods
  on the reconciler struct.
- Use `Owns(&batchv1.Job{})` so Job state changes wake the reconciler;
  use `Watches(...)` with `EnqueueRequestsFromMapFunc` for cross-CR
  dependencies (e.g., `DatabaseConnection` → `ArchiveRule`).

### Metrics

APR-specific Prometheus collectors live in `internal/metrics`. To add
one, define the `*Vec` collector at package scope, register it in
`MustRegister`, and provide a `Record*` convenience wrapper that the
reconciler calls. Emission happens manager-side (reconciler observes a
finished Job) so metrics survive the short-lived Job pod's exit — see
`internal/controller/metrics_emit.go` for the watermark pattern.

### Tests

- Use `t.TempDir()` for filesystem tests.
- Mock `database.Provider` for engine tests — see
  `internal/engine/archiver_test.go` for the mock pattern.
- Engine integration tests use the `//go:build integration` tag and
  run against Docker containers from `dev/docker-compose.yml`.
- Reconciler envtest tests use the shared harness in
  `internal/controller/suite_test.go` and skip cleanly without
  `KUBEBUILDER_ASSETS`.
- The kind end-to-end test uses the `//go:build k8s` tag and assumes
  the cluster + chart are already up.

### Parquet type mapping

Database types are normalized in `internal/format/parquet.go` via
`normalizeType()`. When adding new type mappings, follow the existing
convention: `int/smallint → int32`, `bigint → int64`, `real →
float32`, `double → float64`, `bool → bool`, everything else →
`string`.

## Common Tasks

### Changing CRD types

After editing anything under `api/v1alpha1/`, regenerate generated
code and the chart's bundled CRDs:

```bash
make manifests           # CRDs in config/crd/bases/, RBAC in config/rbac/role.yaml
make generate            # zz_generated.deepcopy.go
make helm-sync-crds      # Copy generated CRDs into charts/apr/crds/
```

### Adding a new database engine

1. Create `internal/provider/database/<engine>/<engine>.go` implementing
   the `database.Provider` interface (`Connect`, `Close`, `ExtractRows`,
   `DeleteRows`, `RestoreRows`, `InferSchema`, `InferPrimaryKey`).
2. Add the engine name to the enum in
   `api/v1alpha1/databaseconnection_types.go` and to the YAML config
   validation in `internal/config/config.go`.
3. Wire the constructor into `internal/cluster/run.go`
   (`buildDatabaseProvider`) and `cmd/apr/main.go` (`makeDBProvider`).
4. Add unit tests in the same package; add integration coverage under
   `integration/integration_test.go` with seed data in `dev/seed/<engine>/`.
5. `make manifests` to regenerate CRD enums.

### Adding a new storage backend

1. Create `internal/provider/storage/<backend>/<backend>.go`
   implementing `storage.Provider` (`Put`, `Get`, `Delete`, `List`,
   `Exists`, `Rename`).
2. Add the type name to the enum in
   `api/v1alpha1/storagebackend_types.go` and the type-aware Secret-key
   list in `internal/controller/storagebackend_controller.go`
   (`requiredSecretKeys`) plus the webhook
   `internal/webhook/storagebackend.go` (`requiredSecretKeysFor`).
3. Wire it into `internal/cluster/run.go` (`buildStorageProvider`) and
   `cmd/apr/main.go` (`makeStorage`).
4. Tests + `make manifests`.

### Adding a webhook validation rule

1. Add the check to the matching validator in `internal/webhook/`.
   Append to the `field.ErrorList` so multi-violation errors stay
   useful.
2. Add a test in the corresponding `*_test.go` covering both the
   happy path and the rejection path.
3. Webhook unit tests use `sigs.k8s.io/controller-runtime/pkg/client/fake`
   with the API scheme; no envtest needed.

### Adding a Prometheus metric

1. Define the collector in `internal/metrics/metrics.go` and add it
   to `MustRegister`.
2. Provide a `Record*` wrapper that the manager-side caller invokes
   (don't observe from inside the engine — those samples are lost
   when the Job pod exits).
3. Update the starter Grafana dashboard at
   `charts/apr/dashboards/apr-overview.json` if relevant.

## Pull Request Process

### Branch naming

Use a prefix that describes the change:

- `feat/description` — new features
- `fix/description` — bug fixes
- `docs/description` — documentation
- `refactor/description` — code restructuring
- `test/description` — test additions

### Commit messages

[Conventional Commits](https://www.conventionalcommits.org/):

```
feat(operator): support Watches() on Secret for DBC reconciler
fix(s3): handle partial rename failures with retry
docs: add validating webhook examples to README
test(controller): cover annotation trigger dedup edge case
```

### What to include

- Clear description of what changed and why
- How to test the changes (unit tests, manual steps)
- Reference related issues (e.g., "Closes #42")

### Before submitting

1. `make test` and `make lint` pass.
2. If you changed reconcilers or types, `make test-envtest` passes.
3. If you changed database/storage providers, `make test-integration`
   passes.
4. If you changed CRD types, the operator manifests, or the chart,
   `make test-k8s-clean` passes (this is the strongest signal that an
   end-to-end install still works).
5. If you regenerated CRDs, `charts/apr/crds/` is up to date
   (`make helm-sync-crds`).
6. Keep PRs focused — one logical change per PR.

## Reporting Issues

### Bug reports

Include:

- APR version (`apr version` / image tag)
- Kubernetes version (if relevant)
- Database engine and version
- Storage backend
- Steps to reproduce
- Expected vs. actual behavior
- Relevant CR YAML or YAML config (redact credentials)
- Manager logs (`kubectl -n apr-system logs deploy/apr-manager`) if
  the issue is reconciler- or webhook-related

### Feature requests

Describe the use case and the behavior you'd like. If you have ideas
about implementation, feel free to include them — but the use case is
the more important part.
