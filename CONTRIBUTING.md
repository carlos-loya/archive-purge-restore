# Contributing to APR

Thanks for your interest in contributing to APR. This guide covers everything you need to get started.

## Development Setup

**Prerequisites:**

- Go 1.25+ (see `go.mod` for the exact version)
- CGO enabled (`CGO_ENABLED=1`, the default) -- required by `go-sqlite3`
- Docker and Docker Compose (for integration tests)

**Getting started:**

```bash
git clone https://github.com/carlos-loya/archive-purge-restore.git
cd archive-purge-restore
make build
./apr version
```

## Running Tests

**Unit tests:**

```bash
make test              # Runs go test ./... -v
make lint              # Runs go vet ./...
```

**Integration tests** (requires Docker -- spins up PostgreSQL 16 and MySQL 8.0):

```bash
make test-integration  # Starts containers, runs integration tests
make test-all          # Unit tests + integration tests
make dev-down          # Tear down containers when done
```

**Run a single package's tests:**

```bash
go test ./internal/engine/ -v
go test ./internal/config/ -v -run TestValidate
```

## Code Conventions

### Error Handling

Always wrap errors with context using `%w`:

```go
if err != nil {
    return fmt.Errorf("failed to archive table %s: %w", table, err)
}
```

### Identifier Quoting

Each database engine quotes identifiers differently. Follow the existing patterns:

- **PostgreSQL** -- `"double_quotes"` with `$1`, `$2`, ... placeholders
- **MySQL** -- `` `backticks` `` with `?` placeholders

### File Layout

- **Interfaces** go in `provider.go` (e.g., `database/provider.go`, `storage/provider.go`)
- **Implementations** go in `{engine}.go` (e.g., `postgres/postgres.go`, `mysql/mysql.go`)
- **Tests** go in `*_test.go` in the same package

### Testing Patterns

- Use `t.TempDir()` for tests that need a temporary filesystem
- Use mock providers for engine tests -- see `internal/engine/archiver_test.go` for the mock pattern
- Integration tests use the `//go:build integration` build tag and run against Docker containers

### Parquet Type Mapping

Database types are normalized in `internal/format/parquet.go` via `normalizeType()`. When adding new type mappings, follow the existing convention: `int/smallint` -> `int32`, `bigint` -> `int64`, `real` -> `float32`, `double` -> `float64`, `bool` -> `bool`, everything else -> `string`.

## Adding a New Database Engine

1. Create a new package under `internal/provider/database/` (e.g., `internal/provider/database/sqlite/`)
2. Implement the `database.Provider` interface:
   - `Connect`, `Close` -- connection lifecycle
   - `ExtractRows` -- query rows older than a cutoff, return a `RowIterator`
   - `DeleteRows` -- delete rows by primary key values
   - `RestoreRows` -- batch-insert rows back into a table
   - `InferSchema` -- return column names, types, and nullability
   - `InferPrimaryKey` -- detect primary key columns
3. Add the engine name to config validation in `internal/config/config.go`
4. Wire it up in `cmd/apr/main.go`
5. Add unit tests in the same package and integration tests under `integration/`

## Adding a New Storage Backend

1. Create a new package under `internal/provider/storage/` (e.g., `internal/provider/storage/gcs/`)
2. Implement the `storage.Provider` interface:
   - `Put` -- write data to a key
   - `Get` -- retrieve data by key
   - `Delete` -- remove an object
   - `List` -- list objects by prefix
   - `Exists` -- check if a key exists
   - `Rename` -- atomically rename an object
3. Add the storage type to config validation in `internal/config/config.go`
4. Wire it up in `cmd/apr/main.go`
5. Add unit tests and, if applicable, integration tests

## Pull Request Process

### Branch Naming

Use a prefix that describes the type of change:

- `feat/description` -- new features
- `fix/description` -- bug fixes
- `docs/description` -- documentation changes
- `refactor/description` -- code restructuring
- `test/description` -- adding or improving tests

### Commit Messages

Follow [Conventional Commits](https://www.conventionalcommits.org/) format:

```
feat(engine): add SQLite database provider
fix(s3): handle partial rename failures with retry
docs: add contributing guide
test(config): add validation edge cases
```

### What to Include in Your PR

- A clear description of what changed and why
- How to test the changes (unit tests, manual steps, etc.)
- Reference any related GitHub issues (e.g., "Closes #42")

### Before Submitting

1. Run `make test` and `make lint` and ensure they pass
2. If you changed database or storage providers, run `make test-integration`
3. Keep PRs focused -- one logical change per PR

## Reporting Issues

### Bug Reports

Include the following:

- APR version (`apr version`)
- Database engine and version
- Storage backend (S3, filesystem)
- Steps to reproduce
- Expected vs. actual behavior
- Relevant config (redact credentials)

### Feature Requests

Describe the use case and the behavior you would like to see. If you have ideas about implementation, feel free to include them.
