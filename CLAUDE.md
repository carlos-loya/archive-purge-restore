# APR (Archive-Purge-Restore)

A Go CLI tool that archives old database rows to object storage as Parquet files, deletes them from the source database, and restores them on demand.

## Build & Test

```bash
make build          # Build binary → ./apr
make test           # go test ./... -v (unit tests only)
make lint           # go vet ./...
make clean          # Remove binary

# Integration tests (requires Docker):
make dev-up          # Start PostgreSQL 16 + MySQL 8.0 in Docker
make dev-down        # Stop containers, remove volumes
make dev-reset       # dev-down + dev-up (clean slate)
make test-integration # dev-up + run integration tests
make test-all        # Unit tests + integration tests

# Build with version:
go build -ldflags "-X main.version=1.0.0" -o apr ./cmd/apr

# Run a single package's tests:
go test ./internal/engine/ -v
go test ./internal/config/ -v -run TestValidate
```

**Note:** `go-sqlite3` requires CGO. Ensure `CGO_ENABLED=1` (the default) when building.

## Project Structure

```
cmd/apr/main.go                          # CLI entry point, wires all components together
dev/
  docker-compose.yml                     # PostgreSQL 16 + MySQL 8.0 for integration tests
  apr.dev.yaml                           # Dev config pointing at Docker containers
  seed/postgres/{01_schema,02_data}.sql  # Seed schema and data for PostgreSQL
  seed/mysql/{01_schema,02_data}.sql     # Seed schema and data for MySQL
integration/integration_test.go          # End-to-end tests (build tag: integration)
internal/
  config/config.go                       # YAML config parsing, validation, defaults
  engine/
    engine.go                            # Orchestration: RunArchive, RunArchiveAll, RunRestore
    archiver.go                          # Two-phase archive: extract → .pending → delete → finalize
    restorer.go                          # Restore: find parquet files → read → INSERT
  provider/
    database/
      provider.go                        # DatabaseProvider + RowIterator + ChunkAwareDeleter interfaces
      postgres/postgres.go               # PostgreSQL: double-quote identifiers, $N placeholders
      mysql/mysql.go                     # MySQL: backtick identifiers, ? placeholders
      timescaledb/timescaledb.go         # TimescaleDB: embeds Postgres, adds chunk-aware drop_chunks()
    storage/
      provider.go                        # StorageProvider interface
      filesystem/filesystem.go           # Local FS (dev/testing)
      s3/s3.go                           # AWS S3 with lifecycle policy support
  format/parquet.go                      # Parquet read/write, DB type → Parquet type mapping
  scheduler/scheduler.go                 # Cron scheduling for daemon mode (robfig/cron)
  history/history.go                     # SQLite execution log (WAL mode)
```

## Architecture

### Key Interfaces

**`database.Provider`** — abstracts database operations:
- `Connect`, `Close` — lifecycle
- `ExtractRows(table, dateColumn, before, batchSize)` → `RowIterator` — streams rows older than cutoff
- `DeleteRows(table, pkColumns, pkValues)` → count — batched delete by primary key
- `RestoreRows(table, columns, rows)` → count — transactional batch insert
- `InferSchema(table)` → `[]ColumnInfo` — column names, types, nullability
- `InferPrimaryKey(table)` → `[]string` — primary key column names

**`storage.Provider`** — abstracts archive storage:
- `Put`, `Get`, `Delete`, `List`, `Exists`, `Rename`

**`database.Row`** is `map[string]any`. **`RowIterator`** streams rows with `Next()/Row()/Err()/Close()`.

### Archive Algorithm (two-phase with batching)

1. **Extract**: For each table, query rows older than `now - days_online` in batches of `batch_size`, write each batch to Parquet at `{db}/{table}/{date}/{runID}_{batch}.parquet.pending`
2. **Delete**: For each table, delete the archived rows by primary key (single-PK uses IN clause in batches of 1000; composite-PK uses OR'd conditions in batches of 500)
3. **Finalize**: Rename `.pending` → final path. Files only reach final state after successful deletion
4. **On failure**: Clean up all `.pending` files, no rows deleted from source

Run IDs are the first 8 characters of a UUID.

### Restore Algorithm

Search storage by `{db}/{table}/{date}/` prefix, optionally filter by run ID (substring match). Read each Parquet file, extract columns, and batch-INSERT into the database.

### Config

Search order: `--config` flag, `./apr.yaml`, `./apr.yml`, `~/.apr/config.yaml`, `/etc/apr/config.yaml`.

Defaults: `batch_size=10000`, `history.path=~/.apr/history.db`, PostgreSQL `ssl_mode=prefer`.

Supported storage types: `filesystem`, `s3`, `r2`, `gcs`. Supported engines: `postgres`, `mysql`, `timescaledb`.

Credentials resolve via `type: env` (reads env vars) or `type: static` (inline, not recommended).

### CLI Commands

```
apr daemon                        # Run scheduler with all rules
apr archive [rule]                # Manual archive (all or specific rule)
apr restore --rule R [--table T] [--date YYYY-MM-DD] [--run-id ID]
apr history [--rule R] [--limit N]
apr validate                      # Validate config
apr version
```

## Key Dependencies

| Package | Purpose |
|---------|---------|
| `spf13/cobra` | CLI framework |
| `gopkg.in/yaml.v3` | Config parsing |
| `parquet-go/parquet-go` | Parquet read/write (uses `GenericWriter[any]`/`Read[any]` with `parquet.Group` schemas) |
| `lib/pq` | PostgreSQL driver |
| `go-sql-driver/mysql` | MySQL driver |
| `mattn/go-sqlite3` | SQLite for history (CGO) |
| `aws/aws-sdk-go-v2` | S3 storage |
| `robfig/cron/v3` | Cron scheduler |
| `google/uuid` | Run ID generation |

## Conventions

- **Identifier quoting**: PostgreSQL/TimescaleDB uses `"double_quotes"` with `$N` placeholders; MySQL uses `` `backticks` `` with `?` placeholders
- **TimescaleDB**: Embeds the Postgres provider, adds `ChunkAwareDeleter` for efficient `drop_chunks()` on hypertables. Falls back to standard Postgres behavior for regular tables or when TimescaleDB extension is unavailable
- **Errors**: Always wrap with `fmt.Errorf("context: %w", err)`
- **File layout**: Interfaces in `provider.go`, implementations in `{engine}.go`, tests in `*_test.go` (same package)
- **Tests**: Use `t.TempDir()` for filesystem tests, mock `database.Provider` for engine tests (see `archiver_test.go` for the mock pattern). Integration tests use `//go:build integration` tag and run against Docker containers
- **Parquet type mapping**: DB types normalize via `normalizeType()` in `format/parquet.go` — int/smallint→int32, bigint→int64, real→float32, double→float64, bool→bool, everything else→string
- **Sanitization**: Archive path components replace non-alphanumeric characters (except `-` and `_`) with `_`
