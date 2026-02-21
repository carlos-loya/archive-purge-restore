# APR — Archive, Purge, Restore

**Stop paying to store rows nobody reads.** APR moves old database rows to cheap object storage as compact Parquet files, deletes them from your database, and brings them back with a single command when you need them.

```
apr archive prod-orders      # Archive old rows → Parquet on S3
apr restore --rule prod-orders --date 2024-06-15   # Bring them back
```

## Why APR?

- **Shrink your database** — archive rows past their retention window without losing them forever
- **Parquet format** — columnar, compressed, and readable by every data tool (Spark, DuckDB, pandas, Athena)
- **Zero-downtime** — two-phase archive ensures no data loss: rows are only deleted after successful upload
- **Composite key support** — handles single and multi-column primary keys out of the box
- **Daemon mode** — set cron schedules per rule and let it run unattended
- **Full audit trail** — every archive and restore is logged with run IDs, row counts, and timestamps

## Supported Databases & Storage

| Databases | Storage backends |
|-----------|-----------------|
| PostgreSQL | AWS S3 |
| MySQL | Local filesystem |

## Quick Start

### Install

```bash
go install github.com/carlos-loya/archive-purge-restore/cmd/apr@latest
```

Or build from source:

```bash
git clone https://github.com/carlos-loya/archive-purge-restore.git
cd archive-purge-restore
make build    # → ./apr
```

### Configure

Create `apr.yaml`:

```yaml
storage:
  type: s3
  s3:
    bucket: my-archive-bucket
    region: us-east-1

rules:
  - name: prod-orders
    schedule: "0 2 * * *"          # 2 AM daily
    batch_size: 10000
    source:
      engine: postgres
      host: db.example.com
      port: 5432
      database: production
      credentials:
        type: env
        username_env: DB_USER
        password_env: DB_PASS
    tables:
      - name: orders
        date_column: created_at
        days_online: 90
      - name: order_items
        date_column: created_at
        days_online: 90
```

```bash
apr validate    # Check your config
```

### Run

```bash
# One-off archive
apr archive prod-orders

# Run as a daemon (uses cron schedules from config)
apr daemon

# Restore archived data
apr restore --rule prod-orders --table orders --date 2024-06-15

# View execution history
apr history --rule prod-orders
```

## How It Works

1. **Extract** — query rows older than `days_online` in batches, write each batch to a `.pending` Parquet file
2. **Delete** — remove archived rows from the source database by primary key
3. **Finalize** — rename `.pending` files to their final path (data is only committed after successful deletion)
4. **On failure** — all `.pending` files are cleaned up, no rows are deleted

Archives are stored at `{database}/{table}/{date}/{runID}_{batch}.parquet`.

## CLI Reference

```
apr daemon                        Run scheduler with all rules
apr archive [rule]                Archive all rules, or a specific rule
apr restore --rule R [flags]      Restore archived data
      --table T                   Specific table (default: all)
      --date YYYY-MM-DD           Specific date
      --run-id ID                 Specific run ID
apr history [--rule R] [--limit N] View execution history
apr validate                      Validate config file
apr version                       Print version
```

## Development

```bash
make build              # Build binary
make test               # Unit tests
make lint               # go vet

# Integration tests (requires Docker)
make test-integration   # Spins up PostgreSQL + MySQL, runs end-to-end tests
make dev-down           # Tear down containers
```

## License

MIT
