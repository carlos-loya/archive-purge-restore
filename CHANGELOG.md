# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/),
and this project adheres to [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added
- TimescaleDB database provider with chunk-aware deletion via `drop_chunks()` for hypertables (#15)
- `ChunkAwareDeleter` interface for providers that support efficient time-range deletion

## [0.2.0] - 2026-02-25

### Added
- MIT license (#36)
- Comprehensive unit tests for S3 storage provider with mock-based testing (#20)
- Connection pool configuration for database providers (`max_open_conns`, `max_idle_conns`, `conn_max_lifetime`, `conn_max_idle_time`) (#17)
- `APR_HISTORY_PATH` environment variable for configuring history database path (#11)
- Godoc comments on config package types and functions (#12)

### Fixed
- S3 Rename partial failures: retry delete up to 3 times, roll back copy on permanent failure (#21)
- Standardized error messages in validate command to match other commands (#18)
- Improved error context in Archiver with run ID, table name, and batch numbers (#16)
- File handle leak and partial write cleanup in filesystem storage Put method (#14)
- Composite PK deletion refactored to use tuple-based IN clause (#13)
- MySQL restore datetime formatting: parse RFC3339 strings back to time.Time

## [0.1.0] - 2026-02-22

### Added
- Core archive-purge-restore workflow with two-phase archiving
- PostgreSQL and MySQL database providers
- S3 and local filesystem storage providers
- Parquet file format for archived data
- Cron-based scheduler for daemon mode
- SQLite execution history log
- CLI commands: `archive`, `restore`, `daemon`, `history`, `validate`, `version`
- YAML configuration with search path resolution
- Docker Compose dev environment with PostgreSQL 16 and MySQL 8.0
- Integration test suite
