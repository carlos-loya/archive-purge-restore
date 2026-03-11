package engine

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/carlos-loya/archive-purge-restore/internal/config"
	"github.com/carlos-loya/archive-purge-restore/internal/format"
	"github.com/carlos-loya/archive-purge-restore/internal/provider/database"
	"github.com/carlos-loya/archive-purge-restore/internal/provider/storage"
	"github.com/google/uuid"
)

// ArchiveResult contains the result of an archive operation for a single table.
type ArchiveResult struct {
	Table       string
	RowsArchived int64
	RowsDeleted  int64
	Files       []string
	Error       error
}

// RunResult contains the overall result of an archive run.
type RunResult struct {
	RunID     string
	Rule      string
	StartTime time.Time
	EndTime   time.Time
	Tables    []ArchiveResult
	Error     error
}

// DryRunTableResult contains the dry-run result for a single table.
type DryRunTableResult struct {
	Table  string
	Count  int64
	Cutoff time.Time
}

// DryRunResult contains the overall dry-run result.
type DryRunResult struct {
	Rule   string
	Tables []DryRunTableResult
}

// Archiver performs archive operations.
type Archiver struct {
	store storage.Provider
	log   *slog.Logger
}

// NewArchiver creates a new Archiver.
func NewArchiver(store storage.Provider, logger *slog.Logger) *Archiver {
	if logger == nil {
		logger = slog.Default()
	}
	return &Archiver{store: store, log: logger}
}

// Archive runs the archive process for a single rule.
func (a *Archiver) Archive(ctx context.Context, rule config.Rule, db database.Provider) (*RunResult, error) {
	runID := uuid.New().String()[:8]
	result := &RunResult{
		RunID:     runID,
		Rule:      rule.Name,
		StartTime: time.Now(),
	}

	a.log.Info("starting archive run", "run_id", runID, "rule", rule.Name)

	// Phase 1: Extract and archive all tables.
	var archives []tableArchive

	for _, tbl := range rule.Tables {
		ta, err := a.archiveTable(ctx, runID, rule, tbl, db, cutoff(tbl.DaysOnline))
		if err != nil {
			result.Error = fmt.Errorf("run %s: archiving table %s: %w", runID, tbl.Name, err)
			result.EndTime = time.Now()
			// Clean up any pending files.
			a.cleanupPendingFiles(ctx, runID, archives)
			return result, result.Error
		}
		archives = append(archives, *ta)
		result.Tables = append(result.Tables, ArchiveResult{
			Table:        tbl.Name,
			RowsArchived: ta.rowCount,
			Files:        ta.files,
		})
	}

	// Phase 2: Delete archived rows from source.
	// If the provider supports chunk-aware deletion (e.g., TimescaleDB),
	// drop fully expired chunks first for efficiency.
	if cad, ok := db.(database.ChunkAwareDeleter); ok {
		for _, ta := range archives {
			if ta.rowCount == 0 {
				continue
			}
			dropped, err := cad.DeleteByTimeRange(ctx, ta.table, ta.dateColumn, ta.cutoff)
			if err != nil {
				a.log.Warn("chunk-aware delete failed, falling back to row-by-row",
					"run_id", runID, "table", ta.table, "error", err)
			} else if dropped > 0 {
				a.log.Info("dropped full chunks", "run_id", runID, "table", ta.table, "chunks", dropped)
			}
		}
	}

	for i, ta := range archives {
		if ta.rowCount == 0 {
			continue
		}

		deleted, err := db.DeleteRows(ctx, ta.table, ta.pkCols, ta.pkValues)
		if err != nil {
			result.Error = fmt.Errorf("run %s: deleting %d rows from %s: %w", runID, ta.rowCount, ta.table, err)
			result.EndTime = time.Now()
			return result, result.Error
		}

		result.Tables[i].RowsDeleted = deleted
		a.log.Info("deleted rows", "run_id", runID, "table", ta.table, "rows", deleted)

		if deleted != ta.rowCount {
			a.log.Warn("deleted row count mismatch",
				"run_id", runID, "table", ta.table, "deleted", deleted, "archived", ta.rowCount)
		}
	}

	// Finalize: rename .pending files to final names.
	for _, ta := range archives {
		for _, f := range ta.files {
			pendingKey := f + ".pending"
			if err := a.store.Rename(ctx, pendingKey, f); err != nil {
				a.log.Warn("failed to finalize file", "run_id", runID, "file", f, "error", err)
			}
		}
	}

	result.EndTime = time.Now()
	a.log.Info("archive run completed", "run_id", runID, "duration", result.EndTime.Sub(result.StartTime))
	return result, nil
}

// ArchiveDryRun simulates an archive run without writing files or deleting rows.
// It counts rows that would be archived per table and returns a summary.
func (a *Archiver) ArchiveDryRun(ctx context.Context, rule config.Rule, db database.Provider) (*DryRunResult, error) {
	result := &DryRunResult{
		Rule: rule.Name,
	}

	for _, tbl := range rule.Tables {
		c := cutoff(tbl.DaysOnline)
		var count int64

		// Count rows by extracting them in batches without writing anything.
		for {
			select {
			case <-ctx.Done():
				return nil, fmt.Errorf("dry-run table %s: %w", tbl.Name, ctx.Err())
			default:
			}

			iter, err := db.ExtractRows(ctx, tbl.Name, tbl.DateColumn, c, rule.BatchSize)
			if err != nil {
				return nil, fmt.Errorf("dry-run table %s: extracting rows: %w", tbl.Name, err)
			}

			batchCount := 0
			for iter.Next() {
				batchCount++
			}
			if err := iter.Err(); err != nil {
				iter.Close()
				return nil, fmt.Errorf("dry-run table %s: iterating rows: %w", tbl.Name, err)
			}
			iter.Close()

			count += int64(batchCount)

			if batchCount == 0 || batchCount < rule.BatchSize {
				break
			}
		}

		result.Tables = append(result.Tables, DryRunTableResult{
			Table:  tbl.Name,
			Count:  count,
			Cutoff: c,
		})
	}

	return result, nil
}

func (a *Archiver) archiveTable(ctx context.Context, runID string, rule config.Rule, tbl config.TableConfig, db database.Provider, cutoff time.Time) (*tableArchive, error) {
	a.log.Info("archiving table", "run_id", runID, "table", tbl.Name, "cutoff", cutoff.Format("2006-01-02"))

	pkCols, err := db.InferPrimaryKey(ctx, tbl.Name)
	if err != nil {
		return nil, fmt.Errorf("table %s: inferring primary key: %w", tbl.Name, err)
	}

	ta := &tableArchive{
		table:      tbl.Name,
		dateColumn: tbl.DateColumn,
		cutoff:     cutoff,
		pkCols:     pkCols,
	}

	batchNum := 0
	for {
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("table %s, batch %d: %w", tbl.Name, batchNum, ctx.Err())
		default:
		}

		iter, err := db.ExtractRows(ctx, tbl.Name, tbl.DateColumn, cutoff, rule.BatchSize)
		if err != nil {
			return nil, fmt.Errorf("table %s, batch %d: extracting rows: %w", tbl.Name, batchNum, err)
		}

		var rows []database.Row
		var columns []database.ColumnInfo
		for iter.Next() {
			if columns == nil {
				columns = iter.Columns()
			}
			row := make(database.Row)
			for k, v := range iter.Row() {
				row[k] = v
			}
			rows = append(rows, row)
		}
		if err := iter.Err(); err != nil {
			iter.Close()
			return nil, fmt.Errorf("table %s, batch %d: iterating rows: %w", tbl.Name, batchNum, err)
		}
		iter.Close()

		if len(rows) == 0 {
			break
		}

		// Collect primary key values for later deletion.
		for _, row := range rows {
			pkVals := make([]any, len(pkCols))
			for j, col := range pkCols {
				pkVals[j] = row[col]
			}
			ta.pkValues = append(ta.pkValues, pkVals)
		}

		// Write to Parquet.
		data, err := format.WriteParquet(columns, rows)
		if err != nil {
			return nil, fmt.Errorf("table %s, batch %d (%d rows): writing parquet: %w", tbl.Name, batchNum, len(rows), err)
		}

		key := archiveKey(rule.Source.Database, tbl.Name, cutoff, runID, batchNum)
		pendingKey := key + ".pending"

		if err := a.store.Put(ctx, pendingKey, bytes.NewReader(data)); err != nil {
			return nil, fmt.Errorf("table %s, batch %d (%d rows): storing archive file %s: %w", tbl.Name, batchNum, len(rows), pendingKey, err)
		}

		ta.files = append(ta.files, key)
		ta.rowCount += int64(len(rows))
		batchNum++

		a.log.Info("archived batch", "run_id", runID, "batch", batchNum, "rows", len(rows), "table", tbl.Name)

		if len(rows) < rule.BatchSize {
			break
		}
	}

	a.log.Info("table archive complete",
		"run_id", runID, "table", tbl.Name, "total_rows", ta.rowCount, "batches", batchNum)
	return ta, nil
}

func (a *Archiver) cleanupPendingFiles(ctx context.Context, runID string, archives []tableArchive) {
	for _, ta := range archives {
		for _, f := range ta.files {
			pendingKey := f + ".pending"
			if err := a.store.Delete(ctx, pendingKey); err != nil {
				a.log.Warn("failed to cleanup pending file", "run_id", runID, "file", pendingKey, "error", err)
			}
		}
	}
}

type tableArchive struct {
	table      string
	dateColumn string
	cutoff     time.Time
	pkCols     []string
	pkValues   [][]any
	files      []string
	rowCount   int64
}

func archiveKey(database, table string, cutoff time.Time, runID string, batch int) string {
	return fmt.Sprintf("%s/%s/%s/%s_%03d.parquet",
		sanitize(database),
		sanitize(table),
		cutoff.Format("2006-01-02"),
		runID,
		batch)
}

func cutoff(daysOnline int) time.Time {
	return time.Now().AddDate(0, 0, -daysOnline)
}

func sanitize(s string) string {
	return strings.Map(func(r rune) rune {
		if r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '-' || r == '_' {
			return r
		}
		return '_'
	}, s)
}
