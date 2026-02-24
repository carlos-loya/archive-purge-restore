package engine

import (
	"bytes"
	"context"
	"fmt"
	"log"
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

// Archiver performs archive operations.
type Archiver struct {
	store storage.Provider
	log   *log.Logger
}

// NewArchiver creates a new Archiver.
func NewArchiver(store storage.Provider, logger *log.Logger) *Archiver {
	if logger == nil {
		logger = log.Default()
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

	a.log.Printf("[%s] starting archive run for rule %q", runID, rule.Name)

	// Phase 1: Extract and archive all tables.
	var archives []tableArchive

	for _, tbl := range rule.Tables {
		cutoff := time.Now().AddDate(0, 0, -tbl.DaysOnline)
		ta, err := a.archiveTable(ctx, runID, rule, tbl, db, cutoff)
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
		a.log.Printf("[%s] deleted %d rows from %s", runID, deleted, ta.table)

		if deleted != ta.rowCount {
			a.log.Printf("[%s] WARNING: deleted %d rows but archived %d from %s",
				runID, deleted, ta.rowCount, ta.table)
		}
	}

	// Finalize: rename .pending files to final names.
	for _, ta := range archives {
		for _, f := range ta.files {
			pendingKey := f + ".pending"
			if err := a.store.Rename(ctx, pendingKey, f); err != nil {
				a.log.Printf("[%s] WARNING: failed to finalize %s: %v", runID, f, err)
			}
		}
	}

	result.EndTime = time.Now()
	a.log.Printf("[%s] archive run completed in %v", runID, result.EndTime.Sub(result.StartTime))
	return result, nil
}

func (a *Archiver) archiveTable(ctx context.Context, runID string, rule config.Rule, tbl config.TableConfig, db database.Provider, cutoff time.Time) (*tableArchive, error) {
	a.log.Printf("[%s] archiving table %s (cutoff: %s)", runID, tbl.Name, cutoff.Format("2006-01-02"))

	pkCols, err := db.InferPrimaryKey(ctx, tbl.Name)
	if err != nil {
		return nil, fmt.Errorf("table %s: inferring primary key: %w", tbl.Name, err)
	}

	ta := &tableArchive{
		table:  tbl.Name,
		pkCols: pkCols,
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

		a.log.Printf("[%s] archived batch %d: %d rows from %s", runID, batchNum, len(rows), tbl.Name)

		if len(rows) < rule.BatchSize {
			break
		}
	}

	a.log.Printf("[%s] table %s: %d total rows archived in %d batches",
		runID, tbl.Name, ta.rowCount, batchNum)
	return ta, nil
}

func (a *Archiver) cleanupPendingFiles(ctx context.Context, runID string, archives []tableArchive) {
	for _, ta := range archives {
		for _, f := range ta.files {
			pendingKey := f + ".pending"
			if err := a.store.Delete(ctx, pendingKey); err != nil {
				a.log.Printf("[%s] WARNING: failed to cleanup %s: %v", runID, pendingKey, err)
			}
		}
	}
}

type tableArchive struct {
	table    string
	pkCols   []string
	pkValues [][]any
	files    []string
	rowCount int64
}

func archiveKey(database, table string, cutoff time.Time, runID string, batch int) string {
	return fmt.Sprintf("%s/%s/%s/%s_%03d.parquet",
		sanitize(database),
		sanitize(table),
		cutoff.Format("2006-01-02"),
		runID,
		batch)
}

func sanitize(s string) string {
	return strings.Map(func(r rune) rune {
		if r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '-' || r == '_' {
			return r
		}
		return '_'
	}, s)
}
