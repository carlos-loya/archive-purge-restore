package engine

import (
	"context"
	"fmt"
	"io"
	"log"
	"time"

	"github.com/carlos-loya/archive-purge-restore/internal/config"
	"github.com/carlos-loya/archive-purge-restore/internal/format"
	"github.com/carlos-loya/archive-purge-restore/internal/provider/database"
	"github.com/carlos-loya/archive-purge-restore/internal/provider/storage"
)

// RestoreOptions specifies what to restore.
type RestoreOptions struct {
	Rule  config.Rule
	Table string // specific table, or empty for all tables in rule
	Date  string // YYYY-MM-DD, or empty for all dates
	RunID string // specific run ID, or empty for all
}

// RestoreResult contains the result of a restore operation.
type RestoreResult struct {
	Rule      string
	StartTime time.Time
	EndTime   time.Time
	Tables    []RestoreTableResult
	Error     error
}

// RestoreTableResult contains the result for a single table restore.
type RestoreTableResult struct {
	Table        string
	RowsRestored int64
	Files        []string
	Error        error
}

// Restorer performs restore operations.
type Restorer struct {
	store storage.Provider
	log   *log.Logger
}

// NewRestorer creates a new Restorer.
func NewRestorer(store storage.Provider, logger *log.Logger) *Restorer {
	if logger == nil {
		logger = log.Default()
	}
	return &Restorer{store: store, log: logger}
}

// Restore reads archived Parquet files and inserts the data back into the database.
func (r *Restorer) Restore(ctx context.Context, opts RestoreOptions, db database.Provider) (*RestoreResult, error) {
	result := &RestoreResult{
		Rule:      opts.Rule.Name,
		StartTime: time.Now(),
	}

	tables := opts.Rule.Tables
	if opts.Table != "" {
		var filtered []config.TableConfig
		for _, t := range tables {
			if t.Name == opts.Table {
				filtered = append(filtered, t)
			}
		}
		if len(filtered) == 0 {
			return nil, fmt.Errorf("table %q not found in rule %q", opts.Table, opts.Rule.Name)
		}
		tables = filtered
	}

	for _, tbl := range tables {
		tr, err := r.restoreTable(ctx, opts, tbl, db)
		if err != nil {
			result.Error = fmt.Errorf("restoring table %s: %w", tbl.Name, err)
			result.EndTime = time.Now()
			return result, result.Error
		}
		result.Tables = append(result.Tables, *tr)
	}

	result.EndTime = time.Now()
	r.log.Printf("restore completed in %v", result.EndTime.Sub(result.StartTime))
	return result, nil
}

func (r *Restorer) restoreTable(ctx context.Context, opts RestoreOptions, tbl config.TableConfig, db database.Provider) (*RestoreTableResult, error) {
	prefix := fmt.Sprintf("%s/%s/", sanitize(opts.Rule.Source.Database), sanitize(tbl.Name))
	if opts.Date != "" {
		prefix += opts.Date + "/"
	}

	r.log.Printf("searching for archive files with prefix %q", prefix)

	objects, err := r.store.List(ctx, prefix)
	if err != nil {
		return nil, fmt.Errorf("listing archive files: %w", err)
	}

	if opts.RunID != "" {
		var filtered []storage.ObjectInfo
		for _, obj := range objects {
			if containsRunID(obj.Key, opts.RunID) {
				filtered = append(filtered, obj)
			}
		}
		objects = filtered
	}

	if len(objects) == 0 {
		return nil, fmt.Errorf("no archive files found for table %s with prefix %q", tbl.Name, prefix)
	}

	result := &RestoreTableResult{
		Table: tbl.Name,
	}

	for _, obj := range objects {
		rc, err := r.store.Get(ctx, obj.Key)
		if err != nil {
			return nil, fmt.Errorf("reading archive file %s: %w", obj.Key, err)
		}

		data, err := io.ReadAll(rc)
		rc.Close()
		if err != nil {
			return nil, fmt.Errorf("reading archive data %s: %w", obj.Key, err)
		}

		columns, rows, err := format.ReadParquet(data)
		if err != nil {
			return nil, fmt.Errorf("parsing parquet file %s: %w", obj.Key, err)
		}

		colNames := make([]string, len(columns))
		for i, c := range columns {
			colNames[i] = c.Name
		}

		inserted, err := db.RestoreRows(ctx, tbl.Name, colNames, rows)
		if err != nil {
			return nil, fmt.Errorf("inserting rows from %s: %w", obj.Key, err)
		}

		result.RowsRestored += inserted
		result.Files = append(result.Files, obj.Key)
		r.log.Printf("restored %d rows from %s", inserted, obj.Key)
	}

	return result, nil
}

func containsRunID(key, runID string) bool {
	return len(key) > 0 && len(runID) > 0 && contains(key, runID)
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && searchString(s, substr)
}

func searchString(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
