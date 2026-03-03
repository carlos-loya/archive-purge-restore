package engine

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"time"

	"github.com/carlos-loya/archive-purge-restore/internal/config"
	"github.com/carlos-loya/archive-purge-restore/internal/format"
	"github.com/carlos-loya/archive-purge-restore/internal/provider/storage"
)

// VerifyOptions specifies what to verify.
type VerifyOptions struct {
	Rule  config.Rule
	Table string // specific table, or empty for all tables in rule
	Date  string // YYYY-MM-DD, or empty for all dates
	RunID string // specific run ID, or empty for all
}

// VerifyFileResult contains the verification result for a single file.
type VerifyFileResult struct {
	Key      string
	Status   string // "OK" or "CORRUPT"
	RowCount int
	Error    error
}

// VerifyTableResult contains the verification result for a single table.
type VerifyTableResult struct {
	Table     string
	Files     []VerifyFileResult
	TotalRows int
}

// VerifyResult contains the overall result of a verify operation.
type VerifyResult struct {
	Rule      string
	StartTime time.Time
	EndTime   time.Time
	Tables    []VerifyTableResult
	Error     error
}

// Verifier performs archive file integrity checks.
type Verifier struct {
	store storage.Provider
	log   *slog.Logger
}

// NewVerifier creates a new Verifier.
func NewVerifier(store storage.Provider, logger *slog.Logger) *Verifier {
	if logger == nil {
		logger = slog.Default()
	}
	return &Verifier{store: store, log: logger}
}

// Verify checks the integrity of archived Parquet files for a rule.
func (v *Verifier) Verify(ctx context.Context, opts VerifyOptions) (*VerifyResult, error) {
	result := &VerifyResult{
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
		tr, err := v.verifyTable(ctx, opts, tbl)
		if err != nil {
			result.Error = fmt.Errorf("verifying table %s: %w", tbl.Name, err)
			result.EndTime = time.Now()
			return result, result.Error
		}
		result.Tables = append(result.Tables, *tr)
	}

	result.EndTime = time.Now()
	v.log.Info("verify completed", "rule", opts.Rule.Name, "duration", result.EndTime.Sub(result.StartTime))
	return result, nil
}

func (v *Verifier) verifyTable(ctx context.Context, opts VerifyOptions, tbl config.TableConfig) (*VerifyTableResult, error) {
	prefix := fmt.Sprintf("%s/%s/", sanitize(opts.Rule.Source.Database), sanitize(tbl.Name))
	if opts.Date != "" {
		prefix += opts.Date + "/"
	}

	v.log.Info("searching for archive files", "prefix", prefix)

	objects, err := v.store.List(ctx, prefix)
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

	result := &VerifyTableResult{
		Table: tbl.Name,
	}

	for _, obj := range objects {
		fr := v.verifyFile(ctx, obj.Key)
		result.Files = append(result.Files, fr)
		result.TotalRows += fr.RowCount
	}

	return result, nil
}

func (v *Verifier) verifyFile(ctx context.Context, key string) VerifyFileResult {
	rc, err := v.store.Get(ctx, key)
	if err != nil {
		v.log.Warn("failed to read archive file", "file", key, "error", err)
		return VerifyFileResult{Key: key, Status: "CORRUPT", Error: fmt.Errorf("reading file: %w", err)}
	}

	data, err := io.ReadAll(rc)
	rc.Close()
	if err != nil {
		v.log.Warn("failed to read archive data", "file", key, "error", err)
		return VerifyFileResult{Key: key, Status: "CORRUPT", Error: fmt.Errorf("reading data: %w", err)}
	}

	_, rows, err := format.ReadParquet(data)
	if err != nil {
		v.log.Warn("corrupt parquet file", "file", key, "error", err)
		return VerifyFileResult{Key: key, Status: "CORRUPT", Error: fmt.Errorf("parsing parquet: %w", err)}
	}

	v.log.Info("verified file", "file", key, "rows", len(rows))
	return VerifyFileResult{Key: key, Status: "OK", RowCount: len(rows)}
}
