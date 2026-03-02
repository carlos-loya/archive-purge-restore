package engine

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/carlos-loya/archive-purge-restore/internal/config"
	"github.com/carlos-loya/archive-purge-restore/internal/provider/database"
	"github.com/carlos-loya/archive-purge-restore/internal/provider/storage"
)

// Engine orchestrates archive and restore operations.
type Engine struct {
	cfg      *config.Config
	store    storage.Provider
	archiver *Archiver
	restorer *Restorer
	log      *slog.Logger
}

// New creates a new Engine.
func New(cfg *config.Config, store storage.Provider, logger *slog.Logger) *Engine {
	if logger == nil {
		logger = slog.Default()
	}
	return &Engine{
		cfg:      cfg,
		store:    store,
		archiver: NewArchiver(store, logger),
		restorer: NewRestorer(store, logger),
		log:      logger,
	}
}

// RunArchive executes the archive process for a specific rule.
func (e *Engine) RunArchive(ctx context.Context, ruleName string, db database.Provider) (*RunResult, error) {
	rule := e.cfg.FindRule(ruleName)
	if rule == nil {
		return nil, fmt.Errorf("rule %q not found", ruleName)
	}
	return e.archiver.Archive(ctx, *rule, db)
}

// RunArchiveDryRun simulates an archive run without making any changes.
func (e *Engine) RunArchiveDryRun(ctx context.Context, ruleName string, db database.Provider) (*DryRunResult, error) {
	rule := e.cfg.FindRule(ruleName)
	if rule == nil {
		return nil, fmt.Errorf("rule %q not found", ruleName)
	}
	return e.archiver.ArchiveDryRun(ctx, *rule, db)
}

// RunArchiveAllDryRun simulates an archive run for all rules without making any changes.
func (e *Engine) RunArchiveAllDryRun(ctx context.Context, dbFactory func(config.SourceConfig) (database.Provider, error)) ([]*DryRunResult, error) {
	var results []*DryRunResult
	for _, rule := range e.cfg.Rules {
		db, err := dbFactory(rule.Source)
		if err != nil {
			return results, fmt.Errorf("creating database provider for rule %s: %w", rule.Name, err)
		}
		if err := db.Connect(ctx); err != nil {
			db.Close()
			return results, fmt.Errorf("connecting to database for rule %s: %w", rule.Name, err)
		}

		result, err := e.archiver.ArchiveDryRun(ctx, rule, db)
		db.Close()

		results = append(results, result)
		if err != nil {
			return results, err
		}
	}
	return results, nil
}

// RunArchiveAll executes the archive process for all rules.
func (e *Engine) RunArchiveAll(ctx context.Context, dbFactory func(config.SourceConfig) (database.Provider, error)) ([]*RunResult, error) {
	var results []*RunResult
	for _, rule := range e.cfg.Rules {
		db, err := dbFactory(rule.Source)
		if err != nil {
			return results, fmt.Errorf("creating database provider for rule %s: %w", rule.Name, err)
		}
		if err := db.Connect(ctx); err != nil {
			db.Close()
			return results, fmt.Errorf("connecting to database for rule %s: %w", rule.Name, err)
		}

		result, err := e.archiver.Archive(ctx, rule, db)
		db.Close()

		results = append(results, result)
		if err != nil {
			return results, err
		}
	}
	return results, nil
}

// RunRestore executes the restore process.
func (e *Engine) RunRestore(ctx context.Context, ruleName, table, date, runID string, dryRun bool, db database.Provider) (*RestoreResult, error) {
	rule := e.cfg.FindRule(ruleName)
	if rule == nil {
		return nil, fmt.Errorf("rule %q not found", ruleName)
	}

	opts := RestoreOptions{
		Rule:   *rule,
		Table:  table,
		Date:   date,
		RunID:  runID,
		DryRun: dryRun,
	}

	return e.restorer.Restore(ctx, opts, db)
}
