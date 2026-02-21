package engine

import (
	"context"
	"fmt"
	"log"
	"os"

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
	log      *log.Logger
}

// New creates a new Engine.
func New(cfg *config.Config, store storage.Provider) *Engine {
	logger := log.New(os.Stderr, "[apr] ", log.LstdFlags)
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
func (e *Engine) RunRestore(ctx context.Context, ruleName, table, date, runID string, db database.Provider) (*RestoreResult, error) {
	rule := e.cfg.FindRule(ruleName)
	if rule == nil {
		return nil, fmt.Errorf("rule %q not found", ruleName)
	}

	opts := RestoreOptions{
		Rule:     *rule,
		Table:    table,
		Date:     date,
		RunID:    runID,
	}

	return e.restorer.Restore(ctx, opts, db)
}
