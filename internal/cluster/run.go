// SPDX-License-Identifier: MIT
// Copyright (c) 2026 Carlos Loya

package cluster

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	aprv1alpha1 "github.com/carlos-loya/archive-purge-restore/api/v1alpha1"
	"github.com/carlos-loya/archive-purge-restore/internal/config"
	"github.com/carlos-loya/archive-purge-restore/internal/engine"
	"github.com/carlos-loya/archive-purge-restore/internal/provider/database"
	dbmysql "github.com/carlos-loya/archive-purge-restore/internal/provider/database/mysql"
	dbpg "github.com/carlos-loya/archive-purge-restore/internal/provider/database/postgres"
	dbtsdb "github.com/carlos-loya/archive-purge-restore/internal/provider/database/timescaledb"
	"github.com/carlos-loya/archive-purge-restore/internal/provider/storage"
	"github.com/carlos-loya/archive-purge-restore/internal/provider/storage/filesystem"
	gcsstore "github.com/carlos-loya/archive-purge-restore/internal/provider/storage/gcs"
	r2store "github.com/carlos-loya/archive-purge-restore/internal/provider/storage/r2"
	s3store "github.com/carlos-loya/archive-purge-restore/internal/provider/storage/s3"
)

// RunArchiveFromCR loads the named ArchiveRule (and its referenced DBC + SB)
// from the cluster, runs the archive engine against it, and writes the
// outcome back to the rule's status. Returns the engine error so the calling
// process exits non-zero on failure (which is what the CronJob wants).
func RunArchiveFromCR(ctx context.Context, c client.Client, ref string, logger *slog.Logger) error {
	namespace, name, err := ParseRef(ref)
	if err != nil {
		return err
	}

	var rule aprv1alpha1.ArchiveRule
	if err := c.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, &rule); err != nil {
		return fmt.Errorf("getting ArchiveRule %s/%s: %w", namespace, name, err)
	}

	dbc, sb, err := loadRuleRefs(ctx, c, &rule)
	if err != nil {
		return err
	}

	cleanup, err := PrepareStorageEnvironment(sb)
	if err != nil {
		return err
	}
	defer cleanup()

	ec, err := TranslateArchive(&rule, dbc, sb)
	if err != nil {
		return err
	}

	store, db, err := buildProviders(ctx, ec)
	if err != nil {
		return err
	}
	defer db.Close()

	eng := engine.New(ec.Config, store, logger)
	result, runErr := eng.RunArchive(ctx, ec.Rule.Name, db)

	// Record the outcome regardless of whether the run succeeded; we want
	// LastRunResult=Failed on failure so users can see it via kubectl.
	if recErr := RecordArchiveResult(ctx, c, namespace, name, result, runErr); recErr != nil {
		logger.Error("recording archive result", "error", recErr)
	}
	return runErr
}

// RunRestoreFromCR loads the named RestoreRequest (and the ArchiveRule it
// references), runs the restore engine, and writes the outcome to the RR's
// status.
func RunRestoreFromCR(ctx context.Context, c client.Client, ref string, logger *slog.Logger) error {
	namespace, name, err := ParseRef(ref)
	if err != nil {
		return err
	}

	var rr aprv1alpha1.RestoreRequest
	if err := c.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, &rr); err != nil {
		return fmt.Errorf("getting RestoreRequest %s/%s: %w", namespace, name, err)
	}

	var rule aprv1alpha1.ArchiveRule
	if err := c.Get(ctx, types.NamespacedName{Namespace: namespace, Name: rr.Spec.ArchiveRuleRef.Name}, &rule); err != nil {
		return fmt.Errorf("getting referenced ArchiveRule %q: %w", rr.Spec.ArchiveRuleRef.Name, err)
	}

	dbc, sb, err := loadRuleRefs(ctx, c, &rule)
	if err != nil {
		return err
	}

	cleanup, err := PrepareStorageEnvironment(sb)
	if err != nil {
		return err
	}
	defer cleanup()

	ec, err := TranslateArchive(&rule, dbc, sb)
	if err != nil {
		return err
	}

	store, db, err := buildProviders(ctx, ec)
	if err != nil {
		return err
	}
	defer db.Close()

	startTime := time.Now()
	eng := engine.New(ec.Config, store, logger)
	result, runErr := eng.RunRestore(ctx, ec.Rule.Name, rr.Spec.Table, rr.Spec.Date, rr.Spec.RunID, false, db)

	if recErr := RecordRestoreResult(ctx, c, namespace, name, result, runErr, startTime); recErr != nil {
		logger.Error("recording restore result", "error", recErr)
	}
	return runErr
}

// loadRuleRefs fetches the DatabaseConnection and StorageBackend referenced
// by an ArchiveRule.
func loadRuleRefs(ctx context.Context, c client.Client, rule *aprv1alpha1.ArchiveRule) (
	*aprv1alpha1.DatabaseConnection, *aprv1alpha1.StorageBackend, error,
) {
	var dbc aprv1alpha1.DatabaseConnection
	dbcKey := types.NamespacedName{Namespace: rule.Namespace, Name: rule.Spec.DatabaseRef.Name}
	if err := c.Get(ctx, dbcKey, &dbc); err != nil {
		return nil, nil, fmt.Errorf("getting DatabaseConnection %q: %w", dbcKey.Name, err)
	}
	var sb aprv1alpha1.StorageBackend
	sbKey := types.NamespacedName{Namespace: rule.Namespace, Name: rule.Spec.StorageRef.Name}
	if err := c.Get(ctx, sbKey, &sb); err != nil {
		return nil, nil, fmt.Errorf("getting StorageBackend %q: %w", sbKey.Name, err)
	}
	return &dbc, &sb, nil
}

func buildProviders(ctx context.Context, ec *EngineConfig) (storage.Provider, database.Provider, error) {
	store, err := buildStorageProvider(ctx, ec.Config.Storage)
	if err != nil {
		return nil, nil, fmt.Errorf("building storage provider: %w", err)
	}
	db, err := buildDatabaseProvider(ec.Rule.Source)
	if err != nil {
		return nil, nil, fmt.Errorf("building database provider: %w", err)
	}
	if err := db.Connect(ctx); err != nil {
		return nil, nil, fmt.Errorf("connecting database: %w", err)
	}
	return store, db, nil
}

// ParseRef splits "<namespace>/<name>" into its parts.
func ParseRef(ref string) (namespace, name string, err error) {
	parts := strings.SplitN(ref, "/", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return "", "", fmt.Errorf("invalid CR ref %q: want <namespace>/<name>", ref)
	}
	return parts[0], parts[1], nil
}

func buildStorageProvider(ctx context.Context, cfg config.StorageConfig) (storage.Provider, error) {
	switch cfg.Type {
	case "filesystem":
		return filesystem.New(cfg.Filesystem.BasePath)
	case "s3":
		return s3store.New(ctx, cfg.S3.Bucket, cfg.S3.Region, cfg.S3.Prefix, cfg.S3.Endpoint)
	case "r2":
		return r2store.New(ctx, cfg.R2.AccountID, cfg.R2.Bucket, cfg.R2.Region, cfg.R2.Prefix)
	case "gcs":
		return gcsstore.New(ctx, cfg.GCS.Bucket, cfg.GCS.Prefix)
	default:
		return nil, fmt.Errorf("unsupported storage type: %s", cfg.Type)
	}
}

func buildDatabaseProvider(src config.SourceConfig) (database.Provider, error) {
	user := src.Credentials.Username
	pass := src.Credentials.Password
	switch src.Engine {
	case "postgres":
		return dbpg.New(src.Host, src.Port, src.Database, user, pass, src.SSLMode, src.Pool), nil
	case "mysql":
		return dbmysql.New(src.Host, src.Port, src.Database, user, pass, src.Pool), nil
	case "timescaledb":
		return dbtsdb.New(src.Host, src.Port, src.Database, user, pass, src.SSLMode, src.Pool, nil), nil
	default:
		return nil, fmt.Errorf("unsupported engine: %s", src.Engine)
	}
}
