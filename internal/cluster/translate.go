// SPDX-License-Identifier: MIT
// Copyright (c) 2026 Carlos Loya

package cluster

import (
	"fmt"
	"os"
	"strings"

	aprv1alpha1 "github.com/carlos-loya/archive-purge-restore/api/v1alpha1"
	"github.com/carlos-loya/archive-purge-restore/internal/config"
	"github.com/carlos-loya/archive-purge-restore/internal/controller"
)

// EngineConfig bundles the synthetic config produced from CRs so it can be
// fed directly into the existing engine + provider plumbing.
type EngineConfig struct {
	Config *config.Config
	Rule   *config.Rule
}

// TranslateArchive produces an EngineConfig from an ArchiveRule and its
// referenced DatabaseConnection / StorageBackend. Database credentials are
// read from process env vars per the controller env-var ABI.
func TranslateArchive(
	rule *aprv1alpha1.ArchiveRule,
	dbc *aprv1alpha1.DatabaseConnection,
	sb *aprv1alpha1.StorageBackend,
) (*EngineConfig, error) {
	src, err := translateSource(dbc)
	if err != nil {
		return nil, err
	}
	storage, err := translateStorage(sb)
	if err != nil {
		return nil, err
	}

	batchSize := int(rule.Spec.BatchSize)
	if batchSize == 0 {
		batchSize = 10000
	}

	cfg := &config.Config{
		Storage: storage,
		Rules: []config.Rule{{
			Name:      rule.Name,
			BatchSize: batchSize,
			Source:    src,
			Tables: []config.TableConfig{{
				Name:       rule.Spec.Table,
				DateColumn: rule.Spec.DateColumn,
				DaysOnline: int(rule.Spec.DaysOnline),
			}},
		}},
	}
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("translated config invalid: %w", err)
	}
	return &EngineConfig{Config: cfg, Rule: &cfg.Rules[0]}, nil
}

func translateSource(dbc *aprv1alpha1.DatabaseConnection) (config.SourceConfig, error) {
	user := os.Getenv(controller.EnvDatabaseUsername)
	pass := os.Getenv(controller.EnvDatabasePassword)
	if user == "" || pass == "" {
		return config.SourceConfig{}, fmt.Errorf(
			"database credentials missing: env vars %s and %s must be set",
			controller.EnvDatabaseUsername, controller.EnvDatabasePassword)
	}

	port := int(dbc.Spec.Port)
	if port == 0 {
		port = defaultPort(dbc.Spec.Engine)
	}

	sslMode := dbc.Spec.SSLMode
	if sslMode == "" && (dbc.Spec.Engine == aprv1alpha1.EnginePostgres ||
		dbc.Spec.Engine == aprv1alpha1.EngineTimescaleDB) {
		sslMode = "prefer"
	}

	return config.SourceConfig{
		Engine:   string(dbc.Spec.Engine),
		Host:     dbc.Spec.Host,
		Port:     port,
		Database: dbc.Spec.Database,
		SSLMode:  sslMode,
		Credentials: config.CredentialConfig{
			Type:     "static",
			Username: user,
			Password: pass,
		},
	}, nil
}

func defaultPort(engine aprv1alpha1.DatabaseEngine) int {
	switch engine {
	case aprv1alpha1.EngineMySQL:
		return 3306
	case aprv1alpha1.EnginePostgres, aprv1alpha1.EngineTimescaleDB:
		return 5432
	default:
		return 0
	}
}

func translateStorage(sb *aprv1alpha1.StorageBackend) (config.StorageConfig, error) {
	sc := config.StorageConfig{Type: string(sb.Spec.Type)}
	switch sb.Spec.Type {
	case aprv1alpha1.StorageS3:
		sc.S3 = &config.S3Config{
			Bucket:   sb.Spec.Bucket,
			Region:   sb.Spec.Region,
			Prefix:   sb.Spec.Prefix,
			Endpoint: sb.Spec.Endpoint,
		}
	case aprv1alpha1.StorageR2:
		sc.R2 = &config.R2Config{
			AccountID: sb.Spec.AccountID,
			Bucket:    sb.Spec.Bucket,
			Region:    sb.Spec.Region,
			Prefix:    sb.Spec.Prefix,
		}
	case aprv1alpha1.StorageGCS:
		sc.GCS = &config.GCSConfig{
			Bucket: sb.Spec.Bucket,
			Prefix: sb.Spec.Prefix,
		}
	case aprv1alpha1.StorageFilesystem:
		sc.Filesystem = &config.FSConfig{BasePath: sb.Spec.Bucket}
	default:
		return sc, fmt.Errorf("unsupported storage type: %s", sb.Spec.Type)
	}
	return sc, nil
}

// PrepareStorageEnvironment performs side-effects that the storage SDKs need
// before a provider is constructed. Today the only case is GCS: the
// service-account JSON arrives as a string env var (per the controller ABI)
// and must be written to a file pointed at by GOOGLE_APPLICATION_CREDENTIALS
// because the GCS Go client expects a file path, not raw JSON.
//
// Returns a cleanup func that removes any temp files created.
func PrepareStorageEnvironment(sb *aprv1alpha1.StorageBackend) (cleanup func(), err error) {
	cleanup = func() {}
	if sb.Spec.Type != aprv1alpha1.StorageGCS {
		return cleanup, nil
	}
	saJSON := os.Getenv(controller.EnvStorageServiceAccountJSON)
	if strings.TrimSpace(saJSON) == "" {
		return cleanup, nil
	}
	f, err := os.CreateTemp("", "apr-gcs-sa-*.json")
	if err != nil {
		return cleanup, fmt.Errorf("creating GCS credentials file: %w", err)
	}
	if _, err := f.WriteString(saJSON); err != nil {
		_ = f.Close()
		_ = os.Remove(f.Name())
		return cleanup, fmt.Errorf("writing GCS credentials file: %w", err)
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(f.Name())
		return cleanup, fmt.Errorf("closing GCS credentials file: %w", err)
	}
	if err := os.Setenv("GOOGLE_APPLICATION_CREDENTIALS", f.Name()); err != nil {
		_ = os.Remove(f.Name())
		return cleanup, fmt.Errorf("setting GOOGLE_APPLICATION_CREDENTIALS: %w", err)
	}
	cleanup = func() { _ = os.Remove(f.Name()) }
	return cleanup, nil
}
