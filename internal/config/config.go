// Package config provides YAML configuration loading and validation for APR.
package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// Config holds the complete APR configuration including storage, history, and archive rules.
type Config struct {
	Storage StorageConfig `yaml:"storage"`
	History HistoryConfig `yaml:"history"`
	Rules   []Rule        `yaml:"rules"`
}

// StorageConfig defines the storage backend for archived data.
type StorageConfig struct {
	Type       string          `yaml:"type"`
	S3         *S3Config       `yaml:"s3,omitempty"`
	R2         *R2Config       `yaml:"r2,omitempty"`
	GCS        *GCSConfig      `yaml:"gcs,omitempty"`
	Filesystem *FSConfig        `yaml:"filesystem,omitempty"`
	Lifecycle  LifecycleConfig `yaml:"lifecycle"`
}

// S3Config defines S3 storage settings.
type S3Config struct {
	Bucket   string `yaml:"bucket"`
	Region   string `yaml:"region"`
	Prefix   string `yaml:"prefix"`
	Endpoint string `yaml:"endpoint,omitempty"`
}

// R2Config defines Cloudflare R2 storage settings.
type R2Config struct {
	AccountID   string           `yaml:"account_id"`
	Bucket      string           `yaml:"bucket"`
	Region      string           `yaml:"region,omitempty"`
	Prefix      string           `yaml:"prefix,omitempty"`
	Credentials CredentialConfig `yaml:"credentials"`
}

// GCSConfig defines Google Cloud Storage settings.
type GCSConfig struct {
	Bucket string `yaml:"bucket"`
	Prefix string `yaml:"prefix,omitempty"`
}

// FSConfig defines local filesystem storage settings.
type FSConfig struct {
	BasePath string `yaml:"base_path"`
}

// LifecycleConfig defines S3 lifecycle policies for archived data.
type LifecycleConfig struct {
	TransitionDays int `yaml:"transition_days"`
	ExpirationDays int `yaml:"expiration_days"`
}

// HistoryConfig defines the SQLite history database location.
type HistoryConfig struct {
	Path string `yaml:"path"`
}

// Rule defines a single archive configuration rule.
type Rule struct {
	Name      string          `yaml:"name"`
	Schedule  string          `yaml:"schedule"`
	BatchSize int             `yaml:"batch_size"`
	Source    SourceConfig    `yaml:"source"`
	Tables    []TableConfig   `yaml:"tables"`
}

// SourceConfig defines database connection settings.
type SourceConfig struct {
	Engine      string           `yaml:"engine"`
	Host        string           `yaml:"host"`
	Port        int              `yaml:"port"`
	Database    string           `yaml:"database"`
	Credentials CredentialConfig `yaml:"credentials"`
	SSLMode     string           `yaml:"ssl_mode,omitempty"`
	Pool        PoolConfig       `yaml:"pool,omitempty"`
}

// PoolConfig defines database connection pool settings.
// Zero values mean the Go database/sql defaults are used.
type PoolConfig struct {
	MaxOpenConns    int           `yaml:"max_open_conns"`
	MaxIdleConns    int           `yaml:"max_idle_conns"`
	ConnMaxLifetime time.Duration `yaml:"conn_max_lifetime"`
	ConnMaxIdleTime time.Duration `yaml:"conn_max_idle_time"`
}

// CredentialConfig defines how to obtain database credentials.
type CredentialConfig struct {
	Type        string `yaml:"type"`
	UsernameEnv string `yaml:"username_env,omitempty"`
	PasswordEnv string `yaml:"password_env,omitempty"`
	Username    string `yaml:"username,omitempty"`
	Password    string `yaml:"password,omitempty"`
}

// TableConfig defines a single table to archive within a rule.
type TableConfig struct {
	Name       string `yaml:"name"`
	DateColumn string `yaml:"date_column"`
	DaysOnline int    `yaml:"days_online"`
}

// Load loads configuration from the specified path or searches default locations.
// If path is empty, searches: ./apr.yaml, ~/.apr/config.yaml, /etc/apr/config.yaml
func Load(path string) (*Config, error) {
	if path != "" {
		return loadFromFile(path)
	}

	searchPaths := []string{
		"apr.yaml",
		"apr.yml",
	}

	if home, err := os.UserHomeDir(); err == nil {
		searchPaths = append(searchPaths, filepath.Join(home, ".apr", "config.yaml"), filepath.Join(home, ".apr", "config.yml"),
		)
	}

	searchPaths = append(searchPaths, "/etc/apr/config.yaml", "/etc/apr/config.yml",
	)

	for _, p := range searchPaths {
		if _, err := os.Stat(p); err == nil {
			return loadFromFile(p)
		}
	}

	return nil, fmt.Errorf("no config file found; searched: %s", strings.Join(searchPaths, ", "))
}

func loadFromFile(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading config file %s: %w", path, err)
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parsing config file %s: %w", path, err)
	}

	cfg.applyDefaults()
	return &cfg, nil
}

// applyDefaults sets default values for unset configuration fields.
// History path precedence: config file > APR_HISTORY_PATH env var > ~/.apr/history.db > /var/lib/apr/history.db
func (c *Config) applyDefaults() {
	// Check APR_HISTORY_PATH env var before falling back to home dir
	if c.History.Path == "" {
		if envPath := os.Getenv("APR_HISTORY_PATH"); envPath != "" {
			c.History.Path = filepath.Clean(envPath)
		} else if home, err := os.UserHomeDir(); err == nil {
			c.History.Path = filepath.Join(home, ".apr", "history.db")
		} else {
			c.History.Path = "/var/lib/apr/history.db"
		}
	}

	for i := range c.Rules {
		if c.Rules[i].BatchSize == 0 {
			c.Rules[i].BatchSize = 10000
		}
		if c.Rules[i].Source.SSLMode == "" && c.Rules[i].Source.Engine == "postgres" {
			c.Rules[i].Source.SSLMode = "prefer"
		}
	}
}

// Validate checks the configuration for errors and required fields.
func (c *Config) Validate() error {
	if c.Storage.Type == "" {
		return fmt.Errorf("storage.type is required")
	}

	switch c.Storage.Type {
	case "s3":
		if c.Storage.S3 == nil {
			return fmt.Errorf("storage.s3 configuration is required when type is s3")
		}
		if c.Storage.S3.Bucket == "" {
			return fmt.Errorf("storage.s3.bucket is required")
		}
		if c.Storage.S3.Region == "" {
			return fmt.Errorf("storage.s3.region is required")
		}
	case "r2":
		if c.Storage.R2 == nil {
			return fmt.Errorf("storage.r2 configuration is required when type is r2")
		}
		if c.Storage.R2.AccountID == "" {
			return fmt.Errorf("storage.r2.account_id is required")
		}
		if c.Storage.R2.Bucket == "" {
			return fmt.Errorf("storage.r2.bucket is required")
		}
	case "gcs":
		if c.Storage.GCS == nil {
			return fmt.Errorf("storage.gcs configuration is required when type is gcs")
		}
		if c.Storage.GCS.Bucket == "" {
			return fmt.Errorf("storage.gcs.bucket is required")
		}
	case "filesystem":
		if c.Storage.Filesystem == nil {
			return fmt.Errorf("storage.filesystem configuration is required when type is filesystem")
		}
		if c.Storage.Filesystem.BasePath == "" {
			return fmt.Errorf("storage.filesystem.base_path is required")
	}
	default:
		return fmt.Errorf("unsupported storage type: %s (must be s3, r2, gcs, or filesystem)", c.Storage.Type)
	}

	if len(c.Rules) == 0 {
		return fmt.Errorf("at least one rule is required")
	}

	ruleNames := make(map[string]bool)
	for i, rule := range c.Rules {
		if rule.Name == "" {
			return fmt.Errorf("rules[%d].name is required", i)
		}
		if ruleNames[rule.Name] {
			return fmt.Errorf("duplicate rule name: %s", rule.Name)
		}
		ruleNames[rule.Name] = true

		if err := validateRule(rule, i); err != nil {
			return err
		}
	}

	return nil
}

func validateRule(rule Rule, index int) error {
	prefix := fmt.Sprintf("rules[%d] (%s)", index, rule.Name)

	if rule.Source.Engine == "" {
		return fmt.Errorf("%s: source.engine is required", prefix)
	}

	switch rule.Source.Engine {
	case "postgres", "mysql":
		// ok
	default:
		return fmt.Errorf("%s: unsupported engine: %s (must be postgres or mysql)", prefix, rule.Source.Engine)
	}

	if rule.Source.Host == "" {
		return fmt.Errorf("%s: source.host is required", prefix)
	}
	if rule.Source.Port == 0 {
		return fmt.Errorf("%s: source.port is required", prefix)
	}
	if rule.Source.Database == "" {
		return fmt.Errorf("%s: source.database is required", prefix)
	}
	if rule.Source.Credentials.Type == "" {
		return fmt.Errorf("%s: source.credentials.type is required", prefix)
	}

	if len(rule.Tables) == 0 {
		return fmt.Errorf("%s: at least one table is required", prefix)
	}

	for j, table := range rule.Tables {
		tPrefix := fmt.Sprintf("%s.tables[%d]", prefix, j)
		if table.Name == "" {
			return fmt.Errorf("%s: name is required", tPrefix)
		}
		if table.DateColumn == "" {
			return fmt.Errorf("%s: date_column is required", tPrefix)
		}
		if table.DaysOnline <= 0 {
			return fmt.Errorf("%s: days_online must be positive", tPrefix)
		}
	}

	if rule.BatchSize <= 0 {
		return fmt.Errorf("%s: batch_size must be positive", prefix)
	}

	return nil
}

// FindRule returns the rule with the specified name, or nil if not found.
func (c *Config) FindRule(name string) *Rule {
	for i := range c.Rules {
		if c.Rules[i].Name == name {
			return &c.Rules[i]
		}
	}
	return nil
}