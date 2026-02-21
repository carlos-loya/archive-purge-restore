package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadFromFile(t *testing.T) {
	content := `
storage:
  type: filesystem
  filesystem:
    base_path: /tmp/apr-test
  lifecycle:
    transition_days: 30
    expiration_days: 1460

history:
  path: /tmp/apr-test/history.db

rules:
  - name: test-rule
    schedule: "0 3 * * *"
    batch_size: 5000
    source:
      engine: postgres
      host: localhost
      port: 5432
      database: testdb
      credentials:
        type: env
        username_env: DB_USER
        password_env: DB_PASS
    tables:
      - name: orders
        date_column: created_at
        days_online: 21
      - name: order_items
        date_column: created_at
        days_online: 21
`
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load() error: %v", err)
	}

	if cfg.Storage.Type != "filesystem" {
		t.Errorf("Storage.Type = %q, want %q", cfg.Storage.Type, "filesystem")
	}
	if cfg.Storage.Filesystem.BasePath != "/tmp/apr-test" {
		t.Errorf("Storage.Filesystem.BasePath = %q, want %q", cfg.Storage.Filesystem.BasePath, "/tmp/apr-test")
	}
	if cfg.Storage.Lifecycle.TransitionDays != 30 {
		t.Errorf("Storage.Lifecycle.TransitionDays = %d, want %d", cfg.Storage.Lifecycle.TransitionDays, 30)
	}
	if len(cfg.Rules) != 1 {
		t.Fatalf("len(Rules) = %d, want 1", len(cfg.Rules))
	}

	rule := cfg.Rules[0]
	if rule.Name != "test-rule" {
		t.Errorf("Rule.Name = %q, want %q", rule.Name, "test-rule")
	}
	if rule.BatchSize != 5000 {
		t.Errorf("Rule.BatchSize = %d, want %d", rule.BatchSize, 5000)
	}
	if rule.Source.Engine != "postgres" {
		t.Errorf("Rule.Source.Engine = %q, want %q", rule.Source.Engine, "postgres")
	}
	if len(rule.Tables) != 2 {
		t.Fatalf("len(Tables) = %d, want 2", len(rule.Tables))
	}
	if rule.Tables[0].Name != "orders" {
		t.Errorf("Table[0].Name = %q, want %q", rule.Tables[0].Name, "orders")
	}
	if rule.Tables[0].DaysOnline != 21 {
		t.Errorf("Table[0].DaysOnline = %d, want %d", rule.Tables[0].DaysOnline, 21)
	}
}

func TestDefaultBatchSize(t *testing.T) {
	content := `
storage:
  type: filesystem
  filesystem:
    base_path: /tmp/apr-test
rules:
  - name: test-rule
    source:
      engine: postgres
      host: localhost
      port: 5432
      database: testdb
      credentials:
        type: env
    tables:
      - name: orders
        date_column: created_at
        days_online: 30
`
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load() error: %v", err)
	}

	if cfg.Rules[0].BatchSize != 10000 {
		t.Errorf("default BatchSize = %d, want 10000", cfg.Rules[0].BatchSize)
	}
}

func TestValidate(t *testing.T) {
	tests := []struct {
		name    string
		cfg     Config
		wantErr bool
	}{
		{
			name: "valid filesystem config",
			cfg: Config{
				Storage: StorageConfig{
					Type:       "filesystem",
					Filesystem: &FSConfig{BasePath: "/tmp/test"},
				},
				Rules: []Rule{
					{
						Name:      "r1",
						BatchSize: 1000,
						Source: SourceConfig{
							Engine:   "postgres",
							Host:     "localhost",
							Port:     5432,
							Database: "db",
							Credentials: CredentialConfig{
								Type: "env",
							},
						},
						Tables: []TableConfig{
							{Name: "t1", DateColumn: "created_at", DaysOnline: 30},
						},
					},
				},
			},
			wantErr: false,
		},
		{
			name: "missing storage type",
			cfg: Config{
				Rules: []Rule{{Name: "r1"}},
			},
			wantErr: true,
		},
		{
			name: "unsupported storage type",
			cfg: Config{
				Storage: StorageConfig{Type: "azure"},
				Rules:   []Rule{{Name: "r1"}},
			},
			wantErr: true,
		},
		{
			name: "no rules",
			cfg: Config{
				Storage: StorageConfig{
					Type:       "filesystem",
					Filesystem: &FSConfig{BasePath: "/tmp/test"},
				},
			},
			wantErr: true,
		},
		{
			name: "duplicate rule names",
			cfg: Config{
				Storage: StorageConfig{
					Type:       "filesystem",
					Filesystem: &FSConfig{BasePath: "/tmp/test"},
				},
				Rules: []Rule{
					{Name: "dup", BatchSize: 1000, Source: SourceConfig{Engine: "postgres", Host: "h", Port: 1, Database: "d", Credentials: CredentialConfig{Type: "env"}}, Tables: []TableConfig{{Name: "t", DateColumn: "c", DaysOnline: 1}}},
					{Name: "dup", BatchSize: 1000, Source: SourceConfig{Engine: "postgres", Host: "h", Port: 1, Database: "d", Credentials: CredentialConfig{Type: "env"}}, Tables: []TableConfig{{Name: "t", DateColumn: "c", DaysOnline: 1}}},
				},
			},
			wantErr: true,
		},
		{
			name: "unsupported engine",
			cfg: Config{
				Storage: StorageConfig{
					Type:       "filesystem",
					Filesystem: &FSConfig{BasePath: "/tmp/test"},
				},
				Rules: []Rule{
					{Name: "r1", BatchSize: 1000, Source: SourceConfig{Engine: "oracle", Host: "h", Port: 1, Database: "d", Credentials: CredentialConfig{Type: "env"}}, Tables: []TableConfig{{Name: "t", DateColumn: "c", DaysOnline: 1}}},
				},
			},
			wantErr: true,
		},
		{
			name: "missing table date_column",
			cfg: Config{
				Storage: StorageConfig{
					Type:       "filesystem",
					Filesystem: &FSConfig{BasePath: "/tmp/test"},
				},
				Rules: []Rule{
					{Name: "r1", BatchSize: 1000, Source: SourceConfig{Engine: "postgres", Host: "h", Port: 1, Database: "d", Credentials: CredentialConfig{Type: "env"}}, Tables: []TableConfig{{Name: "t", DaysOnline: 1}}},
				},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cfg.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestFindRule(t *testing.T) {
	cfg := &Config{
		Rules: []Rule{
			{Name: "rule-a"},
			{Name: "rule-b"},
		},
	}

	if r := cfg.FindRule("rule-a"); r == nil || r.Name != "rule-a" {
		t.Error("FindRule(rule-a) failed")
	}
	if r := cfg.FindRule("rule-b"); r == nil || r.Name != "rule-b" {
		t.Error("FindRule(rule-b) failed")
	}
	if r := cfg.FindRule("nonexistent"); r != nil {
		t.Errorf("FindRule(nonexistent) = %v, want nil", r)
	}
}
