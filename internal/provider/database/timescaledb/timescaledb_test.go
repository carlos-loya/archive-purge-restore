package timescaledb

import (
	"testing"

	"github.com/carlos-loya/archive-purge-restore/internal/config"
)

func TestNew(t *testing.T) {
	p := New("localhost", 5432, "metrics", "user", "pass", "prefer", config.PoolConfig{}, nil)
	if p == nil {
		t.Fatal("New() returned nil")
	}
	if p.Provider == nil {
		t.Fatal("embedded Postgres provider is nil")
	}
	if p.log == nil {
		t.Fatal("logger is nil")
	}
}

func TestNewDefaultLogger(t *testing.T) {
	p := New("localhost", 5432, "metrics", "user", "pass", "", config.PoolConfig{}, nil)
	if p.log == nil {
		t.Error("expected default logger, got nil")
	}
}
