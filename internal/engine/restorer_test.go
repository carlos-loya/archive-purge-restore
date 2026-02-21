package engine

import (
	"bytes"
	"context"
	"log"
	"os"
	"testing"

	"github.com/carlos-loya/archive-purge-restore/internal/config"
	"github.com/carlos-loya/archive-purge-restore/internal/format"
	"github.com/carlos-loya/archive-purge-restore/internal/provider/database"
	"github.com/carlos-loya/archive-purge-restore/internal/provider/storage/filesystem"
)

func TestRestoreBasic(t *testing.T) {
	dir := t.TempDir()
	store, err := filesystem.New(dir)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	// Write a Parquet archive file.
	columns := []database.ColumnInfo{
		{Name: "id", Type: "int4", Nullable: false},
		{Name: "name", Type: "text", Nullable: false},
		{Name: "created_at", Type: "text", Nullable: false},
	}
	rows := []database.Row{
		{"id": int32(1), "name": "Alice", "created_at": "2025-01-01"},
		{"id": int32(2), "name": "Bob", "created_at": "2025-01-02"},
	}

	data, err := format.WriteParquet(columns, rows)
	if err != nil {
		t.Fatal(err)
	}

	key := "testdb/users/2025-01-01/abc12345_000.parquet"
	if err := store.Put(ctx, key, bytes.NewReader(data)); err != nil {
		t.Fatal(err)
	}

	// Create mock DB.
	db := &mockDB{
		schema: columns,
		pkCols: []string{"id"},
	}

	logger := log.New(os.Stderr, "[test] ", log.LstdFlags)
	restorer := NewRestorer(store, logger)

	rule := config.Rule{
		Name: "test-rule",
		Source: config.SourceConfig{
			Database: "testdb",
		},
		Tables: []config.TableConfig{
			{Name: "users", DateColumn: "created_at", DaysOnline: 30},
		},
	}

	opts := RestoreOptions{
		Rule:  rule,
		Table: "users",
		Date:  "2025-01-01",
	}

	result, err := restorer.Restore(ctx, opts, db)
	if err != nil {
		t.Fatalf("Restore() error: %v", err)
	}

	if len(result.Tables) != 1 {
		t.Fatalf("len(Tables) = %d, want 1", len(result.Tables))
	}
	if result.Tables[0].RowsRestored != 2 {
		t.Errorf("RowsRestored = %d, want 2", result.Tables[0].RowsRestored)
	}
	if len(result.Tables[0].Files) != 1 {
		t.Errorf("len(Files) = %d, want 1", len(result.Tables[0].Files))
	}

	// Verify rows were "inserted" into mock DB.
	if len(db.restored) != 2 {
		t.Errorf("len(restored) = %d, want 2", len(db.restored))
	}
}

func TestRestoreByRunID(t *testing.T) {
	dir := t.TempDir()
	store, err := filesystem.New(dir)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	columns := []database.ColumnInfo{
		{Name: "id", Type: "int4", Nullable: false},
		{Name: "val", Type: "text", Nullable: false},
	}

	// Write two archive files with different run IDs.
	rows1 := []database.Row{{"id": int32(1), "val": "run1"}}
	rows2 := []database.Row{{"id": int32(2), "val": "run2"}}

	data1, _ := format.WriteParquet(columns, rows1)
	data2, _ := format.WriteParquet(columns, rows2)

	store.Put(ctx, "testdb/items/2025-01-01/run11111_000.parquet", bytes.NewReader(data1))
	store.Put(ctx, "testdb/items/2025-01-01/run22222_000.parquet", bytes.NewReader(data2))

	db := &mockDB{schema: columns, pkCols: []string{"id"}}
	logger := log.New(os.Stderr, "[test] ", log.LstdFlags)
	restorer := NewRestorer(store, logger)

	rule := config.Rule{
		Name:   "test-rule",
		Source: config.SourceConfig{Database: "testdb"},
		Tables: []config.TableConfig{
			{Name: "items", DateColumn: "created_at", DaysOnline: 30},
		},
	}

	opts := RestoreOptions{
		Rule:  rule,
		Table: "items",
		Date:  "2025-01-01",
		RunID: "run11111",
	}

	result, err := restorer.Restore(ctx, opts, db)
	if err != nil {
		t.Fatalf("Restore() error: %v", err)
	}

	if result.Tables[0].RowsRestored != 1 {
		t.Errorf("RowsRestored = %d, want 1 (only run11111)", result.Tables[0].RowsRestored)
	}
}

func TestRestoreTableNotFound(t *testing.T) {
	dir := t.TempDir()
	store, _ := filesystem.New(dir)
	logger := log.New(os.Stderr, "[test] ", log.LstdFlags)
	restorer := NewRestorer(store, logger)

	rule := config.Rule{
		Name:   "test-rule",
		Source: config.SourceConfig{Database: "testdb"},
		Tables: []config.TableConfig{
			{Name: "users", DateColumn: "created_at", DaysOnline: 30},
		},
	}

	opts := RestoreOptions{
		Rule:  rule,
		Table: "nonexistent",
	}

	_, err := restorer.Restore(context.Background(), opts, &mockDB{})
	if err == nil {
		t.Error("Restore() should return error for nonexistent table")
	}
}
