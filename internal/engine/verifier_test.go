package engine

import (
	"bytes"
	"context"
	"log/slog"
	"os"
	"testing"

	"github.com/carlos-loya/archive-purge-restore/internal/config"
	"github.com/carlos-loya/archive-purge-restore/internal/format"
	"github.com/carlos-loya/archive-purge-restore/internal/provider/database"
	"github.com/carlos-loya/archive-purge-restore/internal/provider/storage/filesystem"
)

func TestVerifyBasic(t *testing.T) {
	dir := t.TempDir()
	store, err := filesystem.New(dir)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	columns := []database.ColumnInfo{
		{Name: "id", Type: "int4", Nullable: false},
		{Name: "name", Type: "text", Nullable: false},
	}
	rows := []database.Row{
		{"id": int32(1), "name": "Alice"},
		{"id": int32(2), "name": "Bob"},
	}

	data, err := format.WriteParquet(columns, rows)
	if err != nil {
		t.Fatal(err)
	}

	key := "testdb/users/2025-01-01/abc12345_000.parquet"
	if err := store.Put(ctx, key, bytes.NewReader(data)); err != nil {
		t.Fatal(err)
	}

	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	verifier := NewVerifier(store, logger)

	rule := config.Rule{
		Name:   "test-rule",
		Source: config.SourceConfig{Database: "testdb"},
		Tables: []config.TableConfig{
			{Name: "users", DateColumn: "created_at", DaysOnline: 30},
		},
	}

	opts := VerifyOptions{
		Rule:  rule,
		Table: "users",
		Date:  "2025-01-01",
	}

	result, err := verifier.Verify(ctx, opts)
	if err != nil {
		t.Fatalf("Verify() error: %v", err)
	}

	if len(result.Tables) != 1 {
		t.Fatalf("len(Tables) = %d, want 1", len(result.Tables))
	}
	tr := result.Tables[0]
	if len(tr.Files) != 1 {
		t.Fatalf("len(Files) = %d, want 1", len(tr.Files))
	}
	if tr.Files[0].Status != "OK" {
		t.Errorf("Status = %q, want OK", tr.Files[0].Status)
	}
	if tr.Files[0].RowCount != 2 {
		t.Errorf("RowCount = %d, want 2", tr.Files[0].RowCount)
	}
	if tr.TotalRows != 2 {
		t.Errorf("TotalRows = %d, want 2", tr.TotalRows)
	}
}

func TestVerifyCorruptFile(t *testing.T) {
	dir := t.TempDir()
	store, err := filesystem.New(dir)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	// Write a valid file.
	columns := []database.ColumnInfo{
		{Name: "id", Type: "int4", Nullable: false},
	}
	validRows := []database.Row{{"id": int32(1)}}
	validData, err := format.WriteParquet(columns, validRows)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Put(ctx, "testdb/orders/2025-01-01/abc12345_000.parquet", bytes.NewReader(validData)); err != nil {
		t.Fatal(err)
	}

	// Write a corrupt file.
	corruptData := []byte("this is not a parquet file")
	if err := store.Put(ctx, "testdb/orders/2025-01-01/def67890_000.parquet", bytes.NewReader(corruptData)); err != nil {
		t.Fatal(err)
	}

	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	verifier := NewVerifier(store, logger)

	rule := config.Rule{
		Name:   "test-rule",
		Source: config.SourceConfig{Database: "testdb"},
		Tables: []config.TableConfig{
			{Name: "orders", DateColumn: "created_at", DaysOnline: 30},
		},
	}

	opts := VerifyOptions{
		Rule:  rule,
		Table: "orders",
		Date:  "2025-01-01",
	}

	result, err := verifier.Verify(ctx, opts)
	if err != nil {
		t.Fatalf("Verify() error: %v", err)
	}

	tr := result.Tables[0]
	if len(tr.Files) != 2 {
		t.Fatalf("len(Files) = %d, want 2", len(tr.Files))
	}

	var okCount, corruptCount int
	for _, f := range tr.Files {
		switch f.Status {
		case "OK":
			okCount++
			if f.RowCount != 1 {
				t.Errorf("OK file RowCount = %d, want 1", f.RowCount)
			}
			if f.Error != nil {
				t.Errorf("OK file has unexpected error: %v", f.Error)
			}
		case "CORRUPT":
			corruptCount++
			if f.Error == nil {
				t.Error("CORRUPT file should have an error")
			}
		}
	}
	if okCount != 1 {
		t.Errorf("okCount = %d, want 1", okCount)
	}
	if corruptCount != 1 {
		t.Errorf("corruptCount = %d, want 1", corruptCount)
	}
	if tr.TotalRows != 1 {
		t.Errorf("TotalRows = %d, want 1", tr.TotalRows)
	}
}

func TestVerifyNoFiles(t *testing.T) {
	dir := t.TempDir()
	store, _ := filesystem.New(dir)
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	verifier := NewVerifier(store, logger)

	rule := config.Rule{
		Name:   "test-rule",
		Source: config.SourceConfig{Database: "testdb"},
		Tables: []config.TableConfig{
			{Name: "users", DateColumn: "created_at", DaysOnline: 30},
		},
	}

	opts := VerifyOptions{
		Rule:  rule,
		Table: "users",
	}

	_, err := verifier.Verify(context.Background(), opts)
	if err == nil {
		t.Error("Verify() should return error when no files found")
	}
}

func TestVerifyFilterByTable(t *testing.T) {
	dir := t.TempDir()
	store, err := filesystem.New(dir)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	columns := []database.ColumnInfo{
		{Name: "id", Type: "int4", Nullable: false},
	}
	data, err := format.WriteParquet(columns, []database.Row{{"id": int32(1)}})
	if err != nil {
		t.Fatal(err)
	}

	// Files for two tables.
	store.Put(ctx, "testdb/users/2025-01-01/abc12345_000.parquet", bytes.NewReader(data))
	store.Put(ctx, "testdb/orders/2025-01-01/abc12345_000.parquet", bytes.NewReader(data))

	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	verifier := NewVerifier(store, logger)

	rule := config.Rule{
		Name:   "test-rule",
		Source: config.SourceConfig{Database: "testdb"},
		Tables: []config.TableConfig{
			{Name: "users", DateColumn: "created_at", DaysOnline: 30},
			{Name: "orders", DateColumn: "created_at", DaysOnline: 30},
		},
	}

	// Verify only the users table.
	opts := VerifyOptions{
		Rule:  rule,
		Table: "users",
	}

	result, err := verifier.Verify(ctx, opts)
	if err != nil {
		t.Fatalf("Verify() error: %v", err)
	}

	if len(result.Tables) != 1 {
		t.Fatalf("len(Tables) = %d, want 1", len(result.Tables))
	}
	if result.Tables[0].Table != "users" {
		t.Errorf("Table = %q, want users", result.Tables[0].Table)
	}
}

func TestVerifyFilterByRunID(t *testing.T) {
	dir := t.TempDir()
	store, err := filesystem.New(dir)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	columns := []database.ColumnInfo{
		{Name: "id", Type: "int4", Nullable: false},
	}

	data1, _ := format.WriteParquet(columns, []database.Row{{"id": int32(1)}})
	data2, _ := format.WriteParquet(columns, []database.Row{{"id": int32(2)}, {"id": int32(3)}})

	store.Put(ctx, "testdb/items/2025-01-01/run11111_000.parquet", bytes.NewReader(data1))
	store.Put(ctx, "testdb/items/2025-01-01/run22222_000.parquet", bytes.NewReader(data2))

	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	verifier := NewVerifier(store, logger)

	rule := config.Rule{
		Name:   "test-rule",
		Source: config.SourceConfig{Database: "testdb"},
		Tables: []config.TableConfig{
			{Name: "items", DateColumn: "created_at", DaysOnline: 30},
		},
	}

	opts := VerifyOptions{
		Rule:  rule,
		Table: "items",
		RunID: "run11111",
	}

	result, err := verifier.Verify(ctx, opts)
	if err != nil {
		t.Fatalf("Verify() error: %v", err)
	}

	tr := result.Tables[0]
	if len(tr.Files) != 1 {
		t.Fatalf("len(Files) = %d, want 1", len(tr.Files))
	}
	if tr.TotalRows != 1 {
		t.Errorf("TotalRows = %d, want 1", tr.TotalRows)
	}
}

func TestVerifyTableNotFound(t *testing.T) {
	dir := t.TempDir()
	store, _ := filesystem.New(dir)
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	verifier := NewVerifier(store, logger)

	rule := config.Rule{
		Name:   "test-rule",
		Source: config.SourceConfig{Database: "testdb"},
		Tables: []config.TableConfig{
			{Name: "users", DateColumn: "created_at", DaysOnline: 30},
		},
	}

	opts := VerifyOptions{
		Rule:  rule,
		Table: "nonexistent",
	}

	_, err := verifier.Verify(context.Background(), opts)
	if err == nil {
		t.Error("Verify() should return error for nonexistent table")
	}
}
