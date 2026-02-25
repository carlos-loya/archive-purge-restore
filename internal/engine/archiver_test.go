package engine

import (
	"context"
	"log"
	"os"
	"testing"
	"time"

	"github.com/carlos-loya/archive-purge-restore/internal/config"
	"github.com/carlos-loya/archive-purge-restore/internal/provider/database"
	"github.com/carlos-loya/archive-purge-restore/internal/provider/storage/filesystem"
)

// mockDB implements database.Provider for testing.
type mockDB struct {
	schema    []database.ColumnInfo
	pkCols    []string
	rows      []database.Row
	deleted   [][]any
	restored  []database.Row
}

func (m *mockDB) Connect(ctx context.Context) error { return nil }
func (m *mockDB) Close() error                      { return nil }

func (m *mockDB) InferSchema(ctx context.Context, table string) ([]database.ColumnInfo, error) {
	return m.schema, nil
}

func (m *mockDB) InferPrimaryKey(ctx context.Context, table string) ([]string, error) {
	return m.pkCols, nil
}

func (m *mockDB) ExtractRows(ctx context.Context, table, dateColumn string, before time.Time, batchSize int) (database.RowIterator, error) {
	// Return rows that are before the cutoff, then clear them.
	var matching []database.Row
	var remaining []database.Row
	for _, r := range m.rows {
		if len(matching) < batchSize {
			matching = append(matching, r)
		} else {
			remaining = append(remaining, r)
		}
	}
	m.rows = remaining
	return &mockIterator{columns: m.schema, rows: matching}, nil
}

func (m *mockDB) DeleteRows(ctx context.Context, table string, pkColumns []string, pkValues [][]any) (int64, error) {
	m.deleted = append(m.deleted, pkValues...)
	return int64(len(pkValues)), nil
}

func (m *mockDB) RestoreRows(ctx context.Context, table string, columns []string, rows []database.Row) (int64, error) {
	m.restored = append(m.restored, rows...)
	return int64(len(rows)), nil
}

type mockIterator struct {
	columns []database.ColumnInfo
	rows    []database.Row
	idx     int
}

func (mi *mockIterator) Columns() []database.ColumnInfo { return mi.columns }
func (mi *mockIterator) Next() bool {
	mi.idx++
	return mi.idx <= len(mi.rows)
}
func (mi *mockIterator) Row() database.Row { return mi.rows[mi.idx-1] }
func (mi *mockIterator) Err() error        { return nil }
func (mi *mockIterator) Close() error      { return nil }

func TestArchiveBasic(t *testing.T) {
	dir := t.TempDir()
	store, err := filesystem.New(dir)
	if err != nil {
		t.Fatal(err)
	}

	logger := log.New(os.Stderr, "[test] ", log.LstdFlags)
	archiver := NewArchiver(store, logger)

	db := &mockDB{
		schema: []database.ColumnInfo{
			{Name: "id", Type: "int4", Nullable: false},
			{Name: "name", Type: "text", Nullable: false},
			{Name: "created_at", Type: "text", Nullable: false},
		},
		pkCols: []string{"id"},
		rows: []database.Row{
			{"id": int32(1), "name": "Alice", "created_at": "2025-01-01"},
			{"id": int32(2), "name": "Bob", "created_at": "2025-01-02"},
			{"id": int32(3), "name": "Charlie", "created_at": "2025-01-03"},
		},
	}

	rule := config.Rule{
		Name:      "test-rule",
		BatchSize: 100,
		Source: config.SourceConfig{
			Database: "testdb",
		},
		Tables: []config.TableConfig{
			{Name: "users", DateColumn: "created_at", DaysOnline: 30},
		},
	}

	ctx := context.Background()
	result, err := archiver.Archive(ctx, rule, db)
	if err != nil {
		t.Fatalf("Archive() error: %v", err)
	}

	if result.RunID == "" {
		t.Error("RunID should not be empty")
	}
	if result.Rule != "test-rule" {
		t.Errorf("Rule = %q, want %q", result.Rule, "test-rule")
	}
	if len(result.Tables) != 1 {
		t.Fatalf("len(Tables) = %d, want 1", len(result.Tables))
	}
	if result.Tables[0].RowsArchived != 3 {
		t.Errorf("RowsArchived = %d, want 3", result.Tables[0].RowsArchived)
	}
	if result.Tables[0].RowsDeleted != 3 {
		t.Errorf("RowsDeleted = %d, want 3", result.Tables[0].RowsDeleted)
	}
	if len(result.Tables[0].Files) != 1 {
		t.Errorf("len(Files) = %d, want 1", len(result.Tables[0].Files))
	}

	// Verify file was finalized (not .pending).
	objects, err := store.List(ctx, "testdb/users/")
	if err != nil {
		t.Fatal(err)
	}
	if len(objects) != 1 {
		t.Fatalf("expected 1 archive file, got %d", len(objects))
	}
	for _, obj := range objects {
		if obj.Key[len(obj.Key)-8:] == ".pending" {
			t.Errorf("file still has .pending suffix: %s", obj.Key)
		}
	}
}

func TestArchiveNoRows(t *testing.T) {
	dir := t.TempDir()
	store, err := filesystem.New(dir)
	if err != nil {
		t.Fatal(err)
	}

	logger := log.New(os.Stderr, "[test] ", log.LstdFlags)
	archiver := NewArchiver(store, logger)

	db := &mockDB{
		schema: []database.ColumnInfo{
			{Name: "id", Type: "int4"},
		},
		pkCols: []string{"id"},
		rows:   nil,
	}

	rule := config.Rule{
		Name:      "empty-rule",
		BatchSize: 100,
		Source:    config.SourceConfig{Database: "testdb"},
		Tables:    []config.TableConfig{{Name: "empty", DateColumn: "created_at", DaysOnline: 30}},
	}

	result, err := archiver.Archive(context.Background(), rule, db)
	if err != nil {
		t.Fatalf("Archive() error: %v", err)
	}
	if len(result.Tables) != 1 {
		t.Fatalf("len(Tables) = %d, want 1", len(result.Tables))
	}
	if result.Tables[0].RowsArchived != 0 {
		t.Errorf("RowsArchived = %d, want 0", result.Tables[0].RowsArchived)
	}
}

func TestArchiveDryRun(t *testing.T) {
	dir := t.TempDir()
	store, err := filesystem.New(dir)
	if err != nil {
		t.Fatal(err)
	}

	logger := log.New(os.Stderr, "[test] ", log.LstdFlags)
	archiver := NewArchiver(store, logger)

	db := &mockDB{
		schema: []database.ColumnInfo{
			{Name: "id", Type: "int4", Nullable: false},
			{Name: "name", Type: "text", Nullable: false},
			{Name: "created_at", Type: "text", Nullable: false},
		},
		pkCols: []string{"id"},
		rows: []database.Row{
			{"id": int32(1), "name": "Alice", "created_at": "2025-01-01"},
			{"id": int32(2), "name": "Bob", "created_at": "2025-01-02"},
			{"id": int32(3), "name": "Charlie", "created_at": "2025-01-03"},
		},
	}

	rule := config.Rule{
		Name:      "test-rule",
		BatchSize: 100,
		Source: config.SourceConfig{
			Database: "testdb",
		},
		Tables: []config.TableConfig{
			{Name: "users", DateColumn: "created_at", DaysOnline: 30},
		},
	}

	ctx := context.Background()
	result, err := archiver.ArchiveDryRun(ctx, rule, db)
	if err != nil {
		t.Fatalf("ArchiveDryRun() error: %v", err)
	}

	if result.Rule != "test-rule" {
		t.Errorf("Rule = %q, want %q", result.Rule, "test-rule")
	}
	if len(result.Tables) != 1 {
		t.Fatalf("len(Tables) = %d, want 1", len(result.Tables))
	}
	if result.Tables[0].Table != "users" {
		t.Errorf("Table = %q, want %q", result.Tables[0].Table, "users")
	}
	if result.Tables[0].Count != 3 {
		t.Errorf("Count = %d, want 3", result.Tables[0].Count)
	}
	if result.Tables[0].Cutoff.IsZero() {
		t.Error("Cutoff should not be zero")
	}

	// Verify no files were written to storage.
	objects, err := store.List(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(objects) != 0 {
		t.Errorf("expected 0 archive files in dry-run, got %d", len(objects))
	}

	// Verify no rows were deleted.
	if len(db.deleted) != 0 {
		t.Errorf("expected 0 deleted rows in dry-run, got %d", len(db.deleted))
	}
}

func TestArchiveDryRunNoRows(t *testing.T) {
	dir := t.TempDir()
	store, err := filesystem.New(dir)
	if err != nil {
		t.Fatal(err)
	}

	logger := log.New(os.Stderr, "[test] ", log.LstdFlags)
	archiver := NewArchiver(store, logger)

	db := &mockDB{
		schema: []database.ColumnInfo{
			{Name: "id", Type: "int4"},
		},
		pkCols: []string{"id"},
		rows:   nil,
	}

	rule := config.Rule{
		Name:      "empty-rule",
		BatchSize: 100,
		Source:    config.SourceConfig{Database: "testdb"},
		Tables:    []config.TableConfig{{Name: "empty", DateColumn: "created_at", DaysOnline: 30}},
	}

	result, err := archiver.ArchiveDryRun(context.Background(), rule, db)
	if err != nil {
		t.Fatalf("ArchiveDryRun() error: %v", err)
	}
	if len(result.Tables) != 1 {
		t.Fatalf("len(Tables) = %d, want 1", len(result.Tables))
	}
	if result.Tables[0].Count != 0 {
		t.Errorf("Count = %d, want 0", result.Tables[0].Count)
	}
}

func TestArchiveDryRunMultipleTables(t *testing.T) {
	dir := t.TempDir()
	store, err := filesystem.New(dir)
	if err != nil {
		t.Fatal(err)
	}

	logger := log.New(os.Stderr, "[test] ", log.LstdFlags)
	archiver := NewArchiver(store, logger)

	db := &mockDB{
		schema: []database.ColumnInfo{
			{Name: "id", Type: "int4", Nullable: false},
			{Name: "val", Type: "text", Nullable: false},
			{Name: "created_at", Type: "text", Nullable: false},
		},
		pkCols: []string{"id"},
		rows: []database.Row{
			{"id": int32(1), "val": "a", "created_at": "2025-01-01"},
			{"id": int32(2), "val": "b", "created_at": "2025-01-02"},
		},
	}

	rule := config.Rule{
		Name:      "multi-table-rule",
		BatchSize: 100,
		Source:    config.SourceConfig{Database: "testdb"},
		Tables: []config.TableConfig{
			{Name: "orders", DateColumn: "created_at", DaysOnline: 30},
			{Name: "order_items", DateColumn: "created_at", DaysOnline: 30},
		},
	}

	result, err := archiver.ArchiveDryRun(context.Background(), rule, db)
	if err != nil {
		t.Fatalf("ArchiveDryRun() error: %v", err)
	}
	if len(result.Tables) != 2 {
		t.Fatalf("len(Tables) = %d, want 2", len(result.Tables))
	}
	if result.Tables[0].Table != "orders" {
		t.Errorf("Tables[0].Table = %q, want %q", result.Tables[0].Table, "orders")
	}
	// The first table consumes the rows, second table gets 0 (mock behavior).
	if result.Tables[0].Count != 2 {
		t.Errorf("Tables[0].Count = %d, want 2", result.Tables[0].Count)
	}
	if result.Tables[1].Table != "order_items" {
		t.Errorf("Tables[1].Table = %q, want %q", result.Tables[1].Table, "order_items")
	}
	if result.Tables[1].Count != 0 {
		t.Errorf("Tables[1].Count = %d, want 0", result.Tables[1].Count)
	}
}
