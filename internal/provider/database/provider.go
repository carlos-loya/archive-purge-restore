package database

import (
	"context"
	"io"
	"time"
)

// ColumnInfo describes a database column.
type ColumnInfo struct {
	Name     string
	Type     string // database-specific type name
	Nullable bool
}

// Row represents a single database row as a map of column name to value.
type Row map[string]any

// RowIterator streams rows from a query result.
type RowIterator interface {
	// Columns returns the column metadata for the result set.
	Columns() []ColumnInfo
	// Next advances to the next row. Returns false when exhausted.
	Next() bool
	// Row returns the current row data.
	Row() Row
	// Err returns any error that occurred during iteration.
	Err() error
	io.Closer
}

// Provider abstracts database operations for archive and restore.
type Provider interface {
	// Connect establishes a connection to the database.
	Connect(ctx context.Context) error
	// ExtractRows streams rows older than the cutoff date, ordered by primary key.
	ExtractRows(ctx context.Context, table, dateColumn string, before time.Time, batchSize int) (RowIterator, error)
	// DeleteRows deletes rows by primary key values. Returns the number of rows deleted.
	DeleteRows(ctx context.Context, table string, pkColumns []string, pkValues [][]any) (int64, error)
	// RestoreRows inserts rows back into the table. Returns the number of rows inserted.
	RestoreRows(ctx context.Context, table string, columns []string, rows []Row) (int64, error)
	// InferSchema returns the column information for a table.
	InferSchema(ctx context.Context, table string) ([]ColumnInfo, error)
	// InferPrimaryKey detects the primary key column(s) of a table.
	InferPrimaryKey(ctx context.Context, table string) ([]string, error)
	// Close releases database resources.
	Close() error
}
