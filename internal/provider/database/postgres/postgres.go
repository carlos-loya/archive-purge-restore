package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/carlos-loya/archive-purge-restore/internal/config"
	"github.com/carlos-loya/archive-purge-restore/internal/provider/database"
	_ "github.com/lib/pq"
)

// Provider implements database.Provider for PostgreSQL.
type Provider struct {
	dsn  string
	pool config.PoolConfig
	db   *sql.DB
}

// New creates a new PostgreSQL provider.
func New(host string, port int, dbname, user, password, sslMode string, pool config.PoolConfig) *Provider {
	if sslMode == "" {
		sslMode = "prefer"
	}
	dsn := fmt.Sprintf("host=%s port=%d dbname=%s user=%s password=%s sslmode=%s",
		host, port, dbname, user, password, sslMode)
	return &Provider{dsn: dsn, pool: pool}
}

// NewFromDSN creates a PostgreSQL provider from a DSN string.
func NewFromDSN(dsn string) *Provider {
	return &Provider{dsn: dsn}
}

func (p *Provider) Connect(ctx context.Context) error {
	db, err := sql.Open("postgres", p.dsn)
	if err != nil {
		return fmt.Errorf("opening postgres connection: %w", err)
	}
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return fmt.Errorf("pinging postgres: %w", err)
	}
	if p.pool.MaxOpenConns != 0 {
		db.SetMaxOpenConns(p.pool.MaxOpenConns)
	}
	if p.pool.MaxIdleConns != 0 {
		db.SetMaxIdleConns(p.pool.MaxIdleConns)
	}
	if p.pool.ConnMaxLifetime != 0 {
		db.SetConnMaxLifetime(p.pool.ConnMaxLifetime)
	}
	if p.pool.ConnMaxIdleTime != 0 {
		db.SetConnMaxIdleTime(p.pool.ConnMaxIdleTime)
	}
	p.db = db
	return nil
}

// DB returns the underlying *sql.DB connection. This is used by
// providers that embed PostgreSQL (e.g., TimescaleDB).
func (p *Provider) DB() *sql.DB {
	return p.db
}

func (p *Provider) Close() error {
	if p.db != nil {
		return p.db.Close()
	}
	return nil
}

func (p *Provider) InferPrimaryKey(ctx context.Context, table string) ([]string, error) {
	query := `
		SELECT a.attname
		FROM pg_index i
		JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
		WHERE i.indrelid = $1::regclass AND i.indisprimary
		ORDER BY array_position(i.indkey, a.attnum)`

	rows, err := p.db.QueryContext(ctx, query, table)
	if err != nil {
		return nil, fmt.Errorf("querying primary key for %s: %w", table, err)
	}
	defer rows.Close()

	var pks []string
	for rows.Next() {
		var col string
		if err := rows.Scan(&col); err != nil {
			return nil, fmt.Errorf("scanning primary key column: %w", err)
		}
		pks = append(pks, col)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating primary key columns: %w", err)
	}

	if len(pks) == 0 {
		return nil, fmt.Errorf("table %s has no primary key", table)
	}
	return pks, nil
}

func (p *Provider) InferSchema(ctx context.Context, table string) ([]database.ColumnInfo, error) {
	query := `
		SELECT column_name, udt_name, is_nullable
		FROM information_schema.columns
		WHERE table_name = $1
		ORDER BY ordinal_position`

	rows, err := p.db.QueryContext(ctx, query, table)
	if err != nil {
		return nil, fmt.Errorf("querying schema for %s: %w", table, err)
	}
	defer rows.Close()

	var columns []database.ColumnInfo
	for rows.Next() {
		var name, typeName, nullable string
		if err := rows.Scan(&name, &typeName, &nullable); err != nil {
			return nil, fmt.Errorf("scanning column info: %w", err)
		}
		columns = append(columns, database.ColumnInfo{
			Name:     name,
			Type:     typeName,
			Nullable: nullable == "YES",
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating columns: %w", err)
	}

	if len(columns) == 0 {
		return nil, fmt.Errorf("table %s not found or has no columns", table)
	}
	return columns, nil
}

func (p *Provider) ExtractRows(ctx context.Context, table, dateColumn string, before time.Time, batchSize int) (database.RowIterator, error) {
	schema, err := p.InferSchema(ctx, table)
	if err != nil {
		return nil, err
	}

	pks, err := p.InferPrimaryKey(ctx, table)
	if err != nil {
		return nil, err
	}

	orderBy := strings.Join(pks, ", ")
	query := fmt.Sprintf("SELECT * FROM %s WHERE %s < $1 ORDER BY %s LIMIT %d",
		quoteIdent(table), quoteIdent(dateColumn), orderBy, batchSize)

	rows, err := p.db.QueryContext(ctx, query, before)
	if err != nil {
		return nil, fmt.Errorf("extracting rows from %s: %w", table, err)
	}

	return &rowIterator{rows: rows, columns: schema}, nil
}

func (p *Provider) DeleteRows(ctx context.Context, table string, pkColumns []string, pkValues [][]any) (int64, error) {
	if len(pkValues) == 0 {
		return 0, nil
	}

	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("beginning delete transaction: %w", err)
	}
	defer tx.Rollback()

	var totalDeleted int64

	// Build DELETE with IN clause for single-column PKs, or batched row comparisons for composite.
	if len(pkColumns) == 1 {
		totalDeleted, err = deleteSinglePK(ctx, tx, table, pkColumns[0], pkValues)
	} else {
		totalDeleted, err = deleteCompositePK(ctx, tx, table, pkColumns, pkValues)
	}
	if err != nil {
		return 0, err
	}

	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("committing delete transaction: %w", err)
	}
	return totalDeleted, nil
}

func deleteSinglePK(ctx context.Context, tx *sql.Tx, table, pkColumn string, pkValues [][]any) (int64, error) {
	const batchSize = 1000
	var total int64

	for i := 0; i < len(pkValues); i += batchSize {
		end := i + batchSize
		if end > len(pkValues) {
			end = len(pkValues)
		}
		batch := pkValues[i:end]

		placeholders := make([]string, len(batch))
		args := make([]any, len(batch))
		for j, pk := range batch {
			placeholders[j] = fmt.Sprintf("$%d", j+1)
			args[j] = pk[0]
		}

		query := fmt.Sprintf("DELETE FROM %s WHERE %s IN (%s)",
			quoteIdent(table), quoteIdent(pkColumn), strings.Join(placeholders, ","))

		result, err := tx.ExecContext(ctx, query, args...)
		if err != nil {
			return 0, fmt.Errorf("deleting batch from %s: %w", table, err)
		}
		n, _ := result.RowsAffected()
		total += n
	}
	return total, nil
}

func deleteCompositePK(ctx context.Context, tx *sql.Tx, table string, pkColumns []string, pkValues [][]any) (int64, error) {
	const batchSize = 500
	var total int64

	quotedCols := make([]string, len(pkColumns))
	for i, col := range pkColumns {
		quotedCols[i] = quoteIdent(col)
	}
	colTuple := "(" + strings.Join(quotedCols, ", ") + ")"

	for i := 0; i < len(pkValues); i += batchSize {
		end := i + batchSize
		if end > len(pkValues) {
			end = len(pkValues)
		}
		batch := pkValues[i:end]

		var tuples []string
		var args []any
		argIdx := 1
		for _, pk := range batch {
			placeholders := make([]string, len(pkColumns))
			for j := range pkColumns {
				placeholders[j] = fmt.Sprintf("$%d", argIdx)
				args = append(args, pk[j])
				argIdx++
			}
			tuples = append(tuples, "("+strings.Join(placeholders, ", ")+")")
		}

		query := fmt.Sprintf("DELETE FROM %s WHERE %s IN (%s)",
			quoteIdent(table), colTuple, strings.Join(tuples, ", "))

		result, err := tx.ExecContext(ctx, query, args...)
		if err != nil {
			return 0, fmt.Errorf("deleting composite PK batch from %s: %w", table, err)
		}
		n, _ := result.RowsAffected()
		total += n
	}
	return total, nil
}

func (p *Provider) RestoreRows(ctx context.Context, table string, columns []string, rows []database.Row) (int64, error) {
	if len(rows) == 0 {
		return 0, nil
	}

	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("beginning restore transaction: %w", err)
	}
	defer tx.Rollback()

	placeholders := make([]string, len(columns))
	for i := range columns {
		placeholders[i] = fmt.Sprintf("$%d", i+1)
	}

	quotedCols := make([]string, len(columns))
	for i, col := range columns {
		quotedCols[i] = quoteIdent(col)
	}

	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
		quoteIdent(table),
		strings.Join(quotedCols, ", "),
		strings.Join(placeholders, ", "))

	stmt, err := tx.PrepareContext(ctx, query)
	if err != nil {
		return 0, fmt.Errorf("preparing insert statement: %w", err)
	}
	defer stmt.Close()

	var total int64
	for _, row := range rows {
		args := make([]any, len(columns))
		for i, col := range columns {
			args[i] = row[col]
		}
		_, err := stmt.ExecContext(ctx, args...)
		if err != nil {
			return total, fmt.Errorf("inserting row: %w", err)
		}
		total++
	}

	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("committing restore transaction: %w", err)
	}
	return total, nil
}

func quoteIdent(name string) string {
	return fmt.Sprintf(`"%s"`, strings.ReplaceAll(name, `"`, `""`))
}

// rowIterator implements database.RowIterator.
type rowIterator struct {
	rows    *sql.Rows
	columns []database.ColumnInfo
	current database.Row
	err     error
}

func (ri *rowIterator) Columns() []database.ColumnInfo {
	return ri.columns
}

func (ri *rowIterator) Next() bool {
	if !ri.rows.Next() {
		return false
	}

	colNames, err := ri.rows.Columns()
	if err != nil {
		ri.err = err
		return false
	}

	values := make([]any, len(colNames))
	ptrs := make([]any, len(colNames))
	for i := range values {
		ptrs[i] = &values[i]
	}

	if err := ri.rows.Scan(ptrs...); err != nil {
		ri.err = err
		return false
	}

	ri.current = make(database.Row, len(colNames))
	for i, col := range colNames {
		ri.current[col] = values[i]
	}
	return true
}

func (ri *rowIterator) Row() database.Row {
	return ri.current
}

func (ri *rowIterator) Err() error {
	if ri.err != nil {
		return ri.err
	}
	return ri.rows.Err()
}

func (ri *rowIterator) Close() error {
	return ri.rows.Close()
}
