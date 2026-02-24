//go:build integration

package integration

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"testing"

	"github.com/carlos-loya/archive-purge-restore/internal/config"
	"github.com/carlos-loya/archive-purge-restore/internal/engine"
	"github.com/carlos-loya/archive-purge-restore/internal/provider/database/mysql"
	"github.com/carlos-loya/archive-purge-restore/internal/provider/database/postgres"
	"github.com/carlos-loya/archive-purge-restore/internal/provider/storage/filesystem"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
)

const (
	pgHost = "localhost"
	pgPort = 15432
	pgDB   = "apr_test_pg"
	pgUser = "apr_test"
	pgPass = "apr_test_pass"

	myHost = "localhost"
	myPort = 13306
	myDB   = "apr_test_my"
	myUser = "apr_test"
	myPass = "apr_test_pass"
)

func pgRule(tables []config.TableConfig) config.Rule {
	return config.Rule{
		Name:      "pg-test",
		BatchSize: 100,
		Source: config.SourceConfig{
			Engine:   "postgres",
			Host:     pgHost,
			Port:     pgPort,
			Database: pgDB,
			SSLMode:  "disable",
			Credentials: config.CredentialConfig{
				Type:     "static",
				Username: pgUser,
				Password: pgPass,
			},
		},
		Tables: tables,
	}
}

func myRule(tables []config.TableConfig) config.Rule {
	return config.Rule{
		Name:      "my-test",
		BatchSize: 100,
		Source: config.SourceConfig{
			Engine:   "mysql",
			Host:     myHost,
			Port:     myPort,
			Database: myDB,
			Credentials: config.CredentialConfig{
				Type:     "static",
				Username: myUser,
				Password: myPass,
			},
		},
		Tables: tables,
	}
}

var ordersTbl = config.TableConfig{
	Name:       "orders",
	DateColumn: "created_at",
	DaysOnline: 30,
}

var orderItemsTbl = config.TableConfig{
	Name:       "order_items",
	DateColumn: "created_at",
	DaysOnline: 30,
}

// resetPostgres truncates tables and re-seeds data.
func resetPostgres(t *testing.T) {
	t.Helper()
	dsn := fmt.Sprintf("host=%s port=%d dbname=%s user=%s password=%s sslmode=disable",
		pgHost, pgPort, pgDB, pgUser, pgPass)
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatalf("opening postgres for reset: %v", err)
	}
	defer db.Close()

	stmts := []string{
		"TRUNCATE orders, order_items RESTART IDENTITY",
		// Old rows
		`INSERT INTO orders (id, customer, amount, notes, shipped, created_at) VALUES
			(1, 'Alice',   99.99,  'Express shipping', TRUE,  '2023-01-15 10:00:00'),
			(2, 'Bob',     NULL,   NULL,               FALSE, '2023-02-20 11:30:00'),
			(3, 'Charlie', 250.00, 'Gift wrap',        TRUE,  '2023-03-10 09:15:00'),
			(4, 'Diana',   15.50,  NULL,               FALSE, '2023-04-05 14:00:00'),
			(5, 'Eve',     0.00,   'Free sample',      TRUE,  '2023-05-25 16:45:00')`,
		// Recent rows
		`INSERT INTO orders (id, customer, amount, notes, shipped, created_at) VALUES
			(6, 'Frank',   120.00, 'Priority',    TRUE,  NOW() - INTERVAL '1 day'),
			(7, 'Grace',   45.99,  NULL,          FALSE, NOW() - INTERVAL '2 days'),
			(8, 'Heidi',   300.00, 'Bulk order',  TRUE,  NOW() - INTERVAL '5 days')`,
		// Old order_items
		`INSERT INTO order_items (order_id, item_id, product, quantity, created_at) VALUES
			(1, 1, 'Widget A', 2,  '2023-01-15 10:00:00'),
			(1, 2, 'Widget B', 1,  '2023-01-15 10:00:00'),
			(2, 1, 'Gadget X', 5,  '2023-02-20 11:30:00'),
			(3, 1, 'Part Y',   10, '2023-03-10 09:15:00'),
			(4, 1, 'Part Z',   3,  '2023-04-05 14:00:00')`,
		// Recent order_items
		`INSERT INTO order_items (order_id, item_id, product, quantity, created_at) VALUES
			(6, 1, 'New Widget', 1, NOW() - INTERVAL '1 day'),
			(7, 1, 'New Gadget', 2, NOW() - INTERVAL '2 days'),
			(8, 1, 'Bulk Item',  50, NOW() - INTERVAL '5 days')`,
		"SELECT setval('orders_id_seq', 8)",
	}
	for _, s := range stmts {
		if _, err := db.Exec(s); err != nil {
			t.Fatalf("reset postgres (%s): %v", s[:40], err)
		}
	}
}

// resetMySQL truncates tables and re-seeds data.
func resetMySQL(t *testing.T) {
	t.Helper()
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?parseTime=true",
		myUser, myPass, myHost, myPort, myDB)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("opening mysql for reset: %v", err)
	}
	defer db.Close()

	stmts := []string{
		"SET FOREIGN_KEY_CHECKS = 0",
		"TRUNCATE TABLE order_items",
		"TRUNCATE TABLE orders",
		"SET FOREIGN_KEY_CHECKS = 1",
		// Old rows
		`INSERT INTO orders (id, customer, amount, notes, shipped, created_at) VALUES
			(1, 'Alice',   99.99,  'Express shipping', TRUE,  '2023-01-15 10:00:00'),
			(2, 'Bob',     NULL,   NULL,               FALSE, '2023-02-20 11:30:00'),
			(3, 'Charlie', 250.00, 'Gift wrap',        TRUE,  '2023-03-10 09:15:00'),
			(4, 'Diana',   15.50,  NULL,               FALSE, '2023-04-05 14:00:00'),
			(5, 'Eve',     0.00,   'Free sample',      TRUE,  '2023-05-25 16:45:00')`,
		// Recent rows
		`INSERT INTO orders (id, customer, amount, notes, shipped, created_at) VALUES
			(6, 'Frank',   120.00, 'Priority',    TRUE,  NOW() - INTERVAL 1 DAY),
			(7, 'Grace',   45.99,  NULL,          FALSE, NOW() - INTERVAL 2 DAY),
			(8, 'Heidi',   300.00, 'Bulk order',  TRUE,  NOW() - INTERVAL 5 DAY)`,
		// Old order_items
		`INSERT INTO order_items (order_id, item_id, product, quantity, created_at) VALUES
			(1, 1, 'Widget A', 2,  '2023-01-15 10:00:00'),
			(1, 2, 'Widget B', 1,  '2023-01-15 10:00:00'),
			(2, 1, 'Gadget X', 5,  '2023-02-20 11:30:00'),
			(3, 1, 'Part Y',   10, '2023-03-10 09:15:00'),
			(4, 1, 'Part Z',   3,  '2023-04-05 14:00:00')`,
		// Recent order_items
		`INSERT INTO order_items (order_id, item_id, product, quantity, created_at) VALUES
			(6, 1, 'New Widget', 1, NOW() - INTERVAL 1 DAY),
			(7, 1, 'New Gadget', 2, NOW() - INTERVAL 2 DAY),
			(8, 1, 'Bulk Item',  50, NOW() - INTERVAL 5 DAY)`,
	}
	for _, s := range stmts {
		if _, err := db.Exec(s); err != nil {
			t.Fatalf("reset mysql (%s): %v", s[:40], err)
		}
	}
}

func countRows(t *testing.T, db *sql.DB, table string) int {
	t.Helper()
	var n int
	if err := db.QueryRow("SELECT COUNT(*) FROM " + table).Scan(&n); err != nil {
		t.Fatalf("counting rows in %s: %v", table, err)
	}
	return n
}

func newLogger() *log.Logger {
	return log.New(os.Stderr, "[integration] ", log.LstdFlags)
}

func TestPostgresArchiveAndRestore(t *testing.T) {
	resetPostgres(t)
	ctx := context.Background()

	// Connect via provider.
	pgProvider := postgres.New(pgHost, pgPort, pgDB, pgUser, pgPass, "disable", config.PoolConfig{})
	if err := pgProvider.Connect(ctx); err != nil {
		t.Fatalf("connecting postgres provider: %v", err)
	}
	defer pgProvider.Close()

	// Filesystem storage in temp dir.
	storePath := t.TempDir()
	store, err := filesystem.New(storePath)
	if err != nil {
		t.Fatalf("creating filesystem store: %v", err)
	}

	logger := newLogger()
	archiver := engine.NewArchiver(store, logger)
	restorer := engine.NewRestorer(store, logger)

	rule := pgRule([]config.TableConfig{ordersTbl})

	// Archive.
	result, err := archiver.Archive(ctx, rule, pgProvider)
	if err != nil {
		t.Fatalf("archive failed: %v", err)
	}

	if len(result.Tables) != 1 {
		t.Fatalf("expected 1 table result, got %d", len(result.Tables))
	}
	if result.Tables[0].RowsArchived != 5 {
		t.Errorf("expected 5 rows archived, got %d", result.Tables[0].RowsArchived)
	}
	if result.Tables[0].RowsDeleted != 5 {
		t.Errorf("expected 5 rows deleted, got %d", result.Tables[0].RowsDeleted)
	}

	// Verify DB state: only 3 recent rows remain.
	dsn := fmt.Sprintf("host=%s port=%d dbname=%s user=%s password=%s sslmode=disable",
		pgHost, pgPort, pgDB, pgUser, pgPass)
	rawDB, _ := sql.Open("postgres", dsn)
	defer rawDB.Close()

	if n := countRows(t, rawDB, "orders"); n != 3 {
		t.Errorf("expected 3 rows remaining, got %d", n)
	}

	// Verify files on disk.
	if len(result.Tables[0].Files) == 0 {
		t.Fatal("expected at least one archive file")
	}

	// Restore.
	opts := engine.RestoreOptions{
		Rule: rule,
	}
	restoreResult, err := restorer.Restore(ctx, opts, pgProvider)
	if err != nil {
		t.Fatalf("restore failed: %v", err)
	}

	if restoreResult.Tables[0].RowsRestored != 5 {
		t.Errorf("expected 5 rows restored, got %d", restoreResult.Tables[0].RowsRestored)
	}

	if n := countRows(t, rawDB, "orders"); n != 8 {
		t.Errorf("expected 8 total rows after restore, got %d", n)
	}
}

func TestPostgresCompositePK(t *testing.T) {
	resetPostgres(t)
	ctx := context.Background()

	pgProvider := postgres.New(pgHost, pgPort, pgDB, pgUser, pgPass, "disable", config.PoolConfig{})
	if err := pgProvider.Connect(ctx); err != nil {
		t.Fatalf("connecting postgres provider: %v", err)
	}
	defer pgProvider.Close()

	storePath := t.TempDir()
	store, err := filesystem.New(storePath)
	if err != nil {
		t.Fatalf("creating filesystem store: %v", err)
	}

	logger := newLogger()
	archiver := engine.NewArchiver(store, logger)
	restorer := engine.NewRestorer(store, logger)

	rule := pgRule([]config.TableConfig{orderItemsTbl})

	result, err := archiver.Archive(ctx, rule, pgProvider)
	if err != nil {
		t.Fatalf("archive failed: %v", err)
	}

	if result.Tables[0].RowsArchived != 5 {
		t.Errorf("expected 5 rows archived, got %d", result.Tables[0].RowsArchived)
	}
	if result.Tables[0].RowsDeleted != 5 {
		t.Errorf("expected 5 rows deleted, got %d", result.Tables[0].RowsDeleted)
	}

	dsn := fmt.Sprintf("host=%s port=%d dbname=%s user=%s password=%s sslmode=disable",
		pgHost, pgPort, pgDB, pgUser, pgPass)
	rawDB, _ := sql.Open("postgres", dsn)
	defer rawDB.Close()

	if n := countRows(t, rawDB, "order_items"); n != 3 {
		t.Errorf("expected 3 rows remaining, got %d", n)
	}

	opts := engine.RestoreOptions{Rule: rule}
	restoreResult, err := restorer.Restore(ctx, opts, pgProvider)
	if err != nil {
		t.Fatalf("restore failed: %v", err)
	}

	if restoreResult.Tables[0].RowsRestored != 5 {
		t.Errorf("expected 5 rows restored, got %d", restoreResult.Tables[0].RowsRestored)
	}

	if n := countRows(t, rawDB, "order_items"); n != 8 {
		t.Errorf("expected 8 total rows after restore, got %d", n)
	}
}

func TestMySQLArchiveAndRestore(t *testing.T) {
	resetMySQL(t)
	ctx := context.Background()

	myProvider := mysql.New(myHost, myPort, myDB, myUser, myPass, config.PoolConfig{})
	if err := myProvider.Connect(ctx); err != nil {
		t.Fatalf("connecting mysql provider: %v", err)
	}
	defer myProvider.Close()

	storePath := t.TempDir()
	store, err := filesystem.New(storePath)
	if err != nil {
		t.Fatalf("creating filesystem store: %v", err)
	}

	logger := newLogger()
	archiver := engine.NewArchiver(store, logger)
	restorer := engine.NewRestorer(store, logger)

	rule := myRule([]config.TableConfig{ordersTbl})

	result, err := archiver.Archive(ctx, rule, myProvider)
	if err != nil {
		t.Fatalf("archive failed: %v", err)
	}

	if result.Tables[0].RowsArchived != 5 {
		t.Errorf("expected 5 rows archived, got %d", result.Tables[0].RowsArchived)
	}
	if result.Tables[0].RowsDeleted != 5 {
		t.Errorf("expected 5 rows deleted, got %d", result.Tables[0].RowsDeleted)
	}

	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?parseTime=true",
		myUser, myPass, myHost, myPort, myDB)
	rawDB, _ := sql.Open("mysql", dsn)
	defer rawDB.Close()

	if n := countRows(t, rawDB, "orders"); n != 3 {
		t.Errorf("expected 3 rows remaining, got %d", n)
	}

	opts := engine.RestoreOptions{Rule: rule}
	restoreResult, err := restorer.Restore(ctx, opts, myProvider)
	if err != nil {
		t.Fatalf("restore failed: %v", err)
	}

	if restoreResult.Tables[0].RowsRestored != 5 {
		t.Errorf("expected 5 rows restored, got %d", restoreResult.Tables[0].RowsRestored)
	}

	if n := countRows(t, rawDB, "orders"); n != 8 {
		t.Errorf("expected 8 total rows after restore, got %d", n)
	}
}

func TestMySQLCompositePK(t *testing.T) {
	resetMySQL(t)
	ctx := context.Background()

	myProvider := mysql.New(myHost, myPort, myDB, myUser, myPass, config.PoolConfig{})
	if err := myProvider.Connect(ctx); err != nil {
		t.Fatalf("connecting mysql provider: %v", err)
	}
	defer myProvider.Close()

	storePath := t.TempDir()
	store, err := filesystem.New(storePath)
	if err != nil {
		t.Fatalf("creating filesystem store: %v", err)
	}

	logger := newLogger()
	archiver := engine.NewArchiver(store, logger)
	restorer := engine.NewRestorer(store, logger)

	rule := myRule([]config.TableConfig{orderItemsTbl})

	result, err := archiver.Archive(ctx, rule, myProvider)
	if err != nil {
		t.Fatalf("archive failed: %v", err)
	}

	if result.Tables[0].RowsArchived != 5 {
		t.Errorf("expected 5 rows archived, got %d", result.Tables[0].RowsArchived)
	}
	if result.Tables[0].RowsDeleted != 5 {
		t.Errorf("expected 5 rows deleted, got %d", result.Tables[0].RowsDeleted)
	}

	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?parseTime=true",
		myUser, myPass, myHost, myPort, myDB)
	rawDB, _ := sql.Open("mysql", dsn)
	defer rawDB.Close()

	if n := countRows(t, rawDB, "order_items"); n != 3 {
		t.Errorf("expected 3 rows remaining, got %d", n)
	}

	opts := engine.RestoreOptions{Rule: rule}
	restoreResult, err := restorer.Restore(ctx, opts, myProvider)
	if err != nil {
		t.Fatalf("restore failed: %v", err)
	}

	if restoreResult.Tables[0].RowsRestored != 5 {
		t.Errorf("expected 5 rows restored, got %d", restoreResult.Tables[0].RowsRestored)
	}

	if n := countRows(t, rawDB, "order_items"); n != 8 {
		t.Errorf("expected 8 total rows after restore, got %d", n)
	}
}

func TestArchiveIsIdempotent(t *testing.T) {
	resetPostgres(t)
	ctx := context.Background()

	pgProvider := postgres.New(pgHost, pgPort, pgDB, pgUser, pgPass, "disable", config.PoolConfig{})
	if err := pgProvider.Connect(ctx); err != nil {
		t.Fatalf("connecting postgres provider: %v", err)
	}
	defer pgProvider.Close()

	storePath := t.TempDir()
	store, err := filesystem.New(storePath)
	if err != nil {
		t.Fatalf("creating filesystem store: %v", err)
	}

	logger := newLogger()
	archiver := engine.NewArchiver(store, logger)

	rule := pgRule([]config.TableConfig{ordersTbl})

	// First archive: should archive 5 old rows.
	result1, err := archiver.Archive(ctx, rule, pgProvider)
	if err != nil {
		t.Fatalf("first archive failed: %v", err)
	}
	if result1.Tables[0].RowsArchived != 5 {
		t.Errorf("first run: expected 5 rows archived, got %d", result1.Tables[0].RowsArchived)
	}

	// Second archive: should archive 0 rows (old rows already deleted).
	result2, err := archiver.Archive(ctx, rule, pgProvider)
	if err != nil {
		t.Fatalf("second archive failed: %v", err)
	}
	if result2.Tables[0].RowsArchived != 0 {
		t.Errorf("second run: expected 0 rows archived, got %d", result2.Tables[0].RowsArchived)
	}

	// DB should still have exactly 3 recent rows.
	dsn := fmt.Sprintf("host=%s port=%d dbname=%s user=%s password=%s sslmode=disable",
		pgHost, pgPort, pgDB, pgUser, pgPass)
	rawDB, _ := sql.Open("postgres", dsn)
	defer rawDB.Close()

	if n := countRows(t, rawDB, "orders"); n != 3 {
		t.Errorf("expected 3 rows remaining, got %d", n)
	}
}

func TestNullableRoundTrip(t *testing.T) {
	resetPostgres(t)
	ctx := context.Background()

	pgProvider := postgres.New(pgHost, pgPort, pgDB, pgUser, pgPass, "disable", config.PoolConfig{})
	if err := pgProvider.Connect(ctx); err != nil {
		t.Fatalf("connecting postgres provider: %v", err)
	}
	defer pgProvider.Close()

	storePath := t.TempDir()
	store, err := filesystem.New(storePath)
	if err != nil {
		t.Fatalf("creating filesystem store: %v", err)
	}

	logger := newLogger()
	archiver := engine.NewArchiver(store, logger)
	restorer := engine.NewRestorer(store, logger)

	rule := pgRule([]config.TableConfig{ordersTbl})

	// Archive (includes rows with NULL amount and notes).
	_, err = archiver.Archive(ctx, rule, pgProvider)
	if err != nil {
		t.Fatalf("archive failed: %v", err)
	}

	// Restore.
	opts := engine.RestoreOptions{Rule: rule}
	_, err = restorer.Restore(ctx, opts, pgProvider)
	if err != nil {
		t.Fatalf("restore failed: %v", err)
	}

	// Verify NULL values survived the Parquet round-trip.
	dsn := fmt.Sprintf("host=%s port=%d dbname=%s user=%s password=%s sslmode=disable",
		pgHost, pgPort, pgDB, pgUser, pgPass)
	rawDB, _ := sql.Open("postgres", dsn)
	defer rawDB.Close()

	// Bob (id=2) had NULL amount and NULL notes.
	var amount sql.NullFloat64
	var notes sql.NullString
	err = rawDB.QueryRow("SELECT amount, notes FROM orders WHERE customer = 'Bob'").Scan(&amount, &notes)
	if err != nil {
		t.Fatalf("querying Bob's row: %v", err)
	}

	if amount.Valid {
		t.Errorf("expected Bob's amount to be NULL, got %v", amount.Float64)
	}
	if notes.Valid {
		t.Errorf("expected Bob's notes to be NULL, got %q", notes.String)
	}

	// Diana (id=4) had amount=15.50 but NULL notes.
	var dianaAmount sql.NullFloat64
	var dianaNotes sql.NullString
	err = rawDB.QueryRow("SELECT amount, notes FROM orders WHERE customer = 'Diana'").Scan(&dianaAmount, &dianaNotes)
	if err != nil {
		t.Fatalf("querying Diana's row: %v", err)
	}

	if !dianaAmount.Valid {
		t.Error("expected Diana's amount to be non-NULL")
	}
	if dianaNotes.Valid {
		t.Errorf("expected Diana's notes to be NULL, got %q", dianaNotes.String)
	}
}
