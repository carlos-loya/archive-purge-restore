package history

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

// EventType represents the type of history event.
type EventType string

const (
	EventArchive EventType = "archive"
	EventRestore EventType = "restore"
)

// Event represents a single archive or restore event.
type Event struct {
	ID           int64
	RunID        string
	Rule         string
	EventType    EventType
	Table        string
	RowCount     int64
	Files        string // JSON array of file keys
	Status       string // "success" or "error"
	ErrorMessage string
	StartTime    time.Time
	EndTime      time.Time
}

// Store manages execution history in SQLite.
type Store struct {
	db *sql.DB
}

// NewStore creates a new history store at the given path.
func NewStore(path string) (*Store, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return nil, fmt.Errorf("creating history directory: %w", err)
	}

	db, err := sql.Open("sqlite3", path+"?_journal_mode=WAL")
	if err != nil {
		return nil, fmt.Errorf("opening history database: %w", err)
	}

	if err := migrate(db); err != nil {
		db.Close()
		return nil, fmt.Errorf("migrating history database: %w", err)
	}

	return &Store{db: db}, nil
}

func migrate(db *sql.DB) error {
	_, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS events (
			id           INTEGER PRIMARY KEY AUTOINCREMENT,
			run_id       TEXT NOT NULL,
			rule         TEXT NOT NULL,
			event_type   TEXT NOT NULL,
			table_name   TEXT NOT NULL,
			row_count    INTEGER NOT NULL DEFAULT 0,
			files        TEXT NOT NULL DEFAULT '[]',
			status       TEXT NOT NULL DEFAULT 'success',
			error_message TEXT NOT NULL DEFAULT '',
			start_time   DATETIME NOT NULL,
			end_time     DATETIME NOT NULL
		);
		CREATE INDEX IF NOT EXISTS idx_events_rule ON events(rule);
		CREATE INDEX IF NOT EXISTS idx_events_run_id ON events(run_id);
		CREATE INDEX IF NOT EXISTS idx_events_start_time ON events(start_time);
	`)
	return err
}

// Record saves a history event.
func (s *Store) Record(e Event) error {
	_, err := s.db.Exec(`
		INSERT INTO events (run_id, rule, event_type, table_name, row_count, files, status, error_message, start_time, end_time)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		e.RunID, e.Rule, string(e.EventType), e.Table, e.RowCount, e.Files,
		e.Status, e.ErrorMessage, e.StartTime, e.EndTime)
	if err != nil {
		return fmt.Errorf("recording event: %w", err)
	}
	return nil
}

// List returns recent history events, optionally filtered by rule.
func (s *Store) List(rule string, limit int) ([]Event, error) {
	var query string
	var args []any

	if rule != "" {
		query = `SELECT id, run_id, rule, event_type, table_name, row_count, files, status, error_message, start_time, end_time
				 FROM events WHERE rule = ? ORDER BY start_time DESC LIMIT ?`
		args = []any{rule, limit}
	} else {
		query = `SELECT id, run_id, rule, event_type, table_name, row_count, files, status, error_message, start_time, end_time
				 FROM events ORDER BY start_time DESC LIMIT ?`
		args = []any{limit}
	}

	rows, err := s.db.Query(query, args...)
	if err != nil {
		return nil, fmt.Errorf("querying events: %w", err)
	}
	defer rows.Close()

	var events []Event
	for rows.Next() {
		var e Event
		var eventType string
		if err := rows.Scan(&e.ID, &e.RunID, &e.Rule, &eventType, &e.Table,
			&e.RowCount, &e.Files, &e.Status, &e.ErrorMessage,
			&e.StartTime, &e.EndTime); err != nil {
			return nil, fmt.Errorf("scanning event: %w", err)
		}
		e.EventType = EventType(eventType)
		events = append(events, e)
	}
	return events, rows.Err()
}

// Close releases database resources.
func (s *Store) Close() error {
	return s.db.Close()
}
