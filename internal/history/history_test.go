package history

import (
	"path/filepath"
	"testing"
	"time"
)

func TestRecordAndList(t *testing.T) {
	dir := t.TempDir()
	store, err := NewStore(filepath.Join(dir, "history.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	now := time.Now()
	events := []Event{
		{
			RunID:     "run-001",
			Rule:      "rule-a",
			EventType: EventArchive,
			Table:     "orders",
			RowCount:  1000,
			Files:     `["db/orders/2025-01-01/run-001_000.parquet"]`,
			Status:    "success",
			StartTime: now.Add(-10 * time.Minute),
			EndTime:   now.Add(-9 * time.Minute),
		},
		{
			RunID:     "run-002",
			Rule:      "rule-b",
			EventType: EventArchive,
			Table:     "users",
			RowCount:  500,
			Files:     `["db/users/2025-01-01/run-002_000.parquet"]`,
			Status:    "success",
			StartTime: now.Add(-5 * time.Minute),
			EndTime:   now.Add(-4 * time.Minute),
		},
		{
			RunID:        "run-003",
			Rule:         "rule-a",
			EventType:    EventRestore,
			Table:        "orders",
			RowCount:     200,
			Files:        `["db/orders/2025-01-01/run-001_000.parquet"]`,
			Status:       "error",
			ErrorMessage: "connection refused",
			StartTime:    now.Add(-2 * time.Minute),
			EndTime:      now.Add(-1 * time.Minute),
		},
	}

	for _, e := range events {
		if err := store.Record(e); err != nil {
			t.Fatalf("Record() error: %v", err)
		}
	}

	// List all events.
	all, err := store.List("", 10)
	if err != nil {
		t.Fatalf("List() error: %v", err)
	}
	if len(all) != 3 {
		t.Errorf("List('', 10) returned %d events, want 3", len(all))
	}
	// Should be ordered by start_time DESC.
	if len(all) >= 2 && all[0].RunID != "run-003" {
		t.Errorf("first event RunID = %s, want run-003 (most recent)", all[0].RunID)
	}

	// Filter by rule.
	ruleA, err := store.List("rule-a", 10)
	if err != nil {
		t.Fatalf("List(rule-a) error: %v", err)
	}
	if len(ruleA) != 2 {
		t.Errorf("List('rule-a', 10) returned %d events, want 2", len(ruleA))
	}

	// Test limit.
	limited, err := store.List("", 1)
	if err != nil {
		t.Fatalf("List('', 1) error: %v", err)
	}
	if len(limited) != 1 {
		t.Errorf("List('', 1) returned %d events, want 1", len(limited))
	}
}

func TestEventFields(t *testing.T) {
	dir := t.TempDir()
	store, err := NewStore(filepath.Join(dir, "history.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	now := time.Now().Truncate(time.Second)
	e := Event{
		RunID:        "test-run",
		Rule:         "test-rule",
		EventType:    EventArchive,
		Table:        "test_table",
		RowCount:     42,
		Files:        `["file1.parquet", "file2.parquet"]`,
		Status:       "success",
		ErrorMessage: "",
		StartTime:    now,
		EndTime:      now.Add(time.Minute),
	}
	if err := store.Record(e); err != nil {
		t.Fatal(err)
	}

	events, err := store.List("", 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 {
		t.Fatal("expected 1 event")
	}

	got := events[0]
	if got.RunID != "test-run" {
		t.Errorf("RunID = %q, want %q", got.RunID, "test-run")
	}
	if got.EventType != EventArchive {
		t.Errorf("EventType = %q, want %q", got.EventType, EventArchive)
	}
	if got.RowCount != 42 {
		t.Errorf("RowCount = %d, want 42", got.RowCount)
	}
	if got.Status != "success" {
		t.Errorf("Status = %q, want %q", got.Status, "success")
	}
}
