package format

import (
	"testing"

	"github.com/carlos-loya/archive-purge-restore/internal/provider/database"
)

func TestWriteAndReadParquet(t *testing.T) {
	columns := []database.ColumnInfo{
		{Name: "id", Type: "int4", Nullable: false},
		{Name: "name", Type: "text", Nullable: false},
		{Name: "amount", Type: "float8", Nullable: true},
		{Name: "active", Type: "bool", Nullable: false},
	}

	rows := []database.Row{
		{"id": int32(1), "name": "Alice", "amount": 99.99, "active": true},
		{"id": int32(2), "name": "Bob", "amount": 42.0, "active": false},
		{"id": int32(3), "name": "Charlie", "amount": nil, "active": true},
	}

	data, err := WriteParquet(columns, rows)
	if err != nil {
		t.Fatalf("WriteParquet() error: %v", err)
	}
	if len(data) == 0 {
		t.Fatal("WriteParquet() returned empty data")
	}

	readCols, readRows, err := ReadParquet(data)
	if err != nil {
		t.Fatalf("ReadParquet() error: %v", err)
	}

	if len(readCols) != len(columns) {
		t.Errorf("ReadParquet() returned %d columns, want %d", len(readCols), len(columns))
	}
	if len(readRows) != len(rows) {
		t.Errorf("ReadParquet() returned %d rows, want %d", len(readRows), len(rows))
	}

	// Verify first row data.
	if len(readRows) > 0 {
		r := readRows[0]
		if v, ok := r["id"]; !ok || toInt32(v) != 1 {
			t.Errorf("row[0][id] = %v, want 1", r["id"])
		}
		name, _ := r["name"].(string)
		if name != "Alice" {
			t.Errorf("row[0][name] = %v, want Alice", r["name"])
		}
	}
}

func TestWriteParquetEmptyRows(t *testing.T) {
	columns := []database.ColumnInfo{
		{Name: "id", Type: "int4"},
	}
	_, err := WriteParquet(columns, nil)
	if err == nil {
		t.Error("WriteParquet(nil rows) should return error")
	}
}

func TestNormalizeType(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"integer", "int32"},
		{"int4", "int32"},
		{"bigint", "int64"},
		{"int8", "int64"},
		{"real", "float32"},
		{"double precision", "float64"},
		{"boolean", "bool"},
		{"bytea", "string"},
		{"text", "string"},
		{"varchar", "string"},
		{"timestamp", "string"},
		{"unknown_type", "string"},
	}
	for _, tt := range tests {
		got := normalizeType(tt.input)
		if got != tt.want {
			t.Errorf("normalizeType(%q) = %q, want %q", tt.input, got, tt.want)
		}
	}
}
