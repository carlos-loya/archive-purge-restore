package format

import (
	"bytes"
	"fmt"
	"io"
	"time"

	"github.com/carlos-loya/archive-purge-restore/internal/provider/database"
	"github.com/parquet-go/parquet-go"
)

// WriteParquet writes rows as a Parquet file and returns the bytes.
func WriteParquet(columns []database.ColumnInfo, rows []database.Row) ([]byte, error) {
	if len(rows) == 0 {
		return nil, fmt.Errorf("no rows to write")
	}

	schema := buildSchema(columns)
	var buf bytes.Buffer
	writer := parquet.NewGenericWriter[any](&buf, schema)

	batch := make([]any, len(rows))
	for i, row := range rows {
		batch[i] = normalizeRow(row, columns)
	}
	if _, err := writer.Write(batch); err != nil {
		return nil, fmt.Errorf("writing rows: %w", err)
	}

	if err := writer.Close(); err != nil {
		return nil, fmt.Errorf("closing parquet writer: %w", err)
	}

	return buf.Bytes(), nil
}

// ReadParquet reads rows from Parquet data.
func ReadParquet(data []byte) ([]database.ColumnInfo, []database.Row, error) {
	reader := bytes.NewReader(data)

	allRows, err := parquet.Read[any](reader, int64(len(data)))
	if err != nil {
		return nil, nil, fmt.Errorf("reading parquet file: %w", err)
	}

	// Re-read to get schema.
	reader.Seek(0, io.SeekStart)
	f, err := parquet.OpenFile(reader, int64(len(data)))
	if err != nil {
		return nil, nil, fmt.Errorf("opening parquet file for schema: %w", err)
	}
	columns := schemaToColumns(f.Schema())

	var rows []database.Row
	for _, r := range allRows {
		m, ok := r.(map[string]any)
		if !ok {
			continue
		}
		row := make(database.Row, len(m))
		for k, v := range m {
			row[k] = v
		}
		rows = append(rows, row)
	}

	return columns, rows, nil
}

// WriteParquetToWriter streams Parquet data to a writer.
func WriteParquetToWriter(w io.Writer, columns []database.ColumnInfo, rows []database.Row) error {
	data, err := WriteParquet(columns, rows)
	if err != nil {
		return err
	}
	_, err = w.Write(data)
	return err
}

func buildSchema(columns []database.ColumnInfo) *parquet.Schema {
	group := make(parquet.Group)
	for _, col := range columns {
		group[col.Name] = columnToNode(col)
	}
	return parquet.NewSchema("archive", group)
}

func columnToNode(col database.ColumnInfo) parquet.Node {
	var node parquet.Node
	switch normalizeType(col.Type) {
	case "int32":
		node = parquet.Int(32)
	case "int64":
		node = parquet.Int(64)
	case "float32":
		node = parquet.Leaf(parquet.FloatType)
	case "float64":
		node = parquet.Leaf(parquet.DoubleType)
	case "bool":
		node = parquet.Leaf(parquet.BooleanType)
	default:
		node = parquet.String()
	}
	if col.Nullable {
		node = parquet.Optional(node)
	}
	return node
}

func normalizeType(dbType string) string {
	switch dbType {
	case "integer", "int", "int4", "smallint", "int2", "tinyint", "mediumint":
		return "int32"
	case "bigint", "int8", "serial", "bigserial":
		return "int64"
	case "real", "float4", "float":
		return "float32"
	case "double precision", "float8", "double":
		return "float64"
	case "boolean", "bool":
		return "bool"
	case "bytea", "blob", "binary", "varbinary":
		return "string"
	default:
		return "string"
	}
}

func normalizeRow(row database.Row, columns []database.ColumnInfo) map[string]any {
	out := make(map[string]any, len(columns))
	for _, col := range columns {
		v, ok := row[col.Name]
		if !ok || v == nil {
			out[col.Name] = nil
			continue
		}
		switch normalizeType(col.Type) {
		case "int32":
			out[col.Name] = toInt32(v)
		case "int64":
			out[col.Name] = toInt64(v)
		case "float32":
			out[col.Name] = toFloat32(v)
		case "float64":
			out[col.Name] = toFloat64(v)
		case "bool":
			out[col.Name] = toBool(v)
		default:
			out[col.Name] = toString(v)
		}
	}
	return out
}

func schemaToColumns(schema *parquet.Schema) []database.ColumnInfo {
	var columns []database.ColumnInfo
	for _, field := range schema.Fields() {
		col := database.ColumnInfo{
			Name:     field.Name(),
			Type:     parquetTypeToString(field),
			Nullable: field.Optional(),
		}
		columns = append(columns, col)
	}
	return columns
}

func parquetTypeToString(node parquet.Node) string {
	if node.Type() == nil {
		return "text"
	}
	switch node.Type().Kind() {
	case parquet.Boolean:
		return "bool"
	case parquet.Int32:
		return "int4"
	case parquet.Int64:
		return "int8"
	case parquet.Float:
		return "float4"
	case parquet.Double:
		return "float8"
	case parquet.ByteArray, parquet.FixedLenByteArray:
		return "text"
	default:
		return "text"
	}
}

func toInt32(v any) int32 {
	switch val := v.(type) {
	case int:
		return int32(val)
	case int32:
		return val
	case int64:
		return int32(val)
	case float64:
		return int32(val)
	default:
		return 0
	}
}

func toInt64(v any) int64 {
	switch val := v.(type) {
	case int:
		return int64(val)
	case int32:
		return int64(val)
	case int64:
		return val
	case float64:
		return int64(val)
	default:
		return 0
	}
}

func toFloat32(v any) float32 {
	switch val := v.(type) {
	case float32:
		return val
	case float64:
		return float32(val)
	case int:
		return float32(val)
	case int64:
		return float32(val)
	default:
		return 0
	}
}

func toFloat64(v any) float64 {
	switch val := v.(type) {
	case float64:
		return val
	case float32:
		return float64(val)
	case int:
		return float64(val)
	case int64:
		return float64(val)
	default:
		return 0
	}
}

func toBool(v any) bool {
	switch val := v.(type) {
	case bool:
		return val
	default:
		return false
	}
}

func toString(v any) string {
	switch val := v.(type) {
	case string:
		return val
	case []byte:
		return string(val)
	case time.Time:
		return val.Format(time.RFC3339Nano)
	default:
		return fmt.Sprintf("%v", val)
	}
}
