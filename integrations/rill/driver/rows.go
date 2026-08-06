package driver

import (
	"fmt"
	"io"
	"reflect"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	runtimev1 "github.com/rilldata/rill/proto/gen/rill/runtime/v1"
)

type rows struct {
	reader *ipc.Reader
	body   io.ReadCloser
	record arrow.RecordBatch
	row    int
	err    error
	closed bool
}

func newRows(reader *ipc.Reader, body io.ReadCloser) *rows {
	return &rows{reader: reader, body: body, row: -1}
}

// Next advances to the next row, pulling another Arrow batch when necessary.
func (r *rows) Next() bool {
	if r.closed || r.err != nil {
		return false
	}
	if r.record != nil && r.row+1 < int(r.record.NumRows()) {
		r.row++
		return true
	}
	for r.reader.Next() {
		r.record = r.reader.Record()
		r.row = 0
		if r.record.NumRows() > 0 {
			return true
		}
	}
	r.err = r.reader.Err()
	return false
}

// Err returns an Arrow stream error observed while advancing.
func (r *rows) Err() error { return r.err }

// Close releases the Arrow reader and underlying HTTP response body.
func (r *rows) Close() error {
	if r.closed {
		return nil
	}
	r.closed = true
	r.reader.Release()
	return r.body.Close()
}

// Scan copies the current row into caller-provided pointers.
func (r *rows) Scan(dest ...any) error {
	if r.record == nil || r.row < 0 {
		return fmt.Errorf("Scan called before Next")
	}
	if len(dest) != int(r.record.NumCols()) {
		return fmt.Errorf("Scan received %d destinations for %d columns", len(dest), r.record.NumCols())
	}
	for i := range dest {
		if err := assignValue(dest[i], arrowValue(r.record.Column(i), r.row)); err != nil {
			return fmt.Errorf("scan column %q: %w", r.record.Schema().Field(i).Name, err)
		}
	}
	return nil
}

// MapScan copies the current row into a map keyed by Arrow field name.
func (r *rows) MapScan(dest map[string]any) error {
	if r.record == nil || r.row < 0 {
		return fmt.Errorf("MapScan called before Next")
	}
	if dest == nil {
		return fmt.Errorf("MapScan destination is nil")
	}
	for i, field := range r.record.Schema().Fields() {
		dest[field.Name] = arrowValue(r.record.Column(i), r.row)
	}
	return nil
}

func arrowValue(column arrow.Array, row int) any {
	if column.IsNull(row) {
		return nil
	}
	value := column.GetOneForMarshal(row)
	switch column.DataType().ID() {
	case arrow.TIMESTAMP:
		if text, ok := value.(string); ok {
			if parsed, err := time.Parse(time.RFC3339Nano, text); err == nil {
				return parsed
			}
		}
	case arrow.DATE32, arrow.DATE64:
		if text, ok := value.(string); ok {
			if parsed, err := time.Parse(time.DateOnly, text); err == nil {
				return parsed
			}
		}
	}
	return value
}

func assignValue(destination, value any) error {
	target := reflect.ValueOf(destination)
	if !target.IsValid() || target.Kind() != reflect.Pointer || target.IsNil() {
		return fmt.Errorf("destination must be a non-nil pointer")
	}
	target = target.Elem()
	if value == nil {
		target.Set(reflect.Zero(target.Type()))
		return nil
	}
	source := reflect.ValueOf(value)
	if source.Type().AssignableTo(target.Type()) {
		target.Set(source)
		return nil
	}
	if target.Kind() == reflect.Interface {
		target.Set(source)
		return nil
	}
	if source.Type().ConvertibleTo(target.Type()) {
		target.Set(source.Convert(target.Type()))
		return nil
	}
	return fmt.Errorf("cannot assign %T to %s", value, target.Type())
}

func runtimeSchema(schema *arrow.Schema) *runtimev1.StructType {
	fields := make([]*runtimev1.StructType_Field, len(schema.Fields()))
	for i, field := range schema.Fields() {
		fields[i] = &runtimev1.StructType_Field{Name: field.Name, Type: runtimeType(field.Type, field.Nullable)}
	}
	return &runtimev1.StructType{Fields: fields}
}

func runtimeType(dataType arrow.DataType, nullable bool) *runtimev1.Type {
	typ := &runtimev1.Type{Nullable: nullable, RawType: dataType.String()}
	switch dataType.ID() {
	case arrow.BOOL:
		typ.Code = runtimev1.Type_CODE_BOOL
	case arrow.INT8:
		typ.Code = runtimev1.Type_CODE_INT8
	case arrow.INT16:
		typ.Code = runtimev1.Type_CODE_INT16
	case arrow.INT32:
		typ.Code = runtimev1.Type_CODE_INT32
	case arrow.INT64:
		typ.Code = runtimev1.Type_CODE_INT64
	case arrow.UINT8:
		typ.Code = runtimev1.Type_CODE_UINT8
	case arrow.UINT16:
		typ.Code = runtimev1.Type_CODE_UINT16
	case arrow.UINT32:
		typ.Code = runtimev1.Type_CODE_UINT32
	case arrow.UINT64:
		typ.Code = runtimev1.Type_CODE_UINT64
	case arrow.FLOAT16, arrow.FLOAT32:
		typ.Code = runtimev1.Type_CODE_FLOAT32
	case arrow.FLOAT64:
		typ.Code = runtimev1.Type_CODE_FLOAT64
	case arrow.STRING, arrow.LARGE_STRING, arrow.STRING_VIEW:
		typ.Code = runtimev1.Type_CODE_STRING
	case arrow.BINARY, arrow.LARGE_BINARY, arrow.FIXED_SIZE_BINARY, arrow.BINARY_VIEW:
		typ.Code = runtimev1.Type_CODE_BYTES
	case arrow.DATE32, arrow.DATE64:
		typ.Code = runtimev1.Type_CODE_DATE
	case arrow.TIME32, arrow.TIME64:
		typ.Code = runtimev1.Type_CODE_TIME
	case arrow.TIMESTAMP:
		typ.Code = runtimev1.Type_CODE_TIMESTAMP
	case arrow.DECIMAL32, arrow.DECIMAL64, arrow.DECIMAL128, arrow.DECIMAL256:
		typ.Code = runtimev1.Type_CODE_DECIMAL
	case arrow.LIST, arrow.LARGE_LIST, arrow.FIXED_SIZE_LIST, arrow.LIST_VIEW, arrow.LARGE_LIST_VIEW:
		typ.Code = runtimev1.Type_CODE_ARRAY
		if list, ok := dataType.(arrow.ListLikeType); ok {
			typ.ArrayElementType = runtimeType(list.Elem(), true)
		}
	case arrow.STRUCT:
		typ.Code = runtimev1.Type_CODE_STRUCT
	case arrow.MAP:
		typ.Code = runtimev1.Type_CODE_MAP
	default:
		typ.Code = runtimev1.Type_CODE_UNSPECIFIED
	}
	return typ
}
