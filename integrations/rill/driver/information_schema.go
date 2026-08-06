package driver

import (
	"context"
	"fmt"
	"strings"

	runtimev1 "github.com/rilldata/rill/proto/gen/rill/runtime/v1"
	"github.com/rilldata/rill/runtime/drivers"
)

// ListDatabaseSchemas returns live Iceberg namespaces through Verglas SQL.
func (c *connection) ListDatabaseSchemas(ctx context.Context, pageSize uint32, pageToken string) ([]*drivers.DatabaseSchemaInfo, string, error) {
	limit := pageLimit(pageSize)
	query := "SELECT DISTINCT table_schema FROM information_schema.tables WHERE table_schema <> 'information_schema'"
	args := []any{}
	if pageToken != "" {
		query += " AND table_schema > ?"
		args = append(args, pageToken)
	}
	query += fmt.Sprintf(" ORDER BY table_schema LIMIT %d", limit+1)
	result, err := c.Query(ctx, &drivers.Statement{Query: query, Args: args})
	if err != nil {
		return nil, "", err
	}
	defer result.Close()
	items := make([]*drivers.DatabaseSchemaInfo, 0, limit+1)
	for result.Next() {
		var schema string
		if err := result.Scan(&schema); err != nil {
			return nil, "", err
		}
		items = append(items, &drivers.DatabaseSchemaInfo{DatabaseSchema: schema})
	}
	if err := result.Err(); err != nil {
		return nil, "", err
	}
	if len(items) <= limit {
		return items, "", nil
	}
	next := items[limit-1].DatabaseSchema
	return items[:limit], next, nil
}

// ListTables returns live tables in one Iceberg namespace.
func (c *connection) ListTables(ctx context.Context, _ string, databaseSchema string, pageSize uint32, pageToken string) ([]*drivers.TableInfo, string, error) {
	limit := pageLimit(pageSize)
	query := "SELECT table_name FROM information_schema.tables WHERE table_schema = ? AND table_name NOT LIKE '%$%'"
	args := []any{databaseSchema}
	if pageToken != "" {
		query += " AND table_name > ?"
		args = append(args, pageToken)
	}
	query += fmt.Sprintf(" ORDER BY table_name LIMIT %d", limit+1)
	result, err := c.Query(ctx, &drivers.Statement{Query: query, Args: args})
	if err != nil {
		return nil, "", err
	}
	defer result.Close()
	items := make([]*drivers.TableInfo, 0, limit+1)
	for result.Next() {
		var name string
		if err := result.Scan(&name); err != nil {
			return nil, "", err
		}
		items = append(items, &drivers.TableInfo{Name: name})
	}
	if err := result.Err(); err != nil {
		return nil, "", err
	}
	if len(items) <= limit {
		return items, "", nil
	}
	next := items[limit-1].Name
	return items[:limit], next, nil
}

// Lookup returns the current Arrow-derived schema for one Iceberg table.
func (c *connection) Lookup(ctx context.Context, database, databaseSchema, name string) (*drivers.OlapTable, error) {
	result, err := c.Query(ctx, &drivers.Statement{
		Query: "SELECT column_name, data_type, is_nullable FROM information_schema.columns WHERE table_schema = ? AND table_name = ? ORDER BY ordinal_position",
		Args:  []any{databaseSchema, name},
	})
	if err != nil {
		return nil, err
	}
	defer result.Close()
	fields := make([]*runtimev1.StructType_Field, 0)
	for result.Next() {
		var column, dataType, nullable string
		if err := result.Scan(&column, &dataType, &nullable); err != nil {
			return nil, err
		}
		fields = append(fields, &runtimev1.StructType_Field{
			Name: column,
			Type: informationSchemaType(dataType, nullable == "YES"),
		})
	}
	if err := result.Err(); err != nil {
		return nil, err
	}
	if len(fields) == 0 {
		return nil, drivers.ErrNotFound
	}
	return &drivers.OlapTable{
		Database:       database,
		DatabaseSchema: databaseSchema,
		Name:           name,
		Schema:         &runtimev1.StructType{Fields: fields},
	}, nil
}

// All returns every live table using Rill's information-schema helper.
func (c *connection) All(ctx context.Context, like string, pageSize uint32, pageToken string) ([]*drivers.OlapTable, string, error) {
	return drivers.AllFromInformationSchema(ctx, like, pageSize, pageToken, c)
}

// LoadPhysicalSize leaves byte estimation to Verglas.
func (c *connection) LoadPhysicalSize(context.Context, []*drivers.OlapTable) error { return nil }

// LoadDDL leaves DDL empty because Iceberg schemas, not SQL DDL, are authoritative.
func (c *connection) LoadDDL(context.Context, *drivers.OlapTable) error { return nil }

func pageLimit(size uint32) int {
	if size == 0 {
		return drivers.DefaultPageSize
	}
	return int(size)
}

// informationSchemaType maps DataFusion and SQL spellings to Rill types.
func informationSchemaType(dataType string, nullable bool) *runtimev1.Type {
	code := runtimev1.Type_CODE_UNSPECIFIED
	switch strings.ToUpper(dataType) {
	case "BOOLEAN", "BOOL":
		code = runtimev1.Type_CODE_BOOL
	case "INT8", "TINYINT":
		code = runtimev1.Type_CODE_INT8
	case "INT16", "SMALLINT":
		code = runtimev1.Type_CODE_INT16
	case "INT32", "INTEGER", "INT":
		code = runtimev1.Type_CODE_INT32
	case "INT64", "BIGINT":
		code = runtimev1.Type_CODE_INT64
	case "UINT8", "UTINYINT":
		code = runtimev1.Type_CODE_UINT8
	case "UINT16", "USMALLINT":
		code = runtimev1.Type_CODE_UINT16
	case "UINT32", "UINTEGER":
		code = runtimev1.Type_CODE_UINT32
	case "UINT64", "UBIGINT":
		code = runtimev1.Type_CODE_UINT64
	case "FLOAT32", "FLOAT", "REAL":
		code = runtimev1.Type_CODE_FLOAT32
	case "FLOAT64", "DOUBLE":
		code = runtimev1.Type_CODE_FLOAT64
	case "UTF8", "LARGEUTF8", "VARCHAR", "TEXT", "STRING":
		code = runtimev1.Type_CODE_STRING
	case "DATE32", "DATE64", "DATE":
		code = runtimev1.Type_CODE_DATE
	case "TIMESTAMP", "TIMESTAMP(NANOSECOND, NONE)", "TIMESTAMP(MICROSECOND, NONE)":
		code = runtimev1.Type_CODE_TIMESTAMP
	case "BINARY", "LARGEBINARY", "BLOB":
		code = runtimev1.Type_CODE_BYTES
	}
	return &runtimev1.Type{Code: code, Nullable: nullable, RawType: dataType}
}
