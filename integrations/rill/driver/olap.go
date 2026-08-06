package driver

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow/ipc"
	runtimev1 "github.com/rilldata/rill/proto/gen/rill/runtime/v1"
	"github.com/rilldata/rill/runtime/drivers"
	"github.com/rilldata/rill/runtime/drivers/duckdb"
	"go.uber.org/zap"
)

const arrowContentType = "application/vnd.apache.arrow.stream"

type queryParameter struct {
	Type  string `json:"type"`
	Value any    `json:"value,omitempty"`
}

type queryRequest struct {
	SQL  string           `json:"sql"`
	Args []queryParameter `json:"args,omitempty"`
}

// Dialect uses Rill's DuckDB SQL generator because Verglas DataFusion accepts
// the same dashboard query constructs and positional placeholder convention.
func (c *connection) Dialect() drivers.Dialect { return duckdb.DialectDuckDB }

// MayBeScaledToZero tells Rill that the query worker may wake on demand.
func (c *connection) MayBeScaledToZero(context.Context) bool { return true }

// WithConnection is unsupported because HTTP requests have no session affinity.
func (c *connection) WithConnection(context.Context, int, drivers.WithConnectionFunc) error {
	return drivers.ErrNotImplemented
}

// Exec executes read-only SQL and closes its streamed result.
func (c *connection) Exec(ctx context.Context, stmt *drivers.Statement) error {
	result, err := c.Query(ctx, stmt)
	if err != nil {
		return err
	}
	return result.Close()
}

// Query sends every statement to Verglas and decodes its Arrow IPC stream.
func (c *connection) Query(ctx context.Context, stmt *drivers.Statement) (*drivers.Result, error) {
	if stmt.DryRun {
		_, err := c.QuerySchema(ctx, stmt.Query, stmt.Args)
		return nil, err
	}
	params, err := encodeParameters(stmt.Args)
	if err != nil {
		return nil, err
	}
	body, err := json.Marshal(queryRequest{SQL: stmt.Query, Args: params})
	if err != nil {
		return nil, fmt.Errorf("encode Verglas query: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.config.Endpoint+"/v1/query", bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("build Verglas query request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", arrowContentType)
	if c.config.Token != "" {
		req.Header.Set("Authorization", "Bearer "+c.config.Token)
	}
	c.logger.Debug("verglas query", zap.String("sql", c.Dialect().SanitizeQueryForLogging(stmt.Query)))
	response, err := c.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("Verglas query request failed: %w", err)
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		defer response.Body.Close()
		message, _ := io.ReadAll(io.LimitReader(response.Body, 64*1024))
		return nil, fmt.Errorf("Verglas query returned %s: %s", response.Status, strings.TrimSpace(string(message)))
	}
	reader, err := ipc.NewReader(response.Body)
	if err != nil {
		response.Body.Close()
		return nil, fmt.Errorf("decode Verglas Arrow stream: %w", err)
	}
	rows := newRows(reader, response.Body)
	return &drivers.Result{Rows: rows, Schema: runtimeSchema(reader.Schema())}, nil
}

// Head returns a bounded preview of one live table.
func (c *connection) Head(ctx context.Context, database, schema, table string, limit int64) (*drivers.Result, error) {
	if limit < 0 {
		return nil, fmt.Errorf("head limit cannot be negative")
	}
	name := c.Dialect().EscapeTable(database, schema, table)
	return c.Query(ctx, &drivers.Statement{Query: fmt.Sprintf("SELECT * FROM %s LIMIT %d", name, limit)})
}

// QuerySchema asks Verglas to plan and return an empty Arrow result.
func (c *connection) QuerySchema(ctx context.Context, query string, args []any) (*runtimev1.StructType, error) {
	query = strings.TrimSuffix(strings.TrimSpace(query), ";")
	result, err := c.Query(ctx, &drivers.Statement{
		Query: fmt.Sprintf("SELECT * FROM (%s) AS __rill_schema LIMIT 0", query),
		Args:  args,
	})
	if err != nil {
		return nil, err
	}
	defer result.Close()
	return result.Schema, nil
}

// InformationSchema returns the live introspection implementation.
func (c *connection) InformationSchema() drivers.InformationSchema { return c }

// EstimateSize leaves worker sizing to Verglas.
func (c *connection) EstimateSize(context.Context) (int64, error) { return -1, nil }

func encodeParameters(values []any) ([]queryParameter, error) {
	params := make([]queryParameter, len(values))
	for i, value := range values {
		var param queryParameter
		switch value := value.(type) {
		case nil:
			param = queryParameter{Type: "null"}
		case bool:
			param = queryParameter{Type: "boolean", Value: value}
		case int:
			param = queryParameter{Type: "int64", Value: int64(value)}
		case int8:
			param = queryParameter{Type: "int64", Value: int64(value)}
		case int16:
			param = queryParameter{Type: "int64", Value: int64(value)}
		case int32:
			param = queryParameter{Type: "int64", Value: int64(value)}
		case int64:
			param = queryParameter{Type: "int64", Value: value}
		case uint:
			param = queryParameter{Type: "uint64", Value: uint64(value)}
		case uint8:
			param = queryParameter{Type: "uint64", Value: uint64(value)}
		case uint16:
			param = queryParameter{Type: "uint64", Value: uint64(value)}
		case uint32:
			param = queryParameter{Type: "uint64", Value: uint64(value)}
		case uint64:
			param = queryParameter{Type: "uint64", Value: value}
		case float32:
			param = queryParameter{Type: "float64", Value: float64(value)}
		case float64:
			param = queryParameter{Type: "float64", Value: value}
		case string:
			param = queryParameter{Type: "string", Value: value}
		case time.Time:
			param = queryParameter{Type: "timestamp", Value: value.UTC().Format(time.RFC3339Nano)}
		default:
			return nil, fmt.Errorf("unsupported Verglas query parameter %d of type %T", i+1, value)
		}
		params[i] = param
	}
	return params, nil
}
