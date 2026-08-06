package driver

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	runtimev1 "github.com/rilldata/rill/proto/gen/rill/runtime/v1"
	"github.com/rilldata/rill/runtime/drivers"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func arrowBody(t *testing.T, schema *arrow.Schema, columns ...arrow.Array) []byte {
	t.Helper()
	record := array.NewRecordBatch(schema, columns, int64(columns[0].Len()))
	defer record.Release()
	var body bytes.Buffer
	writer := ipc.NewWriter(&body, ipc.WithSchema(schema))
	require.NoError(t, writer.Write(record))
	require.NoError(t, writer.Close())
	return body.Bytes()
}

func TestQueryUsesVerglasArrowEndpointAndNeverCachesResults(t *testing.T) {
	var requests atomic.Int64
	schema := arrow.NewSchema([]arrow.Field{{Name: "rows", Type: arrow.PrimitiveTypes.Int64, Nullable: false}}, nil)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, http.MethodPost, r.Method)
		require.Equal(t, "/v1/query", r.URL.Path)
		require.Contains(t, r.Header.Get("Accept"), "application/vnd.apache.arrow.stream")
		var request queryRequest
		require.NoError(t, json.NewDecoder(r.Body).Decode(&request))
		require.Equal(t, "SELECT count(*) AS rows FROM analytics.events WHERE id > ?", request.SQL)
		require.Equal(t, []queryParameter{{Type: "int64", Value: float64(7)}}, request.Args)
		value := requests.Add(1)
		values := array.NewInt64Data(array.NewData(arrow.PrimitiveTypes.Int64, 1, []*memory.Buffer{nil, memory.NewBufferBytes([]byte{
			byte(value), 0, 0, 0, 0, 0, 0, 0,
		})}, nil, 0, 0))
		defer values.Release()
		w.Header().Set("Content-Type", "application/vnd.apache.arrow.stream")
		_, err := w.Write(arrowBody(t, schema, values))
		require.NoError(t, err)
	}))
	defer server.Close()

	connection := newConnection(config{Endpoint: server.URL}, zap.NewNop())
	for want := int64(1); want <= 2; want++ {
		result, err := connection.Query(context.Background(), &drivers.Statement{
			Query: "SELECT count(*) AS rows FROM analytics.events WHERE id > ?",
			Args:  []any{int64(7)},
		})
		require.NoError(t, err)
		require.True(t, result.Next())
		var got int64
		require.NoError(t, result.Scan(&got))
		require.Equal(t, want, got, "each query must use the current Verglas response")
		require.NoError(t, result.Close())
	}
	require.Equal(t, int64(2), requests.Load())
}

func TestInformationSchemaLookupRunsThroughVerglas(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "column_name", Type: arrow.BinaryTypes.String},
		{Name: "data_type", Type: arrow.BinaryTypes.String},
		{Name: "is_nullable", Type: arrow.BinaryTypes.String},
	}, nil)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request queryRequest
		require.NoError(t, json.NewDecoder(r.Body).Decode(&request))
		require.Contains(t, strings.ToLower(request.SQL), "information_schema.columns")
		columns := array.NewStringData(array.NewData(arrow.BinaryTypes.String, 2, []*memory.Buffer{
			nil,
			memory.NewBufferBytes([]byte{0, 0, 0, 0, 2, 0, 0, 0, 6, 0, 0, 0}),
			memory.NewBufferBytes([]byte("idname")),
		}, nil, 0, 0))
		types := array.NewStringData(array.NewData(arrow.BinaryTypes.String, 2, []*memory.Buffer{
			nil,
			memory.NewBufferBytes([]byte{0, 0, 0, 0, 6, 0, 0, 0, 13, 0, 0, 0}),
			memory.NewBufferBytes([]byte("BIGINTVARCHAR")),
		}, nil, 0, 0))
		nullable := array.NewStringData(array.NewData(arrow.BinaryTypes.String, 2, []*memory.Buffer{
			nil,
			memory.NewBufferBytes([]byte{0, 0, 0, 0, 2, 0, 0, 0, 5, 0, 0, 0}),
			memory.NewBufferBytes([]byte("NOYES")),
		}, nil, 0, 0))
		defer columns.Release()
		defer types.Release()
		defer nullable.Release()
		w.Header().Set("Content-Type", "application/vnd.apache.arrow.stream")
		_, err := w.Write(arrowBody(t, schema, columns, types, nullable))
		require.NoError(t, err)
	}))
	defer server.Close()

	connection := newConnection(config{Endpoint: server.URL}, zap.NewNop())
	table, err := connection.Lookup(context.Background(), "", "analytics", "events")
	require.NoError(t, err)
	require.Equal(t, "analytics", table.DatabaseSchema)
	require.Equal(t, "events", table.Name)
	require.Len(t, table.Schema.Fields, 2)
	require.Equal(t, "id", table.Schema.Fields[0].Name)
	require.Equal(t, runtimev1.Type_CODE_INT64, table.Schema.Fields[0].Type.Code)
	require.Equal(t, "name", table.Schema.Fields[1].Name)
	require.Equal(t, runtimev1.Type_CODE_STRING, table.Schema.Fields[1].Type.Code)
}
