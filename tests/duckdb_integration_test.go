package test

import (
	"context"
	"os"
	"testing"
	"time"

	integrations "github.com/ArrowArc/ArrowArc/integrations/duckdb"
	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/stretchr/testify/assert"
)

func TestDuckDBIntegration(t *testing.T) {
	dbFilePath := "test_duckdb.db"
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conn, err := integrations.OpenDuckDBConnection(ctx, dbFilePath)
	assert.NoError(t, err, "Error should be nil when opening DuckDB connection")
	defer integrations.CloseDuckDBConnection(conn)

	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int32},
			{Name: "name", Type: arrow.BinaryTypes.String},
		}, nil,
	)

	b := array.NewRecordBuilder(mem, schema)
	defer b.Release()

	b.Field(0).(*array.Int32Builder).AppendValues([]int32{1, 2, 3}, nil)
	b.Field(1).(*array.StringBuilder).AppendValues([]string{"John", "Jane", "Jack"}, nil)

	record := b.NewRecord()
	defer record.Release()

	recordChan := make(chan arrow.Record, 1)
	recordChan <- record
	close(recordChan)

	writeErrChan := integrations.WriteDuckDBStream(ctx, conn, "test_table", recordChan)

	for err := range writeErrChan {
		assert.NoError(t, err, "Error should be nil when writing to DuckDB")
	}

	readRecordChan, readErrChan := integrations.ReadDuckDBStream(ctx, conn, "test_table")

	var records []arrow.Record
	for rec := range readRecordChan {
		assert.NotNil(t, rec, "Record should not be nil when reading from DuckDB")
		records = append(records, rec)
	}

	select {
	case err := <-readErrChan:
		assert.NoError(t, err, "Error should be nil when reading from DuckDB")
	case <-time.After(1 * time.Second):
	}

	assert.Len(t, records, 1, "There should be 1 record returned from DuckDB")

	t.Cleanup(func() {
		os.Remove(dbFilePath)
	})
}
