// --------------------------------------------------------------------------------
// Author: Thomas F McGeehan V
//
// This file is part of a software project developed by Thomas F McGeehan V.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.
//
// For more information about the MIT License, please visit:
// https://opensource.org/licenses/MIT
//
// Acknowledgment appreciated but not required.
// --------------------------------------------------------------------------------

package test

import (
	"context"
	"os"
	"testing"
	"time"

	integrations "github.com/ArrowArc/ArrowArc/integrations/duckdb"
	helper "github.com/ArrowArc/ArrowArc/pkg/common/utils"
	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/stretchr/testify/assert"
)

func TestDuckDBIntegration(t *testing.T) {
	// If testing on GitHub Actions, skip this test because DuckDB shared library is not available. TODO: Fix this.
	if os.Getenv("CI") == "true" {
		t.Skip("Skipping DuckDB integration test in CI environment.")
	}

	dbFilePath := "test_duckdb.db"
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conn, err := integrations.OpenDuckDBConnection(ctx, dbFilePath, nil)
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

	readRecordChan, readErrChan := integrations.ReadDuckDBStream(ctx, conn, "select * from test_table")

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

func TestDuckDBWithExtension(t *testing.T) {
	// If testing on GitHub Actions, skip this test because DuckDB shared library is not available. TODO: Fix this.
	if os.Getenv("CI") == "true" {
		t.Skip("Skipping DuckDB integration test in CI environment.")
	}

	dbFilePath := ":memory:"
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Load DuckDB with the httpfs extension
	extensions := []integrations.DuckDBExtension{
		{Name: "httpfs", LoadByDefault: true},
	}

	conn, err := integrations.OpenDuckDBConnection(ctx, dbFilePath, extensions)
	assert.NoError(t, err, "Error should be nil when opening DuckDB connection with extensions")
	defer integrations.CloseDuckDBConnection(conn)

	readRecordChan, readErrChan := integrations.ReadDuckDBStream(ctx, conn, `SELECT * FROM 'https://people.sc.fsu.edu/~jburkardt/data/csv/airtravel.csv'`)

	var records []arrow.Record
	for rec := range readRecordChan {
		assert.NotNil(t, rec, "Record should not be nil when reading from HTTP resource via DuckDB")
		records = append(records, rec)
		helper.PrintRecordBatch(rec)
	}

	select {
	case err := <-readErrChan:
		assert.NoError(t, err, "Error should be nil when reading from HTTP resource via DuckDB")
	case <-time.After(1 * time.Second):
	}

	assert.Greater(t, len(records), 0, "There should be records returned from the HTTP resource")

	t.Cleanup(func() {
		os.Remove(dbFilePath)
	})
}
