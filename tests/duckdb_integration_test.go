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
	"io"
	"os"
	"testing"
	"time"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	integrations "github.com/arrowarc/arrowarc/integrations/duckdb"
	helper "github.com/arrowarc/arrowarc/pkg/common/utils"
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

	writer, err := integrations.NewDuckDBRecordWriter(ctx, conn, "test_table")
	assert.NoError(t, err, "Error should be nil when creating DuckDB record writer")

	err = writer.Write(record)
	assert.NoError(t, err, "Error should be nil when writing to DuckDB")
	assert.NoError(t, err, "Error should be nil when closing DuckDB record writer")

	reader, err := integrations.NewDuckDBRecordReader(ctx, conn, "SELECT * FROM test_table")
	assert.NoError(t, err, "Error should be nil when creating DuckDB record reader")

	var records []arrow.Record
	for {
		rec, err := reader.Read()
		if err == io.EOF {
			break
		}
		assert.NoError(t, err, "Error should be nil when reading from DuckDB")
		assert.NotNil(t, rec, "Record should not be nil when reading from DuckDB")
		records = append(records, rec)
		rec.Release()
	}

	assert.NoError(t, err, "Error should be nil when closing DuckDB record reader")

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

	extensions := []integrations.DuckDBExtension{
		{Name: "httpfs", LoadByDefault: true},
	}

	conn, err := integrations.OpenDuckDBConnection(ctx, dbFilePath, extensions)
	assert.NoError(t, err, "Error should be nil when opening DuckDB connection with extensions")
	defer integrations.CloseDuckDBConnection(conn)

	reader, err := integrations.NewDuckDBRecordReader(ctx, conn, `SELECT * FROM 'https://people.sc.fsu.edu/~jburkardt/data/csv/airtravel.csv'`)
	assert.NoError(t, err, "Error should be nil when creating DuckDB record reader")

	var records []arrow.Record
	for {
		rec, err := reader.Read()
		if err == io.EOF {
			break
		}
		assert.NoError(t, err, "Error should be nil when reading from DuckDB")
		assert.NotNil(t, rec, "Record should not be nil when reading from HTTP resource via DuckDB")
		records = append(records, rec)
		helper.PrintRecordBatch(rec)
		rec.Release()
	}

	assert.NoError(t, err, "Error should be nil when closing DuckDB record reader")

	assert.Greater(t, len(records), 0, "There should be records returned from the HTTP resource")

	t.Cleanup(func() {
		os.Remove(dbFilePath)
	})
}
