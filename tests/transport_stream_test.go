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

	duck "github.com/ArrowArc/ArrowArc/integrations/duckdb"
	integrations "github.com/ArrowArc/ArrowArc/integrations/filesystem"
	generator "github.com/ArrowArc/ArrowArc/pkg/parquet"
	transport "github.com/ArrowArc/ArrowArc/pkg/transport"
	"github.com/apache/arrow/go/v17/arrow"
	"github.com/stretchr/testify/assert"
)

func TestTransportStream(t *testing.T) {
	t.Skip("Skipping Transport Stream Test.")
	// If testing on GitHub Actions, skip this test because of a missing dependency. TODO: Fix this.
	if os.Getenv("CI") == "true" {
		t.Skip("Skipping DuckDB integration test in CI environment.")
	}
	inputFilePath := "sample_test.parquet"
	err := generator.GenerateParquetFile(inputFilePath, 100*1024, false)
	assert.NoError(t, err, "Error should be nil when generating Parquet file")

	t.Cleanup(func() {
		os.Remove(inputFilePath)
	})

	tests := []struct {
		duckDBFilePath string
		description    string
	}{
		{
			duckDBFilePath: "test_duckdb.db",
			description:    "Transport stream to DuckDB sink",
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.description, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			// Read records from the Parquet file
			sourceChan, errChan := integrations.ReadParquetFileStream(ctx, inputFilePath, false, 1024, nil, nil, true)

			// Open DuckDB connection
			db, err := duck.OpenDuckDBConnection(ctx, test.duckDBFilePath, nil)
			assert.NoError(t, err, "Error should be nil when opening DuckDB file")
			defer db.Close()

			// Run the transport stream with the DuckDB sink
			transportErrChan := transport.TransportStream(ctx, sourceChan, func(ctx context.Context, recordChan <-chan arrow.Record) <-chan error {
				return duck.WriteDuckDBStream(ctx, db, "test_table", recordChan)
			})

			// Check for errors from the source stream
			for err := range errChan {
				assert.NoError(t, err, "Error should be nil when reading Parquet file")
			}

			// Check for errors from the transport stream
			for err := range transportErrChan {
				assert.NoError(t, err, "Error should be nil during transport stream")
			}

			// Validate the DuckDB sink
			rows, errors := duck.ReadDuckDBStream(ctx, db, "test_table")
			assert.NoError(t, err, "Error should be nil when reading DuckDB table")

			recordCount := 0
			for row := range rows {
				assert.NotNil(t, row, "Record from DuckDB should not be nil")
				recordCount++
			}
			for err := range errors {
				assert.NoError(t, err, "Error should be nil when reading DuckDB table")
			}

			assert.True(t, recordCount > 0, "DuckDB table should contain records")

			// Cleanup DuckDB file
			t.Cleanup(func() {
				os.Remove(test.duckDBFilePath)
			})
		})
	}
}
