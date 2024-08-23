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

	integrations "github.com/ArrowArc/ArrowArc/internal/integrations/filesystem"
	generator "github.com/ArrowArc/ArrowArc/pkg/parquet"
	"github.com/stretchr/testify/assert"
)

func TestReadParquetFileStream(t *testing.T) {
	// Skip this test in CI environment
	if os.Getenv("CI") == "true" {
		t.Skip("Skipping DuckDB integration test in CI environment.")
	}

	t.Parallel() // Parallelize the top-level test

	// Generate a sample Parquet file for testing
	filePath := "sample_test.parquet"
	err := generator.GenerateParquetFile(filePath, 100*1024, false) // 100 KB, simple structure
	assert.NoError(t, err, "Error should be nil when generating Parquet file")

	// Use t.Cleanup to ensure the file is removed after all tests complete
	t.Cleanup(func() {
		os.Remove(filePath)
	})

	tests := []struct {
		filePath    string
		memoryMap   bool
		chunkSize   int64
		columns     []string
		rowGroups   []int
		parallel    bool
		description string
	}{
		{
			filePath:    filePath,
			memoryMap:   false,
			chunkSize:   1024,
			columns:     nil, // Read all columns
			rowGroups:   nil, // Read all row groups
			parallel:    true,
			description: "Read all data in parallel",
		},
		{
			filePath:    filePath,
			memoryMap:   true,
			chunkSize:   2048,
			columns:     []string{"id", "name"}, // Read specific columns
			rowGroups:   nil,                    // Read all row groups
			parallel:    false,
			description: "Read specific columns with memory mapping",
		},
		{
			filePath:    filePath,
			memoryMap:   false,
			chunkSize:   512,
			columns:     nil,      // Read all columns
			rowGroups:   []int{0}, // Read specific row group
			parallel:    true,
			description: "Read specific row group in parallel",
		},
	}

	for _, test := range tests {
		test := test // capture range variable
		t.Run(test.description, func(t *testing.T) {
			t.Parallel() // Parallelize each subtest

			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			recordChan, errChan := integrations.ReadParquetFileStream(ctx, test.filePath, test.memoryMap, test.chunkSize, test.columns, test.rowGroups, test.parallel)

			var recordsRead int
			for record := range recordChan {
				assert.NotNil(t, record, "Record should not be nil")
				recordsRead += int(record.NumRows())
				record.Release()
			}

			for err := range errChan {
				assert.NoError(t, err, "Error should be nil when reading Parquet file")
			}

			assert.Greater(t, recordsRead, 0, "Should have read at least one record")
		})
	}
}
