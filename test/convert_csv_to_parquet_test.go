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
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/apache/arrow/go/v17/arrow"
	converter "github.com/arrowarc/arrowarc/converter"
	"github.com/stretchr/testify/assert"
)

func TestConvertCSVToParquet(t *testing.T) {
	t.Parallel() // Parallelize the top-level test

	// Generate a sample CSV file for testing with header
	csvFilePathWithHeader := fmt.Sprintf("sample_test_with_header_%d.csv", time.Now().UnixNano())
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)

	csvContentWithHeader := `id,name
1,John
2,Jane
3,Jack`

	err := os.WriteFile(csvFilePathWithHeader, []byte(csvContentWithHeader), 0644)
	assert.NoError(t, err, "Error should be nil when generating CSV file with header")

	// Generate a sample CSV file for testing without header
	csvFilePathWithoutHeader := fmt.Sprintf("sample_test_without_header_%d.csv", time.Now().UnixNano())
	csvContentWithoutHeader := `1,John
2,Jane
3,Jack`

	err = os.WriteFile(csvFilePathWithoutHeader, []byte(csvContentWithoutHeader), 0644)
	assert.NoError(t, err, "Error should be nil when generating CSV file without header")

	// Use t.Cleanup to ensure the files are removed after all tests complete
	t.Cleanup(func() {
		os.Remove(csvFilePathWithHeader)
		os.Remove(csvFilePathWithoutHeader)
	})

	tests := []struct {
		csvFilePath      string
		parquetFilePath  string
		schema           *arrow.Schema
		hasHeader        bool
		chunkSize        int64
		delimiter        rune
		nullValues       []string
		stringsCanBeNull bool
		description      string
	}{
		{
			csvFilePath:      csvFilePathWithHeader,
			parquetFilePath:  fmt.Sprintf("output_test_1_%d.parquet", time.Now().UnixNano()),
			schema:           schema,
			hasHeader:        true,
			chunkSize:        1024,
			delimiter:        ',',
			nullValues:       []string{"", "NULL"},
			stringsCanBeNull: true,
			description:      "Convert CSV to Parquet with header",
		},
		{
			csvFilePath:      csvFilePathWithoutHeader,
			parquetFilePath:  fmt.Sprintf("output_test_2_%d.parquet", time.Now().UnixNano()),
			schema:           schema,
			hasHeader:        false,
			chunkSize:        2048,
			delimiter:        ',',
			nullValues:       []string{"", "NULL"},
			stringsCanBeNull: true,
			description:      "Convert CSV to Parquet without header",
		},
	}

	for _, test := range tests {
		test := test // capture range variable
		t.Run(test.description, func(t *testing.T) {
			t.Parallel() // Parallelize each subtest

			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			metrics, err := converter.ConvertCSVToParquet(ctx, test.csvFilePath, test.parquetFilePath, test.hasHeader, 100000, ',', []string{}, true)
			assert.NoError(t, err, "Error should be nil when converting CSV to Parquet")
			fmt.Printf("Conversion completed. Summary: %s\n", metrics)
			_, err = os.Stat(test.parquetFilePath)
			assert.NoError(t, err, "Parquet file should be created")

			t.Cleanup(func() {
				os.Remove(test.parquetFilePath)
			})
		})
	}
}
