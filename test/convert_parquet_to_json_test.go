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

	converter "github.com/arrowarc/arrowarc/converter"
	generator "github.com/arrowarc/arrowarc/generator"
	"github.com/stretchr/testify/assert"
)

func TestConvertParquetToJSON(t *testing.T) {

	parquetFilePath := "sample_test.parquet"
	jsonFilePathWithStructs := "output_test_with_structs.json"
	jsonFilePathWithoutStructs := "output_test_without_structs.json"

	err := generator.GenerateParquetFile(parquetFilePath, 100*1024, false) // 100 KB, simple structure
	assert.NoError(t, err, "Error should be nil when generating Parquet file")

	t.Cleanup(func() {
		os.Remove(parquetFilePath)
		os.Remove(jsonFilePathWithStructs)
		os.Remove(jsonFilePathWithoutStructs)
	})

	tests := []struct {
		parquetFilePath string
		jsonFilePath    string
		memoryMap       bool
		chunkSize       int64
		columns         []string
		rowGroups       []int
		parallel        bool
		includeStructs  bool
		description     string
	}{
		{
			parquetFilePath: parquetFilePath,
			jsonFilePath:    jsonFilePathWithStructs,
			memoryMap:       false,
			chunkSize:       1024,
			columns:         nil, // Read all columns
			rowGroups:       nil, // Read all row groups
			parallel:        true,
			includeStructs:  true,
			description:     "Convert Parquet to JSON with structs",
		},
		{
			parquetFilePath: parquetFilePath,
			jsonFilePath:    jsonFilePathWithoutStructs,
			memoryMap:       true,
			chunkSize:       2048,
			columns:         []string{"id", "name"}, // Read specific columns
			rowGroups:       nil,                    // Read all row groups
			parallel:        false,
			includeStructs:  false,
			description:     "Convert Parquet to JSON without structs",
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.description, func(t *testing.T) {

			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			metrics, err := converter.ConvertParquetToJSON(ctx, test.parquetFilePath, test.jsonFilePath, test.memoryMap, test.chunkSize, test.columns, test.rowGroups, test.parallel, test.includeStructs)
			assert.NoError(t, err, "Error should be nil when converting Parquet to JSON")
			fmt.Printf("Conversion completed. Summary: %s\n", metrics)

			_, err = os.Stat(test.jsonFilePath)
			assert.NoError(t, err, "JSON file should be created")

			t.Cleanup(func() {
				os.Remove(test.jsonFilePath)
			})
		})
	}
}
