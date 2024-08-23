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
	"os"
	"testing"

	. "github.com/ArrowArc/ArrowArc/pkg/parquet"
	"github.com/stretchr/testify/assert"
)

func TestGenerateParquetFile(t *testing.T) {
	t.Parallel() // Parallelize the top-level test
	tests := []struct {
		filePath    string
		targetSize  int64
		complex     bool
		description string
	}{
		{
			filePath:    "test_simple.parquet",
			targetSize:  100 * 1024, // 100 KB
			complex:     false,
			description: "Generate simple Parquet file",
		},
		{
			filePath:    "test_complex.parquet",
			targetSize:  100 * 1024, // 100 KB
			complex:     true,
			description: "Generate complex Parquet file",
		},
	}

	for _, test := range tests {
		test := test // capture range variable
		t.Run(test.description, func(t *testing.T) {
			t.Parallel() // Parallelize each subtest

			// Ensure the generated file is cleaned up after the test, even if it fails
			t.Cleanup(func() {
				os.Remove(test.filePath)
			})

			// Generate the Parquet file
			err := GenerateParquetFile(test.filePath, test.targetSize, test.complex)
			assert.NoError(t, err, "Error should be nil when generating Parquet file")

			// Check the generated file's properties
			info, err := os.Stat(test.filePath)
			assert.NoError(t, err, "Error should be nil when checking Parquet file stats")
			assert.True(t, info.Size() > 0, "Generated Parquet file should have a size greater than 0")

			// Check that the file size is greater than or equal to the target size and within a reasonable range
			expectedMinSize := test.targetSize     // the minimum expected size
			expectedMaxSize := test.targetSize * 2 // the maximum expected size
			assert.GreaterOrEqual(t, info.Size(), expectedMinSize, "Generated Parquet file should be at least the target size")
			assert.LessOrEqual(t, info.Size(), expectedMaxSize, "Generated Parquet file should not be excessively larger than the target size")
		})
	}
}
