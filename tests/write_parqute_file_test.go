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

	"github.com/apache/arrow/go/v17/parquet/compress"
	filesystem "github.com/arrowarc/arrowarc/integrations/filesystem"
	"github.com/arrowarc/arrowarc/pkg/parquet"
	"github.com/stretchr/testify/assert"
)

func TestWriteParquetFileStream(t *testing.T) {
	t.Parallel() // Parallelize the top-level test

	// Generate two sample Parquet files for testing: one simple and one complex
	inputSimpleFilePath := "sample_input_simple.parquet"
	err := parquet.GenerateParquetFile(inputSimpleFilePath, 100*1024, false) // 100 KB, simple structure
	assert.NoError(t, err, "Error should be nil when generating simple input Parquet file")

	inputComplexFilePath := "sample_input_complex.parquet"
	err = parquet.GenerateParquetFile(inputComplexFilePath, 100*1024, true) // 100 KB, complex structure
	assert.NoError(t, err, "Error should be nil when generating complex input Parquet file")

	// Ensure the files are removed after all tests complete
	t.Cleanup(func() {
		os.Remove(inputSimpleFilePath)
		os.Remove(inputComplexFilePath)
	})

	tests := []struct {
		inputFilePath  string
		outputFilePath string
		chunkSize      int64
		description    string
		useCustomOpts  bool
	}{
		{
			inputFilePath:  inputSimpleFilePath,
			outputFilePath: "sample_output_simple.parquet",
			chunkSize:      1024,
			description:    "Read and write simple Parquet file",
			useCustomOpts:  false,
		},
		{
			inputFilePath:  inputComplexFilePath,
			outputFilePath: "sample_output_complex.parquet",
			chunkSize:      2048,
			description:    "Read and write complex Parquet file",
			useCustomOpts:  false,
		},
	}

	for _, test := range tests {
		test := test // capture range variable
		t.Run(test.description, func(t *testing.T) {
			t.Parallel() // Parallelize each subtest

			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			// Create a reader for the input Parquet file
			reader, err := filesystem.ReadParquetFileStream(ctx, test.inputFilePath, false, test.chunkSize, nil, nil, true)
			assert.NoError(t, err, "Error should be nil when reading Parquet file")

			// Determine whether to use custom options or default options
			var opts *filesystem.ParquetWriteOptions
			if test.useCustomOpts {
				opts = &filesystem.ParquetWriteOptions{
					Compression: compress.Codecs.Zstd, // Example of custom compression
					// Other custom options can be set here
				}
			} else {
				opts = filesystem.NewDefaultParquetWriteOptions()
			}

			// Write records to the output Parquet file
			err = filesystem.WriteParquetFileStream(ctx, test.outputFilePath, reader, opts)
			assert.NoError(t, err, "Error should be nil when writing Parquet file")

			// Check if the output file exists and has content
			info, err := os.Stat(test.outputFilePath)
			assert.NoError(t, err, "Error should be nil when checking output Parquet file stats")
			assert.True(t, info.Size() > 0, "Generated output Parquet file should have a size greater than 0")

			t.Cleanup(func() {
				os.Remove(test.outputFilePath)
			})
		})
	}
}
