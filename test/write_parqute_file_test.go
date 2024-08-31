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

	generator "github.com/arrowarc/arrowarc/generator"
	integrations "github.com/arrowarc/arrowarc/integrations/filesystem"
	pipeline "github.com/arrowarc/arrowarc/pipeline"
	"github.com/stretchr/testify/require"
)

func TestWriteParquetFileStream(t *testing.T) {
	t.Parallel() // Parallelize the top-level test

	// Generate two sample Parquet files for testing: one simple and one complex
	inputSimpleFilePath := "sample_input_simple.parquet"
	err := generator.GenerateParquetFile(inputSimpleFilePath, 100*1024, false) // 100 KB, simple structure
	require.NoError(t, err, "Error should be nil when generating simple input Parquet file")

	inputComplexFilePath := "sample_input_complex.parquet"
	err = generator.GenerateParquetFile(inputComplexFilePath, 100*1024, true) // 100 KB, complex structure
	require.NoError(t, err, "Error should be nil when generating complex input Parquet file")

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

			// Setup the Parquet file reader
			reader, err := integrations.NewParquetReader(ctx, test.inputFilePath, &integrations.ParquetReadOptions{
				ChunkSize: test.chunkSize,
			})
			require.NoError(t, err, "Error should be nil when creating Parquet reader")
			require.NotNil(t, reader.Schema(), "Schema should not be nil")

			// Write records to the output Parquet file
			writer, err := integrations.NewParquetWriter(test.outputFilePath, reader.Schema(), integrations.NewDefaultParquetWriterProperties())
			require.NoError(t, err, "Error should be nil when creating Parquet writer")

			// Setup the pipeline to write the records to the Parquet file
			metrics, err := pipeline.NewDataPipeline(reader, writer).Start(ctx)
			require.NoError(t, err, "Error should be nil when starting data pipeline")
			require.NotNil(t, metrics, "Metrics should not be nil")

			// Print the metrics report
			t.Log(metrics)

			t.Cleanup(func() {
				os.Remove(test.outputFilePath)
			})
		})
	}
}
