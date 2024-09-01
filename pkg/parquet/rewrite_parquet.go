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

// Package parquet provides utilities for rewriting Parquet files.
package parquet

import (
	"context"
	"errors"
	"fmt"

	"github.com/apache/arrow/go/v17/parquet"
	integrations "github.com/arrowarc/arrowarc/integrations/filesystem"
	"github.com/arrowarc/arrowarc/pipeline"
)

func RewriteParquetFile(
	ctx context.Context,
	inputFilePath, outputFilePath string,
	memoryMap bool, chunkSize int64,
	columns []string, rowGroups []int, parallel bool,
	parquetWriterProps *parquet.WriterProperties,
) error {
	// Validate input parameters
	if inputFilePath == "" {
		return errors.New("input file path cannot be empty")
	}
	if outputFilePath == "" {
		return errors.New("output file path cannot be empty")
	}
	if chunkSize <= 0 {
		return errors.New("chunk size must be greater than zero")
	}
	if ctx == nil {
		return errors.New("context cannot be nil")
	}

	// Create read options
	readOptions := &integrations.ParquetReadOptions{
		MemoryMap: memoryMap,
		Parallel:  true,
		ChunkSize: chunkSize,
	}
	if parallel {
		readOptions.Parallel = true
	}

	// Create the Parquet reader
	reader, err := integrations.NewParquetReader(ctx, inputFilePath, readOptions)
	if err != nil {
		return fmt.Errorf("failed to create Parquet reader: %w", err)
	}
	defer reader.Close()

	// Use provided ParquetWriter properties or default if none provided
	if parquetWriterProps == nil {
		parquetWriterProps = integrations.NewDefaultParquetWriterProperties()
	}

	// Create the Parquet writer
	writer, err := integrations.NewParquetWriter(outputFilePath, reader.Schema(), parquetWriterProps)
	if err != nil {
		return fmt.Errorf("failed to create Parquet writer: %w", err)
	}
	defer writer.Close()

	// Create the pipeline
	pipeline := pipeline.NewDataPipeline(reader, writer)

	// Start the pipeline
	report, err := pipeline.Start(ctx)
	if err != nil {
		return fmt.Errorf("failed to rewrite Parquet file: %w", err)
	}

	fmt.Println(report)

	return nil
}
