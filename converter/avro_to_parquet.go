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

// Package converter provides utilities for converting data between different formats.
package converter

import (
	"context"
	"errors"
	"fmt"

	"github.com/apache/arrow-go/v18/parquet/compress"
	integrations "github.com/arrowarc/arrowarc/integrations/filesystem"
	"github.com/arrowarc/arrowarc/pipeline"
)

// ConvertAvroToParquet converts an Avro OCF file to a Parquet file.
func ConvertAvroToParquet(ctx context.Context, avroPath, parquetPath string, chunkSize int64, compression compress.Compression) (string, error) {
	// Validate inputs before proceeding
	if err := validateInputs(ctx, avroPath, parquetPath, chunkSize); err != nil {
		return "", err
	}

	// Initialize the Avro reader
	avroReader, err := integrations.NewAvroReader(ctx, avroPath, &integrations.AvroReadOptions{
		ChunkSize: chunkSize,
	})
	if err != nil {
		return "", fmt.Errorf("failed to create Avro reader: %w", err)
	}

	// Ensure Avro reader is closed once the function completes
	defer func() {
		if closeErr := avroReader.Close(); closeErr != nil {
			err = fmt.Errorf("failed to close Avro reader: %w", closeErr)
		}
	}()

	// Initialize the Parquet writer
	parquetWriter, err := integrations.NewParquetWriter(parquetPath, avroReader.Schema(), integrations.NewDefaultParquetWriterProperties())
	if err != nil {
		return "", fmt.Errorf("failed to create Parquet writer: %w", err)
	}

	// Ensure Parquet writer is closed once the function completes
	defer func() {
		if closeErr := parquetWriter.Close(); closeErr != nil {
			err = fmt.Errorf("failed to close Parquet writer: %w", closeErr)
		}
	}()

	// Create a new data pipeline
	p := pipeline.NewDataPipeline(avroReader, parquetWriter)

	// Run the pipeline and capture metrics
	metrics, err := p.Start(ctx)
	if err != nil {
		return "", fmt.Errorf("pipeline failed to start: %w", err)
	}

	// Wait for the pipeline to complete
	if pipelineErr := <-p.Done(); pipelineErr != nil {
		return "", fmt.Errorf("pipeline encountered an error: %w", pipelineErr)
	}

	// Return the pipeline metrics
	return metrics, nil
}

// validateInputs ensures the provided inputs are valid
func validateInputs(ctx context.Context, avroPath, parquetPath string, chunkSize int64) error {
	if avroPath == "" {
		return errors.New("avro file path cannot be empty")
	}
	if parquetPath == "" {
		return errors.New("parquet file path cannot be empty")
	}
	if chunkSize <= 0 {
		return errors.New("chunk size must be greater than zero")
	}
	if ctx == nil {
		return errors.New("context cannot be nil")
	}
	return nil
}
