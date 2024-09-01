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

	integrations "github.com/arrowarc/arrowarc/integrations/filesystem"
	"github.com/arrowarc/arrowarc/pipeline"
	csv "github.com/arrowarc/arrowarc/pkg/csv"
)

// ConvertCSVToParquet converts a CSV file to a Parquet file using Arrow
func ConvertCSVToJSON(
	ctx context.Context,
	csvFilePath, jsonFilePath string,
	hasHeader bool, chunkSize int64,
	delimiter rune,
	nullValues []string,
	stringsCanBeNull bool,
) (string, error) {

	// Validate input parameters
	if csvFilePath == "" {
		return "", errors.New("CSV file path cannot be empty")
	}
	if jsonFilePath == "" {
		return "", errors.New("JSON file path cannot be empty")
	}
	if chunkSize <= 0 {
		return "", errors.New("chunk size must be greater than zero")
	}
	if ctx == nil {
		return "", errors.New("context cannot be nil")
	}

	// Step 1: Infer schema from the CSV file
	schema, err := csv.InferCSVArrowSchema(ctx, csvFilePath, &csv.CSVReadOptions{
		HasHeader:        hasHeader,
		Delimiter:        delimiter,
		NullValues:       nullValues,
		StringsCanBeNull: stringsCanBeNull,
	})
	if err != nil {
		return "", fmt.Errorf("failed to infer schema: %w", err)
	}

	// Step 2: Create CSV reader with the inferred schema
	csvReader, err := integrations.NewCSVReader(ctx, csvFilePath, schema, &integrations.CSVReadOptions{
		HasHeader:        hasHeader,
		ChunkSize:        chunkSize,
		Delimiter:        delimiter,
		NullValues:       nullValues,
		StringsCanBeNull: stringsCanBeNull,
	})
	if err != nil {
		return "", fmt.Errorf("failed to create CSV reader: %w", err)
	}
	defer csvReader.Close()

	// Step 3: Setup Parquet writer with the inferred schema
	jsonWriter, err := integrations.NewJSONWriter(ctx, jsonFilePath)
	if err != nil {
		return "", fmt.Errorf("failed to create JSON writer: %w", err)
	}
	defer jsonWriter.Close()

	// Create pipeline
	p := pipeline.NewDataPipeline(csvReader, jsonWriter)

	// Start the pipeline and wait for completion
	metrics, startErr := p.Start(ctx)
	if startErr != nil {
		return "", fmt.Errorf("failed to start conversion pipeline: %w", startErr)
	}

	// Wait for the pipeline to finish
	if pipelineErr := <-p.Done(); pipelineErr != nil {
		return "", fmt.Errorf("pipeline encountered an error: %w", pipelineErr)
	}

	return metrics, nil
}
