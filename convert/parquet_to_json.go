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

package convert

import (
	"context"
	"fmt"

	filesystem "github.com/arrowarc/arrowarc/integrations/filesystem"
	"github.com/arrowarc/arrowarc/pipeline"
)

func ConvertParquetToJSON(ctx context.Context, parquetFilePath, jsonFilePath string, memoryMap bool, chunkSize int64, columns []string, rowGroups []int, parallel bool, includeStructs bool) (string, error) {
	// Validate input parameters
	if parquetFilePath == "" {
		return "", fmt.Errorf("parquet file path cannot be empty")
	}
	if jsonFilePath == "" {
		return "", fmt.Errorf("JSON file path cannot be empty")
	}
	if chunkSize <= 0 {
		return "", fmt.Errorf("chunk size must be greater than zero")
	}

	// Setup the reader
	reader, err := filesystem.NewParquetReader(ctx, parquetFilePath, &filesystem.ParquetReadOptions{
		MemoryMap: memoryMap,
		RowGroups: rowGroups,
		Parallel:  parallel,
	})
	if err != nil {
		return "", fmt.Errorf("failed to create Parquet reader for file '%s': %w", parquetFilePath, err)
	}

	// Setup the writer
	writer, err := filesystem.NewJSONWriter(ctx, jsonFilePath)
	if err != nil {
		return "", fmt.Errorf("failed to create JSON writer for file '%s': %w", jsonFilePath, err)
	}
	defer func() {
		if cerr := writer.Close(); cerr != nil {
			err = fmt.Errorf("failed to close JSON writer: %w", cerr)
		}
	}()
	// Setup pipeline
	p := pipeline.NewDataPipeline(reader, writer)

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
