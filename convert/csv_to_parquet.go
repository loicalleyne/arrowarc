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
	"errors"
	"fmt"

	integrations "github.com/arrowarc/arrowarc/integrations/filesystem"
	"github.com/arrowarc/arrowarc/pipeline"
)

func ConvertCSVToParquet(
	ctx context.Context,
	csvFilePath, parquetFilePath string,
	hasHeader bool, chunkSize int64,
	delimiter rune,
	nullValues []string,
	stringsCanBeNull bool,
) error {

	// Validate input parameters
	if csvFilePath == "" {
		return errors.New("CSV file path cannot be empty")
	}
	if parquetFilePath == "" {
		return errors.New("parquet file path cannot be empty")
	}
	if chunkSize <= 0 {
		return errors.New("chunk size must be greater than zero")
	}
	if ctx == nil {
		return errors.New("context cannot be nil")
	}

	// Setup CSV reader
	csvReader, err := integrations.NewCSVReader(ctx, csvFilePath, nil, &integrations.CSVReadOptions{
		HasHeader:        hasHeader,
		ChunkSize:        chunkSize,
		Delimiter:        delimiter,
		NullValues:       nullValues,
		StringsCanBeNull: stringsCanBeNull,
	})
	if err != nil {
		return fmt.Errorf("failed to create CSV reader for file '%s': %w", csvFilePath, err)
	}

	// Setup Parquet writer properties
	parquetWriterProps := integrations.NewDefaultParquetWriterProperties()

	schema := csvReader.Schema()

	// Setup Parquet writer
	parquetWriter, err := integrations.NewParquetWriter(parquetFilePath, schema, parquetWriterProps)
	if err != nil {
		return fmt.Errorf("failed to create Parquet writer for file '%s': %w", parquetFilePath, err)
	}
	defer func() {
		if cerr := parquetWriter.Close(); cerr != nil {
			err = fmt.Errorf("failed to close Parquet writer: %w", cerr)
		}
	}()

	// Setup and start pipeline
	metrics, err := pipeline.NewDataPipeline(csvReader, parquetWriter).Start(ctx)
	if err != nil {
		return fmt.Errorf("failed to convert CSV to Parquet: %w", err)
	}

	fmt.Println(metrics.Report())

	return nil
}
