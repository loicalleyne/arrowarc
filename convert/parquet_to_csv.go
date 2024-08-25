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
	"io"
	"strings"

	"github.com/apache/arrow/go/v17/arrow"
	filesystem "github.com/arrowarc/arrowarc/integrations/filesystem"
)

func ConvertParquetToCSV(ctx context.Context, parquetFilePath, csvFilePath string, memoryMap bool, chunkSize int64, columns []string, rowGroups []int, parallel bool, delimiter rune, includeHeader bool, nullValue string, stringsReplacer *strings.Replacer, boolFormatter func(bool) string) error {
	reader, err := filesystem.ReadParquetFileStream(ctx, parquetFilePath, memoryMap, chunkSize, columns, rowGroups, parallel)
	if err != nil {
		return fmt.Errorf("error while creating Parquet reader: %w", err)
	}

	// Read the first record to extract the schema
	firstRecord, err := reader.Read()
	if err != nil {
		if err == io.EOF {
			return fmt.Errorf("no records found in the Parquet file")
		}
		return fmt.Errorf("error reading the first record from Parquet file: %w", err)
	}
	schema := firstRecord.Schema()

	// Filter the schema if specific columns are provided
	if len(columns) > 0 {
		schema = filterSchema(schema, columns)
	}

	writer, err := filesystem.NewCSVRecordWriter(ctx, csvFilePath, schema, delimiter, includeHeader, nullValue, stringsReplacer, boolFormatter)
	if err != nil {
		firstRecord.Release()
		return fmt.Errorf("error while creating CSV writer: %w", err)
	}

	if err := writer.Write(firstRecord); err != nil {
		firstRecord.Release()
		return fmt.Errorf("error writing the first record to CSV file: %w", err)
	}
	firstRecord.Release()

	for {
		record, err := reader.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			return fmt.Errorf("error reading from Parquet file: %w", err)
		}

		if err := writer.Write(record); err != nil {
			return fmt.Errorf("error writing to CSV file: %w", err)
		}

		record.Release()
	}

	return nil
}

func filterSchema(schema *arrow.Schema, columns []string) *arrow.Schema {
	fields := make([]arrow.Field, 0, len(columns))
	for _, col := range columns {
		for _, field := range schema.Fields() {
			if field.Name == col {
				fields = append(fields, field)
				break
			}
		}
	}
	return arrow.NewSchema(fields, nil)
}
