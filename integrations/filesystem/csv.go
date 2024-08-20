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

package file

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/csv"
)

func ReadCSVFileStream(ctx context.Context, filePath string, schema *arrow.Schema, hasHeader bool, chunkSize int64, delimiter rune, nullValues []string, stringsCanBeNull bool) (<-chan arrow.Record, <-chan error) {
	recordChan := make(chan arrow.Record)
	errChan := make(chan error, 1)

	go func() {
		defer close(recordChan)
		defer close(errChan)

		file, err := os.Open(filePath)
		if err != nil {
			errChan <- fmt.Errorf("failed to open CSV file: %w", err)
			return
		}
		defer file.Close()

		options := []csv.Option{
			csv.WithChunk(int(chunkSize)),
			csv.WithComma(delimiter),
			csv.WithHeader(hasHeader),
			csv.WithNullReader(stringsCanBeNull, nullValues...),
		}

		reader := csv.NewReader(file, schema, options...)
		defer reader.Release()

		for {
			select {
			case <-ctx.Done():
				errChan <- ctx.Err()
				return
			default:
			}

			if !reader.Next() {
				if err := reader.Err(); err != nil && err != io.EOF {
					errChan <- fmt.Errorf("error reading CSV record: %w", err)
				}
				return
			}

			record := reader.Record()
			if record == nil {
				continue
			}

			record.Retain()
			recordChan <- record
		}
	}()

	return recordChan, errChan
}

func WriteCSVFileStream(ctx context.Context, filePath string, schema *arrow.Schema, recordChan <-chan arrow.Record, delimiter rune, includeHeader bool, nullValue string, stringsReplacer *strings.Replacer, boolFormatter func(bool) string) <-chan error {
	errChan := make(chan error, 1)

	go func() {
		defer close(errChan)

		file, err := os.Create(filePath)
		if err != nil {
			errChan <- fmt.Errorf("failed to create CSV file: %w", err)
			return
		}
		defer file.Close()

		writer := csv.NewWriter(file, schema,
			csv.WithComma(delimiter),
			csv.WithHeader(includeHeader),
			csv.WithNullWriter(nullValue),
			csv.WithStringsReplacer(stringsReplacer),
			csv.WithBoolWriter(boolFormatter),
		)
		defer writer.Flush()

		for {
			select {
			case <-ctx.Done():
				errChan <- ctx.Err()
				return
			case record, ok := <-recordChan:
				if !ok {
					return
				}

				if err := writer.Write(record); err != nil {
					errChan <- fmt.Errorf("failed to write record to CSV: %w", err)
					return
				}

				if err := writer.Error(); err != nil {
					errChan <- fmt.Errorf("CSV writer encountered an error: %w", err)
					return
				}
			}
		}
	}()

	return errChan
}
