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
	"sync"

	filesystem "github.com/ArrowArc/ArrowArc/internal/integrations/filesystem"
	"github.com/apache/arrow/go/v17/arrow"
)

func ConvertCSVToParquet(ctx context.Context, csvFilePath, parquetFilePath string, schema *arrow.Schema, hasHeader bool, chunkSize int64, delimiter rune, nullValues []string, stringsCanBeNull bool) error {
	recordChan, errChan := filesystem.ReadCSVFileStream(ctx, csvFilePath, schema, hasHeader, chunkSize, delimiter, nullValues, stringsCanBeNull)

	writeErrChan := filesystem.WriteParquetFileStream(ctx, parquetFilePath, recordChan)

	var wg sync.WaitGroup
	wg.Add(2)

	var readErr, writeErr error

	go func() {
		defer wg.Done()
		for err := range errChan {
			if err != nil {
				readErr = fmt.Errorf("error while reading CSV file: %w", err)
				return
			}
		}
	}()

	go func() {
		defer wg.Done()
		for err := range writeErrChan {
			if err != nil {
				writeErr = fmt.Errorf("error while writing Parquet file: %w", err)
				return
			}
		}
	}()

	wg.Wait()

	if readErr != nil {
		return readErr
	}
	if writeErr != nil {
		return writeErr
	}
	return nil
}
