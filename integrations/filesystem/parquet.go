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

package integrations

import (
	"context"
	"fmt"
	"io"
	"os"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/apache/arrow/go/v17/parquet"
	"github.com/apache/arrow/go/v17/parquet/compress"
	"github.com/apache/arrow/go/v17/parquet/file"
	"github.com/apache/arrow/go/v17/parquet/pqarrow"
)

func ReadParquetFileStream(ctx context.Context, filePath string, memoryMap bool, chunkSize int64) (<-chan arrow.Record, <-chan error) {
	recordChan := make(chan arrow.Record)
	errChan := make(chan error, 1)

	go func() {
		defer close(recordChan)
		defer close(errChan)

		// Open the Parquet file with or without memory mapping based on the flag
		parquetRdr, err := file.OpenParquetFile(filePath, memoryMap)
		if err != nil {
			errChan <- fmt.Errorf("failed to open Parquet file: %w", err)
			return
		}
		defer parquetRdr.Close()
		fmt.Println("Successfully opened Parquet file")

		// Create a Parquet to Arrow file reader
		arrowRdr, err := pqarrow.NewFileReader(parquetRdr, pqarrow.ArrowReadProperties{BatchSize: chunkSize, Parallel: true}, memory.DefaultAllocator)
		if err != nil {
			errChan <- fmt.Errorf("failed to create Arrow file reader: %w", err)
			return
		}

		fmt.Println("Successfully created Arrow file reader")

		// Get a RecordReader for all columns and row groups
		recordReader, err := arrowRdr.GetRecordReader(ctx, nil, nil)
		if err != nil {
			errChan <- fmt.Errorf("failed to get record reader: %w", err)
			return
		}
		defer recordReader.Release()
		fmt.Println("Successfully got RecordReader")

		for {
			select {
			case <-ctx.Done():
				errChan <- ctx.Err()
				return
			default:
			}

			if !recordReader.Next() {
				if err := recordReader.Err(); err != nil && err != io.EOF {
					errChan <- err // Send error to the error channel, ignore EOF
				}
				return
			}

			record := recordReader.Record()
			if record == nil {
				continue
			}

			record.Retain()
			recordChan <- record
		}
	}()

	return recordChan, errChan
}

func WriteParquetFileStream(ctx context.Context, filePath string, recordChan <-chan arrow.Record) <-chan error {
	errChan := make(chan error, 1)

	go func() {
		defer close(errChan)

		mem := memory.NewGoAllocator()
		var schema *arrow.Schema
		var parquetWriter *pqarrow.FileWriter

		// Open a file for writing the Parquet data
		file, err := os.Create(filePath)
		if err != nil {
			errChan <- fmt.Errorf("failed to create file: %w", err)
			return
		}
		defer file.Close()

		defer func() {
			if parquetWriter != nil {
				parquetWriter.Close()
			}
		}()

		for {
			select {
			case <-ctx.Done():
				errChan <- ctx.Err()
				return // Exit the goroutine if context is canceled or times out
			case record, ok := <-recordChan:
				if !ok {
					// Channel closed, end of data
					return
				}

				if schema == nil {
					schema = record.Schema()
					writerProps := parquet.NewWriterProperties(
						parquet.WithAllocator(mem),
						parquet.WithCompression(compress.Codecs.Snappy),
					)

					// Create the Parquet writer with the file and schema
					parquetWriter, err = pqarrow.NewFileWriter(schema, file, writerProps, pqarrow.DefaultWriterProps())
					if err != nil {
						errChan <- fmt.Errorf("failed to create Parquet writer: %w", err)
						return
					}
				}

				if err := parquetWriter.Write(record); err != nil {
					errChan <- fmt.Errorf("failed to write record to Parquet: %w", err)
					return
				}
			}
		}
	}()

	return errChan
}
