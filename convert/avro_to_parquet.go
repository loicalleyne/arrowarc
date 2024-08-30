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
	"bufio"
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/avro"
	"github.com/apache/arrow/go/v17/parquet"
	"github.com/apache/arrow/go/v17/parquet/compress"
	pq "github.com/apache/arrow/go/v17/parquet/pqarrow"
)

// ConvertAvroToParquet converts an Avro OCF file to a Parquet file.
func ConvertAvroToParquet(
	ctx context.Context,
	avroFilePath, parquetFilePath string,
	chunkSize int,
	compressionType compress.Compression,
	batchSize int,
	pageSize int64,
	rowGroupSize int64,
	parallelism int,
) error {
	// Validate input parameters
	if avroFilePath == "" {
		return errors.New("avro file path cannot be empty")
	}
	if parquetFilePath == "" {
		return errors.New("parquet file path cannot be empty")
	}
	if chunkSize <= 0 {
		return errors.New("chunk size must be greater than zero")
	}
	if batchSize <= 0 {
		return errors.New("batch size must be greater than zero")
	}
	if pageSize <= 0 {
		return errors.New("page size must be greater than zero")
	}
	if rowGroupSize <= 0 {
		return errors.New("row group size must be greater than zero")
	}
	if ctx == nil {
		return errors.New("context cannot be nil")
	}

	// Open the Avro file in a streaming manner to handle large files
	avroFile, err := os.Open(avroFilePath)
	if err != nil {
		return fmt.Errorf("failed to open Avro file '%s': %w", avroFilePath, err)
	}
	defer avroFile.Close()

	// Create a buffered reader for efficient I/O
	bufferedAvroReader := bufio.NewReaderSize(avroFile, 32*1024) // 32KB buffer size

	// Initialize the Avro reader with chunking
	avroReader, err := avro.NewOCFReader(bufferedAvroReader, avro.WithChunk(chunkSize))
	if err != nil {
		return fmt.Errorf("failed to create Avro OCF reader: %w", err)
	}
	defer avroReader.Release()

	// Open the output Parquet file
	parquetFile, err := os.OpenFile(parquetFilePath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("failed to create Parquet file '%s': %w", parquetFilePath, err)
	}
	defer parquetFile.Close()

	// Configure Parquet writer properties for performance
	writerProps := parquet.NewWriterProperties(
		parquet.WithDictionaryDefault(true),
		parquet.WithVersion(parquet.V2_LATEST),
		parquet.WithCompression(compressionType),
		parquet.WithBatchSize(int64(batchSize)),
		parquet.WithDataPageSize(pageSize),
		parquet.WithMaxRowGroupLength(rowGroupSize),
	)

	// Configure Arrow writer properties
	arrowWriterProps := pq.NewArrowWriterProperties(pq.WithStoreSchema())

	// Create the Parquet writer
	parquetWriter, err := pq.NewFileWriter(avroReader.Schema(), parquetFile, writerProps, arrowWriterProps)
	if err != nil {
		return fmt.Errorf("failed to create Parquet writer: %w", err)
	}
	defer parquetWriter.Close()

	startTime := time.Now()

	// Setup for parallel processing
	recordChan := make(chan arrow.Record, parallelism)
	errorChan := make(chan error, parallelism)
	var wg sync.WaitGroup

	// Start worker goroutines for writing records concurrently
	for i := 0; i < parallelism; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for record := range recordChan {
				if err := parquetWriter.WriteBuffered(record); err != nil {
					errorChan <- fmt.Errorf("failed to write record to Parquet: %w", err)
					return
				}
				record.Release()
			}
		}()
	}

	// Process the Avro records and write to Parquet
	go func() {
		for avroReader.Next() {
			if err := avroReader.Err(); err != nil {
				errorChan <- fmt.Errorf("error reading Avro record: %w", err)
				return
			}
			record := avroReader.Record()
			select {
			case recordChan <- record:
			case <-ctx.Done():
				errorChan <- ctx.Err()
				return
			}
		}
		close(recordChan)
	}()

	// Wait for all goroutines to complete
	wg.Wait()
	close(errorChan)

	// Check for errors from worker goroutines
	for err := range errorChan {
		if err != nil {
			return err
		}
	}

	// Ensure everything is flushed and closed properly
	if err := parquetWriter.Close(); err != nil {
		return fmt.Errorf("failed to close Parquet writer: %w", err)
	}

	fmt.Printf("Conversion completed in %v\n", time.Since(startTime))
	return nil
}
