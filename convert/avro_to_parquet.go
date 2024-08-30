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
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/avro"
	"github.com/apache/arrow/go/v17/parquet/compress"
	pq "github.com/apache/arrow/go/v17/parquet/pqarrow"
	filesystem "github.com/arrowarc/arrowarc/integrations/filesystem"
)

// ConvertAvroToParquet converts an Avro OCF file to a Parquet file.
func ConvertAvroToParquet(ctx context.Context, avroPath, parquetPath string, chunkSize int, compression compress.Compression) error {
	if err := validateInputs(ctx, avroPath, parquetPath, chunkSize); err != nil {
		return err
	}

	avroReader, err := createAvroReader(avroPath, chunkSize)
	if err != nil {
		return err
	}
	defer avroReader.Release()

	parquetWriter, err := createParquetWriter(parquetPath, avroReader.Schema())
	if err != nil {
		return err
	}
	defer parquetWriter.Close()

	return convertData(avroReader, parquetWriter)
}

func validateInputs(ctx context.Context, avroPath, parquetPath string, chunkSize int) error {
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

func createAvroReader(avroPath string, chunkSize int) (*avro.OCFReader, error) {
	data, err := os.ReadFile(avroPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read Avro file '%s': %w", avroPath, err)
	}

	r := bytes.NewReader(data)
	bufReader := bufio.NewReaderSize(r, 4096*8)
	return avro.NewOCFReader(bufReader, avro.WithChunk(chunkSize), avro.WithReadCacheSize(4096*8))
}

func createParquetWriter(parquetPath string, schema *arrow.Schema) (*pq.FileWriter, error) {
	file, err := os.OpenFile(parquetPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to create Parquet file: %w", err)
	}

	writerProps := filesystem.NewDefaultParquetWriterProperties()
	arrowProps := pq.NewArrowWriterProperties(pq.WithStoreSchema())

	return pq.NewFileWriter(schema, file, writerProps, arrowProps)
}

func convertData(reader *avro.OCFReader, writer *pq.FileWriter) error {
	startTime := time.Now()

	for reader.Next() {
		if err := reader.Err(); err != nil {
			return fmt.Errorf("error reading Avro record: %w", err)
		}

		record := reader.Record()
		if err := writer.WriteBuffered(record); err != nil {
			return fmt.Errorf("error writing Parquet record: %w", err)
		}
		record.Release()
	}

	if err := reader.Err(); err != nil && err != io.EOF {
		return fmt.Errorf("error in Avro reader: %w", err)
	}

	fmt.Printf("Conversion completed in %v\n", time.Since(startTime))
	return nil
}
