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
	"strings"

	"cloud.google.com/go/storage"
	"github.com/apache/arrow-go/v18/arrow/arrio"
	"github.com/apache/arrow-go/v18/arrow/csv"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	pool "github.com/arrowarc/arrowarc/internal/memory"
	"google.golang.org/api/option"
)

// FileFormat represents the supported file formats for output.
type FileFormat string

const (
	ParquetFormat FileFormat = "parquet"
	CSVFormat     FileFormat = "csv"
)

// GCSSink represents a Google Cloud Storage sink for writing files.
type GCSSink struct {
	client     *storage.Client
	bucketName string
}

// NewGCSSink creates a new GCSSink with the specified bucket name and credentials file.
func NewGCSSink(ctx context.Context, bucketName, credsFile string) (*GCSSink, error) {
	client, err := storage.NewClient(ctx, option.WithCredentialsFile(credsFile))
	if err != nil {
		return nil, fmt.Errorf("failed to create GCS client: %w", err)
	}

	return &GCSSink{
		client:     client,
		bucketName: bucketName,
	}, nil
}

// WriteToGCS writes data from an Arrow reader to a GCS object in the specified format.
func (s *GCSSink) WriteToGCS(ctx context.Context, reader arrio.Reader, filePath string, format FileFormat, delimiter rune, includeHeader bool, nullValue string, stringsReplacer *strings.Replacer, boolFormatter func(bool) string) error {
	bucket := s.client.Bucket(s.bucketName)
	obj := bucket.Object(filePath)
	writer := obj.NewWriter(ctx)
	defer writer.Close()

	var err error
	switch format {
	case ParquetFormat:
		err = s.writeParquet(ctx, reader, writer)
	case CSVFormat:
		err = s.writeCSV(ctx, reader, writer, delimiter, includeHeader, nullValue, stringsReplacer, boolFormatter)
	default:
		return fmt.Errorf("unsupported file format: %s", format)
	}

	if err != nil {
		return fmt.Errorf("failed to write to GCS: %w", err)
	}
	return nil
}

// writeParquet writes data from an Arrow reader to a Parquet file on GCS.
func (s *GCSSink) writeParquet(ctx context.Context, reader arrio.Reader, writer io.Writer) error {
	alloc := pool.GetAllocator()
	defer pool.PutAllocator(alloc)

	var parquetWriter *pqarrow.FileWriter
	defer func() {
		if parquetWriter != nil {
			parquetWriter.Close()
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			record, err := reader.Read()
			if err == io.EOF {
				return nil
			}
			if err != nil {
				return fmt.Errorf("failed to read record: %w", err)
			}

			if parquetWriter == nil {
				schema := record.Schema()
				writerProps := parquet.NewWriterProperties(parquet.WithAllocator(alloc))
				parquetWriter, err = pqarrow.NewFileWriter(schema, writer, writerProps, pqarrow.NewArrowWriterProperties())
				if err != nil {
					return fmt.Errorf("failed to create Parquet writer: %w", err)
				}
			}

			if err := parquetWriter.Write(record); err != nil {
				return fmt.Errorf("failed to write record to Parquet: %w", err)
			}
		}
	}
}

// writeCSV writes data from an Arrow reader to a CSV file on GCS.
func (s *GCSSink) writeCSV(ctx context.Context, reader arrio.Reader, writer io.Writer, delimiter rune, includeHeader bool, nullValue string, stringsReplacer *strings.Replacer, boolFormatter func(bool) string) error {
	alloc := pool.GetAllocator()
	defer pool.PutAllocator(alloc)

	var csvWriter *csv.Writer

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			record, err := reader.Read()
			if err == io.EOF {
				if csvWriter != nil {
					csvWriter.Flush()
				}
				return nil
			}
			if err != nil {
				return fmt.Errorf("failed to read record: %w", err)
			}

			if csvWriter == nil {
				csvWriter = csv.NewWriter(writer, record.Schema(),
					csv.WithComma(delimiter),
					csv.WithHeader(includeHeader),
					csv.WithNullWriter(nullValue),
					csv.WithStringsReplacer(stringsReplacer),
					csv.WithBoolWriter(boolFormatter),
				)
				defer csvWriter.Flush()
			}

			if err := csvWriter.Write(record); err != nil {
				return fmt.Errorf("failed to write record to CSV: %w", err)
			}

			if err := csvWriter.Error(); err != nil {
				return fmt.Errorf("CSV writer encountered an error: %w", err)
			}
		}
	}
}

// Close releases resources associated with the GCSSink.
func (s *GCSSink) Close() error {
	return s.client.Close()
}
