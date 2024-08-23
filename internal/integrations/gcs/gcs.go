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
	"math"
	"strings"

	"cloud.google.com/go/storage"
	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/csv"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/apache/arrow/go/v17/parquet"
	"github.com/apache/arrow/go/v17/parquet/pqarrow"
	"google.golang.org/api/option"
)

type FileFormat string

const (
	ParquetFormat FileFormat = "parquet"
	CSVFormat     FileFormat = "csv"
)

type GCSSink struct {
	client     *storage.Client
	bucketName string
}

func NewGCSSink(ctx context.Context, bucketName string, credsFile string) (*GCSSink, error) {
	client, err := storage.NewClient(ctx, option.WithCredentialsFile(credsFile))
	if err != nil {
		return nil, fmt.Errorf("failed to create GCS client: %w", err)
	}

	return &GCSSink{
		client:     client,
		bucketName: bucketName,
	}, nil
}

func (s *GCSSink) WriteToGCS(ctx context.Context, recordChan <-chan arrow.Record, filePath string, format FileFormat, delimiter rune, includeHeader bool, nullValue string, stringsReplacer *strings.Replacer, boolFormatter func(bool) string) <-chan error {
	errChan := make(chan error, 1)

	go func() {
		defer close(errChan)

		bucket := s.client.Bucket(s.bucketName)
		obj := bucket.Object(filePath)
		writer := obj.NewWriter(ctx)
		defer writer.Close()

		var err error
		switch format {
		case ParquetFormat:
			err = s.writeParquet(ctx, recordChan, writer)
		case CSVFormat:
			err = s.writeCSV(ctx, recordChan, writer, delimiter, includeHeader, nullValue, stringsReplacer, boolFormatter)
		default:
			err = fmt.Errorf("unsupported file format: %s", format)
		}

		if err != nil {
			errChan <- err
		}
	}()

	return errChan
}

func (s *GCSSink) writeParquet(ctx context.Context, recordChan <-chan arrow.Record, writer io.Writer) error {
	mem := memory.NewGoAllocator()
	var schema *arrow.Schema
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
		case record, ok := <-recordChan:
			if !ok {
				return nil
			}

			if schema == nil {
				schema = record.Schema()
				writerProps := parquet.NewWriterProperties(parquet.WithAllocator(mem), parquet.WithMaxRowGroupLength(math.MaxInt64))
				var err error
				parquetWriter, err = pqarrow.NewFileWriter(schema, writer, writerProps, pqarrow.DefaultWriterProps())
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

func (s *GCSSink) writeCSV(ctx context.Context, recordChan <-chan arrow.Record, writer io.Writer, delimiter rune, includeHeader bool, nullValue string, stringsReplacer *strings.Replacer, boolFormatter func(bool) string) error {
	var csvWriter *csv.Writer

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case record, ok := <-recordChan:
			if !ok {
				if csvWriter != nil {
					csvWriter.Flush()
				}
				return nil
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

func (s *GCSSink) Close() error {
	return s.client.Close()
}
