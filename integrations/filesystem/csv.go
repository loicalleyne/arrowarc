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
	"strings"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/csv"
	"github.com/arrowarc/arrowarc/internal/arrio"
)

// CSVRecordReader implements arrio.Reader for reading records from CSV files.
type CSVRecordReader struct {
	reader *csv.Reader
}

// NewCSVRecordReader creates a new reader for reading records from a CSV file.
func NewCSVRecordReader(ctx context.Context, filePath string, schema *arrow.Schema, hasHeader bool, chunkSize int64, delimiter rune, nullValues []string, stringsCanBeNull bool) (arrio.Reader, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open CSV file: %w", err)
	}

	options := []csv.Option{
		csv.WithChunk(int(chunkSize)),
		csv.WithComma(delimiter),
		csv.WithHeader(hasHeader),
		csv.WithNullReader(stringsCanBeNull, nullValues...),
	}

	reader := csv.NewReader(file, schema, options...)

	return &CSVRecordReader{reader: reader}, nil
}

// Read reads the next record from the CSV file.
func (r *CSVRecordReader) Read() (arrow.Record, error) {
	if !r.reader.Next() {
		if err := r.reader.Err(); err != nil && err != io.EOF {
			return nil, fmt.Errorf("error reading CSV record: %w", err)
		}
		return nil, io.EOF
	}

	record := r.reader.Record()
	if record == nil {
		return nil, io.EOF
	}

	record.Retain() // Retain the record to manage its memory lifecycle
	return record, nil
}

// Close releases resources associated with the CSV reader.
func (r *CSVRecordReader) Close() error {
	if r.reader != nil {
		r.reader.Release()
	}
	return nil
}

// CSVRecordWriter implements arrio.Writer for writing records to CSV files.
type CSVRecordWriter struct {
	writer *csv.Writer
}

// NewCSVRecordWriter creates a new writer for writing records to a CSV file.
func NewCSVRecordWriter(ctx context.Context, filePath string, schema *arrow.Schema, delimiter rune, includeHeader bool, nullValue string, stringsReplacer *strings.Replacer, boolFormatter func(bool) string) (arrio.Writer, error) {
	file, err := os.Create(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to create CSV file: %w", err)
	}

	// Initialize a no-op strings.Replacer if nil
	if stringsReplacer == nil {
		stringsReplacer = strings.NewReplacer()
	}

	writer := csv.NewWriter(file, schema,
		csv.WithComma(delimiter),
		csv.WithHeader(includeHeader),
		csv.WithNullWriter(nullValue),
		csv.WithStringsReplacer(stringsReplacer),
		csv.WithBoolWriter(boolFormatter),
	)

	return &CSVRecordWriter{writer: writer}, nil
}

// Write writes a record to the CSV file.
func (w *CSVRecordWriter) Write(record arrow.Record) error {
	if err := w.writer.Write(record); err != nil {
		return fmt.Errorf("failed to write record to CSV: %w", err)
	}

	if err := w.writer.Error(); err != nil {
		return fmt.Errorf("CSV writer encountered an error: %w", err)
	}

	return nil
}

// Close flushes and closes the CSV writer.
func (w *CSVRecordWriter) Close() error {
	if w.writer != nil {
		w.writer.Flush()
	}
	return nil
}
