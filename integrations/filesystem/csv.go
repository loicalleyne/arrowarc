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

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/csv"
	"github.com/apache/arrow-go/v18/arrow/memory"
	pool "github.com/arrowarc/arrowarc/internal/memory"
)

// CSVReader reads records from a CSV file and implements the Reader interface.
type CSVReader struct {
	reader *csv.Reader
	file   *os.File
	alloc  memory.Allocator
	schema *arrow.Schema
}

// CSVWriter writes records to a CSV file and implements the Writer interface.
type CSVWriter struct {
	writer *csv.Writer
	file   *os.File
	alloc  memory.Allocator
}

// CSVReadOptions defines options for reading CSV files.
type CSVReadOptions struct {
	ChunkSize        int64
	Delimiter        rune
	HasHeader        bool
	NullValues       []string
	StringsCanBeNull bool
}

// CSVWriteOptions defines options for writing CSV files.
type CSVWriteOptions struct {
	Delimiter       rune
	IncludeHeader   bool
	NullValue       string
	StringsReplacer *strings.Replacer
	BoolFormatter   func(bool) string
}

// NewCSVReader creates a new CSV reader for reading records from a CSV file.
func NewCSVReader(ctx context.Context, filePath string, schema *arrow.Schema, opts *CSVReadOptions) (*CSVReader, error) {

	alloc := pool.GetAllocator()

	file, err := os.Open(filePath)
	if err != nil {
		pool.PutAllocator(alloc)
		return nil, fmt.Errorf("failed to open CSV file: %w", err)
	}

	options := []csv.Option{
		csv.WithChunk(int(opts.ChunkSize)),
		csv.WithComma(opts.Delimiter),
		csv.WithHeader(opts.HasHeader),
		csv.WithNullReader(opts.StringsCanBeNull, opts.NullValues...),
		csv.WithAllocator(alloc),
	}

	reader := csv.NewReader(file, schema, options...)

	return &CSVReader{
		reader: reader,
		file:   file,
		alloc:  alloc,
		schema: schema,
	}, nil
}

// Read reads the next record from the CSV file.
func (r *CSVReader) Read() (arrow.Record, error) {
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

// Schema returns the schema of the records being read from the CSV file.
func (r *CSVReader) Schema() *arrow.Schema {
	return r.schema
}

// Close releases resources associated with the CSV reader.
func (r *CSVReader) Close() error {
	defer pool.PutAllocator(r.alloc)
	if r.reader != nil {
		r.reader.Release()
	}
	return r.file.Close()
}

// NewCSVWriter creates a new CSV writer for writing records to a CSV file.
func NewCSVWriter(ctx context.Context, filePath string, schema *arrow.Schema, opts *CSVWriteOptions) (*CSVWriter, error) {
	alloc := pool.GetAllocator()

	file, err := os.Create(filePath)
	if err != nil {
		pool.PutAllocator(alloc)
		return nil, fmt.Errorf("failed to create CSV file: %w", err)
	}

	// Initialize a no-op strings.Replacer if nil
	if opts.StringsReplacer == nil {
		opts.StringsReplacer = strings.NewReplacer()
	}

	writer := csv.NewWriter(file, schema,
		csv.WithComma(opts.Delimiter),
		csv.WithHeader(opts.IncludeHeader),
		csv.WithNullWriter(opts.NullValue),
		csv.WithStringsReplacer(opts.StringsReplacer),
		csv.WithBoolWriter(opts.BoolFormatter),
	)

	return &CSVWriter{
		writer: writer,
		file:   file,
		alloc:  alloc,
	}, nil
}

// Write writes a record to the CSV file.
func (w *CSVWriter) Write(record arrow.Record) error {
	if err := w.writer.Write(record); err != nil {
		return fmt.Errorf("failed to write record to CSV: %w", err)
	}

	if err := w.writer.Error(); err != nil {
		return fmt.Errorf("CSV writer encountered an error: %w", err)
	}

	return nil
}

// Close flushes and closes the CSV writer.
func (w *CSVWriter) Close() error {
	defer pool.PutAllocator(w.alloc)
	if w.writer != nil {
		w.writer.Flush()
	}
	return w.file.Close()
}
