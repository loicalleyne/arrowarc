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
	"github.com/apache/arrow/go/v17/arrow/avro"
	"github.com/apache/arrow/go/v17/arrow/memory"
	pool "github.com/arrowarc/arrowarc/internal/memory"
)

// AvroReader reads records from Avro files and implements the Reader interface.
type AvroReader struct {
	reader *avro.OCFReader
	file   *os.File
	schema *arrow.Schema
	alloc  memory.Allocator
}

// AvroReadOptions defines options for reading Avro files.
type AvroReadOptions struct {
	ChunkSize int64
}

// NewAvroReader creates a new reader for reading records from an Avro file.
func NewAvroReader(ctx context.Context, filePath string, opts *AvroReadOptions) (*AvroReader, error) {
	alloc := pool.GetAllocator()

	file, err := os.Open(filePath)
	if err != nil {
		pool.PutAllocator(alloc)
		return nil, fmt.Errorf("failed to open Avro file: %w", err)
	}

	avroReader, err := avro.NewOCFReader(file, avro.WithAllocator(alloc), avro.WithChunk(int(opts.ChunkSize)))
	if err != nil {
		file.Close()
		pool.PutAllocator(alloc)
		return nil, fmt.Errorf("failed to create Avro OCF reader: %w", err)
	}

	return &AvroReader{
		reader: avroReader,
		file:   file,
		schema: avroReader.Schema(),
		alloc:  alloc,
	}, nil
}

// Read reads the next record from the Avro file.
func (r *AvroReader) Read() (arrow.Record, error) {
	if !r.reader.Next() {
		if err := r.reader.Err(); err != nil && err != io.EOF {
			return nil, fmt.Errorf("error reading Avro record: %w", err)
		}
		return nil, io.EOF
	}

	record := r.reader.Record()
	if record == nil {
		return nil, io.EOF
	}

	record.Retain()
	return record, nil
}

// Schema returns the schema of the records being read from the Avro file.
func (r *AvroReader) Schema() *arrow.Schema {
	return r.schema
}

// Close releases resources associated with the Avro reader.
func (r *AvroReader) Close() error {
	defer pool.PutAllocator(r.alloc)
	if r.reader != nil {
		r.reader.Release()
	}
	return r.file.Close()
}
