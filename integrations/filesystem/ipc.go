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
	"github.com/apache/arrow/go/v17/arrow/arrio"
	"github.com/apache/arrow/go/v17/arrow/ipc"
	"github.com/apache/arrow/go/v17/arrow/memory"
	memoryPool "github.com/arrowarc/arrowarc/internal/memory"
)

// SchemaReader is an interface that extends arrio.Reader to include a Schema method.
type SchemaReader interface {
	arrio.Reader
	Schema() *arrow.Schema
}

// SchemaWriter is an interface that extends arrio.Writer to include a Schema method.
type SchemaWriter interface {
	arrio.Writer
	Schema() *arrow.Schema
}

// IPCRecordReader implements SchemaReader for reading records from IPC files.
type IPCRecordReader struct {
	reader *ipc.Reader
	file   *os.File
	alloc  memory.Allocator
}

// NewIPCRecordReader creates a new reader for reading records from an IPC file.
func NewIPCRecordReader(ctx context.Context, filePath string) (SchemaReader, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open IPC file: %w", err)
	}

	alloc := memoryPool.GetAllocator()

	opts := []ipc.Option{ipc.WithAllocator(alloc)}

	reader, err := ipc.NewReader(file, opts...)
	if err != nil {
		file.Close()
		memoryPool.PutAllocator(alloc)
		return nil, fmt.Errorf("failed to create IPC reader: %w", err)
	}

	return &IPCRecordReader{reader: reader, file: file, alloc: alloc}, nil
}

// Read reads the next record from the IPC file.
func (r *IPCRecordReader) Read() (arrow.Record, error) {
	if !r.reader.Next() {
		if err := r.reader.Err(); err != nil && err != io.EOF {
			return nil, fmt.Errorf("error reading IPC file: %w", err)
		}
		return nil, io.EOF
	}

	record := r.reader.Record()
	record.Retain()
	return record, nil
}

// Schema returns the schema of the records being read from the IPC file.
func (r *IPCRecordReader) Schema() *arrow.Schema {
	return r.reader.Schema()
}

// Close releases resources associated with the IPC reader.
func (r *IPCRecordReader) Close() error {
	defer memoryPool.PutAllocator(r.alloc)
	if r.reader != nil {
		r.reader.Release()
	}
	return r.file.Close()
}

// IPCRecordWriter implements SchemaWriter for writing records to IPC files.
type IPCRecordWriter struct {
	writer *ipc.Writer
	file   *os.File
	schema *arrow.Schema
	alloc  memory.Allocator
}

// NewIPCRecordWriter creates a new writer for writing records to an IPC file.
func NewIPCRecordWriter(ctx context.Context, filePath string, schema *arrow.Schema) (SchemaWriter, error) {
	file, err := os.Create(filePath)
	if err != nil {
		return nil, fmt.Errorf("could not create IPC file: %w", err)
	}

	alloc := memoryPool.GetAllocator()
	opts := []ipc.Option{ipc.WithSchema(schema), ipc.WithAllocator(alloc), ipc.WithCompressConcurrency(2), ipc.WithZstd()}

	writer := ipc.NewWriter(file, opts...)

	return &IPCRecordWriter{writer: writer, file: file, alloc: alloc, schema: schema}, nil
}

// Write writes a record to the IPC file.
func (w *IPCRecordWriter) Write(record arrow.Record) error {
	if err := w.writer.Write(record); err != nil {
		return fmt.Errorf("could not write record: %w", err)
	}
	return nil
}

// Close closes the IPC writer.
func (w *IPCRecordWriter) Close() error {
	defer memoryPool.PutAllocator(w.alloc)
	if w.writer != nil {
		if err := w.writer.Close(); err != nil {
			return fmt.Errorf("failed to close IPC writer: %w", err)
		}
	}
	return w.file.Close()
}

// Schema returns the schema of the records being written to the IPC file.
func (w *IPCRecordWriter) Schema() *arrow.Schema {
	return w.schema
}
