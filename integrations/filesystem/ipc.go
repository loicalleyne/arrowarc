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
	"github.com/apache/arrow/go/v17/arrow/ipc"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/arrowarc/arrowarc/internal/arrio"
	flatbuf "github.com/arrowarc/arrowarc/internal/flatbuf"
)

type IPCRecordReader struct {
	reader *ipc.Reader
}

func NewIPCRecordReader(ctx context.Context, filePath string) (arrio.Reader, error) {
	f, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open IPC file: %w", err)
	}

	reader, err := ipc.NewReader(f)
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("failed to create IPC reader: %w", err)
	}

	return &IPCRecordReader{reader: reader}, nil
}

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

func (r *IPCRecordReader) Close() error {
	if r.reader != nil {
		defer r.reader.Release()
	}
	return nil
}

type IPCRecordWriter struct {
	writer *ipc.Writer
}

func NewIPCRecordWriter(ctx context.Context, filePath string, schema *arrow.Schema) (arrio.Writer, error) {
	f, err := os.Create(filePath)
	if err != nil {
		return nil, fmt.Errorf("could not create IPC file: %w", err)
	}

	mem := memory.NewGoAllocator()
	writer := ipc.NewWriter(f, ipc.WithAllocator(mem), ipc.WithSchema(schema))

	return &IPCRecordWriter{writer: writer}, nil
}

func (w *IPCRecordWriter) Write(record arrow.Record) error {
	if err := w.writer.Write(record); err != nil {
		return fmt.Errorf("could not write record: %w", err)
	}
	return nil
}

func (w *IPCRecordWriter) Close() error {
	if w.writer != nil {
		return w.writer.Close()
	}
	return nil
}

func WriteStreamCompressed(f *os.File, mem memory.Allocator, schema *arrow.Schema, recs []arrow.Record, codec flatbuf.CompressionType, np int) error {
	// Set up IPC writer options with schema, allocator, and compression concurrency
	opts := []ipc.Option{
		ipc.WithSchema(schema),
		ipc.WithAllocator(mem),
		ipc.WithCompressConcurrency(np),
	}

	// Determine the compression codec to use
	switch codec {
	case flatbuf.CompressionTypeLZ4_FRAME:
		opts = append(opts, ipc.WithLZ4())
	case flatbuf.CompressionTypeZSTD:
		opts = append(opts, ipc.WithZstd())
	default:
		// Default to LZ4 compression if codec is not recognized
		opts = append(opts, ipc.WithLZ4())
	}

	// Create the IPC writer
	w := ipc.NewWriter(f, opts...)
	defer func() {
		if err := w.Close(); err != nil {
			fmt.Fprintf(os.Stderr, "error closing IPC writer: %v\n", err)
		}
	}()

	// Write each record to the IPC stream
	for _, rec := range recs {
		if err := w.Write(rec); err != nil {
			return fmt.Errorf("failed to write record: %w", err)
		}
	}

	// Explicitly close the writer and handle any errors
	if err := w.Close(); err != nil {
		return fmt.Errorf("failed to close IPC writer: %w", err)
	}

	return nil
}
