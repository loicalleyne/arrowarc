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
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/ipc"
	"github.com/apache/arrow/go/v17/arrow/memory"
	flatbuf "github.com/arrowarc/arrowarc/internal/flatbuf"
)

func ReadIPCFileStream(ctx context.Context, filePath string) (<-chan arrow.Record, <-chan error) {
	recordChan := make(chan arrow.Record)
	errChan := make(chan error, 1)

	go func() {
		defer close(recordChan)
		defer close(errChan)

		// Open the IPC file
		f, err := os.Open(filePath)
		if err != nil {
			errChan <- fmt.Errorf("failed to open IPC file: %w", err)
			return
		}
		defer f.Close()

		reader, err := ipc.NewReader(f)
		if err != nil {
			errChan <- fmt.Errorf("failed to create IPC reader: %w", err)
			return
		}
		defer reader.Release()

		for {
			select {
			case <-ctx.Done():
				errChan <- ctx.Err()
				return
			default:
			}

			if !reader.Next() {
				if err := reader.Err(); err != nil && err != io.EOF {
					errChan <- fmt.Errorf("error reading IPC file: %w", err)
				}
				return
			}

			record := reader.Record()
			if record == nil {
				continue
			}

			record.Retain()
			recordChan <- record
		}
	}()

	return recordChan, errChan
}

func WriteIPCFileStream(ctx context.Context, filePath string, schema *arrow.Schema, records <-chan arrow.Record) <-chan error {
	errChan := make(chan error, 1)

	go func() {
		defer close(errChan)

		// Create the IPC file
		f, err := os.Create(filePath)
		if err != nil {
			errChan <- fmt.Errorf("could not create file: %w", err)
			return
		}
		defer f.Close()

		mem := memory.NewGoAllocator()
		ww := ipc.NewWriter(f, ipc.WithAllocator(mem), ipc.WithSchema(schema))
		defer func() {
			if closeErr := ww.Close(); closeErr != nil && err == nil {
				errChan <- fmt.Errorf("could not close writer: %w", closeErr)
			}
		}()

		for {
			select {
			case <-ctx.Done():
				errChan <- ctx.Err()
				return
			case record, ok := <-records:
				if !ok {
					return // Channel closed, stop processing
				}

				if err := ww.Write(record); err != nil {
					errChan <- fmt.Errorf("could not write record: %w", err)
					return
				}

				record.Release()
			}
		}
	}()

	return errChan
}

func WriteStreamCompressed(t *testing.T, f *os.File, mem memory.Allocator, schema *arrow.Schema, recs []arrow.Record, codec flatbuf.CompressionType, np int) {

	opts := []ipc.Option{ipc.WithSchema(schema), ipc.WithAllocator(mem), ipc.WithCompressConcurrency(np)}
	switch codec {
	case flatbuf.CompressionTypeLZ4_FRAME:
		opts = append(opts, ipc.WithLZ4())
	case flatbuf.CompressionTypeZSTD:
		opts = append(opts, ipc.WithZstd())
	default:
		t.Fatalf("invalid compression codec %v, only LZ4_FRAME or ZSTD is allowed", codec)
	}

	w := ipc.NewWriter(f, opts...)
	defer w.Close()

	for i, rec := range recs {
		err := w.Write(rec)
		if err != nil {
			t.Fatalf("could not write record[%d]: %v", i, err)
		}
	}

	err := w.Close()
	if err != nil {
		t.Fatal(err)
	}
}
