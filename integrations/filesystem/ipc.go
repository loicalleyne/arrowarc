package file

import (
	"context"
	"fmt"
	"io"
	"os"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/ipc"
	"github.com/apache/arrow/go/v17/arrow/memory"
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
