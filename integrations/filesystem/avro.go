package file

import (
	"context"
	"fmt"
	"io"
	"os"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/avro"
	"github.com/apache/arrow/go/v17/arrow/memory"
)

func ReadAvroFileStream(ctx context.Context, filePath string, chunkSize int64) (<-chan arrow.Record, <-chan error) {
	recordChan := make(chan arrow.Record)
	errChan := make(chan error, 1)

	go func() {
		defer close(recordChan)
		defer close(errChan)

		file, err := os.Open(filePath)
		if err != nil {
			errChan <- fmt.Errorf("failed to open Avro file: %w", err)
			return
		}
		defer file.Close()

		reader, err := avro.NewOCFReader(file, avro.WithAllocator(memory.DefaultAllocator), avro.WithChunk(int(chunkSize)))
		if err != nil {
			errChan <- fmt.Errorf("failed to create Avro OCF reader: %w", err)
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
					errChan <- fmt.Errorf("error reading Avro record: %w", err)
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
