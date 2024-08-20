package file

import (
	"context"
	"fmt"
	"io"
	"os"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
)

func ReadJSONFileStream(ctx context.Context, filePath string, schema *arrow.Schema, chunkSize int) (<-chan arrow.Record, <-chan error) {
	recordChan := make(chan arrow.Record)
	errChan := make(chan error, 1)

	go func() {
		defer close(recordChan)
		defer close(errChan)

		file, err := os.Open(filePath)
		if err != nil {
			errChan <- fmt.Errorf("failed to open JSON file: %w", err)
			return
		}
		defer file.Close()

		jsonReader := array.NewJSONReader(file, schema, array.WithChunk(chunkSize))
		defer jsonReader.Release()

		for {
			select {
			case <-ctx.Done():
				errChan <- ctx.Err()
				return
			default:
			}

			if !jsonReader.Next() {
				if err := jsonReader.Err(); err != nil && err != io.EOF {
					errChan <- fmt.Errorf("error reading JSON record: %w", err)
				}
				return
			}

			record := jsonReader.Record()

			if record == nil {
				continue
			}

			record.Retain()
			recordChan <- record
		}
	}()

	return recordChan, errChan
}
