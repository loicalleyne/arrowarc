package parquet

import (
	"context"
	"fmt"
	"sync"

	filesystem "github.com/ArrowArc/ArrowArc/integrations/filesystem"
)

func RewriteParquetFile(ctx context.Context, inputFilePath, outputFilePath string, memoryMap bool, chunkSize int64, columns []string, rowGroups []int, parallel bool) error {
	recordChan, errChan := filesystem.ReadParquetFileStream(ctx, inputFilePath, memoryMap, chunkSize, columns, rowGroups, parallel)
	writeErrChan := filesystem.WriteParquetFileStream(ctx, outputFilePath, recordChan)

	var wg sync.WaitGroup
	wg.Add(2)

	var readErr, writeErr error

	go func() {
		defer wg.Done()
		for err := range errChan {
			if err != nil {
				readErr = fmt.Errorf("error while reading Parquet file: %w", err)
				return
			}
		}
	}()

	go func() {
		defer wg.Done()
		for err := range writeErrChan {
			if err != nil {
				writeErr = fmt.Errorf("error while writing Parquet file: %w", err)
				return
			}
		}
	}()

	wg.Wait()

	if readErr != nil {
		return readErr
	}
	if writeErr != nil {
		return writeErr
	}
	return nil
}
