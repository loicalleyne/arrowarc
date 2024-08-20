package file

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/csv"
)

func ReadCSVFileStream(ctx context.Context, filePath string, schema *arrow.Schema, hasHeader bool, chunkSize int64, delimiter rune, nullValues []string, stringsCanBeNull bool) (<-chan arrow.Record, <-chan error) {
	recordChan := make(chan arrow.Record)
	errChan := make(chan error, 1)

	go func() {
		defer close(recordChan)
		defer close(errChan)

		file, err := os.Open(filePath)
		if err != nil {
			errChan <- fmt.Errorf("failed to open CSV file: %w", err)
			return
		}
		defer file.Close()

		options := []csv.Option{
			csv.WithChunk(int(chunkSize)),
			csv.WithComma(delimiter),
			csv.WithHeader(hasHeader),
			csv.WithNullReader(stringsCanBeNull, nullValues...),
		}

		reader := csv.NewReader(file, schema, options...)
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
					errChan <- fmt.Errorf("error reading CSV record: %w", err)
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

func WriteCSVFileStream(ctx context.Context, filePath string, schema *arrow.Schema, recordChan <-chan arrow.Record, delimiter rune, includeHeader bool, nullValue string, stringsReplacer *strings.Replacer, boolFormatter func(bool) string) <-chan error {
	errChan := make(chan error, 1)

	go func() {
		defer close(errChan)

		file, err := os.Create(filePath)
		if err != nil {
			errChan <- fmt.Errorf("failed to create CSV file: %w", err)
			return
		}
		defer file.Close()

		writer := csv.NewWriter(file, schema,
			csv.WithComma(delimiter),
			csv.WithHeader(includeHeader),
			csv.WithNullWriter(nullValue),
			csv.WithStringsReplacer(stringsReplacer),
			csv.WithBoolWriter(boolFormatter),
		)
		defer writer.Flush()

		for {
			select {
			case <-ctx.Done():
				errChan <- ctx.Err()
				return
			case record, ok := <-recordChan:
				if !ok {
					return
				}

				if err := writer.Write(record); err != nil {
					errChan <- fmt.Errorf("failed to write record to CSV: %w", err)
					return
				}

				if err := writer.Error(); err != nil {
					errChan <- fmt.Errorf("CSV writer encountered an error: %w", err)
					return
				}
			}
		}
	}()

	return errChan
}
