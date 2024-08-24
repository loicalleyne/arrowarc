package arrowarc

import (
	"context"
	"fmt"
	"sync"

	integrations "github.com/ArrowArc/ArrowArc/internal/integrations/bigquery"
	duckdb "github.com/ArrowArc/ArrowArc/internal/integrations/duckdb"
	filesystem "github.com/ArrowArc/ArrowArc/internal/integrations/filesystem"
)

func TransportParquetToDuckDB(ctx context.Context, parquetFilePath, dbFilePath, tableName string) error {
	conn, err := duckdb.OpenDuckDBConnection(ctx, dbFilePath, nil)
	if err != nil {
		return fmt.Errorf("failed to open DuckDB connection: %w", err)
	}
	defer duckdb.CloseDuckDBConnection(conn)

	recordChan, errChan := filesystem.ReadParquetFileStream(ctx, parquetFilePath, false, 1024, nil, nil, true)

	writeErrChan := duckdb.WriteDuckDBStream(ctx, conn, tableName, recordChan)

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
				writeErr = fmt.Errorf("error while writing to DuckDB: %w", err)
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

func TransportBigQueryToDuckDB(ctx context.Context, projectID string, datasetID string, bigqueryConnector integrations.BigQueryConnector, dbFilePath, tableName string) error {
	conn, err := duckdb.OpenDuckDBConnection(ctx, dbFilePath, nil)
	if err != nil {
		return fmt.Errorf("failed to open DuckDB connection: %w", err)
	}
	defer duckdb.CloseDuckDBConnection(conn)

	recordChan, errChan := integrations.ReadBigQueryStream(ctx, projectID, datasetID, tableName)

	writeErrChan := duckdb.WriteDuckDBStream(ctx, conn, tableName, recordChan)

	var wg sync.WaitGroup
	wg.Add(2)

	var readErr, writeErr error

	go func() {
		defer wg.Done()
		for err := range errChan {
			if err != nil {
				readErr = fmt.Errorf("error while reading BigQuery stream: %w", err)
				return
			}
		}
	}()

	go func() {
		defer wg.Done()
		for err := range writeErrChan {
			if err != nil {
				writeErr = fmt.Errorf("error while writing to DuckDB: %w", err)
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

func TransportDuckDBToParquet(ctx context.Context, dbFilePath, parquetFilePath, query string) error {
	conn, err := duckdb.OpenDuckDBConnection(ctx, dbFilePath, nil)
	if err != nil {
		return fmt.Errorf("failed to open DuckDB connection: %w", err)
	}
	defer duckdb.CloseDuckDBConnection(conn)

	recordChan, errChan := duckdb.ReadDuckDBStream(ctx, conn, query)

	writeErrChan := filesystem.WriteParquetFileStream(ctx, parquetFilePath, recordChan)

	var wg sync.WaitGroup
	wg.Add(2)

	var readErr, writeErr error

	go func() {
		defer wg.Done()
		for err := range errChan {
			if err != nil {
				readErr = fmt.Errorf("error while reading from DuckDB: %w", err)
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
