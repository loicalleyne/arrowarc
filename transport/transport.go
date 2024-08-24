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

package arrowarc

import (
	"context"
	"fmt"
	"os"
	"sync"

	github "github.com/arrowarc/arrowarc/internal/integrations/api/github"
	bq "github.com/arrowarc/arrowarc/internal/integrations/bigquery"
	duckdb "github.com/arrowarc/arrowarc/internal/integrations/duckdb"
	filesystem "github.com/arrowarc/arrowarc/internal/integrations/filesystem"
	postgres "github.com/arrowarc/arrowarc/internal/integrations/postgres"
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

func TransportBigQueryToDuckDB(ctx context.Context, projectID string, datasetID string, bigqueryConnector bq.BigQueryConnector, dbFilePath, tableName string) error {
	conn, err := duckdb.OpenDuckDBConnection(ctx, dbFilePath, nil)
	if err != nil {
		return fmt.Errorf("failed to open DuckDB connection: %w", err)
	}
	defer duckdb.CloseDuckDBConnection(conn)

	recordChan, errChan := bq.ReadBigQueryStream(ctx, projectID, datasetID, tableName)

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

func TransportPostgresToDuckDB(ctx context.Context, postgresSource *postgres.PostgresSource, dbFilePath, tableName string) error {
	conn, err := duckdb.OpenDuckDBConnection(ctx, dbFilePath, nil)
	if err != nil {
		return fmt.Errorf("failed to open DuckDB connection: %w", err)
	}
	defer duckdb.CloseDuckDBConnection(conn)

	psql, err := postgres.NewPostgresSource(ctx, "postgres://localhost:5432/postgres")
	if err != nil {
		return fmt.Errorf("failed to create Postgres source: %w", err)
	}
	defer psql.Close()

	recordChan, errChan := psql.GetPostgresStream(ctx, tableName)

	writeErrChan := duckdb.WriteDuckDBStream(ctx, conn, tableName, recordChan)

	var wg sync.WaitGroup
	wg.Add(2)

	var readErr, writeErr error

	go func() {
		defer wg.Done()
		for err := range errChan {
			if err != nil {
				readErr = fmt.Errorf("error while reading from Postgres: %w", err)
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

func TransportGitHubToDuckDB(ctx context.Context, repos []string, NewGitHubClient, dbFilePath, tableName string) error {
	conn, err := duckdb.OpenDuckDBConnection(ctx, dbFilePath, nil)
	if err != nil {
		return fmt.Errorf("failed to open DuckDB connection: %w", err)
	}
	defer duckdb.CloseDuckDBConnection(conn)

	githubToken := os.Getenv("GITHUB_TOKEN")
	if githubToken == "" {
		return fmt.Errorf("GITHUB_TOKEN environment variable is not set")
	}
	client := github.NewGitHubClient(ctx, githubToken)

	recordChan, errChan := github.ReadGitHubRepoAPIStream(ctx, repos, client)

	writeErrChan := duckdb.WriteDuckDBStream(ctx, conn, tableName, recordChan)

	var wg sync.WaitGroup
	wg.Add(2)

	var readErr, writeErr error

	go func() {
		defer wg.Done()
		for err := range errChan {
			if err != nil {
				readErr = fmt.Errorf("error while reading from GitHub: %w", err)
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

func TransportBigQueryToParquet(ctx context.Context, projectID, datasetID, tableName, parquetFilePath string) error {
	recordChan, errChan := bq.ReadBigQueryStream(ctx, projectID, datasetID, tableName)

	writeErrChan := filesystem.WriteParquetFileStream(ctx, parquetFilePath, recordChan)

	var wg sync.WaitGroup
	wg.Add(2)

	var readErr, writeErr error

	go func() {
		defer wg.Done()
		for err := range errChan {
			if err != nil {
				readErr = fmt.Errorf("error while reading from BigQuery: %w", err)
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
