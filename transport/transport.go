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
	bigquery "github.com/arrowarc/arrowarc/internal/integrations/bigquery"
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

	return processStreams(errChan, writeErrChan)
}

func TransportBigQueryToDuckDB(ctx context.Context, projectID, datasetID string, bigqueryConnector bigquery.BigQueryReadClient, dbFilePath, tableName string) error {
	conn, err := duckdb.OpenDuckDBConnection(ctx, dbFilePath, nil)
	if err != nil {
		return fmt.Errorf("failed to open DuckDB connection: %w", err)
	}
	defer duckdb.CloseDuckDBConnection(conn)

	recordChan, errChan := bigqueryConnector.ReadBigQueryStream(ctx, projectID, datasetID, tableName, "arrow")
	writeErrChan := duckdb.WriteDuckDBStream(ctx, conn, tableName, recordChan)

	return processStreams(errChan, writeErrChan)
}

func TransportDuckDBToParquet(ctx context.Context, dbFilePath, parquetFilePath, query string) error {
	conn, err := duckdb.OpenDuckDBConnection(ctx, dbFilePath, nil)
	if err != nil {
		return fmt.Errorf("failed to open DuckDB connection: %w", err)
	}
	defer duckdb.CloseDuckDBConnection(conn)

	recordChan, errChan := duckdb.ReadDuckDBStream(ctx, conn, query)
	writeErrChan := filesystem.WriteParquetFileStream(ctx, parquetFilePath, recordChan)

	return processStreams(errChan, writeErrChan)
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

	return processStreams(errChan, writeErrChan)
}

func TransportGitHubToDuckDB(ctx context.Context, repos []string, dbFilePath, tableName string) error {
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

	return processStreams(errChan, writeErrChan)
}

func TransportBigQueryToParquet(ctx context.Context, projectID, datasetID, tableName, parquetFilePath string) error {
	bq, err := bigquery.NewBigQueryReadClient(ctx)
	if err != nil {
		return fmt.Errorf("failed to create BigQuery connector: %w", err)
	}

	recordChan, errChan := bq.ReadBigQueryStream(ctx, projectID, datasetID, tableName, "arrow")
	writeErrChan := filesystem.WriteParquetFileStream(ctx, parquetFilePath, recordChan)

	return processStreams(errChan, writeErrChan)
}

// processStreams handles the reading and writing of records, capturing errors and ensuring all goroutines complete.
func processStreams(readErrChan <-chan error, writeErrChan <-chan error) error {
	var wg sync.WaitGroup
	var readErr, writeErr error

	wg.Add(2)

	go func() {
		defer wg.Done()
		for err := range readErrChan {
			if err != nil {
				readErr = fmt.Errorf("error while reading: %w", err)
				return
			}
		}
	}()

	go func() {
		defer wg.Done()
		for err := range writeErrChan {
			if err != nil {
				writeErr = fmt.Errorf("error while writing: %w", err)
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
