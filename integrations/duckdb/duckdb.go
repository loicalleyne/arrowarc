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
	"bytes"
	"context"
	"fmt"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-adbc/go/adbc/drivermgr"
	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/ipc"
)

type DuckDBExtension struct {
	Name          string
	LoadByDefault bool
}

func DefaultExtensions() []DuckDBExtension {
	return []DuckDBExtension{
		{Name: "arrow", LoadByDefault: true},
		{Name: "autocomplete", LoadByDefault: false},
		{Name: "aws", LoadByDefault: false},
		{Name: "azure", LoadByDefault: false},
		{Name: "delta", LoadByDefault: false},
		{Name: "excel", LoadByDefault: false},
		{Name: "fts", LoadByDefault: false},
		{Name: "httpfs", LoadByDefault: false},
		{Name: "iceberg", LoadByDefault: false},
		{Name: "icu", LoadByDefault: false},
		{Name: "inet", LoadByDefault: false},
		{Name: "jemalloc", LoadByDefault: false},
		{Name: "json", LoadByDefault: false},
		{Name: "motherduck", LoadByDefault: false},
		{Name: "mysql_scanner", LoadByDefault: false},
		{Name: "parquet", LoadByDefault: false},
		{Name: "postgres_scanner", LoadByDefault: false},
		{Name: "shell", LoadByDefault: false},
		{Name: "spatial", LoadByDefault: false},
		{Name: "sqlite_scanner", LoadByDefault: false},
		{Name: "substrait", LoadByDefault: false},
		{Name: "tpcds", LoadByDefault: false},
		{Name: "tpch", LoadByDefault: false},
		{Name: "vss", LoadByDefault: false},
	}
}

func OpenDuckDBConnection(ctx context.Context, dbURL string, additionalExtensions []DuckDBExtension) (adbc.Connection, error) {
	drv := drivermgr.Driver{}
	dbConfig := map[string]string{
		"driver":     "/usr/local/lib/libduckdb.dylib",
		"entrypoint": "duckdb_adbc_init",
		"path":       dbURL,
	}

	db, err := drv.NewDatabase(dbConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to open DuckDB database: %w", err)
	}

	conn, err := db.Open(ctx)
	if err != nil || conn == nil {
		return nil, fmt.Errorf("failed to open connection to DuckDB database: %w", err)
	}

	allExtensions := append(DefaultExtensions(), additionalExtensions...)

	for _, ext := range allExtensions {
		if ext.LoadByDefault {
			if err := installAndLoadExtension(conn, ext.Name); err != nil {
				return nil, fmt.Errorf("failed to install/load extension '%s': %w", ext.Name, err)
			}
		}
	}

	return conn, nil
}

func ReadDuckDBStream(ctx context.Context, conn adbc.Connection, query string) (<-chan arrow.Record, <-chan error) {
	recordChan := make(chan arrow.Record)
	errChan := make(chan error, 1)

	go func() {
		defer close(recordChan)
		defer close(errChan)

		stmt, err := conn.NewStatement()
		if err != nil {
			errChan <- fmt.Errorf("failed to create statement: %w", err)
			return
		}
		defer stmt.Close()

		if err := stmt.SetSqlQuery(query); err != nil {
			errChan <- fmt.Errorf("failed to set SQL query '%s': %w", query, err)
			return
		}

		reader, _, err := stmt.ExecuteQuery(ctx)
		if err != nil {
			errChan <- fmt.Errorf("failed to execute query: %w", err)
			return
		}
		defer reader.Release()

		for reader.Next() {
			record := reader.Record()
			record.Retain()

			select {
			case recordChan <- record:
			case <-ctx.Done():
				record.Release()
				return
			}
		}

		if err := reader.Err(); err != nil {
			errChan <- fmt.Errorf("error reading records: %w", err)
		}
	}()

	return recordChan, errChan
}

func WriteDuckDBStream(ctx context.Context, conn adbc.Connection, tableName string, recordChan <-chan arrow.Record) <-chan error {
	errChan := make(chan error, 1)

	go func() {
		defer close(errChan)

		stmt, err := conn.NewStatement()
		if err != nil {
			errChan <- fmt.Errorf("failed to create statement: %w", err)
			return
		}
		defer stmt.Close()

		if err := stmt.SetOption(adbc.OptionKeyIngestMode, adbc.OptionValueIngestModeCreate); err != nil {
			errChan <- fmt.Errorf("failed to set ingest mode: %w", err)
			return
		}

		if err := stmt.SetOption(adbc.OptionKeyIngestTargetTable, tableName); err != nil {
			errChan <- fmt.Errorf("failed to set ingest target table: %w", err)
			return
		}

		for {
			select {
			case <-ctx.Done():
				errChan <- ctx.Err()
				return // Exit the goroutine if context is canceled or times out
			case record, ok := <-recordChan:
				if !ok {
					// Channel closed, end of data
					return
				}

				// Ensure that the record has rows
				if record.NumRows() == 0 {
					errChan <- fmt.Errorf("received record with no rows")
					record.Release()
					continue
				}

				// Create a temporary in-memory IPC stream from the record
				var buf bytes.Buffer
				writer := ipc.NewWriter(&buf, ipc.WithSchema(record.Schema()))
				if err := writer.Write(record); err != nil {
					errChan <- fmt.Errorf("failed to write record to IPC stream: %w", err)
					record.Release()
					return
				}

				if err := writer.Close(); err != nil {
					errChan <- fmt.Errorf("failed to close IPC writer: %w", err)
					record.Release()
					return
				}

				reader, err := ipc.NewReader(&buf)
				if err != nil {
					errChan <- fmt.Errorf("failed to create IPC reader: %w", err)
					record.Release()
					return
				}

				// Bind the IPC stream to the DuckDB statement
				if err := stmt.BindStream(ctx, reader); err != nil {
					errChan <- fmt.Errorf("failed to bind stream: %w", err)
					reader.Release()
					record.Release()
					return
				}

				// Execute the statement to ingest the data
				if _, err := stmt.ExecuteUpdate(ctx); err != nil {
					errChan <- fmt.Errorf("failed to execute update: %w", err)
					reader.Release()
					record.Release()
					return
				}

				// Clean up resources
				reader.Release()
				record.Release()
			}
		}
	}()

	return errChan
}

func CloseDuckDBConnection(conn adbc.Connection) error {
	return conn.Close()
}

func installAndLoadExtension(conn adbc.Connection, extensionName string) error {
	if err := executeQuery(conn, fmt.Sprintf("INSTALL %s;", extensionName)); err != nil {
		return fmt.Errorf("failed to install extension '%s': %w", extensionName, err)
	}
	if err := executeQuery(conn, fmt.Sprintf("LOAD %s;", extensionName)); err != nil {
		return fmt.Errorf("failed to load extension '%s': %w", extensionName, err)
	}
	return nil
}

func executeQuery(conn adbc.Connection, sql string) error {
	stmt, err := conn.NewStatement()
	if err != nil {
		return err
	}
	defer stmt.Close()

	if err := stmt.SetSqlQuery(sql); err != nil {
		return err
	}

	_, _, err = stmt.ExecuteQuery(context.Background())
	return err
}
