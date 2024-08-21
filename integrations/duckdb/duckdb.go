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

func OpenDuckDBConnection(ctx context.Context, dbURL string) (adbc.Connection, error) {
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

	return conn, nil
}

func ReadDuckDBStream(ctx context.Context, conn adbc.Connection, tableName string) (<-chan arrow.Record, <-chan error) {
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

		query := fmt.Sprintf("SELECT * FROM %s", tableName)
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

		if err := stmt.SetOption(adbc.OptionKeyIngestMode, adbc.OptionValueIngestModeAppend); err != nil {
			errChan <- fmt.Errorf("failed to set ingest mode: %w", err)
			return
		}
		if err := stmt.SetOption(adbc.OptionKeyIngestTargetTable, tableName); err != nil {
			errChan <- fmt.Errorf("failed to set ingest target table: %w", err)
			return
		}
		// Create the table if it doesn't exist
		if _, err := stmt.ExecuteUpdate(ctx); err != nil {
			errChan <- fmt.Errorf("failed to create table: %w", err)
			return
		}

		for record := range recordChan {
			if record.NumRows() == 0 {
				errChan <- fmt.Errorf("received record with no rows")
				continue
			}

			fmt.Printf("Writing record with schema: %v\n", record.Schema())

			// Create a temporary in-memory IPC stream from the record
			var buf bytes.Buffer
			writer := ipc.NewWriter(&buf, ipc.WithSchema(record.Schema()))
			if err := writer.Write(record); err != nil {
				errChan <- fmt.Errorf("failed to write record to IPC stream: %w", err)
				return
			}
			writer.Close()

			reader, err := ipc.NewReader(&buf)
			if err != nil {
				errChan <- fmt.Errorf("failed to create IPC reader: %w", err)
				return
			}
			defer reader.Release()

			if err := stmt.BindStream(ctx, reader); err != nil {
				errChan <- fmt.Errorf("failed to bind stream: %w", err)
				return
			}

			if _, err := stmt.ExecuteUpdate(ctx); err != nil {
				errChan <- fmt.Errorf("failed to execute update: %w", err)
				return
			}
		}
	}()

	return errChan
}

func CloseDuckDBConnection(conn adbc.Connection) error {
	return conn.Close()
}
