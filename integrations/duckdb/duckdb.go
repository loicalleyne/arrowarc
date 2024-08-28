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
	"io"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-adbc/go/adbc/drivermgr"
	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/ipc"
	"github.com/apache/arrow/go/v17/arrow/memory"
	pool "github.com/arrowarc/arrowarc/internal/memory"
)

// DuckDBReader reads records from DuckDB and implements the Reader interface.
type DuckDBReader struct {
	db           adbc.Database
	ctx          context.Context
	recordReader array.RecordReader
	conn         adbc.Connection
	schema       *arrow.Schema
	alloc        memory.Allocator
}

// DuckDBReadOptions defines options for reading from DuckDB.
type DuckDBReadOptions struct {
	Extensions []DuckDBExtension
	Query      string
}

// DuckDBExtension represents a DuckDB extension with its name and load preference.
type DuckDBExtension struct {
	Name          string
	LoadByDefault bool
}

// DefaultExtensions returns the default extensions to be loaded in DuckDB.
func DefaultExtensions() []DuckDBExtension {
	return []DuckDBExtension{
		{Name: "arrow", LoadByDefault: false},
	}
}

// RunSQL runs a SQL query on DuckDB and returns the results as Arrow records.
func (r *DuckDBReader) RunSQL(sql string) ([]arrow.Record, error) {
	stmt, err := r.conn.NewStatement()
	if err != nil {
		return nil, fmt.Errorf("failed to create new statement: %w", err)
	}
	defer stmt.Close()

	if err := stmt.SetSqlQuery(sql); err != nil {
		return nil, fmt.Errorf("failed to set SQL query: %w", err)
	}

	out, _, err := stmt.ExecuteQuery(r.ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}
	defer out.Release()

	var result []arrow.Record
	for out.Next() {
		rec := out.Record()
		rec.Retain()
		result = append(result, rec)
	}
	if err := out.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

// NewDuckDBReader creates a new DuckDB reader.
func NewDuckDBReader(ctx context.Context, dbURL string, opts *DuckDBReadOptions) (*DuckDBReader, error) {
	alloc := pool.GetAllocator()

	runner, err := newDuckDBSQLRunner(ctx, dbURL, opts.Extensions)
	if err != nil {
		pool.PutAllocator(alloc)
		return nil, fmt.Errorf("failed to create DuckDB runner: %w", err)
	}

	records, err := runner.RunSQL(opts.Query)
	if err != nil {
		runner.Close()
		pool.PutAllocator(alloc)
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}

	if len(records) == 0 {
		runner.Close()
		pool.PutAllocator(alloc)
		return nil, fmt.Errorf("no records returned from query")
	}

	schema := records[0].Schema()
	reader, err := array.NewRecordReader(schema, records)
	if err != nil {
		runner.Close()
		pool.PutAllocator(alloc)
		return nil, fmt.Errorf("failed to create record reader: %w", err)
	}

	return &DuckDBReader{
		recordReader: reader,
		conn:         runner.conn,
		schema:       schema,
		alloc:        alloc,
	}, nil
}

// Read reads the next record from DuckDB.
func (d *DuckDBReader) Read() (arrow.Record, error) {
	if d.recordReader.Next() {
		record := d.recordReader.Record()
		record.Retain() // Retain the record to ensure it stays valid
		return record, nil
	}
	if err := d.recordReader.Err(); err != nil && err != io.EOF {
		return nil, err
	}
	return nil, io.EOF
}

// Close releases resources associated with the DuckDB reader.
func (d *DuckDBReader) Close() error {
	defer pool.PutAllocator(d.alloc)
	d.recordReader.Release()
	return d.conn.Close()
}

// Schema returns the schema of the records being read from DuckDB.
func (d *DuckDBReader) Schema() *arrow.Schema {
	return d.schema
}

// DuckDBWriter writes records to DuckDB and implements the Writer interface.
type DuckDBWriter struct {
	conn  adbc.Connection
	stmt  adbc.Statement
	table string
	alloc memory.Allocator
}

// NewDuckDBWriter creates a new DuckDB writer.
func NewDuckDBWriter(ctx context.Context, dbURL string, tableName string, extensions []DuckDBExtension) (*DuckDBWriter, error) {
	alloc := pool.GetAllocator()

	runner, err := newDuckDBSQLRunner(ctx, dbURL, extensions)
	if err != nil {
		pool.PutAllocator(alloc)
		return nil, fmt.Errorf("failed to create DuckDB runner: %w", err)
	}

	stmt, err := runner.conn.NewStatement()
	if err != nil {
		runner.Close()
		pool.PutAllocator(alloc)
		return nil, fmt.Errorf("failed to create statement: %w", err)
	}

	if err := stmt.SetOption(adbc.OptionKeyIngestMode, adbc.OptionValueIngestModeCreate); err != nil {
		stmt.Close()
		runner.Close()
		pool.PutAllocator(alloc)
		return nil, fmt.Errorf("failed to set ingest mode: %w", err)
	}
	if err := stmt.SetOption(adbc.OptionKeyIngestTargetTable, tableName); err != nil {
		stmt.Close()
		runner.Close()
		pool.PutAllocator(alloc)
		return nil, fmt.Errorf("failed to set target table: %w", err)
	}

	return &DuckDBWriter{
		conn:  runner.conn,
		stmt:  stmt,
		table: tableName,
		alloc: alloc,
	}, nil
}

// Write writes a record to DuckDB.
func (w *DuckDBWriter) Write(record arrow.Record) error {
	if record.NumRows() == 0 {
		return fmt.Errorf("received record with no rows")
	}

	buf := new(bytes.Buffer)
	writer := ipc.NewWriter(buf, ipc.WithSchema(record.Schema()), ipc.WithAllocator(w.alloc))
	if err := writer.Write(record); err != nil {
		return fmt.Errorf("failed to write record to IPC stream: %w", err)
	}
	if err := writer.Close(); err != nil {
		return fmt.Errorf("failed to close IPC writer: %w", err)
	}

	reader, err := ipc.NewReader(buf, ipc.WithAllocator(w.alloc))
	if err != nil {
		return fmt.Errorf("failed to create IPC reader: %w", err)
	}
	defer reader.Release()

	if err := w.stmt.BindStream(context.Background(), reader); err != nil {
		return fmt.Errorf("failed to bind stream: %w", err)
	}
	if _, err := w.stmt.ExecuteUpdate(context.Background()); err != nil {
		return fmt.Errorf("failed to execute update: %w", err)
	}

	return nil
}

// Close closes the DuckDB writer and releases resources.
func (w *DuckDBWriter) Close() error {
	defer pool.PutAllocator(w.alloc)
	if err := w.stmt.Close(); err != nil {
		return fmt.Errorf("failed to close statement: %w", err)
	}
	return w.conn.Close()
}

// Helper function to initialize a new DuckDB SQL runner.
func newDuckDBSQLRunner(ctx context.Context, dbURL string, additionalExtensions []DuckDBExtension) (*DuckDBReader, error) {
	var drv drivermgr.Driver
	dbConfig := map[string]string{
		"driver":     "/usr/local/lib/libduckdb.dylib",
		"entrypoint": "duckdb_adbc_init",
		"path":       dbURL,
	}
	db, err := drv.NewDatabase(dbConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create DuckDB database: %w", err)
	}

	conn, err := db.Open(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to open connection to DuckDB database: %w", err)
	}

	allExtensions := append(DefaultExtensions(), additionalExtensions...)
	for _, ext := range allExtensions {
		if ext.LoadByDefault {
			if err := installAndLoadExtension(conn, ext.Name); err != nil {
				conn.Close()
				return nil, fmt.Errorf("failed to install/load extension '%s': %w", ext.Name, err)
			}
		}
	}

	return &DuckDBReader{ctx: ctx, conn: conn, db: db}, nil
}

// installAndLoadExtension installs and loads the specified DuckDB extension.
func installAndLoadExtension(conn adbc.Connection, extensionName string) error {
	if err := executeQuery(conn, fmt.Sprintf("INSTALL %s;", extensionName)); err != nil {
		return fmt.Errorf("failed to install extension '%s': %w", extensionName, err)
	}
	if err := executeQuery(conn, fmt.Sprintf("LOAD %s;", extensionName)); err != nil {
		return fmt.Errorf("failed to load extension '%s': %w", extensionName, err)
	}
	return nil
}

// executeQuery executes a SQL query on the DuckDB connection.
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

// RunSQLOnRecord imports a record, executes a SQL query on it, and returns the results.
/*
func (r *DuckDBReader) RunSQLOnRecord(record arrow.Record, sql string, tableName string) ([]arrow.Record, error) {
	serializedRecord, err := serializeRecord(record)
	if err != nil {
		return nil, fmt.Errorf("failed to serialize record: %w", err)
	}

	if err := ImportRecord(serializedRecord, tableName); err != nil {
		return nil, fmt.Errorf("failed to import record: %w", err)
	}

	result, err := r.RunSQL(sql)
	if err != nil {
		return nil, fmt.Errorf("failed to run SQL: %w", err)
	}

	if _, err := r.RunSQL(fmt.Sprintf("DROP TABLE %s", tableName)); err != nil {
		return nil, fmt.Errorf("failed to drop temp table after running query: %w", err)
	}
	return result, nil
}
*/
