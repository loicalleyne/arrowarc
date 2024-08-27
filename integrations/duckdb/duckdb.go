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
	memoryPool "github.com/arrowarc/arrowarc/internal/memory"
)

// DuckDBExtension represents a DuckDB extension with its name and load preference.
type DuckDBExtension struct {
	Name          string
	LoadByDefault bool
}

// DefaultExtensions returns the default extensions to be loaded in DuckDB.
func DefaultExtensions() []DuckDBExtension {
	return []DuckDBExtension{
		{Name: "arrow", LoadByDefault: true},
		// Add other extensions as needed
	}
}

// OpenDuckDBConnection opens a connection to a DuckDB database with the specified extensions.
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
	if err != nil {
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

// DuckDBRecordReader implements arrio.Reader for reading records from DuckDB.
type DuckDBRecordReader struct {
	ctx    context.Context
	conn   adbc.Connection
	query  string
	reader array.RecordReader
}

// NewDuckDBRecordReader creates a new reader for reading records from DuckDB.
func NewDuckDBRecordReader(ctx context.Context, conn adbc.Connection, query string) (*DuckDBRecordReader, error) {
	stmt, err := conn.NewStatement()
	if err != nil {
		return nil, fmt.Errorf("failed to create statement: %w", err)
	}

	if err := stmt.SetSqlQuery(query); err != nil {
		stmt.Close()
		return nil, fmt.Errorf("failed to set SQL query: %w", err)
	}

	reader, _, err := stmt.ExecuteQuery(ctx)
	if err != nil {
		stmt.Close()
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}

	return &DuckDBRecordReader{
		ctx:    ctx,
		conn:   conn,
		query:  query,
		reader: reader,
	}, nil
}

// Schema returns the schema of the records being read from DuckDB.
func (r *DuckDBRecordReader) Schema() *arrow.Schema {
	return r.reader.Schema()
}

// Read reads the next record from DuckDB.
func (r *DuckDBRecordReader) Read() (arrow.Record, error) {
	if !r.reader.Next() {
		if err := r.reader.Err(); err != nil && err != io.EOF {
			return nil, err
		}
		return nil, io.EOF
	}

	record := r.reader.Record()
	record.Retain()
	return record, nil
}

// Close releases resources associated with the DuckDB reader.
func (r *DuckDBRecordReader) Close() error {
	if r.reader != nil {
		r.reader.Release()
	}
	return nil
}

// DuckDBRecordWriter implements arrio.Writer for writing records to DuckDB.
type DuckDBRecordWriter struct {
	ctx       context.Context
	conn      adbc.Connection
	tableName string
	stmt      adbc.Statement
	alloc     memory.Allocator
}

// NewDuckDBRecordWriter creates a new writer for writing records to DuckDB.
func NewDuckDBRecordWriter(ctx context.Context, conn adbc.Connection, tableName string) (*DuckDBRecordWriter, error) {
	stmt, err := conn.NewStatement()
	if err != nil {
		return nil, fmt.Errorf("failed to create statement: %w", err)
	}

	if err := stmt.SetOption(adbc.OptionKeyIngestMode, adbc.OptionValueIngestModeCreate); err != nil {
		stmt.Close()
		return nil, fmt.Errorf("failed to set ingest mode: %w", err)
	}

	if err := stmt.SetOption(adbc.OptionKeyIngestTargetTable, tableName); err != nil {
		stmt.Close()
		return nil, fmt.Errorf("failed to set ingest target table: %w", err)
	}

	alloc := memoryPool.GetAllocator()

	return &DuckDBRecordWriter{
		ctx:       ctx,
		conn:      conn,
		tableName: tableName,
		stmt:      stmt,
		alloc:     alloc,
	}, nil
}

// Write writes a record to DuckDB.
func (w *DuckDBRecordWriter) Write(record arrow.Record) error {
	if record.NumRows() == 0 {
		return fmt.Errorf("received record with no rows")
	}

	var buf bytes.Buffer
	writer := ipc.NewWriter(&buf, ipc.WithSchema(record.Schema()), ipc.WithAllocator(w.alloc))
	if err := writer.Write(record); err != nil {
		return fmt.Errorf("failed to write record to IPC stream: %w", err)
	}

	if err := writer.Close(); err != nil {
		return fmt.Errorf("failed to close IPC writer: %w", err)
	}

	reader, err := ipc.NewReader(&buf, ipc.WithAllocator(w.alloc))
	if err != nil {
		return fmt.Errorf("failed to create IPC reader: %w", err)
	}
	defer reader.Release()

	if err := w.stmt.BindStream(w.ctx, reader); err != nil {
		return fmt.Errorf("failed to bind stream: %w", err)
	}

	if _, err := w.stmt.ExecuteUpdate(w.ctx); err != nil {
		return fmt.Errorf("failed to execute update: %w", err)
	}

	return nil
}

// Close closes the DuckDB writer.
func (w *DuckDBRecordWriter) Close() error {
	defer memoryPool.PutAllocator(w.alloc)
	return w.stmt.Close()
}

// Schema returns the schema of the records being written to DuckDB.
func (w *DuckDBRecordWriter) Schema() *arrow.Schema {
	schema, _ := w.stmt.GetParameterSchema()
	return schema
}

// CloseDuckDBConnection closes the DuckDB connection.
func CloseDuckDBConnection(conn adbc.Connection) error {
	return conn.Close()
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
