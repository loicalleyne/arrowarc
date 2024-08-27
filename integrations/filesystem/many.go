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
	"context"
	"fmt"
	"io"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/arrio"
	"github.com/apache/arrow/go/v17/arrow/memory"
	duckdb "github.com/arrowarc/arrowarc/integrations/duckdb"
	memoryPool "github.com/arrowarc/arrowarc/internal/memory"
)

// DuckDBRecordReader reads records from DuckDB queries and implements the SchemaReader interface.
type DuckDBRecordReader struct {
	reader arrio.Reader
	conn   *adbc.Connection
	query  string
	schema *arrow.Schema
	alloc  memory.Allocator
	closed bool
}

// ReadOptions defines options for reading various file types using DuckDB.
type DuckDBReadOptions struct {
	FileType   string
	FilePath   string
	Extensions []duckdb.DuckDBExtension
}

// NewDuckDBRecordReader creates a new reader for reading records from a DuckDB query.
func NewDuckDBRecordReader(ctx context.Context, conn *adbc.Connection, query string) (*DuckDBRecordReader, error) {
	alloc := memoryPool.GetAllocator()

	reader, err := duckdb.NewDuckDBRecordReader(ctx, *conn, query)
	if err != nil {
		memoryPool.PutAllocator(alloc)
		return nil, fmt.Errorf("failed to create DuckDB record reader: %w", err)
	}

	return &DuckDBRecordReader{
		reader: reader,
		conn:   conn,
		query:  query,
		alloc:  alloc,
	}, nil
}

// Read reads the next record from the DuckDB query result.
func (r *DuckDBRecordReader) Read() (arrow.Record, error) {
	if r.closed {
		return nil, fmt.Errorf("reader is closed")
	}

	record, err := r.reader.Read()
	if err != nil && err != io.EOF {
		return nil, fmt.Errorf("error reading DuckDB record: %w", err)
	}
	if record == nil {
		return nil, io.EOF
	}

	record.Retain()
	return record, nil
}

// Schema returns the schema of the records being read from the DuckDB query.
func (r *DuckDBRecordReader) Schema() *arrow.Schema {
	return r.schema
}

// Close releases resources associated with the DuckDB reader.
func (r *DuckDBRecordReader) Close() error {
	if r.closed {
		return nil
	}

	defer memoryPool.PutAllocator(r.alloc)

	r.closed = true
	if err := duckdb.CloseDuckDBConnection(*r.conn); err != nil {
		return fmt.Errorf("failed to close DuckDB connection: %w", err)
	}
	return nil
}

// ReadFileStream reads data from a file using DuckDB based on the specified file type and returns a SchemaReader.
func ReadFileStream(ctx context.Context, opts *DuckDBReadOptions) (SchemaReader, error) {
	// Set default extensions if none are provided
	if len(opts.Extensions) == 0 {
		opts.Extensions = []duckdb.DuckDBExtension{
			{Name: "httpfs", LoadByDefault: true},
			{Name: opts.FileType, LoadByDefault: true},
		}
	}

	conn, err := duckdb.OpenDuckDBConnection(ctx, "", opts.Extensions)
	if err != nil {
		return nil, fmt.Errorf("failed to open DuckDB connection: %w", err)
	}

	query, err := buildQuery(opts.FileType, opts.FilePath)
	if err != nil {
		duckdb.CloseDuckDBConnection(conn)
		return nil, fmt.Errorf("failed to build query: %w", err)
	}

	reader, err := NewDuckDBRecordReader(ctx, &conn, query)
	if err != nil {
		duckdb.CloseDuckDBConnection(conn)
		return nil, fmt.Errorf("failed to create DuckDB record reader: %w", err)
	}

	return reader, nil
}

// buildQuery constructs a query string based on the file type and path.
func buildQuery(fileType, filePath string) (string, error) {
	switch fileType {
	case "iceberg":
		return fmt.Sprintf("SELECT * FROM iceberg_scan('%s')", filePath), nil
	case "parquet":
		return fmt.Sprintf("SELECT * FROM parquet_scan('%s')", filePath), nil
	case "csv":
		return fmt.Sprintf("SELECT * FROM read_csv_auto('%s')", filePath), nil
	case "json":
		return fmt.Sprintf("SELECT * FROM read_json_auto('%s')", filePath), nil
	default:
		return "", fmt.Errorf("unsupported file type: %s", fileType)
	}
}
