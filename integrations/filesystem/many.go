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

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	duckdb "github.com/arrowarc/arrowarc/integrations/duckdb"
	memoryPool "github.com/arrowarc/arrowarc/internal/memory"
)

// DuckDBRecordReader reads records from DuckDB queries and implements the SchemaReader interface.
type DuckDBRecordReader struct {
	reader array.RecordReader
	runner *duckdb.DuckDBReader
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
func NewDuckDBRecordReader(ctx context.Context, runner *duckdb.DuckDBReader, query string) (*DuckDBRecordReader, error) {
	alloc := memoryPool.GetAllocator()

	// Use the DuckDBSQLRunner's method to run the query and get the records
	records, err := runner.RunSQL(query)
	if err != nil {
		memoryPool.PutAllocator(alloc)
		return nil, fmt.Errorf("failed to run DuckDB query: %w", err)
	}

	schema := records[0].Schema()

	// Create a record reader from the Arrow records
	reader, err := array.NewRecordReader(schema, records)

	return &DuckDBRecordReader{
		reader: reader,
		runner: runner,
		query:  query,
		alloc:  alloc,
		schema: records[0].Schema(),
	}, err
}

// Read reads the next record from the DuckDB query result.
func (r *DuckDBRecordReader) Read() (arrow.Record, error) {
	if r.closed {
		return nil, io.EOF
	}

	return r.reader.Record(), nil
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
	r.reader.Release()
	return nil
}

// ReadFileStream reads data from a file using DuckDB based on the specified file type and returns a SchemaReader.
func ReadFileStream(ctx context.Context, opts *DuckDBReadOptions) (SchemaReader, error) {
	// Set default extensions if none are provided
	if len(opts.Extensions) == 0 {
		opts.Extensions = []duckdb.DuckDBExtension{
			{Name: "httpfs", LoadByDefault: false},
			{Name: opts.FileType, LoadByDefault: true},
		}
	}

	readOpts := duckdb.DuckDBReadOptions{
		Extensions: opts.Extensions,
		Query:      opts.FilePath,
	}

	runner, err := duckdb.NewDuckDBReader(ctx, ":memory:", &readOpts)
	if err != nil {
		return nil, fmt.Errorf("failed to create DuckDB runner: %w", err)
	}

	query, err := buildQuery(opts.FileType, opts.FilePath)
	if err != nil {
		runner.Close()
		return nil, fmt.Errorf("failed to build query: %w", err)
	}

	reader, err := NewDuckDBRecordReader(ctx, runner, query)
	if err != nil {
		runner.Close()
		return nil, fmt.Errorf("failed to create DuckDB record reader: %w", err)
	}

	return reader, nil
}
