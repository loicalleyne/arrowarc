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
	"database/sql/driver"
	"fmt"
	"io"
	"reflect"
	"time"
	"unsafe"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/apache/arrow/go/v17/parquet/file"
	"github.com/apache/arrow/go/v17/parquet/pqarrow"
	pool "github.com/arrowarc/arrowarc/internal/memory"
)

// ParquetRows represents a result set that reads from a Parquet file using Apache Arrow.
type ParquetRows struct {
	recordReader          pqarrow.RecordReader // Arrow record reader
	fileReader            *file.Reader         // Parquet file reader
	schema                *arrow.Schema        // Arrow schema of the file
	curRecord             arrow.Record         // Current Arrow record batch
	curRowIndex           int                  // Current row index within the current batch
	bufferSize            int                  // Size of the batch buffer
	needNewBatch          bool                 // Indicates if a new batch is needed
	useUnsafeStringReader bool                 // Flag for unsafe string reading
	alloc                 memory.Allocator     // Arrow memory allocator
	columns               []string             // Column names
}

// NewParquetReader initializes a new ParquetRows reader with the provided options.
func NewParquetRowsReader(ctx context.Context, filePath string, opts *ParquetReadOptions) (*ParquetRows, error) {
	alloc := pool.GetAllocator()

	// Open the Parquet file
	rdr, err := file.OpenParquetFile(filePath, opts.MemoryMap)
	if err != nil {
		pool.PutAllocator(alloc)
		return nil, fmt.Errorf("failed to open Parquet file: %w", err)
	}
	defer func() {
		if err != nil {
			_ = rdr.Close()
		}
	}()

	// Create an Arrow-based file reader
	fileReader, err := pqarrow.NewFileReader(rdr, opts.toArrowReadProperties(), alloc)
	if err != nil {
		pool.PutAllocator(alloc)
		return nil, fmt.Errorf("failed to create Arrow file reader: %w", err)
	}

	// Retrieve the schema from the file
	schema, err := fileReader.Schema()
	if err != nil {
		pool.PutAllocator(alloc)
		_ = rdr.Close()
		return nil, fmt.Errorf("failed to get schema: %w", err)
	}

	// Initialize the record reader
	recordReader, err := fileReader.GetRecordReader(ctx, opts.ColumnIndices, opts.RowGroups)
	if err != nil {
		pool.PutAllocator(alloc)
		_ = rdr.Close()
		return nil, fmt.Errorf("failed to create record reader: %w", err)
	}

	// Prepare the column names
	var columns []string
	for _, field := range schema.Fields() {
		columns = append(columns, field.Name)
	}

	return &ParquetRows{
		recordReader: recordReader,
		fileReader:   rdr,
		schema:       schema,
		alloc:        alloc,
		columns:      columns,
		bufferSize:   int(opts.ChunkSize),
	}, nil
}

// Columns returns the column names of the Parquet file.
func (p *ParquetRows) Columns() []string {
	return p.columns
}

// Next reads the next record from the Parquet file and stores the values in the dest slice.
func (p *ParquetRows) Next(dest []driver.Value) error {
	if p.curRecord == nil || p.curRowIndex >= int(p.curRecord.NumRows()) {
		if err := p.readNextBatch(); err != nil {
			return err
		}
	}

	for i, col := range p.curRecord.Columns() {
		switch col := col.(type) {
		case *array.String:
			if col.IsNull(p.curRowIndex) {
				dest[i] = nil
			} else if p.useUnsafeStringReader {
				dest[i] = bytesToString([]byte(col.Value(p.curRowIndex)))
			} else {
				dest[i] = col.Value(p.curRowIndex)
			}
		case *array.Binary:
			if col.IsNull(p.curRowIndex) {
				dest[i] = nil
			} else {
				dest[i] = col.Value(p.curRowIndex)
			}
		case *array.Int8:
			if col.IsNull(p.curRowIndex) {
				dest[i] = nil
			} else {
				dest[i] = col.Value(p.curRowIndex)
			}
		case *array.Int16:
			if col.IsNull(p.curRowIndex) {
				dest[i] = nil
			} else {
				dest[i] = col.Value(p.curRowIndex)
			}
		case *array.Int32:
			if col.IsNull(p.curRowIndex) {
				dest[i] = nil
			} else {
				dest[i] = col.Value(p.curRowIndex)
			}
		case *array.Int64:
			if col.IsNull(p.curRowIndex) {
				dest[i] = nil
			} else {
				dest[i] = col.Value(p.curRowIndex)
			}
		case *array.Uint32:
			if col.IsNull(p.curRowIndex) {
				dest[i] = nil
			} else {
				dest[i] = col.Value(p.curRowIndex)
			}
		case *array.Uint64:
			if col.IsNull(p.curRowIndex) {
				dest[i] = nil
			} else {
				dest[i] = col.Value(p.curRowIndex)
			}
		case *array.Float32:
			if col.IsNull(p.curRowIndex) {
				dest[i] = nil
			} else {
				dest[i] = col.Value(p.curRowIndex)
			}
		case *array.Float64:
			if col.IsNull(p.curRowIndex) {
				dest[i] = nil
			} else {
				dest[i] = col.Value(p.curRowIndex)
			}
		case *array.Boolean:
			if col.IsNull(p.curRowIndex) {
				dest[i] = nil
			} else {
				dest[i] = col.Value(p.curRowIndex)
			}
		case *array.Timestamp:
			if col.IsNull(p.curRowIndex) {
				dest[i] = nil
			} else {
				dest[i] = time.Unix(0, int64(col.Value(p.curRowIndex))).UTC()
			}
		case *array.Date32:
			if col.IsNull(p.curRowIndex) {
				dest[i] = nil
			} else {
				dest[i] = time.Unix(int64(col.Value(p.curRowIndex)), 0).UTC()
			}
		case *array.Date64:
			if col.IsNull(p.curRowIndex) {
				dest[i] = nil
			} else {
				dest[i] = time.Unix(int64(col.Value(p.curRowIndex))/(24*3600*1000), 0).UTC()
			}
		case *array.Time32:
			if col.IsNull(p.curRowIndex) {
				dest[i] = nil
			} else {
				dest[i] = time.Unix(int64(col.Value(p.curRowIndex)), 0).UTC()
			}
		default:
			return fmt.Errorf("unsupported column type: %s", col.DataType().ID().String())
		}
	}

	p.curRowIndex++
	return nil
}

// readNextBatch reads the next batch of records.
func (p *ParquetRows) readNextBatch() error {
	if p.recordReader.Next() {
		p.curRecord = p.recordReader.Record()
		p.curRowIndex = 0
		p.curRecord.Retain() // Ensure the record stays valid
		return nil
	}
	if err := p.recordReader.Err(); err != nil && err != io.EOF {
		return err
	}
	return io.EOF
}

// Close releases all resources associated with the reader.
func (p *ParquetRows) Close() error {
	defer pool.PutAllocator(p.alloc)
	if p.curRecord != nil {
		p.curRecord.Release()
	}
	p.recordReader.Release()
	return p.fileReader.Close()
}

// ColumnTypeDatabaseTypeName returns the database type name of the column at the specified index.
func (p *ParquetRows) ColumnTypeDatabaseTypeName(index int) string {
	return p.schema.Field(index).Type.String()
}

// ColumnTypeNullable returns whether the column at the specified index is nullable.
func (p *ParquetRows) ColumnTypeNullable(index int) (nullable, ok bool) {
	return p.schema.Field(index).Nullable, true
}

// ColumnTypePrecisionScale returns the precision and scale for the column at the specified index.
func (p *ParquetRows) ColumnTypePrecisionScale(index int) (precision, scale int64, ok bool) {
	return 0, 0, false
}

// ColumnTypeScanType returns the Go type for scanning the column at the specified index.
func (p *ParquetRows) ColumnTypeScanType(index int) reflect.Type {
	switch p.schema.Field(index).Type.ID() {
	case arrow.BOOL:
		return reflect.TypeOf(false)
	case arrow.INT32:
		return reflect.TypeOf(int32(0))
	case arrow.INT64:
		return reflect.TypeOf(int64(0))
	case arrow.FLOAT32:
		return reflect.TypeOf(float32(0))
	case arrow.FLOAT64:
		return reflect.TypeOf(float64(0))
	case arrow.STRING:
		return reflect.TypeOf("")
	}
	return nil
}

// Helper function for unsafe byte-to-string conversion.
func bytesToString(data []byte) string {
	return *(*string)(unsafe.Pointer(&data))
}
