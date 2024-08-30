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
	"os"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/apache/arrow/go/v17/parquet"
	"github.com/apache/arrow/go/v17/parquet/compress"
	"github.com/apache/arrow/go/v17/parquet/file"
	"github.com/apache/arrow/go/v17/parquet/pqarrow"
	pool "github.com/arrowarc/arrowarc/internal/memory"
)

// ParquetReader reads Parquet files and implements the Reader interface.
type ParquetReader struct {
	recordReader pqarrow.RecordReader
	fileReader   *file.Reader
	schema       *arrow.Schema
	alloc        memory.Allocator
}

// ReadOptions defines options for reading Parquet files.
type ParquetReadOptions struct {
	MemoryMap     bool
	ColumnIndices []int
	RowGroups     []int
	Parallel      bool
	ChunkSize     int64
}

func (o *ParquetReadOptions) toArrowReadProperties() pqarrow.ArrowReadProperties {
	return pqarrow.ArrowReadProperties{
		Parallel:  true,
		BatchSize: 64 * 1024 * 1024, // 64MB batch size
	}
}

// NewDefaultParquetWriteOptions returns default write options for Parquet files.
func NewDefaultParquetWriteOptions() pqarrow.ArrowWriterProperties {
	return pqarrow.NewArrowWriterProperties(
		pqarrow.WithStoreSchema(),
	)
}

// NewDefaultParquetWriterProperties returns default writer properties.
func NewDefaultParquetWriterProperties() *parquet.WriterProperties {
	return parquet.NewWriterProperties(
		parquet.WithCompression(compress.Codecs.Snappy),
		parquet.WithBatchSize(64*1024*1024), // 64MB batch size
		parquet.WithAllocator(pool.GetAllocator()),
		parquet.WithVersion(parquet.V2_LATEST),
		parquet.WithDataPageSize(1024*1024),
		parquet.WithMaxRowGroupLength(64*1024*1024), // 64MB row group length
		parquet.WithCreatedBy("ArrowArc"),
	)
}

// NewParquetReader creates a new Parquet file reader.
func NewParquetReader(ctx context.Context, filePath string, opts *ParquetReadOptions) (*ParquetReader, error) {
	alloc := pool.GetAllocator()

	rdr, err := file.OpenParquetFile(filePath, opts.MemoryMap)
	if err != nil {
		pool.PutAllocator(alloc)
		return nil, fmt.Errorf("failed to open Parquet file: %w", err)
	}

	fileReader, err := pqarrow.NewFileReader(rdr, opts.toArrowReadProperties(), alloc)
	if err != nil {
		pool.PutAllocator(alloc)
		rdr.Close()
		return nil, fmt.Errorf("failed to create Arrow file reader: %w", err)
	}

	schema, err := fileReader.Schema()
	if err != nil {
		pool.PutAllocator(alloc)
		rdr.Close()
		return nil, fmt.Errorf("failed to get schema: %w", err)
	}

	recordReader, err := fileReader.GetRecordReader(ctx, opts.ColumnIndices, opts.RowGroups)
	if err != nil {
		pool.PutAllocator(alloc)
		rdr.Close()
		return nil, fmt.Errorf("failed to create record reader: %w", err)
	}

	return &ParquetReader{
		recordReader: recordReader,
		fileReader:   rdr,
		schema:       schema,
		alloc:        alloc,
	}, nil
}

func (p *ParquetReader) Read() (arrow.Record, error) {
	if p.recordReader.Next() {
		record := p.recordReader.Record()
		record.Retain() // Retain the record to ensure it stays valid
		return record, nil
	}
	if err := p.recordReader.Err(); err != nil && err != io.EOF {
		return nil, err
	}
	return nil, io.EOF
}

func (p *ParquetReader) Close() error {
	defer pool.PutAllocator(p.alloc)
	p.recordReader.Release()
	return p.fileReader.Close()
}

func (p *ParquetReader) Schema() *arrow.Schema {
	return p.schema
}

// ParquetWriter writes records to Parquet files.
type ParquetWriter struct {
	writer *pqarrow.FileWriter
	file   *os.File
	alloc  memory.Allocator
}

// NewParquetWriter creates a new Parquet file writer.
func NewParquetWriter(
	filePath string, schema *arrow.Schema,
	parquetWriterProps *parquet.WriterProperties,
) (*ParquetWriter, error) {

	alloc := pool.GetAllocator()

	file, err := os.Create(filePath)
	if err != nil {
		pool.PutAllocator(alloc)
		return nil, fmt.Errorf("failed to create file: %w", err)
	}

	writer, err := pqarrow.NewFileWriter(schema, file, parquetWriterProps, pqarrow.NewArrowWriterProperties())
	if err != nil {
		file.Close()
		pool.PutAllocator(alloc)
		return nil, fmt.Errorf("failed to create Parquet writer: %w", err)
	}

	return &ParquetWriter{
		writer: writer,
		file:   file,
		alloc:  alloc,
	}, nil
}

func (p *ParquetWriter) Write(record arrow.Record) error {
	if err := p.writer.Write(record); err != nil {
		return fmt.Errorf("failed to write record: %w", err)
	}
	return nil
}

func (p *ParquetWriter) Close() error {
	defer pool.PutAllocator(p.alloc)
	if err := p.writer.Close(); err != nil {
		return fmt.Errorf("failed to close Parquet writer: %w", err)
	}
	return p.file.Close()
}
