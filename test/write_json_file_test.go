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

package test

import (
	"context"
	"io"
	"os"
	"testing"
	"time"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	integrations "github.com/arrowarc/arrowarc/integrations/filesystem"
	"github.com/arrowarc/arrowarc/pipeline"
	"github.com/stretchr/testify/assert"
)

// simpleRecordReader is a minimal implementation of arrio.Reader for testing purposes
type simpleRecordReader struct {
	records []arrow.Record
	index   int
}

func (r *simpleRecordReader) Read() (arrow.Record, error) {
	if r.index >= len(r.records) {
		return nil, io.EOF
	}
	record := r.records[r.index]
	r.index++
	return record, nil
}

func (r *simpleRecordReader) Close() error {
	// No resources to clean up in this simple implementation
	return nil
}

func TestWriteJSONFileStream(t *testing.T) {
	t.Parallel()

	// Define the schema for the records
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)

	// Allocate memory and build the record
	mem := memory.NewGoAllocator()
	b := array.NewRecordBuilder(mem, schema)
	defer b.Release()

	// Populate the record with data
	b.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2, 3}, nil)
	b.Field(1).(*array.StringBuilder).AppendValues([]string{"John", "Jane", "Doe"}, nil)

	// Create the record
	record := b.NewRecord()
	defer record.Release()

	// Create a simpleRecordReader that yields this record
	reader := &simpleRecordReader{records: []arrow.Record{record}}

	// Define the output file path
	outputFilePath := "output_test.json"
	defer os.Remove(outputFilePath)

	// Set up the context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Setup the JSON writer
	jsonWriter, err := integrations.NewJSONWriter(ctx, outputFilePath)
	assert.NoError(t, err, "Error should be nil when creating JSON writer")
	defer jsonWriter.Close()

	// Write the records to a JSON file using WriteJSONFileStream
	writer, err := integrations.NewJSONWriter(ctx, outputFilePath)
	assert.NoError(t, err, "Error should be nil when creating JSON writer")
	defer writer.Close()

	// Setup the pipeline to write the records to the JSON file
	metrics, err := pipeline.NewDataPipeline(reader, writer).Start(ctx)
	assert.NoError(t, err, "Error should be nil when running the pipeline")
	assert.NotNil(t, metrics)

	// Check if the output file exists
	assert.FileExists(t, outputFilePath, "Output file should exist")
	assert.NoError(t, err, "Error should be nil when closing the JSON file writer")

	// Print the metrics report
	t.Log(metrics.Report())

}
