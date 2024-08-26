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
	"os"
	"path/filepath"
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/arrio"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/arrowarc/arrowarc/generator"
	filesystem "github.com/arrowarc/arrowarc/integrations/filesystem"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGenerateAndCopyIPCFiles(t *testing.T) {
	ctx := context.Background()

	// Define a directory for generated IPC files
	dir := t.TempDir()
	recordSets := map[string][]arrow.Record{
		"example_data": createExampleRecords(t),
	}

	// Generate IPC files
	err := generator.GenerateIPCFiles(ctx, dir, recordSets)
	require.NoError(t, err, "Error generating IPC files")

	// Source: IPC file reader
	inputFilePath := filepath.Join(dir, "example_data.arrow")
	src, err := filesystem.NewIPCRecordReader(ctx, inputFilePath)
	require.NoError(t, err, "Error creating IPC reader")

	// Destination: IPC file writer
	outputFilePath := filepath.Join(dir, "output.ipc")
	dst, err := filesystem.NewIPCRecordWriter(ctx, outputFilePath, src.Schema())
	require.NoError(t, err, "Error creating IPC writer")

	// Use arrio.Copy to transport records
	_, err = arrio.Copy(dst, src)
	require.NoError(t, err, "Error during record copy")

	// Check if output file exists
	_, err = os.Stat(outputFilePath)
	assert.NoError(t, err, "Output file does not exist")

	t.Log("Records copied successfully from", inputFilePath, "to", outputFilePath)
}

func createExampleRecords(t *testing.T) []arrow.Record {
	mem := memory.NewGoAllocator()

	// Define the schema for the records
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "age", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
	}, nil)

	// Create record builders
	b := array.NewRecordBuilder(mem, schema)
	defer b.Release()

	// Populate the record with data
	b.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2, 3, 4, 5}, nil)
	b.Field(1).(*array.StringBuilder).AppendValues([]string{"John", "Jane", "Doe", "Alice", "Bob"}, nil)
	b.Field(2).(*array.Int32Builder).AppendValues([]int32{34, 28, 45, 30, 50}, nil)

	// Create a single record
	record := b.NewRecord()
	defer record.Release()

	// Return a slice containing the single record
	return []arrow.Record{record}
}
