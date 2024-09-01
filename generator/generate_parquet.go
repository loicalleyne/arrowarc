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

// Package generator provides utilities for generating test data in various formats.
package generator

import (
	"crypto/rand"
	"fmt"
	"log"
	"math/big"
	"os"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/apache/arrow/go/v17/parquet"
	"github.com/apache/arrow/go/v17/parquet/compress"
	"github.com/apache/arrow/go/v17/parquet/pqarrow"
	"github.com/go-faker/faker/v4"
)

// GenerateParquetFile generates a Parquet file with or without nested structures based on the complex flag.
func GenerateParquetFile(filePath string, targetSize int64, complex bool) error {
	mem := memory.NewGoAllocator()

	// Define the schema, optionally including nested fields based on the complex flag
	var schema *arrow.Schema
	if complex {
		userDetailsSchema := arrow.StructOf(
			arrow.Field{Name: "age", Type: arrow.PrimitiveTypes.Int32},
			arrow.Field{Name: "email", Type: arrow.BinaryTypes.String},
		)
		userSchema := arrow.StructOf(
			arrow.Field{Name: "id", Type: arrow.PrimitiveTypes.Int64},
			arrow.Field{Name: "name", Type: arrow.BinaryTypes.String},
			arrow.Field{Name: "details", Type: userDetailsSchema},
		)
		schema = arrow.NewSchema([]arrow.Field{
			{Name: "user", Type: userSchema},
		}, nil)
	} else {
		// Flat schema
		schema = arrow.NewSchema([]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64},
			{Name: "name", Type: arrow.BinaryTypes.String},
		}, nil)
	}

	// Create the Parquet file writer
	outputFile, err := os.Create(filePath)
	if err != nil {
		return fmt.Errorf("failed to create output file: %w", err)
	}
	defer outputFile.Close()

	writerProps := parquet.NewWriterProperties(
		parquet.WithAllocator(mem),
		parquet.WithCompression(compress.Codecs.Snappy), // Enable Snappy compression
		parquet.WithDataPageVersion(parquet.DataPageV2), // Use DataPageV2 for better compression ratio
	)
	parquetWriter, err := pqarrow.NewFileWriter(schema, outputFile, writerProps, pqarrow.DefaultWriterProps())
	if err != nil {
		return fmt.Errorf("failed to create Parquet writer: %w", err)
	}
	defer parquetWriter.Close()

	// Generate and write data until the target file size is reached
	currentSize := int64(0)
	recordCount := 0
	batchSize := 1000 // Number of rows per batch

	for currentSize < targetSize {
		// Generate a batch of dummy data
		rdm := secureRandInt(100)
		records := generateDummyData(mem, schema, batchSize, &rdm, complex)
		recordCount += batchSize

		// Write the batch to the Parquet file
		err := parquetWriter.Write(records)
		if err != nil {
			return fmt.Errorf("failed to write records to Parquet: %w", err)
		}
		records.Release()

		// Update the current file size
		info, err := outputFile.Stat()
		if err != nil {
			return fmt.Errorf("failed to get file stats: %w", err)
		}
		currentSize = info.Size()

		if currentSize >= targetSize {
			break
		}
	}

	log.Printf("Generated Parquet file with %d records, size: %.2f MB\n", recordCount, float64(currentSize)/(1<<20))
	return nil
}

func secureRandInt(max int64) int64 {
	n, err := rand.Int(rand.Reader, big.NewInt(max))
	if err != nil {
		log.Fatalf("failed to generate secure random number: %v", err)
	}
	return n.Int64()
}

// generateDummyData generates data based on the schema, optionally including nested structures.
func generateDummyData(mem memory.Allocator, schema *arrow.Schema, numRows int, rnd *int64, complex bool) arrow.Record {
	if complex {
		// Complex: with nested structures
		structBldr := array.NewStructBuilder(mem, schema.Fields()[0].Type.(*arrow.StructType))
		defer structBldr.Release()

		idBldr := structBldr.FieldBuilder(0).(*array.Int64Builder)
		nameBldr := structBldr.FieldBuilder(1).(*array.StringBuilder)
		detailsBldr := structBldr.FieldBuilder(2).(*array.StructBuilder)
		ageBldr := detailsBldr.FieldBuilder(0).(*array.Int32Builder)
		emailBldr := detailsBldr.FieldBuilder(1).(*array.StringBuilder)

		for i := 0; i < numRows; i++ {
			structBldr.Append(true)
			idBldr.Append(int64(i))
			nameBldr.Append(faker.Name())
			detailsBldr.Append(true)
			ageBldr.Append(int32(int(*rnd)))
			emailBldr.Append(faker.Email())
		}

		userArray := structBldr.NewArray().(*array.Struct)
		defer userArray.Release()

		return array.NewRecord(schema, []arrow.Array{userArray}, int64(userArray.Len()))
	} else {
		// Simple: flat structure
		idBldr := array.NewInt64Builder(mem)
		defer idBldr.Release()
		nameBldr := array.NewStringBuilder(mem)
		defer nameBldr.Release()

		for i := 0; i < numRows; i++ {
			idBldr.Append(int64(i))
			nameBldr.Append(faker.Name())
		}

		idArray := idBldr.NewArray()
		defer idArray.Release()
		nameArray := nameBldr.NewArray()
		defer nameArray.Release()

		return array.NewRecord(schema, []arrow.Array{idArray, nameArray}, int64(idArray.Len()))
	}
}
