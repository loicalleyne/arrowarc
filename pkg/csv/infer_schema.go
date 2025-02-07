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

// Package csv provides utilities for inferring the Arrow schema from a CSV file.
package csv

import (
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
)

type CSVReadOptions struct {
	Delimiter        rune
	HasHeader        bool
	StringsCanBeNull bool
	NullValues       []string
	ParseTimestamps  bool
	TimestampFormat  string
}

type inferenceError struct {
	Row    int
	Column int
	Err    error
}

func (e *inferenceError) Error() string {
	return fmt.Sprintf("error at row %d, column %d: %v", e.Row, e.Column, e.Err)
}

const (
	maxRowsToInfer = 1000
	batchSize      = 100
)

// InferCSVArrowSchema infers the Arrow schema from a CSV file
func InferCSVArrowSchema(ctx context.Context, filePath string, opts *CSVReadOptions) (*arrow.Schema, error) {
	if err := validateOptions(opts); err != nil {
		return nil, err
	}

	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open CSV file: %w", err)
	}

	done := make(chan struct{})
	defer func() {
		close(done)
		file.Close()
	}()

	go func() {
		select {
		case <-ctx.Done():
			file.Close()
		case <-done:
		}
	}()

	reader := csv.NewReader(file)
	reader.Comma = opts.Delimiter
	reader.TrimLeadingSpace = true

	headers, err := readHeaders(reader, opts)
	if err != nil {
		return nil, err
	}

	columnTypes := make([]arrow.DataType, len(headers))
	columnNullability := make([]bool, len(headers))

	rowChannel := make(chan []string, batchSize)
	errChan := make(chan *inferenceError, runtime.NumCPU())

	var wg sync.WaitGroup
	numWorkers := runtime.NumCPU()

	// Start workers
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go processRows(rowChannel, columnTypes, columnNullability, opts, &wg, errChan)
	}

	// Read and process rows
	rowCount := 0
	for rowCount < maxRowsToInfer {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
			row, err := reader.Read()
			if err == io.EOF {
				break
			}
			if err != nil {
				return nil, fmt.Errorf("error reading CSV row: %w", err)
			}
			rowChannel <- row
			rowCount++
		}
	}

	close(rowChannel)
	wg.Wait()

	// Check for errors
	select {
	case err := <-errChan:
		return nil, err
	default:
	}

	return buildSchema(headers, columnTypes, columnNullability, opts), nil
}

// inferColumnType detects the type of a given column based on the observed value
func inferColumnType(current arrow.DataType, value string, opts *CSVReadOptions) arrow.DataType {
	// Early exit if already string type or empty value
	if current == arrow.BinaryTypes.String || isNullValue(value, opts.NullValues) {
		return current
	}

	// Try parsing in order of specificity
	if current == nil || current == arrow.PrimitiveTypes.Int64 {
		if _, err := strconv.ParseInt(value, 10, 64); err == nil {
			return arrow.PrimitiveTypes.Int64
		}
	}

	if current == nil || current == arrow.PrimitiveTypes.Float64 || current == arrow.PrimitiveTypes.Int64 {
		if _, err := strconv.ParseFloat(value, 64); err == nil {
			return arrow.PrimitiveTypes.Float64
		}
	}

	if current == nil || current == arrow.FixedWidthTypes.Boolean {
		lower := strings.ToLower(value)
		if lower == "true" || lower == "false" || lower == "1" || lower == "0" {
			return arrow.FixedWidthTypes.Boolean
		}
	}

	// Try parsing as timestamp if configured
	if opts.ParseTimestamps {
		if _, err := time.Parse(opts.TimestampFormat, value); err == nil {
			return arrow.FixedWidthTypes.Timestamp_us
		}
	}

	// Default to string
	return arrow.BinaryTypes.String
}

// Helper functions for type detection

func parseDate(value string) (bool, error) {
	// Implement a simple date parsing logic
	if len(value) == 10 && strings.Count(value, "-") == 2 {
		return true, nil
	}
	return false, fmt.Errorf("not a date")
}

func parseTimestamp(value string) (bool, error) {
	// Implement a simple timestamp parsing logic
	if strings.Contains(value, "T") {
		parts := strings.Split(value, "T")
		if len(parts) == 2 {
			return true, nil
		}
	}
	return false, fmt.Errorf("not a timestamp")
}

func isNullValue(value string, nullValues []string) bool {
	for _, nullValue := range nullValues {
		if value == nullValue {
			return true
		}
	}
	return false
}

// Add options validation
func validateOptions(opts *CSVReadOptions) error {
	if opts == nil {
		return errors.New("CSV read options cannot be nil")
	}
	if opts.Delimiter == 0 {
		opts.Delimiter = ','
	}
	if opts.NullValues == nil {
		opts.NullValues = []string{"", "NULL", "null", "NA", "na"}
	}
	return nil
}

// Add metadata to schema
func buildSchema(headers []string, types []arrow.DataType, nullability []bool, opts *CSVReadOptions) *arrow.Schema {
	fields := make([]arrow.Field, len(headers))
	for i, name := range headers {
		fields[i] = arrow.Field{
			Name:     name,
			Type:     types[i],
			Nullable: nullability[i],
			Metadata: arrow.MetadataFrom(map[string]string{
				"original_index": strconv.Itoa(i),
				"inferred_from":  "csv",
			}),
		}
	}

	metadata := arrow.MetadataFrom(map[string]string{
		"delimiter":   string(opts.Delimiter),
		"has_header":  strconv.FormatBool(opts.HasHeader),
		"inferred_at": time.Now().UTC().Format(time.RFC3339),
	})
	return arrow.NewSchema(fields, &metadata)
}

func processRows(rowChan chan []string, types []arrow.DataType, nullability []bool, opts *CSVReadOptions, wg *sync.WaitGroup, errChan chan *inferenceError) {
	defer wg.Done()

	for row := range rowChan {
		for colIndex, value := range row {
			types[colIndex] = inferColumnType(types[colIndex], value, opts)
			if isNullValue(value, opts.NullValues) {
				nullability[colIndex] = true
			}
		}
	}
}

func readHeaders(reader *csv.Reader, opts *CSVReadOptions) ([]string, error) {
	headers, err := reader.Read()
	if err != nil {
		return nil, fmt.Errorf("failed to read headers: %w", err)
	}

	if !opts.HasHeader {
		// Generate default headers (col0, col1, etc.)
		for i := range headers {
			headers[i] = fmt.Sprintf("col%d", i)
		}
	}

	return headers, nil
}
