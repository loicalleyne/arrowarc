package infer

import (
	"context"
	"encoding/csv"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"

	"github.com/apache/arrow/go/v17/arrow"
)

type CSVReadOptions struct {
	Delimiter        rune
	HasHeader        bool
	StringsCanBeNull bool
	NullValues       []string
}

// InferCSVArrowSchema infers the Arrow schema from a CSV file
func InferCSVArrowSchema(ctx context.Context, filePath string, opts *CSVReadOptions) (*arrow.Schema, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open CSV file: %w", err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	reader.Comma = opts.Delimiter
	reader.TrimLeadingSpace = true

	// Read header if present
	var headers []string
	if opts.HasHeader {
		headers, err = reader.Read()
		if err != nil {
			return nil, fmt.Errorf("failed to read CSV header: %w", err)
		}
	} else {
		firstRow, err := reader.Read()
		if err != nil {
			return nil, fmt.Errorf("failed to read first row: %w", err)
		}
		for i := range firstRow {
			headers = append(headers, fmt.Sprintf("field%d", i+1))
		}
	}

	columnTypes := make([]arrow.DataType, len(headers))
	columnNullability := make([]bool, len(headers))
	rows := 0

	// Use a channel to process rows in parallel
	rowChannel := make(chan []string)
	var wg sync.WaitGroup
	mu := sync.Mutex{}

	// Worker to process rows
	for i := 0; i < 4; i++ { // Use 4 goroutines for parallelism
		wg.Add(1)
		go func() {
			defer wg.Done()
			for row := range rowChannel {
				for colIndex, value := range row {
					mu.Lock()
					columnTypes[colIndex] = inferColumnType(columnTypes[colIndex], value, opts)
					if isNullValue(value, opts.NullValues) {
						columnNullability[colIndex] = true
					}
					mu.Unlock()
				}
			}
		}()
	}

	// Read up to 1000 rows
	for rows < 1000 {
		row, err := reader.Read()
		if err != nil {
			break // EOF or other error
		}
		rowChannel <- row
		rows++
	}

	close(rowChannel)
	wg.Wait()

	// Build Arrow schema from inferred data types
	fields := make([]arrow.Field, len(headers))
	for i, name := range headers {
		fields[i] = arrow.Field{Name: name, Type: columnTypes[i], Nullable: columnNullability[i]}
	}

	return arrow.NewSchema(fields, nil), nil
}

// inferColumnType detects the type of a given column based on the observed value
func inferColumnType(currentType arrow.DataType, value string, opts *CSVReadOptions) arrow.DataType {
	if isNullValue(value, opts.NullValues) {
		return currentType
	}

	// Attempt to parse the value as different data types
	if _, err := strconv.Atoi(value); err == nil {
		if currentType == nil || currentType.ID() == arrow.STRING {
			return arrow.PrimitiveTypes.Int64
		}
	}

	if _, err := strconv.ParseFloat(value, 64); err == nil {
		if currentType == nil || currentType.ID() == arrow.STRING || currentType.ID() == arrow.PrimitiveTypes.Int64.ID() {
			return arrow.PrimitiveTypes.Float64
		}
	}

	if _, err := parseDate(value); err == nil {
		if currentType == nil || currentType.ID() == arrow.STRING {
			return arrow.FixedWidthTypes.Date32
		}
	}

	if _, err := parseTimestamp(value); err == nil {
		if currentType == nil || currentType.ID() == arrow.STRING {
			return arrow.FixedWidthTypes.Timestamp_ms
		}
	}

	if value == "true" || value == "false" {
		if currentType == nil || currentType.ID() == arrow.STRING {
			return arrow.FixedWidthTypes.Boolean
		}
	}

	return arrow.BinaryTypes.String // Default to string if no other types match
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
