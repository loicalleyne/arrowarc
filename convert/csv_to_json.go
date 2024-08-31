package convert

import (
	"context"
	"errors"
	"fmt"

	integrations "github.com/arrowarc/arrowarc/integrations/filesystem"
	"github.com/arrowarc/arrowarc/pipeline"
	csv "github.com/arrowarc/arrowarc/pkg/csv"
)

// ConvertCSVToParquet converts a CSV file to a Parquet file using Arrow
func ConvertCSVToJSON(
	ctx context.Context,
	csvFilePath, jsonFilePath string,
	hasHeader bool, chunkSize int64,
	delimiter rune,
	nullValues []string,
	stringsCanBeNull bool,
) (string, error) {

	// Validate input parameters
	if csvFilePath == "" {
		return "", errors.New("CSV file path cannot be empty")
	}
	if jsonFilePath == "" {
		return "", errors.New("JSON file path cannot be empty")
	}
	if chunkSize <= 0 {
		return "", errors.New("chunk size must be greater than zero")
	}
	if ctx == nil {
		return "", errors.New("context cannot be nil")
	}

	// Step 1: Infer schema from the CSV file
	schema, err := csv.InferCSVArrowSchema(ctx, csvFilePath, &csv.CSVReadOptions{
		HasHeader:        hasHeader,
		Delimiter:        delimiter,
		NullValues:       nullValues,
		StringsCanBeNull: stringsCanBeNull,
	})
	if err != nil {
		return "", fmt.Errorf("failed to infer schema: %w", err)
	}

	// Step 2: Create CSV reader with the inferred schema
	csvReader, err := integrations.NewCSVReader(ctx, csvFilePath, schema, &integrations.CSVReadOptions{
		HasHeader:        hasHeader,
		ChunkSize:        chunkSize,
		Delimiter:        delimiter,
		NullValues:       nullValues,
		StringsCanBeNull: stringsCanBeNull,
	})
	if err != nil {
		return "", fmt.Errorf("failed to create CSV reader: %w", err)
	}
	defer csvReader.Close()

	// Step 3: Setup Parquet writer with the inferred schema
	jsonWriter, err := integrations.NewJSONWriter(ctx, jsonFilePath)
	if err != nil {
		return "", fmt.Errorf("failed to create JSON writer: %w", err)
	}
	defer jsonWriter.Close()

	// Step 4: Setup and start the pipeline for conversion
	metrics, err := pipeline.NewDataPipeline(csvReader, jsonWriter).Start(ctx)
	if err != nil {
		return "", fmt.Errorf("failed to convert CSV to Parquet: %w", err)
	}

	return metrics, nil
}
