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
	"fmt"
	"io"
	"os"
	"testing"
	"time"

	bigquery "github.com/arrowarc/arrowarc/integrations/bigquery"
	parquet "github.com/arrowarc/arrowarc/integrations/filesystem"
	"github.com/arrowarc/arrowarc/pipeline"
	"github.com/arrowarc/arrowarc/pkg/common/utils"
	"github.com/stretchr/testify/assert"
)

func TestReadBigQueryStream(t *testing.T) {
	utils.LoadEnv()
	// Skip this test in CI environment if GCP credentials are not set
	if os.Getenv("CI") == "true" || os.Getenv("GOOGLE_APPLICATION_CREDENTIALS") == "" {
		t.Skip("Skipping BigQuery integration test in CI environment or when GCP credentials are not set.")
	}

	t.Parallel()

	projectID := "tfmv-371720"
	datasetID := "tpch"
	tableID := "region"

	tests := []struct {
		projectID   string
		datasetID   string
		tableID     string
		description string
	}{
		{
			projectID:   projectID,
			datasetID:   datasetID,
			tableID:     tableID,
			description: "Read from BigQuery table",
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.description, func(t *testing.T) {
			t.Parallel()

			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()

			bq, err := bigquery.NewBigQueryReadClient(ctx)
			assert.NoError(t, err, "Error should be nil when creating BigQuery connector")

			reader, err := bq.NewBigQueryReader(ctx, test.projectID, test.datasetID, test.tableID)
			assert.NoError(t, err, "Error should be nil when creating BigQuery Arrow reader")
			defer reader.Close() // Ensure reader is closed regardless of success or failure

			var recordsRead int
			for {
				record, err := reader.Read()
				if err == io.EOF {
					break
				}
				assert.NoError(t, err, "Error should be nil when reading from BigQuery table")
				assert.NotNil(t, record, "Record should not be nil")
				recordsRead += int(record.NumRows())
				record.Release()
			}

			assert.Greater(t, recordsRead, 0, "Should have read at least one record")
		})
	}
}

func TestWriteToParquetFromBigQuery(t *testing.T) {
	utils.LoadEnv()
	// Skip this test in CI environment if GCP credentials are not set
	if os.Getenv("CI") == "true" || os.Getenv("GOOGLE_APPLICATION_CREDENTIALS") == "" {
		t.Skip("Skipping BigQuery to Parquet integration test in CI environment or when GCP credentials are not set.")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Initialize BigQuery client
	bq, err := bigquery.NewBigQueryReadClient(ctx)
	assert.NoError(t, err, "Error should be nil when creating BigQuery client")

	// Create a BigQuery reader
	reader, err := bq.NewBigQueryReader(ctx, "tfmv-371720", "tpch", "region")
	assert.NoError(t, err, "Error should be nil when creating BigQuery Arrow reader")
	assert.NotNil(t, reader, "Reader should not be nil")
	defer reader.Close()

	// Initialize Parquet writer
	schema, err := reader.Schema()
	assert.NoError(t, err, "Error should be nil when getting schema")
	assert.NotNil(t, schema, "Schema should not be nil")

	props := parquet.NewDefaultParquetWriterProperties()

	parquetWriter, err := parquet.NewParquetWriter("region.parquet", schema, props)
	assert.NoError(t, err, "Error should be nil when creating Parquet writer")
	assert.NotNil(t, parquetWriter, "Parquet writer should not be nil")
	defer parquetWriter.Close()

	// Setup the pipeline to write records from BigQuery to Parquet
	p := pipeline.NewDataPipeline(reader, parquetWriter)
	assert.NoError(t, err, "Error should be nil when creating data pipeline")

	// Run the pipeline
	r, err := p.Start(ctx)
	assert.NoError(t, err, "Error should be nil when running data pipeline")
	// Print the transport report
	fmt.Println(r.Report())
}

/*
func TestWriteToDuckDBFromBigQuery(t *testing.T) {
	utils.LoadEnv()
	// Skip this test in CI environment if GCP credentials are not set
	if os.Getenv("CI") == "true" || os.Getenv("GOOGLE_APPLICATION_CREDENTIALS") == "" {
		t.Skip("Skipping BigQuery to DuckDB integration test in CI environment or when GCP credentials are not set.")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Initialize BigQuery client
	bq, err := bigquery.NewBigQueryReadClient(ctx)
	assert.NoError(t, err, "Error should be nil when creating BigQuery client")

	// Create a BigQuery reader
	reader, err := bq.NewBigQueryReader(ctx, "tfmv-371720", "tpch", "region")
	assert.NoError(t, err, "Error should be nil when creating BigQuery Arrow reader")
	defer reader.Close() // Ensure reader is closed regardless of success or failure

	// Initialize DuckDB in-memory database and writer
	duckDBURL := ":memory:?cache=shared"
	duckDBWriter, err := duckdb.NewDuckDBWriter(ctx, duckDBURL, "region", []duckdb.DuckDBExtension{
		{Name: "inet", LoadByDefault: true},
		{Name: "iceberg", LoadByDefault: true},
		{Name: "fts", LoadByDefault: true},
		{Name: "icu", LoadByDefault: true},
	})

	assert.NoError(t, err, "Error should be nil when creating DuckDB writer")
	defer duckDBWriter.Close() // Ensure DuckDB writer is closed regardless of success or failure

	// Setup the pipeline to write records from BigQuery to DuckDB
	p := pipeline.NewDataPipeline(reader, duckDBWriter)
	assert.NoError(t, err, "Error should be nil when creating data pipeline")

	// Run the pipeline
	r, err := p.Start(ctx)
	assert.NoError(t, err, "Error should be nil when running data pipeline")
	// print the transport report
	r.Report()

	// Query DuckDB to get the count of records
	duckDBReader, err := duckdb.NewDuckDBReader(ctx, duckDBURL, &duckdb.DuckDBReadOptions{
		Query: "SELECT COUNT(*) FROM region",
	})
	assert.NoError(t, err, "Error should be nil when creating DuckDB reader")
	defer duckDBReader.Close() // Ensure DuckDB reader is closed regardless of success or failure

	countRecord, err := duckDBReader.Read()
	assert.NoError(t, err, "Error should be nil when reading from DuckDB")

	fmt.Printf("Total rows written to DuckDB: %d\n", countRecord.NumRows())
	countRecord.Release()
}
*/
