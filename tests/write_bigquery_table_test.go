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
	"os"
	"testing"
	"time"

	"github.com/apache/arrow/go/v17/arrow"
	bigquery "github.com/arrowarc/arrowarc/integrations/bigquery"
	arrdata "github.com/arrowarc/arrowarc/internal/arrdata"
	helper "github.com/arrowarc/arrowarc/pkg/common/utils"
	"github.com/stretchr/testify/assert"
)

func TestWriteArrowRecordsToBigQuery(t *testing.T) {
	// Load environment variables
	helper.LoadEnv()
	serviceAccountJSON := os.Getenv("GOOGLE_APPLICATION_CREDENTIALS_JSON")
	// Skip test in CI environment if GCP credentials are not set
	if os.Getenv("CI") == "true" || os.Getenv("GOOGLE_APPLICATION_CREDENTIALS") == "" {
		t.Skip("Skipping BigQuery integration test in CI environment or when GCP credentials are not set.")
	}

	if serviceAccountJSON == "" {
		t.Fatal("Service account JSON not provided")
	}

	t.Parallel()

	// BigQuery table details
	projectID := "tfmv-371720"
	datasetID := "tpch"
	tableID := "region"

	tests := []struct {
		description string
	}{
		{
			description: "Write to BigQuery table",
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.description, func(t *testing.T) {
			t.Parallel()

			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()

			// Define Arrow schema
			schema := arrow.NewSchema([]arrow.Field{
				{Name: "regionkey", Type: arrow.PrimitiveTypes.Int64},
				{Name: "name", Type: arrow.BinaryTypes.String},
			}, nil)

			// Initialize BigQuery write client with schema
			bqClient, err := bigquery.NewBigQueryWriteClient(ctx, serviceAccountJSON, schema)
			assert.NoError(t, err, "Error should be nil when creating BigQuery write client")

			// Prepare Arrow records
			records := arrdata.MakeRegionRecords()
			fmt.Printf("Number of records created: %d\n", len(records)) // Log the number of records created

			// Create a new BigQuery writer using the updated interface
			writer, err := bigquery.NewBigQueryRecordWriter(ctx, bqClient, projectID, datasetID, tableID, nil)
			assert.NoError(t, err, "Error should be nil when creating BigQuery record writer")

			for _, record := range records {
				fmt.Printf("Generated Record: %v\n", record)
				err := writer.Write(record)
				assert.NoError(t, err, "Error should be nil when writing record to BigQuery")
				record.Release()
			}

			assert.NoError(t, err, "Error should be nil when closing BigQuery record writer")

			// Verify data written to BigQuery
			readClient, err := bigquery.NewBigQueryReadClient(ctx)
			assert.NoError(t, err, "Error should be nil when creating BigQuery read client")

			recordReader, err := readClient.NewBigQueryReader(ctx, projectID, datasetID, tableID)
			assert.NoError(t, err, "Error should be nil when creating BigQuery Arrow reader")

			var recordsRead int
			for {
				rec, err := recordReader.Read()
				if err != nil {
					assert.NoError(t, err, "Error should be nil when reading from BigQuery table")
					break
				}
				assert.NotNil(t, rec, "Record should not be nil")
				recordsRead += int(rec.NumRows())
				rec.Release()
			}

			assert.Greater(t, recordsRead, 0, "Should have read at least one record")
			assert.NoError(t, err, "Error should be nil when closing BigQuery record writer")

		})
	}
}
