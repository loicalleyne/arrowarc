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
	"testing"
	"time"

	bigquery "github.com/arrowarc/arrowarc/internal/integrations/bigquery"
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

			recordChan, errChan := bq.ReadBigQueryStream(ctx, test.projectID, test.datasetID, test.tableID, "arrow")

			var recordsRead int
			for record := range recordChan {
				assert.NotNil(t, record, "Record should not be nil")
				recordsRead += int(record.NumRows())
				record.Release()
			}

			for err := range errChan {
				assert.NoError(t, err, "Error should be nil when reading from BigQuery table")
			}

			assert.Greater(t, recordsRead, 0, "Should have read at least one record")
		})
	}
}
