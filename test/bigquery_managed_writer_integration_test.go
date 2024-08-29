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

	"cloud.google.com/go/bigquery"
	x "github.com/arrowarc/arrowarc/experiments"
)

func TestAppends(t *testing.T) {
	// Skip test in CI environment if DuckDB shared library is not available.
	if os.Getenv("CI") == "true" {
		t.Skip("Skipping DuckDB integration test in CI environment.")
	}

	projectID := "tfmv-371720"
	ctx := context.Background()

	client, err := bigquery.NewClient(ctx, projectID)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	meta := &bigquery.DatasetMetadata{
		Location: "US", // See https://cloud.google.com/bigquery/docs/locations
	}
	testDatasetID, err := x.UniqueBQName("arrowarc_managed_writer_test")
	if err != nil {
		t.Fatalf("couldn't generate unique resource name: %v", err)
	}
	if err := client.Dataset(testDatasetID).Create(ctx, meta); err != nil {
		t.Fatalf("failed to create test dataset: %v", err)
	}
	// Cleanup dataset at end of test.
	defer client.Dataset(testDatasetID).DeleteWithContents(ctx)

	testTableID, err := x.UniqueBQName("testtable")
	if err != nil {
		t.Fatalf("couldn't generate unique table id: %v", err)
	}

	pendingSchema := bigquery.Schema{
		{Name: "bool_col", Type: bigquery.BooleanFieldType},
		{Name: "bytes_col", Type: bigquery.BytesFieldType},
		{Name: "float64_col", Type: bigquery.FloatFieldType},
		{Name: "int64_col", Type: bigquery.IntegerFieldType},
		{Name: "string_col", Type: bigquery.StringFieldType},
		{Name: "date_col", Type: bigquery.DateFieldType},
		{Name: "datetime_col", Type: bigquery.DateTimeFieldType},
		{Name: "geography_col", Type: bigquery.GeographyFieldType},
		{Name: "numeric_col", Type: bigquery.NumericFieldType},
		{Name: "bignumeric_col", Type: bigquery.BigNumericFieldType},
		{Name: "time_col", Type: bigquery.TimeFieldType},
		{Name: "timestamp_col", Type: bigquery.TimestampFieldType},

		{Name: "int64_list", Type: bigquery.IntegerFieldType, Repeated: true},
		{Name: "struct_col", Type: bigquery.RecordFieldType,
			Schema: bigquery.Schema{
				{Name: "sub_int_col", Type: bigquery.IntegerFieldType},
			}},
		{Name: "struct_list", Type: bigquery.RecordFieldType, Repeated: true,
			Schema: bigquery.Schema{
				{Name: "sub_int_col", Type: bigquery.IntegerFieldType},
			}},
		{Name: "row_num", Type: bigquery.IntegerFieldType, Required: true},
	}

	if err := client.Dataset(testDatasetID).Table(testTableID).Create(ctx, &bigquery.TableMetadata{
		Schema: pendingSchema,
	}); err != nil {
		t.Fatalf("failed to create destination table(%q %q): %v", testDatasetID, testTableID, err)
	}

	t.Run("PendingStream", func(t *testing.T) {
		if err := x.AppendToPendingStream(io.Discard, projectID, testDatasetID, testTableID); err != nil {
			t.Errorf("AppendToPendingStream(%q %q): %v", testDatasetID, testTableID, err)
		}
	})

	t.Run("DefaultStream", func(t *testing.T) {
		if err := x.AppendToDefaultStream(io.Discard, projectID, testDatasetID, testTableID); err != nil {
			t.Errorf("AppendToDefaultStream(%q %q): %v", testDatasetID, testTableID, err)
		}
	})

}
