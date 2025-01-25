package test

import (
	"context"
	"fmt"
	"io"
	"os"
	"testing"

	"cloud.google.com/go/bigquery"
	x "github.com/arrowarc/arrowarc/integrations/managed_writer"
	utils "github.com/arrowarc/arrowarc/internal/testutil"
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
	testDatasetID, err := utils.UniqueBQName("arrowarc_managed_writer_test")
	if err != nil {
		t.Fatalf("couldn't generate unique resource name: %v", err)
	}
	if err := client.Dataset(testDatasetID).Create(ctx, meta); err != nil {
		t.Fatalf("failed to create test dataset: %v", err)
	}
	// Cleanup dataset at end of test.
	defer client.Dataset(testDatasetID).DeleteWithContents(ctx)
	testTableID, err := utils.UniqueBQName("testtable")
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

	// Create the primary table with the pending schema
	if err := client.Dataset(testDatasetID).Table(testTableID).Create(ctx, &bigquery.TableMetadata{
		Schema: pendingSchema,
	}); err != nil {
		t.Fatalf("failed to create destination table(%q %q): %v", testDatasetID, testTableID, err)
	}

	t.Run("AppendArrowRecordToBigQuery", func(t *testing.T) {
		result, err := x.AppendArrowRecordToBigQuery(io.Discard, projectID, testDatasetID, testTableID, &pendingSchema)
		fmt.Printf("result: %v\n", result)
		if err != nil {
			t.Errorf("AppendArrowRecordToBigQuery(%q %q): %v", testDatasetID, testTableID, err)
		}
	})

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
