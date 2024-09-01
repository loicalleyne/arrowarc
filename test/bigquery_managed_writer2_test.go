package test

import (
	"context"
	"io"
	"os"
	"testing"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/bigquery/storage/apiv1/storagepb"
	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	x "github.com/arrowarc/arrowarc/experiments"
)

func TestAppends2(t *testing.T) {
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
		Location:    "US",
		Description: "ArrowArc managed writer test dataset",
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

	// Create an Arrow record batch that matches the BigQuery table schema
	arrowRecord, err := createArrowRecord()
	if err != nil {
		t.Fatalf("failed to create arrow record: %v", err)
	}

	t.Run("DefaultStream", func(t *testing.T) {
		if err := x.AppendToDefaultStream2(io.Discard, projectID, testDatasetID, testTableID, arrowRecord, &storagepb.TableSchema{
			Fields: []*storagepb.TableFieldSchema{
				{Name: "bool_col", Type: storagepb.TableFieldSchema_BOOL},
				{Name: "bytes_col", Type: storagepb.TableFieldSchema_BYTES},
				{Name: "float64_col", Type: storagepb.TableFieldSchema_DOUBLE},
				{Name: "int64_col", Type: storagepb.TableFieldSchema_INT64},
				{Name: "string_col", Type: storagepb.TableFieldSchema_STRING},
				{Name: "date_col", Type: storagepb.TableFieldSchema_DATE},
				{Name: "datetime_col", Type: storagepb.TableFieldSchema_DATETIME},
				{Name: "geography_col", Type: storagepb.TableFieldSchema_GEOGRAPHY},
				{Name: "numeric_col", Type: storagepb.TableFieldSchema_NUMERIC},
				{Name: "bignumeric_col", Type: storagepb.TableFieldSchema_BIGNUMERIC},
				{Name: "time_col", Type: storagepb.TableFieldSchema_TIME},
				{Name: "timestamp_col", Type: storagepb.TableFieldSchema_TIMESTAMP},
				{Name: "int64_list", Type: storagepb.TableFieldSchema_INT64, Mode: storagepb.TableFieldSchema_REPEATED},
				{Name: "struct_col", Type: storagepb.TableFieldSchema_STRUCT,
					Fields: []*storagepb.TableFieldSchema{
						{Name: "sub_int_col", Type: storagepb.TableFieldSchema_INT64, Mode: storagepb.TableFieldSchema_NULLABLE},
					},
				},
				{Name: "struct_list", Type: storagepb.TableFieldSchema_STRUCT, Mode: storagepb.TableFieldSchema_REPEATED,
					Fields: []*storagepb.TableFieldSchema{
						{Name: "sub_int_col", Type: storagepb.TableFieldSchema_INT64, Mode: storagepb.TableFieldSchema_NULLABLE},
					},
				},
				{Name: "row_num", Type: storagepb.TableFieldSchema_INT64, Mode: storagepb.TableFieldSchema_REQUIRED},
			},
		}); err != nil {
			t.Errorf("AppendToDefaultStream2(%q %q): %v", testDatasetID, testTableID, err)
		}
	})

}

// createArrowRecord creates a sample Arrow record that matches the BigQuery table schema.
func createArrowRecord() (arrow.Record, error) {
	pool := memory.NewGoAllocator()
	builder := array.NewRecordBuilder(pool, arrow.NewSchema(
		[]arrow.Field{
			{Name: "bool_col", Type: arrow.FixedWidthTypes.Boolean},
			{Name: "bytes_col", Type: arrow.BinaryTypes.Binary},
			{Name: "float64_col", Type: arrow.PrimitiveTypes.Float64},
			{Name: "int64_col", Type: arrow.PrimitiveTypes.Int64},
			{Name: "string_col", Type: arrow.BinaryTypes.String},
			{Name: "date_col", Type: arrow.FixedWidthTypes.Date32},
			{Name: "datetime_col", Type: arrow.FixedWidthTypes.Timestamp_ms},
			{Name: "geography_col", Type: arrow.BinaryTypes.String},
			{Name: "numeric_col", Type: arrow.PrimitiveTypes.Float64},
			{Name: "bignumeric_col", Type: arrow.PrimitiveTypes.Float64},
			{Name: "time_col", Type: arrow.FixedWidthTypes.Time64ns},
			{Name: "timestamp_col", Type: arrow.FixedWidthTypes.Timestamp_ms},
			{Name: "int64_list", Type: arrow.ListOf(arrow.PrimitiveTypes.Int64)},
			{Name: "struct_col", Type: arrow.StructOf(arrow.Field{Name: "sub_int_col", Type: arrow.PrimitiveTypes.Int64})},
			{Name: "struct_list", Type: arrow.ListOf(arrow.StructOf(arrow.Field{Name: "sub_int_col", Type: arrow.PrimitiveTypes.Int64}))},
			{Name: "row_num", Type: arrow.PrimitiveTypes.Int64},
		},
		nil,
	))

	defer builder.Release()

	// Populate the record with sample data
	builder.Field(0).(*array.BooleanBuilder).AppendValues([]bool{true, false}, nil)                     // bool_col
	builder.Field(1).(*array.BinaryBuilder).AppendValues([][]byte{[]byte("foo"), []byte("bar")}, nil)   // bytes_col
	builder.Field(2).(*array.Float64Builder).AppendValues([]float64{3.14, 2.71}, nil)                   // float64_col
	builder.Field(3).(*array.Int64Builder).AppendValues([]int64{42, 7}, nil)                            // int64_col
	builder.Field(4).(*array.StringBuilder).AppendValues([]string{"hello", "world"}, nil)               // string_col
	builder.Field(5).(*array.Date32Builder).AppendValues([]arrow.Date32{1, 2}, nil)                     // date_col
	builder.Field(6).(*array.TimestampBuilder).AppendValues([]arrow.Timestamp{1, 2}, nil)               // datetime_col
	builder.Field(7).(*array.StringBuilder).AppendValues([]string{"POINT(30 10)", "POINT(10 30)"}, nil) // geography_col
	builder.Field(8).(*array.Float64Builder).AppendValues([]float64{3.14, 2.71}, nil)                   // numeric_col
	builder.Field(9).(*array.Float64Builder).AppendValues([]float64{3.14, 2.71}, nil)                   // bignumeric_col
	builder.Field(10).(*array.Time64Builder).AppendValues([]arrow.Time64{1, 2}, nil)                    // time_col
	builder.Field(11).(*array.TimestampBuilder).AppendValues([]arrow.Timestamp{1, 2}, nil)              // timestamp_col

	// Populate the list and struct fields correctly
	listBuilder := builder.Field(12).(*array.ListBuilder) // int64_list
	for i := 0; i < 2; i++ {
		listBuilder.Append(true)
		valueBuilder := listBuilder.ValueBuilder().(*array.Int64Builder)
		valueBuilder.AppendValues([]int64{1, 2}, nil)
	}

	structBuilder := builder.Field(13).(*array.StructBuilder) // struct_col
	for i := 0; i < 2; i++ {
		structBuilder.Append(true)
		structBuilder.FieldBuilder(0).(*array.Int64Builder).Append(100)
	}

	listStructBuilder := builder.Field(14).(*array.ListBuilder) // struct_list
	for i := 0; i < 2; i++ {
		listStructBuilder.Append(true)
		subStructBuilder := listStructBuilder.ValueBuilder().(*array.StructBuilder)
		subStructBuilder.Append(true)
		subStructBuilder.FieldBuilder(0).(*array.Int64Builder).Append(200)
	}

	builder.Field(15).(*array.Int64Builder).AppendValues([]int64{1, 2}, nil) // row_num

	// Create the record
	record := builder.NewRecord()
	return record, nil
}
