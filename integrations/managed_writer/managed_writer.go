package managed_writer

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"time"

	bq "cloud.google.com/go/bigquery"
	"cloud.google.com/go/bigquery/storage/apiv1/storagepb"
	"cloud.google.com/go/bigquery/storage/managedwriter"
	"cloud.google.com/go/bigquery/storage/managedwriter/adapt"
	"github.com/GoogleCloudPlatform/golang-samples/bigquery/snippets/managedwriter/exampleproto"
	arrow "github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/arrowarc/arrowarc/internal/memory"
	"github.com/arrowarc/arrowarc/pkg/arrowproto"
	"google.golang.org/protobuf/proto"
)

// AppendArrowRecordToBigQuery writes Arrow records to BigQuery using the managed writer.
func AppendArrowRecordToBigQuery(w io.Writer, projectID, datasetID, tableID string, tableSchema *bq.Schema) (*managedwriter.AppendResult, error) {
	ctx := context.Background()
	// Instantiate a managedwriter client to handle interactions with the service.
	client, err := managedwriter.NewClient(ctx, projectID,
		managedwriter.WithMultiplexing(), // Enables connection sharing.
	)
	if err != nil {
		return nil, fmt.Errorf("managedwriter.NewClient: %w", err)
	}
	// Close the client when we exit the function.
	defer client.Close()

	var m *exampleproto.SampleData
	descriptorProto, err := adapt.NormalizeDescriptor(m.ProtoReflect().Descriptor())
	if err != nil {
		return nil, fmt.Errorf("NormalizeDescriptor: %w", err)
	}

	// Define the table reference
	tableReference := managedwriter.TableParentFromParts(projectID, datasetID, tableID)

	// Create a new managed stream for writing to BigQuery
	managedStream, err := client.NewManagedStream(ctx,
		managedwriter.WithType(managedwriter.DefaultStream),
		managedwriter.WithDestinationTable(tableReference),
		managedwriter.WithSchemaDescriptor(descriptorProto),
	)
	if err != nil {
		return nil, fmt.Errorf("NewManagedStream: %w", err)
	}
	defer managedStream.Close()

	arrowRecords, err := generateDefaultArrowMessages(1)
	if err != nil {
		return nil, fmt.Errorf("generateDefaultArrowMessages: %w", err)
	}

	protoMessages, err := arrowproto.ConvertArrowRecordToProtoMessages(arrowRecords[0], descriptorProto)
	if err != nil {
		return nil, fmt.Errorf("ConvertArrowRecordToProtoMessages: %w", err)
	}

	// Serialize each Protobuf message into binary format
	rows := make([][]byte, len(protoMessages))
	for i, msg := range protoMessages {
		b, err := proto.Marshal(msg)
		if err != nil {
			return nil, fmt.Errorf("error marshaling message %d: %w", i, err)
		}
		rows[i] = b
	}
	result, err := managedStream.AppendRows(ctx, rows)
	if err != nil {
		return nil, fmt.Errorf("AppendRows error: %w", err)
	}
	return result, nil
}

func generateDefaultArrowMessages(numMessages int) ([]arrow.Record, error) {
	// Define the Arrow schema matching your example Protobuf message
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "bool_col", Type: arrow.FixedWidthTypes.Boolean},
		{Name: "bytes_col", Type: arrow.BinaryTypes.Binary},
		{Name: "float64_col", Type: arrow.PrimitiveTypes.Float64},
		{Name: "int64_col", Type: arrow.PrimitiveTypes.Int64},
		{Name: "string_col", Type: arrow.BinaryTypes.String},
		{Name: "date_col", Type: arrow.PrimitiveTypes.Int32},
		{Name: "datetime_col", Type: arrow.BinaryTypes.String},
		{Name: "geography_col", Type: arrow.BinaryTypes.String},
		{Name: "numeric_col", Type: arrow.BinaryTypes.String},
		{Name: "bignumeric_col", Type: arrow.BinaryTypes.String},
		{Name: "time_col", Type: arrow.BinaryTypes.String},
		{Name: "timestamp_col", Type: arrow.PrimitiveTypes.Int64},
		{Name: "int64_list", Type: arrow.ListOf(arrow.PrimitiveTypes.Int64)},
		{Name: "row_num", Type: arrow.PrimitiveTypes.Int64},
		{Name: "struct_col", Type: arrow.StructOf(
			arrow.Field{Name: "sub_int_col", Type: arrow.PrimitiveTypes.Int64},
		)},
		{Name: "struct_list", Type: arrow.ListOf(arrow.StructOf(
			arrow.Field{Name: "sub_int_col", Type: arrow.PrimitiveTypes.Int64},
		))},
	}, nil)

	pool := memory.NewGoAllocator()
	builder := array.NewRecordBuilder(pool, schema)
	defer builder.Release()

	// Generate random data for Arrow records
	for i := 0; i < numMessages; i++ {
		// Create random data corresponding to the schema
		random := rand.New(rand.NewSource(time.Now().UnixNano()))
		builder.Field(0).(*array.BooleanBuilder).Append(true)
		builder.Field(1).(*array.BinaryBuilder).Append([]byte("some bytes"))
		builder.Field(2).(*array.Float64Builder).Append(3.14)
		builder.Field(3).(*array.Int64Builder).Append(123)
		builder.Field(4).(*array.StringBuilder).Append("example string value")
		builder.Field(5).(*array.Int32Builder).Append(int32(time.Now().UnixNano() / 86400000000000))
		builder.Field(6).(*array.StringBuilder).Append("2022-01-01 12:13:14.000000")
		builder.Field(7).(*array.StringBuilder).Append("POINT(-122.350220 47.649154)")
		builder.Field(8).(*array.StringBuilder).Append("99999999999999999999999999999.999999999")
		builder.Field(9).(*array.StringBuilder).Append("578960446186580977117854925043439539266.34992332820282019728792003956564819967")
		builder.Field(10).(*array.StringBuilder).Append("12:13:14.000000")
		builder.Field(11).(*array.Int64Builder).Append(time.Now().UnixNano() / 1000)

		// Fill int64 list
		listBuilder := builder.Field(12).(*array.ListBuilder)
		listBuilder.Append(true)
		int64Builder := listBuilder.ValueBuilder().(*array.Int64Builder)
		int64Builder.AppendValues([]int64{2, 4, 6, 8}, nil)

		builder.Field(13).(*array.Int64Builder).Append(23)

		// Fill struct fields
		structBuilder := builder.Field(14).(*array.StructBuilder)
		structBuilder.Append(true)
		structBuilder.FieldBuilder(0).(*array.Int64Builder).Append(random.Int63())

		// Fill struct list
		listStructBuilder := builder.Field(15).(*array.ListBuilder)
		listStructBuilder.Append(true)
		structListValueBuilder := listStructBuilder.ValueBuilder().(*array.StructBuilder)
		for j := 0; j < int(random.Int63n(5)+1); j++ {
			structListValueBuilder.Append(true)
			structListValueBuilder.FieldBuilder(0).(*array.Int64Builder).Append(random.Int63())
		}
	}

	// Create a single Arrow Record
	record := builder.NewRecord()
	defer record.Release()

	return []arrow.Record{record}, nil
}

// generateExampleMessages generates a slice of serialized protobuf messages using a statically defined
// and compiled protocol buffer file, and returns the binary serialized representation.
func generateExampleDefaultMessages(numMessages int) ([][]byte, error) {
	msgs := make([][]byte, numMessages)
	for i := 0; i < numMessages; i++ {

		// instantiate a new random source.
		random := rand.New(
			rand.NewSource(time.Now().UnixNano()),
		)

		// Our example data embeds an array of structs, so we'll construct that first.
		sl := make([]*exampleproto.SampleStruct, 5)
		for i := 0; i < int(random.Int63n(5)+1); i++ {
			sl[i] = &exampleproto.SampleStruct{
				SubIntCol: proto.Int64(random.Int63()),
			}
		}

		m := &exampleproto.SampleData{
			BoolCol:    proto.Bool(true),
			BytesCol:   []byte("some bytes"),
			Float64Col: proto.Float64(3.14),
			Int64Col:   proto.Int64(123),
			StringCol:  proto.String("example string value"),

			// These types require special encoding/formatting to transmit.

			// DATE values are number of days since the Unix epoch.

			DateCol: proto.Int32(int32(time.Now().UnixNano() / 86400000000000)),

			// DATETIME uses the literal format.
			DatetimeCol: proto.String("2022-01-01 12:13:14.000000"),

			// GEOGRAPHY uses Well-Known-Text (WKT) format.
			GeographyCol: proto.String("POINT(-122.350220 47.649154)"),

			// NUMERIC and BIGNUMERIC can be passed as string, or more efficiently
			// using a packed byte representation.
			NumericCol:    proto.String("99999999999999999999999999999.999999999"),
			BignumericCol: proto.String("578960446186580977117854925043439539266.34992332820282019728792003956564819967"),

			// TIME also uses literal format.
			TimeCol: proto.String("12:13:14.000000"),

			// TIMESTAMP uses microseconds since Unix epoch.
			TimestampCol: proto.Int64(time.Now().UnixNano() / 1000),

			// Int64List is an array of INT64 types.
			Int64List: []int64{2, 4, 6, 8},

			// This is a required field in the schema, and thus must be present.
			RowNum: proto.Int64(23),

			// StructCol is a single nested message.
			StructCol: &exampleproto.SampleStruct{
				SubIntCol: proto.Int64(random.Int63()),
			},

			// StructList is a repeated array of a nested message.
			StructList: sl,
		}

		b, err := proto.Marshal(m)
		if err != nil {
			return nil, fmt.Errorf("error generating message %d: %w", i, err)
		}
		msgs[i] = b
	}
	return msgs, nil
}

// appendToDefaultStream demonstrates using the managedwriter package to write some example data
// to a default stream.
func AppendToDefaultStream(w io.Writer, projectID, datasetID, tableID string) error {
	// projectID := "myproject"
	// datasetID := "mydataset"
	// tableID := "mytable"

	ctx := context.Background()
	// Instantiate a managedwriter client to handle interactions with the service.
	client, err := managedwriter.NewClient(ctx, projectID,
		managedwriter.WithMultiplexing(), // Enables connection sharing.
	)
	if err != nil {
		return fmt.Errorf("managedwriter.NewClient: %w", err)
	}
	// Close the client when we exit the function.
	defer client.Close()

	// We need to communicate the descriptor of the protocol buffer message we're using, which
	// is analagous to the "schema" for the message.  Both SampleData and SampleStruct are
	// two distinct messages in the compiled proto file, so we'll use adapt.NormalizeDescriptor
	// to unify them into a single self-contained descriptor representation.
	var m *exampleproto.SampleData
	descriptorProto, err := adapt.NormalizeDescriptor(m.ProtoReflect().Descriptor())
	if err != nil {
		return fmt.Errorf("NormalizeDescriptor: %w", err)
	}

	// Build the formatted reference to the destination table.
	tableReference := managedwriter.TableParentFromParts(projectID, datasetID, tableID)

	// Instantiate a ManagedStream, which manages low level details like connection state and provides
	// additional features like a future-like callback for appends, etc.  Default streams are provided by
	// the system, so there's no need to create them.
	managedStream, err := client.NewManagedStream(ctx,
		managedwriter.WithType(managedwriter.DefaultStream),
		managedwriter.WithDestinationTable(tableReference),
		managedwriter.WithSchemaDescriptor(descriptorProto),
	)
	if err != nil {
		return fmt.Errorf("NewManagedStream: %w", err)
	}
	// Automatically close the writer when we're done.
	defer managedStream.Close()

	// First, we'll append a single row.
	rows, err := generateExampleDefaultMessages(1)
	if err != nil {
		return fmt.Errorf("generateExampleMessages: %w", err)
	}

	// We can append data asyncronously, so we'll check our appends at the end.
	var results []*managedwriter.AppendResult

	result, err := managedStream.AppendRows(ctx, rows)
	if err != nil {
		return fmt.Errorf("AppendRows first call error: %w", err)
	}
	results = append(results, result)

	// This time, we'll append three more rows in a single request.
	rows, err = generateExampleMessages(3)
	if err != nil {
		return fmt.Errorf("generateExampleMessages: %w", err)
	}
	result, err = managedStream.AppendRows(ctx, rows)
	if err != nil {
		return fmt.Errorf("AppendRows second call error: %w", err)
	}
	results = append(results, result)

	// Finally, we'll append two more rows.
	rows, err = generateExampleMessages(2)
	if err != nil {
		return fmt.Errorf("generateExampleMessages: %w", err)
	}
	result, err = managedStream.AppendRows(ctx, rows)
	if err != nil {
		return fmt.Errorf("AppendRows third call error: %w", err)
	}
	results = append(results, result)

	// We've been collecting references to our status callbacks to allow us to append in a faster
	// asynchronous fashion.  Normally you could do this in another goroutine or similar, but for
	// this example we'll now iterate through those results and verify they were all successful.
	for k, v := range results {
		// GetResult blocks until we receive a response from the API.
		recvOffset, err := v.GetResult(ctx)
		if err != nil {
			return fmt.Errorf("append %d returned error: %w", k, err)
		}
		fmt.Fprintf(w, "Successfully appended data at offset %d.\n", recvOffset)
	}

	// This stream is a default stream, which means it doesn't require any form of finalization
	// or commit.  The rows were automatically committed to the table.
	return nil
}

// generateExampleMessages generates a slice of serialized protobuf messages using a statically defined
// and compiled protocol buffer file, and returns the binary serialized representation.
func generateExampleMessages(numMessages int) ([][]byte, error) {
	msgs := make([][]byte, numMessages)
	for i := 0; i < numMessages; i++ {

		random := rand.New(rand.NewSource(time.Now().UnixNano()))

		// Our example data embeds an array of structs, so we'll construct that first.
		sList := make([]*exampleproto.SampleStruct, 5)
		for i := 0; i < int(random.Int63n(5)+1); i++ {
			sList[i] = &exampleproto.SampleStruct{
				SubIntCol: proto.Int64(random.Int63()),
			}
		}

		m := &exampleproto.SampleData{
			BoolCol:    proto.Bool(true),
			BytesCol:   []byte("some bytes"),
			Float64Col: proto.Float64(3.14),
			Int64Col:   proto.Int64(123),
			StringCol:  proto.String("example string value"),

			// These types require special encoding/formatting to transmit.

			// DATE values are number of days since the Unix epoch.

			DateCol: proto.Int32(int32(time.Now().UnixNano() / 86400000000000)),

			// DATETIME uses the literal format.
			DatetimeCol: proto.String("2022-01-01 12:13:14.000000"),

			// GEOGRAPHY uses Well-Known-Text (WKT) format.
			GeographyCol: proto.String("POINT(-122.350220 47.649154)"),

			// NUMERIC and BIGNUMERIC can be passed as string, or more efficiently
			// using a packed byte representation.
			NumericCol:    proto.String("99999999999999999999999999999.999999999"),
			BignumericCol: proto.String("578960446186580977117854925043439539266.34992332820282019728792003956564819967"),

			// TIME also uses literal format.
			TimeCol: proto.String("12:13:14.000000"),

			// TIMESTAMP uses microseconds since Unix epoch.
			TimestampCol: proto.Int64(time.Now().UnixNano() / 1000),

			// Int64List is an array of INT64 types.
			Int64List: []int64{2, 4, 6, 8},

			// This is a required field, and thus must be present.
			RowNum: proto.Int64(23),

			// StructCol is a single nested message.
			StructCol: &exampleproto.SampleStruct{
				SubIntCol: proto.Int64(random.Int63()),
			},

			// StructList is a repeated array of a nested message.
			StructList: sList,
		}

		b, err := proto.Marshal(m)
		if err != nil {
			return nil, fmt.Errorf("error generating message %d: %w", i, err)
		}
		msgs[i] = b
	}
	return msgs, nil
}

func AppendToPendingStream(w io.Writer, projectID, datasetID, tableID string) error {
	// projectID := "myproject"
	// datasetID := "mydataset"
	// tableID := "mytable"

	ctx := context.Background()
	// Instantiate a managedwriter client to handle interactions with the service.
	client, err := managedwriter.NewClient(ctx, projectID)
	if err != nil {
		return fmt.Errorf("managedwriter.NewClient: %w", err)
	}
	// Close the client when we exit the function.
	defer client.Close()

	// Create a new pending stream.  We'll use the stream name to construct a writer.
	pendingStream, err := client.CreateWriteStream(ctx, &storagepb.CreateWriteStreamRequest{
		Parent: fmt.Sprintf("projects/%s/datasets/%s/tables/%s", projectID, datasetID, tableID),
		WriteStream: &storagepb.WriteStream{
			Type: storagepb.WriteStream_PENDING,
		},
	})
	if err != nil {
		return fmt.Errorf("CreateWriteStream: %w", err)
	}

	// We need to communicate the descriptor of the protocol buffer message we're using, which
	// is analagous to the "schema" for the message.  Both SampleData and SampleStruct are
	// two distinct messages in the compiled proto file, so we'll use adapt.NormalizeDescriptor
	// to unify them into a single self-contained descriptor representation.
	m := &exampleproto.SampleData{}
	descriptorProto, err := adapt.NormalizeDescriptor(m.ProtoReflect().Descriptor())
	if err != nil {
		return fmt.Errorf("NormalizeDescriptor: %w", err)
	}

	// Instantiate a ManagedStream, which manages low level details like connection state and provides
	// additional features like a future-like callback for appends, etc.  NewManagedStream can also create
	// the stream on your behalf, but in this example we're being explicit about stream creation.
	managedStream, err := client.NewManagedStream(ctx, managedwriter.WithStreamName(pendingStream.GetName()),
		managedwriter.WithSchemaDescriptor(descriptorProto))
	if err != nil {
		return fmt.Errorf("NewManagedStream: %w", err)
	}
	defer managedStream.Close()

	// First, we'll append a single row.
	rows, err := generateExampleMessages(1)
	if err != nil {
		return fmt.Errorf("generateExampleMessages: %w", err)
	}

	// We'll keep track of the current offset in the stream with curOffset.
	var curOffset int64
	// We can append data asyncronously, so we'll check our appends at the end.
	var results []*managedwriter.AppendResult

	result, err := managedStream.AppendRows(ctx, rows, managedwriter.WithOffset(0))
	if err != nil {
		return fmt.Errorf("AppendRows first call error: %w", err)
	}
	results = append(results, result)

	// Advance our current offset.
	curOffset = curOffset + 1

	// This time, we'll append three more rows in a single request.
	rows, err = generateExampleMessages(3)
	if err != nil {
		return fmt.Errorf("generateExampleMessages: %w", err)
	}
	result, err = managedStream.AppendRows(ctx, rows, managedwriter.WithOffset(curOffset))
	if err != nil {
		return fmt.Errorf("AppendRows second call error: %w", err)
	}
	results = append(results, result)

	// Advance our offset again.
	curOffset = curOffset + 3

	// Finally, we'll append two more rows.
	rows, err = generateExampleMessages(2)
	if err != nil {
		return fmt.Errorf("generateExampleMessages: %w", err)
	}
	result, err = managedStream.AppendRows(ctx, rows, managedwriter.WithOffset(curOffset))
	if err != nil {
		return fmt.Errorf("AppendRows third call error: %w", err)
	}
	results = append(results, result)

	// Now, we'll check that our batch of three appends all completed successfully.
	// Monitoring the results could also be done out of band via a goroutine.
	for k, v := range results {
		// GetResult blocks until we receive a response from the API.
		recvOffset, err := v.GetResult(ctx)
		if err != nil {
			return fmt.Errorf("append %d returned error: %w", k, err)
		}
		fmt.Fprintf(w, "Successfully appended data at offset %d.\n", recvOffset)
	}

	// We're now done appending to this stream.  We now mark pending stream finalized, which blocks
	// further appends.
	rowCount, err := managedStream.Finalize(ctx)
	if err != nil {
		return fmt.Errorf("error during Finalize: %w", err)
	}

	fmt.Fprintf(w, "Stream %s finalized with %d rows.\n", managedStream.StreamName(), rowCount)

	// To commit the data to the table, we need to run a batch commit.  You can commit several streams
	// atomically as a group, but in this instance we'll only commit the single stream.
	req := &storagepb.BatchCommitWriteStreamsRequest{
		Parent:       managedwriter.TableParentFromStreamName(managedStream.StreamName()),
		WriteStreams: []string{managedStream.StreamName()},
	}

	resp, err := client.BatchCommitWriteStreams(ctx, req)
	if err != nil {
		return fmt.Errorf("client.BatchCommit: %w", err)
	}
	if len(resp.GetStreamErrors()) > 0 {
		return fmt.Errorf("stream errors present: %v", resp.GetStreamErrors())
	}

	fmt.Fprintf(w, "Table data committed at %s\n", resp.GetCommitTime().AsTime().Format(time.RFC3339Nano))

	return nil
}
