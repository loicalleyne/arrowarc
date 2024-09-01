package experiments

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sync"

	storagepb "cloud.google.com/go/bigquery/storage/apiv1/storagepb"
	"cloud.google.com/go/bigquery/storage/managedwriter"
	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
)

// Helper function to return a pointer to the given FieldDescriptorProto_Type
func protoFieldType(t descriptorpb.FieldDescriptorProto_Type) *descriptorpb.FieldDescriptorProto_Type {
	return &t
}

// Helper function to return a pointer to the given FieldDescriptorProto_Label
func protoFieldLabel(l descriptorpb.FieldDescriptorProto_Label) *descriptorpb.FieldDescriptorProto_Label {
	return &l
}

// BuildDescriptorFromBQSchema builds a proto descriptor from a BigQuery schema.
func BuildDescriptorFromBQSchema(schema *storagepb.TableSchema, name string) (*descriptorpb.DescriptorProto, error) {
	descriptorProto := &descriptorpb.DescriptorProto{
		Name:  proto.String(name),
		Field: make([]*descriptorpb.FieldDescriptorProto, 0, len(schema.Fields)),
	}

	for i, field := range schema.Fields {
		fieldDescriptor := &descriptorpb.FieldDescriptorProto{
			Name:     proto.String(field.Name),
			JsonName: proto.String(field.Name),
			Number:   proto.Int32(int32(i + 1)),
			Type:     protoFieldType(bqTypeToProtoFieldType(field.Type)),
		}

		if field.Mode == storagepb.TableFieldSchema_REPEATED {
			fieldDescriptor.Label = protoFieldLabel(descriptorpb.FieldDescriptorProto_LABEL_REPEATED)
		}

		if field.Type == storagepb.TableFieldSchema_STRUCT {
			nestedDescriptor, err := BuildDescriptorFromBQSchema(&storagepb.TableSchema{Fields: field.Fields}, field.Name+"Type")
			if err != nil {
				return nil, err
			}
			fieldDescriptor.TypeName = proto.String("." + name + "." + *nestedDescriptor.Name)
			fieldDescriptor.Type = protoFieldType(descriptorpb.FieldDescriptorProto_TYPE_MESSAGE)
			descriptorProto.NestedType = append(descriptorProto.NestedType, nestedDescriptor)
		}

		descriptorProto.Field = append(descriptorProto.Field, fieldDescriptor)
	}

	return descriptorProto, nil
}

// bqTypeToProtoFieldType converts BigQuery field types to Protobuf field types.
func bqTypeToProtoFieldType(fieldType storagepb.TableFieldSchema_Type) descriptorpb.FieldDescriptorProto_Type {
	switch fieldType {
	case storagepb.TableFieldSchema_BOOL:
		return descriptorpb.FieldDescriptorProto_TYPE_BOOL
	case storagepb.TableFieldSchema_BYTES:
		return descriptorpb.FieldDescriptorProto_TYPE_BYTES
	case storagepb.TableFieldSchema_DOUBLE:
		return descriptorpb.FieldDescriptorProto_TYPE_DOUBLE
	case storagepb.TableFieldSchema_INT64:
		return descriptorpb.FieldDescriptorProto_TYPE_INT64
	case storagepb.TableFieldSchema_STRING:
		return descriptorpb.FieldDescriptorProto_TYPE_STRING
	case storagepb.TableFieldSchema_DATE, storagepb.TableFieldSchema_DATETIME,
		storagepb.TableFieldSchema_TIME, storagepb.TableFieldSchema_TIMESTAMP:
		return descriptorpb.FieldDescriptorProto_TYPE_STRING
	case storagepb.TableFieldSchema_STRUCT:
		return descriptorpb.FieldDescriptorProto_TYPE_MESSAGE
	default:
		return descriptorpb.FieldDescriptorProto_TYPE_STRING
	}
}

// ConvertArrowToProto converts an Arrow record batch to a slice of proto messages.
func ConvertArrowToProto(batch arrow.Record, descriptor protoreflect.MessageDescriptor) ([]*storagepb.AppendRowsRequest_ProtoRows, error) {
	protoRows := make([]*storagepb.AppendRowsRequest_ProtoRows, 0, batch.NumRows())

	var protoMessagePool = sync.Pool{
		New: func() interface{} {
			return dynamicpb.NewMessage(descriptor)
		},
	}

	for i := 0; i < int(batch.NumRows()); i++ {
		msg := protoMessagePool.Get().(*dynamicpb.Message)
		msg.Range(func(fd protoreflect.FieldDescriptor, v protoreflect.Value) bool {
			msg.Clear(fd)
			return true
		})

		for j, col := range batch.Columns() {
			field := descriptor.Fields().Get(j)
			fieldName := string(field.Name())
			value, err := getValue(col, i)
			if err != nil {
				return nil, fmt.Errorf("error getting value for field %s: %w", fieldName, err)
			}

			if value == nil {
				continue // Skip null values
			}

			protoValue, err := convertToProtoValue(value, field)
			if err != nil {
				return nil, fmt.Errorf("error converting value for field %s: %w", fieldName, err)
			}

			msg.Set(field, protoValue)
		}

		serialized, err := proto.Marshal(msg)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal proto message: %w", err)
		}

		protoRows = append(protoRows, &storagepb.AppendRowsRequest_ProtoRows{
			ProtoRows: &storagepb.AppendRowsRequest_ProtoData{
				Rows: &storagepb.ProtoRows{
					SerializedRows: [][]byte{serialized},
				},
			},
		})

		protoMessagePool.Put(msg)
	}

	return protoRows, nil
}

// convertToProtoValue converts an Arrow value to a Protobuf value.
func convertToProtoValue(value interface{}, fd protoreflect.FieldDescriptor) (protoreflect.Value, error) {
	switch fd.Kind() {
	case protoreflect.BoolKind:
		v, ok := value.(bool)
		if !ok {
			return protoreflect.Value{}, fmt.Errorf("invalid type %T for bool field", value)
		}
		return protoreflect.ValueOfBool(v), nil
	case protoreflect.Int32Kind:
		v, ok := value.(int32)
		if !ok {
			return protoreflect.Value{}, fmt.Errorf("invalid type %T for int32 field", value)
		}
		return protoreflect.ValueOfInt32(v), nil
	case protoreflect.Int64Kind:
		v, ok := value.(int64)
		if !ok {
			return protoreflect.Value{}, fmt.Errorf("invalid type %T for int64 field", value)
		}
		return protoreflect.ValueOfInt64(v), nil
	case protoreflect.Uint32Kind:
		v, ok := value.(uint32)
		if !ok {
			return protoreflect.Value{}, fmt.Errorf("invalid type %T for uint32 field", value)
		}
		return protoreflect.ValueOfUint32(v), nil
	case protoreflect.Uint64Kind:
		v, ok := value.(uint64)
		if !ok {
			return protoreflect.Value{}, fmt.Errorf("invalid type %T for uint64 field", value)
		}
		return protoreflect.ValueOfUint64(v), nil
	case protoreflect.FloatKind:
		v, ok := value.(float32)
		if !ok {
			return protoreflect.Value{}, fmt.Errorf("invalid type %T for float field", value)
		}
		return protoreflect.ValueOfFloat32(v), nil
	case protoreflect.DoubleKind:
		v, ok := value.(float64)
		if !ok {
			return protoreflect.Value{}, fmt.Errorf("invalid type %T for double field", value)
		}
		return protoreflect.ValueOfFloat64(v), nil
	case protoreflect.StringKind:
		v, ok := value.(string)
		if !ok {
			return protoreflect.Value{}, fmt.Errorf("invalid type %T for string field", value)
		}
		return protoreflect.ValueOfString(v), nil
	case protoreflect.BytesKind:
		v, ok := value.([]byte)
		if !ok {
			return protoreflect.Value{}, fmt.Errorf("invalid type %T for bytes field", value)
		}
		return protoreflect.ValueOfBytes(v), nil
	default:
		return protoreflect.Value{}, fmt.Errorf("unsupported proto field kind: %v", fd.Kind())
	}
}

// AppendToDefaultStream2 appends data to the default BigQuery stream.
func AppendToDefaultStream2(w io.Writer, projectID, datasetID, tableID string, batch arrow.Record, schema *storagepb.TableSchema) error {
	ctx := context.Background()

	client, err := managedwriter.NewClient(ctx, projectID, managedwriter.WithMultiplexing())
	if err != nil {
		return fmt.Errorf("failed to create managed writer client: %w", err)
	}
	defer client.Close()

	tableReference := managedwriter.TableParentFromParts(projectID, datasetID, tableID)

	// Build the proto descriptor from the BigQuery schema
	descriptor, err := BuildDescriptorFromBQSchema(schema, "TopLevelSchema")
	if err != nil {
		return fmt.Errorf("failed to build proto descriptor: %w", err)
	}

	managedStream, err := client.NewManagedStream(ctx,
		managedwriter.WithType(managedwriter.DefaultStream),
		managedwriter.WithDestinationTable(tableReference),
		managedwriter.WithSchemaDescriptor(descriptor),
	)
	if err != nil {
		return fmt.Errorf("failed to create managed stream: %w", err)
	}
	defer managedStream.Close()

	protoMessages, err := ConvertArrowToProto(batch, descriptor.ProtoReflect().Descriptor())
	if err != nil {
		return fmt.Errorf("failed to convert Arrow to Proto: %w", err)
	}

	serializedRows := make([][]byte, len(protoMessages))
	for i, protoRow := range protoMessages {
		serializedRows[i] = protoRow.ProtoRows.Rows.SerializedRows[0]
	}

	fmt.Fprintf(w, "Attempting to append %d rows\n", len(serializedRows))

	result, err := managedStream.AppendRows(ctx, serializedRows)
	if err != nil {
		return fmt.Errorf("failed to append rows: %w", err)
	}

	fullResponse, respErr := result.FullResponse(ctx)
	if respErr != nil {
		return fmt.Errorf("failed to get full response: %w", respErr)
	}
	b, err := json.MarshalIndent(fullResponse, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal result: %w", err)
	}
	fmt.Printf("Full response: %s\n", b)

	recvOffset, err := result.GetResult(ctx)
	if err != nil {
		handleGRPCError(w, err)
		return fmt.Errorf("failed to get append result: %w", err)
	}

	fmt.Fprintf(w, "Successfully appended data at offset %d.\n", recvOffset)

	return nil
}

// handleGRPCError handles gRPC errors with detailed messages.
func handleGRPCError(w io.Writer, err error) {
	fmt.Fprintf(w, "Error details: %+v\n", err)
	if grpcErr, ok := err.(interface{ GRPCStatus() *status.Status }); ok {
		st := grpcErr.GRPCStatus()
		fmt.Fprintf(w, "gRPC error code: %s\n", st.Code())
		fmt.Fprintf(w, "gRPC error message: %s\n", st.Message())
		for _, detail := range st.Details() {
			if rowErrors, ok := detail.(*storagepb.RowError); ok {
				fmt.Fprintf(w, "Row error - index: %d, message: %s\n", rowErrors.Index, rowErrors.Message)
			}
		}
	}
}

// ArrowRecordToProtoMessage converts an Arrow record to a Protobuf message.
func ArrowRecordToProtoMessage(record arrow.Record, protoMsg proto.Message) error {
	msg := protoMsg.ProtoReflect()

	for i := 0; i < int(record.NumCols()); i++ {
		field := record.Schema().Field(i)
		col := record.Column(i)

		err := setProtoField(msg, field, col)
		if err != nil {
			return err
		}
	}

	return nil
}

// setProtoField sets a field in a Protobuf message from an Arrow column.
func setProtoField(msg protoreflect.Message, field arrow.Field, col arrow.Array) error {
	fd := msg.Descriptor().Fields().ByName(protoreflect.Name(field.Name))
	if fd == nil {
		return nil // Field not in proto, skip
	}

	switch col := col.(type) {
	case *array.String:
		for i := 0; i < col.Len(); i++ {
			if col.IsValid(i) {
				msg.Set(fd, protoreflect.ValueOfString(col.Value(i)))
			}
		}
	case *array.Int64:
		for i := 0; i < col.Len(); i++ {
			if col.IsValid(i) {
				msg.Set(fd, protoreflect.ValueOfInt64(col.Value(i)))
			}
		}
	case *array.Int32:
		for i := 0; i < col.Len(); i++ {
			if col.IsValid(i) {
				msg.Set(fd, protoreflect.ValueOfInt32(col.Value(i)))
			}
		}
	case *array.Float32:
		for i := 0; i < col.Len(); i++ {
			if col.IsValid(i) {
				msg.Set(fd, protoreflect.ValueOfFloat32(col.Value(i)))
			}
		}
	case *array.Float64:
		for i := 0; i < col.Len(); i++ {
			if col.IsValid(i) {
				msg.Set(fd, protoreflect.ValueOfFloat64(col.Value(i)))
			}
		}
	case *array.Boolean:
		for i := 0; i < col.Len(); i++ {
			if col.IsValid(i) {
				msg.Set(fd, protoreflect.ValueOfBool(col.Value(i)))
			}
		}
	case *array.Binary:
		for i := 0; i < col.Len(); i++ {
			if col.IsValid(i) {
				msg.Set(fd, protoreflect.ValueOfBytes(col.Value(i)))
			}
		}
	case *array.Date32:
		for i := 0; i < col.Len(); i++ {
			if col.IsValid(i) {
				msg.Set(fd, protoreflect.ValueOfString(col.Value(i).ToTime().Format("2006-01-02")))
			}
		}
	case *array.Date64:
		for i := 0; i < col.Len(); i++ {
			if col.IsValid(i) {
				msg.Set(fd, protoreflect.ValueOfString(col.Value(i).ToTime().Format("2006-01-02 15:04:05")))
			}
		}
	case *array.Timestamp:
		timeUnit := col.DataType().(*arrow.TimestampType).Unit
		for i := 0; i < col.Len(); i++ {
			if col.IsValid(i) {
				msg.Set(fd, protoreflect.ValueOfString(col.Value(i).ToTime(timeUnit).Format("2006-01-02 15:04:05.999999999")))
			}
		}
	case *array.List:
		listValue := msg.Mutable(fd).List()
		listArray := col.ListValues()
		offsets := col.Offsets()
		for i := 0; i < col.Len(); i++ {
			start, end := offsets[i], offsets[i+1]
			for j := start; j < end; j++ {
				elem := listValue.AppendMutable()
				err := setProtoField(elem.Message(), field.Type.(*arrow.ListType).ElemField(), array.NewSlice(listArray, int64(j), int64(j+1)))
				if err != nil {
					return err
				}
			}
		}
	case *array.Struct:
		structValue := msg.Mutable(fd).Message()
		for j := 0; j < col.NumField(); j++ {
			err := setProtoField(structValue, field.Type.(*arrow.StructType).Field(j), col.Field(j))
			if err != nil {
				return err
			}
		}
	case *array.Map:
		mapValue := msg.Mutable(fd).Map()
		for i := 0; i < col.Len(); i++ {
			if col.IsValid(i) {
				key, err := getValue(col.Keys(), i)
				if err != nil {
					return err
				}
				value, err := getValue(col.Items(), i)
				if err != nil {
					return err
				}
				var mapKey protoreflect.MapKey
				switch k := key.(type) {
				case string:
					mapKey = protoreflect.ValueOfString(k).MapKey()
				case int64:
					mapKey = protoreflect.ValueOfInt64(k).MapKey()
				case int32:
					mapKey = protoreflect.ValueOfInt32(k).MapKey()
				case bool:
					mapKey = protoreflect.ValueOfBool(k).MapKey()
				default:
					return fmt.Errorf("unsupported map key type: %T", key)
				}
				mapValue.Set(mapKey, protoreflect.ValueOf(value))
			}
		}
	case *array.FixedSizeBinary:
		for i := 0; i < col.Len(); i++ {
			if col.IsValid(i) {
				msg.Set(fd, protoreflect.ValueOfBytes(col.Value(i)))
			}
		}
	case *array.Time32:
		for i := 0; i < col.Len(); i++ {
			if col.IsValid(i) {
				msg.Set(fd, protoreflect.ValueOfInt32(int32(col.Value(i))))
			}
		}
	case *array.Time64:
		for i := 0; i < col.Len(); i++ {
			if col.IsValid(i) {
				msg.Set(fd, protoreflect.ValueOfInt64(int64(col.Value(i))))
			}
		}
	default:
		return fmt.Errorf("unsupported Arrow type: %T", col)
	}

	return nil
}

func getValue(arr arrow.Array, index int) (interface{}, error) {
	if !arr.IsValid(index) {
		return nil, nil
	}

	switch arr := arr.(type) {
	case *array.Boolean:
		return arr.Value(index), nil
	case *array.Int32:
		return arr.Value(index), nil
	case *array.Int64:
		return arr.Value(index), nil
	case *array.Uint32:
		return arr.Value(index), nil
	case *array.Uint64:
		return arr.Value(index), nil
	case *array.Float32:
		return arr.Value(index), nil
	case *array.Float64:
		return arr.Value(index), nil
	case *array.String:
		return arr.Value(index), nil
	case *array.Binary:
		return arr.Value(index), nil
	case *array.FixedSizeBinary:
		return arr.Value(index), nil
	case *array.Date32:
		return arr.Value(index).ToTime().Format("2006-01-02"), nil
	case *array.Date64:
		return arr.Value(index).ToTime().Format("2006-01-02 15:04:05"), nil
	case *array.Timestamp:
		timeUnit := arr.DataType().(*arrow.TimestampType).Unit
		return arr.Value(index).ToTime(timeUnit).Format("2006-01-02 15:04:05.999999999"), nil
	default:
		return nil, fmt.Errorf("unsupported Arrow array type: %T", arr)
	}
}
