package experiments

import (
	"fmt"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/bigquery/storage/apiv1/storagepb"
)

var fieldTypeMap = map[bigquery.FieldType]storagepb.TableFieldSchema_Type{
	bigquery.StringFieldType:     storagepb.TableFieldSchema_STRING,
	bigquery.BytesFieldType:      storagepb.TableFieldSchema_BYTES,
	bigquery.IntegerFieldType:    storagepb.TableFieldSchema_INT64,
	bigquery.FloatFieldType:      storagepb.TableFieldSchema_DOUBLE,
	bigquery.BooleanFieldType:    storagepb.TableFieldSchema_BOOL,
	bigquery.TimestampFieldType:  storagepb.TableFieldSchema_TIMESTAMP,
	bigquery.RecordFieldType:     storagepb.TableFieldSchema_STRUCT,
	bigquery.DateFieldType:       storagepb.TableFieldSchema_DATE,
	bigquery.TimeFieldType:       storagepb.TableFieldSchema_TIME,
	bigquery.DateTimeFieldType:   storagepb.TableFieldSchema_DATETIME,
	bigquery.NumericFieldType:    storagepb.TableFieldSchema_NUMERIC,
	bigquery.BigNumericFieldType: storagepb.TableFieldSchema_BIGNUMERIC,
	bigquery.GeographyFieldType:  storagepb.TableFieldSchema_GEOGRAPHY,
	bigquery.RangeFieldType:      storagepb.TableFieldSchema_RANGE,
	bigquery.JSONFieldType:       storagepb.TableFieldSchema_JSON,
}

func bqFieldToProto(in *bigquery.FieldSchema) (*storagepb.TableFieldSchema, error) {
	if in == nil {
		return nil, nil
	}
	out := &storagepb.TableFieldSchema{
		Name:        in.Name,
		Description: in.Description,
	}

	// Type conversion.
	typ, ok := fieldTypeMap[in.Type]
	if !ok {
		return nil, fmt.Errorf("could not convert field (%s) due to unknown type value: %s", in.Name, in.Type)
	}
	out.Type = typ

	// Mode conversion.  Repeated trumps required.
	out.Mode = storagepb.TableFieldSchema_NULLABLE
	if in.Repeated {
		out.Mode = storagepb.TableFieldSchema_REPEATED
	}
	if !in.Repeated && in.Required {
		out.Mode = storagepb.TableFieldSchema_REQUIRED
	}

	if in.RangeElementType != nil {
		eleType, ok := fieldTypeMap[in.RangeElementType.Type]
		if !ok {
			return nil, fmt.Errorf("could not convert rante element type in %s: %q", in.Name, in.Type)
		}
		out.RangeElementType = &storagepb.TableFieldSchema_FieldElementType{
			Type: eleType,
		}
	}

	for _, s := range in.Schema {
		subField, err := bqFieldToProto(s)
		if err != nil {
			return nil, err
		}
		out.Fields = append(out.Fields, subField)
	}
	return out, nil
}

func protoToBQField(in *storagepb.TableFieldSchema) (*bigquery.FieldSchema, error) {
	if in == nil {
		return nil, nil
	}
	out := &bigquery.FieldSchema{
		Name:        in.GetName(),
		Description: in.GetDescription(),
		Repeated:    in.GetMode() == storagepb.TableFieldSchema_REPEATED,
		Required:    in.GetMode() == storagepb.TableFieldSchema_REQUIRED,
	}

	typeResolved := false
	for k, v := range fieldTypeMap {
		if v == in.GetType() {
			out.Type = k
			typeResolved = true
			break
		}
	}
	if !typeResolved {
		return nil, fmt.Errorf("could not convert proto type to bigquery type: %v", in.GetType().String())
	}

	if in.GetRangeElementType() != nil {
		eleType := in.GetRangeElementType().GetType()
		ret := &bigquery.RangeElementType{}
		typeResolved := false
		for k, v := range fieldTypeMap {
			if v == eleType {
				ret.Type = k
				typeResolved = true
				break
			}
		}
		if !typeResolved {
			return nil, fmt.Errorf("could not convert proto range element type to bigquery type: %v", eleType.String())
		}
		out.RangeElementType = ret
	}

	for _, s := range in.Fields {
		subField, err := protoToBQField(s)
		if err != nil {
			return nil, err
		}
		out.Schema = append(out.Schema, subField)
	}
	return out, nil
}

// BQSchemaToStorageTableSchema converts a bigquery Schema into the protobuf-based TableSchema used
// by the BigQuery Storage WriteClient.
func BQSchemaToStorageTableSchema(in bigquery.Schema) (*storagepb.TableSchema, error) {
	if in == nil {
		return nil, nil
	}
	out := &storagepb.TableSchema{}
	for _, s := range in {
		converted, err := bqFieldToProto(s)
		if err != nil {
			return nil, err
		}
		out.Fields = append(out.Fields, converted)
	}
	return out, nil
}

// StorageTableSchemaToBQSchema converts a TableSchema from the BigQuery Storage WriteClient
// into the equivalent BigQuery Schema.
func StorageTableSchemaToBQSchema(in *storagepb.TableSchema) (bigquery.Schema, error) {
	if in == nil {
		return nil, nil
	}
	var out bigquery.Schema
	for _, s := range in.Fields {
		converted, err := protoToBQField(s)
		if err != nil {
			return nil, err
		}
		out = append(out, converted)
	}
	return out, nil
}
