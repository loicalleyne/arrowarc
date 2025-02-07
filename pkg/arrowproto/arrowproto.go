package arrowproto

import (
	"errors"
	"fmt"
	"math"
	"reflect"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	commonv1 "go.opentelemetry.io/proto/otlp/common/v1"
	"golang.org/x/exp/constraints"
	"google.golang.org/genproto/googleapis/type/date"
	"google.golang.org/genproto/googleapis/type/timeofday"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/runtime/protoimpl"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Constants and error messages
const (
	maxDepth = 10
	Unknown  = "@unknown"
)

var (
	ErrMaxDepth       = errors.New("max depth reached, either the message is deeply nested or a circular dependency was introduced")
	otelAnyDescriptor = (&commonv1.AnyValue{}).ProtoReflect().Descriptor()
)

// Helper types
type valueFn func(protoreflect.Value, bool) error
type encodeFn func(value protoreflect.Value, a arrow.Array, row int) protoreflect.Value

// Node represents a mapping between proto fields and Arrow schema.
type node struct {
	parent   *node
	field    arrow.Field
	setup    func(array.Builder) valueFn
	write    valueFn
	desc     protoreflect.Descriptor
	children []*node
	encode   encodeFn
	hash     map[string]*node
}

// A Message is a template to apply to a message.
type Message map[protoreflect.Name]Value

// ConvertArrowRecordToProtoMessages converts an Apache Arrow Record to a list of Proto messages.
func ConvertArrowRecordToProtoMessages(record arrow.Record, messageType proto.Message) ([]proto.Message, error) {
	if record == nil {
		return nil, errors.New("arrow record is nil")
	}

	numRows := int(record.NumRows())
	messages := make([]proto.Message, numRows)

	for i := 0; i < numRows; i++ {
		msg := proto.Clone(messageType)
		if err := unmarshalRow(record, i, msg.ProtoReflect()); err != nil {
			return nil, fmt.Errorf("error unmarshaling row %d: %w", i, err)
		}
		messages[i] = msg
	}

	return messages, nil
}

// unmarshalRow processes each row in an Arrow Record and fills the corresponding fields in a proto message.
func unmarshalRow(record arrow.Record, row int, msg protoreflect.Message) error {
	fields := msg.Descriptor().Fields()
	for i := 0; i < fields.Len(); i++ {
		fd := fields.Get(i)
		colIndex := record.Schema().FieldIndices(string(fd.Name()))
		if len(colIndex) == 0 {
			continue
		}

		col := record.Column(colIndex[0])
		if col.IsNull(row) {
			continue
		}

		value, err := getArrowValue(col, row, fd)
		if err != nil {
			return fmt.Errorf("error getting value for field %s: %w", fd.Name(), err)
		}

		setProtoField(msg, fd, value)
	}

	handleExtensions(record, row, msg)
	return nil
}

// handleExtensions processes extensions for a proto message.
func handleExtensions(record arrow.Record, row int, msg protoreflect.Message) {
	protoregistry.GlobalTypes.RangeExtensionsByMessage(msg.Descriptor().FullName(), func(xt protoreflect.ExtensionType) bool {
		xd := xt.TypeDescriptor()
		colIndex := record.Schema().FieldIndices(string(xd.Name()))
		if len(colIndex) == 0 {
			return true
		}

		col := record.Column(colIndex[0])
		if col.IsNull(row) {
			return true
		}

		value, err := getArrowValue(col, row, xd)
		if err != nil {
			return true
		}

		msg.Set(xd, protoreflect.ValueOf(value))
		return true
	})
}

// setProtoField sets the value of a field in a proto message based on its descriptor.
func setProtoField(msg protoreflect.Message, fd protoreflect.FieldDescriptor, value interface{}) {
	switch {
	case fd.IsList():
		list := msg.Mutable(fd).List()
		listValue := reflect.ValueOf(value)
		for j := 0; j < listValue.Len(); j++ {
			list.Append(protoreflect.ValueOf(listValue.Index(j).Interface()))
		}
	case fd.IsMap():
		mapValue := msg.Mutable(fd).Map()
		mapReflect := reflect.ValueOf(value)
		for _, key := range mapReflect.MapKeys() {
			mapKey := fieldValue(fd.MapKey(), key.Interface())
			mapVal := fieldValue(fd.MapValue(), mapReflect.MapIndex(key).Interface())
			mapValue.Set(mapKey.MapKey(), mapVal)
		}
	default:
		msg.Set(fd, protoreflect.ValueOf(value))
	}
}

// getArrowValue converts an Arrow column value to the corresponding Go type compatible with Proto field.
func getArrowValue(col arrow.Array, row int, fd protoreflect.FieldDescriptor) (interface{}, error) {
	switch arr := col.(type) {
	case *array.Boolean:
		return arr.Value(row), nil
	case *array.Int8:
		return int32(arr.Value(row)), nil
	case *array.Int16:
		return int32(arr.Value(row)), nil
	case *array.Int32, *array.Int64, *array.Uint8, *array.Uint16, *array.Uint32, *array.Uint64, *array.Float32, *array.Float64, *array.String, *array.Binary:
		return arr.(interface{ Value(int) interface{} }).Value(row), nil
	case *array.Timestamp:
		return timestampToProto(arr, row, fd)
	case *array.Date32:
		return dateToProto32(arr, row)
	case *array.Date64:
		return dateToProto64(arr, row)
	case *array.Time32:
		return time32ToProto(arr, row)
	case *array.Time64:
		return time64ToProto(arr, row)
	case *array.List:
		return getListValue(arr, row, fd)
	case *array.Struct:
		return getStructValue(arr, row, fd)
	case *array.Map:
		return getMapValue(arr, row, fd)
	default:
		return nil, fmt.Errorf("unsupported Arrow type: %T", col)
	}
}

// Helper functions for handling specific types

func getMapValue(arr *array.Map, row int, fd protoreflect.FieldDescriptor) (interface{}, error) {
	start, end := arr.ValueOffsets(row)
	mapValue := make(map[interface{}]interface{}, end-start)
	for i := start; i < end; i++ {
		key, err := getArrowValue(arr.Keys(), int(i), fd.MapKey())
		if err != nil {
			return nil, fmt.Errorf("error converting map key for field %s: %w", fd.Name(), err)
		}
		value, err := getArrowValue(arr.Items(), int(i), fd.MapValue())
		if err != nil {
			return nil, fmt.Errorf("error converting map value for field %s: %w", fd.Name(), err)
		}
		mapValue[key] = value
	}
	return mapValue, nil
}

func timestampToProto(arr *array.Timestamp, row int, fd protoreflect.FieldDescriptor) (*timestamppb.Timestamp, error) {
	ts := arr.Value(row)
	t := ts.ToTime(arr.DataType().(*arrow.TimestampType).Unit)
	return timestamppb.New(t), nil
}

func dateToProto32(arr *array.Date32, row int) (*date.Date, error) {
	dateVal := arr.Value(row).ToTime()
	return &date.Date{
		Year:  int32(dateVal.Year()),
		Month: int32(dateVal.Month()),
		Day:   int32(dateVal.Day()),
	}, nil
}

func dateToProto64(arr *array.Date64, row int) (*date.Date, error) {
	dateVal := arr.Value(row).ToTime()
	return &date.Date{
		Year:  int32(dateVal.Year()),
		Month: int32(dateVal.Month()),
		Day:   int32(dateVal.Day()),
	}, nil
}

func time32ToProto(arr *array.Time32, row int) (*timeofday.TimeOfDay, error) {
	timeVal := arr.Value(row)
	millis := int64(timeVal)
	return &timeofday.TimeOfDay{
		Hours:   int32(millis / 3600000),
		Minutes: int32((millis % 3600000) / 60000),
		Seconds: int32((millis % 60000) / 1000),
		Nanos:   int32((millis % 1000) * 1000000),
	}, nil
}

func time64ToProto(arr *array.Time64, row int) (*timestamppb.Timestamp, error) {
	timeVal := arr.Value(row)
	nanos := int64(timeVal)
	return &timestamppb.Timestamp{
		Seconds: nanos / 1000000000,
		Nanos:   int32(nanos % 1000000000),
	}, nil
}

func getListValue(arr *array.List, row int, fd protoreflect.FieldDescriptor) (interface{}, error) {
	start, end := arr.ValueOffsets(row)
	values := make([]interface{}, end-start)
	for i := start; i < end; i++ {
		value, err := getArrowValue(arr.ListValues(), int(i), fd.Message().Fields().ByNumber(1))
		if err != nil {
			return nil, err
		}
		values[i-start] = value
	}
	return values, nil
}

func getStructValue(arr *array.Struct, row int, fd protoreflect.FieldDescriptor) (interface{}, error) {
	msgType := protoimpl.X.MessageTypeOf(fd.Message().FullName())
	msg := msgType.New().Interface()

	for i := 0; i < arr.NumField(); i++ {
		fieldArr := arr.Field(i)
		fieldDesc := fd.Message().Fields().ByNumber(protoreflect.FieldNumber(i + 1))
		value, err := getArrowValue(fieldArr, row, fieldDesc)
		if err != nil {
			return nil, fmt.Errorf("error converting Arrow value for field %s: %w", fieldDesc.Name(), err)
		}
		setProtoField(msg.ProtoReflect(), fieldDesc, value)
	}

	return msg, nil
}

// Simplified functions
func fieldValue(fd protoreflect.FieldDescriptor, v any) protoreflect.Value {
	switch o := v.(type) {
	case int:
		return handleIntField(fd, o)
	case float64:
		return handleFloatField(fd, o)
	case string:
		return handleStringField(fd, o)
	case []byte:
		if fd.Kind() != protoreflect.BytesKind {
			panic(fmt.Sprintf("%v: unsupported type []byte for kind %v", fd.FullName(), fd.Kind()))
		}
		return protoreflect.ValueOf(append([]byte{}, o...))
	default:
		panic(fmt.Sprintf("%v: unsupported value type %T for kind %v", fd.FullName(), v, fd.Kind()))
	}
}

// Helper functions for fieldValue
func handleIntField(fd protoreflect.FieldDescriptor, o int) protoreflect.Value {
	switch fd.Kind() {
	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind:
		return checkRange(fd, int32(o), math.MinInt32, math.MaxInt32)
	case protoreflect.Uint32Kind, protoreflect.Fixed32Kind:
		return checkRange(fd, uint32(o), 0, math.MaxUint32)
	case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
		return protoreflect.ValueOf(int64(o))
	case protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		if o < 0 {
			panic(fmt.Sprintf("%v: value %v out of range [%v, %v]", fd.FullName(), o, uint64(0), uint64(math.MaxUint64)))
		}
		return protoreflect.ValueOf(uint64(o))
	case protoreflect.FloatKind:
		return protoreflect.ValueOf(float32(o))
	case protoreflect.DoubleKind:
		return protoreflect.ValueOf(float64(o))
	case protoreflect.EnumKind:
		return protoreflect.ValueOf(protoreflect.EnumNumber(o))
	default:
		panic(fmt.Sprintf("%v: unsupported type int for kind %v", fd.FullName(), fd.Kind()))
	}
}

func handleFloatField(fd protoreflect.FieldDescriptor, o float64) protoreflect.Value {
	switch fd.Kind() {
	case protoreflect.FloatKind:
		if o < -math.MaxFloat32 || o > math.MaxFloat32 {
			panic(fmt.Sprintf("%v: value %v out of range for float32", fd.FullName(), o))
		}
		return protoreflect.ValueOf(float32(o))
	case protoreflect.DoubleKind:
		return protoreflect.ValueOf(o)
	default:
		panic(fmt.Sprintf("%v: unsupported type float64 for kind %v", fd.FullName(), fd.Kind()))
	}
}

func handleStringField(fd protoreflect.FieldDescriptor, o string) protoreflect.Value {
	switch fd.Kind() {
	case protoreflect.BytesKind:
		return protoreflect.ValueOf([]byte(o))
	case protoreflect.EnumKind:
		enumVal := fd.Enum().Values().ByName(protoreflect.Name(o))
		if enumVal == nil {
			panic(fmt.Sprintf("%v: invalid enum value %v", fd.FullName(), o))
		}
		return protoreflect.ValueOf(enumVal.Number())
	default:
		panic(fmt.Sprintf("%v: unsupported type string for kind %v", fd.FullName(), fd.Kind()))
	}
}

func checkRange[T constraints.Ordered](fd protoreflect.FieldDescriptor, val, min, max T) protoreflect.Value {
	if val < min || val > max {
		panic(fmt.Sprintf("%v: value %v out of range [%v, %v]", fd.FullName(), val, min, max))
	}
	return protoreflect.ValueOf(val)
}

type message struct {
	root    *node
	schema  *arrow.Schema
	builder *array.RecordBuilder
}

// A Value is a value assignable to a field.
type Value any

// Build applies the template to a message.
func (template Message) Build(m protoreflect.Message) {
	md := m.Descriptor()
	fields := md.Fields()
	exts := make(map[protoreflect.Name]protoreflect.FieldDescriptor)
	protoregistry.GlobalTypes.RangeExtensionsByMessage(md.FullName(), func(xt protoreflect.ExtensionType) bool {
		xd := xt.TypeDescriptor()
		exts[xd.Name()] = xd
		return true
	})
	for k, v := range template {
		if k == Unknown {
			m.SetUnknown(protoreflect.RawFields(v.([]byte)))
			continue
		}
		fd := fields.ByName(k)
		if fd == nil {
			fd = exts[k]
		}
		if fd == nil {
			panic(fmt.Sprintf("%v.%v: not found", md.FullName(), k))
		}
		assignField(m, fd, v)
	}
}

func assignField(m protoreflect.Message, fd protoreflect.FieldDescriptor, v any) {
	switch {
	case fd.IsList():
		list := m.Mutable(fd).List()
		s := reflect.ValueOf(v)
		for i := 0; i < s.Len(); i++ {
			if fd.Message() == nil {
				list.Append(fieldValue(fd, s.Index(i).Interface()))
			} else {
				e := list.NewElement()
				s.Index(i).Interface().(Message).Build(e.Message())
				list.Append(e)
			}
		}
	case fd.IsMap():
		mapv := m.Mutable(fd).Map()
		rm := reflect.ValueOf(v)
		for _, k := range rm.MapKeys() {
			mk := fieldValue(fd.MapKey(), k.Interface()).MapKey()
			if fd.MapValue().Message() == nil {
				mv := fieldValue(fd.MapValue(), rm.MapIndex(k).Interface())
				mapv.Set(mk, mv)
			} else if mapv.Has(mk) {
				mv := mapv.Get(mk).Message()
				rm.MapIndex(k).Interface().(Message).Build(mv)
			} else {
				mv := mapv.NewValue()
				rm.MapIndex(k).Interface().(Message).Build(mv.Message())
				mapv.Set(mk, mv)
			}
		}
	default:
		if fd.Message() == nil {
			m.Set(fd, fieldValue(fd, v))
		} else {
			v.(Message).Build(m.Mutable(fd).Message())
		}
	}
}

func unmarshal[T proto.Message](n *node, r arrow.Record, rows []int) []T {
	if rows == nil {
		rows = make([]int, r.NumRows())
		for i := range rows {
			rows[i] = i
		}
	}
	o := make([]T, len(rows))
	var a T
	ref := a.ProtoReflect()
	for idx, row := range rows {
		msg := ref.New()
		for i := 0; i < int(r.NumCols()); i++ {
			name := r.ColumnName(i)
			nx, ok := n.hash[name]
			if !ok {
				panic(fmt.Sprintf("field %s not found in node %v", name, n.field.Name))
			}
			if r.Column(i).IsNull(row) {
				continue
			}
			fs := nx.desc.(protoreflect.FieldDescriptor)
			switch {
			case fs.IsList():
				ls := r.Column(i).(*array.List)
				start, end := ls.ValueOffsets(row)
				val := ls.ListValues()
				if start != end {
					lv := msg.NewField(fs)
					list := lv.List()
					for k := start; k < end; k++ {
						list.Append(nx.encode(list.NewElement(), val, int(k)))
					}
					msg.Set(fs, lv)
				}
			case fs.IsMap():
				panic("MAP not supported")
			default:
				msg.Set(fs, nx.encode(msg.NewField(fs), r.Column(i), row))
			}
		}
		o[idx] = msg.Interface().(T)
	}
	return o
}

func build(msg protoreflect.Message) *message {
	root := &node{
		desc:  msg.Descriptor(),
		field: arrow.Field{},
		hash:  make(map[string]*node),
	}
	fields := msg.Descriptor().Fields()
	root.children = make([]*node, fields.Len())
	a := make([]arrow.Field, fields.Len())
	for i := 0; i < fields.Len(); i++ {
		x := createNode(root, fields.Get(i), 0)
		root.children[i] = x
		root.hash[x.field.Name] = x
		a[i] = root.children[i].field
	}
	as := arrow.NewSchema(a, nil)

	return &message{
		root:   root,
		schema: as,
	}
}

func (m *message) build(mem memory.Allocator) {
	b := array.NewRecordBuilder(mem, m.schema)
	for i, ch := range m.root.children {
		ch.build(b.Field(i))
	}
	m.builder = b
}

func (m *message) append(msg protoreflect.Message) {
	m.root.WriteMessage(msg)
}

func (m *message) NewRecord() arrow.Record {
	return m.builder.NewRecord()
}

func createNode(parent *node, field protoreflect.FieldDescriptor, depth int) *node {
	if depth >= maxDepth {
		panic(ErrMaxDepth)
	}
	name, ok := parent.field.Metadata.GetValue("path")
	if ok {
		name += "." + string(field.Name())
	} else {
		name = string(field.Name())
	}
	n := &node{
		parent: parent,
		desc:   field,
		field: arrow.Field{
			Name:     string(field.Name()),
			Nullable: nullable(field),
			Metadata: arrow.MetadataFrom(map[string]string{
				"path": name,
			}),
		},
		hash: make(map[string]*node),
	}
	t, err := n.baseType(field)
	if err != nil {
		panic(err)
	}
	n.field.Type = t

	if n.field.Type != nil {
		return n
	}

	// Handling messages
	if msg := field.Message(); msg != nil {
		switch msg {
		case otelAnyDescriptor:
			n.field.Type = arrow.BinaryTypes.Binary
			n.field.Nullable = true
			n.setup = func(b array.Builder) valueFn {
				a := b.(*array.BinaryBuilder)
				return func(v protoreflect.Value, set bool) error {
					if !v.IsValid() {
						a.AppendNull()
						return nil
					}
					e := v.Message().Interface().(*commonv1.AnyValue)
					bs, err := proto.Marshal(e)
					if err != nil {
						return err
					}
					a.Append(bs)
					return nil
				}
			}
			n.encode = func(value protoreflect.Value, a arrow.Array, row int) protoreflect.Value {
				if a.IsNull(row) {
					return protoreflect.Value{}
				}
				msg := value.Message()
				v := a.(*array.Binary).Value(row)
				proto.Unmarshal(v, msg.Interface())
				return value
			}
		}

		if n.field.Type != nil {
			if field.IsList() {
				n.field.Type = arrow.ListOf(n.field.Type)
				setup := n.setup
				n.setup = func(b array.Builder) valueFn {
					ls := b.(*array.ListBuilder)
					value := setup(ls.ValueBuilder())
					return func(v protoreflect.Value, set bool) error {
						if !v.IsValid() {
							ls.AppendNull()
							return nil
						}
						ls.Append(true)
						list := v.List()
						for i := 0; i < list.Len(); i++ {
							if err := value(list.Get(i), true); err != nil {
								return err
							}
						}
						return nil
					}
				}
			}
			return n
		}
	}

	// Further processing of fields
	f := field.Message().Fields()
	n.children = make([]*node, f.Len())
	a := make([]arrow.Field, f.Len())
	for i := 0; i < f.Len(); i++ {
		x := createNode(n, f.Get(i), depth+1)
		n.children[i] = x
		n.hash[x.field.Name] = x
		a[i] = n.children[i].field
	}
	n.field.Type = arrow.StructOf(a...)
	n.field.Nullable = true
	n.setup = func(b array.Builder) valueFn {
		a := b.(*array.StructBuilder)
		fs := make([]valueFn, len(n.children))
		for i := range n.children {
			fs[i] = n.children[i].setup(a.FieldBuilder(i))
		}
		return func(v protoreflect.Value, set bool) error {
			if !v.IsValid() {
				a.AppendNull()
				return nil
			}
			a.Append(true)
			msg := v.Message()
			fields := msg.Descriptor().Fields()
			for i := 0; i < fields.Len(); i++ {
				if err := fs[i](msg.Get(fields.Get(i)), msg.Has(fields.Get(i))); err != nil {
					return err
				}
			}
			return nil
		}
	}
	n.encode = func(value protoreflect.Value, a arrow.Array, row int) protoreflect.Value {
		msg := value.Message()
		s := a.(*array.Struct)
		typ := a.DataType().(*arrow.StructType)
		for j := 0; j < s.NumField(); j++ {
			f := typ.Field(j)
			nx, ok := n.hash[f.Name]
			if !ok {
				panic(fmt.Sprintf("field %s not found in node %v", f.Name, n.field.Name))
			}
			if s.Field(j).IsNull(row) {
				continue
			}
			fs := nx.desc.(protoreflect.FieldDescriptor)
			switch {
			case fs.IsList():
				ls := s.Field(j).(*array.List)
				start, end := ls.ValueOffsets(row)
				if start != end {
					lv := msg.Mutable(fs)
					list := lv.List()
					va := ls.ListValues()
					for k := start; k < end; k++ {
						list.Append(nx.encode(list.NewElement(), va, int(k)))
					}
					msg.Set(fs, lv)
				}
			case fs.IsMap():
				panic("MAP not supported")
			default:
				msg.Set(fs, nx.encode(msg.NewField(fs), s.Field(j), row))
			}
		}
		return value
	}
	if field.IsList() {
		n.field.Type = arrow.ListOf(n.field.Type)
		setup := n.setup
		n.setup = func(b array.Builder) valueFn {
			ls := b.(*array.ListBuilder)
			value := setup(ls.ValueBuilder())
			return func(v protoreflect.Value, set bool) error {
				if !v.IsValid() {
					ls.AppendNull()
					return nil
				}
				ls.Append(true)
				list := v.List()
				for i := 0; i < list.Len(); i++ {
					if err := value(list.Get(i), true); err != nil {
						return err
					}
				}
				return nil
			}
		}
	}
	if field.ContainingOneof() != nil {
		setup := n.setup
		n.setup = func(b array.Builder) valueFn {
			do := setup(b)
			return func(v protoreflect.Value, set bool) error {
				if !set {
					b.AppendNull()
					return nil
				}
				return do(v, set)
			}
		}
	}
	return n
}

func (n *node) build(a array.Builder) {
	n.write = n.setup(a)
}

func (n *node) WriteMessage(msg protoreflect.Message) {
	f := msg.Descriptor().Fields()
	for i := 0; i < f.Len(); i++ {
		n.children[i].write(msg.Get(f.Get(i)), msg.Has(f.Get(i)))
	}
}

// baseType converts a protobuf field descriptor to an equivalent Arrow data type.
// It returns (arrow.DataType, error) where the error is non-nil for unsupported types.
func (n *node) baseType(field protoreflect.FieldDescriptor) (arrow.DataType, error) {
	switch field.Kind() {
	case protoreflect.BoolKind:
		return arrow.FixedWidthTypes.Boolean, nil
	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind:
		return arrow.PrimitiveTypes.Int32, nil
	case protoreflect.Uint32Kind, protoreflect.Fixed32Kind:
		return arrow.PrimitiveTypes.Uint32, nil
	case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
		return arrow.PrimitiveTypes.Int64, nil
	case protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		return arrow.PrimitiveTypes.Uint64, nil
	case protoreflect.FloatKind:
		return arrow.PrimitiveTypes.Float32, nil
	case protoreflect.DoubleKind:
		return arrow.PrimitiveTypes.Float64, nil
	case protoreflect.StringKind:
		return arrow.BinaryTypes.String, nil
	case protoreflect.BytesKind:
		return arrow.BinaryTypes.Binary, nil
	case protoreflect.EnumKind:
		return arrow.PrimitiveTypes.Int32, nil // Enums are typically represented as int32
	case protoreflect.MessageKind:
		if field.Message() == otelAnyDescriptor {
			return arrow.BinaryTypes.Binary, nil
		}
		return nil, nil // Allow complex message types to be handled elsewhere
	case protoreflect.GroupKind:
		// Groups are deprecated in proto3, but we'll handle them as nested structs
		return nil, nil
	default:
		return nil, fmt.Errorf("unsupported protobuf kind: %v", field.Kind())
	}
}

func nullable(f protoreflect.FieldDescriptor) bool {
	return f.HasOptionalKeyword() || f.ContainingOneof() != nil || f.Kind() == protoreflect.BytesKind
}
