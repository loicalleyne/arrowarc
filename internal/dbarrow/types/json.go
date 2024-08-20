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

package dbarrow

import (
	"bytes"
	"fmt"
	"reflect"
	"strings"

	"github.com/goccy/go-json"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
)

type JSONBuilder struct {
	*array.ExtensionBuilder
}

func NewJSONBuilder(builder *array.ExtensionBuilder) *JSONBuilder {
	return &JSONBuilder{ExtensionBuilder: builder}
}

func (b *JSONBuilder) AppendBytes(v []byte) {
	if v == nil {
		b.AppendNull()
		return
	}

	b.ExtensionBuilder.Builder.(*array.BinaryBuilder).Append(v)
}

func (b *JSONBuilder) Append(v any) {
	if v == nil {
		b.AppendNull()
		return
	}

	data, err := json.MarshalWithOption(v, json.DisableHTMLEscape())
	if err != nil {
		panic(err)
	}

	b.ExtensionBuilder.Builder.(*array.BinaryBuilder).Append(data)
}

func (b *JSONBuilder) UnsafeAppend(v any) {
	data, err := json.MarshalWithOption(v, json.DisableHTMLEscape())
	if err != nil {
		panic(err)
	}

	b.ExtensionBuilder.Builder.(*array.BinaryBuilder).UnsafeAppend(data)
}

func (b *JSONBuilder) AppendValueFromString(s string) error {
	if s == array.NullValueStr {
		b.AppendNull()
		return nil
	}
	return b.UnmarshalOne(json.NewDecoder(strings.NewReader(s)))
}

func (b *JSONBuilder) AppendValues(v []any, valid []bool) {
	if len(v) != len(valid) && len(valid) != 0 {
		panic("len(v) != len(valid) && len(valid) != 0")
	}

	data := make([][]byte, len(v))
	var err error
	for i := range v {
		if len(valid) > 0 && !valid[i] {
			continue
		}
		data[i], err = json.MarshalWithOption(v[i], json.DisableHTMLEscape())
		if err != nil {
			panic(err)
		}
	}
	b.ExtensionBuilder.Builder.(*array.BinaryBuilder).AppendValues(data, valid)
}

func (b *JSONBuilder) UnmarshalOne(dec *json.Decoder) error {
	var buf any
	err := dec.Decode(&buf)
	if err != nil {
		return err
	}
	if buf == nil {
		b.AppendNull()
	} else {
		b.Append(buf)
	}
	return nil
}

func (b *JSONBuilder) Unmarshal(dec *json.Decoder) error {
	for dec.More() {
		if err := b.UnmarshalOne(dec); err != nil {
			return err
		}
	}
	return nil
}

func (b *JSONBuilder) UnmarshalJSON(data []byte) error {
	dec := json.NewDecoder(bytes.NewReader(data))
	t, err := dec.Token()
	if err != nil {
		return err
	}

	if delim, ok := t.(json.Delim); !ok || delim != '[' {
		return fmt.Errorf("JSON builder must unpack from JSON array, found %s", delim)
	}

	return b.Unmarshal(dec)
}

func (b *JSONBuilder) NewJSONArray() *JSONArray {
	return b.NewExtensionArray().(*JSONArray)
}

// JSONArray is a simple array which is a Binary
type JSONArray struct {
	array.ExtensionArrayBase
}

func (a *JSONArray) String() string {
	arr := a.Storage().(*array.Binary)
	o := new(strings.Builder)
	o.WriteString("[")
	for i := 0; i < arr.Len(); i++ {
		if i > 0 {
			o.WriteString(" ")
		}
		switch {
		case a.IsNull(i):
			o.WriteString(array.NullValueStr)
		default:
			fmt.Fprintf(o, "%q", a.ValueStr(i))
		}
	}
	o.WriteString("]")
	return o.String()
}

func (a *JSONArray) Value(i int) any {
	if a.IsNull(i) {
		return nil
	}

	var data any
	err := json.Unmarshal(a.Storage().(*array.Binary).Value(i), &data)
	if err != nil {
		panic(fmt.Errorf("invalid json: %w", err))
	}
	return data
}

func (a *JSONArray) ValueStr(i int) string {
	switch {
	case a.IsNull(i):
		return array.NullValueStr
	default:
		return string(a.GetOneForMarshal(i).(json.RawMessage))
	}
}

func (a *JSONArray) MarshalJSON() ([]byte, error) {
	values := make([]json.RawMessage, a.Len())
	for i := 0; i < a.Len(); i++ {
		if a.IsNull(i) {
			continue
		}
		values[i] = a.GetOneForMarshal(i).(json.RawMessage)
	}
	return json.MarshalWithOption(values, json.DisableHTMLEscape())
}

func (a *JSONArray) GetOneForMarshal(i int) any {
	if a.IsNull(i) {
		return nil
	}
	return json.RawMessage(a.Storage().(*array.Binary).Value(i))
}

type JSONType struct {
	arrow.ExtensionBase
}

func NewJSONType() *JSONType {
	return &JSONType{ExtensionBase: arrow.ExtensionBase{Storage: &arrow.BinaryType{}}}
}

func (*JSONType) ArrayType() reflect.Type {
	return reflect.TypeOf(JSONArray{})
}

func (*JSONType) ExtensionName() string {
	return "json"
}

func (*JSONType) String() string {
	return "json"
}

func (e *JSONType) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf(`{"name":"%s","metadata":%s}`, e.ExtensionName(), e.Serialize())), nil
}

func (*JSONType) Serialize() string {
	return "json-serialized"
}

func (*JSONType) Deserialize(storageType arrow.DataType, data string) (arrow.ExtensionType, error) {
	if data != "json-serialized" {
		return nil, fmt.Errorf("type identifier did not match: '%s'", data)
	}
	if !arrow.TypeEqual(storageType, &arrow.BinaryType{}) {
		return nil, fmt.Errorf("invalid storage type for *JSONType: %s", storageType.Name())
	}
	return NewJSONType(), nil
}

func (e *JSONType) ExtensionEquals(other arrow.ExtensionType) bool {
	return e.ExtensionName() == other.ExtensionName()
}

func (*JSONType) NewBuilder(bldr *array.ExtensionBuilder) array.Builder {
	return NewJSONBuilder(bldr)
}
