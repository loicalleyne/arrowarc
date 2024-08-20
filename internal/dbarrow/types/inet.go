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
	"net"
	"reflect"
	"strings"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/goccy/go-json"
)

type InetBuilder struct {
	*array.ExtensionBuilder
}

func NewInetBuilder(builder *array.ExtensionBuilder) *InetBuilder {
	return &InetBuilder{ExtensionBuilder: builder}
}

func (b *InetBuilder) Append(v *net.IPNet) {
	if v == nil {
		b.AppendNull()
		return
	}
	b.ExtensionBuilder.Builder.(*array.BinaryBuilder).Append([]byte(v.String()))
}

func (b *InetBuilder) UnsafeAppend(v *net.IPNet) {
	b.ExtensionBuilder.Builder.(*array.BinaryBuilder).UnsafeAppend([]byte(v.String()))
}

func (b *InetBuilder) AppendValues(v []*net.IPNet, valid []bool) {
	if len(v) != len(valid) && len(valid) != 0 {
		panic("len(v) != len(valid) && len(valid) != 0")
	}

	data := make([][]byte, len(v))
	for i, v := range v {
		if len(valid) > 0 && !valid[i] {
			continue
		}
		data[i] = []byte(v.String())
	}
	b.ExtensionBuilder.Builder.(*array.BinaryBuilder).AppendValues(data, valid)
}

func (b *InetBuilder) AppendValueFromString(s string) error {
	if s == array.NullValueStr {
		b.AppendNull()
		return nil
	}
	_, data, err := net.ParseCIDR(s)
	if err != nil {
		return err
	}
	b.Append(data)
	return nil
}

func (b *InetBuilder) UnmarshalOne(dec *json.Decoder) error {
	t, err := dec.Token()
	if err != nil {
		return err
	}

	var val *net.IPNet
	var ip net.IP
	switch v := t.(type) {
	case string:
		ip, val, err = net.ParseCIDR(v)
		if err != nil {
			return err
		}
		val.IP = ip
	case []byte:
		ip, val, err = net.ParseCIDR(string(v))
		if err != nil {
			return err
		}
		val.IP = ip
	case nil:
		b.AppendNull()
		return nil
	default:
		return &json.UnmarshalTypeError{
			Value:  fmt.Sprint(t),
			Type:   reflect.TypeOf([]byte{}),
			Offset: dec.InputOffset(),
			Struct: "String",
		}
	}

	b.Append(val)
	return nil
}

func (b *InetBuilder) Unmarshal(dec *json.Decoder) error {
	for dec.More() {
		if err := b.UnmarshalOne(dec); err != nil {
			return err
		}
	}
	return nil
}

func (b *InetBuilder) UnmarshalJSON(data []byte) error {
	dec := json.NewDecoder(bytes.NewReader(data))
	t, err := dec.Token()
	if err != nil {
		return err
	}

	if delim, ok := t.(json.Delim); !ok || delim != '[' {
		return fmt.Errorf("fixed size binary builder must unpack from json array, found %s", delim)
	}

	return b.Unmarshal(dec)
}

func (b *InetBuilder) NewInetArray() *InetArray {
	return b.NewExtensionArray().(*InetArray)
}

type InetArray struct {
	array.ExtensionArrayBase
}

func (a *InetArray) String() string {
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

func (a *InetArray) Value(i int) *net.IPNet {
	if a.IsNull(i) {
		return nil
	}
	cidr := string(a.Storage().(*array.Binary).Value(i))
	if len(cidr) == 0 {
		return &net.IPNet{
			IP:   net.IPv4zero,
			Mask: make(net.IPMask, len(net.IPv4zero)),
		}
	}
	ip, ipnet, err := net.ParseCIDR(cidr)
	if err != nil {
		panic(fmt.Errorf("invalid ip+net: %w", err))
	}
	ipnet.IP = ip

	return ipnet
}

func (a *InetArray) ValueStr(i int) string {
	switch {
	case a.IsNull(i):
		return array.NullValueStr
	default:
		return a.Value(i).String()
	}
}

func (a *InetArray) GetOneForMarshal(i int) any {
	if a.IsNull(i) {
		return nil
	}
	return string(a.Storage().(*array.Binary).Value(i))
}

func (a *InetArray) MarshalJSON() ([]byte, error) {
	arr := a.Storage().(*array.Binary)
	values := make([]any, a.Len())
	for i := 0; i < a.Len(); i++ {
		if a.IsValid(i) {
			values[i] = string(arr.Value(i))
		} else {
			values[i] = nil
		}
	}
	return json.Marshal(values)
}

type InetType struct {
	arrow.ExtensionBase
}

func NewInetType() *InetType {
	return &InetType{ExtensionBase: arrow.ExtensionBase{Storage: &arrow.BinaryType{}}}
}

func (*InetType) ArrayType() reflect.Type {
	return reflect.TypeOf(InetArray{})
}

func (*InetType) ExtensionName() string {
	return "inet"
}

func (*InetType) String() string {
	return "inet"
}

func (*InetType) Serialize() string {
	return "inet-serialized"
}

func (*InetType) Deserialize(storageType arrow.DataType, data string) (arrow.ExtensionType, error) {
	if data != "inet-serialized" {
		return nil, fmt.Errorf("type identifier did not match: '%s'", data)
	}
	if !arrow.TypeEqual(storageType, &arrow.BinaryType{}) {
		return nil, fmt.Errorf("invalid storage type for InetType: %s", storageType.Name())
	}
	return NewInetType(), nil
}

func (u *InetType) ExtensionEquals(other arrow.ExtensionType) bool {
	return u.ExtensionName() == other.ExtensionName()
}

func (*InetType) NewBuilder(bldr *array.ExtensionBuilder) array.Builder {
	return NewInetBuilder(bldr)
}
