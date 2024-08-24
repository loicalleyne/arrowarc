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

package utils

import (
	pb "google.golang.org/protobuf/types/known/structpb"
)

// DecodeToMap converts a pb.Struct to a map from strings to Go types.
// DecodeToMap panics if s is invalid.
func DecodeToMap(s *pb.Struct) map[string]interface{} {
	if s == nil {
		return nil
	}
	m := map[string]interface{}{}
	for k, v := range s.Fields {
		m[k] = decodeValue(v)
	}
	return m
}

func decodeValue(v *pb.Value) interface{} {
	switch k := v.Kind.(type) {
	case *pb.Value_NullValue:
		return nil
	case *pb.Value_NumberValue:
		return k.NumberValue
	case *pb.Value_StringValue:
		return k.StringValue
	case *pb.Value_BoolValue:
		return k.BoolValue
	case *pb.Value_StructValue:
		return DecodeToMap(k.StructValue)
	case *pb.Value_ListValue:
		s := make([]interface{}, len(k.ListValue.Values))
		for i, e := range k.ListValue.Values {
			s[i] = decodeValue(e)
		}
		return s
	default:
		panic("protostruct: unknown kind")
	}
}
