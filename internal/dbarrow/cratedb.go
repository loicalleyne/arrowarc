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
	xtype "github.com/ArrowArc/ArrowArc/internal/dbarrow/types"
	"github.com/apache/arrow/go/v17/arrow"
)

func CrateDBToArrow(t string) arrow.DataType {
	t = Normalize(t)
	if IsArrayType(t) {
		return arrow.ListOf(CrateDBToArrow(t[:len(t)-2]))
	}

	if dt, matched := ParseDataType(t); matched {
		return dt
	}

	switch t {
	case "boolean":
		return arrow.FixedWidthTypes.Boolean
	case "smallserial":
		return arrow.PrimitiveTypes.Int16
	case "serial":
		return arrow.PrimitiveTypes.Int32
	case "bigserial", "serial8":
		return arrow.PrimitiveTypes.Int64
	case "smallint", "int2":
		return arrow.PrimitiveTypes.Int16
	case "integer", "int", "int4":
		return arrow.PrimitiveTypes.Int32
	case "bigint", "int8":
		return arrow.PrimitiveTypes.Int64
	case "real", "float4":
		return arrow.PrimitiveTypes.Float32
	case "double precision", "float8":
		return arrow.PrimitiveTypes.Float64
	case "uuid":
		return arrow.BinaryTypes.String
	case "bytea":
		return arrow.BinaryTypes.Binary
	case "date":
		return arrow.FixedWidthTypes.Date32
	case "json", "jsonb", "object":
		return xtype.ExtensionTypes.JSON
	case "cidr":
		return xtype.ExtensionTypes.Inet
	case "macaddr", "macaddr8":
		return arrow.BinaryTypes.String
	case "inet", "ip":
		return xtype.ExtensionTypes.Inet
	default:
		return arrow.BinaryTypes.String
	}
}
