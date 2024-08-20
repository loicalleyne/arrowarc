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
	"fmt"

	xtype "github.com/ArrowArc/ArrowArc/internal/dbarrow/types"
	"github.com/apache/arrow/go/v17/arrow"
)

func PgToArrow(t string) arrow.DataType {
	t = Normalize(t)
	if IsArrayType(t) {
		return arrow.ListOf(PgToArrow(t[:len(t)-2]))
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
		return xtype.ExtensionTypes.UUID
	case "bytea":
		return arrow.BinaryTypes.Binary
	case "date":
		return arrow.FixedWidthTypes.Date32
	case "json", "jsonb":
		return xtype.ExtensionTypes.JSON
	case "cidr", "inet":
		return xtype.ExtensionTypes.Inet
	case "macaddr", "macaddr8":
		return xtype.ExtensionTypes.MAC
	default:
		return arrow.BinaryTypes.String
	}
}

func ArrowToPg(t arrow.DataType) string {
	switch dt := t.(type) {
	case *arrow.BooleanType:
		return "boolean"
	case *arrow.Int8Type, *arrow.Uint8Type, *arrow.Int16Type:
		return "smallint"
	case *arrow.Int32Type, *arrow.Uint16Type:
		return "integer"
	case *arrow.Int64Type, *arrow.Uint32Type:
		return "bigint"
	case *arrow.Uint64Type:
		return "numeric(20,0)"
	case *arrow.Float32Type:
		return "real"
	case *arrow.Float64Type:
		return "double precision"
	case *arrow.Decimal128Type:
		return fmt.Sprintf("numeric(%d,%d)", dt.Precision, dt.Scale)
	case *arrow.Decimal256Type:
		return fmt.Sprintf("numeric(%d,%d)", dt.Precision, dt.Scale)
	case *arrow.StringType:
		return "text"
	case *arrow.BinaryType, *arrow.LargeBinaryType:
		return "bytea"
	case *arrow.TimestampType:
		return "timestamp without time zone"
	case *arrow.Time32Type, *arrow.Time64Type:
		return "time without time zone"
	case *arrow.Date32Type, *arrow.Date64Type:
		return "date"
	case *xtype.UUIDType:
		return "uuid"
	case *xtype.JSONType:
		return "jsonb"
	case *xtype.MACType:
		return "macaddr"
	case *xtype.InetType:
		return "inet"
	case *arrow.ListType:
		return ArrowToPg(dt.Elem()) + "[]"
	case *arrow.MapType:
		return "jsonb"
	default:
		return "text"
	}
}
