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
	"regexp"
	"strconv"
	"strings"

	"github.com/apache/arrow/go/v17/arrow"
)

var (
	reTimestamp = regexp.MustCompile(`timestamp\s*(?:\(([0-6])\))?(?: with(?:out)? time zone)?`)
	reTime      = regexp.MustCompile(`time\s*(?:\(([0-6])\))?(?: with(?:out)? time zone)?`)
	reNumeric   = regexp.MustCompile(`numeric\s*(?:\(([0-9]+)\s*,\s*([0-9]+)\))?`)
)

// Normalize standardizes the string representation of a data type.
func Normalize(t string) string {
	return strings.ToLower(strings.TrimSpace(t))
}

// IsArrayType checks if the given type string represents an array type.
func IsArrayType(t string) bool {
	return strings.HasSuffix(t, "[]")
}

// ParseDataType handles parsing of complex data types such as timestamp, time, and numeric.
func ParseDataType(t string) (arrow.DataType, bool) {
	parsers := []func(string) (arrow.DataType, bool){
		parseTimestamp,
		parseTime,
		parseNumeric,
	}
	for _, parser := range parsers {
		if dt, matched := parser(t); matched {
			return dt, true
		}
	}
	return nil, false
}

func parseTimestamp(t string) (arrow.DataType, bool) {
	matches := reTimestamp.FindStringSubmatch(t)
	if len(matches) == 0 {
		return nil, false
	}

	switch matches[1] {
	case "0":
		return arrow.FixedWidthTypes.Timestamp_s, true
	case "1", "2", "3":
		return arrow.FixedWidthTypes.Timestamp_ms, true
	default:
		return arrow.FixedWidthTypes.Timestamp_us, true
	}
}

func parseTime(t string) (arrow.DataType, bool) {
	matches := reTime.FindStringSubmatch(t)
	if len(matches) == 0 {
		return nil, false
	}

	switch matches[1] {
	case "0":
		return arrow.FixedWidthTypes.Time32s, true
	case "1", "2", "3":
		return arrow.FixedWidthTypes.Time32ms, true
	default:
		return arrow.FixedWidthTypes.Time64us, true
	}
}

func parseNumeric(t string) (arrow.DataType, bool) {
	matches := reNumeric.FindStringSubmatch(t)
	if len(matches) == 0 {
		if t == "numeric" {
			return &arrow.Decimal128Type{Precision: 38, Scale: 0}, true
		}
		return nil, false
	}

	precision, err := strconv.ParseInt(matches[1], 10, 64)
	if err != nil || precision == 0 {
		return nil, false
	}

	scale, err := strconv.ParseInt(matches[2], 10, 64)
	if err != nil {
		return nil, false
	}

	if precision == 20 && scale == 0 {
		return arrow.PrimitiveTypes.Uint64, true
	}

	switch {
	case precision <= 38:
		return &arrow.Decimal128Type{Precision: int32(precision), Scale: int32(scale)}, true
	case precision <= 76:
		return &arrow.Decimal256Type{Precision: int32(precision), Scale: int32(scale)}, true
	default:
		return arrow.BinaryTypes.String, true
	}
}
