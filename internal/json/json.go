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

package json

import (
	"bytes"
	"fmt"
	"io"

	"github.com/goccy/go-json"
)

// Type aliases to maintain compatibility with the json package types.
type (
	Decoder            = json.Decoder
	Encoder            = json.Encoder
	Marshaler          = json.Marshaler
	Delim              = json.Delim
	UnmarshalTypeError = json.UnmarshalTypeError
	Number             = json.Number
	Unmarshaler        = json.Unmarshaler
	RawMessage         = json.RawMessage
)

// Marshal safely marshals the provided value to JSON.
func Marshal(v interface{}) ([]byte, error) {
	data, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("json: failed to marshal: %w", err)
	}
	return data, nil
}

// Unmarshal safely unmarshals the provided JSON data into the provided value.
func Unmarshal(data []byte, v interface{}) error {
	if len(data) == 0 {
		return fmt.Errorf("json: cannot unmarshal empty data")
	}
	if err := json.Unmarshal(data, v); err != nil {
		return fmt.Errorf("json: failed to unmarshal: %w", err)
	}
	return nil
}

// NewDecoder initializes and returns a new JSON Decoder.
func NewDecoder(r io.Reader) *Decoder {
	return json.NewDecoder(r)
}

// NewEncoder initializes and returns a new JSON Encoder.
func NewEncoder(w io.Writer) *Encoder {
	return json.NewEncoder(w)
}

// EncodeToString marshals and encodes the provided value directly into a string.
func EncodeToString(v interface{}) (string, error) {
	data, err := Marshal(v)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

// DecodeFromString decodes JSON data from a string into the provided value.
func DecodeFromString(s string, v interface{}) error {
	if s == "" {
		return fmt.Errorf("json: cannot decode from empty string")
	}
	return Unmarshal([]byte(s), v)
}

// PrettyPrint marshals the provided value into a pretty-printed JSON string.
func PrettyPrint(v interface{}) (string, error) {
	var buf bytes.Buffer
	enc := NewEncoder(&buf)
	enc.SetIndent("", "  ")
	if err := enc.Encode(v); err != nil {
		return "", fmt.Errorf("json: failed to pretty print: %w", err)
	}
	return buf.String(), nil
}

// ValidateJSON checks if the provided byte slice is valid JSON.
func ValidateJSON(data []byte) error {
	var js json.RawMessage
	if err := Unmarshal(data, &js); err != nil {
		return fmt.Errorf("json: invalid JSON: %w", err)
	}
	return nil
}

// ValidateJSONString checks if the provided string is valid JSON.
func ValidateJSONString(s string) error {
	return ValidateJSON([]byte(s))
}
