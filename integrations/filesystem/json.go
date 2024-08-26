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

package integrations

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/arrio"
)

// JSONRecordReader implements arrio.Reader for reading records from JSON files.
type JSONRecordReader struct {
	ctx        context.Context
	file       *os.File
	jsonReader *array.JSONReader
	schema     *arrow.Schema
}

// NewJSONRecordReader creates a new reader for reading records from a JSON file.
func NewJSONRecordReader(ctx context.Context, filePath string, schema *arrow.Schema, chunkSize int) (arrio.Reader, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open JSON file: %w", err)
	}

	jsonReader := array.NewJSONReader(file, schema, array.WithChunk(chunkSize))

	return &JSONRecordReader{
		ctx:        ctx,
		file:       file,
		jsonReader: jsonReader,
		schema:     schema,
	}, nil
}

// Read reads the next record from the JSON file.
func (r *JSONRecordReader) Read() (arrow.Record, error) {
	select {
	case <-r.ctx.Done():
		return nil, r.ctx.Err()
	default:
	}

	if !r.jsonReader.Next() {
		if err := r.jsonReader.Err(); err != nil && err != io.EOF {
			return nil, fmt.Errorf("error reading JSON record: %w", err)
		}
		return nil, io.EOF
	}

	record := r.jsonReader.Record()
	if record != nil {
		record.Retain()
	}
	return record, nil
}

// Schema returns the schema of the records being read from the JSON file.
func (r *JSONRecordReader) Schema() *arrow.Schema {
	return r.schema
}

// Close releases resources associated with the JSON reader.
func (r *JSONRecordReader) Close() error {
	r.jsonReader.Release()
	return r.file.Close()
}

// JSONRecordWriter implements arrio.Writer for writing records to JSON files.
type JSONRecordWriter struct {
	file    *os.File
	encoder *json.Encoder
}

// NewJSONRecordWriter creates a new writer for writing records to a JSON file.
func NewJSONRecordWriter(ctx context.Context, filePath string) (arrio.Writer, error) {
	file, err := os.Create(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to create JSON file: %w", err)
	}

	encoder := json.NewEncoder(file)

	return &JSONRecordWriter{
		file:    file,
		encoder: encoder,
	}, nil
}

// Write writes a record to the JSON file.
func (w *JSONRecordWriter) Write(record arrow.Record) error {
	structArray := array.RecordToStructArray(record)
	if err := w.encoder.Encode(structArray); err != nil {
		return fmt.Errorf("error writing JSON record: %w", err)
	}
	return nil
}

// Close closes the JSON writer.
func (w *JSONRecordWriter) Close() error {
	return w.file.Close()
}

// WriteJSONFileStream writes records from a reader to a JSON file.
func WriteJSONFileStream(ctx context.Context, filePath string, reader arrio.Reader) error {
	writer, err := NewJSONRecordWriter(ctx, filePath)
	if err != nil {
		return fmt.Errorf("failed to create JSON writer: %w", err)
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		record, err := reader.Read()
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return fmt.Errorf("failed to read record: %w", err)
		}

		if err := writer.Write(record); err != nil {
			return fmt.Errorf("failed to write record to JSON: %w", err)
		}
		record.Release()
	}
}

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
func NewDecoder(r io.Reader) *json.Decoder {
	return json.NewDecoder(r)
}

// NewEncoder initializes and returns a new JSON Encoder.
func NewEncoder(w io.Writer) *json.Encoder {
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
