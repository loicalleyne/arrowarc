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
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/arrowarc/arrowarc/internal/arrio"
)

func ReadJSONFileStream(ctx context.Context, filePath string, schema *arrow.Schema, chunkSize int) (arrio.Reader, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open JSON file: %w", err)
	}

	jsonReader := array.NewJSONReader(file, schema, array.WithChunk(chunkSize))

	return &JSONRecordReader{
		ctx:        ctx,
		file:       file,
		jsonReader: jsonReader,
	}, nil
}

type JSONRecordReader struct {
	ctx        context.Context
	file       *os.File
	jsonReader *array.JSONReader
}

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

func (r *JSONRecordReader) Close() error {
	r.jsonReader.Release()
	return r.file.Close()
}

func WriteJSONFileStream(ctx context.Context, filePath string, reader arrio.Reader) error {
	file, err := os.Create(filePath)
	if err != nil {
		return fmt.Errorf("failed to create file: %w", err)
	}
	defer func() {
		if cerr := file.Close(); cerr != nil && err == nil {
			err = fmt.Errorf("failed to close file: %w", cerr)
		}
	}()

	encoder := json.NewEncoder(file)

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

		structArray := array.RecordToStructArray(record)
		if err := encoder.Encode(structArray); err != nil {
			return fmt.Errorf("error writing JSON record: %w", err)
		}
		record.Release()
	}
}
