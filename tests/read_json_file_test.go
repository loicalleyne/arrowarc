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

package test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/apache/arrow/go/v17/arrow"
	integrations "github.com/arrowarc/arrowarc/internal/integrations/filesystem"
	"github.com/stretchr/testify/assert"
)

func TestReadJSONFileStream(t *testing.T) {
	t.Parallel()

	inputFilePath := "input_test.json"
	defer os.Remove(inputFilePath)

	sampleJSON := `
		{"id": 1, "name": "John"},
		{"id": 2, "name": "Jane"},
		{"id": 3, "name": "Doe"}
	`
	err := os.WriteFile(inputFilePath, []byte(sampleJSON), 0644)
	assert.NoError(t, err, "Error should be nil when writing the sample JSON file")

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	recordChan, errChan := integrations.ReadJSONFileStream(ctx, inputFilePath, schema, 1024)

	var recordsRead int
	for record := range recordChan {
		assert.NotNil(t, record, "Record should not be nil")
		recordsRead += int(record.NumRows())
		record.Release()
	}

	for err := range errChan {
		assert.NoError(t, err, "Error should be nil when reading JSON file")
	}

	assert.Equal(t, 3, recordsRead, "Should have read three records")
}
