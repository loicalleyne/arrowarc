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
	"io"
	"os"
	"testing"
	"time"

	"github.com/apache/arrow/go/v17/arrow"
	. "github.com/arrowarc/arrowarc/integrations/filesystem"
	helper "github.com/arrowarc/arrowarc/pkg/common/utils"
	"github.com/stretchr/testify/assert"
)

func TestReadIcebergFileStream(t *testing.T) {
	// If testing on GitHub Actions, skip this test because DuckDB shared library is not available. TODO: Fix this.
	if os.Getenv("CI") == "true" {
		t.Skip("Skipping Iceberg file stream test in CI environment.")
	}

	// Publicly available Iceberg dataset (replace with an actual public URL if available)
	icebergFilePath := "https://public-datasets.s3.amazonaws.com/iceberg/tpch/lineitem"

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	reader, err := ReadIcebergFileStream(ctx, icebergFilePath)
	assert.NoError(t, err, "Error should be nil when reading from Iceberg file stream")

	var records []arrow.Record
	for {
		rec, err := reader.Read()
		if err == context.Canceled || err == io.EOF {
			break
		}
		assert.NoError(t, err, "Error should be nil when reading from Iceberg file stream")
		assert.NotNil(t, rec, "Record should not be nil when reading from Iceberg file")
		records = append(records, rec)
		helper.PrintRecordBatch(rec)
		rec.Release()
	}

	assert.Greater(t, len(records), 0, "There should be at least 1 record returned from the Iceberg file")

	assert.NoError(t, err, "Error should be nil when closing the Iceberg file reader")
}
