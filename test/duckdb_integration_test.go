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

	duckdb "github.com/arrowarc/arrowarc/integrations/duckdb"
	"github.com/stretchr/testify/assert"
)

func TestDuckDBIntegration(t *testing.T) {
	// Skip test in CI environment if DuckDB shared library is not available.
	if os.Getenv("CI") == "true" {
		t.Skip("Skipping DuckDB integration test in CI environment.")
	}

	parquetFilePath := "/Users/thomasmcgeehan/ArrowArc/arrowarc/data/parquet/flights.parquet"
	duckdbFilePath := ":memory:"
	query := "SELECT * FROM parquet_scan('" + parquetFilePath + "')"

	readOpts := &duckdb.DuckDBReadOptions{
		Query: query,
	}

	// Read Parquet file.
	reader, err := duckdb.NewDuckDBReader(context.Background(), duckdbFilePath, readOpts)
	assert.NoError(t, err, "Error should be nil when creating DuckDB reader")
	defer reader.Close()

	// Print the schema
	schema := reader.Schema()
	t.Logf("Schema: %v", schema)
}
