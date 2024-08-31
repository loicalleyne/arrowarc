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
	"fmt"
	"os"
	"testing"
	"time"

	filesystem "github.com/arrowarc/arrowarc/integrations/filesystem"
	integrations "github.com/arrowarc/arrowarc/integrations/postgres"
	pipeline "github.com/arrowarc/arrowarc/pipeline"
	"github.com/stretchr/testify/assert"
)

const (
	dbURL = "postgresql://postgres:postgres@localhost:5432/postgres"
)

// Test for extracting a Postgres table to Parquet using the filesystem integration
func TestExtractPostgresTableToParquet(t *testing.T) {
	if os.Getenv("CI") == "true" {
		t.Skip("Skipping Postgres integration test in CI environment.")
	}

	source, err := integrations.NewPostgresSource(context.Background(), dbURL)
	assert.NoError(t, err, "Error should be nil when creating Postgres source")
	defer source.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	reader, err := source.GetPostgresRecordReader(ctx, "part")
	assert.NoError(t, err, "Error should be nil when getting Postgres record reader")

	outputFile := "test_output.parquet"
	defer os.Remove(outputFile)

	// Initialize filesystem integration
	fsWriter, err := filesystem.NewParquetWriter(outputFile, reader.Schema(), filesystem.NewDefaultParquetWriterProperties())
	assert.NoError(t, err, "Error should be nil when creating Parquet writer")

	// Setup the pipeline
	p := pipeline.NewDataPipeline(reader, fsWriter)
	assert.NoError(t, err, "Error should be nil when creating data pipeline")

	metrics, err := p.Start(ctx)
	assert.NoError(t, err, "Error should be nil when starting data pipeline")
	fmt.Printf("Pipeline metrics: %+v\n", metrics)

	// Verify the Parquet file is created
	_, err = os.Stat(outputFile)
	assert.NoError(t, err, "Parquet file should be created successfully")
}
