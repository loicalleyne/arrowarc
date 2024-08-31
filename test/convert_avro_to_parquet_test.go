package test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/apache/arrow/go/v17/parquet/compress"
	convert "github.com/arrowarc/arrowarc/converter"
	"github.com/arrowarc/arrowarc/pkg/common/utils"
	"github.com/stretchr/testify/assert"
)

func TestConvertAvroToParquet(t *testing.T) {
	t.Parallel()    // Run the test in parallel
	utils.LoadEnv() // Load any required environment variables

	// Define test cases
	tests := []struct {
		avroFilePath     string
		parquetFilePath  string
		chunkSize        int64
		compressionCodec compress.Compression
		description      string
	}{
		{
			avroFilePath:     "/Users/thomasmcgeehan/ArrowArc/arrowarc/data/avro/part.avro",
			parquetFilePath:  fmt.Sprintf("partexample_test_1_%d.parquet", time.Now().UnixNano()),
			chunkSize:        10 * 1024 * 1024, // 10 MB
			compressionCodec: compress.Codecs.Snappy,
			description:      "Convert Avro to Parquet with Snappy compression",
		},
	}

	// Execute each test case as a subtest
	for _, test := range tests {
		test := test // Capture range variable
		t.Run(test.description, func(t *testing.T) {
			t.Parallel() // Run subtests in parallel

			ctx, cancel := context.WithTimeout(context.Background(), 600*time.Second) // 10 minutes
			defer cancel()

			// Perform the conversion
			metrics, err := convert.ConvertAvroToParquet(ctx, test.avroFilePath, test.parquetFilePath, test.chunkSize, compress.Codecs.Snappy)

			// Assert no error and non-nil metrics
			assert.NoError(t, err, "Error should be nil when converting Avro to Parquet")
			assert.NotNil(t, metrics, "Metrics should not be nil after conversion")

			// Print metrics for debugging
			fmt.Printf("Metrics: %+v\n", metrics)

			// Check if the Parquet file is created
			_, err = os.Stat(test.parquetFilePath)
			assert.NoError(t, err, "Parquet file should be created")

			// Cleanup generated Parquet file after the test
			t.Cleanup(func() {
				os.Remove(test.parquetFilePath)
			})
		})
	}
}
