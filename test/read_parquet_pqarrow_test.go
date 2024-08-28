package test

import (
	"context"
	"database/sql/driver"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/apache/arrow/go/v17/arrow/memory"
	x "github.com/arrowarc/arrowarc/experiments"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

// ParquetRowsTestSuite defines a test suite for ParquetRows.
type ParquetRowsTestSuite struct {
	suite.Suite
	testFilePath string
	alloc        *memory.CheckedAllocator
}

// SetupSuite initializes the test suite by setting up any required resources.
func (suite *ParquetRowsTestSuite) SetupSuite() {
	suite.alloc = memory.NewCheckedAllocator(memory.NewGoAllocator())
	suite.testFilePath = filepath.Join(
		"/Users",
		"thomasmcgeehan",
		"ArrowArc",
		"arrowarc",
		"data",
		"parquet",
		"flights.parquet",
	)
}

// TearDownSuite cleans up any resources allocated by the test suite.
func (suite *ParquetRowsTestSuite) TearDownSuite() {
	suite.alloc.AssertSize(suite.T(), 0)
}

// TestParquetReader verifies the behavior of the ParquetRows reader.
func (suite *ParquetRowsTestSuite) TestParquetReader() {

	ctx := context.Background()
	reader, err := x.NewParquetRowsReader(ctx, suite.testFilePath)
	assert.NoError(suite.T(), err, "Failed to create Parquet reader")
	assert.NotNil(suite.T(), reader, "Expected non-nil Parquet reader")

	// Test Columns method
	columns := reader.Columns()
	assert.NotEmpty(suite.T(), columns, "Expected columns to be non-empty")

	var recordsRead int
	// Test reading rows
	dest := make([]driver.Value, len(columns))
	for {
		err := reader.Next(dest)
		if err == io.EOF {
			break
		}
		recordsRead++
		assert.NoError(suite.T(), err, "Unexpected error while reading rows")
	}

	// Close the reader and assert no errors
	err = reader.Close()
	assert.NoError(suite.T(), err, "Failed to close Parquet reader")
	fmt.Println(recordsRead)
}

// TestParquetRowsSuite runs the test suite.
func TestParquetRowsSuite(t *testing.T) {
	if os.Getenv("CI") == "true" {
		t.Skip("Skipping Parquet integration test in CI environment.")
	}
	suite.Run(t, new(ParquetRowsTestSuite))
}
