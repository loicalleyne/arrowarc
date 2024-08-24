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
	"database/sql"
	"os"
	"testing"
	"time"

	integrations "github.com/arrowarc/arrowarc/internal/integrations/postgres"
	embeddedpostgres "github.com/fergusstrange/embedded-postgres"
	"github.com/stretchr/testify/assert"
)

func TestGetArrowStreamSuccess(t *testing.T) {

	if os.Getenv("CI") == "true" {
		t.Skip("Skipping DuckDB integration test in CI environment.")
	}

	postgres := embeddedpostgres.NewDatabase(embeddedpostgres.DefaultConfig().Port(5433))
	err := postgres.Start()
	assert.NoError(t, err, "Embedded Postgres should start without error")

	defer func() {
		err := postgres.Stop()
		assert.NoError(t, err, "Embedded Postgres should stop without error")
	}()

	db, err := sql.Open("postgres", "host=localhost port=5433 user=postgres password=postgres dbname=postgres sslmode=disable")
	assert.NoError(t, err, "Should be able to connect to embedded Postgres")

	_, err = db.Exec("CREATE TABLE test_table (id INT PRIMARY KEY, name TEXT)")
	assert.NoError(t, err, "Should be able to create test table")

	_, err = db.Exec("INSERT INTO test_table (id, name) VALUES (1, 'John'), (2, 'Jane'), (3, 'Jack')")
	assert.NoError(t, err, "Should be able to insert data into test table")

	dbURL := "postgresql://postgres:postgres@localhost:5433/postgres"
	source, err := integrations.NewPostgresSource(context.Background(), dbURL)
	assert.NoError(t, err, "Error should be nil when creating Postgres source")

	defer source.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	recordChan, errChan := source.GetPostgresStream(ctx, "test_table")

	select {
	case err := <-errChan:
		assert.NoError(t, err, "Error should be nil during the stream")
	default:
	}

	var recordCount int
	for record := range recordChan {
		assert.NotNil(t, record, "Record should not be nil")
		recordCount += int(record.NumRows())
		record.Release() // Ensure to release the record
	}

	assert.Equal(t, 3, recordCount, "There should be 3 rows returned")
}
