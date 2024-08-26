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
	"fmt"
	"io"
	"strings"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-adbc/go/adbc/drivermgr"
	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/arrio"
)

type PostgresSource struct {
	conn adbc.Connection
}

func NewPostgresSource(ctx context.Context, dbURL string) (*PostgresSource, error) {
	var drv drivermgr.Driver
	db, err := drv.NewDatabase(map[string]string{
		"driver":          "/usr/local/lib/libadbc_driver_postgresql.dylib",
		adbc.OptionKeyURI: dbURL,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create ADBC database: %w", err)
	}

	conn, err := db.Open(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to open connection: %w", err)
	}

	return &PostgresSource{conn: conn}, nil
}

type PostgresRecordReader struct {
	ctx       context.Context
	stmt      adbc.Statement
	recordSet array.RecordReader
}

func (p *PostgresSource) GetPostgresRecordReader(ctx context.Context, tableName string) (arrio.Reader, error) {
	stmt, err := p.conn.NewStatement()
	if err != nil {
		return nil, fmt.Errorf("failed to create statement: %w", err)
	}

	query := fmt.Sprintf("SELECT * FROM %s", tableName)
	if err := stmt.SetSqlQuery(query); err != nil {
		stmt.Close()
		return nil, fmt.Errorf("failed to set SQL query: %w", err)
	}

	recordSet, _, err := stmt.ExecuteQuery(ctx)
	if err != nil {
		stmt.Close()
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}

	return &PostgresRecordReader{
		ctx:       ctx,
		stmt:      stmt,
		recordSet: recordSet,
	}, nil
}

func (r *PostgresRecordReader) Read() (arrow.Record, error) {
	if !r.recordSet.Next() {
		if err := r.recordSet.Err(); err != nil && err != io.EOF {
			return nil, err
		}
		return nil, io.EOF
	}

	record := r.recordSet.Record()
	record.Retain()
	return record, nil
}

func (r *PostgresRecordReader) Close() error {
	r.recordSet.Release()
	return r.stmt.Close()
}

func (p *PostgresSource) Close() error {
	return p.conn.Close()
}

func (r *PostgresRecordReader) Schema() *arrow.Schema {
	return r.recordSet.Schema()
}

type PostgresSink struct {
	conn adbc.Connection
}

// NewPostgresSink creates a new PostgresSink with an open ADBC connection.
func NewPostgresSink(ctx context.Context, dbURL string) (*PostgresSink, error) {
	var drv drivermgr.Driver
	db, err := drv.NewDatabase(map[string]string{
		"driver":          "/usr/local/lib/libadbc_driver_postgresql.dylib",
		adbc.OptionKeyURI: dbURL,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create ADBC database: %w", err)
	}

	conn, err := db.Open(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to open connection: %w", err)
	}

	return &PostgresSink{conn: conn}, nil
}

// IngestToPostgres ingests records from arrio.Reader into the specified PostgreSQL table.
func (p *PostgresSink) IngestToPostgres(ctx context.Context, tableName string, schema *arrow.Schema, reader arrio.Reader) error {
	// Construct the SQL query based on the schema
	columns := make([]string, len(schema.Fields()))
	values := make([]string, len(schema.Fields()))
	for i, field := range schema.Fields() {
		columns[i] = field.Name
		values[i] = fmt.Sprintf("$%d", i+1)
	}
	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", tableName, strings.Join(columns, ", "), strings.Join(values, ", "))

	// Prepare the statement
	stmt, err := p.conn.NewStatement()
	if err != nil {
		return fmt.Errorf("failed to create statement: %w", err)
	}
	defer stmt.Close()

	if err := stmt.SetSqlQuery(query); err != nil {
		return fmt.Errorf("failed to set SQL query: %w", err)
	}

	// Read records from the arrio.Reader and bind them to the statement
	for {
		record, err := reader.Read()
		if err != nil {
			if err == io.EOF {
				break // End of stream
			}
			return fmt.Errorf("failed to read record: %w", err)
		}
		/*
			r := array.RecordToStructArray(record)

			// Bind the record set as a stream
			if err := stmt.BindStream(ctx, arrio.NewRecordReader(r)); err != nil {
				record.Release()
				return fmt.Errorf("failed to bind stream: %w", err)
			}
		*/
		// Execute the insert statement
		if _, err := stmt.ExecuteUpdate(ctx); err != nil {
			record.Release()
			return fmt.Errorf("failed to execute update: %w", err)
		}

		record.Release()
	}

	return nil
}

// Close closes the ADBC connection.
func (p *PostgresSink) Close() error {
	return p.conn.Close()
}
