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

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-adbc/go/adbc/drivermgr"
	"github.com/apache/arrow/go/v17/arrow"
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

func (p *PostgresSource) GetArrowStream(ctx context.Context, tableName string) (<-chan arrow.Record, <-chan error) {
	recordChan := make(chan arrow.Record)
	errChan := make(chan error, 1)

	go func() {
		defer close(recordChan)
		defer close(errChan)

		stmt, err := p.conn.NewStatement()
		if err != nil {
			errChan <- fmt.Errorf("failed to create statement: %w", err)
			return
		}
		defer stmt.Close()

		query := fmt.Sprintf("SELECT * FROM %s", tableName)
		if err := stmt.SetSqlQuery(query); err != nil {
			errChan <- fmt.Errorf("failed to set SQL query: %w", err)
			return
		}

		reader, _, err := stmt.ExecuteQuery(ctx)
		if err != nil {
			errChan <- fmt.Errorf("failed to execute query: %w", err)
			return
		}
		defer reader.Release()

		for reader.Next() {
			record := reader.Record()
			record.Retain()

			select {
			case recordChan <- record:
			case <-ctx.Done():
				record.Release()
				return
			}
		}

		if err := reader.Err(); err != nil {
			errChan <- fmt.Errorf("error reading records: %w", err)
		}
	}()

	return recordChan, errChan
}

func (p *PostgresSource) Close() error {
	return p.conn.Close()
}
