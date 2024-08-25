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

package transport

import (
	"context"
	"fmt"
)

type DataSource string
type DataSink string

const (
	ParquetSource  DataSource = "parquet"
	BigQuerySource DataSource = "bigquery"
	DuckDBSource   DataSource = "duckdb"
	PostgresSource DataSource = "postgres"
	GitHubSource   DataSource = "github"

	ParquetSink  DataSink = "parquet"
	DuckDBSink   DataSink = "duckdb"
	BigQuerySink DataSink = "bigquery"
	PostgresSink DataSink = "postgres"
)

func Transport(ctx context.Context, source DataSource, sink DataSink, sourceDetails map[string]string, sinkDetails map[string]string) error {
	switch {
	case source == ParquetSource && sink == DuckDBSink:
		return TransportParquetToDuckDB(ctx, sourceDetails["filePath"], sinkDetails["dbFilePath"], sinkDetails["tableName"])
	default:
		return fmt.Errorf("transport from %s to %s is not yet implemented", source, sink)
	}
}

func TransportParquetToDuckDB(ctx context.Context, parquetFilePath, dbFilePath, tableName string) error {
	return nil
}
