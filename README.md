# ArrowArc

[![Go Report Card](https://goreportcard.com/badge/github.com/arrowarc/arrowarc)](https://goreportcard.com/report/github.com/arrowarc/arrowarc) [![ArrowArc Build](https://github.com/arrowarc/arrowarc/actions/workflows/ci.yml/badge.svg)](https://github.com/arrowarc/arrowarc/actions/workflows/ci.yml) [![Go Reference](https://pkg.go.dev/badge/github.com/arrowarc/arrowarc@v0.1.0.svg)](https://pkg.go.dev/github.com/arrowarc/arrowarc@v0.1.0)

ArrowArc is an experimental data transport mechanism that uses Apache Arrow for high-performance data manipulation. It is designed to be a zero-code, zero-config, and zero-maintenance data transport mechanism.

## Getting Started

You have several options to use ArrowArc:

1. Use the command line utilities to transport data.
2. Use the library in your Go program.
3. Use a YAML configuration file to define your data pipelines.

### Command Line Utilities

Use the `arrowarc` command to get started. It will display a help menu with available commands, including demos and benchmarks.

### Go Library

Example of setting up a pipeline to transport data from BigQuery to DuckDB:

```go

// Setup the BigQuery client and reader
bq, err := integrations.NewBigQueryReadClient(ctx)
reader, err := bq.NewBigQueryReader(ctx, projectID, datasetID, tableID)

// Setup the DuckDB client and writer
duck, err := integrations.OpenDuckDBConnection(ctx, dbFilePath)
writer, err := integrations.NewDuckDBRecordWriter(ctx, duck, tableID)

// Create and start the data pipeline
p, err := pipeline.NewDataPipeline(reader, writer)

// Start the pipeline
err = p.Start(ctx)
if err != nil {
    log.Fatalf("Failed to start pipeline: %v", err)
}

// Wait for the pipeline to finish
if pipelineErr := <-p.Done(); pipelineErr != nil {
    return "", fmt.Errorf("pipeline encountered an error: %w", pipelineErr)
}

// Print the Transport Report
fmt.Println(p.Report())

```
You can expect a report similar to this:

```json
{
  "start_time": "2024-08-31T10:22:23-05:00",
  "end_time": "2024-08-31T10:22:26-05:00",
  "records_processed": 4000000,
  "total_size": "0.63 GB",
  "total_duration": "3.34s",
  "throughput": "1197492.21 records/s",
  "throughput_size": "194.11 MB/s"
}
```

---

## Features

### CLI Utilities

| Utility             | Status |
|---------------------|--------|
| Transport Table     | ✅     |
| Rewrite Parquet     | ✅     |
| Generate Parquet    | ✅     |
| Generate IPC        | ✅     |
| Avro To Parquet     | ✅     |
| CSV To Parquet      | ✅     |
| CSV To JSON         | ✅     |
| JSON To Parquet     | ✅     |
| Parquet to CSV      | ✅     |
| Parquet to JSON     | ✅     |
| Flight Server       | ✅     |
| Sync Table          | ❌     |
| Validate Table      | ❌     |

### Integrations

#### Database Integrations

| Database    | Extraction | Ingestion |
|-------------|------------|-----------|
| PostgreSQL  | ✅         | 🚧        |
| BigQuery    | ✅         | ✅        |
| DuckDB      | ✅         | ✅        |
| Spanner     | ✅         | ❌        |
| CockroachDB | ✅         | 🚧        |
| MySQL       | 🚧         | ❌        |
| Oracle      | ❌         | ❌        |
| Snowflake   | ❌         | ❌        |
| SQLite      | ❌         | ❌        |
| Flight      | ❌         | ❌        |

#### Cloud Storage Integrations

| Provider                       | Extraction | Ingestion |
|--------------------------------|------------|-----------|
| Google Cloud Storage (GCS)     | ✅         | ✅        |
| Amazon S3                      | ❌         | ❌        |
| Azure Blob Storage             | ❌         | ❌        |

#### Filesystem Formats

| Format    | Extraction | Ingestion |
|-----------|------------|-----------|
| Parquet   | ✅         | ✅        |
| Avro      | ✅         | ❌        |
| CSV       | ✅         | ✅        |
| JSON      | ✅         | ✅        |
| IPC       | ✅         | ✅        |
| Iceberg   | ✅         | ❌        |

## Contributing

We welcome all [contributions](./CONTRIBUTING.md). Please see the [Code of Conduct](./CODE_OF_CONDUCT.md).

## License

Please see the [LICENSE](./LICENSE) for more details.
