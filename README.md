![Alt text](assets/images/ArrowArcLogo.png)

[![Go Report Card](https://goreportcard.com/badge/github.com/arrowarc/arrowarc)](https://goreportcard.com/report/github.com/arrowarc/arrowarc) 
[![ArrowArc Build](https://github.com/arrowarc/arrowarc/actions/workflows/ci.yml/badge.svg)](https://github.com/arrowarc/arrowarc/actions/workflows/ci.yml) 
[![Go Reference](https://pkg.go.dev/badge/github.com/arrowarc/arrowarc@v0.1.0.svg)](https://pkg.go.dev/github.com/arrowarc/arrowarc@v0.1.0)

**Welcome to ArrowArc**—a passion-driven project designed to push the boundaries of data processing speed on modern hardware. ArrowArc isn't here to rival the giants of Big Data; instead, it’s an exploration of how efficiently data can be moved and processed using Go and Apache Arrow, leveraging today's powerful tools.

---

## Why Go and Apache Arrow?

I love working with Go for its elegance and powerful concurrency features. Combine that with Apache Arrow, which is optimized for in-memory data processing, and you have a recipe for high-performance data manipulation. ArrowArc gets data into Arrow format as quickly as possible and keeps it there, allowing for efficient, low-latency processing.

---

## Zero-Code Configuration

ArrowArc is built with simplicity in mind. It's designed to be entirely configurable, so you can set it up and let it run—no coding required to sync or transport your data. Just define your configuration, and ArrowArc takes care of the rest (theoretically).

---

## Utility Functions

ArrowArc also includes several utility functions that originated from my own need for integration testing. You're find utilities to generate various file formats on the fly, use embedded postgres and more.

---

## Getting Started

ArrowArc is still very much a work in progress, but if you’re like me and enjoy experimenting with data processing, I’d love to hear from you.

### Example: Streaming Data from Bigquery and Writing to DuckDB

Here’s a quick example of setting up a pipeline in ArrowArc to transport data from BigQuery to DuckDB.

```go
ctx := context.Background()

// Setup the BigQuery client and reader
bq, err := integrations.NewBigQueryReadClient(ctx)
reader, err := bq.NewBigQueryReader(ctx, projectID, datasetID, tableID)

// Setup the DuckDB client and writer
duck, err := integrations.OpenDuckDBConnection(ctx, dbFilePath)
writer, err := integrations.NewDuckDBRecordWriter(ctx, duck, tableID)

// Create and start the data pipeline
p := pipeline.NewDataPipeline(reader, writer).Start(ctx)
```

---

### ArrowArc Feature Matrix

I’m actively working on adding new features and integrations. Here’s where things stand:

- `✅` - Implemented
- `🚧` - In Progress
- `❌` - Not Started

---

### Command Line Utilities

| Utility             | Status       |
|---------------------|--------------|
| **Transport**       | ✅           |
| **Sync Table**      | ❌           |
| **Validate Table**  | ❌           |
| **Rewrite Parquet** | ✅           |
| **Generate Parquet**| ✅           |
| **CSV To Parquet**  | ✅           |
| **JSON To Parquet** | ✅           |
| **Parquet to CSV**  | ✅           |
| **Parquet to JSON** | ✅           |

---

### Integration Types

#### 1. Database Integrations

| Database        | Extraction | Ingestion |
|-----------------|------------|-----------|
| **PostgreSQL**  | ✅         | 🚧        |
| **MySQL**       | 🚧         | ❌        |
| **Oracle**      | ❌         | ❌        |
| **BigQuery**    | ✅         | 🚧        |
| **Snowflake**   | ❌         | ❌        |
| **DuckDB**      | ✅         | ✅        |
| **SQLite**      | ❌         | ❌        |
| **Spanner**     | ✅         | ❌        |
| **CockroachDB** | ✅         | 🚧        |
| **Flight**      | ❌         | ❌        |

---

#### 2. Cloud Storage Integrations

| Provider                         | Extraction | Ingestion |
|----------------------------------|------------|-----------|
| **Google Cloud Storage (GCS)**   | ✅         | ✅        |
| **Amazon S3**                    | ❌         | ❌        |
| **Azure Blob Storage**           | ❌         | ❌        |

---

#### 3. Filesystem Formats

| Format        | Extraction | Ingestion |
|---------------|------------|-----------|
| **Parquet**   | ✅         | ✅        |
| **Avro**      | ✅         | ❌        |
| **CSV**       | ✅         | ✅        |
| **JSON**      | ✅         | ✅        |
| **IPC**       | ✅         | ✅        |
| **Iceberg**   | ✅         | ❌        |

---

## Contributing

We welcome all [contributions](./CONTRIBUTING.md). Please see the [Code of Conduct](./CODE_OF_CONDUCT.md).

## License

Please see the [LICENSE](./LICENSE) for more details.
