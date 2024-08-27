![Alt text](assets/images/ArrowArcLogo.png)

[![Go Report Card](https://goreportcard.com/badge/github.com/arrowarc/arrowarc)](https://goreportcard.com/report/github.com/arrowarc/arrowarc) [![ArrowArc Build](https://github.com/arrowarc/arrowarc/actions/workflows/ci.yml/badge.svg)](https://github.com/arrowarc/arrowarc/actions/workflows/ci.yml)

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
reader, err := bq.NewBigQueryArrowReader(ctx, projectID, datasetID, tableID)

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

## Technical Deep Dive

ArrowArc is engineered to maximize data processing speed by leveraging the capabilities of Apache Arrow, a columnar in-memory data format that enables efficient analytics and data exchange. At its core, ArrowArc takes full advantage of Apache Arrow’s design principles, such as zero-copy reads and efficient serialization, to achieve high-performance data processing across various systems.

### Leveraging Apache Arrow

Apache Arrow is the backbone of ArrowArc, providing a standardized format for representing complex data structures in memory. Arrow's columnar layout is designed to optimize access patterns for modern CPUs, enabling efficient operations like filtering, aggregation, and vectorized processing. In ArrowArc, Arrow's in-memory format is used extensively to facilitate high-throughput data exchanges between different systems, such as databases, cloud storage, and file formats like Parquet and Avro.

ArrowArc employs Arrow’s IPC (Inter-Process Communication) mechanisms for moving data between components. This allows ArrowArc to maintain the data in its native columnar format throughout the pipeline, reducing the overhead of data serialization and deserialization. The result is a significant reduction in CPU and memory usage, which is crucial for achieving the high performance that ArrowArc targets.

### The Role of FlatBuffers in ArrowArc

FlatBuffers play a critical role in the high-performance nature of Apache Arrow, and by extension, ArrowArc. FlatBuffers are a memory-efficient serialization library developed by Google, designed to provide fast access to serialized data without requiring unpacking or extra copying. Arrow uses FlatBuffers to define its metadata, such as schema and record batch structures, allowing for rapid data exchange between systems.

In ArrowArc, FlatBuffers are leveraged indirectly through Arrow’s data structures. When data is moved through an ArrowArc pipeline, the metadata—structured with FlatBuffers—enables quick interpretation and manipulation of the data without unnecessary memory overhead. This integration of Arrow and FlatBuffers ensures that ArrowArc can handle large volumes of data with minimal latency, making it well-suited for scenarios where data needs to be streamed or processed in real-time.

### High-Performance Data Exchange

The combination of Apache Arrow’s columnar format and FlatBuffers' efficient serialization allows ArrowArc to excel in scenarios where data must be exchanged between different storage formats and computing environments. ArrowArc pipelines are designed to minimize the bottlenecks typically associated with data processing, such as the need for intermediate conversions or data copying.

For example, when streaming data from BigQuery to DuckDB, ArrowArc maintains the data in Arrow's format as much as possible, reducing the need for transformations that could introduce latency. This direct approach to data handling ensures that the data pipeline remains as lightweight and efficient as possible, allowing for near real-time processing even with large datasets.
