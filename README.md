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

## High-Performance Data Exchange: Engineering Deep Dive

ArrowArc's architecture is meticulously crafted to optimize data exchange performance by combining the unique strengths of Apache Arrow's columnar format with the efficient serialization capabilities of FlatBuffers. This technical synergy is at the heart of ArrowArc's ability to maintain a lean and responsive data pipeline, effectively mitigating common performance bottlenecks such as intermediate data conversions, redundant copying, and format mismatches.

### Apache Arrow’s Columnar Format

At its core, Apache Arrow uses a columnar memory layout that is designed to maximize CPU cache efficiency and enable SIMD (Single Instruction, Multiple Data) operations, which are critical for high-performance data processing. Unlike row-based formats, Arrow stores data in contiguous memory blocks, allowing ArrowArc to efficiently access, filter, and aggregate large datasets with minimal cache misses. This format is particularly advantageous for analytical workloads that involve large-scale scans and transformations, as it minimizes the overhead associated with fetching and processing data.

ArrowArc leverages this columnar format to keep data in its most optimal state throughout the entire pipeline. Whether data is being ingested from an external source like BigQuery or written to a storage system like DuckDB, ArrowArc avoids unnecessary conversions by preserving the Arrow format. This approach not only reduces the computational load but also minimizes memory allocations and deallocations, which can significantly impact performance in high-throughput systems.

### FlatBuffers’ Efficient Serialization

FlatBuffers, developed by Google, is a cross-platform serialization library that allows for zero-copy deserialization. This means that data serialized with FlatBuffers can be accessed directly in its serialized form without the need for unpacking or copying, which is a common source of latency in data processing systems.

In ArrowArc, FlatBuffers is indirectly utilized through Apache Arrow’s metadata structures. Arrow uses FlatBuffers to define its schemas, record batches, and other metadata, which are essential for interpreting and manipulating data within the pipeline. By employing FlatBuffers, ArrowArc can quickly interpret and utilize this metadata, ensuring that data remains in its columnar format without the overhead of traditional serialization/deserialization processes. This is particularly beneficial when data needs to be exchanged between different components of the pipeline, as it allows ArrowArc to maintain a high degree of efficiency and speed.

### Minimizing Data Transformation Overhead

A key design principle of ArrowArc is the minimization of data transformation overhead. Traditional data pipelines often require data to be serialized, deserialized, and converted between different formats as it moves between stages of the pipeline. Each of these steps introduces latency, consumes CPU cycles, and increases the risk of bottlenecks, especially when dealing with large volumes of data.

ArrowArc circumvents these issues by keeping data in its native Arrow format as much as possible. For example, when streaming data from BigQuery to DuckDB, ArrowArc ensures that the data remains in Arrow’s columnar format throughout the process. By doing so, ArrowArc eliminates the need for intermediate conversions that could introduce delays, ensuring that the pipeline remains streamlined and efficient.

### Achieving Near-Real-Time Performance

The combination of Apache Arrow’s efficient in-memory format and FlatBuffers’ lightweight serialization allows ArrowArc to achieve near-real-time performance, even when processing large datasets. By minimizing unnecessary data transformations and leveraging highly efficient data structures, ArrowArc is capable of sustaining high throughput with low latency.

In practical terms, this means that ArrowArc can handle data streaming and transformation tasks that would typically require more complex and resource-intensive systems. Whether the task involves moving data from cloud storage, processing it in an on-premises database, or writing it to various file formats, ArrowArc is designed to handle these operations with minimal overhead, making it a powerful tool for engineers looking to push the limits of modern data processing.
