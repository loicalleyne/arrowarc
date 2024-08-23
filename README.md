![Alt text](assets/images/ArrowArcLogo.png)

[![Go Report Card](https://goreportcard.com/badge/github.com/ArrowArc/ArrowArc)](https://goreportcard.com/report/github.com/ArrowArc/ArrowArc) [![ArrowArc Build](https://github.com/ArrowArc/ArrowArc/actions/workflows/ci.yml/badge.svg)](https://github.com/ArrowArc/ArrowArc/actions/workflows/ci.yml)

ArrowArc is a high-performance data integration platform designed to challenge the status quo in big data processing. At its core, ArrowArc is built on the principle that **"small data is the new big data,"**—ArrowArc recognizes that, for most use cases, traditional Big Data tools are often an expensive overkill. With modern hardware and technological advancements, ArrowArc enables the efficient processing of substantial data volumes on a single node, making it a powerful and practical alternative.

---

## Overview

ArrowArc is architected to handle complex data integration tasks with a focus on extreme performance, low latency, and high throughput. At the heart of ArrowArc's design is a commitment to efficient, concurrent data processing—an area where Go excels due to its built-in support for concurrency and its lightweight goroutines.

### Go Channels for Data Streaming

The core of ArrowArc is built around a common pattern for data extraction and ingestion, utilizing Go channels as the interface for streaming data between various sources and sinks:

```go
<-chan arrow.Record, <-chan error
```

## Intent of the Architecture

The architecture of ArrowArc is designed to expose a flexible and efficient pattern for both data extraction and ingestion, where all integrations (sources and sinks) follow a common interface using channels to stream Apache Arrow records. This approach ensures that data flows through the system with minimal overhead, maintaining the high-speed processing capabilities of Apache Arrow while leveraging the concurrency features of Go.

By adhering to this common pattern, ArrowArc demonstrates how different components can be integrated into a cohesive system, making it easier to extend and adapt to new data formats and storage solutions.

---

## Components

### Integrations

ArrowArc is engineered with a singular focus: achieving breakneck speed and simplifying data integration workflows centered around Apache Arrow. By abstracting away much of the inherent complexity, ArrowArc enables developers to integrate with various platforms, storage providers, and data formats, both for extraction and ingestion purposes.

#### Example: Google Cloud BigQuery

```go
func ReadBigQueryStream(ctx context.Context, projectID, datasetID, tableID string) (<-chan arrow.Record, <-chan error) {
}
```

### Sinks

Data can be streamed and written to various sinks, such as Google Cloud Storage and Postgres. ArrowArc supports writing data in formats like Parquet and CSV or platforms like DuckDB, with more integrations coming.

#### Example: Filesystem Sink

```go
func WriteParquetFileStream(ctx context.Context, filePath string, recordChan <-chan arrow.Record) <-chan error {
}
```

---

### Gluing it All Together

ArrowArc simplifies high-performance data synchronization, enabling tasks like rewriting a Parquet file with minimal code.

```go
// Stream data from a Parquet file using a memory map in 1,000,000 record batches
recordChan, errChan := GetParquetArrowStream(ctx, inputFilePath, true, 1000000)

// Handle errors
go func() {
    if err := <-errChan; err != nil {
        log.Fatalf("Error streaming from Parquet file: %v", err)
    }
}()

// Write data to an output Parquet file
if err := WriteParquetFileStream(ctx context.Context, filePath string, recordChan <-chan arrow.Record); err != nil {
    log.Fatalf("Error writing to output Parquet file: %v", err)
}
```

In just a few lines of code, ArrowArc can stream data from a Parquet file and write it back out, all while leveraging the power of Apache Arrow for in-memory data processing. This streamlined approach allows you to focus on building effective solutions without getting entangled in the complexities of data handling.

Additionally, ArrowArc makes it easy to read data once from a source and write it to multiple sinks efficiently using ```TransportStream```.

```go
ctx := context.Background()
sourceChan := make(chan arrow.Record)
defer close(sourceChan)

// Define multiple sinks
jsonSink := func(ctx context.Context, recordChan <-chan arrow.Record) <-chan error {
    return WriteJSONFileStream(ctx, "output.json", recordChan)
}

parquetSink := func(ctx context.Context, recordChan <-chan arrow.Record) <-chan error {
    return WriteParquetFileStream(ctx, "output.parquet", recordChan)
}

// Use TransportStream with both JSON and Parquet sinks
errChan := transport.TransportStream(ctx, sourceChan, jsonSink, parquetSink)

if err := <-errChan; err != nil {
    fmt.Printf("TransportStream encountered an error: %v\n", err)
} else {
    fmt.Println("TransportStream completed successfully")
}
```

---

### ArrowArc Feature Matrix

This tables below indicate the status of the planned features of ArrowArc, including command line utilities, integrations, and cloud storage provider support. The status of each feature is indicated as follows:

- `✅` - Implemented
- `🚧` - In Progress
- `❌` - Not Started

### Features Overview

#### Command Line Utilities

| Utility             | Status       |
|---------------------|--------------|
| **Transport** | 🚧           |
| **Rewrite Parquet** | ✅           |
| **Generate Parquet** | ✅           |
| **Convert CSV**     | ✅           |
| **Sync Table**      | ❌           |

---

#### Integration Types

##### 1. **Database Integrations**

| Database        | Extraction | Ingestion |
|-----------------|------------|-----------|
| **PostgreSQL**  | ✅         | 🚧        |
| **MySQL**       | 🚧         | ❌        |
| **Oracle**      | ❌         | ❌        |
| **BigQuery**    | ✅         | 🚧        |
| **Snowflake**   | ❌         | ❌        |
| **DuckDB**      | ✅         | ✅        |
| **SQLite**   | ❌         | ❌        |
| **Spanner**   | ❌         | ❌        |
| **CockroachDB**  | ✅         | 🚧        |
| **Flight**      | ❌         | ❌        |

##### 2. **Cloud Storage Integrations**

| Provider                         | Extraction | Ingestion |
|----------------------------------|------------|-----------|
| **Google Cloud Storage (GCS)**   | ✅         | ✅        |
| **Amazon S3**                    | ❌         | ❌        |
| **Azure Blob Storage**           | ❌         | ❌        |

##### 3. **Filesystem Formats**

| Format        | Extraction | Ingestion |
|---------------|------------|-----------|
| **Parquet**   | ✅         | ✅        |
| **Avro**      | ✅         | ❌        |
| **CSV**       | ✅         | ✅        |
| **JSON**      | ✅         | ✅        |
| **IPC**       | ✅         | ✅        |
| **Iceberg**   | ❌         | ❌        |

---

## Contributing

We welcome all [contributions](./CONTRIBUTING.md). Please see the [Code of Conduct](./CODE_OF_CONDUCT.md).

## :page_facing_up: License

Please see the [LICENSE](./LICENSE) for more details.
