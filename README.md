![Alt text](assets/images/ArrowArcLogo.png)

ArrowArc is a high-performance data integration platform designed to challenge the status quo in big data processing. At its core, ArrowArc is built on the principle that "small data is the new big data"—embracing the idea that speed, simplicity, and efficiency in data processing are paramount, even as the industry continues to focus on massive datasets.

Leveraging the power of Apache Arrow, Go, and other cutting-edge technologies, ArrowArc enables rapid data movement between various sources and sinks. The platform is designed with a singular focus on breakneck speed and simplification of complex data workflows, abstracting away the intricacies of modern data integration while leveraging the capabilities of modern hardware.

By shifting the focus from merely handling large-scale data to optimizing the performance of smaller, more targeted datasets, ArrowArc is poised to redefine what is possible in the realm of data processing. Whether you're dealing with cloud storage, relational databases, or diverse file formats, ArrowArc offers a streamlined, high-performance solution that prioritizes efficiency and ease of use, making it the ideal platform for modern data integration challenges.

---

## Overview

ArrowArc is architected to handle complex data integration tasks with a focus on extreme performance, low latency, and high throughput. The core of ArrowArc is built around a common pattern for data extraction and ingestion, utilizing Go channels as the interface for streaming data between various sources and sinks.

```go
<-chan arrow.Record, <-chan error
```

## Intent of the Architecture

The architecture of ArrowArc is designed to expose a flexible and efficient pattern for both data extraction and ingestion, where all integrations (sources and sinks) follow a common interface using channels to stream Apache Arrow records. This approach ensures that data flows through the system with minimal overhead, maintaining the high-speed processing capabilities of Apache Arrow while leveraging the concurrency features of Go.

By adhering to this common pattern, ArrowArc demonstrates how different components can be integrated into a cohesive system, making it easier to extend and adapt to new data formats and storage solutions.

---

### Key Features

- **Common Extraction and Ingestion Pattern**: All integrations leverage a standardized approach using a common interface for both input and output, simplifying the development and extension of connectors and sinks.
- **Streaming Data Integration**: Efficiently stream data between sources and sinks with minimal overhead.
- **Support for Multiple Formats**: Write data to Parquet, CSV, and more, directly to destinations like Google Cloud Storage (GCS).
- **Go-Based Architecture**: Built entirely in Go, ArrowArc maximizes performance, scalability, and concurrency management.
- **Apache Arrow Integration**: Utilizing Apache Arrow for in-memory columnar data processing, ArrowArc ensures high-speed data handling and interoperability between systems.
- **Extensible and Modular Design**: Easily extend ArrowArc to support additional data formats or cloud storage providers by following the established patterns.

## Components

### Integrations

ArrowArc is engineered with a singular focus: achieving breakneck speed and simplifying data integration workflows centered around Apache Arrow. By abstracting away much of the inherent complexity, ArrowArc enables developers to integrate with various platforms, storage providers, and data formats, both for extraction and ingestion purposes.

#### Example: Google Cloud BigQuery

```go
type BigQueryConnector struct {
    Client *bqStorage.BigQueryReadClient
}

func (bq *BigQueryConnector) GetBigQueryArrowStream(ctx context.Context, projectID string, datasetID string, tableID string, format string) (<-chan arrow.Record, error) {
}
```

### Sinks

Data can be streamed and written to various sinks, such as Google Cloud Storage and Postgres. ArrowArc supports writing data in formats like Parquet and CSV or platforms like DuckDB, with more integrations coming.

#### Example: Google Cloud Storage

```go
type GCSSink struct {
    client     *storage.Client
    bucketName string
}

func (s *GCSSink) WriteToGCS(ctx context.Context, records <-chan arrow.Record, filePath string, format FileFormat, delimiter ...rune) error {
}
```

---

### Gluing it All Together

ArrowArc simplifies high-performance data synchronization, allowing you to accomplish tasks like rewriting a Parquet file with minimal code.

```go
// Stream data from a Parquet file
recordChan, err := GetParquetArrowStream(ctx, inFilePath, true, 1000000)
if err != nil {
    log.Fatalf("Failed to read Parquet: %v", err)
}

// Write the streamed data back to a new Parquet file
err = WriteParquet(ctx, outputFilePath, recordChan)
if err != nil {
    log.Fatalf("Failed to write Parquet: %v", err)
}
```

With just two lines of code, ArrowArc can stream data from a Parquet file and write it back out, all while leveraging the power of Apache Arrow for in-memory data processing. This minimalistic approach ensures that you can focus on building effective solutions without getting bogged down by the intricacies of data handling.

---

## :page_facing_up: License

ArrowArc is bestowed upon the realm under the MIT License. Refer to the [LICENSE](./LICENSE) scroll for more details.

## :bow_and_arrow: Author

Thomas F McGeehan V - The Robin Hood of Data!
