package pipeline

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"sync"
	"time"

	"github.com/apache/arrow/go/v17/arrow"
	interfaces "github.com/arrowarc/arrowarc/internal/interfaces"
)

// Metrics stores pipeline processing metrics
// Metrics stores pipeline processing metrics
type Metrics struct {
	sync.Mutex
	RecordsProcessed int
	TotalBytes       int64
	StartTime        time.Time
	EndTime          time.Time
	TotalDuration    time.Duration
	Throughput       float64
	ThroughputBytes  float64
}

// UpdateMetrics calculates the total duration, throughput, and throughput in bytes.
func (m *Metrics) UpdateMetrics() {
	m.Lock()
	defer m.Unlock()

	// Calculate total duration
	m.TotalDuration = m.EndTime.Sub(m.StartTime)

	// Avoid division by zero in throughput calculation
	if m.TotalDuration > 0 {
		m.Throughput = float64(m.RecordsProcessed) / m.TotalDuration.Seconds()
		m.ThroughputBytes = float64(m.TotalBytes) / m.TotalDuration.Seconds()
	} else {
		m.Throughput = 0
		m.ThroughputBytes = 0
	}
}

// Report generates a summary of the collected metrics
// Report generates a summary of the collected metrics
func (m *Metrics) Report() string {
	m.Lock()
	defer m.Unlock()

	report := struct {
		RecordsProcessed int           `json:"records_processed"`
		TotalBytes       int64         `json:"total_bytes"`
		StartTime        time.Time     `json:"start_time"`
		EndTime          time.Time     `json:"end_time"`
		TotalDuration    time.Duration `json:"total_duration"`
		Throughput       float64       `json:"throughput"`
		ThroughputBytes  float64       `json:"throughput_bytes"`
	}{
		RecordsProcessed: m.RecordsProcessed,
		TotalBytes:       m.TotalBytes,
		StartTime:        m.StartTime,
		EndTime:          m.EndTime,
		TotalDuration:    m.TotalDuration,
		Throughput:       m.Throughput,
		ThroughputBytes:  m.ThroughputBytes,
	}

	jsonData, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return fmt.Sprintf("Error generating report: %v", err)
	}

	return string(jsonData)
}

// Duration returns the total duration of the pipeline
func (m *Metrics) Duration() time.Duration {
	return m.EndTime.Sub(m.StartTime)
}

// DataPipeline defines the structure for a data processing pipeline
type DataPipeline struct {
	reader  interfaces.Reader
	writer  interfaces.Writer
	errCh   chan error
	metrics *Metrics
}

// NewDataPipeline creates a new DataPipeline instance
func NewDataPipeline(reader interfaces.Reader, writer interfaces.Writer) *DataPipeline {
	return &DataPipeline{
		reader: reader,
		writer: writer,
		errCh:  make(chan error, 1), // Buffer size of 1 to capture any errors
		metrics: &Metrics{
			StartTime: time.Now(),
		},
	}
}

// Start begins the pipeline processing and returns the metrics report
func (dp *DataPipeline) Start(ctx context.Context) (string, error) {
	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Channel for records with a buffer size of 100
	recordChan := make(chan arrow.Record, 100)

	// Start the reader
	wg.Add(1)
	go dp.startReader(ctx, recordChan, &wg)

	// Start the writer
	wg.Add(1)
	go dp.startWriter(ctx, recordChan, &wg)

	// Monitor goroutines and handle errors
	go func() {
		wg.Wait()
		close(dp.errCh)
		dp.metrics.Lock()
		dp.metrics.EndTime = time.Now()
		dp.metrics.Unlock()
		dp.metrics.UpdateMetrics()
	}()

	// Listen for errors
	for err := range dp.errCh {
		if err != nil {
			cancel() // Cancel the context to stop all operations
			return "", err
		}
	}

	// Create a transport report
	report := struct {
		RecordsProcessed int     `json:"records_processed"`
		TotalBytes       int64   `json:"total_bytes"`
		TotalDuration    string  `json:"total_duration"`
		Throughput       float64 `json:"throughput_records_per_second"`
		ThroughputBytes  float64 `json:"throughput_bytes_per_second"`
	}{
		RecordsProcessed: dp.metrics.RecordsProcessed,
		TotalBytes:       dp.metrics.TotalBytes,
		TotalDuration:    dp.metrics.TotalDuration.String(),
		Throughput:       dp.metrics.Throughput,
		ThroughputBytes:  dp.metrics.ThroughputBytes,
	}

	jsonReport, err := PrettyPrint(report)
	if err != nil {
		return "", fmt.Errorf("failed to marshal transport report: %w", err)
	}

	return jsonReport, nil
}

// startReader reads records from the reader and sends them to the channel
func (dp *DataPipeline) startReader(ctx context.Context, ch chan arrow.Record, wg *sync.WaitGroup) {
	defer wg.Done()
	defer close(ch) // Close the channel to signal writer when done
	defer dp.reader.Close()

	for {
		select {
		case <-ctx.Done():
			log.Println("Context canceled, stopping reader.")
			return
		default:
			record, err := dp.reader.Read()
			if err == io.EOF {
				log.Println("Reached end of reader stream.")
				return
			}
			if err != nil {
				log.Printf("Error reading record: %v", err)
				dp.errCh <- err
				return
			}

			if record == nil || record.NumCols() == 0 || record.NumRows() == 0 {
				log.Println("Received empty or invalid record, skipping.")
				record.Release() // Release the invalid or empty record to avoid memory leaks
				continue
			}

			dp.metrics.Lock()
			dp.metrics.RecordsProcessed += int(record.NumRows())
			recordSize := calculateRecordSize(record)
			dp.metrics.TotalBytes += recordSize
			dp.metrics.Unlock()

			select {
			case ch <- record:
			case <-ctx.Done():
				log.Println("Context canceled, stopping reader.")
				return
			}
		}
	}
}

// calculateRecordSize calculates the approximate size of a record based on its columns
func calculateRecordSize(record arrow.Record) int64 {
	size := int64(0)
	for _, col := range record.Columns() {
		for _, buf := range col.Data().Buffers() {
			if buf != nil {
				size += int64(buf.Len())
			}
		}
	}
	return size
}

// startWriter receives records from the channel and writes them using the writer
func (dp *DataPipeline) startWriter(ctx context.Context, ch chan arrow.Record, wg *sync.WaitGroup) {
	defer wg.Done()
	defer dp.writer.Close()

	for {
		select {
		case <-ctx.Done():
			log.Println("Context canceled, stopping writer.")
			return
		case record, ok := <-ch:
			if !ok {
				log.Println("Channel closed, stopping writer.")
				return // Exit the writer when channel is closed
			}

			if record == nil || record.NumCols() == 0 || record.NumRows() == 0 {
				log.Println("Received empty or invalid record, skipping.")
				record.Release() // Release the invalid or empty record to avoid memory leaks
				continue
			}

			if err := dp.writer.Write(record); err != nil {
				log.Printf("Error writing record: %v", err)
				dp.errCh <- err
				return
			}
			record.Release()
		}
	}
}

// rettyPrint marshals the provided value into a pretty-printed JSON string.
func PrettyPrint(v interface{}) (string, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetIndent("", "  ")
	if err := enc.Encode(v); err != nil {
		return "", fmt.Errorf("json: failed to pretty print: %w", err)
	}
	return buf.String(), nil
}
