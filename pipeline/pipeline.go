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

package pipeline

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"math"

	"github.com/apache/arrow/go/v17/arrow"
	interfaces "github.com/arrowarc/arrowarc/internal/interfaces"
)

// Metrics stores pipeline processing metrics
type Metrics struct {
	RecordsProcessed int64
	TotalBytes       int64
	StartTime        time.Time
	EndTime          time.Time
	TotalDuration    int64 // nanoseconds
	Throughput       int64 // records per second * 100 (for two decimal places)
	ThroughputBytes  int64 // bytes per second
	endTimeUnix      int64
}

// UpdateMetrics calculates the total duration, throughput, and throughput in bytes.
func (m *Metrics) UpdateMetrics() {
	endTime := time.Now()
	m.EndTime = endTime
	atomic.StoreInt64(&m.endTimeUnix, endTime.UnixNano())

	recordsProcessed := atomic.LoadInt64(&m.RecordsProcessed)
	totalBytes := atomic.LoadInt64(&m.TotalBytes)

	duration := endTime.Sub(m.StartTime)
	atomic.StoreInt64(&m.TotalDuration, int64(duration))

	if duration > 0 {
		throughput := float64(recordsProcessed) / duration.Seconds()
		throughputBytes := float64(totalBytes) / duration.Seconds()
		atomic.StoreInt64(&m.Throughput, int64(throughput*100))
		atomic.StoreInt64(&m.ThroughputBytes, int64(throughputBytes))
	}
}

// Report generates a summary of the collected metrics
func (m *Metrics) Report() string {
	report := generateMetricsReport(m)
	jsonData, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return fmt.Sprintf("Error generating report: %v", err)
	}
	return string(jsonData)
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
	errChan := make(chan error, 1)
	go func() {
		wg.Wait()
		close(dp.errCh)
		dp.metrics.UpdateMetrics()
		close(errChan)
	}()

	// Listen for errors and handle context cancellation
	select {
	case err := <-dp.errCh:
		if err != nil {
			cancel() // Cancel the context to stop all operations
			return "", err
		}
	case <-errChan:
		// All goroutines have finished without error
	case <-ctx.Done():
		return "", ctx.Err()
	case <-time.After(30 * time.Minute): // Adjust timeout as needed
		cancel()
		return "", fmt.Errorf("pipeline execution timed out")
	}

	// Create a transport report
	report := generateMetricsReport(dp.metrics)
	jsonReport, err := PrettyPrint(report)
	if err != nil {
		return "", fmt.Errorf("failed to marshal transport report: %w", err)
	}

	return jsonReport, nil
}

func generateMetricsReport(metrics *Metrics) map[string]interface{} {
	recordsProcessed := atomic.LoadInt64(&metrics.RecordsProcessed)
	totalBytes := atomic.LoadInt64(&metrics.TotalBytes)
	duration := time.Duration(atomic.LoadInt64(&metrics.TotalDuration))
	throughput := float64(atomic.LoadInt64(&metrics.Throughput)) / 100
	throughputBytes := atomic.LoadInt64(&metrics.ThroughputBytes)

	return map[string]interface{}{
		"StartTime":        metrics.StartTime.Format(time.RFC3339),
		"EndTime":          time.Unix(0, atomic.LoadInt64(&metrics.endTimeUnix)).Format(time.RFC3339),
		"RecordsProcessed": recordsProcessed,
		"TotalBytes":       formatBytes(totalBytes),
		"TotalDuration":    formatDuration(duration),
		"Throughput":       formatThroughput(throughput),
		"ThroughputBytes":  formatThroughputBytes(float64(throughputBytes)),
	}
}

func formatBytes(bytes int64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.2f %cB", float64(bytes)/float64(div), "KMGTPE"[exp])
}

func formatDuration(d time.Duration) string {
	return d.Round(time.Millisecond).String()
}

func formatThroughput(t float64) string {
	return fmt.Sprintf("%.2f records/second", math.Round(t*100)/100)
}

func formatThroughputBytes(t float64) string {
	return fmt.Sprintf("%s/second", formatBytes(int64(t)))
}

// startReader reads records from the reader and sends them to the channel
func (dp *DataPipeline) startReader(ctx context.Context, ch chan arrow.Record, wg *sync.WaitGroup) {
	defer wg.Done()
	defer close(ch)
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
				select {
				case dp.errCh <- fmt.Errorf("reader error: %w", err):
				default:
					log.Printf("Error channel full, discarding error: %v", err)
				}
				return
			}

			if record == nil || record.NumCols() == 0 || record.NumRows() == 0 {
				log.Println("Received empty or invalid record, skipping.")
				record.Release()
				continue
			}

			atomic.AddInt64(&dp.metrics.RecordsProcessed, int64(record.NumRows()))
			recordSize := calculateRecordSize(record)
			atomic.AddInt64(&dp.metrics.TotalBytes, recordSize)

			select {
			case ch <- record:
			case <-ctx.Done():
				log.Println("Context canceled, stopping reader.")
				record.Release()
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
				select {
				case dp.errCh <- fmt.Errorf("writer error: %w", err):
				default:
					log.Printf("Error channel full, discarding error: %v", err)
				}
				record.Release()
				return
			}
			record.Release()
		}
	}
}

// PrettyPrint marshals the provided value into a pretty-printed JSON string.
func PrettyPrint(v interface{}) (string, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetIndent("", "  ")
	if err := enc.Encode(v); err != nil {
		return "", fmt.Errorf("json: failed to pretty print: %w", err)
	}
	return buf.String(), nil
}

// Done returns a channel that the pipeline can be waited on
func (dp *DataPipeline) Done() <-chan error {
	doneCh := make(chan error, 1)

	go func() {
		defer close(doneCh)

		if err, ok := <-dp.errCh; ok {
			doneCh <- err
		}
	}()

	return doneCh
}

// Metrics returns the pipeline metrics
func (dp *DataPipeline) Metrics() *Metrics {
	return dp.metrics
}
