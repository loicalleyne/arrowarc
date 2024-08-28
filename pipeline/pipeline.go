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
	"context"
	"fmt"
	"io"
	"log"
	"sync"
	"time"

	"github.com/apache/arrow/go/v17/arrow"
	interfaces "github.com/arrowarc/arrowarc/internal/interfaces"
)

// Metrics stores pipeline processing metrics
type Metrics struct {
	RecordsProcessed int
	TotalBytes       int64
	StartTime        time.Time
	EndTime          time.Time
}

// Duration returns the total duration of the pipeline
func (m *Metrics) Duration() time.Duration {
	return m.EndTime.Sub(m.StartTime)
}

// Report generates a summary of the collected metrics
func (m *Metrics) Report() string {
	return fmt.Sprintf("Records Processed: %d\nTotal Bytes: %d\nTotal Duration: %v\nThroughput (records/second): %f\nThroughput (bytes/second): %f",
		m.RecordsProcessed,
		m.TotalBytes,
		m.Duration(),
		float64(m.RecordsProcessed)/m.Duration().Seconds(),
		float64(m.TotalBytes)/m.Duration().Seconds(),
	)
}

// DataPipeline defines the structure for a data processing pipeline
type DataPipeline struct {
	reader  interfaces.Reader // single reader
	writer  interfaces.Writer // single writer
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
func (dp *DataPipeline) Start(ctx context.Context) (*Metrics, error) {
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

	// Wait for all goroutines to finish and then close the error channel
	go func() {
		wg.Wait()
		close(dp.errCh)
		dp.metrics.EndTime = time.Now()
	}()

	// Listen for errors
	for err := range dp.errCh {
		if err != nil {
			cancel() // Cancel the context to stop all operations
			return nil, err
		}
	}

	// Return the collected metrics
	return dp.metrics, nil
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
				return
			}
			if err != nil {
				log.Printf("Error reading record: %v", err)
				dp.errCh <- err
				return
			}

			// Validate the record
			if record == nil || record.NumCols() == 0 || record.NumRows() == 0 {
				log.Println("Received empty or invalid record, skipping.")
				record.Release() // Release the invalid or empty record to avoid memory leaks
				continue
			}

			// Update metrics
			dp.metrics.RecordsProcessed++
			dp.metrics.TotalBytes += int64(record.Schema().Metadata().FindKey("size"))

			select {
			case ch <- record:
			case <-ctx.Done():
				log.Println("Context canceled, stopping reader.")
				return
			}
		}
	}
}

// startWriter receives records from the channel and writes them using the writer
func (dp *DataPipeline) startWriter(ctx context.Context, ch chan arrow.Record, wg *sync.WaitGroup) {
	defer wg.Done()
	defer dp.writer.Close()

	for {
		select {
		case <-ctx.Done():
			return
		case record, ok := <-ch:
			if !ok {
				return // Channel closed, exit the writer
			}

			// Validate the record before writing, ignore EOF records
			if record == nil || record.NumCols() == 0 || record.NumRows() == 0 {
				log.Println("Received empty or invalid record, skipping.")
				record.Release() // Release the invalid or empty record to avoid memory leaks
				continue
			}

			// Write the record to the writer

			if err := dp.writer.Write(record); err != nil {
				log.Printf("Error writing record: %v", err)
				dp.errCh <- err
				return
			}
			record.Release()
		}
	}
}
