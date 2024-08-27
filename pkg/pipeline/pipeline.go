package pipeline

import (
	"context"
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
	StartTime        time.Time
	EndTime          time.Time
}

// Duration returns the total duration of the pipeline
func (m *Metrics) Duration() time.Duration {
	return m.EndTime.Sub(m.StartTime)
}

// Report logs the collected metrics
func (m *Metrics) Report() {
	log.Printf("Records Processed: %d", m.RecordsProcessed)
	log.Printf("Total Duration: %v", m.Duration())
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

// Start begins the pipeline processing
func (dp *DataPipeline) Start(ctx context.Context) error {
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
		dp.metrics.Report() // Report the metrics once processing is complete
	}()

	// Listen for errors
	for err := range dp.errCh {
		if err != nil {
			cancel() // Cancel the context to stop all operations
			return err
		}
	}

	return nil
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

			// Update metrics
			dp.metrics.RecordsProcessed++

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
			if err := dp.writer.Write(record); err != nil {
				log.Printf("Error writing record: %v", err)
				dp.errCh <- err
				return
			}
			record.Release()
		}
	}
}
