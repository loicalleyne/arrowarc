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

package integrations

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	bqStorage "cloud.google.com/go/bigquery/storage/apiv1"
	storagepb "cloud.google.com/go/bigquery/storage/apiv1/storagepb"
	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/ipc"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/googleapis/gax-go/v2"
	"google.golang.org/api/option"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// BigQueryReadClient is a client for interacting with BigQuery Storage API.
type BigQueryReadClient struct {
	client      *bqStorage.BigQueryReadClient
	callOptions *BigQueryReadCallOptions
}

// BigQueryReadCallOptions contains the retry settings for each method of BigQueryReadClient.
type BigQueryReadCallOptions struct {
	CreateReadSession []gax.CallOption
	ReadRows          []gax.CallOption
}

// NewBigQueryReadClient creates a new BigQueryReadClient with customized options.
func NewBigQueryReadClient(ctx context.Context, opts ...option.ClientOption) (*BigQueryReadClient, error) {

	client, err := bqStorage.NewBigQueryReadClient(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create BigQueryReadClient: %w", err)
	}

	return &BigQueryReadClient{
		client:      client,
		callOptions: defaultBigQueryReadCallOptions(),
	}, nil
}

func defaultBigQueryReadCallOptions() *BigQueryReadCallOptions {
	return &BigQueryReadCallOptions{
		CreateReadSession: []gax.CallOption{
			gax.WithTimeout(600 * time.Second),
			gax.WithRetry(func() gax.Retryer {
				return gax.OnCodes([]codes.Code{
					codes.DeadlineExceeded,
					codes.Unavailable,
				}, gax.Backoff{
					Initial:    100 * time.Millisecond,
					Max:        60 * time.Second,
					Multiplier: 1.30,
				})
			}),
		},
		ReadRows: []gax.CallOption{
			gax.WithRetry(func() gax.Retryer {
				return gax.OnCodes([]codes.Code{
					codes.Unavailable,
				}, gax.Backoff{
					Initial:    100 * time.Millisecond,
					Max:        60 * time.Second,
					Multiplier: 1.30,
				})
			}),
		},
	}
}

// ReadBigQueryStream reads data from a BigQuery table in Arrow format, handling multiple streams.
func (bq *BigQueryReadClient) ReadBigQueryStream(ctx context.Context, projectID, datasetID, tableID string, format string) (<-chan arrow.Record, <-chan error) {
	dataFormat := storagepb.DataFormat_ARROW
	if format != "arrow" {
		errChan := make(chan error, 1)
		errChan <- fmt.Errorf("unsupported format: %s", format)
		close(errChan)
		return nil, errChan
	}

	req := &storagepb.CreateReadSessionRequest{
		Parent: fmt.Sprintf("projects/%s", projectID),
		ReadSession: &storagepb.ReadSession{
			Table:      fmt.Sprintf("projects/%s/datasets/%s/tables/%s", projectID, datasetID, tableID),
			DataFormat: dataFormat,
		},
		MaxStreamCount: 5,
	}

	session, err := bq.client.CreateReadSession(ctx, req, bq.callOptions.CreateReadSession...)
	if err != nil {
		errChan := make(chan error, 1)
		errChan <- fmt.Errorf("failed to create read session: %w", err)
		close(errChan)
		return nil, errChan
	}

	if len(session.GetStreams()) == 0 {
		errChan := make(chan error, 1)
		errChan <- fmt.Errorf("no streams available in session")
		close(errChan)
		return nil, errChan
	}

	recordChan := make(chan arrow.Record)
	errChan := make(chan error, 1)
	var wg sync.WaitGroup

	for _, stream := range session.GetStreams() {
		wg.Add(1)
		go func(streamName string) {
			defer wg.Done()
			bq.processStream(ctx, session.GetArrowSchema().GetSerializedSchema(), streamName, recordChan, errChan)
		}(stream.Name)
	}

	// Wait for all streams to complete
	go func() {
		wg.Wait()
		close(recordChan)
		close(errChan)
	}()

	return recordChan, errChan
}

func (bq *BigQueryReadClient) processStream(ctx context.Context, schemaBytes []byte, streamName string, recordChan chan<- arrow.Record, errChan chan<- error) {
	stream, err := bq.client.ReadRows(ctx, &storagepb.ReadRowsRequest{ReadStream: streamName}, bq.callOptions.ReadRows...)
	if err != nil {
		errChan <- fmt.Errorf("failed to stream rows: %w", err)
		return
	}

	mem := memory.NewGoAllocator()

	// Initialize the schema using the schema bytes
	buf := bytes.NewBuffer(schemaBytes)
	r, err := ipc.NewReader(buf, ipc.WithAllocator(mem))
	if err != nil {
		errChan <- fmt.Errorf("failed to create IPC reader for schema: %w", err)
		return
	}
	schema := r.Schema()

	for {
		response, err := stream.Recv()
		if err != nil {
			if status.Code(err) == codes.Canceled || err == io.EOF {
				return
			}
			errChan <- fmt.Errorf("error receiving stream response: %w", err)
			return
		}

		undecodedBatch := response.GetArrowRecordBatch().GetSerializedRecordBatch()
		if len(undecodedBatch) > 0 {
			// Reset the buffer and reuse it for each batch
			buf = bytes.NewBuffer(schemaBytes)
			buf.Write(undecodedBatch)

			r, err = ipc.NewReader(buf, ipc.WithAllocator(mem), ipc.WithSchema(schema))
			if err != nil {
				errChan <- fmt.Errorf("failed to create IPC reader for batch: %w", err)
				return
			}

			for r.Next() {
				record := r.Record()
				recordChan <- record
			}

			if err := r.Err(); err != nil {
				errChan <- fmt.Errorf("error reading records: %w", err)
				return
			}
		}
	}
}
