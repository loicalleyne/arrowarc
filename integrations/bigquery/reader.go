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
	memoryPool "github.com/arrowarc/arrowarc/internal/memory"
	helper "github.com/arrowarc/arrowarc/pkg/common/utils"
	"github.com/googleapis/gax-go/v2"
	"google.golang.org/api/option"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type BigQueryReadClient struct {
	client      *bqStorage.BigQueryReadClient
	callOptions *BigQueryReadCallOptions
}

type BigQueryReadCallOptions struct {
	CreateReadSession []gax.CallOption
	ReadRows          []gax.CallOption
	ReadStream        []gax.CallOption
}

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

func (bq *BigQueryReadClient) NewBigQueryReader(ctx context.Context, projectID, datasetID, tableID string) (*BigQueryReader, error) {
	// Define the ArrowSerializationOptions with compression
	arrowSerializationOptions := &storagepb.ArrowSerializationOptions{
		BufferCompression: storagepb.ArrowSerializationOptions_LZ4_FRAME, // Use LZ4_FRAME compression
	}

	// Create ReadOptions and set ArrowSerializationOptions
	readOptions := &storagepb.ReadSession_TableReadOptions{
		OutputFormatSerializationOptions: &storagepb.ReadSession_TableReadOptions_ArrowSerializationOptions{
			ArrowSerializationOptions: arrowSerializationOptions,
		},
	}

	// Create the ReadSession request
	req := &storagepb.CreateReadSessionRequest{
		Parent: fmt.Sprintf("projects/%s", projectID),
		ReadSession: &storagepb.ReadSession{
			Table:       fmt.Sprintf("projects/%s/datasets/%s/tables/%s", projectID, datasetID, tableID),
			DataFormat:  storagepb.DataFormat_ARROW,
			ReadOptions: readOptions,
		},
		MaxStreamCount: 1,
	}
	session, err := bq.client.CreateReadSession(ctx, req, bq.callOptions.CreateReadSession...)
	if err != nil {
		return nil, fmt.Errorf("failed to create read session: %w", err)
	}

	if len(session.GetStreams()) == 0 {
		return nil, fmt.Errorf("no streams available in session")
	}

	alloc := memoryPool.GetAllocator()

	// Ensure schema is properly initialized
	schemaBytes := session.GetArrowSchema().GetSerializedSchema()
	if len(schemaBytes) == 0 {
		return nil, fmt.Errorf("failed to retrieve schema bytes")
	}

	// Initialize the IPC reader for schema validation
	buf := bytes.NewBuffer(schemaBytes)
	ipcReader, err := ipc.NewReader(buf, ipc.WithAllocator(alloc))
	if err != nil {
		return nil, fmt.Errorf("failed to create initial IPC reader for schema: %w", err)
	}

	return &BigQueryReader{
		ctx:         ctx,
		client:      bq.client,
		callOptions: bq.callOptions,
		schemaBytes: schemaBytes,
		streams:     session.GetStreams(),
		mem:         alloc,
		buf:         bytes.NewBuffer(nil),
		r:           ipcReader,
	}, nil
}

type BigQueryReader struct {
	ctx         context.Context
	client      *bqStorage.BigQueryReadClient
	callOptions *BigQueryReadCallOptions
	schemaBytes []byte
	streams     []*storagepb.ReadStream
	mem         memory.Allocator

	r    *ipc.Reader
	buf  *bytes.Buffer
	once sync.Once
}

func (r *BigQueryReader) Read() (arrow.Record, error) {
	var err error

	// Ensure the IPC reader is properly initialized with the schema
	r.once.Do(func() {
		if r.r == nil {
			r.buf.Write(r.schemaBytes)
			r.r, err = ipc.NewReader(r.buf, ipc.WithAllocator(r.mem))
			if err != nil {
				err = fmt.Errorf("failed to create initial IPC reader for schema: %w", err)
			}
		}
	})
	if err != nil {
		return nil, err
	}

	// Ensure there is at least one stream available
	if len(r.streams) == 0 {
		return nil, fmt.Errorf("no streams available")
	}

	// Get the single stream name
	streamName := r.streams[0].Name
	offset := int64(0)
	retries := 0
	retryLimit := 3

	// Create a channel to receive ReadRowsResponses
	responseChan := make(chan *storagepb.ReadRowsResponse)
	errorChan := make(chan error, 1)

	// Launch a goroutine to read from the stream
	go func() {
		defer close(responseChan)
		defer close(errorChan) // Close errorChan when done

		for retries < retryLimit {
			stream, err := r.client.ReadRows(r.ctx, &storagepb.ReadRowsRequest{
				ReadStream: streamName,
				Offset:     offset,
			}, r.callOptions.ReadRows...)

			if err != nil {
				errorChan <- fmt.Errorf("failed to stream rows: %w", err)
				return
			}
			defer stream.CloseSend()

			for {
				response, err := stream.Recv()
				if err != nil {
					if err == io.EOF || status.Code(err) == codes.Canceled || r.ctx.Err() != nil {
						errorChan <- io.EOF
						return
					}

					if status.Code(err) == codes.ResourceExhausted {
						retryDelay := time.Duration(2<<retries) * time.Second
						time.Sleep(retryDelay)
						retries++
						break // Retry the entire loop
					}
					errorChan <- fmt.Errorf("error receiving stream response: %w", err)
					return
				}
				responseChan <- response
			}
		}
		errorChan <- fmt.Errorf("retries exhausted, no more records to read")
	}()

	// Process responses from the channel
	for {
		select {
		case response, ok := <-responseChan:
			if !ok {
				return nil, io.EOF // Channel closed, end of stream
			}

			// Handle Arrow record batches in the response
			undecodedBatch := response.GetArrowRecordBatch().GetSerializedRecordBatch()
			if len(undecodedBatch) > 0 {
				record, err := r.processRecordBatch(undecodedBatch)
				if err != nil {
					return nil, err
				}
				if record != nil {
					// Update offset by adding the number of rows in the current response
					offset += response.GetRowCount()
					return record, nil
				}
			} else {
				// If no undecodedBatch, continue to next response
				offset += response.GetRowCount() // Ensure offset is updated even if no data in batch
			}
		case err := <-errorChan:
			return nil, err
		case <-r.ctx.Done():
			return nil, r.ctx.Err() // Handle context cancellation
		}
	}
}

func (r *BigQueryReader) processRecordBatch(undecodedBatch []byte) (arrow.Record, error) {
	// Reset the buffer for the new batch
	r.buf.Reset()
	r.buf.Write(r.schemaBytes)
	r.buf.Write(undecodedBatch)

	// Re-create the IPC reader with the new buffer
	var err error
	r.r, err = ipc.NewReader(r.buf, ipc.WithAllocator(r.mem), ipc.WithSchema(r.r.Schema()))
	if err != nil {
		return nil, fmt.Errorf("failed to create IPC reader for batch: %w", err)
	}

	// Read the first record in the current batch
	if r.r.Next() {
		record := r.r.Record()
		record.Retain() // Retain record to ensure it stays valid
		helper.PrintRecordBatch(record)
		return record, nil
	}

	// Check for errors during record reading
	if rErr := r.r.Err(); rErr != nil {
		return nil, fmt.Errorf("error reading records: %w", rErr)
	}

	// No records found in this batch
	return nil, nil
}

func (r *BigQueryReader) Close() error {
	defer memoryPool.PutAllocator(r.mem)
	if r.r != nil {
		r.r.Release()
	}
	return nil
}

func (r *BigQueryReader) Schema() (*arrow.Schema, error) {
	// Ensure the IPC reader is initialized and schema is available
	if r.r != nil {
		return r.r.Schema(), nil
	}
	return nil, fmt.Errorf("schema is not initialized")
}
