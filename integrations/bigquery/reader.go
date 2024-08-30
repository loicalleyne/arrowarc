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
)

type BigQueryReadClient struct {
	client      *bqStorage.BigQueryReadClient
	callOptions *BigQueryReadCallOptions
}

type BigQueryReadCallOptions struct {
	CreateReadSession []gax.CallOption
	ReadRows          []gax.CallOption
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

// NewBigQueryReader creates a new BigQueryReader for the specified table
func (bq *BigQueryReadClient) NewBigQueryReader(ctx context.Context, projectID, datasetID, tableID string) (*BigQueryReader, error) {
	// Define the ArrowSerializationOptions with compression
	arrowSerializationOptions := &storagepb.ArrowSerializationOptions{
		BufferCompression: storagepb.ArrowSerializationOptions_LZ4_FRAME,
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
	stream      storagepb.BigQueryRead_ReadRowsClient
	offset      int64
	r           *ipc.Reader
	buf         *bytes.Buffer
}

// Read reads the next record from the BigQuery stream
func (r *BigQueryReader) Read() (arrow.Record, error) {
	for {
		if r.r != nil && r.r.Next() {
			record := r.r.Record()
			record.Retain()
			return record, nil
		}

		response, err := r.readNextResponse()
		if err != nil {
			return nil, err
		}

		undecodedBatch := response.GetArrowRecordBatch().GetSerializedRecordBatch()
		if len(undecodedBatch) > 0 {
			if record, err := r.processRecordBatch(undecodedBatch); err != nil {
				return nil, err
			} else if record != nil {
				return record, nil
			}
		}
	}
}

// readNextResponse reads the next response from the BigQuery stream
func (r *BigQueryReader) readNextResponse() (*storagepb.ReadRowsResponse, error) {
	if r.stream == nil {
		if len(r.streams) == 0 {
			return nil, io.EOF
		}
		var err error
		r.stream, err = r.client.ReadRows(r.ctx, &storagepb.ReadRowsRequest{
			ReadStream: r.streams[0].Name,
			Offset:     r.offset,
		}, r.callOptions.ReadRows...)
		if err != nil {
			return nil, fmt.Errorf("failed to create ReadRows stream: %w", err)
		}
	}

	response, err := r.stream.Recv()
	if err == io.EOF {
		r.stream = nil
		return nil, err
	}
	if err != nil {
		return nil, fmt.Errorf("error receiving stream response: %w", err)
	}

	r.offset += response.GetRowCount()
	return response, nil
}

// Process a batch of records and create an Arrow record
func (r *BigQueryReader) processRecordBatch(undecodedBatch []byte) (arrow.Record, error) {
	// Reset the buffer for the new batch
	r.buf.Reset()
	r.buf.Write(r.schemaBytes)
	r.buf.Write(undecodedBatch)

	// Create a new IPC reader with the updated buffer
	var err error
	r.r, err = ipc.NewReader(r.buf, ipc.WithAllocator(r.mem), ipc.WithSchema(r.r.Schema()))
	if err != nil {
		return nil, fmt.Errorf("failed to create IPC reader for batch: %w", err)
	}

	// Read the first record in the current batch
	if r.r.Next() {
		record := r.r.Record()
		record.Retain() // Retain the record to ensure it stays valid
		helper.PrintRecordBatch(record)
		return record, nil
	}

	// Check for errors during record reading
	if rErr := r.r.Err(); rErr != nil && rErr != io.EOF {
		return nil, fmt.Errorf("error reading record batch: %w", rErr)
	}

	// No records found in this batch
	return nil, nil
}

// Close closes the BigQueryReader and releases resources
func (r *BigQueryReader) Close() error {
	defer memoryPool.PutAllocator(r.mem)
	if r.r != nil {
		r.r.Release()
	}
	return nil
}

// Schema returns the schema of the BigQueryReader
func (r *BigQueryReader) Schema() (*arrow.Schema, error) {
	// Ensure the IPC reader is initialized and schema is available
	if r.r != nil {
		return r.r.Schema(), nil
	}
	return nil, fmt.Errorf("schema is not initialized")
}
