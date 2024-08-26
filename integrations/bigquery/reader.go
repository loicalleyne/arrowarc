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
	"github.com/apache/arrow/go/v17/arrow/arrio"
	"github.com/apache/arrow/go/v17/arrow/ipc"
	"github.com/apache/arrow/go/v17/arrow/memory"
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

func (bq *BigQueryReadClient) NewBigQueryArrowReader(ctx context.Context, projectID, datasetID, tableID string) (arrio.Reader, error) {
	req := &storagepb.CreateReadSessionRequest{
		Parent: fmt.Sprintf("projects/%s", projectID),
		ReadSession: &storagepb.ReadSession{
			Table:      fmt.Sprintf("projects/%s/datasets/%s/tables/%s", projectID, datasetID, tableID),
			DataFormat: storagepb.DataFormat_ARROW,
		},
		MaxStreamCount: 5,
	}

	session, err := bq.client.CreateReadSession(ctx, req, bq.callOptions.CreateReadSession...)
	if err != nil {
		return nil, fmt.Errorf("failed to create read session: %w", err)
	}

	if len(session.GetStreams()) == 0 {
		return nil, fmt.Errorf("no streams available in session")
	}

	return &bigQueryArrowReader{
		ctx:         ctx,
		client:      bq.client,
		callOptions: bq.callOptions,
		schemaBytes: session.GetArrowSchema().GetSerializedSchema(),
		streams:     session.GetStreams(),
		mem:         memory.NewGoAllocator(),
	}, nil
}

type bigQueryArrowReader struct {
	ctx         context.Context
	client      *bqStorage.BigQueryReadClient
	callOptions *BigQueryReadCallOptions
	schemaBytes []byte
	streams     []*storagepb.ReadStream
	mem         *memory.GoAllocator

	streamIdx int
	once      sync.Once
	buf       *bytes.Buffer
	r         *ipc.Reader
}

func (r *bigQueryArrowReader) Read() (arrow.Record, error) {
	var err error
	r.once.Do(func() {
		r.buf = bytes.NewBuffer(r.schemaBytes)
		r.r, err = ipc.NewReader(r.buf, ipc.WithAllocator(r.mem))
	})
	if err != nil {
		return nil, fmt.Errorf("failed to initialize IPC reader: %w", err)
	}

	for r.streamIdx < len(r.streams) {
		if r.r.Next() {
			return r.r.Record(), nil
		}

		if err := r.r.Err(); err != nil {
			if status.Code(err) == codes.Canceled || err == io.EOF || r.ctx.Err() != nil {
				return nil, io.EOF
			}
			return nil, fmt.Errorf("error reading records: %w", err)
		}

		streamName := r.streams[r.streamIdx].Name
		r.streamIdx++

		stream, err := r.client.ReadRows(r.ctx, &storagepb.ReadRowsRequest{ReadStream: streamName}, r.callOptions.ReadRows...)
		if err != nil {
			return nil, fmt.Errorf("failed to stream rows: %w", err)
		}

		response, err := stream.Recv()
		if err != nil {
			if status.Code(err) == codes.Canceled || err == io.EOF || r.ctx.Err() != nil {
				continue // Move to the next stream
			}
			return nil, fmt.Errorf("error receiving stream response: %w", err)
		}

		undecodedBatch := response.GetArrowRecordBatch().GetSerializedRecordBatch()
		if len(undecodedBatch) > 0 {
			r.buf.Reset()
			r.buf.Write(undecodedBatch)

			r.r, err = ipc.NewReader(r.buf, ipc.WithAllocator(r.mem), ipc.WithSchema(r.r.Schema()))
			if err != nil {
				return nil, fmt.Errorf("failed to create IPC reader for batch: %w", err)
			}
		}
	}

	return nil, io.EOF
}

func (r *bigQueryArrowReader) Close() error {
	if r.r != nil {
		r.r.Release()
	}
	return nil
}

func (r *bigQueryArrowReader) Schema() *arrow.Schema {
	return r.r.Schema()
}
