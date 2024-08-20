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

	bqStorage "cloud.google.com/go/bigquery/storage/apiv1"
	bqStoragepb "cloud.google.com/go/bigquery/storage/apiv1/storagepb"
	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/ipc"
	"github.com/apache/arrow/go/v17/arrow/memory"
)

const (
	ARROW_FORMAT = "arrow"
	AVRO_FORMAT  = "avro"
)

type BigQueryConnector struct {
	Client *bqStorage.BigQueryReadClient
}

func NewBigQueryConnector(ctx context.Context) (*BigQueryConnector, error) {
	client, err := bqStorage.NewBigQueryReadClient(ctx)
	if err != nil {
		return nil, err
	}
	return &BigQueryConnector{Client: client}, nil
}

func (bq *BigQueryConnector) CreateReadSession(ctx context.Context, projectID, datasetID, tableID string, format bqStoragepb.DataFormat) (*bqStoragepb.ReadSession, error) {
	tableReference := fmt.Sprintf("projects/%s/datasets/%s/tables/%s", projectID, datasetID, tableID)

	createReadSessionRequest := &bqStoragepb.CreateReadSessionRequest{
		Parent: fmt.Sprintf("projects/%s", projectID),
		ReadSession: &bqStoragepb.ReadSession{
			Table:      tableReference,
			DataFormat: format,
		},
		MaxStreamCount: 1,
	}

	session, err := bq.Client.CreateReadSession(ctx, createReadSessionRequest)
	if err != nil {
		return nil, err
	}

	return session, nil
}

func ReadBigQueryStream(ctx context.Context, projectID, datasetID, tableID string) (<-chan arrow.Record, <-chan error) {
	recordChan := make(chan arrow.Record)
	errChan := make(chan error, 1)

	go func() {
		defer close(recordChan)
		defer close(errChan)

		connector, err := NewBigQueryConnector(ctx)
		if err != nil {
			errChan <- fmt.Errorf("failed to create BigQuery connector: %w", err)
			return
		}
		defer connector.Close()

		session, err := connector.CreateReadSession(ctx, projectID, datasetID, tableID, bqStoragepb.DataFormat_ARROW)
		if err != nil {
			errChan <- fmt.Errorf("failed to create read session: %w", err)
			return
		}

		if len(session.GetStreams()) == 0 {
			errChan <- fmt.Errorf("no streams available in session")
			return
		}

		streamName := session.GetStreams()[0].Name
		stream, err := connector.Client.ReadRows(ctx, &bqStoragepb.ReadRowsRequest{ReadStream: streamName})
		if err != nil {
			errChan <- fmt.Errorf("failed to stream rows: %w", err)
			return
		}

		alloc := memory.NewGoAllocator()

		for {
			response, err := stream.Recv()
			if err != nil {
				if err == io.EOF {
					break
				}
				errChan <- fmt.Errorf("error receiving stream response: %w", err)
				return
			}

			batch := response.GetArrowRecordBatch().GetSerializedRecordBatch()
			reader, err := ipc.NewReader(bytes.NewReader(batch), ipc.WithAllocator(alloc))
			if err != nil {
				errChan <- fmt.Errorf("failed to create IPC reader: %w", err)
				return
			}

			for reader.Next() {
				record := reader.Record()
				record.Retain()
				select {
				case recordChan <- record:
				case <-ctx.Done():
					record.Release()
					errChan <- ctx.Err()
					return
				}
			}

			if err := reader.Err(); err != nil {
				errChan <- fmt.Errorf("error reading records: %w", err)
				return
			}
		}
	}()

	return recordChan, errChan
}

func (bq *BigQueryConnector) Close() error {
	return bq.Client.Close()
}
