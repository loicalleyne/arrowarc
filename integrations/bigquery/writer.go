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
	"os"
	"sync"

	storage "cloud.google.com/go/bigquery/storage/apiv1"
	storagepb "cloud.google.com/go/bigquery/storage/apiv1/storagepb"
	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/ipc"
	"github.com/apache/arrow/go/v17/arrow/memory"
	memoryPool "github.com/arrowarc/arrowarc/internal/memory"
	helper "github.com/arrowarc/arrowarc/pkg/common/utils"
	"google.golang.org/api/option"
)

type BigQueryWriteClient struct {
	client *storage.BigQueryWriteClient
	schema *arrow.Schema
}

type BigQueryWriteOptions struct {
	WriteStreamType storagepb.WriteStream_Type
	Allocator       memory.Allocator
}

func NewDefaultBigQueryWriteOptions() *BigQueryWriteOptions {
	return &BigQueryWriteOptions{
		WriteStreamType: storagepb.WriteStream_COMMITTED,
		Allocator:       memoryPool.GetAllocator(),
	}
}

func NewBigQueryWriteClient(ctx context.Context, serviceAccountJSON string, schema *arrow.Schema) (*BigQueryWriteClient, error) {
	// Check if the provided string is a file path
	if _, err := os.Stat(serviceAccountJSON); err == nil {
		content, err := os.ReadFile(serviceAccountJSON)
		if err != nil {
			return nil, fmt.Errorf("failed to read service account JSON file: %w", err)
		}
		serviceAccountJSON = string(content)
	}

	client, err := storage.NewBigQueryWriteClient(ctx, option.WithCredentialsJSON([]byte(serviceAccountJSON)))
	if err != nil {
		return nil, fmt.Errorf("failed to create BigQuery Storage API client: %w", err)
	}

	return &BigQueryWriteClient{
		client: client,
		schema: schema,
	}, nil
}

type BigQueryRecordWriter struct {
	client        *BigQueryWriteClient
	appendClient  storagepb.BigQueryWrite_AppendRowsClient
	writeStream   *storagepb.WriteStream
	protoSchema   *storagepb.ProtoSchema
	buffer        *bytes.Buffer
	ipcWriter     *ipc.Writer
	writeDone     sync.WaitGroup
	writerOptions *BigQueryWriteOptions
}

func NewBigQueryRecordWriter(ctx context.Context, client *BigQueryWriteClient, projectID, datasetID, tableID string, opts *BigQueryWriteOptions) (*BigQueryRecordWriter, error) {
	if opts == nil {
		opts = NewDefaultBigQueryWriteOptions()
	}

	tableName := fmt.Sprintf("projects/%s/datasets/%s/tables/%s", projectID, datasetID, tableID)

	writeStream, err := client.client.CreateWriteStream(ctx, &storagepb.CreateWriteStreamRequest{
		Parent: tableName,
		WriteStream: &storagepb.WriteStream{
			Type: opts.WriteStreamType,
		},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create write stream: %w", err)
	}

	appendClient, err := client.client.AppendRows(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to open AppendRows client: %w", err)
	}

	buffer := &bytes.Buffer{}
	ipcWriter := ipc.NewWriter(buffer, ipc.WithSchema(client.schema), ipc.WithAllocator(opts.Allocator))

	return &BigQueryRecordWriter{
		client:        client,
		appendClient:  appendClient,
		writeStream:   writeStream,
		protoSchema:   helper.ConvertSchemaSPB(client.schema),
		buffer:        buffer,
		ipcWriter:     ipcWriter,
		writerOptions: opts,
	}, nil
}

func (w *BigQueryRecordWriter) Write(record arrow.Record) error {
	if !w.client.schema.Equal(record.Schema()) {
		return fmt.Errorf("schema mismatch: expected %v but got %v", w.client.schema, record.Schema())
	}

	w.writeDone.Add(1)
	go func(record arrow.Record) {
		defer w.writeDone.Done()

		w.buffer.Reset()

		// Write the record to the IPC writer
		if err := w.ipcWriter.Write(record); err != nil {
			fmt.Printf("error writing record to buffer: %v\n", err)
			return
		}

		serializedData := w.buffer.Bytes()

		protoData := &storagepb.AppendRowsRequest_ProtoData{
			Rows: &storagepb.ProtoRows{
				SerializedRows: [][]byte{serializedData},
			},
			WriterSchema: w.protoSchema,
		}

		appendReq := &storagepb.AppendRowsRequest{
			WriteStream: w.writeStream.GetName(),
			Rows:        &storagepb.AppendRowsRequest_ProtoRows{ProtoRows: protoData},
		}

		if err := w.appendClient.Send(appendReq); err != nil {
			fmt.Printf("error sending AppendRowsRequest: %v\n", err)
		}
	}(record)

	return nil
}

func (w *BigQueryRecordWriter) Close() error {
	w.writeDone.Wait()

	if err := w.ipcWriter.Close(); err != nil {
		return fmt.Errorf("failed to close IPC writer: %w", err)
	}

	_, err := w.client.client.FinalizeWriteStream(context.TODO(), &storagepb.FinalizeWriteStreamRequest{
		Name: w.writeStream.GetName(),
	})

	defer memoryPool.PutAllocator(w.writerOptions.Allocator)

	return err
}
