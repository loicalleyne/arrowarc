package interfaces

import (
	"context"

	"github.com/apache/arrow/go/v17/arrow"
)

type Source interface {
	Init(ctx context.Context) error
	Read() (DataBatch, error)
	Close() error
}

type Sink interface {
	Init(ctx context.Context) error
	Write(batch DataBatch) error
	Close() error
}

type DataBatch struct {
	Data     interface{}       // The actual data, e.g., Arrow Record, byte slice, etc.
	Metadata map[string]string // Metadata associated with the data, key-value pairs
	Error    error             // Error associated with the batch
	Schema   *arrow.Schema     // Schema associated with the data
}

type Reader interface {
	Read() (arrow.Record, error)
	Close() error
}

type Writer interface {
	Write(arrow.Record) error
	Close() error
}

type SourceSink interface {
	Source
	Sink
}
