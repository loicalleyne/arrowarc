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
