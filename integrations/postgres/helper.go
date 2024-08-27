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
	"github.com/apache/arrow/go/v17/arrow"
)

// SingleRecordReader is a custom RecordReader that wraps a single arrow.Record.
type SingleRecordReader struct {
	record arrow.Record
	done   bool
}

// NewSingleRecordReader creates a new SingleRecordReader.
func NewSingleRecordReader(record arrow.Record) *SingleRecordReader {
	return &SingleRecordReader{record: record, done: false}
}

// Schema returns the schema of the record.
func (r *SingleRecordReader) Schema() *arrow.Schema {
	return r.record.Schema()
}

// Next advances to the next record (in this case, only one record is available).
func (r *SingleRecordReader) Next() bool {
	if r.done {
		return false
	}
	r.done = true
	return true
}

// Record returns the current record.
func (r *SingleRecordReader) Record() arrow.Record {
	return r.record
}

// Err always returns nil as there is no error state in this simple reader.
func (r *SingleRecordReader) Err() error {
	return nil
}

// Release releases resources associated with the reader.
func (r *SingleRecordReader) Release() {
	r.record.Release()
}

// Retain increases the reference count of the record.
func (r *SingleRecordReader) Retain() {
	r.record.Retain()
}

// Close releases resources associated with the SingleRecordReader.
func (r *SingleRecordReader) Close() error {
	return nil
}
