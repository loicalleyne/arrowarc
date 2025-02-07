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

package test

import (
	"fmt"
	"io"
	"os"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/arrio"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/arrowarc/arrowarc/internal/arrdata"
)

type copyKind int

const (
	fileKind copyKind = iota
	streamKind
)

func (k copyKind) write(t *testing.T, f *os.File, mem memory.Allocator, schema *arrow.Schema, recs []arrow.Record) {
	t.Helper()

	switch k {
	case fileKind:
		arrdata.WriteFile(t, f, mem, schema, recs)
	case streamKind:
		arrdata.WriteStream(t, f, mem, schema, recs)
	default:
		panic("invalid copyKind")
	}
}

func (k copyKind) check(t *testing.T, f *os.File, mem memory.Allocator, schema *arrow.Schema, recs []arrow.Record) {
	t.Helper()

	switch k {
	case fileKind:
		arrdata.CheckArrowFile(t, f, mem, schema, recs)
	case streamKind:
		arrdata.CheckArrowStream(t, f, mem, schema, recs)
	default:
		panic("invalid copyKind")
	}
}

func TestCopy(t *testing.T) {
	tempDir := t.TempDir()

	for _, tc := range []struct {
		name     string
		src, dst copyKind
	}{
		{name: "file2file", src: fileKind, dst: fileKind},
		{name: "file2stream", src: fileKind, dst: streamKind},
		{name: "stream2file", src: streamKind, dst: fileKind},
		{name: "stream2stream", src: streamKind, dst: streamKind},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for name, recs := range arrdata.Records {
				t.Run(name, func(t *testing.T) {
					for _, tcopy := range []struct {
						n    int
						want int
						err  error
					}{
						{-1, len(recs), nil},
						{1, 1, nil},
						{0, 0, nil},
						{len(recs), len(recs), nil},
						{len(recs) + 1, len(recs), io.EOF},
					} {
						t.Run(fmt.Sprintf("-copy-n=%d", tcopy.n), func(t *testing.T) {
							mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
							defer mem.AssertSize(t, 0)

							f, err := os.CreateTemp(tempDir, "go-arrow-copy-")
							if err != nil {
								t.Fatal(err)
							}
							defer f.Close()

							o, err := os.CreateTemp(tempDir, "go-arrow-copy-")
							if err != nil {
								t.Fatal(err)
							}
							defer o.Close()

							tc.src.write(t, f, mem, recs[0].Schema(), recs)
							tc.src.check(t, f, mem, recs[0].Schema(), recs)

							_, err = f.Seek(0, io.SeekStart)
							if err != nil {
								t.Fatal(err)
							}

							var r arrio.Reader
							switch tc.src {
							case fileKind:
								rr, err := ipc.NewFileReader(f, ipc.WithSchema(recs[0].Schema()), ipc.WithAllocator(mem))
								if err != nil {
									t.Fatal(err)
								}
								defer rr.Close()
								r = rr
							case streamKind:
								rr, err := ipc.NewReader(f, ipc.WithSchema(recs[0].Schema()), ipc.WithAllocator(mem))
								if err != nil {
									t.Fatal(err)
								}
								defer rr.Release()
								r = rr
							default:
								t.Fatalf("invalid src type %v", tc.src)
							}

							var w interface {
								arrio.Writer
								io.Closer
							}

							switch tc.dst {
							case fileKind:
								w, err = ipc.NewFileWriter(o, ipc.WithSchema(recs[0].Schema()), ipc.WithAllocator(mem))
								if err != nil {
									t.Fatal(err)
								}
							case streamKind:
								w = ipc.NewWriter(o, ipc.WithSchema(recs[0].Schema()), ipc.WithAllocator(mem))
							default:
								t.Fatalf("invalid dst type %v", tc.dst)
							}
							defer w.Close()

							var (
								n int64
							)
							switch tcopy.n {
							case -1:
								n, err = arrio.Copy(w, r)
							case len(recs) + 1:
								n, err = arrio.CopyN(w, r, int64(tcopy.n))
							default:
								n, err = arrio.CopyN(w, r, int64(tcopy.n))
							}

							switch err {
							case nil:
								if tcopy.err != nil {
									t.Fatalf("got a nil error, want=%v", tcopy.err)
								}
							default:
								switch tcopy.err {
								case nil:
									t.Fatalf("invalid error: got=%v, want=%v", err, tcopy.err)
								default:
									if tcopy.err.Error() != err.Error() {
										t.Fatalf("invalid error: got=%v, want=%v", err, tcopy.err)
									}
								}
							}

							if got, want := n, int64(tcopy.want); got != want {
								t.Fatalf("invalid number of records copied: got=%d, want=%d", got, want)
							}

							err = w.Close()
							if err != nil {
								t.Fatal(err)
							}

							tc.dst.check(t, o, mem, recs[0].Schema(), recs[:tcopy.want])
						})
					}
				})
			}
		})
	}
}
