package arrowutils

import (
	"bytes"
	"container/heap"
	"fmt"
	"math"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/arrowarc/arrowarc/internal/arcpq/builder"
)

// SortDirection indicates the order direction.
type SortDirection int

const (
	Ascending SortDirection = iota
	Descending
)

// comparison returns 1 for ascending, -1 for descending
func (d SortDirection) comparison() int {
	if d == Ascending {
		return 1
	}
	return -1
}

// SortingColumn describes which column (by index) to sort on,
// whether nulls should come first, and the sort direction.
type SortingColumn struct {
	Index      int
	NullsFirst bool
	Direction  SortDirection
}

// cursor holds the current position for a given record.
type cursor struct {
	r      arrow.Record
	curIdx int
}

// cursorHeap implements a min-heap over record cursors according to the provided sorting columns.
type cursorHeap struct {
	cursors     []cursor
	orderByCols []SortingColumn
}

// Len implements heap.Interface.
func (h *cursorHeap) Len() int {
	return len(h.cursors)
}

// Less implements heap.Interface. It compares the cursor at index i and j.
func (h *cursorHeap) Less(i, j int) bool {
	for idx, sc := range h.orderByCols {
		c1 := h.cursors[i]
		c2 := h.cursors[j]
		col1 := c1.r.Column(sc.Index)
		col2 := c2.r.Column(sc.Index)

		// First, compare nulls.
		if cmp, ok := nullComparison(col1.IsNull(c1.curIdx), col2.IsNull(c2.curIdx)); ok {
			if sc.NullsFirst {
				return cmp < 0
			}
			return cmp > 0
		}

		// Compare non-null values.
		cmp := h.compare(idx, i, j)
		if cmp != 0 {
			// If direction is ascending, we want cmp < 0 to return true
			// If direction is descending, we want cmp > 0 to return true
			return (cmp * int(sc.Direction.comparison())) < 0
		}
	}
	return false
}

// compare compares the values at the given column (specified by orderByCols[idx]) between cursors i and j.
func (h *cursorHeap) compare(idx, i, j int) int {
	c1 := h.cursors[i]
	c2 := h.cursors[j]
	sc := h.orderByCols[idx]

	switch arr1 := c1.r.Column(sc.Index).(type) {
	case *array.Binary:
		arr2 := c2.r.Column(sc.Index).(*array.Binary)
		return bytes.Compare(arr1.Value(c1.curIdx), arr2.Value(c2.curIdx))
	case *array.String:
		arr2 := c2.r.Column(sc.Index).(*array.String)
		return strings.Compare(arr1.Value(c1.curIdx), arr2.Value(c2.curIdx))
	case *array.Int64:
		arr2 := c2.r.Column(sc.Index).(*array.Int64)
		v1, v2 := arr1.Value(c1.curIdx), arr2.Value(c2.curIdx)
		switch {
		case v1 == v2:
			return 0
		case v1 < v2:
			return -1
		default:
			return 1
		}
	case *array.Int32:
		arr2 := c2.r.Column(sc.Index).(*array.Int32)
		v1, v2 := arr1.Value(c1.curIdx), arr2.Value(c2.curIdx)
		switch {
		case v1 == v2:
			return 0
		case v1 < v2:
			return -1
		default:
			return 1
		}
	case *array.Uint64:
		arr2 := c2.r.Column(sc.Index).(*array.Uint64)
		v1, v2 := arr1.Value(c1.curIdx), arr2.Value(c2.curIdx)
		switch {
		case v1 == v2:
			return 0
		case v1 < v2:
			return -1
		default:
			return 1
		}
	case *array.Dictionary:
		// Assume binary dictionary for now.
		dict1, ok1 := arr1.Dictionary().(*array.Binary)
		arr2, ok2 := c2.r.Column(sc.Index).(*array.Dictionary)
		if !ok1 || !ok2 {
			panic(fmt.Sprintf("unsupported dictionary type: %T, %T", arr1.Dictionary(), c2.r.Column(sc.Index)))
		}
		dict2 := arr2.Dictionary().(*array.Binary)
		idx1 := arr1.GetValueIndex(c1.curIdx)
		idx2 := arr2.GetValueIndex(c2.curIdx)
		return bytes.Compare(dict1.Value(idx1), dict2.Value(idx2))
	default:
		panic(fmt.Sprintf("unsupported type for record merging: %T", arr1))
	}
}

// Swap implements heap.Interface.
func (h *cursorHeap) Swap(i, j int) {
	h.cursors[i], h.cursors[j] = h.cursors[j], h.cursors[i]
}

// Push implements heap.Interface. It panics since the number of cursors is fixed.
func (h *cursorHeap) Push(x interface{}) {
	panic("Push is not supported: fixed number of cursors")
}

// Pop implements heap.Interface.
func (h *cursorHeap) Pop() interface{} {
	n := len(h.cursors)
	if n == 0 {
		return nil
	}
	x := h.cursors[n-1]
	h.cursors = h.cursors[:n-1]
	return x
}

// MergeRecords merges the given Arrow records into a single record.
// All records must share the same schema. The records are assumed to be sorted
// (ascending order) on the columns specified in orderByCols. If limit is zero,
// all rows are merged.
func MergeRecords(
	mem memory.Allocator,
	records []arrow.Record,
	orderByCols []SortingColumn,
	limit uint64,
) (arrow.Record, error) {
	if len(records) == 0 {
		return nil, fmt.Errorf("no records provided")
	}

	// Initialize cursors—one per record.
	cursors := make([]cursor, len(records))
	for i, rec := range records {
		cursors[i] = cursor{r: rec, curIdx: 0}
	}
	h := &cursorHeap{
		cursors:     cursors,
		orderByCols: orderByCols,
	}

	schema := records[0].Schema()
	recordBuilder := builder.NewRecordBuilder(mem, schema)
	defer recordBuilder.Release()

	// If no limit is specified, use a very high limit.
	if limit == 0 {
		limit = math.MaxUint64
	}
	var count uint64

	heap.Init(h)
	for h.Len() > 0 && count < limit {
		// The smallest cursor is always at index 0.
		current := &h.cursors[0]
		r := current.r
		idx := current.curIdx

		// Append each column value to the record builder.
		for colIdx, fieldBuilder := range recordBuilder.Fields() {
			if err := builder.AppendValue(fieldBuilder, r.Column(colIdx), idx); err != nil {
				return nil, err
			}
		}

		// If this cursor is exhausted, remove it; otherwise, advance and fix the heap.
		if idx+1 >= int(r.NumRows()) {
			heap.Pop(h)
		} else {
			current.curIdx++
			heap.Fix(h, 0)
		}
		count++
	}

	return recordBuilder.NewRecord(), nil
}
