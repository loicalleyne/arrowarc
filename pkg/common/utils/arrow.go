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

package utils

import (
	"fmt"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/ipc"
)

func PrintRecordBatch(record arrow.Record) error {
	numRows := int(record.NumRows())
	numCols := int(record.NumCols())
	fmt.Printf("Record batch with %d rows and %d columns\n", numRows, numCols)

	for i := 0; i < numCols; i++ {
		fmt.Printf("Column %d: %s, Type: %s\n", i, record.Schema().Field(i).Name, record.Schema().Field(i).Type)
	}

	for rowIndex := 0; rowIndex < numRows; rowIndex++ {
		for colIndex := 0; colIndex < numCols; colIndex++ {
			column := record.Column(colIndex)
			switch col := column.(type) {
			case *array.String:
				fmt.Printf("Row %d, Column %d: %s\n", rowIndex, colIndex, col.Value(rowIndex))
			case *array.Float64:
				fmt.Printf("Row %d, Column %d: %.2f\n", rowIndex, colIndex, col.Value(rowIndex))
			default:
				fmt.Printf("Row %d, Column %d: Unknown type\n", rowIndex, colIndex)
			}
		}
	}

	return nil
}

func IpcReaderToChannel(reader *ipc.Reader) <-chan arrow.Record {
	recordChan := make(chan arrow.Record)

	go func() {
		defer close(recordChan)
		for reader.Next() {
			recordChan <- reader.Record()
		}
	}()

	return recordChan
}

func ArrowRecordToString(record arrow.Record) string {
	var result string
	for i := 0; i < int(record.NumRows()); i++ {
		for j := 0; j < int(record.NumCols()); j++ {
			column := record.Column(j)
			switch col := column.(type) {
			case *array.String:
				result += fmt.Sprintf("%s ", col.Value(i))
			case *array.Float64:
				result += fmt.Sprintf("%.2f ", col.Value(i))
			default:
				result += "Unknown type "
			}
		}
		if i < int(record.NumRows())-1 {
			result += "\n" // Add a newline after each row except the last one
		}
	}
	return result
}
