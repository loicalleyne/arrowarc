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

package main

import (
	"context"
	"fmt"
	"log"
	"time"

	converter "github.com/arrowarc/arrowarc/converter"
	"github.com/docopt/docopt-go"
)

func main() {
	usage := `CSV to Parquet Converter.

Usage:
  csv_to_parquet --csv=<csv_file> --parquet=<parquet_file> [--header=<true|false>] [--chunk-size=<bytes>] [--delimiter=<char>] [--null=<value>] [--strings-can-be-null=<true|false>]
  csv_to_parquet -h | --help

Options:
  -h --help                             Show this screen.
  --csv=<csv_file>                      Path to the input CSV file.
  --parquet=<parquet_file>              Path to the output Parquet file.
  --header=<true|false>                 Indicates if the CSV file has a header [default: true].
  --chunk-size=<bytes>                  Number of bytes to read per chunk [default: 1024].
  --delimiter=<char>                    Delimiter used in the CSV file [default: ,].
  --null=<value>                        Value representing null in the CSV file [default: NULL].
  --strings-can-be-null=<true|false>    Indicates if strings can be null [default: true].
`

	arguments, err := docopt.ParseDoc(usage)
	if err != nil {
		log.Fatalf("Error parsing arguments: %v", err)
	}

	csvPath, _ := arguments.String("--csv")
	parquetPath, _ := arguments.String("--parquet")
	hasHeader, _ := arguments.Bool("--header")
	chunkSize, _ := arguments.Int("--chunk-size")
	delimiter, _ := arguments.String("--delimiter")
	stringsCanBeNull, _ := arguments.Bool("--strings-can-be-null")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	metrics, err := converter.ConvertCSVToParquet(ctx, csvPath, parquetPath, hasHeader, int64(chunkSize), rune(delimiter[0]), []string{}, stringsCanBeNull)
	if err != nil {
		log.Fatalf("Error converting CSV to Parquet: %v", err)
	}
	fmt.Printf("Conversion completed. Summary: %s\n", metrics)
}
