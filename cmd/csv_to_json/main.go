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
	"log"
	"strings"
	"time"

	converter "github.com/arrowarc/arrowarc/convert"
	"github.com/docopt/docopt-go"
)

func main() {
	usage := `CSV to JSON Converter.

Usage:
  csv_to_json --csv=<csv_file> --json=<json_file> [--header=<true|false>] [--chunk-size=<bytes>] [--delimiter=<char>] [--null=<value>] [--strings-can-be-null=<true|false>]
  csv_to_json -h | --help

Options:
  -h --help                             Show this screen.
  --csv=<csv_file>                      Path to the input CSV file.
  --json=<json_file>                    Path to the output JSON file.
  --header=<true|false>                 Indicates if the CSV file has a header [default: true].
  --chunk-size=<bytes>                  Number of bytes to read per chunk [default: 1024].
  --delimiter=<char>                    Delimiter used in the CSV file [default: ,].
  --null=<value>                        Value to be considered as null [default: null].
  --strings-can-be-null=<true|false>   Indicates if strings can be considered as null [default: false].
`

	arguments, err := docopt.ParseDoc(usage)
	if err != nil {
		log.Fatalf("Error parsing arguments: %v", err)
	}

	csvPath, _ := arguments.String("--csv")
	jsonPath, _ := arguments.String("--json")
	hasHeader, _ := arguments.Bool("--header")
	chunkSize, _ := arguments.Int("--chunk-size")
	delimiter, _ := arguments.String("--delimiter")
	nullValues, _ := arguments.String("--null")
	stringsCanBeNull, _ := arguments.Bool("--strings-can-be-null")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	err = converter.ConvertCSVToJSON(ctx, csvPath, jsonPath, hasHeader, int64(chunkSize), rune(delimiter[0]), strings.Split(nullValues, ","), stringsCanBeNull)
	if err != nil {
		log.Fatalf("Error converting CSV to JSON: %v", err)
	}
	log.Println("CSV to JSON conversion completed successfully")
}
