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
	"strconv"
	"strings"
	"time"

	converter "github.com/arrowarc/arrowarc/convert"
	"github.com/docopt/docopt-go"
)

func main() {
	usage := `Parquet to JSON Converter.

Usage:
  parquet_to_json --parquet=<parquet_file> --json=<json_file> [--memory-map] [--chunk-size=<bytes>] [--columns=<col1,col2,...>] [--row-groups=<rg1,rg2,...>] [--parallel] [--include-structs]
  parquet_to_json -h | --help

Options:
  -h --help                               Show this screen.
  --parquet=<parquet_file>                Path to the input Parquet file.
  --json=<json_file>                      Path to the output JSON file.
  --memory-map                            Enable memory mapping for reading the input file.
  --chunk-size=<bytes>                    Number of bytes to read per chunk [default: 1024].
  --columns=<col1,col2,...>               List of columns to read.
  --row-groups=<rg1,rg2,...>              List of row groups to read.
  --parallel                              Enable parallel processing.
  --include-structs                       Include nested structures in the JSON output.
`

	arguments, err := docopt.ParseDoc(usage)
	if err != nil {
		log.Fatalf("Error parsing arguments: %v", err)
	}

	parquetPath, _ := arguments.String("--parquet")
	jsonPath, _ := arguments.String("--json")
	memoryMap, _ := arguments.Bool("--memory-map")
	chunkSize, _ := arguments.Int("--chunk-size")
	columns, _ := arguments.String("--columns")
	rowGroups, _ := arguments.String("--row-groups")
	parallel, _ := arguments.Bool("--parallel")
	includeStructs, _ := arguments.Bool("--include-structs")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	columnsList := parseCommaSeparatedList(columns)
	rowGroupsList := parseCommaSeparatedList(rowGroups)
	intRowGroupsList := make([]int, len(rowGroupsList))
	for i, rg := range rowGroupsList {
		intRowGroup, err := strconv.Atoi(rg)
		if err != nil {
			log.Fatalf("Error converting row group to integer: %v", err)
		}
		intRowGroupsList[i] = intRowGroup
	}

	err = converter.ConvertParquetToJSON(ctx, parquetPath, jsonPath, memoryMap, int64(chunkSize), columnsList, intRowGroupsList, parallel, includeStructs)
	if err != nil {
		log.Fatalf("Error converting Parquet to JSON: %v", err)
	}

	log.Println("Parquet to JSON conversion completed successfully")
}

func parseCommaSeparatedList(input string) []string {
	if input == "" {
		return nil
	}
	return strings.Split(input, ",")
}
