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
	"time"

	pq "github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	parquet "github.com/arrowarc/arrowarc/pkg/parquet"
	"github.com/docopt/docopt-go"
)

func main() {
	usage := `Rewrite Parquet File.

Usage:
  rewrite_parquet --input=<input_file> --output=<output_file> [--memory-map] [--chunk-size=<bytes>] [--columns=<col1,col2,...>] [--row-groups=<rg1,rg2,...>] [--parallel] [--compression=<codec>] [--batch-size=<rows>]
  rewrite_parquet -h | --help

Options:
  -h --help                 Show this screen.
  --input=<input_file>      Path to the input Parquet file.
  --output=<output_file>    Path to the output Parquet file.
  --memory-map              Enable memory mapping for reading the input file.
  --chunk-size=<bytes>      Number of bytes to read per chunk [default: 1024].
  --parallel                Enable parallel processing.
`

	arguments, err := docopt.ParseDoc(usage)
	if err != nil {
		log.Fatalf("Error parsing arguments: %v", err)
	}

	// Parse input arguments
	inputFilePath, _ := arguments.String("--input")
	outputFilePath, _ := arguments.String("--output")
	memoryMap, _ := arguments.Bool("--memory-map")
	chunkSize, _ := arguments.Int("--chunk-size")
	parallel, _ := arguments.Bool("--parallel")

	// Set up context with a timeout
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	if err != nil {
		log.Fatalf("Error converting row group to integer: %v", err)
	}

	writerProps := pq.NewWriterProperties(pq.WithCompression(compress.Codecs.Snappy), pq.WithBatchSize(int64(chunkSize)))

	// Call the rewritten Parquet file function
	err = parquet.RewriteParquetFile(ctx, inputFilePath, outputFilePath, memoryMap, int64(chunkSize), nil, nil, parallel, writerProps)
	if err != nil {
		log.Fatalf("Error rewriting Parquet file: %v", err)
	}

	log.Println("Parquet file rewritten successfully")
}
