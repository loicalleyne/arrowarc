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

	"github.com/apache/arrow-go/v18/parquet/compress"
	converter "github.com/arrowarc/arrowarc/converter"
	"github.com/docopt/docopt-go"
)

func main() {
	usage := `Avro to Parquet Converter.

Usage:
  avro_to_parquet --avro=<avro_file> --parquet=<parquet_file> [--chunk-size=<bytes>] [--compression=<type>]
  avro_to_parquet -h | --help

Options:
  -h --help                                 Show this screen.
  --avro=<avro_file>                        Path to the input Avro file.
  --parquet=<parquet_file>                  Path to the output Parquet file.
  --chunk-size=<bytes>                      Number of bytes to read per chunk [default: 8192].
  --compression=<type>                      Compression type to use (e.g., none, snappy, gzip) [default: snappy].
`

	arguments, err := docopt.ParseDoc(usage)
	if err != nil {
		log.Fatalf("Error parsing arguments: %v", err)
	}

	avroFilePath, _ := arguments.String("--avro")
	parquetFilePath, _ := arguments.String("--parquet")
	chunkSize, _ := arguments.Int("--chunk-size")
	compressionTypeStr, _ := arguments.String("--compression")

	// Map compression type to the appropriate constant
	var compressionType compress.Compression
	switch compressionTypeStr {
	case "snappy":
		compressionType = compress.Codecs.Snappy
	case "gzip":
		compressionType = compress.Codecs.Gzip
	case "brotli":
		compressionType = compress.Codecs.Brotli
	case "zstd":
		compressionType = compress.Codecs.Zstd
	case "none":
		compressionType = compress.Codecs.Uncompressed
	default:
		log.Fatalf("Invalid compression type: %s", compressionTypeStr)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()

	metrics, err := converter.ConvertAvroToParquet(
		ctx,
		avroFilePath,
		parquetFilePath,
		int64(chunkSize),
		compressionType,
	)
	if err != nil {
		log.Fatalf("Failed to convert Avro to Parquet: %v", err)
	}

	fmt.Printf("Conversion completed. Summary: %s\n", metrics)
}
