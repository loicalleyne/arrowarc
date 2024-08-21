package main

import (
	"context"
	"log"
	"strconv"
	"strings"
	"time"

	pq "github.com/ArrowArc/ArrowArc/pkg/parquet"
	"github.com/docopt/docopt-go"
)

func main() {
	usage := `Rewrite Parquet File.

Usage:
  rewrite_parquet --input=<input_file> --output=<output_file> [--memory-map] [--chunk-size=<bytes>] [--columns=<col1,col2,...>] [--row-groups=<rg1,rg2,...>] [--parallel]
  rewrite_parquet -h | --help

Options:
  -h --help               Show this screen.
  --input=<input_file>    Path to the input Parquet file.
  --output=<output_file>  Path to the output Parquet file.
  --memory-map            Enable memory mapping for reading the input file.
  --chunk-size=<bytes>    Number of bytes to read per chunk [default: 1024].
  --columns=<col1,col2,...> List of columns to read.
  --row-groups=<rg1,rg2,...> List of row groups to read.
  --parallel              Enable parallel processing.
`

	arguments, err := docopt.ParseDoc(usage)
	if err != nil {
		log.Fatalf("Error parsing arguments: %v", err)
	}

	inputFilePath, _ := arguments.String("--input")
	outputFilePath, _ := arguments.String("--output")
	memoryMap, _ := arguments.Bool("--memory-map")
	chunkSize, _ := arguments.Int("--chunk-size")
	columns, _ := arguments.String("--columns")
	rowGroups, _ := arguments.String("--row-groups")
	parallel, _ := arguments.Bool("--parallel")

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

	err = pq.RewriteParquetFile(ctx, inputFilePath, outputFilePath, memoryMap, int64(chunkSize), columnsList, intRowGroupsList, parallel)
	if err != nil {
		log.Fatalf("Error rewriting Parquet file: %v", err)
	}
	log.Println("Parquet file rewritten successfully")
}

func parseCommaSeparatedList(input string) []string {
	if input == "" {
		return nil
	}
	return strings.Split(input, ",")
}
