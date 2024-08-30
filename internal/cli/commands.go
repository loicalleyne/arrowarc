package cli

import (
	"context"
	"fmt"

	"github.com/apache/arrow/go/v17/parquet/compress"
	converter "github.com/arrowarc/arrowarc/convert"
	generator "github.com/arrowarc/arrowarc/generator"
	pq "github.com/arrowarc/arrowarc/pkg/parquet"
)

func ExecuteCommand(choice string) error {
	switch choice {
	case "Generate Parquet":
		return generateParquet()
	case "Parquet to CSV":
		return parquetToCSV()
	case "CSV to Parquet":
		return csvToParquet()
	case "Parquet to JSON":
		return parquetToJSON()
	case "Rewrite Parquet":
		return rewriteParquet()
	case "Run Flight Tests":
		return runFlightTests()
	case "Avro to Parquet":
		return avroToParquet()
	default:
		return fmt.Errorf("unknown command: %s", choice)
	}
}

func generateParquet() error {
	fmt.Print("Enter the path for the new Parquet file: ")
	var path string
	fmt.Scanln(&path)
	return generator.GenerateParquetFile(path, 1000, true)
}

func parquetToCSV() error {
	fmt.Print("Enter the path of the Parquet file: ")
	var parquetPath string
	fmt.Scanln(&parquetPath)
	fmt.Print("Enter the path for the output CSV file: ")
	var csvPath string
	fmt.Scanln(&csvPath)
	return converter.ConvertParquetToCSV(context.Background(), parquetPath, csvPath, true, 100000, []string{}, []int{}, false, ',', false, "", nil, nil)
}

func csvToParquet() error {
	fmt.Print("Enter the path of the CSV file: ")
	var csvPath string
	fmt.Scanln(&csvPath)
	fmt.Print("Enter the path for the output Parquet file: ")
	var parquetPath string
	fmt.Scanln(&parquetPath)
	return converter.ConvertCSVToParquet(context.Background(), csvPath, parquetPath, nil, true, 100000, ',', []string{}, true)
}

func parquetToJSON() error {
	fmt.Print("Enter the path of the Parquet file: ")
	var parquetPath string
	fmt.Scanln(&parquetPath)
	fmt.Print("Enter the path for the output JSON file: ")
	var jsonPath string
	fmt.Scanln(&jsonPath)
	return converter.ConvertParquetToJSON(context.Background(), parquetPath, jsonPath, true, 100000, []string{}, []int{}, true, true)
}

func rewriteParquet() error {
	fmt.Print("Enter the path of the Parquet file to rewrite: ")
	var inputPath string
	fmt.Scanln(&inputPath)
	fmt.Print("Enter the path for the rewritten Parquet file: ")
	var outputPath string
	fmt.Scanln(&outputPath)
	return pq.RewriteParquetFile(context.Background(), inputPath, outputPath, true, 100000, []string{}, []int{}, true, nil)
}

func runFlightTests() error {
	fmt.Println("Running Arrow Flight tests...")
	// Implement Arrow Flight tests here
	return fmt.Errorf("arrow Flight tests not implemented yet")
}

func avroToParquet() error {
	fmt.Print("Enter the path of the Avro file: ")
	var avroPath string
	fmt.Scanln(&avroPath)
	fmt.Print("Enter the path for the output Parquet file: ")
	var parquetPath string
	fmt.Scanln(&parquetPath)
	return converter.ConvertAvroToParquet(context.Background(), avroPath, parquetPath, 100000, compress.Codecs.Snappy)
}
