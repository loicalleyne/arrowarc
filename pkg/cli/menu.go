package cli

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/apache/arrow/go/v17/parquet/compress"
	converter "github.com/arrowarc/arrowarc/convert"
	generator "github.com/arrowarc/arrowarc/generator"
	flightclient "github.com/arrowarc/arrowarc/integrations/flight/sqlite"
	pq "github.com/arrowarc/arrowarc/pkg/parquet"
)

var commandFunctions = map[string]func(){
	"generate_parquet": generateParquet,
	"parquet_to_csv":   parquetToCSV,
	"csv_to_parquet":   csvToParquet,
	"parquet_to_json":  parquetToJSON,
	"rewrite_parquet":  rewriteParquet,
	"flight":           flight,
	"avro_to_parquet":  avroToParquet,
}

// DisplayMenu dynamically builds the CLI menu and handles user input
func DisplayMenu() {
	commands, err := getCommands()
	if err != nil {
		fmt.Printf("Error retrieving commands: %v\n", err)
		return
	}

	fmt.Println("Welcome to ArrowArc CLI")
	fmt.Println("========================")
	fmt.Println("Select an option:")
	for i, cmd := range commands {
		fmt.Printf("%d. %s\n", i+1, cmd)
	}
	fmt.Println("X. Exit")

	reader := bufio.NewReader(os.Stdin)

	for {
		fmt.Print("\nEnter your choice: ")
		input, _ := reader.ReadString('\n')
		choice := strings.TrimSpace(input)

		if choice == "X" || choice == "x" {
			fmt.Println("Exiting ArrowArc CLI. Goodbye!")
			os.Exit(0)
		}

		idx := parseChoice(choice, len(commands))
		if idx == -1 {
			fmt.Println("Invalid choice. Please select a valid option.")
			continue
		}

		command := commands[idx]
		if fn, exists := commandFunctions[command]; exists {
			fn()
		} else {
			fmt.Printf("Command %s is not implemented.\n", command)
		}
	}
}

// getCommands retrieves available commands from the cmd directory
func getCommands() ([]string, error) {
	var commands []string
	entries, err := os.ReadDir("../../cmd")
	if err != nil {
		return nil, err
	}

	for _, entry := range entries {
		if entry.IsDir() {
			commands = append(commands, entry.Name())
		}
	}
	return commands, nil
}

// parseChoice converts the user input into an integer index
func parseChoice(input string, numCommands int) int {
	choice, err := strconv.Atoi(input)
	if err != nil || choice < 1 || choice > numCommands {
		return -1
	}
	return choice - 1
}

// generateParquet generates a Parquet file
func generateParquet() {
	filePath := getUserInput("Enter the output file path: ")
	complex := getUserInput("Generate complex data? (y/n): ") == "y"
	targetSize := getInt64Input("Enter target file size in bytes: ")

	fmt.Println("Generating Parquet file...")
	err := generator.GenerateParquetFile(filePath, targetSize, complex)
	if err != nil {
		fmt.Println("Error generating Parquet file:", err)
	} else {
		fmt.Println("Parquet file generated successfully")
	}
}

// parquetToCSV converts a Parquet file to CSV
func parquetToCSV() {
	inputFile := getUserInput("Enter the input Parquet file path: ")
	outputFile := getUserInput("Enter the output CSV file path: ")

	fmt.Println("Converting Parquet to CSV...")
	err := converter.ConvertParquetToCSV(context.Background(), inputFile, outputFile, false, 0, []string{}, []int{}, false, ',', false, "", nil, nil)
	if err != nil {
		fmt.Println("Error converting Parquet to CSV:", err)
	} else {
		fmt.Println("Conversion completed successfully")
	}
}

// csvToParquet converts a CSV file to Parquet
func csvToParquet() {
	inputFile := getUserInput("Enter the input CSV file path: ")
	outputFile := getUserInput("Enter the output Parquet file path: ")

	fmt.Println("Converting CSV to Parquet...")
	err := converter.ConvertCSVToParquet(context.Background(), inputFile, outputFile, nil, true, 0, ',', []string{}, false)
	if err != nil {
		fmt.Println("Error converting CSV to Parquet:", err)
	} else {
		fmt.Println("Conversion completed successfully")
	}
}

// parquetToJSON converts a Parquet file to JSON
func parquetToJSON() {
	inputFile := getUserInput("Enter the input Parquet file path: ")
	outputFile := getUserInput("Enter the output JSON file path: ")

	fmt.Println("Converting Parquet to JSON...")
	err := converter.ConvertParquetToJSON(context.Background(), inputFile, outputFile, false, 0, []string{}, []int{}, false, false)
	if err != nil {
		fmt.Println("Error converting Parquet to JSON:", err)
	} else {
		fmt.Println("Conversion completed successfully")
	}
}

// rewriteParquet rewrites a Parquet file
func rewriteParquet() {
	inputFile := getUserInput("Enter the input Parquet file path: ")
	outputFile := getUserInput("Enter the output Parquet file path: ")

	fmt.Println("Rewriting Parquet file...")
	err := pq.RewriteParquetFile(context.Background(), inputFile, outputFile, false, 0, []string{}, []int{}, false, nil)
	if err != nil {
		fmt.Println("Error rewriting Parquet file:", err)
	} else {
		fmt.Println("Parquet file rewritten successfully")
	}
}

// flight performs Arrow Flight operations
func flight() {
	serverAddress := getUserInput("Enter the Arrow Flight server address: ")

	flightclient.StartFlightClient(serverAddress)

	fmt.Println("Arrow Flight client started successfully")
}

// avroToParquet converts an Avro file to Parquet
func avroToParquet() {
	inputFile := getUserInput("Enter the input Avro file path: ")
	outputFile := getUserInput("Enter the output Parquet file path: ")

	fmt.Println("Converting Avro to Parquet...")
	err := converter.ConvertAvroToParquet(context.Background(), inputFile, outputFile, 100000, compress.Codecs.Snappy)
	if err != nil {
		fmt.Println("Error converting Avro to Parquet:", err)
	} else {
		fmt.Println("Conversion completed successfully")
	}
}

// Helper function to get int64 input
func getInt64Input(prompt string) int64 {
	for {
		input := getUserInput(prompt)
		value, err := strconv.ParseInt(input, 10, 64)
		if err == nil {
			return value
		}
		fmt.Println("Invalid input. Please enter a valid integer.")
	}
}

// getUserInput gets user input from the console
func getUserInput(prompt string) string {
	fmt.Print(prompt)
	reader := bufio.NewReader(os.Stdin)
	input, _ := reader.ReadString('\n')
	return strings.TrimSpace(input)
}
