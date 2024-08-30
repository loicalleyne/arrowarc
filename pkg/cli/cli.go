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

package cli

import (
	"context"
	"fmt"

	"github.com/apache/arrow/go/v17/arrow/flight"
	"github.com/apache/arrow/go/v17/arrow/flight/flightsql"
	"github.com/apache/arrow/go/v17/parquet/compress"
	converter "github.com/arrowarc/arrowarc/convert"
	sqlite "github.com/arrowarc/arrowarc/integrations/flight/sqlite"
	pq "github.com/arrowarc/arrowarc/pkg/parquet"
	"github.com/charmbracelet/lipgloss"
	"github.com/spf13/cobra"
)

var (
	subtle    = lipgloss.AdaptiveColor{Light: "#D9DCCF", Dark: "#383838"}
	highlight = lipgloss.AdaptiveColor{Light: "#874BFD", Dark: "#7D56F4"}
	special   = lipgloss.AdaptiveColor{Light: "#43BF6D", Dark: "#73F59F"}

	divider = lipgloss.NewStyle().
		SetString("•").
		Padding(0, 1).
		Foreground(subtle).
		String()
	url = lipgloss.NewStyle().Foreground(subtle)

	infoStyle = lipgloss.NewStyle().
			Foreground(subtle).
			BorderStyle(lipgloss.NormalBorder()).
			BorderForeground(subtle).
			Padding(0, 1)

	helpStyle = lipgloss.NewStyle().
			Foreground(subtle).
			Italic(true)

	itemStyle = lipgloss.NewStyle().PaddingLeft(4)

	activeItemStyle = lipgloss.NewStyle().
			PaddingLeft(2).
			Foreground(highlight).
			SetString("> ")

	checkMark = lipgloss.NewStyle().SetString("✓").
			Foreground(special).
			PaddingRight(1).
			String()

	warningStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("208")).
			PaddingRight(1).
			String()

	errorStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("196")).
			PaddingRight(1).
			String()

	successStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("40")).
			PaddingRight(1).
			String()
)

func GenerateParquetCmd() *cobra.Command {
	var (
		inputFile  string
		outputFile string
	)

	cmd := &cobra.Command{
		Use:   "generate",
		Short: "Generate Parquet file",
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Println(checkMark + "Generating Parquet file...")
			err := converter.ConvertAvroToParquet(context.Background(), inputFile, outputFile, 100000, compress.Codecs.Snappy)
			if err != nil {
				fmt.Println(errorStyle + "Error generating Parquet file: " + err.Error())
			}
			fmt.Println(checkMark + "Parquet file generated successfully")
		},
	}

	cmd.Flags().StringVarP(&inputFile, "input", "i", "", "Input Avro file path")
	cmd.Flags().StringVarP(&outputFile, "output", "o", "", "Output Parquet file path")

	cmd.MarkFlagRequired("input")
	cmd.MarkFlagRequired("output")

	return cmd
}

func ParquetToCSVCmd() *cobra.Command {
	var (
		inputFile  string
		outputFile string
	)

	cmd := &cobra.Command{
		Use:   "parquet2csv",
		Short: "Convert Parquet to CSV",
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Println(checkMark + "Converting Parquet to CSV...")
			err := converter.ConvertParquetToCSV(context.Background(), inputFile, outputFile, false, 1000000, []string{}, []int{}, true, ',', true, "", nil, nil)
			if err != nil {
				fmt.Println(errorStyle + "Error converting Parquet to CSV: " + err.Error())
			}
			fmt.Println(checkMark + "CSV file generated successfully")
		},
	}

	cmd.Flags().StringVarP(&inputFile, "input", "i", "", "Input Parquet file path")
	cmd.Flags().StringVarP(&outputFile, "output", "o", "", "Output CSV file path")

	cmd.MarkFlagRequired("input")
	cmd.MarkFlagRequired("output")

	return cmd
}

func CSVToParquetCmd() *cobra.Command {
	var (
		inputFile  string
		outputFile string
	)

	cmd := &cobra.Command{
		Use:   "csv2parquet",
		Short: "Convert CSV to Parquet",
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Println(checkMark + "Converting CSV to Parquet...")
			err := converter.ConvertCSVToParquet(context.Background(), inputFile, outputFile, true, 1000000, ',', []string{}, true)
			if err != nil {
				fmt.Println(errorStyle + "Error converting CSV to Parquet: " + err.Error())
			}
			fmt.Println(checkMark + "Parquet file generated successfully")
		},
	}

	cmd.Flags().StringVarP(&inputFile, "input", "i", "", "Input CSV file path")
	cmd.Flags().StringVarP(&outputFile, "output", "o", "", "Output Parquet file path")

	cmd.MarkFlagRequired("input")
	cmd.MarkFlagRequired("output")

	return cmd
}

func ParquetToJSONCmd() *cobra.Command {
	var (
		inputFile  string
		outputFile string
	)

	cmd := &cobra.Command{
		Use:   "parquet2json",
		Short: "Convert Parquet to JSON",
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Println(checkMark + "Converting Parquet to JSON...")
			err := converter.ConvertParquetToJSON(context.Background(), inputFile, outputFile, false, 1000000, []string{}, []int{}, true, false)
			if err != nil {
				fmt.Println(errorStyle + "Error converting Parquet to JSON: " + err.Error())
			}
			fmt.Println(checkMark + "JSON file generated successfully")
		},
	}

	cmd.Flags().StringVarP(&inputFile, "input", "i", "", "Input Parquet file path")
	cmd.Flags().StringVarP(&outputFile, "output", "o", "", "Output JSON file path")

	cmd.MarkFlagRequired("input")
	cmd.MarkFlagRequired("output")

	return cmd
}

func RewriteParquetCmd() *cobra.Command {
	var (
		inputFile  string
		outputFile string
	)

	cmd := &cobra.Command{
		Use:   "rewrite",
		Short: "Rewrite Parquet file",
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Println(checkMark + "Rewriting Parquet file...")
			err := pq.RewriteParquetFile(context.Background(), inputFile, outputFile, false, 1000000, []string{}, []int{}, true, nil)
			if err != nil {
				fmt.Println(errorStyle + "Error rewriting Parquet file: " + err.Error())
			}
			fmt.Println(checkMark + "Parquet file rewritten successfully")
		},
	}

	cmd.Flags().StringVarP(&inputFile, "input", "i", "", "Input Parquet file path")
	cmd.Flags().StringVarP(&outputFile, "output", "o", "", "Output Parquet file path")

	cmd.MarkFlagRequired("input")
	cmd.MarkFlagRequired("output")

	return cmd
}

func FlightCmd() *cobra.Command {
	var (
		serverAddress string
		action        string
	)

	cmd := &cobra.Command{
		Use:   "flight",
		Short: "Run flight tests",
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Println(checkMark + "Starting Sqlite Flight Server on " + serverAddress + "...")
			serverMiddleware := []flight.ServerMiddleware{
				flight.CreateServerMiddleware(&sqlite.ServerExpectHeaderMiddleware{}),
				flight.CreateServerMiddleware(&sqlite.ServerMiddlewareAddHeader{}),
			}

			// Initialize the Flight server with middleware
			s := flight.NewServerWithMiddleware(serverMiddleware)
			s.Init(serverAddress)

			// Set up the Flight SQL server (e.g., SQLite-based server)
			db, err := sqlite.CreateDB()
			if err != nil {
				fmt.Println(errorStyle + "Error creating SQLite database: " + err.Error())
				return
			}

			srv, err := sqlite.NewSQLiteFlightSQLServer(db)
			if err != nil {
				fmt.Println(errorStyle + "Error creating SQLite Flight SQL server: " + err.Error())
				return
			}
			s.RegisterFlightService(flightsql.NewFlightServer(srv))
			// Start the server in a goroutine
			go s.Serve()
			defer s.Shutdown()

			fmt.Println(checkMark + "Flight Server started successfully")
		},
	}

	cmd.Flags().StringVarP(&serverAddress, "server", "s", "localhost:8080", "Flight server address")
	cmd.Flags().StringVarP(&action, "action", "a", "list", "Flight action (list, put, get)")

	return cmd
}

func AvroToParquetCmd() *cobra.Command {

	var (
		inputFile  string
		outputFile string
	)

	cmd := &cobra.Command{
		Use:   "avro2parquet",
		Short: "Convert Avro to Parquet",
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Println(checkMark + "Converting Avro to Parquet...")
			err := converter.ConvertAvroToParquet(context.Background(), inputFile, outputFile, 100000, compress.Codecs.Snappy)
			if err != nil {
				fmt.Println(errorStyle + "Error converting Avro to Parquet: " + err.Error())
			}
			fmt.Println(checkMark + "Parquet file generated successfully")
		},
	}

	cmd.Flags().StringVarP(&inputFile, "input", "i", "", "Input Avro file path")
	cmd.Flags().StringVarP(&outputFile, "output", "o", "", "Output Parquet file path")

	cmd.MarkFlagRequired("input")
	cmd.MarkFlagRequired("output")

	return cmd
}

func DisplayMenu() {
	menu := lipgloss.JoinVertical(lipgloss.Left,
		itemStyle.Render("1. Generate Parquet file"),
		itemStyle.Render("2. Convert Parquet to CSV"),
		itemStyle.Render("3. Convert CSV to Parquet"),
		itemStyle.Render("4. Convert Parquet to JSON"),
		itemStyle.Render("5. Rewrite Parquet file"),
		itemStyle.Render("6. Run flight tests"),
		itemStyle.Render("7. Convert Avro to Parquet"),
		itemStyle.Render("8. Exit"),
	)

	fmt.Println(lipgloss.NewStyle().
		BorderStyle(lipgloss.RoundedBorder()).
		BorderForeground(highlight).
		Padding(1).
		Render(menu))
}
