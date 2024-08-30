package cli

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/apache/arrow/go/v17/parquet/compress"
	converter "github.com/arrowarc/arrowarc/convert"
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
	return &cobra.Command{
		Use:   "generate",
		Short: "Generate Parquet file",
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Println(checkMark + "Generating Parquet file...")
			// Actual implementation would go here
		},
	}
}

func ParquetToCSVCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "parquet2csv",
		Short: "Convert Parquet to CSV",
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Println(checkMark + "Converting Parquet to CSV...")
			// Actual implementation would go here
		},
	}
}

func CSVToParquetCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "csv2parquet",
		Short: "Convert CSV to Parquet",
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Println(checkMark + "Converting CSV to Parquet...")
			// Actual implementation would go here
		},
	}
}

func ParquetToJSONCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "parquet2json",
		Short: "Convert Parquet to JSON",
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Println(checkMark + "Converting Parquet to JSON...")
			// Actual implementation would go here
		},
	}
}

func RewriteParquetCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "rewrite",
		Short: "Rewrite Parquet file",
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Println(checkMark + "Rewriting Parquet file...")
			// Actual implementation would go here
		},
	}
}

func FlightCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "flight",
		Short: "Run flight tests",
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Println(checkMark + "Running flight tests...")
			// Actual implementation would go here
		},
	}
}

func AvroToParquetCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "avro2parquet",
		Short: "Convert Avro to Parquet",
		Run: func(cmd *cobra.Command, args []string) {
			reader := bufio.NewReader(os.Stdin)

			fmt.Print("Enter the path to the Avro file: ")
			avroPath, _ := reader.ReadString('\n')
			avroPath = strings.TrimSpace(avroPath)

			fmt.Print("Enter the path for the output Parquet file: ")
			parquetPath, _ := reader.ReadString('\n')
			parquetPath = strings.TrimSpace(parquetPath)

			fmt.Println(checkMark + "Converting Avro to Parquet...")

			// Create a context with a timeout
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
			defer cancel()

			// Run the conversion in a goroutine
			errChan := make(chan error, 1)
			go func() {
				errChan <- converter.ConvertAvroToParquet(ctx, avroPath, parquetPath, 1, compress.Codecs.Snappy)
			}()

			// Wait for the conversion to complete or for the context to be cancelled
			select {
			case err := <-errChan:
				if err != nil {
					fmt.Printf("Error during conversion: %v\n", err)
				} else {
					fmt.Printf("Converted %s to %s\n", avroPath, parquetPath)
				}
			case <-ctx.Done():
				fmt.Println("Conversion timed out or was cancelled")
			}
		},
	}
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
