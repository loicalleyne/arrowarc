package main

import (
	"fmt"
	"os"

	cli "github.com/arrowarc/arrowarc/internal/cli"
)

func main() {
	if err := cli.RunMenu(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}
