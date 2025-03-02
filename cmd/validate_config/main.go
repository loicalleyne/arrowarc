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
	"fmt"
	"log"
	"os"

	"github.com/arrowarc/arrowarc/pkg/common/config"
	"github.com/docopt/docopt-go"
)

const defaultConfigPath = "../config/workflow.yaml"

func main() {
	usage := `ArrowArc Configuration Validator.

Usage:
  arrowarc-validate-config [--config=<config_file>]
  arrowarc-validate-config -h | --help

Options:
  -h --help                          Show this screen.
  --config=<config_file>             Path to the ArrowArc configuration file. [default: ../config/workflow.yaml]
`

	// Parse command-line arguments
	arguments, err := docopt.ParseDoc(usage)
	if err != nil {
		log.Fatalf("Error parsing arguments: %v", err)
	}

	// Retrieve config file path
	configPath := getConfigPath(arguments)

	// Parse and validate the configuration
	cfg, err := config.ParseConfig(configPath)
	if err != nil {
		log.Fatalf("Failed to parse config: %v", err)
	}

	if err := cfg.Validate(); err != nil {
		log.Fatalf("Configuration validation failed: %v", err)
	}

	fmt.Println("Configuration is valid.")
}

// getConfigPath returns the config path provided by the user or the default path
func getConfigPath(arguments docopt.Opts) string {
	configPath, _ := arguments.String("--config")

	// If no config path is provided, check the default location
	if configPath == "" {
		if _, err := os.Stat(defaultConfigPath); err == nil {
			return defaultConfigPath
		}
		log.Fatalf("Configuration file path is required. Use --config=<config_file>")
	}

	// Check if the provided config file path exists
	if _, err := os.Stat(configPath); os.IsNotExist(err) {
		log.Fatalf("Configuration file '%s' does not exist.", configPath)
	}

	return configPath
}
