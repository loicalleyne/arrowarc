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

// Package config provides configuration utilities.
package config

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Workflow struct {
		Version      string        `yaml:"version"`
		Name         string        `yaml:"name"`
		Description  string        `yaml:"description"`
		Integrations []Integration `yaml:"integrations"`
		Conversions  []Conversion  `yaml:"conversions"`
		Tasks        []Task        `yaml:"tasks"`
		Settings     Settings      `yaml:"settings"`
		Secrets      []Secret      `yaml:"secrets"`
		Monitoring   struct {
			Enable          bool              `yaml:"enable"`
			MetricsEndpoint string            `yaml:"metrics_endpoint"`
			AlertThresholds map[string]string `yaml:"alert_thresholds"`
		} `yaml:"monitoring"`
		Resources struct {
			CPULimit         string `yaml:"cpu_limit"`
			MemoryLimit      string `yaml:"memory_limit"`
			StorageLimit     string `yaml:"storage_limit"`
			ExecutionTimeout string `yaml:"execution_timeout"`
			MaxRetries       int    `yaml:"max_retries"`
		} `yaml:"resources"`
	} `yaml:"workflow"`
}

type Settings struct {
	ParallelTasks int    `yaml:"parallel_tasks"`
	RetryAttempts int    `yaml:"retry_attempts"`
	LogLevel      string `yaml:"log_level"`
	TempDirectory string `yaml:"temp_directory"`
	MaxMemory     string `yaml:"max_memory"`
}

type Secret struct {
	Name     string `yaml:"name"`
	Type     string `yaml:"type"`
	Provider string `yaml:"provider"`
	Path     string `yaml:"path"`
	Key      string `yaml:"key"`
	Version  string `yaml:"version"`
}

type Integration struct {
	Name     string                 `yaml:"name"`
	Type     string                 `yaml:"type"`
	Provider string                 `yaml:"provider"`
	Mode     string                 `yaml:"mode"`
	Config   map[string]interface{} `yaml:"config"`
}

type Conversion struct {
	Name         string                 `yaml:"name"`
	InputFormat  string                 `yaml:"input_format"`
	OutputFormat string                 `yaml:"output_format"`
	Options      map[string]interface{} `yaml:"options"`
}

type Task struct {
	Name        string `yaml:"name"`
	Source      string `yaml:"source"`
	Destination string `yaml:"destination"`
	Conversion  string `yaml:"conversion"`
	Query       string `yaml:"query,omitempty"`
	FileName    string `yaml:"file_name,omitempty"`
}

// SecretProvider enums
type SecretProvider string

const (
	SecretProviderHashicorp SecretProvider = "hashicorp"
	SecretProviderAWS       SecretProvider = "aws"
	SecretProviderGCP       SecretProvider = "gcp"
	SecretProviderAzure     SecretProvider = "azure"
)

func ParseConfig(configPath string) (*Config, error) {
	configFile, err := os.Open(configPath)
	if err != nil {
		return nil, err
	}
	defer configFile.Close()

	var config Config
	decoder := yaml.NewDecoder(configFile)
	err = decoder.Decode(&config)
	if err != nil {
		return nil, err
	}

	return &config, nil
}

func (c *Config) Validate() error {
	// Basic settings validation
	if err := c.validateSettings(); err != nil {
		return err
	}

	// Validate integrations (sources and destinations)
	if err := c.validateIntegrations(); err != nil {
		return err
	}

	// Validate conversions
	if err := c.validateConversions(); err != nil {
		return err
	}

	// Validate secrets
	if err := c.validateSecrets(); err != nil {
		return err
	}

	// Validate tasks (depends on integrations and conversions)
	if err := c.validateTasks(); err != nil {
		return err
	}

	return nil
}

func (c *Config) validateSettings() error {
	if c.Workflow.Settings.ParallelTasks < 1 {
		return fmt.Errorf("parallel_tasks must be greater than 0")
	}
	if c.Workflow.Settings.RetryAttempts <= 0 {
		return fmt.Errorf("retry_attempts must be greater than 0")
	}
	return nil
}

func (c *Config) validateIntegrations() error {
	for _, integration := range c.Workflow.Integrations {
		if integration.Name == "" {
			return fmt.Errorf("integration name cannot be empty")
		}
		if integration.Type == "" {
			return fmt.Errorf("integration '%s' must have a type", integration.Name)
		}
		if integration.Provider == "" {
			return fmt.Errorf("integration '%s' must have a provider", integration.Name)
		}
	}
	return nil
}

func (c *Config) validateConversions() error {
	for _, conversion := range c.Workflow.Conversions {
		if conversion.Name == "" {
			return fmt.Errorf("conversion name cannot be empty")
		}
		if conversion.InputFormat == "" || conversion.OutputFormat == "" {
			return fmt.Errorf("conversion '%s' must have both input and output formats", conversion.Name)
		}
	}
	return nil
}

func (c *Config) validateSecrets() error {
	for _, secret := range c.Workflow.Secrets {
		if secret.Name == "" || secret.Type == "" {
			return fmt.Errorf("secret name and type are required")
		}
		if secret.Provider == "" {
			return fmt.Errorf("secret '%s' must have a provider", secret.Name)
		}
	}
	return nil
}

func (c *Config) validateTasks() error {
	for _, task := range c.Workflow.Tasks {
		if task.Name == "" {
			return fmt.Errorf("task name cannot be empty")
		}
		if task.Source == "" {
			return fmt.Errorf("task '%s' must have a source", task.Name)
		}
		if task.Destination == "" {
			return fmt.Errorf("task '%s' must have a destination", task.Name)
		}
		if task.Conversion == "" {
			return fmt.Errorf("task '%s' must have a conversion", task.Name)
		}
		// Additional checks could be added here to ensure that the source, destination,
		// and conversion referenced in the task actually exist in the configuration
	}
	return nil
}
