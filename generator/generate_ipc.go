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

// Package generator provides utilities for generating test data in various formats.
package generator

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/arrio"
	integrations "github.com/arrowarc/arrowarc/integrations/filesystem"
)

func GenerateIPCFiles(ctx context.Context, dir string, recordSets map[string][]arrow.Record) error {
	if err := os.MkdirAll(dir, 0755); err != nil && !os.IsExist(err) {
		return fmt.Errorf("failed to create directory: %w", err)
	}

	for name, records := range recordSets {
		if len(records) == 0 {
			continue
		}

		filePath := filepath.Join(dir, fmt.Sprintf("%s.arrow", name))

		if err := writeIPCFile(ctx, filePath, records); err != nil {
			return fmt.Errorf("failed to write IPC file for %s: %w", name, err)
		}
	}

	return nil
}

func writeIPCFile(ctx context.Context, filePath string, records []arrow.Record) error {

	writer, err := integrations.NewIPCRecordWriter(ctx, filePath, records[0].Schema())
	if err != nil {
		return fmt.Errorf("failed to create IPC writer: %w", err)
	}

	reader, err := integrations.NewIPCRecordReader(ctx, filePath)
	if err != nil {
		return fmt.Errorf("failed to create IPC reader: %w", err)
	}

	if _, err := arrio.Copy(writer, reader); err != nil {
		return fmt.Errorf("failed to copy records to IPC file: %w", err)
	}

	return nil
}
