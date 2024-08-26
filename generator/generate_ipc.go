package generator

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/arrio"
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
