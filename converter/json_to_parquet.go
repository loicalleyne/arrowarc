package converter

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"

	"go.uber.org/zap"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
)

// InferSchemaFromReader reads JSON lines from r and infers an Arrow schema
// using up to maxCount records. It returns the inferred schema, the number of
// records scanned, and any error encountered.
func InferSchemaFromReader(r io.Reader, maxCount int) (*arrow.Schema, int, error) {
	logger, _ := zap.NewProduction()
	defer logger.Sync()

	scanner := bufio.NewScanner(r)
	var samples []map[string]interface{}
	count := 0

	for scanner.Scan() {
		line := scanner.Bytes()
		var obj map[string]interface{}
		if err := json.Unmarshal(line, &obj); err != nil {
			logger.Error("failed to unmarshal JSON", zap.Error(err))
			return nil, count, fmt.Errorf("failed to parse JSON: %w", err)
		}
		samples = append(samples, obj)
		count++
		if count >= maxCount {
			break
		}
	}
	if err := scanner.Err(); err != nil {
		logger.Error("scanner error", zap.Error(err))
		return nil, count, err
	}
	schema, err := inferSchema(samples)
	if err != nil {
		logger.Error("failed to infer schema", zap.Error(err))
		return nil, count, err
	}
	return schema, count, nil
}

// inferSchema infers an Arrow schema from a slice of JSON objects.
// For each field, if conflicting types are encountered, the type falls back to string.
func inferSchema(samples []map[string]interface{}) (*arrow.Schema, error) {
	fieldTypes := make(map[string]arrow.DataType)
	for _, obj := range samples {
		for k, v := range obj {
			inferred, err := inferType(v)
			if err != nil {
				return nil, err
			}
			if existing, ok := fieldTypes[k]; ok {
				if existing.ID() != inferred.ID() {
					// Conflicting types: default to string.
					fieldTypes[k] = arrow.BinaryTypes.String
				}
			} else {
				fieldTypes[k] = inferred
			}
		}
	}
	var fields []arrow.Field
	for name, dt := range fieldTypes {
		fields = append(fields, arrow.Field{Name: name, Type: dt, Nullable: true})
	}
	// Sort fields alphabetically for consistency.
	sort.Slice(fields, func(i, j int) bool {
		return fields[i].Name < fields[j].Name
	})
	return arrow.NewSchema(fields, nil), nil
}

// inferType determines an Arrow data type from a JSON value.
// JSON numbers become float64; booleans and strings are mapped to their Arrow equivalents.
// If v is nil or an unrecognized type, string is returned.
func inferType(v interface{}) (arrow.DataType, error) {
	if v == nil {
		return arrow.BinaryTypes.String, nil
	}
	switch v.(type) {
	case bool:
		return arrow.FixedWidthTypes.Boolean, nil
	case float64:
		return arrow.PrimitiveTypes.Float64, nil
	case string:
		return arrow.BinaryTypes.String, nil
	default:
		return arrow.BinaryTypes.String, nil
	}
}

// SchemaFromFile opens a JSON file and infers its Arrow schema by reading up to maxCount records.
func SchemaFromFile(inputFile string, maxCount int) (*arrow.Schema, int, error) {
	logger, _ := zap.NewProduction()
	defer logger.Sync()

	f, err := os.Open(inputFile)
	if err != nil {
		logger.Error("failed to open input file", zap.String("file", inputFile), zap.Error(err))
		return nil, 0, err
	}
	defer f.Close()

	r := bufio.NewReaderSize(f, 32*1024)
	return InferSchemaFromReader(r, maxCount)
}

// RecordsFromFile reads JSON records from inputFile, converts them to Arrow records
// using the provided schema, and writes them to outputFile in Parquet format.
// If munger is non-nil, the input stream is first processed through it.
func RecordsFromFile(inputFile, outputFile string, schema *arrow.Schema, munger func(io.Reader, io.Writer) error, writerProps ...parquet.WriterProperty) (int, error) {
	logger, _ := zap.NewProduction()
	defer logger.Sync()

	f, err := os.Open(inputFile)
	if err != nil {
		logger.Error("failed to open input file", zap.String("file", inputFile), zap.Error(err))
		return 0, err
	}
	defer f.Close()

	outFile, err := os.Create(outputFile)
	if err != nil {
		logger.Error("failed to create output file", zap.String("file", outputFile), zap.Error(err))
		return 0, err
	}
	defer outFile.Close()

	// Create Arrow writer
	arrowWriter, err := pqarrow.NewFileWriter(schema, outFile, parquet.NewWriterProperties(writerProps...), pqarrow.NewArrowWriterProperties(pqarrow.WithAllocator(memory.DefaultAllocator)))
	if err != nil {
		logger.Error("failed to create Arrow writer", zap.Error(err))
		return 0, err
	}

	// Set up the JSON reader
	const chunk = 1024
	var rdr *array.JSONReader
	rReader := bufio.NewReaderSize(f, 128*1024*1024)
	if munger != nil {
		pr, pwPipe := io.Pipe()
		go func() {
			defer pwPipe.Close()
			if err := munger(rReader, pwPipe); err != nil {
				logger.Error("munger error", zap.Error(err))
			}
		}()
		rdr = array.NewJSONReader(pr, schema, array.WithChunk(chunk))
	} else {
		rdr = array.NewJSONReader(rReader, schema, array.WithChunk(chunk))
	}
	defer rdr.Release()

	totalRecords := 0
	for rdr.Next() {
		rec := rdr.Record()
		if err := arrowWriter.Write(rec); err != nil {
			logger.Error("failed to write record batch", zap.Error(err))
			return totalRecords, fmt.Errorf("failed to write record batch: %w", err)
		}
		totalRecords += int(rec.NumRows())
	}

	if err := rdr.Err(); err != nil {
		logger.Error("JSON reader error", zap.Error(err))
		return totalRecords, err
	}

	if err := arrowWriter.Close(); err != nil {
		logger.Error("error closing Arrow writer", zap.Error(err))
		return totalRecords, err
	}

	if err := outFile.Close(); err != nil {
		logger.Error("error closing Parquet writer", zap.Error(err))
		return totalRecords, err
	}

	return totalRecords, nil
}
