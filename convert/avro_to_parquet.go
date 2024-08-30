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

package convert

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/apache/arrow/go/v17/arrow/avro"
	"github.com/apache/arrow/go/v17/parquet"
	"github.com/apache/arrow/go/v17/parquet/compress"
	pq "github.com/apache/arrow/go/v17/parquet/pqarrow"
)

// ConvertAvroToParquet converts an Avro OCF file to a Parquet file.
func ConvertAvroToParquet(
	ctx context.Context,
	avroFilePath, parquetFilePath string,
	chunkSize int,
	compression compress.Compression,
) error {
	// Validate input parameters
	if avroFilePath == "" {
		return errors.New("avro file path cannot be empty")
	}
	if parquetFilePath == "" {
		return errors.New("parquet file path cannot be empty")
	}
	if chunkSize <= 0 {
		return errors.New("chunk size must be greater than zero")
	}
	if ctx == nil {
		return errors.New("context cannot be nil")
	}

	// Read the Avro file
	data, err := os.ReadFile(avroFilePath)
	if err != nil {
		return fmt.Errorf("failed to read Avro file '%s': %w", avroFilePath, err)
	}
	ts := time.Now()
	r := bytes.NewReader(data)
	ior := bufio.NewReaderSize(r, 4096*8)
	chunk := 1024 * 8
	av2arReader, err := avro.NewOCFReader(ior, avro.WithChunk(chunk))
	if err != nil {
		return fmt.Errorf("failed to create Avro OCF reader: %w", err)
	}
	defer av2arReader.Release()

	// Write the Parquet file
	fp, err := os.OpenFile(avroFilePath+".parquet", os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		fmt.Println(err)
		os.Exit(4)
	}
	defer fp.Close()
	pwProperties := parquet.NewWriterProperties(parquet.WithDictionaryDefault(true),
		parquet.WithVersion(parquet.V2_LATEST),
		parquet.WithCompression(compress.Codecs.Snappy),
		parquet.WithBatchSize(1024*32),
		parquet.WithDataPageSize(1024*1024),
		parquet.WithMaxRowGroupLength(64*1024*1024),
	)
	awProperties := pq.NewArrowWriterProperties(pq.WithStoreSchema())

	pr, err := pq.NewFileWriter(av2arReader.Schema(), fp, pwProperties, awProperties)
	if err != nil {
		fmt.Println(err)
		os.Exit(5)
	}
	defer pr.Close()
	fmt.Printf("parquet version: %v\n", pwProperties.Version())

	for av2arReader.Next() {
		if av2arReader.Err() != nil {
			fmt.Println(err)
			os.Exit(6)
		}
		recs := av2arReader.Record()
		err = pr.WriteBuffered(recs)
		if err != nil {
			fmt.Println(err)
			os.Exit(7)
		}
		recs.Release()
	}
	if av2arReader.Err() != nil {
		fmt.Println(av2arReader.Err())
	}

	pr.Close()
	log.Printf("time to convert: %v\n", time.Since(ts))
	return nil

}
