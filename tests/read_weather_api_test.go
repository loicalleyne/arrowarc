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

package test

import (
	"context"
	"io"
	"net/http"
	"testing"
	"time"

	weather "github.com/arrowarc/arrowarc/integrations/api/weather"
	config "github.com/arrowarc/arrowarc/pkg/common/config"
	helper "github.com/arrowarc/arrowarc/pkg/common/utils"
	"github.com/stretchr/testify/assert"
)

func TestWeatherAPIStream(t *testing.T) {
	// Define the list of cities to fetch data for
	cities := []config.City{
		{Name: "New York", Latitude: 40.7128, Longitude: -74.0060},
		{Name: "Tokyo", Latitude: 35.6895, Longitude: 139.6917},
		{Name: "London", Latitude: 51.5074, Longitude: -0.1278},
	}

	// Set up a context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	// Create a new HTTP client
	client := http.DefaultClient

	// Create a new WeatherAPIReader
	reader, err := weather.NewWeatherReader(ctx, cities, client)
	assert.NoError(t, err, "Error should be nil when creating Weather API reader")

	// Read records from the Weather API
	var recordsRead int
	for {
		record, err := reader.Read()
		if err == context.Canceled || err == io.EOF {
			break
		}
		assert.NoError(t, err, "Error should be nil when reading from Weather API stream")

		assert.NotNil(t, record, "Record should not be nil")
		helper.PrintRecordBatch(record)
		recordsRead += int(record.NumRows())
		record.Release() // Release the record after processing
	}

	assert.Greater(t, recordsRead, 0, "Should have read at least one record")
}
