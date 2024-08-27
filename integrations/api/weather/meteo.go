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

package integrations

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"

	"github.com/goccy/go-json"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	memoryPool "github.com/arrowarc/arrowarc/internal/memory"
	config "github.com/arrowarc/arrowarc/pkg/common/config"
)

// WeatherReader reads weather data from an API and implements the Reader interface.
type WeatherReader struct {
	ctx      context.Context
	cities   []config.City
	client   *http.Client
	schema   *arrow.Schema
	currCity int
	alloc    memory.Allocator
}

// NewWeatherReader creates a new reader for reading weather data from a list of cities.
func NewWeatherReader(ctx context.Context, cities []config.City, client *http.Client) (*WeatherReader, error) {
	alloc := memoryPool.GetAllocator()

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "city", Type: arrow.BinaryTypes.String},
		{Name: "latitude", Type: arrow.PrimitiveTypes.Float64},
		{Name: "longitude", Type: arrow.PrimitiveTypes.Float64},
		{Name: "temperature", Type: arrow.PrimitiveTypes.Float64},
	}, nil)

	if client == nil {
		client = http.DefaultClient
	}

	return &WeatherReader{
		ctx:      ctx,
		cities:   cities,
		client:   client,
		schema:   schema,
		currCity: 0,
		alloc:    alloc,
	}, nil
}

// Schema returns the schema of the records being read from the Weather API.
func (r *WeatherReader) Schema() *arrow.Schema {
	return r.schema
}

// Read reads the next record of weather data from the API.
func (r *WeatherReader) Read() (arrow.Record, error) {
	if r.currCity >= len(r.cities) {
		return nil, io.EOF
	}

	city := r.cities[r.currCity]
	r.currCity++

	jsonData, err := fetchWeatherData(r.ctx, city.Latitude, city.Longitude, r.client)
	if err != nil {
		return nil, err
	}

	jsonDataBytes, err := json.Marshal(jsonData)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal JSON data: %w", err)
	}

	jsonReader := array.NewJSONReader(bytes.NewReader(jsonDataBytes), r.schema)
	if jsonReader == nil {
		return nil, fmt.Errorf("failed to create JSON reader")
	}
	defer jsonReader.Release()

	if jsonReader.Next() {
		record := jsonReader.Record()
		record.Retain() // Retain the record to ensure it stays valid after returning
		return record, nil
	}

	if err := jsonReader.Err(); err != nil {
		return nil, err
	}

	return nil, io.EOF
}

// Close releases any resources associated with the WeatherReader.
func (r *WeatherReader) Close() error {
	defer memoryPool.PutAllocator(r.alloc)
	// Additional cleanup logic if needed
	return nil
}

// fetchWeatherData calls the Open-Meteo API to retrieve weather data for a specific location.
func fetchWeatherData(ctx context.Context, latitude, longitude float64, client *http.Client) (map[string]interface{}, error) {
	url := fmt.Sprintf("%s?latitude=%.4f&longitude=%.4f&hourly=temperature_2m&current_weather=true", config.OpenMeteoAPIURL, latitude, longitude)

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to call Open-Meteo API: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("Open-Meteo API error: %s", resp.Status)
	}

	var result map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("failed to decode API response: %w", err)
	}

	return result, nil
}
