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
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	xtypes "github.com/ArrowArc/ArrowArc/internal/dbarrow/types"
	"github.com/ArrowArc/ArrowArc/pkg/common/config"
	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
)

func ReadWeatherAPIStream(ctx context.Context, cities []config.City) (<-chan arrow.Record, <-chan error) {
	recordChan := make(chan arrow.Record)
	errChan := make(chan error, 1)

	go func() {
		defer close(recordChan)
		defer close(errChan)

		pool := memory.NewGoAllocator()
		schema := config.OpenMeteoSchema

		for _, city := range cities {
			select {
			case <-ctx.Done():
				errChan <- ctx.Err()
				return
			default:
			}

			temp, err := fetchWeatherData(ctx, city.Latitude, city.Longitude)
			if err != nil {
				errChan <- err
				return
			}

			bldr := array.NewRecordBuilder(pool, schema)
			defer bldr.Release()

			bldr.Field(0).(*array.StringBuilder).Append(city.Name)

			// Use the JSONBuilder since the schema defines the temperature as JSON
			jsonBuilder := bldr.Field(1).(*xtypes.JSONBuilder)
			jsonData := map[string]float64{"temperature": temp}
			jsonBuilder.Append(jsonData)

			rec := bldr.NewRecord()
			recordChan <- rec
			rec.Release()
		}
	}()

	return recordChan, errChan
}

func fetchWeatherData(ctx context.Context, latitude, longitude float64) (float64, error) {
	url := fmt.Sprintf("%s?latitude=%.4f&longitude=%.4f&hourly=temperature_2m&current_weather=true", config.OpenMeteoAPIURL, latitude, longitude)

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return 0, fmt.Errorf("failed to create request: %w", err)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, fmt.Errorf("failed to call Open-Meteo API: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("Open-Meteo API error: %s", resp.Status)
	}

	var result struct {
		CurrentWeather struct {
			Temperature float64 `json:"temperature"`
		} `json:"current_weather"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return 0, fmt.Errorf("failed to decode API response: %w", err)
	}

	return result.CurrentWeather.Temperature, nil
}
