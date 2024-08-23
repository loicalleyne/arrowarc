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
	"encoding/json"
	"fmt"
	"net/http"

	config "github.com/ArrowArc/ArrowArc/pkg/common/config"
	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
)

func ReadWeatherAPIStream(ctx context.Context, cities []config.City) (<-chan arrow.Record, <-chan error) {
	recordChan := make(chan arrow.Record)
	errChan := make(chan error, 1)

	go func() {
		defer close(recordChan)
		defer close(errChan)

		schema := arrow.NewSchema([]arrow.Field{
			{Name: "city", Type: arrow.BinaryTypes.String},
			{Name: "latitude", Type: arrow.PrimitiveTypes.Float64},
			{Name: "longitude", Type: arrow.PrimitiveTypes.Float64},
			{Name: "temperature", Type: arrow.PrimitiveTypes.Float64},
		}, nil)

		for _, city := range cities {
			select {
			case <-ctx.Done():
				errChan <- ctx.Err()
				return
			default:
			}

			jsonData, err := fetchWeatherData(ctx, city.Latitude, city.Longitude)
			if err != nil {
				errChan <- err
				return
			}

			jsonDataBytes, err := json.Marshal(jsonData)
			if err != nil {
				errChan <- err
				return
			}

			jsonReader := array.NewJSONReader(bytes.NewReader(jsonDataBytes), schema)
			if jsonReader == nil {
				errChan <- fmt.Errorf("failed to create JSON reader")
				return
			}
			defer jsonReader.Release()

			for jsonReader.Next() {
				record := jsonReader.Record()
				if record == nil {
					continue
				}

				record.Retain()
				recordChan <- record
			}

			if err := jsonReader.Err(); err != nil {
				errChan <- err
				return
			}
		}
	}()

	return recordChan, errChan
}

func fetchWeatherData(ctx context.Context, latitude, longitude float64) (map[string]interface{}, error) {
	url := fmt.Sprintf("%s?latitude=%.4f&longitude=%.4f&hourly=temperature_2m&current_weather=true", config.OpenMeteoAPIURL, latitude, longitude)

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	resp, err := http.DefaultClient.Do(req)
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
