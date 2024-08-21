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
	"testing"
	"time"

	weather "github.com/ArrowArc/ArrowArc/integrations/api/weather"
	"github.com/ArrowArc/ArrowArc/pkg/common/config"
	"github.com/stretchr/testify/assert"
)

func TestWeatherAPIStreamToDuckDB(t *testing.T) {
	cities := []config.City{
		{Name: "New York", Latitude: 40.7128, Longitude: -74.0060},
		{Name: "Tokyo", Latitude: 35.6895, Longitude: 139.6917},
		{Name: "London", Latitude: 51.5074, Longitude: -0.1278},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	recordChan, errChan := weather.ReadWeatherAPIStream(ctx, cities)

	var errs []error
	done := make(chan struct{})

	go func() {
		for err := range errChan {
			if err != nil {
				errs = append(errs, err)
			}
		}
		close(done)
	}()

	for record := range recordChan {
		assert.NotNil(t, record)
	}

	<-done

	for _, err := range errs {
		assert.NoError(t, err)
	}
}
