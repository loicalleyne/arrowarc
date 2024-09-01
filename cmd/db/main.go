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

package main

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/apache/arrow/go/v16/arrow"
	"github.com/apache/arrow/go/v16/arrow/memory"
	"github.com/polarsignals/frostdb"
	"github.com/polarsignals/frostdb/query"
	"github.com/polarsignals/frostdb/query/logicalplan"
)

var (
	columnstore *frostdb.ColumnStore
	database    *frostdb.DB
)

func main() {
	initFrostDB()
	loadDemoData()

	engine := query.NewEngine(memory.DefaultAllocator, database.TableProvider())

	reader := bufio.NewReader(os.Stdin)
	for {
		fmt.Print("Enter query type (city_stats/day_snowfall) or 'quit' to exit: ")
		queryType, _ := reader.ReadString('\n')
		queryType = strings.TrimSpace(queryType)

		if queryType == "quit" {
			break
		}

		var result []arrow.Record
		var err error

		switch queryType {
		case "city_stats":
			err = executeCityStatsQuery(engine, &result)
		case "day_snowfall":
			err = executeDaySnowfallQuery(engine, &result)
		default:
			fmt.Println("Unknown query type")
			continue
		}

		if err != nil {
			log.Printf("Error executing query: %v\n", err)
			continue
		}

		printResults(result)
	}
}

func initFrostDB() {
	var err error
	columnstore, err = frostdb.New()
	if err != nil {
		log.Fatalf("Failed to create columnstore: %v", err)
	}

	database, err = columnstore.DB(context.Background(), "weather_db")
	if err != nil {
		log.Fatalf("Failed to create database: %v", err)
	}
}

func loadDemoData() {
	type WeatherRecord struct {
		City     map[string]string `frostdb:",rle_dict,asc(0)"`
		Day      string            `frostdb:",rle_dict,asc(1)"`
		Snowfall float64
	}

	table, err := frostdb.NewGenericTable[WeatherRecord](
		database, "snowfall_table", memory.DefaultAllocator,
	)
	if err != nil {
		log.Fatalf("Failed to create table: %v", err)
	}
	defer table.Release()

	montreal := map[string]string{"name": "Montreal", "province": "Quebec"}
	toronto := map[string]string{"name": "Toronto", "province": "Ontario"}
	minneapolis := map[string]string{"name": "Minneapolis", "state": "Minnesota"}

	_, err = table.Write(context.Background(),
		WeatherRecord{Day: "Mon", Snowfall: 20, City: montreal},
		WeatherRecord{Day: "Tue", Snowfall: 0, City: montreal},
		WeatherRecord{Day: "Wed", Snowfall: 30, City: montreal},
		WeatherRecord{Day: "Thu", Snowfall: 25.1, City: montreal},
		WeatherRecord{Day: "Fri", Snowfall: 10, City: montreal},
		WeatherRecord{Day: "Mon", Snowfall: 15, City: toronto},
		WeatherRecord{Day: "Tue", Snowfall: 25, City: toronto},
		WeatherRecord{Day: "Wed", Snowfall: 30, City: toronto},
		WeatherRecord{Day: "Thu", Snowfall: 0, City: toronto},
		WeatherRecord{Day: "Fri", Snowfall: 5, City: toronto},
		WeatherRecord{Day: "Mon", Snowfall: 40.8, City: minneapolis},
		WeatherRecord{Day: "Tue", Snowfall: 15, City: minneapolis},
		WeatherRecord{Day: "Wed", Snowfall: 32.3, City: minneapolis},
		WeatherRecord{Day: "Thu", Snowfall: 10, City: minneapolis},
		WeatherRecord{Day: "Fri", Snowfall: 12, City: minneapolis},
	)
	if err != nil {
		log.Fatalf("Failed to write demo data: %v", err)
	}

	fmt.Println("Demo data loaded successfully.")
}

func printResults(results []arrow.Record) {
	for _, record := range results {
		fmt.Println(record)
	}
}

func executeCityStatsQuery(engine *query.LocalEngine, result *[]arrow.Record) error {
	return engine.ScanTable("snowfall_table").
		Aggregate(
			[]*logicalplan.AggregationFunction{
				logicalplan.Max(logicalplan.Col("snowfall")),
				logicalplan.Min(logicalplan.Col("snowfall")),
				logicalplan.Avg(logicalplan.Col("snowfall")),
			},
			[]logicalplan.Expr{logicalplan.Col("city.name")},
		).
		Execute(context.Background(), func(ctx context.Context, r arrow.Record) error {
			*result = append(*result, r)
			return nil
		})
}

func executeDaySnowfallQuery(engine *query.LocalEngine, result *[]arrow.Record) error {
	return engine.ScanTable("snowfall_table").
		Aggregate(
			[]*logicalplan.AggregationFunction{
				logicalplan.Sum(logicalplan.Col("snowfall")),
			},
			[]logicalplan.Expr{logicalplan.Col("day")},
		).
		Execute(context.Background(), func(ctx context.Context, r arrow.Record) error {
			*result = append(*result, r)
			return nil
		})
}
