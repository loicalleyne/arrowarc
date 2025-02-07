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

// Once the server is started, you can use the client example or connect via JDBC: jdbc:arrow-flight-sql://localhost:12345?useEncryption=false

package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	sqlite "github.com/arrowarc/arrowarc/integrations/flight/sqlite"
	"github.com/docopt/docopt-go"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	usage := `Flight SQL Server.

Usage:
  flight_server --address=<address>
  flight_server -h | --help

Options:
  -h --help                      Show this screen.
  --address=<address>            Address to bind the server to [default: localhost:12345].
`

	arguments, err := docopt.ParseDoc(usage)
	if err != nil {
		log.Fatalf("Error parsing arguments: %v", err)
	}

	address, _ := arguments.String("--address")

	// Validate address
	if err := validateAddress(address); err != nil {
		log.Fatalf("Invalid address: %v", err)
	}

	// Start the server in the main goroutine
	startFlightSQLServer(address)

	// Run the client code in a separate goroutine to validate the server is up
	go func() {
		// Wait for a moment to allow the server to start
		time.Sleep(2 * time.Second)

		// Perform listFlights to check if the server is running
		if err := listFlights(address); err != nil {
			log.Fatalf("Error during listFlights: %v", err)
		} else {
			log.Println("Flight SQL Server is up and running at", address)
		}
	}()

	// Prevent the main goroutine from exiting immediately
	select {}
}

// validateAddress checks if the address is valid and has both a host and a port.
func validateAddress(address string) error {
	// Split the address into host and port
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return fmt.Errorf("invalid format: %v", err)
	}

	// Validate host is not empty
	if strings.TrimSpace(host) == "" {
		return fmt.Errorf("host is empty")
	}

	// Validate port is not empty and is a valid number
	if strings.TrimSpace(port) == "" {
		return fmt.Errorf("port is empty")
	}

	if _, err := net.LookupHost(host); err != nil {
		return fmt.Errorf("invalid host: %v", err)
	}

	return nil
}

// startFlightSQLServer initializes and starts the Flight SQL server using the SQLite example
func startFlightSQLServer(address string) {
	// Initialize the SQLite database
	db, err := sqlite.CreateDB()
	if err != nil {
		log.Fatalf("Failed to create SQLite database: %v", err)
	}
	defer db.Close()

	// Create the Flight SQL server
	srv, err := sqlite.NewSQLiteFlightSQLServer(db)
	if err != nil {
		log.Fatalf("Failed to create Flight SQL server: %v", err)
	}

	// Initialize the Flight server with middleware (if needed)
	server := flight.NewServerWithMiddleware(
		[]flight.ServerMiddleware{},
	)
	server.Init(address)

	// Register the Flight SQL service
	flightSQLServer := flightsql.NewFlightServer(srv)
	server.RegisterFlightService(flightSQLServer)

	log.Printf("Starting Flight SQL server on %s...\n", address)

	// Start the Flight SQL server
	if err := server.Serve(); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}

// listFlights performs a simple client-server transaction to list flights
func listFlights(address string) error {
	// Initialize Flight client
	client, err := flight.NewClientWithMiddleware(address, nil, nil, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	defer client.Close()

	// Send a ListFlights request to the server
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	flights, err := client.ListFlights(ctx, &flight.Criteria{})
	if err != nil {
		return err
	}
	fmt.Println("ListFlights response:", flights)
	return nil
}
