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

package experiments

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/apache/arrow/go/v17/arrow/flight"
	"github.com/apache/arrow/go/v17/arrow/flight/flightsql"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

// Middleware for adding headers to the gRPC context
type ServerMiddlewareAddHeader struct{}

func (s *ServerMiddlewareAddHeader) StartCall(ctx context.Context) context.Context {
	grpc.SetHeader(ctx, metadata.Pairs("foo", "bar"))
	return ctx
}

func (s *ServerMiddlewareAddHeader) CallCompleted(ctx context.Context, err error) {
	grpc.SetTrailer(ctx, metadata.Pairs("super", "duper"))
	if err != nil {
		log.Printf("Server call completed with error: %v", err)
		return
	}
}

// Additional middlewares to be added
type ServerTraceMiddleware struct{}
type ServerExpectHeaderMiddleware struct{}

func (s *ServerTraceMiddleware) StartCall(ctx context.Context) context.Context {
	fmt.Println("Tracing server call...")
	return ctx
}

func (s *ServerTraceMiddleware) CallCompleted(ctx context.Context, err error) {
	if err != nil {
		fmt.Printf("Trace error: %v\n", err)
	}
}

func (s *ServerExpectHeaderMiddleware) StartCall(ctx context.Context) context.Context {
	fmt.Println("Expecting header in server call...")
	return ctx
}

func (s *ServerExpectHeaderMiddleware) CallCompleted(ctx context.Context, err error) {
	fmt.Println("Completed call with expected header")
}

// StartSQLiteServer initializes and starts the SQLite-backed Flight SQL server
func StartSQLiteServer() {
	// Initialize SQLite in-memory database
	db, err := CreateDB()
	if err != nil {
		log.Fatalf("Failed to create SQLite database: %v", err)
	}
	defer db.Close()

	// Create Flight SQL server
	srv, err := NewSQLiteFlightSQLServer(db)
	if err != nil {
		log.Fatalf("Failed to create FlightSQL server: %v", err)
	}

	// Create middleware
	serverMiddleware := []flight.ServerMiddleware{
		flight.CreateServerMiddleware(&ServerMiddlewareAddHeader{}),
		flight.CreateServerMiddleware(&ServerTraceMiddleware{}),
		flight.CreateServerMiddleware(&ServerExpectHeaderMiddleware{}),
	}

	// Initialize and start server
	address := "localhost:12345"
	server := flight.NewServerWithMiddleware(serverMiddleware)
	server.Init(address)
	server.RegisterFlightService(flightsql.NewFlightServer(srv))

	// Graceful shutdown handling
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	go func() {
		fmt.Printf("Starting Flight SQL Server on %s...\n", address)
		if err := server.Serve(); err != nil {
			log.Fatalf("Failed to serve: %v", err)
		}
	}()

	<-stop
	fmt.Println("Shutting down SQLite Flight SQL Server gracefully...")
	server.Shutdown()
}

// StartFlightClient initializes a Flight client and processes example requests
func StartFlightClient(address string) {
	// Initialize Flight client with required middleware
	client, err := flight.NewClientWithMiddleware(address, nil, []flight.ClientMiddleware{
		flight.CreateClientMiddleware(flight.NewCookieMiddleware()),
	}, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Failed to create Flight client: %v", err)
	}
	defer client.Close()

	// Example client request: ListFlights
	flightStream, err := client.ListFlights(context.Background(), &flight.Criteria{})
	if err != nil {
		log.Fatalf("Failed to list flights: %v", err)
	}

	// Process the flight stream
	for {
		info, err := flightStream.Recv()
		if err != nil {
			if err.Error() == "EOF" {
				break
			}
			log.Fatalf("Error receiving flight info: %v", err)
		}

		fmt.Printf("Received flight: %v\n", info.GetFlightDescriptor().GetPath())
	}
}
