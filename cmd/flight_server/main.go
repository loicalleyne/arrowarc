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
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"github.com/apache/arrow/go/v17/arrow/flight"
	"github.com/apache/arrow/go/v17/arrow/flight/flightsql"
	sqllite "github.com/arrowarc/arrowarc/experiments/flightsql/sqllite"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

func main() {
	var (
		host = flag.String("host", "localhost", "hostname to bind to")
		port = flag.Int("port", 12345, "port to bind to")
	)

	flag.Parse()

	// Create the in-memory SQLite database
	db, err := sqllite.CreateDB()
	if err != nil {
		log.Fatalf("Failed to create SQLite database: %v", err)
	}
	defer db.Close()

	// Create the SQLiteFlightSQL server
	srv, err := sqllite.NewSQLiteFlightSQLServer(db)
	if err != nil {
		log.Fatalf("Failed to create FlightSQL server: %v", err)
	}

	// Create a new gRPC server
	grpcServer := grpc.NewServer()

	// Create a new FlightSQL service instance
	flightServer := flightsql.NewFlightServer(srv)

	// Register the Flight SQL service with the gRPC server
	flight.RegisterFlightServiceServer(grpcServer, flightServer)

	// Enable gRPC reflection
	reflection.Register(grpcServer)

	// Start listening on the specified address
	listener, err := net.Listen("tcp", net.JoinHostPort(*host, strconv.Itoa(*port)))
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	// Graceful shutdown handling
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	go func() {
		fmt.Printf("Starting SQLite Flight SQL Server on %s...\n", listener.Addr().String())
		if err := grpcServer.Serve(listener); err != nil {
			log.Fatalf("Failed to serve: %v", err)
		}
	}()

	<-stop
	fmt.Println("\nShutting down SQLite Flight SQL Server gracefully...")
	grpcServer.GracefulStop()
}
